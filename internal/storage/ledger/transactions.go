package ledger

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/paginate"
	. "github.com/hanzo-fi/go-libs/v5/pkg/types/collections"
	"github.com/hanzo-fi/go-libs/v5/pkg/types/metadata"
	"github.com/hanzo-fi/go-libs/v5/pkg/types/pointer"
	"github.com/hanzo-fi/go-libs/v5/pkg/types/time"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/storage/dialect"
	"github.com/hanzo-fi/ledger/internal/tracing"
	"github.com/hanzo-fi/ledger/pkg/features"
)

func (store *Store) CommitTransaction(ctx context.Context, tx *ledger.Transaction) error {

	postCommitVolumes, err := store.UpdateVolumes(ctx, tx.VolumeUpdates()...)
	if err != nil {
		return fmt.Errorf("failed to update balances: %w", err)
	}
	tx.PostCommitVolumes = postCommitVolumes.Copy()

	err = store.InsertTransaction(ctx, tx)
	if err != nil {
		return fmt.Errorf("failed to insert transaction: %w", err)
	}

	if store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
		moves := ledger.Moves{}
		postings := make([]ledger.Posting, len(tx.Postings))
		copy(postings, tx.Postings)
		slices.Reverse(postings)

		for _, posting := range postings {
			moves = append(moves, &ledger.Move{
				Account:           posting.Destination,
				Amount:            (*paginate.BigInt)(posting.Amount),
				Asset:             posting.Asset,
				InsertionDate:     tx.InsertedAt,
				EffectiveDate:     tx.Timestamp,
				PostCommitVolumes: pointer.For(postCommitVolumes[posting.Destination][posting.Asset].Copy()),
				TransactionID:     *tx.ID,
			})
			postCommitVolumes.AddInput(posting.Destination, posting.Asset, new(big.Int).Neg(posting.Amount))

			moves = append(moves, &ledger.Move{
				IsSource:          true,
				Account:           posting.Source,
				Amount:            (*paginate.BigInt)(posting.Amount),
				Asset:             posting.Asset,
				InsertionDate:     tx.InsertedAt,
				EffectiveDate:     tx.Timestamp,
				PostCommitVolumes: pointer.For(postCommitVolumes[posting.Source][posting.Asset].Copy()),
				TransactionID:     *tx.ID,
			})
			postCommitVolumes.AddOutput(posting.Source, posting.Asset, new(big.Int).Neg(posting.Amount))
		}

		slices.Reverse(moves)

		if err := store.InsertMoves(ctx, moves...); err != nil {
			return fmt.Errorf("failed to insert moves: %w", err)
		}

		if store.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
			tx.PostCommitEffectiveVolumes = moves.ComputePostCommitEffectiveVolumes()
		}
	}

	return nil
}

func (store *Store) InsertTransaction(ctx context.Context, tx *ledger.Transaction) error {
	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"InsertTransaction",
		store.tracer,
		store.insertTransactionHistogram,
		func(ctx context.Context) (*ledger.Transaction, error) {
			// Stamp the date columns that previously defaulted to transaction_date()
			// (timestamp, inserted_at) and the updated_at the set_transaction_updated_at
			// trigger copied from inserted_at. A caller-provided value wins, exactly as
			// the plpgsql defaults/trigger only fired `when null`; the shared per-tx
			// date keeps the moves (InsertionDate/EffectiveDate, derived below) aligned.
			d := store.transactionDate()
			if tx.Timestamp.IsZero() {
				tx.Timestamp = d
			}
			if tx.InsertedAt.IsZero() {
				tx.InsertedAt = d
			}
			if tx.UpdatedAt.IsZero() {
				tx.UpdatedAt = tx.InsertedAt
			}

			type transaction struct {
				*ledger.Transaction `bun:",extend"`
				Sources             []string         `bun:"sources,notnull"`
				Destinations        []string         `bun:"destinations,notnull"`
				SourcesArrays       []map[string]any `bun:"sources_arrays,notnull"`
				DestinationsArrays  []map[string]any `bun:"destinations_arrays,notnull"`
			}

			sources := Map(tx.Postings, ledger.Posting.GetSource)
			sourcesArrays := Map(sources, explodeAddress)
			destinations := Map(tx.Postings, ledger.Posting.GetDestination)
			destinationsArrays := Map(destinations, explodeAddress)

			query := store.db.NewInsert().
				Model(&transaction{
					Transaction:        tx,
					Sources:            sources,
					Destinations:       destinations,
					SourcesArrays:      sourcesArrays,
					DestinationsArrays: destinationsArrays,
				}).
				ModelTableExpr(store.GetPrefixedRelationName("transactions")).
				Value("ledger", "?", store.ledger.Name).
				Returning("id, timestamp, inserted_at, updated_at")

			// A transaction carries three instants and the store stamps any
			// the caller left open, all off the same per-transaction clock.
			at := store.transactionDate()
			if tx.Timestamp.IsZero() {
				query = query.Value("timestamp", "?", at)
			}
			if tx.InsertedAt.IsZero() {
				query = query.Value("inserted_at", "?", at)
			}
			if tx.UpdatedAt.IsZero() {
				query = query.Value("updated_at", "?", at)
			}

			if tx.ID == nil {
				next := store.dialect.NextID(store.ledger.Bucket, "transactions", store.ledger.Name, store.ledger.ID)
				query = query.Value("id", next.SQL, next.Args...)
			}

			_, err := query.Exec(ctx)
			if err != nil {
				err = store.dialect.ResolveError(err)
				switch {
				case errors.Is(err, dialect.ErrConstraint{}):
					switch dialect.Constraint(err) {
					case "transactions_reference":
						return nil, NewErrTransactionReferenceConflict(tx.Reference)
					case "transactions_ledger":
						return nil, NewErrConcurrentTransaction(*tx.ID)
					}

					return nil, err
				default:
					return nil, err
				}
			}

			// Append the initial metadata-history revision, replacing the retired
			// insert_transaction_metadata_history plpgsql trigger (revision 1 dated at
			// the transaction timestamp).
			if err := store.appendTransactionMetadataHistory(ctx, *tx.ID, tx.Timestamp, tx.Metadata); err != nil {
				return nil, err
			}

			return tx, nil
		},
		func(ctx context.Context, tx *ledger.Transaction) {
			trace.SpanFromContext(ctx).SetAttributes(
				attribute.String("transaction.id", fmt.Sprint(tx.ID)),
				attribute.String("transaction.timestamp", tx.Timestamp.Format(time.RFC3339Nano)),
				attribute.String("transaction.reference", tx.Reference),
			)
		},
	))
}

// updateTxWithRetrieve try to apply to provided update query and check (if the update return no rows modified), that the row exists
func (store *Store) updateTxWithRetrieve(ctx context.Context, id uint64, query *bun.UpdateQuery) (*ledger.Transaction, bool, error) {
	type modifiedEntity struct {
		ledger.Transaction `bun:",extend"`
		Modified           bool `bun:"modified"`
	}
	me := &modifiedEntity{}

	err := store.db.NewSelect().
		With("upd", query).
		ModelTableExpr(
			"(?) transactions",
			store.db.NewSelect().
				ColumnExpr("upd.*, true as modified").
				ModelTableExpr("upd").
				UnionAll(
					store.db.NewSelect().
						ModelTableExpr(store.GetPrefixedRelationName("transactions")).
						ColumnExpr("*, false as modified").
						Where("id = ? and ledger = ?", id, store.ledger.Name).
						Limit(1),
				),
		).
		Model(me).
		ColumnExpr("*").
		Limit(1).
		Scan(ctx)
	if err != nil {
		return &me.Transaction, me.Modified, store.dialect.ResolveError(err)
	}

	// Every update that actually modified the row would have fired the retired
	// update_transaction_metadata_history plpgsql trigger (created `after update`,
	// so metadata updates, deletes and reverts alike). Mirror it in Go: append a
	// new revision dated at the row's updated_at with its resulting metadata.
	if me.Modified {
		if err := store.appendTransactionMetadataHistory(ctx, id, me.Transaction.UpdatedAt, me.Transaction.Metadata); err != nil {
			return &me.Transaction, me.Modified, err
		}
	}

	return &me.Transaction, me.Modified, nil
}

// appendTransactionMetadataHistory writes one transactions_metadata row mirroring
// the retired {insert,update}_transaction_metadata_history plpgsql triggers: the
// revision is max(revision)+1 for the (ledger, transaction) or 1 when none exists,
// computed inline in SQL so it stays atomic under the transaction row lock the
// caller already holds. No-op unless the feature is SYNC, matching the condition
// under which the triggers were created.
func (store *Store) appendTransactionMetadataHistory(ctx context.Context, id uint64, at time.Time, m metadata.Metadata) error {
	if !store.ledger.HasFeature(features.FeatureTransactionMetadataHistory, "SYNC") {
		return nil
	}
	if m == nil {
		m = metadata.Metadata{}
	}
	// A zero date maps to NULL, matching the `nullzero` date columns the triggers
	// copied from (new.timestamp / new.updated_at) — the PIT reads filter on
	// `date <= ?`, so NULL must stay NULL.
	var date any
	if !at.IsZero() {
		date = at
	}
	table := store.GetPrefixedRelationName("transactions_metadata")
	_, err := store.db.NewRaw(
		"insert into "+table+" (ledger, transactions_id, revision, date, metadata) "+
			"values (?, ?, coalesce((select max(revision) + 1 from "+table+" where transactions_id = ? and ledger = ?), 1), ?, ?)",
		store.ledger.Name, id, id, store.ledger.Name, date, m,
	).Exec(ctx)
	return store.dialect.ResolveError(err)
}

func (store *Store) RevertTransaction(ctx context.Context, id uint64, at time.Time) (tx *ledger.Transaction, modified bool, err error) {
	_, err = tracing.TraceWithMetric(
		ctx,
		"RevertTransaction",
		store.tracer,
		store.revertTransactionHistogram,
		func(ctx context.Context) (*ledger.Transaction, error) {
			query := store.db.NewUpdate().
				Model(&ledger.Transaction{}).
				ModelTableExpr(store.GetPrefixedRelationName("transactions")).
				Where("id = ?", id).
				Where("reverted_at is null").
				Where("ledger = ?", store.ledger.Name).
				Returning("*")
			if at.IsZero() {
				at = store.transactionDate()
			}
			query = query.
				Set("reverted_at = ?", at).
				Set("updated_at = ?", at)

			tx, modified, err = store.updateTxWithRetrieve(ctx, id, query)
			return nil, err
		},
	)
	return tx, modified, err
}

func (store *Store) UpdateTransactionMetadata(ctx context.Context, id uint64, m metadata.Metadata, at time.Time) (tx *ledger.Transaction, modified bool, err error) {
	_, err = tracing.TraceWithMetric(
		ctx,
		"UpdateTransactionMetadata",
		store.tracer,
		store.updateTransactionMetadataHistogram,
		func(ctx context.Context) (*ledger.Transaction, error) {

			holds := store.dialect.Holds("metadata", toEntries(m))
			updateQuery := store.db.NewUpdate().
				Model(&ledger.Transaction{}).
				ModelTableExpr(store.GetPrefixedRelationName("transactions")).
				Where("id = ?", id).
				Where("ledger = ?", store.ledger.Name).
				Set("metadata = "+store.dialect.Merge("metadata", "?"), m).
				Where("not "+holds.SQL, holds.Args...).
				Returning("*")
			if at.IsZero() {
				at = store.transactionDate()
			}
			updateQuery = updateQuery.Set("updated_at = ?", at)

			tx, modified, err = store.updateTxWithRetrieve(ctx, id, updateQuery)

			return nil, store.dialect.ResolveError(err)
		},
	)
	return tx, modified, err
}

func (store *Store) DeleteTransactionMetadata(ctx context.Context, id uint64, key string, at time.Time) (tx *ledger.Transaction, modified bool, err error) {
	_, err = tracing.TraceWithMetric(
		ctx,
		"DeleteTransactionMetadata",
		store.tracer,
		store.deleteTransactionMetadataHistogram,
		func(ctx context.Context) (*ledger.Transaction, error) {
			remove := store.dialect.Remove("metadata", key)
			has := store.dialect.Has("metadata", key)
			updateQuery := store.db.NewUpdate().
				Model(&ledger.Transaction{}).
				ModelTableExpr(store.GetPrefixedRelationName("transactions")).
				Set("metadata = "+remove.SQL, remove.Args...).
				Where("id = ?", id).
				Where("ledger = ?", store.ledger.Name).
				Where(has.SQL, has.Args...).
				Returning("*")
			if at.IsZero() {
				at = store.transactionDate()
			}
			updateQuery = updateQuery.Set("updated_at = ?", at)

			tx, modified, err = store.updateTxWithRetrieve(ctx, id, updateQuery)
			return nil, store.dialect.ResolveError(err)
		},
	)
	return tx, modified, err
}

func (store *Store) filterAddressOnTransactions(address string, source, destination bool) dialect.Fragment {
	columns := make([]string, 0, 2)
	if isPartialAddress(address) {
		if source {
			columns = append(columns, "sources_arrays")
		}
		if destination {
			columns = append(columns, "destinations_arrays")
		}
		return anyOf(columns, func(column string) dialect.Fragment {
			return store.dialect.SegmentsMatch(column, addressSegments(address))
		})
	}
	if source {
		columns = append(columns, "sources")
	}
	if destination {
		columns = append(columns, "destinations")
	}
	return anyOf(columns, func(column string) dialect.Fragment {
		return store.dialect.ArrayHolds(column, address)
	})
}

// addressSegments explodes a partial address into the segments an exploded
// address array must carry. A nil segment asserts the address ends there.
func addressSegments(address string) map[string]any {
	src := strings.Split(address, ":")
	segments := map[string]any{}
	for i, segment := range src {
		if len(segment) == 0 {
			continue
		}
		if i == len(src)-1 && segment == "..." {
			break
		}
		segments[fmt.Sprint(i)] = segment
	}
	if src[len(src)-1] != "..." {
		segments[fmt.Sprint(len(src))] = nil
	}
	return segments
}

// anyOf disjoins one fragment per column.
func anyOf(columns []string, build func(string) dialect.Fragment) dialect.Fragment {
	parts := make([]string, 0, len(columns))
	args := make([]any, 0, len(columns))
	for _, column := range columns {
		fragment := build(column)
		parts = append(parts, fragment.SQL)
		args = append(args, fragment.Args...)
	}
	return dialect.Fragment{SQL: strings.Join(parts, " or "), Args: args}
}

func toEntries(m metadata.Metadata) map[string]any {
	entries := make(map[string]any, len(m))
	for k, v := range m {
		entries[k] = v
	}
	return entries
}

func assetAddressArray(v any) ([]string, error) {
	value := v.([]any)
	addresses := Map(value, func(v any) string {
		return v.(string)
	})
	for _, address := range addresses {
		if isPartialAddress(address) {
			return nil, NewErrInvalidQuery("IN operator only supports full addresses")
		}
	}

	return addresses, nil
}
