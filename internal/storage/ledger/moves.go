package ledger

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/hanzo-fi/go-libs/v5/pkg/types/time"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/storage/dialect"
	"github.com/hanzo-fi/ledger/internal/tracing"
	"github.com/hanzo-fi/ledger/pkg/features"
)

func (store *Store) InsertMoves(ctx context.Context, moves ...*ledger.Move) error {
	_, err := tracing.TraceWithMetric(
		ctx,
		"InsertMoves",
		store.tracer,
		store.insertMovesHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			// Without the effective-volume chain, the moves are independent: one bulk insert.
			if !store.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
				_, err := store.db.NewInsert().
					Model(&moves).
					Value("ledger", "?", store.ledger.Name).
					ModelTableExpr(store.GetPrefixedRelationName("moves")).
					Returning("post_commit_volumes, post_commit_effective_volumes").
					Exec(ctx)

				return store.dialect.ResolveError(err)
			}

			return store.insertMovesDeriving(ctx, moves...)
		}),
	)

	return err
}

// move is a moves row as this engine writes it: every column stated, including
// the two the Postgres triggers would have filled.
type move struct {
	Ledger                     string           `bun:"ledger"`
	TransactionID              uint64           `bun:"transactions_id"`
	IsSource                   bool             `bun:"is_source"`
	Account                    string           `bun:"accounts_address"`
	Asset                      string           `bun:"asset"`
	Amount                     *paginate.BigInt `bun:"amount"`
	InsertionDate              time.Time        `bun:"insertion_date,nullzero"`
	EffectiveDate              time.Time        `bun:"effective_date,nullzero"`
	PostCommitVolumes          *ledger.Volumes  `bun:"post_commit_volumes"`
	PostCommitEffectiveVolumes *ledger.Volumes  `bun:"post_commit_effective_volumes"`
}

// insertMovesDeriving carries out what the Postgres triggers do, in Go and at
// full precision: a move takes the effective volumes of the last move standing
// before it on the same account and asset, and every move that follows it in
// effective time shifts by the same delta.
//
// The engine that needs this admits one writer at a time, so reading the
// preceding move and inserting after it cannot interleave with another writer.
func (store *Store) insertMovesDeriving(ctx context.Context, moves ...*ledger.Move) error {
	for _, m := range moves {
		input, output := delta(m)

		standing, err := store.effectiveVolumesBefore(ctx, m)
		if err != nil {
			return err
		}
		m.PostCommitEffectiveVolumes = &ledger.Volumes{
			Input:  new(big.Int).Add(standing.Input, input),
			Output: new(big.Int).Add(standing.Output, output),
		}

		if _, err := store.db.NewInsert().
			Model(&move{
				Ledger:                     store.ledger.Name,
				TransactionID:              m.TransactionID,
				IsSource:                   m.IsSource,
				Account:                    m.Account,
				Asset:                      m.Asset,
				Amount:                     m.Amount,
				InsertionDate:              m.InsertionDate,
				EffectiveDate:              m.EffectiveDate,
				PostCommitVolumes:          m.PostCommitVolumes,
				PostCommitEffectiveVolumes: m.PostCommitEffectiveVolumes,
			}).
			ModelTableExpr(store.GetPrefixedRelationName("moves")).
			Exec(ctx); err != nil {
			return store.dialect.ResolveError(err)
		}

		if err := store.shiftEffectiveVolumes(ctx, m, input, output); err != nil {
			return err
		}
	}

	return nil
}

// effectiveVolumesBefore reads the effective volumes standing immediately
// before a move. A move opening an account and asset stands on zero.
func (store *Store) effectiveVolumesBefore(ctx context.Context, m *ledger.Move) (ledger.Volumes, error) {
	standing := make([]string, 0, 1)
	err := store.db.NewSelect().
		TableExpr(store.GetPrefixedRelationName("moves")).
		ColumnExpr("post_commit_effective_volumes").
		Where("ledger = ?", store.ledger.Name).
		Where("accounts_address = ?", m.Account).
		Where("asset = ?", m.Asset).
		Where("effective_date <= ?", m.EffectiveDate).
		Where("post_commit_effective_volumes is not null").
		Order("effective_date desc", "seq desc").
		Limit(1).
		Scan(ctx, &standing)
	if err != nil && !errors.Is(store.dialect.ResolveError(err), dialect.ErrNotFound) {
		return ledger.Volumes{}, fmt.Errorf("reading effective volumes: %w", store.dialect.ResolveError(err))
	}
	if len(standing) == 0 {
		return ledger.NewEmptyVolumes(), nil
	}

	return readVolumes(standing[0])
}

// readVolumes parses the stored form of a volumes pair.
func readVolumes(raw string) (ledger.Volumes, error) {
	volumes := ledger.Volumes{}
	if err := volumes.Scan(raw); err != nil {
		return ledger.Volumes{}, fmt.Errorf("reading volumes %q: %w", raw, err)
	}
	return volumes, nil
}

// shiftEffectiveVolumes moves every later move by this move's delta, so a
// backdated transaction leaves the ones after it consistent.
func (store *Store) shiftEffectiveVolumes(ctx context.Context, m *ledger.Move, input, output *big.Int) error {
	var (
		sequences []int64
		volumes   []string
	)
	err := store.db.NewSelect().
		TableExpr(store.GetPrefixedRelationName("moves")).
		ColumnExpr("seq").
		ColumnExpr("post_commit_effective_volumes").
		Where("ledger = ?", store.ledger.Name).
		Where("accounts_address = ?", m.Account).
		Where("asset = ?", m.Asset).
		Where("effective_date > ?", m.EffectiveDate).
		Where("post_commit_effective_volumes is not null").
		Scan(ctx, &sequences, &volumes)
	if err != nil && !errors.Is(store.dialect.ResolveError(err), dialect.ErrNotFound) {
		return fmt.Errorf("reading later moves: %w", store.dialect.ResolveError(err))
	}

	for i, seq := range sequences {
		standing, err := readVolumes(volumes[i])
		if err != nil {
			return err
		}
		shifted := &ledger.Volumes{
			Input:  new(big.Int).Add(standing.Input, input),
			Output: new(big.Int).Add(standing.Output, output),
		}
		if _, err := store.db.NewUpdate().
			TableExpr(store.GetPrefixedRelationName("moves")).
			Set("post_commit_effective_volumes = ?", shifted).
			Where("seq = ?", seq).
			Exec(ctx); err != nil {
			return fmt.Errorf("shifting effective volumes: %w", store.dialect.ResolveError(err))
		}
	}

	return nil
}

// delta is what a move adds to the running volumes of its account and asset.
func delta(m *ledger.Move) (input, output *big.Int) {
	amount := (*big.Int)(m.Amount)
	if m.IsSource {
		return new(big.Int), new(big.Int).Set(amount)
	}
	return new(big.Int).Set(amount), new(big.Int)
}
