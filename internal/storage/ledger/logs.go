package ledger

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/postgres"
	"github.com/hanzo-fi/go-libs/v5/pkg/types/pointer"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/tracing"
	"github.com/hanzo-fi/ledger/pkg/features"
)

// Log override ledger.Log to be able to properly read/write payload which is jsonb
// on the database and 'any' on the Log structure (data column)
type Log struct {
	*ledger.Log `bun:",extend"`

	Ledger  string     `bun:"ledger,type:varchar"`
	Data    RawMessage `bun:"data,type:jsonb"`
	Memento []byte     `bun:"memento,type:bytea"`
}

func (log Log) ToCore() ledger.Log {
	payload, err := ledger.HydrateLog(log.Type, log.Data)
	if err != nil {
		panic(fmt.Errorf("hydrating log data: %w", err))
	}
	log.Log.Data = payload

	return *log.Log
}

type RawMessage json.RawMessage

func (j RawMessage) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return string(j), nil
}

func (store *Store) InsertLog(ctx context.Context, log *ledger.Log) error {

	_, err := tracing.TraceWithMetric(
		ctx,
		"InsertLog",
		store.tracer,
		store.insertLogHistogram,
		tracing.NoResult(func(ctx context.Context) error {

			// date previously defaulted to transaction_date(); stamp the shared per-tx
			// date in Go now that the default is retired, so the log carries the same
			// instant as the transaction/accounts/moves written in this db transaction
			// (and, under FeatureHashLogs, so the hash covers the exact stored date).
			if log.Date.IsZero() {
				log.Date = store.transactionDate()
			}

			// We lock logs table as we need than the last log does not change until the transaction commit
			if store.ledger.HasFeature(features.FeatureHashLogs, "SYNC") {
				_, err := store.db.NewRaw(`select pg_advisory_xact_lock(?)`, store.ledger.ID).Exec(ctx)
				if err != nil {
					return postgres.ResolveError(err)
				}

				// Chain the log hash in Go via the canonical ledger.Log.ComputeHash —
				// the same hashing ledgercore uses — instead of the retired set_log_hash
				// plpgsql trigger. Under the advisory lock the head is stable, and the
				// result is byte-identical to the plpgsql hash by construction (the
				// plpgsql was written to match ComputeHash).
				previous, err := store.readLogHead(ctx)
				if err != nil {
					return err
				}
				log.ComputeHash(previous)
			}

			payloadData, err := json.Marshal(log.Data)
			if err != nil {
				return fmt.Errorf("failed to marshal log data: %w", err)
			}

			mementoObject := log.Data.(any)
			if memento, ok := mementoObject.(ledger.Memento); ok {
				mementoObject = memento.GetMemento()
			}

			mementoData, err := json.Marshal(mementoObject)
			if err != nil {
				return err
			}

			query := store.db.
				NewInsert().
				Model(&Log{
					Log:     log,
					Ledger:  store.ledger.Name,
					Data:    payloadData,
					Memento: mementoData,
				}).
				ModelTableExpr(store.GetPrefixedRelationName("logs")).
				Returning("*")

			if log.ID == nil {
				query = query.Value("id", "nextval(?)", store.GetPrefixedRelationName(fmt.Sprintf(`"log_id_%d"`, store.ledger.ID)))
			}

			_, err = query.Exec(ctx)
			if err != nil {
				err := postgres.ResolveError(err)
				switch {
				case errors.Is(err, postgres.ErrConstraintsFailed{}):
					if err.(postgres.ErrConstraintsFailed).GetConstraint() == "logs_idempotency_key" {
						return NewErrIdempotencyKeyConflict(log.IdempotencyKey)
					}
				default:
					return fmt.Errorf("inserting log: %w", err)
				}
			}

			return nil
		}),
	)

	return err
}

// readLogHead returns the ledger's current log head (only its hash is needed to
// chain the next log), or nil if the ledger has no logs yet. Callers must hold
// the per-ledger advisory lock so the head is stable until the chained log is
// inserted.
func (store *Store) readLogHead(ctx context.Context) (*ledger.Log, error) {
	var hash []byte
	err := store.db.NewSelect().
		ColumnExpr("hash").
		TableExpr(store.GetPrefixedRelationName("logs")).
		Where("ledger = ?", store.ledger.Name).
		Order("id DESC").
		Limit(1).
		Scan(ctx, &hash)
	if err != nil {
		err = postgres.ResolveError(err)
		if postgres.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	return &ledger.Log{Hash: hash}, nil
}

func (store *Store) ReadLogWithIdempotencyKey(ctx context.Context, key string) (*ledger.Log, error) {
	return tracing.TraceWithMetric(
		ctx,
		"ReadLogWithIdempotencyKey",
		store.tracer,
		store.readLogWithIdempotencyKeyHistogram,
		func(ctx context.Context) (*ledger.Log, error) {
			ret := &Log{}
			if err := store.db.NewSelect().
				Model(ret).
				ModelTableExpr(store.GetPrefixedRelationName("logs")).
				Column("*").
				Where("idempotency_key = ?", key).
				Where("ledger = ?", store.ledger.Name).
				Limit(1).
				Scan(ctx); err != nil {
				return nil, postgres.ResolveError(err)
			}

			return pointer.For(ret.ToCore()), nil
		},
	)
}
