package ledger

import (
	"context"
	"database/sql"
	"math/big"

	"github.com/formancehq/go-libs/v5/pkg/storage/postgres"

	ledger "github.com/hanzo-fi/ledger/internal"
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

				return postgres.ResolveError(err)
			}

			// FeatureMovesHistoryPostCommitEffectiveVolumes=SYNC: post_commit_effective_volumes
			// is computed here in Go — the port of the retired set_effective_volumes (before
			// insert) / update_effective_volumes (after insert) triggers. Moves are chained one
			// row at a time, in slice (seq) order, so each row sees the rows inserted before it
			// exactly as the per-row triggers did. Amounts are exact integers, so the Go add is
			// byte-identical to the plpgsql numeric arithmetic.
			for _, move := range moves {
				if err := store.insertMoveWithEffectiveVolumes(ctx, move); err != nil {
					return err
				}
			}
			return nil
		}),
	)

	return err
}

// insertMoveWithEffectiveVolumes inserts a single move with its
// post_commit_effective_volumes and back-dates every later-effective move for the
// same account+asset, replacing the set_effective_volumes/update_effective_volumes
// triggers. The delta is +amount on outputs when is_source, else +amount on inputs.
func (store *Store) insertMoveWithEffectiveVolumes(ctx context.Context, move *ledger.Move) error {
	moves := store.GetPrefixedRelationName("moves")

	amount := (*big.Int)(move.Amount)
	inputDelta, outputDelta := big.NewInt(0), big.NewInt(0)
	if move.IsSource {
		outputDelta = amount
	} else {
		inputDelta = amount
	}

	// set_effective_volumes: the running effective volume is the most recent move
	// (effective_date desc, seq desc) at or before this one. The row is not yet
	// inserted, so every candidate has seq < this move's seq, which collapses the
	// trigger's `effective_date < d or (effective_date = d and seq < new.seq)` to
	// `effective_date <= d`. Missing (first move for the account/asset) means a zero base.
	prior := ledger.NewEmptyVolumes()
	var raw sql.NullString
	err := store.db.NewSelect().
		ColumnExpr("post_commit_effective_volumes::text").
		ModelTableExpr(moves).
		Where("accounts_address = ?", move.Account).
		Where("asset = ?", move.Asset).
		Where("ledger = ?", store.ledger.Name).
		Where("effective_date <= ?", move.EffectiveDate).
		OrderExpr("effective_date desc, seq desc").
		Limit(1).
		Scan(ctx, &raw)
	if err != nil {
		err = postgres.ResolveError(err)
		if !postgres.IsNotFoundError(err) {
			return err
		}
	} else if raw.Valid {
		if err := prior.Scan(raw.String); err != nil {
			return err
		}
	}

	move.PostCommitEffectiveVolumes = &ledger.Volumes{
		Input:  new(big.Int).Add(prior.Input, inputDelta),
		Output: new(big.Int).Add(prior.Output, outputDelta),
	}

	// Write the move; post_commit_effective_volumes is stored the same way
	// post_commit_volumes is (Volumes.Value renders the composite text).
	if _, err := store.db.NewInsert().
		Model(move).
		Value("ledger", "?", store.ledger.Name).
		ModelTableExpr(moves).
		Exec(ctx); err != nil {
		return postgres.ResolveError(err)
	}

	// update_effective_volumes: back-date every later-effective move by this delta
	// (the exact plpgsql arithmetic, keeping amount as its proven numeric param type).
	_, err = store.db.NewRaw(`update `+moves+`
		set post_commit_effective_volumes = (
			(post_commit_effective_volumes).inputs + case when ? then 0 else ? end,
			(post_commit_effective_volumes).outputs + case when ? then ? else 0 end
		)
		where accounts_address = ? and asset = ? and ledger = ? and effective_date > ?`,
		move.IsSource, move.Amount, move.IsSource, move.Amount,
		move.Account, move.Asset, store.ledger.Name, move.EffectiveDate,
	).Exec(ctx)

	return postgres.ResolveError(err)
}
