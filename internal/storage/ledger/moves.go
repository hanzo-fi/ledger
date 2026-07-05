package ledger

import (
	"context"

	"github.com/formancehq/go-libs/v5/pkg/storage/postgres"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/tracing"
)

func (store *Store) InsertMoves(ctx context.Context, moves ...*ledger.Move) error {
	_, err := tracing.TraceWithMetric(
		ctx,
		"InsertMoves",
		store.tracer,
		store.insertMovesHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			_, err := store.db.NewInsert().
				Model(&moves).
				Value("ledger", "?", store.ledger.Name).
				ModelTableExpr(store.GetPrefixedRelationName("moves")).
				Returning("post_commit_volumes, post_commit_effective_volumes").
				Exec(ctx)

			return postgres.ResolveError(err)
		}),
	)

	return err
}
