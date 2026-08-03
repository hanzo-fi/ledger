package ledger

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/storage/common"
	"github.com/hanzo-fi/ledger/internal/storage/dialect"
)

// expandStore builds the store the accounts handler needs to emit SQL. Nothing
// here talks to a database: bun only needs a dialect to render a statement.
func expandStore(t *testing.T) *Store {
	t.Helper()
	return New(
		bun.NewDB(nil, pgdialect.New()),
		dialect.SQL{},
		nil,
		ledger.MustNewWithDefault("demo"),
	)
}

// TestAccountsExpandEmittedSQL pins the SQL emitted for ?expand=. The value is
// caller controlled and lands in a ColumnExpr alias, which bun renders raw, so
// anything but a known expansion has to be refused before it gets there.
func TestAccountsExpandEmittedSQL(t *testing.T) {
	t.Parallel()

	handler := accountsResourceHandler{store: expandStore(t)}

	t.Run("known expansion is accepted", func(t *testing.T) {
		t.Parallel()

		q, _, err := handler.Expand(common.ResourceQuery[any]{}, "volumes")
		require.NoError(t, err)
		require.NotNil(t, q)
		t.Logf("EMITTED SQL: %s", q.String())
		require.Contains(t, q.String(), "as volumes")
	})

	for _, payload := range []string{
		"x/**/from/**/accounts/**/where/**/(select/**/1)=1",
		"x'||(select/**/current_user)||'",
		"volumes/**/union/**/select/**/1/**/1",
	} {
		t.Run(payload, func(t *testing.T) {
			t.Parallel()

			q, _, err := handler.Expand(common.ResourceQuery[any]{}, payload)
			if q != nil {
				t.Logf("EMITTED SQL: %s", q.String())
			}
			require.Error(t, err, "an unknown expansion must be refused, not rendered")
			require.Nil(t, q)
		})
	}
}
