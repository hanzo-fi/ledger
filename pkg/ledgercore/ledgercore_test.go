package ledgercore

import (
	"context"
	"database/sql"
	"math/big"
	"os/exec"
	stdtime "time"

	"testing"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	logging "github.com/hanzo-fi/go-libs/v5/pkg/observe/log"
	"github.com/hanzo-fi/go-libs/v5/pkg/testing/docker"
	"github.com/hanzo-fi/go-libs/v5/pkg/testing/platform/pgtesting"
	"github.com/hanzo-fi/go-libs/v5/pkg/types/time"

	ledger "github.com/hanzo-fi/ledger/internal"
)

// TestDoubleEntryRoundTrip is the decomplect gate: the same double-entry
// round-trip — create ledger, post a balanced transaction, assert both
// balances, assert the log hash chains, revert, assert balances restored and
// the revert log chained — runs over the dialect-agnostic Go spine on BOTH
// SQLite (default, per-tenant file) and Postgres, and the resulting hash-chain
// head is byte-identical across dialects. Fixed timestamps make the hashes
// reproducible so the cross-dialect equality is meaningful.
func TestDoubleEntryRoundTrip(t *testing.T) {
	base := time.New(stdtime.Date(2026, 7, 5, 12, 0, 0, 0, stdtime.UTC))
	revertAt := time.New(stdtime.Date(2026, 7, 5, 13, 0, 0, 0, stdtime.UTC))

	heads := map[string][]byte{}

	t.Run("sqlite", func(t *testing.T) {
		db, err := openLedgerFile(t.TempDir(), "acme")
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })

		heads["sqlite"] = runRoundTrip(t, db, base, revertAt)
	})

	t.Run("postgres", func(t *testing.T) {
		if exec.Command("docker", "info").Run() != nil {
			t.Skip("docker unavailable; skipping postgres dialect")
		}
		srv := pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))
		database := srv.NewDatabase(t)

		sqldb, err := sql.Open("pgx", database.ConnString())
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqldb.Close() })

		db := bun.NewDB(sqldb, pgdialect.New(), bun.WithDiscardUnknownColumns())
		heads["postgres"] = runRoundTrip(t, db, base, revertAt)
	})

	if len(heads["sqlite"]) > 0 && len(heads["postgres"]) > 0 {
		require.Equal(t, heads["sqlite"], heads["postgres"],
			"hash-chain head must be byte-identical across dialects (dialect-agnostic)")
		t.Logf("dialect-agnostic: chain head byte-identical on both dialects: sqlite=%x postgres=%x",
			heads["sqlite"], heads["postgres"])
	}
}

// runRoundTrip executes the double-entry scenario against db and returns the
// hash-chain head. It uses fixed timestamps so the hashes are reproducible.
func runRoundTrip(t *testing.T, db bun.IDB, base, revertAt time.Time) []byte {
	t.Helper()
	ctx := context.Background()

	require.NoError(t, Migrate(ctx, db))
	store := New(db, "acme")

	// Post a balanced transaction: 100 USD from alice to bob.
	tx := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("alice", "bob", "USD", big.NewInt(100))).
		WithTimestamp(base)
	require.NoError(t, store.CommitTransaction(ctx, &tx, nil))
	require.NotNil(t, tx.ID)
	require.Equal(t, uint64(1), *tx.ID)

	requireBalance(t, ctx, store, "alice", "USD", -100)
	requireBalance(t, ctx, store, "bob", "USD", 100)

	agg, err := store.GetAggregatedBalances(ctx)
	require.NoError(t, err)
	require.Equal(t, "-100", agg["alice"]["USD"].String())
	require.Equal(t, "100", agg["bob"]["USD"].String())

	// The NEW_TRANSACTION log hashes consistently.
	require.NoError(t, store.VerifyHashChain(ctx))
	head1, err := store.lastLog(ctx)
	require.NoError(t, err)
	require.NotNil(t, head1)
	require.Equal(t, uint64(1), *head1.ID)

	// Revert restores balances.
	reversal, err := store.RevertTransaction(ctx, *tx.ID, revertAt)
	require.NoError(t, err)
	require.NotNil(t, reversal.ID)
	require.Equal(t, uint64(2), *reversal.ID)

	requireBalance(t, ctx, store, "alice", "USD", 0)
	requireBalance(t, ctx, store, "bob", "USD", 0)

	// Original is marked reverted.
	original, err := store.getTransaction(ctx, *tx.ID)
	require.NoError(t, err)
	require.NotNil(t, original.RevertedAt)
	require.False(t, original.RevertedAt.IsZero())

	// The chain now spans NEW_TRANSACTION -> REVERTED_TRANSACTION and stays
	// consistent (the revert log is chained onto the first).
	require.NoError(t, store.VerifyHashChain(ctx))
	head2, err := store.lastLog(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), *head2.ID)
	require.NotEqual(t, head1.Hash, head2.Hash)

	// A second revert of the same transaction is refused.
	_, err = store.RevertTransaction(ctx, *tx.ID, revertAt)
	require.Error(t, err)

	return head2.Hash
}

func requireBalance(t *testing.T, ctx context.Context, store *Store, account, asset string, want int64) {
	t.Helper()
	bal, err := store.GetBalance(ctx, account, asset)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(want).String(), bal.String(),
		"balance of %s/%s", account, asset)
}
