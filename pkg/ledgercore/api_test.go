package ledgercore

import (
	"context"
	"database/sql"
	"math/big"
	"os/exec"
	"testing"
	stdtime "time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/docker"
	"github.com/formancehq/go-libs/v5/pkg/testing/platform/pgtesting"
)

// TestPostIdempotencyAndReads exercises the caller-facing engine surface — the
// idempotent Post, the idempotency-key lookup, the prefix/rollup balances, the
// journal listing, and the WithTx read-then-write guard — over the SAME
// dialect-agnostic Go on BOTH SQLite and Postgres, and asserts the resulting
// hash-chain head is byte-identical across dialects. This is the parity gate for
// the idempotency-key item on the honest-remaining list: it proves at-most-once
// posting and the atomic overdraw guard hold identically on both backends.
func TestPostIdempotencyAndReads(t *testing.T) {
	const cents = "cents"
	base := stdtime.Date(2026, 7, 5, 12, 0, 0, 0, stdtime.UTC)

	// scenario runs the full flow against db and returns the hash-chain head.
	scenario := func(t *testing.T, db bun.IDB) []byte {
		ctx := context.Background()
		require.NoError(t, Migrate(ctx, db))
		s := New(db, "acme")

		// 1) First accrual under key "k1": revenue -> reserve, 500.
		rec, err := s.Post(ctx, []Posting{{Source: "revenue:platform", Destination: "fund:reserve", Asset: cents, Amount: big.NewInt(500)}},
			PostParams{IdempotencyKey: "k1", Reference: "accrual:2026-07", Metadata: map[string]string{"kind": "accrual"}, Timestamp: base})
		require.NoError(t, err)
		require.False(t, rec.Deduped)
		require.Equal(t, uint64(1), rec.TransactionID)
		requireBal(t, ctx, s, "fund:reserve", cents, 500)
		requireBal(t, ctx, s, "revenue:platform", cents, -500)

		// 2) Replaying key "k1" (even with different postings) is at-most-once: it
		//    returns the ORIGINAL record, writes nothing, moves no balance.
		replay, err := s.Post(ctx, []Posting{{Source: "revenue:platform", Destination: "fund:reserve", Asset: cents, Amount: big.NewInt(999)}},
			PostParams{IdempotencyKey: "k1", Timestamp: base})
		require.NoError(t, err)
		require.True(t, replay.Deduped)
		require.Equal(t, uint64(1), replay.TransactionID)
		require.Equal(t, "accrual:2026-07", replay.Reference)
		requireBal(t, ctx, s, "fund:reserve", cents, 500) // unchanged — no double post

		// 3) Idempotency lookup reconstructs the record (metadata round-trips).
		got, ok, err := s.PostByIdempotencyKey(ctx, "k1")
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, uint64(1), got.TransactionID)
		require.Equal(t, "accrual", got.Metadata["kind"])
		_, ok, err = s.PostByIdempotencyKey(ctx, "absent")
		require.NoError(t, err)
		require.False(t, ok)

		// 4) A backed payout under key "k2": reserve -> payout:referral, 200.
		_, err = s.Post(ctx, []Posting{{Source: "fund:reserve", Destination: "payout:referral", Asset: cents, Amount: big.NewInt(200)}},
			PostParams{IdempotencyKey: "k2", Timestamp: base})
		require.NoError(t, err)
		requireBal(t, ctx, s, "fund:reserve", cents, 300)

		// 5) Prefix rollup sees only the payout namespace.
		payouts, err := s.PrefixBalances(ctx, "payout:", cents)
		require.NoError(t, err)
		require.Len(t, payouts, 1)
		require.Equal(t, "200", payouts["payout:referral"].String())

		// 6) The journal lists newest-first.
		posts, err := s.ListPosts(ctx, 0)
		require.NoError(t, err)
		require.Len(t, posts, 2)
		require.Equal(t, uint64(2), posts[0].TransactionID)
		require.Equal(t, uint64(1), posts[1].TransactionID)

		// 7) WithTx overdraw guard: a read-then-write that would drive the reserve
		//    negative and returns an error rolls the whole tx back — nothing posts.
		err = s.WithTx(ctx, func(tx *Store) error {
			bal, berr := tx.GetBalance(ctx, "fund:reserve", cents)
			require.NoError(t, berr)
			require.Equal(t, "300", bal.String())
			if _, perr := tx.Post(ctx, []Posting{{Source: "fund:reserve", Destination: "payout:affiliate", Asset: cents, Amount: big.NewInt(1000)}},
				PostParams{IdempotencyKey: "k3", Timestamp: base}); perr != nil {
				return perr
			}
			return context.Canceled // reject: abort the guarded post
		})
		require.ErrorIs(t, err, context.Canceled)
		requireBal(t, ctx, s, "fund:reserve", cents, 300)   // rolled back
		requireBal(t, ctx, s, "payout:affiliate", cents, 0) // never posted
		_, ok, err = s.PostByIdempotencyKey(ctx, "k3")      // and no k3 log survived
		require.NoError(t, err)
		require.False(t, ok)

		// 8) A committed WithTx post persists.
		err = s.WithTx(ctx, func(tx *Store) error {
			_, perr := tx.Post(ctx, []Posting{{Source: "fund:reserve", Destination: "payout:affiliate", Asset: cents, Amount: big.NewInt(100)}},
				PostParams{IdempotencyKey: "k4", Timestamp: base})
			return perr
		})
		require.NoError(t, err)
		requireBal(t, ctx, s, "fund:reserve", cents, 200)
		requireBal(t, ctx, s, "payout:affiliate", cents, 100)

		// 9) The whole chain verifies.
		require.NoError(t, s.VerifyHashChain(ctx))
		head, err := s.lastLog(ctx)
		require.NoError(t, err)
		require.NotNil(t, head)
		return head.Hash
	}

	heads := map[string][]byte{}
	t.Run("sqlite", func(t *testing.T) {
		db, err := openLedgerFile(t.TempDir(), "acme")
		require.NoError(t, err)
		t.Cleanup(func() { _ = db.Close() })
		heads["sqlite"] = scenario(t, db)
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
		heads["postgres"] = scenario(t, db)
	})

	if len(heads["sqlite"]) > 0 && len(heads["postgres"]) > 0 {
		require.Equal(t, heads["sqlite"], heads["postgres"],
			"idempotency-key hash-chain head must be byte-identical across dialects")
		t.Logf("dialect-agnostic idempotency: chain head byte-identical: sqlite=%x postgres=%x",
			heads["sqlite"], heads["postgres"])
	}
}

func requireBal(t *testing.T, ctx context.Context, s *Store, account, asset string, want int64) {
	t.Helper()
	bal, err := s.GetBalance(ctx, account, asset)
	require.NoError(t, err)
	require.Equal(t, big.NewInt(want).String(), bal.String(), "balance of %s/%s", account, asset)
}
