package dialect_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	"github.com/hanzo-fi/ledger/internal/storage/dialect"
)

func TestDSNSelectsEngine(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		dsn  string
		sql  bool
		dir  string
		wire string
	}{
		{dsn: "", dir: dialect.Store},
		{dsn: "sqlite://data", dir: "data"},
		{dsn: "sqlite:///var/lib/ledger", dir: "/var/lib/ledger"},
		{dsn: "sqlite://data/ledger.db", dir: "data"},
		{dsn: "file:/tmp/x/main.db?cache=shared", dir: "/tmp/x"},
		{dsn: "/var/lib/ledger", dir: "/var/lib/ledger"},
		{dsn: "sql://user:pass@host:5432/ledger", sql: true, wire: "postgres://user:pass@host:5432/ledger"},
		{dsn: "sql://host/ledger", sql: true, wire: "postgres://host/ledger"},
		// The engine is named by one scheme only. Postgres' own spelling is
		// not a second way to ask for it - it is a path, and so SQLite.
		{dsn: "postgres://user:pass@host:5432/ledger", dir: "postgres://user:pass@host:5432/ledger"},
		{dsn: "postgresql://host/ledger", dir: "postgresql://host/ledger"},
	} {
		t.Run(tc.dsn, func(t *testing.T) {
			require.Equal(t, tc.sql, dialect.IsSQL(tc.dsn))
			if tc.sql {
				require.Equal(t, tc.wire, dialect.Wire(tc.dsn))
			} else {
				require.Equal(t, tc.dir, dialect.Dir(tc.dsn))
			}
		})
	}
}

func TestDefaultIsSQLite(t *testing.T) {
	t.Parallel()

	db, d, err := dialect.Open(context.Background(), dialect.Config{DSN: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.Equal(t, "sqlite", d.Name())
}

// A bucket is an attached file and an attachment belongs to a connection, so
// every connection the pool hands out has to resolve every known bucket.
func TestEveryConnectionResolvesABucket(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, d, err := dialect.Open(ctx, dialect.Config{DSN: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, d.OpenBucket(ctx, db, "_default"))
	_, err = db.ExecContext(ctx, `create table "_default".probe (id integer primary key)`)
	require.NoError(t, err)

	// Drop the pooled connection, so the next queries run on a fresh one.
	db.DB.SetMaxIdleConns(0)
	db.DB.SetMaxIdleConns(1)

	for i := 0; i < 8; i++ {
		_, err = db.ExecContext(ctx, `insert into "_default".probe (id) values (?)`, i)
		require.NoError(t, err, "a replacement connection lost the bucket")
	}

	count, err := db.NewSelect().TableExpr(`"_default".probe`).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 8, count)
}

func TestBucketIsAttached(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, d, err := dialect.Open(ctx, dialect.Config{DSN: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.NoError(t, d.OpenBucket(ctx, db, "_default"))
	// Attaching twice is idempotent.
	require.NoError(t, d.OpenBucket(ctx, db, "_default"))

	_, err = db.ExecContext(ctx, `create table "_default".probe (id integer primary key)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `insert into "_default".probe (id) values (1)`)
	require.NoError(t, err)

	require.NoError(t, d.DropBucket(ctx, db, "_default"))
	_, err = db.ExecContext(ctx, `select 1 from "_default".probe`)
	require.Error(t, err)
}

func TestObjectVocabulary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, d, err := dialect.Open(ctx, dialect.Config{DSN: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(ctx, `create table t (metadata text, sources text, segments text)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `insert into t values ('{"a":"1","b":"2"}', '["users:001","world"]', '[{"0":"users","1":"001","2":null}]')`)
	require.NoError(t, err)

	holds := d.Holds("metadata", map[string]any{"a": "1"})
	count, err := db.NewSelect().Table("t").Where(holds.SQL, holds.Args...).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	holds = d.Holds("metadata", map[string]any{"a": "9"})
	count, err = db.NewSelect().Table("t").Where(holds.SQL, holds.Args...).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	has := d.Has("metadata", "b")
	count, err = db.NewSelect().Table("t").Where(has.SQL, has.Args...).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	arr := d.ArrayHolds("sources", "world")
	count, err = db.NewSelect().Table("t").Where(arr.SQL, arr.Args...).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	meets := d.ArrayMeets("sources", []string{"nope", "users:001"})
	count, err = db.NewSelect().Table("t").Where(meets.SQL, meets.Args...).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	seg := d.SegmentsMatch("segments", map[string]any{"0": "users", "2": nil})
	count, err = db.NewSelect().Table("t").Where(seg.SQL, seg.Args...).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, count)

	seg = d.SegmentsMatch("segments", map[string]any{"0": "orders"})
	count, err = db.NewSelect().Table("t").Where(seg.SQL, seg.Args...).Count(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, count)

	_, err = db.NewUpdate().Table("t").Set("metadata = "+d.Merge("metadata", "?"), map[string]string{"c": "3"}).Where("1 = 1").Exec(ctx)
	require.NoError(t, err)
	var metadata string
	require.NoError(t, db.QueryRowContext(ctx, `select metadata from t`).Scan(&metadata))
	require.JSONEq(t, `{"a":"1","b":"2","c":"3"}`, metadata)

	remove := d.Remove("metadata", "a")
	_, err = db.NewUpdate().Table("t").Set("metadata = "+remove.SQL, remove.Args...).Where("1 = 1").Exec(ctx)
	require.NoError(t, err)
	require.NoError(t, db.QueryRowContext(ctx, `select metadata from t`).Scan(&metadata))
	require.JSONEq(t, `{"b":"2","c":"3"}`, metadata)
}

func TestConstraintIsNamed(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, d, err := dialect.Open(ctx, dialect.Config{DSN: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(ctx, `create table logs (ledger text, idempotency_key text)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `create unique index logs_idempotency_key on logs (ledger, idempotency_key)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `insert into logs values ('a', 'k')`)
	require.NoError(t, err)

	_, err = db.ExecContext(ctx, `insert into logs values ('a', 'k')`)
	resolved := d.ResolveError(err)
	require.ErrorIs(t, resolved, dialect.ErrConstraint{})
	require.Equal(t, "logs_idempotency_key", dialect.Constraint(resolved))
}

// A write transaction on this engine excludes every other writer, which is
// what the per-ledger lock exists to guarantee. Taking it is therefore free,
// and taking it twice must not wedge.
func TestLedgerLockIsFree(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, d, err := dialect.Open(ctx, dialect.Config{DSN: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	held, release, err := d.LockLedger(ctx, db, 1)
	require.NoError(t, err)
	require.NotNil(t, held)

	taken := make(chan struct{})
	go func() {
		defer close(taken)
		_, inner, err := d.LockLedger(ctx, db, 1)
		require.NoError(t, err)
		require.NoError(t, inner())
	}()

	select {
	case <-taken:
	case <-time.After(2 * time.Second):
		t.Fatal("taking the ledger lock a second time blocked")
	}

	require.NoError(t, release())
}

// One writer is what keeps a ledger whole: concurrent transactions all land,
// none writes over another.
func TestOneWriterKeepsEveryWrite(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db, _, err := dialect.Open(ctx, dialect.Config{DSN: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	_, err = db.ExecContext(ctx, `create table balances (account text primary key, amount integer)`)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `insert into balances values ('a', 0)`)
	require.NoError(t, err)

	// Two writers racing on the same row must leave the sum they agreed on.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `update balances set amount = amount + 10 where account = 'a'`)
				return err
			}))
		}()
	}
	wg.Wait()

	var amount int
	require.NoError(t, db.QueryRowContext(ctx, `select amount from balances`).Scan(&amount))
	require.Equal(t, 80, amount, "a concurrent write was lost")
}
