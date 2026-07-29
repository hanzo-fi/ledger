package ledger_test

import (
	"context"
	"fmt"
	"math/big"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/hanzo-fi/go-libs/v5/pkg/types/time"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/storage/bucket"
	"github.com/hanzo-fi/ledger/internal/storage/common"
	"github.com/hanzo-fi/ledger/internal/storage/dialect"
	"github.com/hanzo-fi/ledger/internal/storage/driver"
	ledgerstore "github.com/hanzo-fi/ledger/internal/storage/ledger"
	systemstore "github.com/hanzo-fi/ledger/internal/storage/system"
)

// openSQLite brings up a whole ledger on a temporary SQLite store: no server,
// no container, no configuration.
func openSQLite(t *testing.T) (*driver.Driver, dialect.Dialect) {
	t.Helper()

	ctx := context.Background()
	db, d, err := dialect.Open(ctx, dialect.Config{DSN: t.TempDir()})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	sqlite, ok := d.(*dialect.SQLite)
	require.True(t, ok)

	buckets := bucket.NewSQLiteFactory(sqlite)
	ret := driver.New(
		db,
		d,
		ledgerstore.NewFactory(db, d),
		buckets,
		systemstore.NewStoreFactory(d),
	)
	require.NoError(t, ret.Initialize(ctx))

	return ret, d
}

func newSQLiteLedger(t *testing.T) *ledgerstore.Store {
	t.Helper()

	d, _ := openSQLite(t)
	l, err := ledger.New(uuid.NewString()[:8], ledger.NewDefaultConfiguration())
	require.NoError(t, err)

	store, err := d.CreateLedger(context.Background(), l)
	require.NoError(t, err)

	return store
}

func transfer(t *testing.T, source, destination, asset string, amount int64, at time.Time) *ledger.Transaction {
	t.Helper()

	tx := ledger.NewTransaction().
		WithPostings(ledger.NewPosting(source, destination, asset, big.NewInt(amount))).
		WithTimestamp(at)

	return &tx
}

func TestSQLiteCommitsBalancedTransaction(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteLedger(t)
	now := time.Now()

	tx := transfer(t, "world", "users:001", "USD/2", 100, now)
	require.NoError(t, store.CommitTransaction(ctx, tx))

	require.NotNil(t, tx.ID)
	require.Equal(t, uint64(1), *tx.ID)

	// Every posting lands on both sides.
	balances, err := store.GetBalances(ctx, ledgerstore.BalanceQuery{
		"world":     []string{"USD/2"},
		"users:001": []string{"USD/2"},
	})
	require.NoError(t, err)
	require.Equal(t, big.NewInt(-100), balances["world"]["USD/2"])
	require.Equal(t, big.NewInt(100), balances["users:001"]["USD/2"])

	// The two sides sum to zero: the double entry invariant.
	total := new(big.Int)
	for _, assets := range balances {
		for _, balance := range assets {
			total.Add(total, balance)
		}
	}
	require.Zero(t, total.Sign(), "the postings of a transaction must sum to zero")
}

func TestSQLiteRunsTransactionIDsDense(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteLedger(t)
	now := time.Now()

	for i := uint64(1); i <= 5; i++ {
		tx := transfer(t, "world", "users:001", "USD/2", 10, now)
		require.NoError(t, store.CommitTransaction(ctx, tx))
		require.Equal(t, i, *tx.ID, "transaction ids must be dense per ledger")
	}
}

func TestSQLiteChainsLogs(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteLedger(t)
	now := time.Now()

	var previous *ledger.Log
	for i := 0; i < 3; i++ {
		tx := transfer(t, "world", "users:001", "USD/2", 10, now)
		require.NoError(t, store.CommitTransaction(ctx, tx))

		log := ledger.NewLog(ledger.CreatedTransaction{Transaction: *tx}).WithDate(now)
		require.NoError(t, store.InsertLog(ctx, &log))

		// The stored hash is the one the verifier computes over the chain.
		expected := ledger.NewLog(ledger.CreatedTransaction{Transaction: *tx}).WithDate(now)
		expected.ComputeHash(previous)
		require.Equal(t, expected.Hash, log.Hash, "log %d is not chained onto its predecessor", i)
		require.NotEmpty(t, log.Hash)

		previous = &log
	}
}

func TestSQLiteHoldsEffectiveVolumes(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteLedger(t)
	now := time.Now()

	later := transfer(t, "world", "users:001", "USD/2", 100, now)
	require.NoError(t, store.CommitTransaction(ctx, later))
	require.Equal(t,
		ledger.NewVolumesInt64(100, 0),
		later.PostCommitEffectiveVolumes["users:001"]["USD/2"],
	)

	// A backdated transaction stands before the one already recorded, and the
	// later one shifts by its delta.
	earlier := transfer(t, "world", "users:001", "USD/2", 50, now.Add(-time.Hour))
	require.NoError(t, store.CommitTransaction(ctx, earlier))
	require.Equal(t,
		ledger.NewVolumesInt64(50, 0),
		earlier.PostCommitEffectiveVolumes["users:001"]["USD/2"],
	)

	volumes, err := store.GetBalances(ctx, ledgerstore.BalanceQuery{"users:001": []string{"USD/2"}})
	require.NoError(t, err)
	require.Equal(t, big.NewInt(150), volumes["users:001"]["USD/2"])
}

func TestSQLiteKeepsAmountsExact(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteLedger(t)
	now := time.Now()

	// Wider than the engine's integers: the ledger must not round it.
	amount, ok := new(big.Int).SetString("170141183460469231731687303715884105727", 10)
	require.True(t, ok)

	tx := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "users:001", "USD/2", amount)).
		WithTimestamp(now)
	require.NoError(t, store.CommitTransaction(ctx, &tx))

	balances, err := store.GetBalances(ctx, ledgerstore.BalanceQuery{"users:001": []string{"USD/2"}})
	require.NoError(t, err)
	require.Equal(t, amount, balances["users:001"]["USD/2"])
}

// pairs states volumes the way a pair is written, so an assertion compares the
// digits carried rather than the shape a big.Int happens to hold them in.
func pairs(volumes ledger.VolumesByAssets) map[string]string {
	ret := map[string]string{}
	for asset, v := range volumes {
		ret[asset] = fmt.Sprintf("(%s, %s)", v.Input, v.Output)
	}
	return ret
}

// totalsExactly holds a store to reporting volumes wider than an engine's
// integers as they were written: read one at a time, and read totalled.
//
// It is one body on either engine, because the reads are the same on either
// engine: a pair travels as the digits it was written with, and the total is
// Go's.
func totalsExactly(t *testing.T, store *ledgerstore.Store) {
	t.Helper()

	ctx := context.Background()
	now := time.Now()

	// Two amounts, each already wider than 2^63-1, whose total is 2^127-1.
	first := new(big.Int).Lsh(big.NewInt(1), 126)
	second := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 126), big.NewInt(1))
	total := new(big.Int).Add(first, second)
	require.Equal(t, "170141183460469231731687303715884105727", total.String())

	one := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "users:001", "USD/2", first)).
		WithTimestamp(now)
	require.NoError(t, store.CommitTransaction(ctx, &one))
	require.NoError(t, store.UpsertAccounts(ctx, one.AccountsWithDefaultMetadata(nil, nil)...))

	// One amount, standing alone: a total is what was written, not what an
	// engine integer can hold.
	aggregated, err := store.AggregatedVolumes().GetOne(ctx, common.ResourceQuery[ledger.GetAggregatedVolumesOptions]{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"USD/2": fmt.Sprintf("(%s, %s)", first, first),
	}, pairs(aggregated.Aggregated))

	// The same amount read one account at a time.
	accounts, err := store.Accounts().Paginate(ctx, common.InitialPaginatedQuery[any]{
		Options: common.ResourceQuery[any]{Expand: []string{"volumes"}},
	})
	require.NoError(t, err)
	held := map[string]map[string]string{}
	for _, account := range accounts.Data {
		held[account.Address] = pairs(account.Volumes)
	}
	require.Equal(t, map[string]map[string]string{
		"world":     {"USD/2": fmt.Sprintf("(0, %s)", first)},
		"users:001": {"USD/2": fmt.Sprintf("(%s, 0)", first)},
	}, held)

	other := ledger.NewTransaction().
		WithPostings(ledger.NewPosting("world", "users:002", "USD/2", second)).
		WithTimestamp(now)
	require.NoError(t, store.CommitTransaction(ctx, &other))
	require.NoError(t, store.UpsertAccounts(ctx, other.AccountsWithDefaultMetadata(nil, nil)...))

	// Two of them added: world paid both out, the two accounts took them in.
	aggregated, err = store.AggregatedVolumes().GetOne(ctx, common.ResourceQuery[ledger.GetAggregatedVolumesOptions]{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"USD/2": fmt.Sprintf("(%s, %s)", total, total),
	}, pairs(aggregated.Aggregated))
}

func TestSQLiteTotalsAmountsExact(t *testing.T) {
	totalsExactly(t, newSQLiteLedger(t))
}

func TestSQLiteRejectsDuplicateIdempotencyKey(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteLedger(t)
	now := time.Now()

	tx := transfer(t, "world", "users:001", "USD/2", 10, now)
	require.NoError(t, store.CommitTransaction(ctx, tx))

	first := ledger.NewLog(ledger.CreatedTransaction{Transaction: *tx}).
		WithDate(now).
		WithIdempotencyKey("once")
	require.NoError(t, store.InsertLog(ctx, &first))

	second := ledger.NewLog(ledger.CreatedTransaction{Transaction: *tx}).
		WithDate(now).
		WithIdempotencyKey("once")
	require.Error(t, store.InsertLog(ctx, &second))

	read, err := store.ReadLogWithIdempotencyKey(ctx, "once")
	require.NoError(t, err)
	require.Equal(t, first.Hash, read.Hash)
}

func TestSQLiteUpsertsAccounts(t *testing.T) {
	ctx := context.Background()
	store := newSQLiteLedger(t)
	now := time.Now()

	tx := transfer(t, "world", "users:001", "USD/2", 10, now)
	require.NoError(t, store.CommitTransaction(ctx, tx))

	accounts := tx.AccountsWithDefaultMetadata(nil, nil)
	require.NoError(t, store.UpsertAccounts(ctx, accounts...))
	// Upserting the same accounts again must settle, not stack a second write
	// on the first.
	require.NoError(t, store.UpsertAccounts(ctx, accounts...))

	second := transfer(t, "world", "users:002", "USD/2", 20, now)
	require.NoError(t, store.CommitTransaction(ctx, second))
	require.NoError(t, store.UpsertAccounts(ctx, second.AccountsWithDefaultMetadata(nil, nil)...))

	balances, err := store.GetBalances(ctx, ledgerstore.BalanceQuery{
		"users:001": []string{"USD/2"},
		"users:002": []string{"USD/2"},
		"world":     []string{"USD/2"},
	})
	require.NoError(t, err)
	require.Equal(t, big.NewInt(10), balances["users:001"]["USD/2"])
	require.Equal(t, big.NewInt(20), balances["users:002"]["USD/2"])
	require.Equal(t, big.NewInt(-30), balances["world"]["USD/2"])
}
