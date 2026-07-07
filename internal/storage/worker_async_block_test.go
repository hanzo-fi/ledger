//go:build it

package storage

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/debug"
	"github.com/formancehq/go-libs/v5/pkg/testing/docker"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/storage/bucket"
	"github.com/hanzo-fi/ledger/internal/storage/driver"
	ledgerstore "github.com/hanzo-fi/ledger/internal/storage/ledger"
	systemstore "github.com/hanzo-fi/ledger/internal/storage/system"
	"github.com/hanzo-fi/ledger/pkg/features"
)

// newAsyncBlockLedger builds a fresh Postgres-backed ledger store with the async
// log-hash feature enabled, returning the store, its db and the ledger.
func newAsyncBlockLedger(t docker.T) (*ledgerstore.Store, *bun.DB, ledger.Ledger) {
	t.Helper()

	ctx := logging.TestingContext()
	pgDatabase := workerTestSrv.NewDatabase(t)

	debugHook := debug.NewQueryHook()
	db, err := connect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions(), debugHook)
	require.NoError(t, err)

	require.NoError(t, systemstore.New(db).Migrate(ctx))

	d := driver.New(
		db,
		ledgerstore.NewFactory(db),
		bucket.NewDefaultFactory(),
		systemstore.NewStoreFactory(),
	)

	l, err := ledger.New("blocks", ledger.Configuration{
		Bucket:   ledger.DefaultBucket,
		Features: features.MinimalFeatureSet.With(features.FeatureHashLogs, "ASYNC"),
	})
	require.NoError(t, err)

	store, err := d.CreateLedger(ctx, l)
	require.NoError(t, err)

	return store, db, *l
}

func TestAsyncBlockRunnerHash(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	store, db, l := newAsyncBlockLedger(t)

	// Insert a handful of logs (>2 full blocks + a partial one) so we exercise the
	// bootstrap block, chained blocks and the final short block.
	const (
		nbLogs    = 7
		blockSize = 3
	)
	for i := 0; i < nbLogs; i++ {
		log := ledger.NewLog(ledger.CreatedTransaction{
			Transaction: ledger.NewTransaction().
				WithPostings(ledger.NewPosting("world", "bank", "USD", big.NewInt(100))).
				WithMetadata(metadata.Metadata{"idx": fmt.Sprint(i), "utf8": "½\\'>"}),
			AccountMetadata: ledger.AccountMetadata{},
		})
		require.NoError(t, store.InsertLog(ctx, &log))
	}

	runner := NewAsyncBlockRunner(logging.Testing(), db, AsyncBlockRunnerConfig{MaxBlockSize: blockSize})
	require.NoError(t, runner.processLedger(ctx, l))

	// Read every block in chain order (previous asc == from_id asc).
	type blockRow struct {
		Previous int64  `bun:"previous"`
		FromID   int64  `bun:"from_id"`
		ToID     int64  `bun:"to_id"`
		Hash     []byte `bun:"hash"`
	}
	blocks := make([]blockRow, 0)
	require.NoError(t, db.NewSelect().
		ColumnExpr("previous").
		ColumnExpr("from_id").
		ColumnExpr("to_id").
		ColumnExpr("hash").
		TableExpr("?.logs_blocks", bun.Ident(l.Bucket)).
		Where("ledger = ?", l.Name).
		OrderExpr("previous asc").
		Scan(ctx, &blocks))

	// 7 logs, block size 3 => 3 blocks: [1..3], [4..6], [7..7].
	require.Len(t, blocks, 3)

	var previousHash []byte
	for i, block := range blocks {
		require.NotEmpty(t, block.Hash, "block %d hash", i)

		// Reference hash: the EXACT retired create_block expression evaluated in SQL
		// (pgcrypto digest over coalesce(prev.hash,'') || string_agg(part,'')).
		// Byte-equality proves the Go crypto/sha256 port is byte-identical to the
		// plpgsql it replaced.
		var reference []byte
		err := db.NewRaw(
			`select public.digest(coalesce(?::bytea, '') || string_agg( `+
				`type || encode(memento, 'escape') || (to_json(date::timestamp)#>>'{}') || `+
				`coalesce(idempotency_key, '') || id, ''), 'sha256'::text) `+
				`from (select * from ?.logs where id > ? and ledger = ? order by id limit ?) logs`,
			previousHash, bun.Ident(l.Bucket), block.FromID, l.Name, blockSize,
		).Scan(ctx, &reference)
		require.NoError(t, err)
		require.Equal(t, reference, block.Hash, "block %d hash must match the plpgsql reference", i)

		previousHash = block.Hash
	}
}
