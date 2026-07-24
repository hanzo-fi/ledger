//go:build it

package bucket_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"

	logging "github.com/hanzo-fi/go-libs/v5/pkg/observe/log"
	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/connect"
	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/debug"

	"github.com/hanzo-fi/ledger/internal/storage/bucket"
	"github.com/hanzo-fi/ledger/internal/storage/system"
)

func TestBuckets(t *testing.T) {
	ctx := logging.TestingContext()
	name := uuid.NewString()[:8]

	pgDatabase := srv.NewDatabase(t)
	db, err := connect.OpenSQLDB(ctx, pgDatabase.ConnectionOptions())
	require.NoError(t, err)

	if testing.Verbose() {
		db.AddQueryHook(debug.NewQueryHook())
	}

	require.NoError(t, system.Migrate(ctx, db))

	b := bucket.NewDefault(noop.Tracer{}, name)
	require.NoError(t, b.Migrate(ctx, db))
}
