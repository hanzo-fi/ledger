//go:build it

package cmd

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/hanzo-fi/go-libs/v5/pkg/observe/log"
	"github.com/hanzo-fi/go-libs/v5/pkg/testing/docker"
	"github.com/hanzo-fi/go-libs/v5/pkg/testing/platform/pgtesting"

	"github.com/hanzo-fi/ledger/internal/storage/dialect"
)

func TestBucketsUpgrade(t *testing.T) {
	t.Parallel()

	dockerPool := docker.NewPool(t, logging.Testing())
	srv := pgtesting.CreatePostgresServer(t, dockerPool)
	ctx := logging.TestingContext()

	type testCase struct {
		name string
		args []string
	}

	for _, tc := range []testCase{
		{
			name: "nominal",
			args: []string{"test"},
		},
		{
			name: "upgrade all",
			args: []string{"*"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := srv.NewDatabase(t)

			args := []string{
				"--" + dialect.DSNFlag, dialect.DSN(db.ConnString()),
			}
			args = append(args, tc.args...)

			cmd := NewBucketUpgrade()
			cmd.SetOut(io.Discard)
			cmd.SetArgs(args)
			require.NoError(t, cmd.ExecuteContext(ctx))
		})
	}
}
