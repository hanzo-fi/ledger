//go:build it

package bucket_test

import (
	"testing"

	logging "github.com/hanzo-fi/go-libs/v5/pkg/observe/log"
	"github.com/hanzo-fi/go-libs/v5/pkg/testing/docker"
	"github.com/hanzo-fi/go-libs/v5/pkg/testing/platform/pgtesting"
	. "github.com/hanzo-fi/go-libs/v5/pkg/testing/utils"
)

var (
	srv *pgtesting.PostgresServer
)

func TestMain(m *testing.M) {
	WithTestMain(func(t *TestingTForMain) int {
		srv = pgtesting.CreatePostgresServer(t, docker.NewPool(t, logging.Testing()))

		return m.Run()
	})
}
