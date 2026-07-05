//go:build it

package env

import (
	"context"
	"testing"

	ledgerclient "github.com/hanzo-fi/ledger/pkg/client"
)

type Env interface {
	Client() *ledgerclient.Formance
	Stop(ctx context.Context) error
}

type EnvFactory interface {
	Create(ctx context.Context, b *testing.B) Env
}

var FallbackEnvFactory EnvFactory = nil
