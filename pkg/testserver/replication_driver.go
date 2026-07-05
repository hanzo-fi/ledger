package testserver

import (
	"context"

	"github.com/hanzo-fi/ledger/internal/replication/drivers"
)

type Driver interface {
	Config() map[string]any
	Name() string
	ReadMessages(ctx context.Context) ([]drivers.LogWithLedger, error)
	Clear(ctx context.Context) error
}
