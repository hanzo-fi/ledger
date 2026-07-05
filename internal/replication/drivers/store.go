package drivers

import (
	"context"

	ledger "github.com/hanzo-fi/ledger/internal"
)

//go:generate mockgen -source store.go -destination store_generated.go -package drivers . Store
type Store interface {
	GetExporter(ctx context.Context, id string) (*ledger.Exporter, error)
}
