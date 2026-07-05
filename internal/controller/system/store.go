package system

import (
	"context"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	ledger "github.com/hanzo-fi/ledger/internal"
	ledgercontroller "github.com/hanzo-fi/ledger/internal/controller/ledger"
	"github.com/hanzo-fi/ledger/internal/storage/common"
	"github.com/hanzo-fi/ledger/internal/storage/system"
)

type Store interface {
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	Ledgers() common.PaginatedResource[ledger.Ledger, system.ListLedgersQueryPayload]
	UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error
	DeleteLedgerMetadata(ctx context.Context, param string, key string) error
	DeleteBucket(ctx context.Context, bucket string) error
	RestoreBucket(ctx context.Context, bucket string) error
}

type Driver interface {
	OpenLedger(context.Context, string) (ledgercontroller.Store, *ledger.Ledger, error)
	CreateLedger(context.Context, *ledger.Ledger) error
	GetSystemStore() Store
}
