package system

import (
	"github.com/uptrace/bun"
	"go.uber.org/fx"

	"github.com/hanzo-fi/ledger/internal/storage/dialect"
)

func NewFXModule() fx.Option {
	return fx.Options(
		fx.Provide(func(db *bun.DB, d dialect.Dialect) *DefaultStore {
			return New(db, d)
		}),
	)
}
