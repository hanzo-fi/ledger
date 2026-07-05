package bus

import (
	"go.uber.org/fx"

	"github.com/hanzo-fi/ledger/internal/controller/ledger"
)

func NewFxModule() fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotate(NewLedgerListener, fx.As(new(ledger.Listener)))),
	)
}
