package dialect

import (
	"context"
	"time"

	"github.com/spf13/pflag"
	"github.com/uptrace/bun"
	"go.uber.org/fx"

	logging "github.com/hanzo-fi/go-libs/v5/pkg/observe/log"
	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/debug"
)

const (
	DSNFlag             = "dsn"
	MaxOpenConnsFlag    = "max-open-conns"
	MaxIdleConnsFlag    = "max-idle-conns"
	ConnMaxIdleTimeFlag = "conn-max-idle-time"
	ConnMaxLifetimeFlag = "conn-max-lifetime"
)

// AddFlags registers the one flag that says where the ledger is stored. Left
// unset it is a SQLite store under ./ledger.store, so local development needs
// no database and no configuration.
func AddFlags(flags *pflag.FlagSet) {
	flags.String(DSNFlag, "", "Where the ledger is stored. Empty or a path is SQLite; sql://… is hanzoai/sql")
	flags.Int(MaxOpenConnsFlag, 20, "Max open connections (sql:// only; SQLite takes one writer)")
	flags.Int(MaxIdleConnsFlag, 0, "Max idle connections")
	flags.Duration(ConnMaxIdleTimeFlag, time.Minute, "Max idle time for a connection")
	flags.Duration(ConnMaxLifetimeFlag, 0, "Max lifetime of a connection")
}

// ConfigFromFlags reads the storage configuration off a command.
func ConfigFromFlags(flags *pflag.FlagSet) Config {
	dsn, _ := flags.GetString(DSNFlag)
	maxOpenConns, _ := flags.GetInt(MaxOpenConnsFlag)
	maxIdleConns, _ := flags.GetInt(MaxIdleConnsFlag)
	connMaxIdleTime, _ := flags.GetDuration(ConnMaxIdleTimeFlag)
	connMaxLifetime, _ := flags.GetDuration(ConnMaxLifetimeFlag)

	return Config{
		DSN:             dsn,
		MaxOpenConns:    maxOpenConns,
		MaxIdleConns:    maxIdleConns,
		ConnMaxIdleTime: connMaxIdleTime,
		ConnMaxLifetime: connMaxLifetime,
	}
}

// NewFXModule opens the store and publishes both the connection and the engine
// it speaks to.
func NewFXModule(config Config, debugMode bool) fx.Option {
	return fx.Options(
		fx.Provide(func(logger logging.Logger) (*bun.DB, Dialect, error) {
			hook := debug.NewQueryHook()
			hook.Debug = debugMode

			db, d, err := Open(
				logging.ContextWithLogger(context.Background(), logger),
				config,
				hook,
			)
			if err != nil {
				return nil, nil, err
			}
			logger.WithFields(map[string]any{"engine": d.Name()}).Info("store opened")

			return db, d, nil
		}),
		fx.Invoke(func(lc fx.Lifecycle, db *bun.DB) {
			lc.Append(fx.Hook{
				OnStop: func(context.Context) error { return db.Close() },
			})
		}),
	)
}
