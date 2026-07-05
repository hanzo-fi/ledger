// Package bunconnect is the single seam that decides which SQL dialect + driver
// the ledger's *bun.DB is built on. It is the "one place" the pgdialect/pgx
// wiring (which lives in go-libs) is chosen versus a pure-Go SQLite backend.
//
//   - postgres (opt-in): delegates verbatim to go-libs storagefx.BunConnectModule,
//     preserving the shared/scale behavior. No query behavior changes.
//   - sqlite (default): builds a *bun.DB on the pure-Go modernc.org/sqlite driver
//     with sqlitedialect. CGO-free, so it still builds under CGO_ENABLED=0.
//     Intended for embedded, per-tenant Hanzo Base files (single-writer, zero
//     contention). See LLM notes / report for the per-tenant + IAM wiring plan.
package bunconnect

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/spf13/pflag"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"
	"github.com/uptrace/bun/extra/bunotel"
	"go.uber.org/fx"
	_ "modernc.org/sqlite" // pure-Go, CGO-free SQLite driver, registered as "sqlite"

	"github.com/formancehq/go-libs/v5/pkg/fx/storagefx"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
)

// Driver selects the storage backend.
type Driver string

const (
	// DriverSQLite is the default: embedded, per-tenant Hanzo Base (SQLite).
	DriverSQLite Driver = "sqlite"
	// DriverPostgres is opt-in: shared transactional store for multi-instance scale.
	DriverPostgres Driver = "postgres"
)

const (
	// StorageDriverFlag binds to the STORAGE_DRIVER env var via the service viper layer.
	StorageDriverFlag = "storage-driver"
	// SQLiteDSNFlag is the SQLite DSN, used when storage-driver=sqlite.
	SQLiteDSNFlag = "sqlite-dsn"

	// defaultSQLiteDSN: WAL + a generous busy timeout so the single-writer model
	// serializes cleanly, foreign keys on for referential integrity.
	defaultSQLiteDSN = "file:ledger.db?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)"
)

// AddFlags registers the storage-driver selection flags. Call alongside connect.AddFlags.
func AddFlags(flags *pflag.FlagSet) {
	flags.String(StorageDriverFlag, string(DriverSQLite),
		"Storage driver: `sqlite` (default; embedded Hanzo Base, per-tenant) or `postgres` (shared/scale)")
	flags.String(SQLiteDSNFlag, defaultSQLiteDSN,
		"SQLite DSN (used when --storage-driver=sqlite)")
}

// FromFlags reads and validates the storage driver + SQLite DSN from flags.
// If the flags are not registered on this command, it falls back to the default
// (sqlite) so shared helpers stay robust regardless of which command called them.
func FromFlags(flags *pflag.FlagSet) (Driver, string, error) {
	if flags.Lookup(StorageDriverFlag) == nil {
		return DriverSQLite, defaultSQLiteDSN, nil
	}
	raw, _ := flags.GetString(StorageDriverFlag)
	dsn, _ := flags.GetString(SQLiteDSNFlag)
	d := Driver(raw)
	switch d {
	case DriverSQLite, DriverPostgres:
		return d, dsn, nil
	default:
		return "", "", fmt.Errorf("invalid --%s %q: want %q or %q",
			StorageDriverFlag, raw, DriverSQLite, DriverPostgres)
	}
}

// Module returns the fx module that provides *bun.DB for the selected driver.
func Module(driver Driver, opts connect.ConnectionOptions, sqliteDSN string, debug bool) fx.Option {
	switch driver {
	case DriverPostgres:
		return storagefx.BunConnectModule(opts, debug)
	case DriverSQLite:
		return sqliteModule(sqliteDSN)
	default:
		return fx.Error(fmt.Errorf("invalid storage driver %q", driver))
	}
}

func sqliteModule(dsn string) fx.Option {
	return fx.Options(
		fx.Provide(func(logger logging.Logger) (*bun.DB, error) {
			logger.Infof("opening sqlite database (dsn=%s)", dsn)
			return OpenSQLiteDB(dsn)
		}),
		fx.Invoke(func(lc fx.Lifecycle, db *bun.DB, logger logging.Logger) {
			lc.Append(fx.Hook{
				OnStop: func(context.Context) error {
					logger.Info("closing database connection...")
					return db.Close()
				},
			})
		}),
	)
}

// OpenSQLiteDB opens a bun.DB backed by the pure-Go SQLite driver.
// A single connection is used: per-tenant SQLite is single-writer, so serializing
// access is both sufficient and the safe default (avoids SQLITE_BUSY under load).
func OpenSQLiteDB(dsn string) (*bun.DB, error) {
	sqldb, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening sqlite db: %w", err)
	}
	sqldb.SetMaxOpenConns(1)

	db := bun.NewDB(sqldb, sqlitedialect.New(), bun.WithDiscardUnknownColumns())
	db.AddQueryHook(bunotel.NewQueryHook(bunotel.WithFormattedQueries(true)))

	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("pinging sqlite db: %w", err)
	}
	return db, nil
}
