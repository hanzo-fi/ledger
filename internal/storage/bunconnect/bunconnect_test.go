package bunconnect

import (
	"context"
	"testing"

	"github.com/spf13/pflag"

	"github.com/formancehq/go-libs/v5/pkg/storage/bun/connect"
)

// TestSQLiteRoundTrip proves the sqlite dialect+driver seam is functional:
// open -> create table -> insert -> select back, all through bun.
func TestSQLiteRoundTrip(t *testing.T) {
	db, err := OpenSQLiteDB("file:roundtrip?mode=memory&cache=shared&_pragma=foreign_keys(1)")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	if _, err := db.NewCreateTable().
		Model((*account)(nil)).
		IfNotExists().
		Exec(ctx); err != nil {
		t.Fatalf("create table: %v", err)
	}

	if _, err := db.NewInsert().
		Model(&account{Address: "world", Balance: 100}).
		Exec(ctx); err != nil {
		t.Fatalf("insert: %v", err)
	}

	var got account
	if err := db.NewSelect().
		Model(&got).
		Where("address = ?", "world").
		Scan(ctx); err != nil {
		t.Fatalf("select: %v", err)
	}
	if got.Address != "world" || got.Balance != 100 {
		t.Fatalf("unexpected row: %+v", got)
	}

	if got := db.Dialect().Name().String(); got != "sqlite" {
		t.Fatalf("expected sqlite dialect, got %q", got)
	}
}

type account struct {
	ID      int64  `bun:"id,pk,autoincrement"`
	Address string `bun:"address,notnull"`
	Balance int64  `bun:"balance,notnull"`
}

func TestFromFlagsDefaultsAndValidation(t *testing.T) {
	// Unregistered flags -> default sqlite.
	empty := pflag.NewFlagSet("empty", pflag.ContinueOnError)
	d, dsn, err := FromFlags(empty)
	if err != nil || d != DriverSQLite || dsn != defaultSQLiteDSN {
		t.Fatalf("unregistered: got (%q,%q,%v)", d, dsn, err)
	}

	// Registered + explicit postgres.
	fs := pflag.NewFlagSet("fs", pflag.ContinueOnError)
	AddFlags(fs)
	_ = fs.Set(StorageDriverFlag, "postgres")
	d, _, err = FromFlags(fs)
	if err != nil || d != DriverPostgres {
		t.Fatalf("postgres: got (%q,%v)", d, err)
	}

	// Invalid driver -> error.
	fs2 := pflag.NewFlagSet("fs2", pflag.ContinueOnError)
	AddFlags(fs2)
	_ = fs2.Set(StorageDriverFlag, "mysql")
	if _, _, err := FromFlags(fs2); err == nil {
		t.Fatalf("expected error for invalid driver")
	}

	// --postgres-uri alone (driver left at default) opts into Postgres.
	fs3 := pflag.NewFlagSet("fs3", pflag.ContinueOnError)
	AddFlags(fs3)
	connect.AddFlags(fs3)
	_ = fs3.Set(connect.PostgresURIFlag, "postgres://localhost/ledger")
	if d, _, err := FromFlags(fs3); err != nil || d != DriverPostgres {
		t.Fatalf("postgres-uri opt-in: got (%q,%v)", d, err)
	}

	// Explicit --storage-driver=sqlite wins over a --postgres-uri.
	fs4 := pflag.NewFlagSet("fs4", pflag.ContinueOnError)
	AddFlags(fs4)
	connect.AddFlags(fs4)
	_ = fs4.Set(StorageDriverFlag, "sqlite")
	_ = fs4.Set(connect.PostgresURIFlag, "postgres://localhost/ledger")
	if d, _, err := FromFlags(fs4); err != nil || d != DriverSQLite {
		t.Fatalf("explicit sqlite wins: got (%q,%v)", d, err)
	}
}
