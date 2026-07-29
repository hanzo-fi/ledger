package dialect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	hanzosqlite "github.com/hanzoai/sqlite"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/sqlitedialect"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/connect"
)

// Store is where SQLite keeps its files when the DSN names no directory.
const Store = "ledger.store"

// Scheme names the server engine in a DSN: sql://… reaches hanzoai/sql,
// PostgreSQL with pgvector. It is the only spelling the ledger accepts.
const Scheme = "sql"

// wire is the scheme hanzoai/sql speaks on the wire. The DSN a reader writes
// names the engine; the driver underneath still wants its own scheme, so the
// two are exchanged here and nowhere else.
const wire = "postgres"

// Config says which database to open and how hard to hold it.
type Config struct {
	// DSN selects the engine. sql://… reaches hanzoai/sql; anything else -
	// sqlite://path, file:path, a bare path, or nothing at all - reaches
	// SQLite. Local development needs no configuration.
	DSN string

	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxIdleTime time.Duration
	ConnMaxLifetime time.Duration
}

// Open resolves the DSN to an engine and connects to it. This is the one place
// the choice is made; everything downstream reads the returned Dialect.
func Open(ctx context.Context, config Config, hooks ...bun.QueryHook) (*bun.DB, Dialect, error) {
	if IsSQL(config.DSN) {
		db, err := connect.OpenSQLDB(ctx, connect.ConnectionOptions{
			DatabaseSourceName: Wire(config.DSN),
			MaxOpenConns:       config.MaxOpenConns,
			MaxIdleConns:       config.MaxIdleConns,
			ConnMaxIdleTime:    config.ConnMaxIdleTime,
			ConnMaxLifetime:    config.ConnMaxLifetime,
		}, hooks...)
		if err != nil {
			return nil, nil, err
		}
		return db, SQL{}, nil
	}
	return openSQLite(config, hooks...)
}

// IsSQL reports whether a DSN names the server engine.
func IsSQL(dsn string) bool {
	return strings.HasPrefix(dsn, Scheme+"://")
}

// Wire restates a DSN in the scheme the driver dials. It is the one place the
// engine's name and its wire protocol's name are exchanged.
func Wire(dsn string) string {
	return wire + "://" + strings.TrimPrefix(dsn, Scheme+"://")
}

// Dir reads the directory a SQLite DSN keeps its bucket files in. The DSN may
// name a directory, a file inside one, or nothing.
func Dir(dsn string) string {
	switch {
	case dsn == "":
		return Store
	case strings.HasPrefix(dsn, "sqlite://"):
		dsn = strings.TrimPrefix(dsn, "sqlite://")
	case strings.HasPrefix(dsn, "sqlite:"):
		dsn = strings.TrimPrefix(dsn, "sqlite:")
	case strings.HasPrefix(dsn, "file:"):
		dsn = strings.TrimPrefix(dsn, "file:")
	}
	if question := strings.IndexByte(dsn, '?'); question >= 0 {
		dsn = dsn[:question]
	}
	if dsn == "" {
		return Store
	}
	if unescaped, err := url.PathUnescape(dsn); err == nil {
		dsn = unescaped
	}
	if strings.HasSuffix(dsn, ".db") || strings.HasSuffix(dsn, ".sqlite") {
		return filepath.Dir(dsn)
	}
	return dsn
}

// openSQLite opens the store and holds the pool at one connection.
//
// This is what serializes writers. A wider pool lets two deferred transactions
// read the same rows and write back over each other, which a ledger cannot
// survive - see TestOneWriterKeepsEveryWrite. It also keeps a bucket
// attachment stable, since an attachment belongs to a connection.
func openSQLite(config Config, hooks ...bun.QueryHook) (*bun.DB, Dialect, error) {
	dir := Dir(config.DSN)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, nil, fmt.Errorf("creating store directory %q: %w", dir, err)
	}

	d := NewSQLite(dir)

	// A bucket is an attached file, and an attachment belongs to the
	// connection that made it. The pool is free to replace a connection, so
	// every connection it opens attaches the buckets already known.
	probe, err := hanzosqlite.OpenDB(filepath.Join(dir, "main.db"), nil)
	if err != nil {
		return nil, nil, fmt.Errorf("opening store %q: %w", dir, err)
	}
	dsn := hanzosqlite.DSN(filepath.Join(dir, "main.db"), nil)
	sqldb := sql.OpenDB(&connector{driver: probe.Driver(), dsn: dsn, dialect: d})
	if err := probe.Close(); err != nil {
		return nil, nil, err
	}

	sqldb.SetMaxOpenConns(1)
	sqldb.SetMaxIdleConns(1)
	sqldb.SetConnMaxLifetime(0)
	d.pool = sqldb

	db := bun.NewDB(sqldb, sqlitedialect.New(), bun.WithDiscardUnknownColumns())
	for _, hook := range hooks {
		db.AddQueryHook(hook)
	}
	if err := db.Ping(); err != nil {
		return nil, nil, err
	}

	return db, d, nil
}

// connector opens connections that already resolve every known bucket.
type connector struct {
	driver  driver.Driver
	dsn     string
	dialect *SQLite
}

func (c *connector) Driver() driver.Driver { return c.driver }

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.driver.Open(c.dsn)
	if err != nil {
		return nil, err
	}
	execer, ok := conn.(driver.ExecerContext)
	if !ok {
		return nil, errors.New("sqlite: the driver cannot attach a bucket")
	}
	for _, bucket := range c.dialect.Buckets() {
		for _, statement := range []string{
			fmt.Sprintf(`attach database '%s' as "%s"`, c.dialect.Path(bucket), bucket),
			// A bucket writes ahead like the store it hangs off; a journal is
			// per database, not inherited from the one attaching it.
			fmt.Sprintf(`pragma "%s".journal_mode = WAL`, bucket),
			fmt.Sprintf(`pragma "%s".busy_timeout = 10000`, bucket),
		} {
			if _, err := execer.ExecContext(ctx, statement, nil); err != nil {
				_ = conn.Close()
				return nil, fmt.Errorf("attaching bucket %q: %w", bucket, err)
			}
		}
	}

	return conn, nil
}

// Statements cuts a schema into the statements it declares. Comments are
// dropped first so that a semicolon inside prose cannot cut a statement in two.
func Statements(schema string) []string {
	body := make([]string, 0)
	for _, line := range strings.Split(schema, "\n") {
		if comment := strings.Index(line, "--"); comment >= 0 {
			line = line[:comment]
		}
		body = append(body, line)
	}

	statements := make([]string, 0)
	for _, statement := range strings.Split(strings.Join(body, "\n"), ";") {
		if strings.TrimSpace(statement) == "" {
			continue
		}
		statements = append(statements, statement)
	}

	return statements
}
