package ledgercore

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/uptrace/bun"

	"github.com/hanzo-fi/ledger/internal/storage/bunconnect"
)

// slugPattern guards per-tenant file names: a slug maps 1:1 to a file on disk,
// so it must not contain path separators or traversal.
var slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// openLedgerFile opens (creating the directory if needed) a per-tenant SQLite
// file at dir/{slug}.db over the pure-Go modernc driver (via bunconnect). It is a
// TEST helper only — deliberately kept out of the importable engine so a service
// that imports ledgercore does not inherit modernc's database/sql "sqlite"
// registration (which would collide with a host binary's own SQLite driver).
// Production callers supply their own *bun.DB to New; ledger-fi's serve path opens
// via bunconnect directly.
func openLedgerFile(dir, slug string) (*bun.DB, error) {
	if !slugPattern.MatchString(slug) {
		return nil, fmt.Errorf("invalid ledger slug %q: want %s", slug, slugPattern.String())
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("creating data dir %q: %w", dir, err)
	}
	path := filepath.Join(dir, slug+".db")
	dsn := fmt.Sprintf(
		"file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)",
		path,
	)
	return bunconnect.OpenSQLiteDB(dsn)
}
