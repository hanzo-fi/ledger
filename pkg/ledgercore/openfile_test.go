package ledgercore

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"

	"github.com/uptrace/bun"

	"github.com/hanzo-fi/ledger/internal/storage/dialect"
)

// slugPattern guards per-tenant file names: a slug maps 1:1 to a file on disk,
// so it must not contain path separators or traversal.
var slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// openLedgerFile opens (creating the directory if needed) a per-tenant SQLite
// store under dir. It is a TEST helper only — deliberately kept out of the
// importable engine so a service that imports ledgercore does not inherit a
// database/sql "sqlite" registration (which would collide with a host binary's
// own SQLite driver). Production callers supply their own *bun.DB to New; the
// ledger's serve path opens through the dialect seam.
func openLedgerFile(dir, slug string) (*bun.DB, error) {
	if !slugPattern.MatchString(slug) {
		return nil, fmt.Errorf("invalid ledger slug %q: want %s", slug, slugPattern.String())
	}
	db, _, err := dialect.Open(context.Background(), dialect.Config{DSN: filepath.Join(dir, slug)})
	if err != nil {
		return nil, fmt.Errorf("opening ledger store: %w", err)
	}
	return db, nil
}
