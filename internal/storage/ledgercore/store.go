// Package ledgercore is the dialect-agnostic double-entry spine of the ledger.
//
// Formance braided the double-entry business rules INTO the storage engine as
// ~38 plpgsql functions + 6 triggers (see internal/storage/bucket/migrations).
// That makes the ledger's logic run only where it is STORED (Postgres), so the
// same tables on SQLite are inert. This package lifts that logic OUT into plain
// Go over bun, leaving storage as ordinary tables that bun creates and drives
// identically on SQLite and Postgres. The log hash chain — the crown jewel of
// double-entry integrity — is computed by the canonical, already-dialect-agnostic
// ledger.Log.ChainLog (internal/log.go); the plpgsql compute_hash/set_log_hash
// were written to match it, so reusing it guarantees byte-identical results on
// both dialects by construction.
//
// Scope: the core write path (postings -> moves -> balances + chained log),
// revert, and the balance/volume read path. Effective-volume back-dating,
// metadata history, and async hash blocks remain plpgsql-only for now (see the
// package README / task report for the honest remaining list).
package ledgercore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/uptrace/bun"

	"github.com/hanzo-fi/ledger/internal/storage/bunconnect"
)

// Store is a dialect-agnostic ledger store scoped to a single ledger (tenant).
// db is a *bun.DB or bun.Tx; the same code runs on sqlitedialect and pgdialect.
type Store struct {
	db     bun.IDB
	ledger string
}

// New returns a Store scoped to ledgerName over the given bun handle.
func New(db bun.IDB, ledgerName string) *Store {
	return &Store{db: db, ledger: ledgerName}
}

// Ledger returns the ledger (tenant) name this store is scoped to.
func (s *Store) Ledger() string { return s.ledger }

// slugPattern guards per-tenant file names: a slug maps 1:1 to a file on disk,
// so it must not contain path separators or traversal.
var slugPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// OpenLedgerFile opens (creating the directory if needed) a per-tenant SQLite
// file at dir/{slug}.db. This is the Base HIP-0105 per-tenant-file model:
// Formance's schema-per-bucket becomes one SQLite file per bucket/ledger.
// Single-writer-per-file (SetMaxOpenConns(1) in bunconnect) serializes writes,
// so advisory locks are unneeded on this path.
func OpenLedgerFile(dir, slug string) (*bun.DB, error) {
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

// nextID atomically allocates the next per-ledger sequence value for name
// (e.g. "transaction" or "log"). Formance's Postgres sequences become rows in a
// counter table; the upsert increments in place and returns the new value, which
// both sqlitedialect and pgdialect support via ON CONFLICT ... DO UPDATE.
func (s *Store) nextID(ctx context.Context, name string) (uint64, error) {
	var v uint64
	err := s.db.NewRaw(
		`insert into sequences (ledger, name, value) values (?, ?, 1)
		 on conflict (ledger, name) do update set value = sequences.value + 1
		 returning value`,
		s.ledger, name,
	).Scan(ctx, &v)
	if err != nil {
		return 0, fmt.Errorf("allocating %s id: %w", name, err)
	}
	return v, nil
}
