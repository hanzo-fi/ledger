// Package ledgercore is the dialect-agnostic double-entry spine of the ledger —
// the ONE double-entry engine, importable by any Hanzo service that needs a
// balanced, hash-chained journal (the ledger's own production store, and the
// cloud treasury's reserve fund).
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
// The engine is DRIVER-FREE: New takes a caller-provided bun.IDB, so the caller
// owns which SQLite/Postgres driver is registered. This keeps the package free of
// any database/sql.Register("sqlite", …) side effect, so importing it never
// collides with a host binary's own SQLite driver (e.g. the cloud's encrypted
// hanzoai/sqlite). A per-tenant SQLite-file opener lives in the test helper, not
// here, precisely so importers do not inherit a driver.
//
// Scope: the core write path (postings -> moves -> balances + chained log),
// revert, idempotency-key dedup, a transactional scope, and the balance/volume
// read path. Effective-volume back-dating, metadata history, and async hash
// blocks remain plpgsql-only for now (see the package README / task report for
// the honest remaining list).
package ledgercore

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"
)

// Store is a dialect-agnostic ledger store scoped to a single ledger (tenant).
// db is a *bun.DB or bun.Tx; the same code runs on sqlitedialect and pgdialect.
type Store struct {
	db     bun.IDB
	ledger string
}

// New returns a Store scoped to ledgerName over the given bun handle. The caller
// owns the connection (and thus the registered SQL driver); Migrate builds the
// schema on it.
func New(db bun.IDB, ledgerName string) *Store {
	return &Store{db: db, ledger: ledgerName}
}

// Ledger returns the ledger (tenant) name this store is scoped to.
func (s *Store) Ledger() string { return s.ledger }

// WithTx runs fn against a Store bound to a single database transaction,
// committing on nil and rolling back on error. It is the atomicity seam for a
// read-then-write guard (e.g. "balance >= amount THEN post") — the balance read
// and the resulting post commit or roll back together, so two concurrent debits
// can never both pass the guard. When the Store is already tx-scoped, fn reuses
// that transaction (the single-writer-per-tenant-file model needs no savepoint).
func (s *Store) WithTx(ctx context.Context, fn func(*Store) error) error {
	switch db := s.db.(type) {
	case *bun.DB:
		return db.RunInTx(ctx, nil, func(ctx context.Context, tx bun.Tx) error {
			return fn(&Store{db: tx, ledger: s.ledger})
		})
	case bun.Tx:
		return fn(s)
	default:
		return fmt.Errorf("ledgercore: WithTx requires *bun.DB or bun.Tx, got %T", s.db)
	}
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
