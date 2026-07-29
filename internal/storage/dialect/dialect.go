// Package dialect holds every difference between the SQL engines the ledger
// runs on. Engine choice is made once, from the DSN, and reaches the rest of
// the storage layer as a single value. No other package branches on the engine.
package dialect

import (
	"context"

	"github.com/uptrace/bun"
)

// Fragment is a SQL expression with its bound arguments. Verbs return one so a
// verb whose argument shape differs between engines stays a single call site.
type Fragment struct {
	SQL  string
	Args []any
}

func frag(sql string, args ...any) Fragment {
	return Fragment{SQL: sql, Args: args}
}

// Dialect is the complete vocabulary in which the two engines disagree.
//
// Everything the storage layer expresses outside these verbs is written once
// and runs unchanged on both: bun renders placeholders and identifiers, and
// bucket relations are addressed as "bucket".relation on both engines (a
// Postgres schema, an attached database file on SQLite).
type Dialect interface {
	// Name is the engine name, as it appears in a DSN scheme.
	Name() string

	// OpenBucket makes "bucket".relation resolvable. DropBucket destroys it.
	OpenBucket(ctx context.Context, db bun.IDB, bucket string) error
	DropBucket(ctx context.Context, db bun.IDB, bucket string) error

	// NextID yields the next value of a ledger's contiguous id counter for
	// relation (transactions, logs). Ids are dense per ledger, not per table.
	NextID(bucket, relation, ledger string, counter int) Fragment

	// SyncID brings the counter back level with the rows already recorded,
	// which an import leaves it behind.
	SyncID(ctx context.Context, db bun.IDB, bucket, relation, ledger string, counter int) error

	// LockLedger serializes writers over one ledger and returns the release.
	LockLedger(ctx context.Context, db bun.IDB, ledgerID int) (bun.IDB, func() error, error)

	// LockRows makes a select hold its rows against concurrent writers until
	// the surrounding transaction ends.
	LockRows(q *bun.SelectQuery) *bun.SelectQuery

	// Object verbs read and write JSON objects (metadata, features). Merge
	// and Remove are expressions so a caller composes them; Holds and Has
	// carry arguments whose shape differs between engines.
	Merge(left, right string) string
	// Canonical is the stored form of an object reduced so that two of them
	// compare by value rather than by how they were written.
	Canonical(column string) string
	Remove(column, key string) Fragment
	Has(column, key string) Fragment
	Holds(column string, entries map[string]any) Fragment

	// Array verbs read JSON arrays of strings (a transaction's sources and
	// destinations) and arrays of segment objects (their exploded addresses).
	ArrayHolds(column, value string) Fragment
	ArrayMeets(column string, values []string) Fragment
	SegmentsMatch(column string, segments map[string]any) Fragment

	// Declares reports whether the schema is stated in one declaration rather
	// than reached by replaying a recorded history.
	Declares() bool

	// Pair builds a volumes value out of an input and an output expression,
	// and Volumes reads one back as the JSON object a reader scans, at the
	// precision it was written.
	Pair(bucket, input, output string) string
	Volumes(column string) string

	// Gather is the aggregate of a JSON object: one object holding every key
	// with the value standing against it.
	Gather(key, value string) string

	// ResolveError maps an engine error onto the storage error vocabulary.
	ResolveError(err error) error
}
