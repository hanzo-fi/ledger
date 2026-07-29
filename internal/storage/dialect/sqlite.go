package dialect

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/uptrace/bun"
)

// SQLite runs a bucket as an attached database file, ids from a per-ledger max,
// ledger serialization on the single writer the engine already enforces, and
// derives log hashes in Go.
//
// The engine takes one writer at a time, so the pool is held at a single
// connection (see Open). That is also what makes an attached bucket stable:
// the attachment belongs to the connection.
type SQLite struct {
	dir  string
	pool *sql.DB

	mu   sync.Mutex
	open map[string]struct{}
}

// Buckets are the buckets every connection has to resolve.
func (d *SQLite) Buckets() []string {
	d.mu.Lock()
	defer d.mu.Unlock()

	buckets := make([]string, 0, len(d.open))
	for bucket := range d.open {
		buckets = append(buckets, bucket)
	}

	return buckets
}

func NewSQLite(dir string) *SQLite {
	return &SQLite{
		dir:  dir,
		open: map[string]struct{}{},
	}
}

func (*SQLite) Name() string { return "sqlite" }

// Path is the file backing a bucket.
func (d *SQLite) Path(bucket string) string {
	return filepath.Join(d.dir, bucket+".db")
}

func (d *SQLite) OpenBucket(ctx context.Context, db bun.IDB, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.open[bucket]; ok {
		return nil
	}
	if err := os.MkdirAll(d.dir, 0o750); err != nil {
		return fmt.Errorf("creating store directory: %w", err)
	}
	if _, err := db.ExecContext(ctx, `attach database ? as ?`, d.Path(bucket), bun.Ident(bucket)); err != nil {
		// Re-attaching the same name is not an error worth failing on: the
		// connection already resolves the bucket.
		if !strings.Contains(err.Error(), "already in use") {
			return fmt.Errorf("attaching bucket %q: %w", bucket, err)
		}
	}
	// A journal cannot change inside a transaction. The connection that opens
	// a bucket mid-transaction keeps the default until it is replaced; every
	// connection opened from here on gets the pragmas from the connector.
	if _, inTransaction := db.(bun.Tx); !inTransaction {
		for _, pragma := range []string{"journal_mode = WAL", "busy_timeout = 10000"} {
			if _, err := db.ExecContext(ctx, fmt.Sprintf(`pragma "%s".%s`, bucket, pragma)); err != nil {
				return fmt.Errorf("preparing bucket %q: %w", bucket, err)
			}
		}
	}
	d.open[bucket] = struct{}{}

	return nil
}

func (d *SQLite) DropBucket(ctx context.Context, db bun.IDB, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.open[bucket]; ok {
		if _, err := db.ExecContext(ctx, `detach database ?`, bun.Ident(bucket)); err != nil {
			return fmt.Errorf("detaching bucket %q: %w", bucket, err)
		}
		delete(d.open, bucket)
	}
	for _, suffix := range []string{"", "-wal", "-shm"} {
		if err := os.Remove(d.Path(bucket) + suffix); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("removing bucket %q: %w", bucket, err)
		}
	}
	return nil
}

// NextID reads the ledger's own maximum. The engine admits a single writer, so
// the read and the insert that follows it cannot interleave, and ids stay dense
// even across a rolled back transaction.
func (*SQLite) NextID(bucket, relation, ledger string, _ int) Fragment {
	return frag(
		fmt.Sprintf(`(select coalesce(max(id), 0) + 1 from "%s".%s where ledger = ?)`, bucket, relation),
		ledger,
	)
}

// SyncID has nothing to do: the counter is the ledger's own maximum, so it is
// never behind the rows it counts.
func (*SQLite) SyncID(context.Context, bun.IDB, string, string, string, int) error {
	return nil
}

// LockLedger has nothing to take. A write transaction on this engine already
// excludes every other writer for its whole duration - strictly more than the
// per-ledger advisory lock it stands in for - so the exclusion the caller is
// asking for is already held.
func (d *SQLite) LockLedger(_ context.Context, db bun.IDB, _ int) (bun.IDB, func() error, error) {
	return db, func() error { return nil }, nil
}

// LockRows is a no-op: a write transaction already excludes every other writer.
func (*SQLite) LockRows(q *bun.SelectQuery) *bun.SelectQuery { return q }

func (*SQLite) Merge(left, right string) string {
	return "json_patch(coalesce(" + left + ", '{}'), coalesce(" + right + ", '{}'))"
}

func (*SQLite) Canonical(column string) string { return "json(" + column + ")" }

func (*SQLite) Remove(column, key string) Fragment {
	return frag("json_remove("+column+", ?)", path(key))
}

func (*SQLite) Has(column, key string) Fragment {
	return frag("json_extract("+column+", ?) is not null", path(key))
}

func (*SQLite) Holds(column string, entries map[string]any) Fragment {
	if len(entries) == 0 {
		return frag("1 = 1")
	}
	tests := make([]string, 0, len(entries))
	args := make([]any, 0, len(entries)*2)
	for key, value := range entries {
		tests = append(tests, "json_extract("+column+", ?) = ?")
		args = append(args, path(key), value)
	}
	return frag("("+strings.Join(tests, " and ")+")", args...)
}

func (*SQLite) ArrayHolds(column, value string) Fragment {
	return frag("exists (select 1 from json_each("+column+") where value = ?)", value)
}

func (*SQLite) ArrayMeets(column string, values []string) Fragment {
	placeholders := strings.TrimRight(strings.Repeat("?,", len(values)), ",")
	args := make([]any, 0, len(values))
	for _, v := range values {
		args = append(args, v)
	}
	return frag(
		"exists (select 1 from json_each("+column+") where value in ("+placeholders+"))",
		args...,
	)
}

// SegmentsMatch asks whether any element of an exploded address array carries
// the given segments. A nil segment asserts the address ends there.
func (*SQLite) SegmentsMatch(column string, segments map[string]any) Fragment {
	tests := make([]string, 0, len(segments))
	args := make([]any, 0, len(segments)*2)
	for key, value := range segments {
		if value == nil {
			tests = append(tests, "json_extract(element.value, ?) is null")
			args = append(args, path(key))
			continue
		}
		tests = append(tests, "json_extract(element.value, ?) = ?")
		args = append(args, path(key), value)
	}
	if len(tests) == 0 {
		return frag("1 = 1")
	}
	return frag(
		"exists (select 1 from json_each("+column+") element where "+strings.Join(tests, " and ")+")",
		args...,
	)
}

// Declares is true: the schema is one declaration rather than a replayed
// history of migrations.
func (*SQLite) Declares() bool { return true }

// Pair writes a volumes value the way the model already writes one, so a pair
// built inside a query and a pair read out of a column read the same.
func (*SQLite) Pair(_, input, output string) string {
	return fmt.Sprintf("('(' || %s || ', ' || %s || ')')", input, output)
}

// Volumes reads one side of that pair back.
func (*SQLite) Volumes(column, field string) string {
	if field == "inputs" {
		return fmt.Sprintf("cast(substr(%s, 2, instr(%s, ',') - 2) as integer)", column, column)
	}
	return fmt.Sprintf(
		"cast(substr(%s, instr(%s, ',') + 1, length(%s) - instr(%s, ',') - 1) as integer)",
		column, column, column, column,
	)
}

func (*SQLite) Object(pairs ...string) string {
	return "json_object(" + strings.Join(pairs, ", ") + ")"
}

func (*SQLite) Gather(key, value string) string {
	return fmt.Sprintf("json_group_object(%s, json(%s))", key, value)
}

// ResolveError reads the engine's diagnostic. Both SQLite backends this driver
// can be built on (the C library and the pure Go engine) report the same
// wording, so the wording is what is matched.
func (*SQLite) ResolveError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, sql.ErrNoRows) {
		return ErrNotFound
	}
	message := err.Error()
	switch {
	case strings.Contains(message, "constraint failed"):
		return NewErrConstraint(constraintName(message), err)
	case strings.Contains(message, "database is locked"),
		strings.Contains(message, "database table is locked"):
		return ErrDeadlock
	case strings.Contains(message, "readonly database"):
		return ErrReadOnly
	case strings.Contains(message, "no such table"):
		return ErrMissingTable
	}
	return err
}

// constraintName reads the index behind a uniqueness failure. The engine
// reports the columns it indexes ("UNIQUE constraint failed: logs.ledger,
// logs.idempotency_key"), so the name is the relation followed by the columns
// that are not the ledger discriminator - which is exactly how the schema
// names its unique indexes.
func constraintName(message string) string {
	_, columns, found := strings.Cut(message, "constraint failed: ")
	if !found {
		return ""
	}
	// The pure Go engine appends the extended result code.
	if open := strings.LastIndex(columns, " ("); open >= 0 && strings.HasSuffix(columns, ")") {
		columns = columns[:open]
	}
	var relation string
	parts := make([]string, 0, 2)
	for _, column := range strings.Split(columns, ",") {
		column = strings.TrimSpace(column)
		table, name, ok := strings.Cut(column, ".")
		if !ok {
			continue
		}
		relation = table
		if name == "ledger" {
			continue
		}
		parts = append(parts, name)
	}
	if relation == "" {
		return ""
	}
	if len(parts) == 0 {
		return relation + "_ledger"
	}
	return relation + "_" + strings.Join(parts, "_")
}

func path(key string) string {
	return `$."` + strings.ReplaceAll(key, `"`, `""`) + `"`
}

var _ Dialect = (*SQLite)(nil)
