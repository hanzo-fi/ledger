package dialect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/uptrace/bun"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/postgres"
)

// SQL is the server engine, hanzoai/sql: PostgreSQL with pgvector. It runs a
// bucket as a schema, ids from a per-ledger sequence, and ledger serialization
// on transaction-scoped advisory locks.
type SQL struct{}

func (SQL) Name() string { return Scheme }

func (SQL) OpenBucket(ctx context.Context, db bun.IDB, bucket string) error {
	_, err := db.ExecContext(ctx, `create schema if not exists ?`, bun.Ident(bucket))
	return err
}

func (SQL) DropBucket(ctx context.Context, db bun.IDB, bucket string) error {
	_, err := db.ExecContext(ctx, `drop schema if exists ? cascade`, bun.Ident(bucket))
	return err
}

func (SQL) NextID(bucket, relation, _ string, counter int) Fragment {
	return frag(`nextval(?)`, fmt.Sprintf(`"%s"."%s_id_%d"`, bucket, singular(relation), counter))
}

func (SQL) SyncID(ctx context.Context, db bun.IDB, bucket, relation, ledger string, counter int) error {
	_, err := db.NewRaw(fmt.Sprintf(
		`select setval('"%s"."%s_id_%d"', (select max(id) from "%s".%s where ledger = ?)::bigint)`,
		bucket, singular(relation), counter, bucket, relation,
	), ledger).Exec(ctx)

	return err
}

func (SQL) LockLedger(ctx context.Context, db bun.IDB, ledgerID int) (bun.IDB, func() error, error) {
	key := fmt.Sprintf("ledger:%d", ledgerID)
	switch db := db.(type) {
	case *bun.DB:
		conn, err := db.Conn(ctx)
		if err != nil {
			return nil, nil, err
		}
		if _, err := conn.ExecContext(ctx, `select pg_advisory_lock(hashtext(?))`, key); err != nil {
			_ = conn.Close()
			return nil, nil, err
		}
		return conn, func() error {
			if _, err := conn.ExecContext(ctx, `select pg_advisory_unlock(hashtext(?))`, key); err != nil {
				return err
			}
			return conn.Close()
		}, nil
	case bun.Tx:
		if _, err := db.ExecContext(ctx, `select pg_advisory_xact_lock(hashtext(?))`, key); err != nil {
			return nil, nil, err
		}
		// A transaction-scoped lock is released by the commit.
		return db, func() error { return nil }, nil
	default:
		return nil, nil, fmt.Errorf("cannot lock ledger on %T", db)
	}
}

func (SQL) LockRows(q *bun.SelectQuery) *bun.SelectQuery { return q.For("update") }

func (SQL) Merge(left, right string) string {
	return "(" + left + " || " + right + ")"
}

func (SQL) Canonical(column string) string { return column }

func (SQL) Remove(column, key string) Fragment {
	return frag("("+column+" - ?)", key)
}

func (SQL) Has(column, key string) Fragment {
	return frag(column+" -> ? is not null", key)
}

func (SQL) Holds(column string, entries map[string]any) Fragment {
	return frag(column+" @> ?", entries)
}

func (SQL) ArrayHolds(column, value string) Fragment {
	data, err := json.Marshal([]string{value})
	if err != nil {
		panic(err)
	}
	return frag(column+" @> ?", string(data))
}

func (SQL) ArrayMeets(column string, values []string) Fragment {
	placeholders := strings.TrimRight(strings.Repeat("?,", len(values)), ",")
	args := make([]any, 0, len(values))
	for _, v := range values {
		args = append(args, v)
	}
	return frag(column+` \?| array[`+placeholders+`]`, args...)
}

func (SQL) SegmentsMatch(column string, segments map[string]any) Fragment {
	data, err := json.Marshal([]any{segments})
	if err != nil {
		panic(err)
	}
	return frag(column+" @> ?", string(data))
}

func (SQL) Declares() bool { return false }

func (SQL) Pair(bucket, input, output string) string {
	return fmt.Sprintf(`(%s, %s)::"%s".volumes`, input, output, bucket)
}

func (SQL) Volumes(column string) string {
	return fmt.Sprintf(`json_build_object('input', (%s).inputs, 'output', (%s).outputs)`, column, column)
}

func (SQL) Gather(key, value string) string {
	return fmt.Sprintf("public.aggregate_objects(json_build_object(%s, %s)::jsonb)", key, value)
}

// ResolveError maps the engine's diagnostics onto the shared vocabulary. A
// named constraint keeps its name, which is what the store reads to tell an
// idempotency clash from a reference clash.
func (SQL) ResolveError(err error) error {
	resolved := postgres.ResolveError(err)

	var constraint postgres.ErrConstraintsFailed
	if errors.As(resolved, &constraint) {
		return NewErrConstraint(constraint.GetConstraint(), resolved)
	}

	return resolved
}

// singular maps a relation name onto the counter name the bucket creates for
// it: the transactions relation counts through transaction_id_<n>.
func singular(relation string) string {
	return strings.TrimSuffix(relation, "s")
}

var _ Dialect = SQL{}
