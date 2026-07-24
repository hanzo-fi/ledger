package ledgercore

import (
	"context"
	"fmt"

	"github.com/uptrace/bun"

	"github.com/hanzo-fi/go-libs/v5/pkg/types/time"
)

// The rows below are plain tables — no plpgsql, no composite types, no gin
// indexes, no Postgres schemas. Amounts and volume components are stored as
// decimal TEXT (bigints -> TEXT, exact, no precision loss on either dialect);
// dates as RFC3339 TEXT via go-libs time.Time Value/Scan. []byte columns are
// left untyped so bun emits BYTEA on Postgres and BLOB on SQLite.

type accountRow struct {
	bun.BaseModel `bun:"table:accounts,alias:accounts"`

	Seq           int64     `bun:"seq,pk,autoincrement"`
	Ledger        string    `bun:"ledger,notnull"`
	Address       string    `bun:"address,notnull"`
	InsertionDate time.Time `bun:"insertion_date,type:varchar,nullzero"`
	UpdatedAt     time.Time `bun:"updated_at,type:varchar,nullzero"`
	Metadata      string    `bun:"metadata,type:varchar,notnull,default:'{}'"`
}

type moveRow struct {
	bun.BaseModel `bun:"table:moves,alias:moves"`

	Seq           int64     `bun:"seq,pk,autoincrement"`
	Ledger        string    `bun:"ledger,notnull"`
	TransactionID uint64    `bun:"transactions_id,notnull"`
	Account       string    `bun:"account_address,notnull"`
	Asset         string    `bun:"asset,notnull"`
	Amount        string    `bun:"amount,type:varchar,notnull"`
	IsSource      bool      `bun:"is_source,notnull"`
	InsertionDate time.Time `bun:"insertion_date,type:varchar,nullzero"`
	EffectiveDate time.Time `bun:"effective_date,type:varchar,nullzero"`
	PostCommitIn  string    `bun:"post_commit_inputs,type:varchar,notnull"`
	PostCommitOut string    `bun:"post_commit_outputs,type:varchar,notnull"`
}

type transactionRow struct {
	bun.BaseModel `bun:"table:transactions,alias:transactions"`

	Seq        int64     `bun:"seq,pk,autoincrement"`
	Ledger     string    `bun:"ledger,notnull"`
	ID         uint64    `bun:"id,notnull"`
	Timestamp  time.Time `bun:"timestamp,type:varchar,nullzero"`
	Reference  string    `bun:"reference,type:varchar,nullzero"`
	Postings   string    `bun:"postings,type:varchar,notnull"`
	Metadata   string    `bun:"metadata,type:varchar,notnull,default:'{}'"`
	RevertedAt time.Time `bun:"reverted_at,type:varchar,nullzero"`
	InsertedAt time.Time `bun:"inserted_at,type:varchar,nullzero"`
	UpdatedAt  time.Time `bun:"updated_at,type:varchar,nullzero"`
}

type logRow struct {
	bun.BaseModel `bun:"table:logs,alias:logs"`

	Seq            int64     `bun:"seq,pk,autoincrement"`
	Ledger         string    `bun:"ledger,notnull"`
	ID             uint64    `bun:"id,notnull"`
	Type           string    `bun:"type,notnull"`
	Hash           []byte    `bun:"hash,notnull"`
	Date           time.Time `bun:"date,type:varchar,nullzero"`
	Data           string    `bun:"data,type:varchar,notnull"`
	Memento        []byte    `bun:"memento"`
	IdempotencyKey string    `bun:"idempotency_key,type:varchar,nullzero"`
}

type sequenceRow struct {
	bun.BaseModel `bun:"table:sequences,alias:sequences"`

	Ledger string `bun:"ledger,pk"`
	Name   string `bun:"name,pk"`
	Value  uint64 `bun:"value,notnull"`
}

// Migrate creates the plain double-entry tables and indexes if absent. It is
// idempotent and dialect-agnostic: the exact same bun models build the schema on
// SQLite and Postgres.
func Migrate(ctx context.Context, db bun.IDB) error {
	models := []any{
		(*accountRow)(nil),
		(*moveRow)(nil),
		(*transactionRow)(nil),
		(*logRow)(nil),
		(*sequenceRow)(nil),
	}
	for _, m := range models {
		if _, err := db.NewCreateTable().Model(m).IfNotExists().Exec(ctx); err != nil {
			return fmt.Errorf("creating table for %T: %w", m, err)
		}
	}

	indexes := []struct {
		name    string
		model   any
		unique  bool
		columns []string
		where   string
	}{
		{"accounts_ledger_address", (*accountRow)(nil), true, []string{"ledger", "address"}, ""},
		{"transactions_ledger_id", (*transactionRow)(nil), true, []string{"ledger", "id"}, ""},
		{"logs_ledger_id", (*logRow)(nil), true, []string{"ledger", "id"}, ""},
		{"moves_balance", (*moveRow)(nil), false, []string{"ledger", "account_address", "asset", "seq"}, ""},
		// Idempotency: a (ledger, idempotency_key) is unique among the logs that
		// carry one. A PARTIAL index (only where a key is set) lets the many
		// key-less logs coexist while a repeated key collides — the same guard
		// Formance enforces with logs_idempotency_key, valid on both dialects.
		{"logs_ledger_ik", (*logRow)(nil), true, []string{"ledger", "idempotency_key"}, "idempotency_key <> ''"},
	}
	for _, ix := range indexes {
		q := db.NewCreateIndex().Model(ix.model).IfNotExists().Index(ix.name).Column(ix.columns...)
		if ix.unique {
			q = q.Unique()
		}
		if ix.where != "" {
			q = q.Where(ix.where)
		}
		if _, err := q.Exec(ctx); err != nil {
			return fmt.Errorf("creating index %s: %w", ix.name, err)
		}
	}
	return nil
}
