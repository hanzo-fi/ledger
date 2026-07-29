package bucket

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"text/template"

	"github.com/uptrace/bun"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/migrations"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/storage/dialect"
	"github.com/hanzo-fi/ledger/pkg/features"
)

//go:embed sqlite.sql
var sqliteSchema string

// SQLite states the bucket shape in one declaration instead of replaying the
// recorded history. Nothing has been deployed on it, so there is nothing to
// travel from: the schema is the schema, and it is always current.
type SQLite struct {
	dialect *dialect.SQLite
	name    string
}

func NewSQLite(d *dialect.SQLite, name string) *SQLite {
	return &SQLite{dialect: d, name: name}
}

func (b *SQLite) Migrate(ctx context.Context, db bun.IDB, _ ...migrations.Option) error {
	if err := b.dialect.OpenBucket(ctx, db, b.name); err != nil {
		return err
	}
	buf := bytes.NewBuffer(nil)
	if err := template.Must(template.New("schema").Parse(sqliteSchema)).
		Execute(buf, map[string]string{"Schema": b.name}); err != nil {
		return fmt.Errorf("templating bucket schema: %w", err)
	}
	for _, statement := range dialect.Statements(buf.String()) {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("declaring bucket %q: %w", b.name, err)
		}
	}
	return nil
}

func (b *SQLite) AddLedger(ctx context.Context, db bun.IDB, l ledger.Ledger) error {
	for _, setup := range sqliteLedgerSetups {
		if !l.Features.Match(setup.requireFeatures) {
			continue
		}
		buf := bytes.NewBuffer(nil)
		if err := template.Must(template.New("sql").Parse(setup.script)).Execute(buf, l); err != nil {
			return fmt.Errorf("executing template: %w", err)
		}
		if _, err := db.ExecContext(ctx, buf.String()); err != nil {
			return fmt.Errorf("adding ledger to bucket: %w", err)
		}
	}
	return nil
}

func (b *SQLite) IsInitialized(ctx context.Context, db bun.IDB) (bool, error) {
	if err := b.dialect.OpenBucket(ctx, db, b.name); err != nil {
		return false, err
	}
	count, err := db.NewSelect().
		TableExpr(fmt.Sprintf(`"%s".sqlite_schema`, b.name)).
		Where("type = ? and name = ?", "table", "transactions").
		Count(ctx)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

// The schema is stated, not migrated, so it is current by construction.
func (b *SQLite) IsUpToDate(ctx context.Context, db bun.IDB) (bool, error) {
	return b.IsInitialized(ctx, db)
}

func (b *SQLite) HasMinimalVersion(ctx context.Context, db bun.IDB) (bool, error) {
	return b.IsInitialized(ctx, db)
}

func (b *SQLite) GetLastVersion(context.Context, bun.IDB) (int, error) {
	return MinimalSchemaVersion, nil
}

func (b *SQLite) GetMigrationsInfo(context.Context, bun.IDB) ([]migrations.Info, error) {
	return []migrations.Info{{
		Version: fmt.Sprint(MinimalSchemaVersion),
		Name:    "Bucket schema",
		State:   "applied",
	}}, nil
}

var _ Bucket = (*SQLite)(nil)

type sqliteFactory struct {
	dialect *dialect.SQLite
}

// NewSQLiteFactory builds buckets on SQLite.
func NewSQLiteFactory(d *dialect.SQLite) Factory {
	return &sqliteFactory{dialect: d}
}

func (f *sqliteFactory) Create(name string) Bucket {
	return NewSQLite(f.dialect, name)
}

// GetMigrator has no meaning without a recorded history; the driver only uses
// it to look for a rollback, and a stated schema cannot have one.
func (f *sqliteFactory) GetMigrator(string, bun.IDB) *migrations.Migrator {
	return migrations.NewMigrator(nil)
}

type sqliteLedgerSetup struct {
	requireFeatures features.FeatureSet
	script          string
}

// The per ledger triggers, gated on the same features as their Postgres
// counterparts. The log chain hash and the effective volumes are absent on
// purpose: one needs a digest and the other arithmetic wider than the engine's
// integers, so the store derives both in Go.
var sqliteLedgerSetups = []sqliteLedgerSetup{
	{
		requireFeatures: features.FeatureSet{features.FeatureTransactionMetadataHistory: "SYNC"},
		script: `
		create trigger if not exists "{{.Bucket}}"."insert_transaction_metadata_history_{{.ID}}"
		after insert on transactions
		when new.ledger = '{{.Name}}'
		begin
			insert into transactions_metadata (ledger, transactions_id, revision, date, metadata)
			values (new.ledger, new.id, 1, new.timestamp, new.metadata);
		end;`,
	},
	{
		requireFeatures: features.FeatureSet{features.FeatureTransactionMetadataHistory: "SYNC"},
		script: `
		create trigger if not exists "{{.Bucket}}"."update_transaction_metadata_history_{{.ID}}"
		after update on transactions
		when new.ledger = '{{.Name}}'
		begin
			insert into transactions_metadata (ledger, transactions_id, revision, date, metadata)
			select new.ledger, new.id, coalesce((
				select revision + 1
				from transactions_metadata
				where transactions_id = new.id and ledger = new.ledger
				order by revision desc
				limit 1
			), 1), new.updated_at, new.metadata;
		end;`,
	},
	{
		requireFeatures: features.FeatureSet{features.FeatureAccountMetadataHistory: "SYNC"},
		script: `
		create trigger if not exists "{{.Bucket}}"."insert_account_metadata_history_{{.ID}}"
		after insert on accounts
		when new.ledger = '{{.Name}}'
		begin
			insert into accounts_metadata (ledger, accounts_address, revision, date, metadata)
			values (new.ledger, new.address, 1, new.insertion_date, new.metadata);
		end;`,
	},
	{
		requireFeatures: features.FeatureSet{features.FeatureAccountMetadataHistory: "SYNC"},
		script: `
		create trigger if not exists "{{.Bucket}}"."update_account_metadata_history_{{.ID}}"
		after update on accounts
		when new.ledger = '{{.Name}}'
		begin
			insert into accounts_metadata (ledger, accounts_address, revision, date, metadata)
			select new.ledger, new.address, coalesce((
				select revision + 1
				from accounts_metadata
				where accounts_address = new.address and ledger = new.ledger
				order by revision desc
				limit 1
			), 1), new.updated_at, new.metadata;
		end;`,
	},
}
