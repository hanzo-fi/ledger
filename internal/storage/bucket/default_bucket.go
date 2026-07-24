package bucket

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"text/template"

	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/migrations"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/pkg/features"
)

// stateless version (+1 regarding directory name, as migrations start from 1 in the lib)
const MinimalSchemaVersion = 50

type DefaultBucket struct {
	name string

	tracer trace.Tracer
}

func (b *DefaultBucket) IsInitialized(ctx context.Context, db bun.IDB) (bool, error) {
	_, err := GetMigrator(db, b.name).GetLastVersion(ctx)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, migrations.ErrMissingVersionTable) {
		return false, nil
	}
	return false, err
}

func (b *DefaultBucket) IsUpToDate(ctx context.Context, db bun.IDB) (bool, error) {
	return GetMigrator(db, b.name).IsUpToDate(ctx)
}

func (b *DefaultBucket) Migrate(ctx context.Context, db bun.IDB, options ...migrations.Option) error {
	return runMigrate(ctx, b.tracer, db, b.name, append(options, migrations.WithTracer(b.tracer))...)
}

func (b *DefaultBucket) HasMinimalVersion(ctx context.Context, db bun.IDB) (bool, error) {
	lastVersion, err := b.GetLastVersion(ctx, db)
	if err != nil {
		return false, err
	}

	return lastVersion >= MinimalSchemaVersion, nil
}

func (b *DefaultBucket) GetLastVersion(ctx context.Context, db bun.IDB) (int, error) {
	return GetMigrator(db, b.name).GetLastVersion(ctx)
}

func (b *DefaultBucket) GetMigrationsInfo(ctx context.Context, db bun.IDB) ([]migrations.Info, error) {
	return GetMigrator(db, b.name).GetMigrations(ctx)
}

func (b *DefaultBucket) AddLedger(ctx context.Context, db bun.IDB, l ledger.Ledger) error {

	for _, setup := range ledgerSetups {
		if l.Features.Match(setup.requireFeatures) {
			tpl := template.Must(template.New("sql").Parse(setup.script))
			buf := bytes.NewBuffer(nil)
			if err := tpl.Execute(buf, l); err != nil {
				return fmt.Errorf("executing template: %w", err)
			}

			_, err := db.ExecContext(ctx, buf.String())
			if err != nil {
				return fmt.Errorf("executing sql: %w", err)
			}
		}
	}

	return nil
}

func NewDefault(tracer trace.Tracer, name string) *DefaultBucket {
	return &DefaultBucket{

		name:   name,
		tracer: tracer,
	}
}

type ledgerSetup struct {
	requireFeatures features.FeatureSet
	script          string
}

// notes: Be careful if changing the order of the migration.
// The actions on tables are organized following the same order used when committing a new transaction.
// It prevents deadlocks.
var ledgerSetups = []ledgerSetup{
	{
		script: `
		-- create a sequence for transactions by ledger instead of a sequence of the table as we want to have contiguous ids
		-- notes: we can still have "holes" on ids since a sql transaction can be reverted after a usage of the sequence
		create sequence "{{.Bucket}}"."transaction_id_{{.ID}}" owned by "{{.Bucket}}".transactions.id;
		select setval('"{{.Bucket}}"."transaction_id_{{.ID}}"', coalesce((
			select max(id) + 1
			from "{{.Bucket}}"."transactions"
			where ledger = '{{ .Name }}'
		), 1)::bigint, false);
		`,
	},
	// Metadata history (Feature{Transaction,Account}MetadataHistory=SYNC) is now
	// appended in Go by the Store's metadata write paths (InsertTransaction,
	// updateTxWithRetrieve, UpdateAccountsMetadata, DeleteAccountMetadata,
	// UpsertAccounts), so no per-ledger {insert,update}_{transaction,account}_metadata_history
	// triggers are created. See migration 56-retire-metadata-history-plpgsql, which
	// drops the triggers + trigger functions on existing ledgers.
	// The effective-volume chain (FeatureMovesHistoryPostCommitEffectiveVolumes=SYNC)
	// is now computed in Go by Store.InsertMoves, so no per-ledger
	// set_effective_volumes/update_effective_volumes triggers are created. See
	// migration 58-retire-effective-volumes-plpgsql, which drops the triggers +
	// trigger functions on existing ledgers.
	{
		script: `
		-- create a sequence for logs by ledger instead of a sequence of the table as we want to have contiguous ids
		-- notes: we can still have "holes" on id since a sql transaction can be reverted after a usage of the sequence
		create sequence "{{.Bucket}}"."log_id_{{.ID}}" owned by "{{.Bucket}}".logs.id;
		select setval('"{{.Bucket}}"."log_id_{{.ID}}"', coalesce((
			select max(id) + 1
			from "{{.Bucket}}".logs
			where ledger = '{{ .Name }}'
		), 1)::bigint, false);
		`,
	},
	// The log hash chain (FeatureHashLogs=SYNC) is now computed in Go by
	// Store.InsertLog via the canonical ledger.Log.ComputeHash, so no per-ledger
	// set_log_hash trigger is created. See migration 54-retire-log-hash-plpgsql,
	// which drops the trigger + set_log_hash/compute_hash functions.
}
