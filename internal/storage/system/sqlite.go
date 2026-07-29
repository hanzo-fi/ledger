package system

import (
	"context"
	_ "embed"
	"fmt"

	"github.com/hanzo-fi/ledger/internal/storage/dialect"
)

//go:embed sqlite.sql
var sqliteSchema string

// declare states the system schema. An engine that declares its schema has no
// history to replay, so this runs whole and is idempotent.
func (d *DefaultStore) declare(ctx context.Context) error {
	if err := d.dialect.OpenBucket(ctx, d.db, SchemaSystem); err != nil {
		return err
	}
	for _, statement := range dialect.Statements(sqliteSchema) {
		if _, err := d.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("declaring system schema: %w", err)
		}
	}
	return nil
}
