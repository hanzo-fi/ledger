package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"

	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/go-libs/v5/pkg/storage/postgres"
	. "github.com/formancehq/go-libs/v5/pkg/types/collections"
	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/tracing"
	"github.com/hanzo-fi/ledger/pkg/features"
)

var (
	balanceRegex = regexp.MustCompile(`balance\[(.*)]`)
)

func (store *Store) UpdateAccountsMetadata(ctx context.Context, m map[string]metadata.Metadata, at time.Time) error {
	_, err := tracing.TraceWithMetric(
		ctx,
		"UpdateAccountsMetadata",
		store.tracer,
		store.updateAccountsMetadataHistogram,
		tracing.NoResult(func(ctx context.Context) error {

			span := trace.SpanFromContext(ctx)
			span.SetAttributes(attribute.StringSlice("accounts", Keys(m)))

			type AccountWithLedger struct {
				ledger.Account `bun:",extend"`
				Ledger         string   `bun:"ledger,type:varchar"`
				AddressArray   []string `bun:"address_array,type:jsonb"`
			}

			accounts := make([]AccountWithLedger, 0)
			for account, accountMetadata := range m {
				accounts = append(accounts, AccountWithLedger{
					Ledger: store.ledger.Name,
					Account: ledger.Account{
						Address:       account,
						Metadata:      accountMetadata,
						FirstUsage:    at,
						InsertionDate: at,
						UpdatedAt:     at,
					},
					AddressArray: strings.Split(account, ":"),
				})
			}

			type affectedAccount struct {
				Address   string            `bun:"address"`
				UpdatedAt *time.Time        `bun:"updated_at"`
				Metadata  metadata.Metadata `bun:"metadata,type:jsonb"`
			}
			affected := make([]affectedAccount, 0, len(accounts))

			_, err := store.db.NewInsert().
				Model(&accounts).
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				On("conflict (ledger, address) do update").
				Set("metadata = accounts.metadata || excluded.metadata").
				Set("updated_at = excluded.updated_at").
				Set("first_usage = case when excluded.first_usage < accounts.first_usage then excluded.first_usage else accounts.first_usage end").
				Where("not accounts.metadata @> excluded.metadata").
				Returning("address, updated_at, metadata").
				Exec(ctx, &affected)
			if err != nil {
				return postgres.ResolveError(err)
			}

			// Every inserted or updated account would have fired the retired
			// {insert,update}_account_metadata_history plpgsql triggers. Mirror them in
			// Go with the effective stored date: when `at` is zero the accounts.updated_at
			// / insertion_date columns default to transaction_date() (identical values in
			// one statement), so the returned updated_at is exactly the date the triggers
			// copied — not the zero `at`.
			for _, a := range affected {
				var date time.Time
				if a.UpdatedAt != nil {
					date = *a.UpdatedAt
				}
				if err := store.appendAccountMetadataHistory(ctx, a.Address, date, a.Metadata); err != nil {
					return err
				}
			}

			span.SetAttributes(attribute.Int("upserted", len(affected)))

			return nil
		}),
	)
	return err
}

func (store *Store) DeleteAccountMetadata(ctx context.Context, account, key string) error {
	_, err := tracing.TraceWithMetric(
		ctx,
		"DeleteAccountMetadata",
		store.tracer,
		store.deleteAccountMetadataHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			type affectedAccount struct {
				UpdatedAt *time.Time        `bun:"updated_at"`
				Metadata  metadata.Metadata `bun:"metadata,type:jsonb"`
			}
			affected := make([]affectedAccount, 0, 1)

			_, err := store.db.NewUpdate().
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				Set("metadata = metadata - ?", key).
				Where("address = ?", account).
				Where("ledger = ?", store.ledger.Name).
				Returning("updated_at, metadata").
				Exec(ctx, &affected)
			if err != nil {
				return postgres.ResolveError(err)
			}

			// This update does not touch updated_at, so the retired
			// update_account_metadata_history trigger dated the new revision at the
			// account's existing updated_at (which may be NULL) — reproduce that from
			// the returned row.
			for _, a := range affected {
				var at time.Time
				if a.UpdatedAt != nil {
					at = *a.UpdatedAt
				}
				if err := store.appendAccountMetadataHistory(ctx, account, at, a.Metadata); err != nil {
					return err
				}
			}

			return nil
		}),
	)
	return err
}

// appendAccountMetadataHistory writes one accounts_metadata row mirroring the
// retired {insert,update}_account_metadata_history plpgsql triggers: the revision
// is max(revision)+1 for the (ledger, account) or 1 when none exists (matching the
// insert trigger's hardcoded 1 for a fresh account), computed inline in SQL so it
// stays atomic under the transaction row lock the caller already holds. No-op
// unless the feature is SYNC, matching the condition under which the triggers were
// created.
func (store *Store) appendAccountMetadataHistory(ctx context.Context, address string, at time.Time, m metadata.Metadata) error {
	if !store.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") {
		return nil
	}
	if m == nil {
		m = metadata.Metadata{}
	}
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	// A zero date maps to NULL, matching the `nullzero` date columns the triggers
	// copied from (new.insertion_date / new.updated_at) — the PIT reads filter on
	// `date <= ?`, so NULL must stay NULL.
	var date any
	if !at.IsZero() {
		date = at
	}
	table := store.GetPrefixedRelationName("accounts_metadata")
	_, err = store.db.NewRaw(
		"insert into "+table+" (ledger, accounts_address, revision, date, metadata) "+
			"values (?, ?, coalesce((select max(revision) + 1 from "+table+" where accounts_address = ? and ledger = ?), 1), ?, ?::jsonb)",
		store.ledger.Name, address, address, store.ledger.Name, date, string(data),
	).Exec(ctx)
	return postgres.ResolveError(err)
}

func (store *Store) UpsertAccounts(ctx context.Context, accounts ...ledger.AccountWithDefaultMetadata) error {
	return tracing.SkipResult(tracing.TraceWithMetric(
		ctx,
		"UpsertAccounts",
		store.tracer,
		store.upsertAccountsHistogram,
		tracing.NoResult(func(ctx context.Context) error {
			span := trace.SpanFromContext(ctx)
			span.SetAttributes(attribute.StringSlice("accounts", Map(accounts, func(a ledger.AccountWithDefaultMetadata) string {
				return a.Account.Address
			})))

			type account struct {
				*ledger.Account `bun:",extend"`
				AddressArray    []string          `bun:"address_array,type:jsonb"`
				DefaultMetadata metadata.Metadata `bun:"default_metadata,type:jsonb"`
				Index           int               `bun:"batch_index,type:jsonb"`
			}

			batchIdx := 0
			rows := Map(accounts, func(from ledger.AccountWithDefaultMetadata) account {
				idx := batchIdx
				batchIdx += 1
				if from.Metadata == nil {
					from.Metadata = metadata.Metadata{}
				}
				if from.DefaultMetadata == nil {
					from.DefaultMetadata = metadata.Metadata{}
				}
				return account{
					Account:         from.Account,
					AddressArray:    strings.Split(from.Address, ":"),
					DefaultMetadata: from.DefaultMetadata,
					Index:           idx,
				}
			})

			// Reproduce the retired {insert,update}_account_metadata_history triggers in
			// the same statement as the upsert. Postgres runs data-modifying CTEs exactly
			// once even when unreferenced, so these append the history rows atomically:
			// inserted accounts get revision 1 dated at insertion_date, updated accounts
			// get max(revision)+1 dated at updated_at — byte-identical to the trigger rows.
			// Emitted only when the feature is SYNC, matching when the triggers existed.
			accountMetadataHistoryCTEs := ""
			if store.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") {
				accountMetadataHistoryCTEs = `,
					inserted_history AS (
						INSERT INTO ?1.accounts_metadata (ledger, accounts_address, revision, date, metadata)
						SELECT ?2, ir.address, 1, ir.insertion_date, ir.metadata
						FROM inserted_rows ir
					),
					updated_history AS (
						INSERT INTO ?1.accounts_metadata (ledger, accounts_address, revision, date, metadata)
						SELECT ?2, ur.address,
							COALESCE((
								SELECT max(revision) + 1
								FROM ?1.accounts_metadata am
								WHERE am.accounts_address = ur.address AND am.ledger = ?2
							), 1),
							ur.updated_at, ur.metadata
						FROM updated_rows ur
					)`
			}

			var returnedRows []account
			err := store.db.NewRaw(`
				WITH
					data_batch (address, metadata, first_usage, insertion_date, updated_at, address_array, default_metadata, batch_index)
						AS (?0),
					existing_accounts AS (
						SELECT a.address
						FROM ?1.accounts a
						JOIN data_batch d
							ON a.address = d.address
							AND a.ledger = ?2
					),
					updated_rows AS (
						-- If present: update
						UPDATE ?1.accounts a
						SET
							metadata = a.metadata || d.metadata,
							first_usage = LEAST(d.first_usage, a.first_usage),
							updated_at = COALESCE(d.updated_at, ?1.transaction_date())
						FROM data_batch d
						WHERE a.address = d.address and ledger = ?2 and (d.first_usage < a.first_usage or not a.metadata @> d.metadata)
						RETURNING a.address, a.metadata, a.first_usage, a.updated_at, a.insertion_date, d.batch_index
					),
					inserted_rows AS (
						-- If not present: insert
						INSERT INTO ?1.accounts (address, metadata, first_usage, updated_at, insertion_date, ledger, address_array)
						SELECT
							d.address,
							d.default_metadata || d.metadata,
							COALESCE(d.first_usage, ?1.transaction_date()),
							COALESCE(d.updated_at, ?1.transaction_date()),
							COALESCE(d.insertion_date, ?1.transaction_date()),
							?2,
							d.address_array
						FROM data_batch d
						WHERE d.address NOT IN (SELECT address FROM existing_accounts)
						RETURNING address, metadata, first_usage, updated_at, insertion_date,
							(SELECT batch_index FROM data_batch WHERE address = ?1.accounts.address)
					)`+accountMetadataHistoryCTEs+`
				SELECT * FROM updated_rows
				UNION ALL SELECT * FROM inserted_rows`,
				store.db.NewValues(&rows),
				bun.Ident(store.ledger.Bucket),
				store.ledger.Name,
			).Scan(ctx, &returnedRows)
			if err != nil {
				return fmt.Errorf("upserting accounts: %w", postgres.ResolveError(err))
			}

			for _, row := range returnedRows {
				rows[row.Index].Metadata = row.Metadata
				rows[row.Index].FirstUsage = row.FirstUsage
				rows[row.Index].InsertionDate = row.InsertionDate
				rows[row.Index].UpdatedAt = row.UpdatedAt
			}

			span.SetAttributes(attribute.Int("upserted", len(returnedRows)))

			return nil
		}),
	))
}
