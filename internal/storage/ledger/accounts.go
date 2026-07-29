package ledger

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"strings"

	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	. "github.com/hanzo-fi/go-libs/v5/pkg/types/collections"
	"github.com/hanzo-fi/go-libs/v5/pkg/types/metadata"
	"github.com/hanzo-fi/go-libs/v5/pkg/types/time"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/storage/dialect"
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

			// first_usage/insertion_date/updated_at previously defaulted to
			// transaction_date() when the caller passed a zero time; stamp the shared
			// per-tx date in Go now that the default is retired, so both the insert and
			// the `updated_at = excluded.updated_at` conflict path store that instant.
			if at.IsZero() {
				at = store.transactionDate()
			}

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
				Metadata  metadata.Metadata `bun:"metadata"`
			}
			affected := make([]affectedAccount, 0, len(accounts))

			merged := store.dialect.Merge("accounts.metadata", "excluded.metadata")
			_, err := store.db.NewInsert().
				Model(&accounts).
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				On("conflict (ledger, address) do update").
				Set("metadata = "+merged).
				Set("updated_at = excluded.updated_at").
				Set("first_usage = case when excluded.first_usage < accounts.first_usage then excluded.first_usage else accounts.first_usage end").
				// The merge changes nothing when the new metadata is already
				// held, which is when the trigger did not fire either.
				Where(merged+" <> "+store.dialect.Canonical("accounts.metadata")).
				Returning("address, updated_at, metadata").
				Exec(ctx, &affected)
			if err != nil {
				return store.dialect.ResolveError(err)
			}

			// Every inserted or updated account would have fired the retired
			// {insert,update}_account_metadata_history plpgsql triggers. Mirror them in
			// Go with the effective stored date: the returned updated_at (the per-tx date
			// stamped above when `at` was zero) is exactly the date the triggers copied.
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
				Metadata  metadata.Metadata `bun:"metadata"`
			}
			affected := make([]affectedAccount, 0, 1)

			remove := store.dialect.Remove("metadata", key)
			_, err := store.db.NewUpdate().
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				Set("metadata = "+remove.SQL, remove.Args...).
				Where("address = ?", account).
				Where("ledger = ?", store.ledger.Name).
				Returning("updated_at, metadata").
				Exec(ctx, &affected)
			if err != nil {
				return store.dialect.ResolveError(err)
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
	// A zero date maps to NULL, matching the `nullzero` date columns the triggers
	// copied from (new.insertion_date / new.updated_at) — the PIT reads filter on
	// `date <= ?`, so NULL must stay NULL.
	var date any
	if !at.IsZero() {
		date = at
	}
	table := store.GetPrefixedRelationName("accounts_metadata")
	_, err := store.db.NewRaw(
		"insert into "+table+" (ledger, accounts_address, revision, date, metadata) "+
			"values (?, ?, coalesce((select max(revision) + 1 from "+table+" where accounts_address = ? and ledger = ?), 1), ?, ?)",
		store.ledger.Name, address, address, store.ledger.Name, date, m,
	).Exec(ctx)
	return store.dialect.ResolveError(err)
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

			type row struct {
				*ledger.Account `bun:",extend"`
				Ledger          string   `bun:"ledger,type:varchar"`
				AddressArray    []string `bun:"address_array,type:jsonb"`
			}

			// The default metadata is what an account starts with; explicit
			// metadata wins over it. Merging here keeps the statement one
			// upsert on either engine.
			rows := Map(accounts, func(from ledger.AccountWithDefaultMetadata) row {
				merged := metadata.Metadata{}
				for k, v := range from.DefaultMetadata {
					merged[k] = v
				}
				for k, v := range from.Metadata {
					merged[k] = v
				}
				account := *from.Account
				account.Metadata = merged
				return row{
					Account:      &account,
					Ledger:       store.ledger.Name,
					AddressArray: strings.Split(from.Address, ":"),
				}
			})

			// The metadata history the retired {insert,update}_account_metadata_history
			// triggers kept is appended in Go, so it needs the rows as they stood
			// before the upsert: a new account opens its history, and an existing one
			// extends it only when the upsert actually moved it.
			prior, err := store.accountsBefore(ctx, Map(rows, func(r row) string { return r.Address }))
			if err != nil {
				return err
			}

			merged := store.dialect.Merge("accounts.metadata", "excluded.metadata")
			earlier := "excluded.first_usage < accounts.first_usage"
			// Nothing observable changed when the merge is a no-op and the
			// first usage did not move back, so updated_at holds still.
			unchanged := "(" + merged + " = " + store.dialect.Canonical("accounts.metadata") + " and not (" + earlier + "))"

			returned := make([]row, 0, len(rows))
			err = store.db.NewInsert().
				Model(&rows).
				ModelTableExpr(store.GetPrefixedRelationName("accounts")).
				On("conflict (ledger, address) do update").
				Set("metadata = "+merged).
				Set("first_usage = case when "+earlier+" then excluded.first_usage else accounts.first_usage end").
				Set("updated_at = case when "+unchanged+" then accounts.updated_at else excluded.updated_at end").
				Returning("address, metadata, first_usage, insertion_date, updated_at").
				Scan(ctx, &returned)
			if err != nil {
				return fmt.Errorf("upserting accounts: %w", store.dialect.ResolveError(err))
			}

			stored := make(map[string]row, len(returned))
			for _, r := range returned {
				stored[r.Address] = r
			}
			for _, account := range accounts {
				r, ok := stored[account.Address]
				if !ok {
					continue
				}
				account.Account.Metadata = r.Metadata
				account.Account.FirstUsage = r.FirstUsage
				account.Account.InsertionDate = r.InsertionDate
				account.Account.UpdatedAt = r.UpdatedAt
			}

			for _, r := range returned {
				before, existed := prior[r.Address]
				switch {
				case !existed:
					// insert_account_metadata_history: revision 1, dated at insertion.
					if err := store.appendAccountMetadataHistory(ctx, r.Address, r.InsertionDate, r.Metadata); err != nil {
						return err
					}
				case moved(before, *r.Account):
					// update_account_metadata_history: the next revision, dated at update.
					if err := store.appendAccountMetadataHistory(ctx, r.Address, r.UpdatedAt, r.Metadata); err != nil {
						return err
					}
				}
			}

			span.SetAttributes(attribute.Int("upserted", len(returned)))

			return nil
		}),
	))
}

// accountsBefore reads the named accounts as they stand before an upsert, which
// is what says whether the upsert went on to move them.
func (store *Store) accountsBefore(ctx context.Context, addresses []string) (map[string]ledger.Account, error) {
	before := make(map[string]ledger.Account, len(addresses))
	if len(addresses) == 0 || !store.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") {
		return before, nil
	}

	standing := make([]ledger.Account, 0, len(addresses))
	err := store.db.NewSelect().
		Model(&standing).
		ModelTableExpr(store.GetPrefixedRelationName("accounts")+" as account").
		Column("address", "metadata", "first_usage").
		Where("ledger = ?", store.ledger.Name).
		Where("address in (?)", bun.In(addresses)).
		Scan(ctx)
	if err != nil && !errors.Is(store.dialect.ResolveError(err), dialect.ErrNotFound) {
		return nil, fmt.Errorf("reading accounts: %w", store.dialect.ResolveError(err))
	}
	for _, account := range standing {
		before[account.Address] = account
	}

	return before, nil
}

// moved reports whether an upsert changed an account. The stored metadata is the
// merge of what was there with what arrived, and the stored first usage the
// earlier of the two, so either differing from what stood before is exactly the
// condition on which the retired update trigger fired.
func moved(before, after ledger.Account) bool {
	return !maps.Equal(before.Metadata, after.Metadata) ||
		!before.FirstUsage.Equal(after.FirstUsage)
}
