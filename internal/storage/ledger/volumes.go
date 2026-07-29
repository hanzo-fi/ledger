package ledger

import (
	"context"
	"math/big"
	"slices"
	"strings"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/tracing"
)

// accountsVolumes is a row of the running volumes relation: what one account
// has taken in and paid out, in one ledger and asset.
type accountsVolumes struct {
	ledger.AccountsVolumes `bun:",extend"`
	Ledger                 string `bun:"ledger,type:varchar"`
}

// holdVolumes takes the rows for the accounts and assets named and reads what
// stands on them, held against every other writer until the surrounding
// transaction ends.
//
// An account standing at zero is written a row first, so it is a row like any
// other and takes the same hold; the rows are taken in one order, so two
// writers meeting on the same accounts queue rather than deadlock. Every
// account and asset asked for comes back, at zero if it has never moved.
func (store *Store) holdVolumes(ctx context.Context, keys []ledger.AccountsVolumes) (ledger.PostCommitVolumes, error) {
	held := ledger.PostCommitVolumes{}
	if len(keys) == 0 {
		return held, nil
	}

	rows := make([]accountsVolumes, 0, len(keys))
	conditions := make([]string, 0, len(keys))
	args := make([]any, 0, len(keys)*3)
	for _, key := range keys {
		rows = append(rows, accountsVolumes{
			Ledger: store.ledger.Name,
			AccountsVolumes: ledger.AccountsVolumes{
				Account: key.Account,
				Asset:   key.Asset,
				Input:   new(big.Int),
				Output:  new(big.Int),
			},
		})
		conditions = append(conditions, "ledger = ? and accounts_address = ? and asset = ?")
		args = append(args, store.ledger.Name, key.Account, key.Asset)

		if _, ok := held[key.Account]; !ok {
			held[key.Account] = ledger.VolumesByAssets{}
		}
		held[key.Account][key.Asset] = ledger.NewEmptyVolumes()
	}

	slices.SortStableFunc(rows, func(i, j accountsVolumes) int {
		if order := strings.Compare(i.Account, j.Account); order != 0 {
			return order
		}
		return strings.Compare(i.Asset, j.Asset)
	})

	if _, err := store.db.NewInsert().
		Model(&rows).
		ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
		On("conflict do nothing").
		Exec(ctx); err != nil {
		return nil, store.dialect.ResolveError(err)
	}

	if err := store.db.NewSelect().
		Model(&rows).
		ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
		Column("accounts_address", "asset", "input", "output").
		Where("("+strings.Join(conditions, ") OR (")+")", args...).
		Apply(store.dialect.LockRows).
		// Read in the order the rows were written in, for the same reason.
		Order("accounts_address", "asset").
		Scan(ctx); err != nil {
		return nil, store.dialect.ResolveError(err)
	}

	for _, row := range rows {
		if assets, ok := held[row.Account]; ok {
			assets[row.Asset] = ledger.Volumes{Input: row.Input, Output: row.Output}
		}
	}

	return held, nil
}

// UpdateVolumes adds what the postings moved to what each account already
// stands at, and answers with what it stands at now.
//
// The addition is Go's, over big.Int. A ledger's volumes are arbitrary
// precision integers and an engine adds what its own integers hold - one
// carries 64 bit integers and rounds past them, the other a numeric - so an
// engine is asked to store a total, never to reach one.
func (store *Store) UpdateVolumes(ctx context.Context, additions ...ledger.AccountsVolumes) (ledger.PostCommitVolumes, error) {
	return tracing.TraceWithMetric(
		ctx,
		"UpdateBalances",
		store.tracer,
		store.updateBalancesHistogram,
		func(ctx context.Context) (ledger.PostCommitVolumes, error) {

			held, err := store.holdVolumes(ctx, additions)
			if err != nil {
				return nil, err
			}

			standing := ledger.PostCommitVolumes{}
			rows := make([]accountsVolumes, 0, len(additions))
			for _, addition := range additions {
				was := held[addition.Account][addition.Asset]
				volumes := ledger.Volumes{
					Input:  new(big.Int).Add(was.Input, addition.Input),
					Output: new(big.Int).Add(was.Output, addition.Output),
				}

				if _, ok := standing[addition.Account]; !ok {
					standing[addition.Account] = ledger.VolumesByAssets{}
				}
				standing[addition.Account][addition.Asset] = volumes

				rows = append(rows, accountsVolumes{
					Ledger: store.ledger.Name,
					AccountsVolumes: ledger.AccountsVolumes{
						Account: addition.Account,
						Asset:   addition.Asset,
						Input:   volumes.Input,
						Output:  volumes.Output,
					},
				})
			}
			if len(rows) == 0 {
				return standing, nil
			}

			if _, err := store.db.NewInsert().
				Model(&rows).
				ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
				On("conflict (ledger, accounts_address, asset) do update").
				Set("input = excluded.input").
				Set("output = excluded.output").
				Exec(ctx); err != nil {
				return nil, store.dialect.ResolveError(err)
			}

			return standing, nil
		},
	)
}
