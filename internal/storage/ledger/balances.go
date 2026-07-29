package ledger

import (
	"context"
	"math/big"
	"slices"
	"strings"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/tracing"
)

func (store *Store) GetBalances(ctx context.Context, query BalanceQuery) (ledger.Balances, error) {
	return tracing.TraceWithMetric(
		ctx,
		"GetBalances",
		store.tracer,
		store.getBalancesHistogram,
		func(ctx context.Context) (ledger.Balances, error) {
			conditions := make([]string, 0)
			args := make([]any, 0)
			for account, assets := range query {
				for _, asset := range assets {
					conditions = append(conditions, "ledger = ? and accounts_address = ? and asset = ?")
					args = append(args, store.ledger.Name, account, asset)
				}
			}

			type AccountsVolumesWithLedger struct {
				ledger.AccountsVolumes `bun:",extend"`
				Ledger                 string `bun:"ledger,type:varchar"`
			}

			accountsVolumes := make([]AccountsVolumesWithLedger, 0)
			for account, assets := range query {
				for _, asset := range assets {
					accountsVolumes = append(accountsVolumes, AccountsVolumesWithLedger{
						Ledger: store.ledger.Name,
						AccountsVolumes: ledger.AccountsVolumes{
							Account: account,
							Asset:   asset,
							Input:   new(big.Int),
							Output:  new(big.Int),
						},
					})
				}
			}

			// prevent deadlocks by sorting the accountsVolumes slice
			slices.SortStableFunc(accountsVolumes, func(i, j AccountsVolumesWithLedger) int {
				if i.Account < j.Account {
					return -1
				} else if i.Account > j.Account {
					return 1
				} else if i.Asset < j.Asset {
					return -1
				} else if i.Asset > j.Asset {
					return 1
				} else {
					return 0
				}
			})

			// Give every queried account and asset a row before reading, so an
			// account standing at zero locks like any other. A rolled back
			// transaction takes these rows with it.
			if _, err := store.db.NewInsert().
				Model(&accountsVolumes).
				ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
				On("conflict do nothing").
				Exec(ctx); err != nil {
				return nil, store.dialect.ResolveError(err)
			}

			err := store.db.NewSelect().
				Model(&accountsVolumes).
				ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
				Column("accounts_address", "asset", "input", "output").
				Where("("+strings.Join(conditions, ") OR (")+")", args...).
				Apply(store.dialect.LockRows).
				// notes(gfyrag): Keep order, it ensures consistent locking order and limit deadlocks
				Order("accounts_address", "asset").
				Scan(ctx)
			if err != nil {
				return nil, store.dialect.ResolveError(err)
			}

			ret := ledger.Balances{}
			for _, volumes := range accountsVolumes {
				if _, ok := ret[volumes.Account]; !ok {
					ret[volumes.Account] = map[string]*big.Int{}
				}
				ret[volumes.Account][volumes.Asset] = new(big.Int).Sub(volumes.Input, volumes.Output)
			}

			// Fill empty balances with 0 value
			for account, assets := range query {
				if _, ok := ret[account]; !ok {
					ret[account] = map[string]*big.Int{}
				}
				for _, asset := range assets {
					if _, ok := ret[account][asset]; !ok {
						ret[account][asset] = big.NewInt(0)
					}
				}
			}

			return ret, nil
		},
	)
}
