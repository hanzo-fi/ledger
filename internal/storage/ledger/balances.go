package ledger

import (
	"context"
	"math/big"

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
			keys := make([]ledger.AccountsVolumes, 0, len(query))
			for account, assets := range query {
				for _, asset := range assets {
					keys = append(keys, ledger.AccountsVolumes{Account: account, Asset: asset})
				}
			}

			held, err := store.holdVolumes(ctx, keys)
			if err != nil {
				return nil, err
			}

			ret := ledger.Balances{}
			for account := range query {
				ret[account] = map[string]*big.Int{}
			}
			for account, assets := range held {
				for asset, volumes := range assets {
					ret[account][asset] = volumes.Balance()
				}
			}

			return ret, nil
		},
	)
}
