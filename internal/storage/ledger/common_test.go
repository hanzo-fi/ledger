//go:build it

package ledger_test

import (
	"context"

	ledger "github.com/hanzo-fi/ledger/internal"
	ledgerstore "github.com/hanzo-fi/ledger/internal/storage/ledger"
)

func commitTransactionAndUpsertAccounts(ctx context.Context, store *ledgerstore.Store, tx *ledger.Transaction) error {
	err := store.CommitTransaction(ctx, tx)
	if err != nil {
		return err
	}
	accounts := tx.AccountsWithDefaultMetadata(nil, nil)
	return store.UpsertAccounts(ctx, accounts...)
}
