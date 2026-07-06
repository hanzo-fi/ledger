package ledgercore

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v5/pkg/types/pointer"

	ledger "github.com/hanzo-fi/ledger/internal"
)

// GetBalance is the ported get_account_balance: the balance (inputs - outputs)
// of the latest move for (account, asset), or zero if the account never moved.
func (s *Store) GetBalance(ctx context.Context, account, asset string) (*big.Int, error) {
	in, out, err := s.lastVolumes(ctx, account, asset)
	if err != nil {
		return nil, err
	}
	return new(big.Int).Sub(in, out), nil
}

// GetAggregatedBalances is the balance-aggregation read path: for every
// (account, asset) it returns the balance from that pair's latest move. One
// correlated-subquery scan, valid on both dialects.
func (s *Store) GetAggregatedBalances(ctx context.Context) (map[string]map[string]*big.Int, error) {
	var rows []moveRow
	err := s.db.NewSelect().
		Model(&rows).
		Column("account_address", "asset", "post_commit_inputs", "post_commit_outputs").
		Where("ledger = ?", s.ledger).
		Where(`seq = (select max(m2.seq) from moves m2 ` +
			`where m2.ledger = moves.ledger and m2.account_address = moves.account_address and m2.asset = moves.asset)`).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("aggregating balances: %w", err)
	}

	ret := map[string]map[string]*big.Int{}
	for _, r := range rows {
		in, err := parseBig(r.PostCommitIn)
		if err != nil {
			return nil, err
		}
		out, err := parseBig(r.PostCommitOut)
		if err != nil {
			return nil, err
		}
		if ret[r.Account] == nil {
			ret[r.Account] = map[string]*big.Int{}
		}
		ret[r.Account][r.Asset] = new(big.Int).Sub(in, out)
	}
	return ret, nil
}

// VerifyHashChain walks the ledger's logs in id order and recomputes each hash
// from its stored payload and its predecessor's hash, asserting the chain is
// internally consistent. This is the double-entry integrity gate — it fails if
// any log was tampered with or the chain was miscomputed.
func (s *Store) VerifyHashChain(ctx context.Context) error {
	var rows []logRow
	err := s.db.NewSelect().
		Model(&rows).
		Where("ledger = ?", s.ledger).
		Order("id ASC").
		Scan(ctx)
	if err != nil {
		return fmt.Errorf("reading logs: %w", err)
	}

	var prev *ledger.Log
	for _, r := range rows {
		payload, err := ledger.HydrateLog(ledger.LogTypeFromString(r.Type), []byte(r.Data))
		if err != nil {
			return fmt.Errorf("hydrating log %d: %w", r.ID, err)
		}
		recomputed := ledger.NewLog(payload).WithDate(r.Date).WithIdempotencyKey(r.IdempotencyKey).ChainLog(prev)
		if !bytes.Equal(recomputed.Hash, r.Hash) {
			return fmt.Errorf("hash chain broken at log %d: recomputed %x != stored %x", r.ID, recomputed.Hash, r.Hash)
		}
		if *recomputed.ID != r.ID {
			return fmt.Errorf("log id chain broken at log %d: recomputed %d", r.ID, *recomputed.ID)
		}
		prev = &ledger.Log{ID: pointer.For(r.ID), Hash: r.Hash}
	}
	return nil
}
