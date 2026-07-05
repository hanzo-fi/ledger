package ledgercore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/hanzo-fi/ledger/internal"
)

// CommitTransaction is the ported insert_transaction/insert_posting/insert_move
// plpgsql spine, in Go: allocate the transaction id, upsert involved accounts,
// derive each move's running post-commit volumes, persist the transaction, and
// append a chained NEW_TRANSACTION log. tx is mutated in place (ID, InsertedAt,
// PostCommitVolumes).
func (s *Store) CommitTransaction(ctx context.Context, tx *ledger.Transaction, accountMetadata ledger.AccountMetadata) error {
	now := time.Now()
	if tx.Timestamp.IsZero() {
		tx.Timestamp = now
	}
	if tx.InsertedAt.IsZero() {
		tx.InsertedAt = now
	}
	if tx.Metadata == nil {
		tx.Metadata = metadata.Metadata{}
	}

	id, err := s.nextID(ctx, "transaction")
	if err != nil {
		return err
	}
	tx.ID = pointer.For(id)

	pcv, err := s.applyPostings(ctx, id, tx.Postings, tx.InsertedAt, tx.Timestamp)
	if err != nil {
		return err
	}
	tx.PostCommitVolumes = pcv

	if err := s.insertTransactionRow(ctx, tx); err != nil {
		return err
	}

	return s.appendLog(ctx, ledger.CreatedTransaction{
		Transaction:     *tx,
		AccountMetadata: accountMetadata,
	}, tx.Timestamp)
}

// RevertTransaction marks transaction id reverted and posts a mirror transaction
// (reversed postings) that restores balances, then appends a chained
// REVERTED_TRANSACTION log. This is handle_log's REVERTED_TRANSACTION branch +
// revert_transaction, in Go. It returns the reverting transaction.
func (s *Store) RevertTransaction(ctx context.Context, id uint64, at time.Time) (*ledger.Transaction, error) {
	original, err := s.getTransaction(ctx, id)
	if err != nil {
		return nil, err
	}
	if original.RevertedAt != nil && !original.RevertedAt.IsZero() {
		return nil, fmt.Errorf("transaction %d already reverted", id)
	}
	if at.IsZero() {
		at = time.Now()
	}

	// The already-reverted guard is the load-check above; under the single-writer
	// per-tenant-file model (and Postgres transactions) that check + this update
	// are serialized, so an in-SQL `reverted_at is null` predicate is redundant.
	res, err := s.db.NewUpdate().
		Model((*transactionRow)(nil)).
		Set("reverted_at = ?", at).
		Set("updated_at = ?", at).
		Where("ledger = ?", s.ledger).
		Where("id = ?", id).
		Exec(ctx)
	if err != nil {
		return nil, fmt.Errorf("marking transaction %d reverted: %w", id, err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return nil, fmt.Errorf("transaction %d not found", id)
	}
	original.RevertedAt = pointer.For(at)

	reversal := ledger.NewTransaction().
		WithPostings(original.Postings.Reverse()...).
		WithTimestamp(at).
		WithInsertedAt(at)

	rid, err := s.nextID(ctx, "transaction")
	if err != nil {
		return nil, err
	}
	reversal.ID = pointer.For(rid)

	pcv, err := s.applyPostings(ctx, rid, reversal.Postings, at, at)
	if err != nil {
		return nil, err
	}
	reversal.PostCommitVolumes = pcv

	if err := s.insertTransactionRow(ctx, &reversal); err != nil {
		return nil, err
	}

	if err := s.appendLog(ctx, ledger.RevertedTransaction{
		RevertedTransaction: *original,
		RevertTransaction:   reversal,
	}, at); err != nil {
		return nil, err
	}

	return &reversal, nil
}

// applyPostings upserts accounts and inserts the source/destination moves for
// each posting (in order, source move then destination move — matching plpgsql
// insert_posting), carrying running post-commit volumes seeded from the last
// move per (account, asset). It returns the final volumes per account/asset.
func (s *Store) applyPostings(
	ctx context.Context,
	txID uint64,
	postings ledger.Postings,
	insertionDate, effectiveDate time.Time,
) (ledger.PostCommitVolumes, error) {
	running := ledger.PostCommitVolumes{}
	moves := make([]*moveRow, 0, len(postings)*2)

	step := func(account, asset string, amount *big.Int, isSource bool) (ledger.Volumes, error) {
		cur, err := s.cumulative(ctx, running, account, asset)
		if err != nil {
			return ledger.Volumes{}, err
		}
		if isSource {
			cur.Output.Add(cur.Output, amount)
		} else {
			cur.Input.Add(cur.Input, amount)
		}
		if running[account] == nil {
			running[account] = ledger.VolumesByAssets{}
		}
		running[account][asset] = cur
		return cur, nil
	}

	for _, p := range postings {
		if err := s.upsertAccount(ctx, p.Source, insertionDate); err != nil {
			return nil, err
		}
		if err := s.upsertAccount(ctx, p.Destination, insertionDate); err != nil {
			return nil, err
		}

		sv, err := step(p.Source, p.Asset, p.Amount, true)
		if err != nil {
			return nil, err
		}
		moves = append(moves, s.buildMove(txID, p.Source, p.Asset, p.Amount, true, insertionDate, effectiveDate, sv))

		dv, err := step(p.Destination, p.Asset, p.Amount, false)
		if err != nil {
			return nil, err
		}
		moves = append(moves, s.buildMove(txID, p.Destination, p.Asset, p.Amount, false, insertionDate, effectiveDate, dv))
	}

	if len(moves) > 0 {
		if _, err := s.db.NewInsert().Model(&moves).Exec(ctx); err != nil {
			return nil, fmt.Errorf("inserting moves: %w", err)
		}
	}
	return running, nil
}

// cumulative returns the running volumes for (account, asset) as a fresh,
// mutable Volumes: the in-flight value if this transaction already touched it,
// otherwise the last persisted move's volumes (or zero).
func (s *Store) cumulative(ctx context.Context, running ledger.PostCommitVolumes, account, asset string) (ledger.Volumes, error) {
	if v, ok := running[account][asset]; ok {
		return v.Copy(), nil
	}
	in, out, err := s.lastVolumes(ctx, account, asset)
	if err != nil {
		return ledger.Volumes{}, err
	}
	return ledger.Volumes{Input: in, Output: out}, nil
}

func (s *Store) buildMove(txID uint64, account, asset string, amount *big.Int, isSource bool, insertion, effective time.Time, pcv ledger.Volumes) *moveRow {
	return &moveRow{
		Ledger:        s.ledger,
		TransactionID: txID,
		Account:       account,
		Asset:         asset,
		Amount:        amount.String(),
		IsSource:      isSource,
		InsertionDate: insertion,
		EffectiveDate: effective,
		PostCommitIn:  pcv.Input.String(),
		PostCommitOut: pcv.Output.String(),
	}
}

func (s *Store) upsertAccount(ctx context.Context, address string, date time.Time) error {
	_, err := s.db.NewInsert().
		Model(&accountRow{
			Ledger:        s.ledger,
			Address:       address,
			InsertionDate: date,
			UpdatedAt:     date,
			Metadata:      "{}",
		}).
		On("CONFLICT (ledger, address) DO NOTHING").
		Exec(ctx)
	if err != nil {
		return fmt.Errorf("upserting account %q: %w", address, err)
	}
	return nil
}

func (s *Store) insertTransactionRow(ctx context.Context, tx *ledger.Transaction) error {
	postingsJSON, err := json.Marshal(tx.Postings)
	if err != nil {
		return fmt.Errorf("marshaling postings: %w", err)
	}
	metaJSON, err := json.Marshal(tx.Metadata)
	if err != nil {
		return fmt.Errorf("marshaling metadata: %w", err)
	}
	row := &transactionRow{
		Ledger:     s.ledger,
		ID:         *tx.ID,
		Timestamp:  tx.Timestamp,
		Reference:  tx.Reference,
		Postings:   string(postingsJSON),
		Metadata:   string(metaJSON),
		InsertedAt: tx.InsertedAt,
		UpdatedAt:  tx.InsertedAt,
	}
	if tx.RevertedAt != nil {
		row.RevertedAt = *tx.RevertedAt
	}
	if _, err := s.db.NewInsert().Model(row).Exec(ctx); err != nil {
		return fmt.Errorf("inserting transaction %d: %w", *tx.ID, err)
	}
	return nil
}

// appendLog chains payload onto the ledger's log head and persists it. The hash
// is computed by the canonical ledger.Log.ChainLog (identical Go on both
// dialects), so the hash chain is byte-identical on SQLite and Postgres.
func (s *Store) appendLog(ctx context.Context, payload ledger.LogPayload, date time.Time) error {
	prev, err := s.lastLog(ctx)
	if err != nil {
		return err
	}
	chained := ledger.NewLog(payload).WithDate(date).ChainLog(prev)

	dataJSON, err := json.Marshal(chained.Data)
	if err != nil {
		return fmt.Errorf("marshaling log data: %w", err)
	}
	memento := dataJSON
	if m, ok := chained.Data.(ledger.Memento); ok {
		if memento, err = json.Marshal(m.GetMemento()); err != nil {
			return fmt.Errorf("marshaling log memento: %w", err)
		}
	}

	row := &logRow{
		Ledger:  s.ledger,
		ID:      *chained.ID,
		Type:    chained.Type.String(),
		Hash:    chained.Hash,
		Date:    chained.Date,
		Data:    string(dataJSON),
		Memento: memento,
	}
	if _, err := s.db.NewInsert().Model(row).Exec(ctx); err != nil {
		return fmt.Errorf("inserting log %d: %w", *chained.ID, err)
	}
	return nil
}

func (s *Store) lastLog(ctx context.Context) (*ledger.Log, error) {
	row := new(logRow)
	err := s.db.NewSelect().
		Model(row).
		Column("id", "hash").
		Where("ledger = ?", s.ledger).
		Order("id DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading log head: %w", err)
	}
	return &ledger.Log{ID: pointer.For(row.ID), Hash: row.Hash}, nil
}

func (s *Store) lastVolumes(ctx context.Context, account, asset string) (*big.Int, *big.Int, error) {
	row := new(moveRow)
	err := s.db.NewSelect().
		Model(row).
		Column("post_commit_inputs", "post_commit_outputs").
		Where("ledger = ?", s.ledger).
		Where("account_address = ?", account).
		Where("asset = ?", asset).
		Order("seq DESC").
		Limit(1).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return big.NewInt(0), big.NewInt(0), nil
		}
		return nil, nil, fmt.Errorf("reading volumes for %s/%s: %w", account, asset, err)
	}
	in, err := parseBig(row.PostCommitIn)
	if err != nil {
		return nil, nil, err
	}
	out, err := parseBig(row.PostCommitOut)
	if err != nil {
		return nil, nil, err
	}
	return in, out, nil
}

func (s *Store) getTransaction(ctx context.Context, id uint64) (*ledger.Transaction, error) {
	row := new(transactionRow)
	err := s.db.NewSelect().
		Model(row).
		Where("ledger = ?", s.ledger).
		Where("id = ?", id).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("transaction %d not found", id)
		}
		return nil, fmt.Errorf("reading transaction %d: %w", id, err)
	}

	tx := ledger.NewTransaction()
	tx.ID = pointer.For(row.ID)
	tx.Timestamp = row.Timestamp
	tx.Reference = row.Reference
	tx.InsertedAt = row.InsertedAt
	tx.UpdatedAt = row.UpdatedAt
	if err := json.Unmarshal([]byte(row.Postings), &tx.Postings); err != nil {
		return nil, fmt.Errorf("unmarshaling postings for transaction %d: %w", id, err)
	}
	if row.Metadata != "" {
		if err := json.Unmarshal([]byte(row.Metadata), &tx.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshaling metadata for transaction %d: %w", id, err)
		}
	}
	if !row.RevertedAt.IsZero() {
		tx.RevertedAt = pointer.For(row.RevertedAt)
	}
	return &tx, nil
}

func parseBig(s string) (*big.Int, error) {
	v, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("invalid numeric %q", s)
	}
	return v, nil
}
