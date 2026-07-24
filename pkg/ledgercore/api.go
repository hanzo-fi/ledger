package ledgercore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	stdtime "time"

	"github.com/hanzo-fi/go-libs/v5/pkg/types/metadata"
	ledgertime "github.com/hanzo-fi/go-libs/v5/pkg/types/time"

	ledger "github.com/hanzo-fi/ledger/internal"
)

// This file is the caller-facing surface of the engine — the API a service USES
// to keep books, expressed entirely in standard-library and go-libs value types
// (string, *big.Int, map, stdlib time). It deliberately exposes NO
// github.com/hanzo-fi/ledger/internal type, so an external module (e.g. the cloud
// treasury) can post and read against the ONE engine without reaching into the
// ledger's internals. The internal ledger.Transaction / ledger.Log stay an
// implementation detail behind this seam.

// Posting is one caller-facing balanced leg: value moves Source -> Destination in
// Asset by Amount (a positive magnitude — direction is carried by source vs
// destination, exactly like the ledger's own postings).
type Posting struct {
	Source      string
	Destination string
	Asset       string
	Amount      *big.Int
}

// PostParams carries the idempotency and provenance of a Post.
type PostParams struct {
	// IdempotencyKey, when non-empty, makes the post at-most-once: a repeated key
	// on the same ledger returns the already-committed record (Deduped) instead of
	// posting again. Enforced by the logs_ledger_ik unique partial index.
	IdempotencyKey string
	// Reference is an optional caller reference stored on the transaction.
	Reference string
	// Metadata is opaque caller key/values, round-tripped verbatim — where a caller
	// stashes its own domain object so ListPosts / PostByIdempotencyKey can rebuild
	// it without the engine knowing the caller's schema.
	Metadata map[string]string
	// Timestamp is the booking time; the zero value means "now".
	Timestamp stdtime.Time
}

// Record is a caller-facing view of one committed transaction — enough to
// reconstruct a caller's own domain object. TransactionID is the engine's
// monotonic per-ledger id; Deduped reports that a Post returned an existing entry
// under a repeated idempotency key rather than writing a new one.
type Record struct {
	TransactionID uint64
	Reference     string
	Metadata      map[string]string
	Postings      []Posting
	Timestamp     stdtime.Time
	Deduped       bool
}

// Post commits a balanced set of postings as one transaction and returns its
// Record. With a non-empty IdempotencyKey it is at-most-once: if a prior post
// used the same key on this ledger, the existing record is returned with
// Deduped=true and nothing new is written. Callers wanting a read-then-write
// guard (e.g. an overdraw check) run Post inside WithTx alongside the guard read.
func (s *Store) Post(ctx context.Context, postings []Posting, params PostParams) (Record, error) {
	if len(postings) == 0 {
		return Record{}, errors.New("ledgercore: Post requires at least one posting")
	}
	if params.IdempotencyKey != "" {
		if existing, ok, err := s.PostByIdempotencyKey(ctx, params.IdempotencyKey); err != nil {
			return Record{}, err
		} else if ok {
			existing.Deduped = true
			return existing, nil
		}
	}

	tx := ledger.NewTransaction().WithPostings(toLedgerPostings(postings)...)
	if params.Reference != "" {
		tx = tx.WithReference(params.Reference)
	}
	if len(params.Metadata) > 0 {
		tx = tx.WithMetadata(metadata.Metadata(params.Metadata))
	}
	if !params.Timestamp.IsZero() {
		tx = tx.WithTimestamp(ledgertime.New(params.Timestamp))
	}

	if err := s.commit(ctx, &tx, nil, params.IdempotencyKey); err != nil {
		return Record{}, err
	}
	rec := recordFromTransaction(tx)
	return rec, nil
}

// PostByIdempotencyKey returns the Record committed under key on this ledger, if
// any. It is the idempotency lookup a caller runs (typically inside WithTx) before
// deciding to post — the engine backing of an "entry by ref" check.
func (s *Store) PostByIdempotencyKey(ctx context.Context, key string) (Record, bool, error) {
	if key == "" {
		return Record{}, false, nil
	}
	row := new(logRow)
	err := s.db.NewSelect().
		Model(row).
		Where("ledger = ?", s.ledger).
		Where("idempotency_key = ?", key).
		Limit(1).
		Scan(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Record{}, false, nil
		}
		return Record{}, false, fmt.Errorf("reading log for idempotency key %q: %w", key, err)
	}
	payload, err := ledger.HydrateLog(ledger.LogTypeFromString(row.Type), []byte(row.Data))
	if err != nil {
		return Record{}, false, fmt.Errorf("hydrating log for idempotency key %q: %w", key, err)
	}
	created, ok := payload.(ledger.CreatedTransaction)
	if !ok {
		return Record{}, false, fmt.Errorf("log for idempotency key %q is not a transaction (%s)", key, row.Type)
	}
	return recordFromTransaction(created.Transaction), true, nil
}

// ListPosts returns the most recent committed transactions as Records (newest
// first). limit <= 0 returns every transaction — used to fold the whole journal
// (e.g. a caller computing a commitment over all entries).
func (s *Store) ListPosts(ctx context.Context, limit int) ([]Record, error) {
	q := s.db.NewSelect().
		Model((*transactionRow)(nil)).
		Where("ledger = ?", s.ledger).
		Order("id DESC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	var rows []transactionRow
	if err := q.Scan(ctx, &rows); err != nil {
		return nil, fmt.Errorf("listing transactions: %w", err)
	}
	out := make([]Record, 0, len(rows))
	for i := range rows {
		rec, err := recordFromTxRow(&rows[i])
		if err != nil {
			return nil, err
		}
		out = append(out, rec)
	}
	return out, nil
}

// PrefixBalances returns account -> balance for asset for every account whose
// address starts with prefix — the scope-aware read (a caller rolls up its own
// "payout:" or "org:<tenant>:" namespace). Balances come from each account's
// latest move, the same source as GetBalance.
func (s *Store) PrefixBalances(ctx context.Context, prefix, asset string) (map[string]*big.Int, error) {
	var rows []moveRow
	err := s.db.NewSelect().
		Model(&rows).
		Column("account_address", "post_commit_inputs", "post_commit_outputs").
		Where("ledger = ?", s.ledger).
		Where("asset = ?", asset).
		Where("account_address LIKE ?", prefix+"%").
		Where(`seq = (select max(m2.seq) from moves m2 ` +
			`where m2.ledger = moves.ledger and m2.account_address = moves.account_address and m2.asset = moves.asset)`).
		Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("prefix balances %q: %w", prefix, err)
	}
	out := make(map[string]*big.Int, len(rows))
	for _, r := range rows {
		in, err := parseBig(r.PostCommitIn)
		if err != nil {
			return nil, err
		}
		o, err := parseBig(r.PostCommitOut)
		if err != nil {
			return nil, err
		}
		out[r.Account] = new(big.Int).Sub(in, o)
	}
	return out, nil
}

func toLedgerPostings(ps []Posting) []ledger.Posting {
	out := make([]ledger.Posting, len(ps))
	for i, p := range ps {
		out[i] = ledger.NewPosting(p.Source, p.Destination, p.Asset, p.Amount)
	}
	return out
}

func fromLedgerPostings(ps ledger.Postings) []Posting {
	out := make([]Posting, len(ps))
	for i, p := range ps {
		out[i] = Posting{Source: p.Source, Destination: p.Destination, Asset: p.Asset, Amount: p.Amount}
	}
	return out
}

func recordFromTransaction(tx ledger.Transaction) Record {
	rec := Record{
		Reference: tx.Reference,
		Metadata:  map[string]string(tx.Metadata),
		Postings:  fromLedgerPostings(tx.Postings),
		Timestamp: tx.Timestamp.Time,
	}
	if tx.ID != nil {
		rec.TransactionID = *tx.ID
	}
	return rec
}

func recordFromTxRow(row *transactionRow) (Record, error) {
	rec := Record{
		TransactionID: row.ID,
		Reference:     row.Reference,
		Timestamp:     row.Timestamp.Time,
	}
	var postings ledger.Postings
	if err := json.Unmarshal([]byte(row.Postings), &postings); err != nil {
		return Record{}, fmt.Errorf("unmarshaling postings for transaction %d: %w", row.ID, err)
	}
	rec.Postings = fromLedgerPostings(postings)
	if row.Metadata != "" {
		m := map[string]string{}
		if err := json.Unmarshal([]byte(row.Metadata), &m); err != nil {
			return Record{}, fmt.Errorf("unmarshaling metadata for transaction %d: %w", row.ID, err)
		}
		if len(m) > 0 {
			rec.Metadata = m
		}
	}
	return rec, nil
}
