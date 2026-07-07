package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/storage/postgres"

	ledger "github.com/hanzo-fi/ledger/internal"
	storagecommon "github.com/hanzo-fi/ledger/internal/storage/common"
	systemstore "github.com/hanzo-fi/ledger/internal/storage/system"
	"github.com/hanzo-fi/ledger/pkg/features"
)

type AsyncBlockRunnerConfig struct {
	MaxBlockSize int
	Schedule     cron.Schedule
}

type AsyncBlockRunner struct {
	stopChannel chan chan struct{}
	logger      logging.Logger
	db          *bun.DB
	cfg         AsyncBlockRunnerConfig
	tracer      trace.Tracer
}

func (r *AsyncBlockRunner) Name() string {
	return "Async block hasher"
}

func (r *AsyncBlockRunner) Run(ctx context.Context) error {

	now := time.Now()
	next := r.cfg.Schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx); err != nil {
				r.logger.Errorf("error running block runner: %v", err)
			}

			now = time.Now()
			next = r.cfg.Schedule.Next(now).Sub(now)
		case ch := <-r.stopChannel:
			close(ch)
			return nil
		}
	}
}

func (r *AsyncBlockRunner) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.stopChannel <- ch:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
	return nil
}

func (r *AsyncBlockRunner) run(ctx context.Context) error {

	ctx, span := r.tracer.Start(ctx, "Run")
	defer span.End()

	initialQuery := storagecommon.InitialPaginatedQuery[systemstore.ListLedgersQueryPayload]{
		Options: storagecommon.ResourceQuery[systemstore.ListLedgersQueryPayload]{
			Builder: query.Match(fmt.Sprintf("features[%s]", features.FeatureHashLogs), "ASYNC"),
		},
	}
	systemStore := systemstore.New(r.db)
	return storagecommon.Iterate(
		ctx,
		initialQuery,
		systemStore.Ledgers().Paginate,
		func(cursor *paginate.Cursor[ledger.Ledger]) error {
			for _, l := range cursor.Data {
				if err := r.processLedger(ctx, l); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func (r *AsyncBlockRunner) processLedger(ctx context.Context, l ledger.Ledger) error {
	ctx, span := r.tracer.Start(ctx, "RunForLedger")
	defer span.End()

	span.SetAttributes(attribute.String("ledger", l.Name))

	// The log-block hash chain is computed in Go (crypto/sha256 over the same
	// canonical body the retired create_block/create_blocks plpgsql built) — the
	// async counterpart of the sync log-hash port. The whole per-ledger run is one
	// transaction, matching the atomicity of the former `call create_blocks(...)`.
	return r.db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
		previous, err := r.readPreviousBlock(ctx, tx, l)
		if err != nil {
			return err
		}
		for {
			next, err := r.createBlock(ctx, tx, l, previous)
			if err != nil {
				return err
			}
			if next.maxLogID == 0 {
				return nil
			}
			previous = next
		}
	})
}

// logBlock mirrors the retired plpgsql `block` composite type (max_log_id,
// block_id, hash) used to chain log blocks.
type logBlock struct {
	maxLogID int64
	blockID  int64
	hash     []byte
}

type blockLogRow struct {
	ID   int64  `bun:"id"`
	Part string `bun:"part"`
}

// readPreviousBlock returns the ledger's most recent log block, or the zero block
// (max_log_id/block_id 0, nil hash) when the ledger has none yet — matching the
// create_blocks bootstrap of `(0, 0, null)::block`.
func (r *AsyncBlockRunner) readPreviousBlock(ctx context.Context, tx bun.IDB, l ledger.Ledger) (logBlock, error) {
	block := logBlock{}
	err := tx.NewSelect().
		ColumnExpr("to_id").
		ColumnExpr("id").
		ColumnExpr("hash").
		TableExpr("?.logs_blocks", bun.Ident(l.Bucket)).
		Where("ledger = ?", l.Name).
		OrderExpr("previous desc").
		Limit(1).
		Scan(ctx, &block.maxLogID, &block.blockID, &block.hash)
	if err != nil {
		err = postgres.ResolveError(err)
		if postgres.IsNotFoundError(err) {
			return logBlock{}, nil
		}
		return logBlock{}, err
	}
	return block, nil
}

// createBlock hashes the next window of up-to-MaxBlockSize logs after the previous
// block and inserts one logs_blocks row, returning the new block; it returns the
// zero block when no logs remain. The hash is byte-identical to the retired
// create_block plpgsql: sha256 over `\x`+hex(previous.hash) followed by the
// per-log parts — Postgres renders `coalesce(previous.hash,'') || string_agg(...)`
// as exactly that text, which pgcrypto's digest() hashed.
func (r *AsyncBlockRunner) createBlock(ctx context.Context, tx bun.IDB, l ledger.Ledger, previous logBlock) (logBlock, error) {
	rows := make([]blockLogRow, 0, r.cfg.MaxBlockSize)
	err := tx.NewSelect().
		ColumnExpr("id").
		ColumnExpr("type || encode(memento, 'escape') || (to_json(date::timestamp)#>>'{}') || coalesce(idempotency_key, '') || id as part").
		TableExpr("?.logs", bun.Ident(l.Bucket)).
		Where("id > ?", previous.maxLogID).
		Where("ledger = ?", l.Name).
		OrderExpr("id").
		Limit(r.cfg.MaxBlockSize).
		Scan(ctx, &rows)
	if err != nil {
		return logBlock{}, postgres.ResolveError(err)
	}
	if len(rows) == 0 {
		return logBlock{}, nil
	}

	body := strings.Builder{}
	body.WriteString(`\x`)
	body.WriteString(hex.EncodeToString(previous.hash))
	var maxLogID int64
	for _, row := range rows {
		body.WriteString(row.Part)
		if row.ID > maxLogID {
			maxLogID = row.ID
		}
	}
	hash := sha256.Sum256([]byte(body.String()))

	block := logBlock{maxLogID: maxLogID, hash: hash[:]}
	if err := tx.NewRaw(
		`insert into ?.logs_blocks (ledger, previous, from_id, to_id, hash, date) `+
			`values (?, ?, ?, ?, ?, now()) returning id`,
		bun.Ident(l.Bucket), l.Name, previous.blockID, previous.maxLogID, maxLogID, block.hash,
	).Scan(ctx, &block.blockID); err != nil {
		return logBlock{}, postgres.ResolveError(err)
	}
	return block, nil
}

func NewAsyncBlockRunner(logger logging.Logger, db *bun.DB, cfg AsyncBlockRunnerConfig, opts ...Option) *AsyncBlockRunner {
	ret := &AsyncBlockRunner{
		stopChannel: make(chan chan struct{}),
		logger:      logger,
		db:          db,
		cfg:         cfg,
	}

	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}

	return ret
}

type Option func(*AsyncBlockRunner)

func WithTracer(tracer trace.Tracer) Option {
	return func(r *AsyncBlockRunner) {
		r.tracer = tracer
	}
}

var defaultOptions = []Option{
	WithTracer(noop.Tracer{}),
}

func NewAsyncBlockRunnerModule(cfg AsyncBlockRunnerConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(logger logging.Logger, db *bun.DB) (*AsyncBlockRunner, error) {
			return NewAsyncBlockRunner(logger, db, cfg), nil
		}),
		fx.Invoke(func(lc fx.Lifecycle, asyncBlockRunner *AsyncBlockRunner) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						if err := asyncBlockRunner.Run(context.WithoutCancel(ctx)); err != nil {
							panic(err)
						}
					}()

					return nil
				},
				OnStop: asyncBlockRunner.Stop,
			})
		}),
	)
}
