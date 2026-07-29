package system

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/uptrace/bun"

	"github.com/hanzo-fi/go-libs/v5/pkg/observe"
	logging "github.com/hanzo-fi/go-libs/v5/pkg/observe/log"

	ledger "github.com/hanzo-fi/ledger/internal"
	ledgercontroller "github.com/hanzo-fi/ledger/internal/controller/ledger"
	"github.com/hanzo-fi/ledger/internal/storage/dialect"
)

type controllerFacade struct {
	ledgercontroller.Controller
	mu      sync.RWMutex
	ledger  ledger.Ledger
	dialect dialect.Dialect
}

func (c *controllerFacade) handleState(ctx context.Context, dryRun bool, fn func(ctrl ledgercontroller.Controller) error) error {
	c.mu.RLock()
	l := c.ledger
	c.mu.RUnlock()

	if l.State == ledger.StateInUse {
		return fn(c.Controller)
	}

	ctrl, tx, err := c.BeginTX(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = ctrl.Rollback(ctx)
	}()

	if err := withLock(ctx, ctrl, func(ctrl ledgercontroller.Controller, conn bun.IDB) error {

		// todo: remove that in a later version
		ret, err := tx.NewUpdate().
			Model(&l).
			Set("state = ?", ledger.StateInUse).
			Where("id = ? and state = ?", l.ID, ledger.StateInitializing).
			Exec(ctx)
		if err != nil {
			return err
		}

		rowsAffected, err := ret.RowsAffected()
		if err != nil {
			return err
		}

		if rowsAffected > 0 {
			for _, relation := range []string{"transactions", "logs"} {
				if err := c.dialect.SyncID(ctx, tx, l.Bucket, relation, l.Name, l.ID); err != nil {
					return fmt.Errorf("failed to update %s id counter: %w", relation, err)
				}
			}
		}

		if err := fn(ctrl); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return err
	}

	if !dryRun {
		if err := ctrl.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}

		c.mu.Lock()
		c.ledger.State = ledger.StateInUse
		c.mu.Unlock()
	} else {
		if err := ctrl.Rollback(ctx); err != nil {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
	}

	return nil
}

func (c *controllerFacade) CreateTransaction(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
	var (
		log            *ledger.Log
		ret            *ledger.CreatedTransaction
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, ret, idempotencyHit, err = ctrl.CreateTransaction(ctx, parameters)
		return err
	})

	return log, ret, idempotencyHit, err
}

func (c *controllerFacade) RevertTransaction(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, bool, error) {
	var (
		log            *ledger.Log
		ret            *ledger.RevertedTransaction
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, ret, idempotencyHit, err = ctrl.RevertTransaction(ctx, parameters)
		return err
	})

	return log, ret, idempotencyHit, err
}

func (c *controllerFacade) SaveTransactionMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.SaveTransactionMetadata]) (*ledger.Log, bool, error) {
	var (
		log            *ledger.Log
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, idempotencyHit, err = ctrl.SaveTransactionMetadata(ctx, parameters)
		return err
	})

	return log, idempotencyHit, err
}

func (c *controllerFacade) SaveAccountMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]) (*ledger.Log, bool, error) {
	var (
		log            *ledger.Log
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, idempotencyHit, err = ctrl.SaveAccountMetadata(ctx, parameters)
		return err
	})

	return log, idempotencyHit, err
}

func (c *controllerFacade) DeleteTransactionMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.DeleteTransactionMetadata]) (*ledger.Log, bool, error) {
	var (
		log            *ledger.Log
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, idempotencyHit, err = ctrl.DeleteTransactionMetadata(ctx, parameters)
		return err
	})

	return log, idempotencyHit, err
}

func (c *controllerFacade) DeleteAccountMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.DeleteAccountMetadata]) (*ledger.Log, bool, error) {
	var (
		log            *ledger.Log
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, idempotencyHit, err = ctrl.DeleteAccountMetadata(ctx, parameters)
		return err
	})
	return log, idempotencyHit, err
}

func (c *controllerFacade) InsertSchema(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.InsertSchema]) (*ledger.Log, *ledger.InsertedSchema, bool, error) {
	var (
		log            *ledger.Log
		ret            *ledger.InsertedSchema
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, ret, idempotencyHit, err = ctrl.InsertSchema(ctx, parameters)
		return err
	})
	return log, ret, idempotencyHit, err
}

func (c *controllerFacade) Import(ctx context.Context, stream chan ledger.Log) error {
	return withLock(ctx, c.Controller, func(ctrl ledgercontroller.Controller, conn bun.IDB) error {
		// todo: remove that in a later version
		if err := conn.NewSelect().Model(&c.ledger).
			Where("id = ?", c.ledger.ID).
			Scan(ctx); err != nil {
			return err
		}

		if c.ledger.State != ledger.StateInitializing {
			return ledgercontroller.NewErrImport(errors.New("ledger is not in initializing state"))
		}

		return ctrl.Import(ctx, stream)
	})
}

var _ ledgercontroller.Controller = (*controllerFacade)(nil)

func newLedgerStateTracker(ctrl ledgercontroller.Controller, ledger ledger.Ledger, d dialect.Dialect) ledgercontroller.Controller {
	return &controllerFacade{
		Controller: ctrl,
		ledger:     ledger,
		dialect:    d,
	}
}

func withLock(ctx context.Context, ctrl ledgercontroller.Controller, fn func(ctrl ledgercontroller.Controller, conn bun.IDB) error) error {
	lockedCtrl, conn, release, err := ctrl.LockLedger(ctx)
	if err != nil {
		return fmt.Errorf("failed to lock ledger: %w", err)
	}

	defer func() {
		if err := release(); err != nil {
			logging.FromContext(ctx).Errorf(
				"failed to release lock: %v",
				err,
			)
			observe.RecordError(ctx, fmt.Errorf("failed to release lock: %v", err))
		}
	}()

	return fn(lockedCtrl, conn)
}
