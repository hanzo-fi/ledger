package ledger

import (
	"sync"
	"sync/atomic"

	"github.com/uptrace/bun"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/storage/bucket"
	"github.com/hanzo-fi/ledger/internal/storage/dialect"
)

type Factory interface {
	Create(bucket.Bucket, ledger.Ledger) *Store
}

type DefaultFactory struct {
	db      *bun.DB
	dialect dialect.Dialect
	options []Option

	mu          sync.Mutex
	bucketFlags map[string]*atomic.Bool
}

func NewFactory(db *bun.DB, d dialect.Dialect, options ...Option) *DefaultFactory {
	return &DefaultFactory{
		db:          db,
		dialect:     d,
		options:     options,
		bucketFlags: make(map[string]*atomic.Bool),
	}
}

func (d *DefaultFactory) Create(b bucket.Bucket, l ledger.Ledger) *Store {
	d.mu.Lock()
	flag, ok := d.bucketFlags[l.Bucket]
	if !ok {
		flag = &atomic.Bool{}
		d.bucketFlags[l.Bucket] = flag
	}
	d.mu.Unlock()

	store := New(d.db, d.dialect, b, l, d.options...)
	store.aloneInBucket = flag

	return store
}
