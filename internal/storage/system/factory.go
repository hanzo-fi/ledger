package system

import (
	"github.com/uptrace/bun"

	"github.com/hanzo-fi/ledger/internal/storage/dialect"
)

type StoreFactory interface {
	Create(db bun.IDB) Store
}

type DefaultStoreFactory struct {
	dialect dialect.Dialect
	options []Option
}

func (s DefaultStoreFactory) Create(db bun.IDB) Store {
	return New(db, s.dialect, s.options...)
}

var _ StoreFactory = DefaultStoreFactory{}

func NewStoreFactory(d dialect.Dialect, opts ...Option) DefaultStoreFactory {
	return DefaultStoreFactory{dialect: d, options: opts}
}
