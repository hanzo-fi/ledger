package dialect

import (
	"errors"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/postgres"
)

// The storage error vocabulary. Both engines resolve their native errors onto
// these, so nothing above the storage layer reads an engine error.
//
// The sentinels are the values go-libs already uses so that errors.Is keeps
// matching across the service; this file is the only place that name appears.
var (
	ErrNotFound     = postgres.ErrNotFound
	ErrDeadlock     = postgres.ErrDeadlockDetected
	ErrSerialize    = postgres.ErrSerialization
	ErrMissingTable = postgres.ErrMissingTable
	ErrReadOnly     = postgres.ErrReadOnlyTransaction
)

// ErrConstraint reports a violated uniqueness or check constraint, named.
type ErrConstraint struct {
	Name string
	err  error
}

func NewErrConstraint(name string, err error) ErrConstraint {
	return ErrConstraint{Name: name, err: err}
}

func (e ErrConstraint) Error() string {
	if e.err != nil {
		return e.err.Error()
	}
	return "constraint failed: " + e.Name
}

func (e ErrConstraint) Unwrap() error { return e.err }

// Is matches any ErrConstraint, so errors.Is(err, ErrConstraint{}) asks only
// whether a constraint failed; Name says which.
func (e ErrConstraint) Is(err error) bool {
	var target ErrConstraint
	return errors.As(err, &target)
}

// Constraint returns the name of the violated constraint, or "".
func Constraint(err error) string {
	var target ErrConstraint
	if errors.As(err, &target) {
		return target.Name
	}
	return ""
}
