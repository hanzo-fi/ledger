package ledger

import (
	"context"
	"database/sql"
	"math/big"
	"reflect"

	"github.com/uptrace/bun"

	"github.com/hanzo-fi/go-libs/v5/pkg/types/time"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/queries"
	"github.com/hanzo-fi/ledger/internal/storage/common"
	"github.com/hanzo-fi/ledger/internal/tracing"
	"github.com/hanzo-fi/ledger/pkg/features"
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

// holding is the pair one account stands at in one asset, as it was written.
type holding struct {
	Account string                   `bun:"accounts_address"`
	Asset   string                   `bun:"asset"`
	Volumes sql.Null[ledger.Volumes] `bun:"volumes"`
}

// holders reads the balance every account stands at and keeps the ones standing
// in the relation the operator names to value. An empty asset asks after every
// asset: an account is kept if it stands in that relation in any of them.
//
// A balance is an arbitrary precision integer and no engine compares one at a
// ledger's width. One engine's integers are 64 bits wide, so a balance wider
// than that becomes a float and two distinct balances compare equal; the other
// holds a numeric, exact, but a value reaching it as text compares by storage
// class rather than by value. So the comparison is Go's, over big.Int, as the
// total of an aggregated read already is: the statement carries the rows and
// the addresses the fold selected, which every engine does the same, and the
// relation is Go's, which is where the ledger's arithmetic lives.
func (store *Store) holders(
	ctx context.Context,
	pit *time.Time,
	asset, operator string,
	value *big.Int,
) ([]string, error) {

	rows, err := store.standings(ctx, pit, asset)
	if err != nil {
		return nil, err
	}

	addresses := make([]string, 0)
	kept := make(map[string]struct{}, len(rows))
	for _, row := range rows {
		if !row.Volumes.Valid {
			continue
		}
		keep, err := stands(operator, row.Volumes.V.Balance(), value)
		if err != nil {
			return nil, err
		}
		if _, held := kept[row.Account]; keep && !held {
			kept[row.Account] = struct{}{}
			addresses = append(addresses, row.Account)
		}
	}

	return addresses, nil
}

// standings reads the pair every account stands at, one row per account and
// asset. Without a point in time that is the running pair the ledger keeps;
// with one it is the pair the account stood at after its last move, which is
// the same reduction stated with a window rather than with a distinct - one
// statement both engines run.
func (store *Store) standings(ctx context.Context, pit *time.Time, asset string) ([]holding, error) {
	var rows *bun.SelectQuery

	if pit != nil && !pit.IsZero() {
		if !store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
			return nil, NewErrMissingFeature(features.FeatureMovesHistory)
		}

		moves := store.newScopedSelect().
			ModelTableExpr(store.GetPrefixedRelationName("moves")).
			ColumnExpr("accounts_address").
			ColumnExpr("asset").
			ColumnExpr("post_commit_effective_volumes as volumes").
			ColumnExpr("row_number() over (partition by accounts_address, asset order by effective_date desc, seq desc) as ordinal").
			Where("effective_date <= ?", pit)
		if asset != "" {
			moves = moves.Where("asset = ?", asset)
		}

		rows = store.db.NewSelect().
			ModelTableExpr("(?) moves", moves).
			ColumnExpr("accounts_address").
			ColumnExpr("asset").
			ColumnExpr("volumes").
			Where("ordinal = 1")
	} else {
		rows = store.newScopedSelect().
			ModelTableExpr(store.GetPrefixedRelationName("accounts_volumes")).
			ColumnExpr("accounts_address").
			ColumnExpr("asset").
			ColumnExpr(store.dialect.Pair(store.ledger.Bucket, "input", "output") + " as volumes")
		if asset != "" {
			rows = rows.Where("asset = ?", asset)
		}
	}

	ret := make([]holding, 0)
	if err := rows.Model(&ret).Scan(ctx); err != nil {
		return nil, store.dialect.ResolveError(err)
	}

	return ret, nil
}

// stands reports whether a balance stands in the relation the operator names to
// a value.
func stands(operator string, balance, value *big.Int) (bool, error) {
	switch cmp := balance.Cmp(value); operator {
	case queries.OperatorMatch:
		return cmp == 0, nil
	case queries.OperatorLT:
		return cmp < 0, nil
	case queries.OperatorLTE:
		return cmp <= 0, nil
	case queries.OperatorGT:
		return cmp > 0, nil
	case queries.OperatorGTE:
		return cmp >= 0, nil
	default:
		return false, common.NewErrInvalidQuery("operator '%s' is not allowed for a balance", operator)
	}
}

// number reads a filter operand as the integer it names. A ledger's numbers are
// arbitrary precision, so an operand is read at that precision whatever shape it
// arrives in.
func number(value any) (*big.Int, error) {
	switch v := value.(type) {
	case *big.Int:
		return v, nil
	case big.Int:
		return &v, nil
	}

	held := reflect.ValueOf(value)
	for held.Kind() == reflect.Pointer && !held.IsNil() {
		held = held.Elem()
	}
	switch held.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return big.NewInt(held.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return new(big.Int).SetUint64(held.Uint()), nil
	case reflect.Float32, reflect.Float64:
		exact, accuracy := big.NewFloat(held.Float()).Int(nil)
		if accuracy != big.Exact {
			return nil, common.NewErrInvalidQuery("'%v' is not an integer", value)
		}
		return exact, nil
	}

	return nil, common.NewErrInvalidQuery("'%v' is not a number", value)
}
