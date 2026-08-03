package ledger

import (
	"context"
	"fmt"

	"github.com/stoewer/go-strcase"
	"github.com/uptrace/bun"

	"github.com/hanzo-fi/ledger/internal/queries"
	"github.com/hanzo-fi/ledger/internal/storage/common"
	"github.com/hanzo-fi/ledger/pkg/features"
)

type accountsResourceHandler struct {
	store *Store
}

func (h accountsResourceHandler) Schema() queries.EntitySchema {
	return queries.AccountSchema
}

func (h accountsResourceHandler) BuildDataset(opts common.RepositoryHandlerBuildContext[any]) (*bun.SelectQuery, error) {
	ret := h.store.newScopedSelect().
		ModelTableExpr(h.store.GetPrefixedRelationName("accounts")).
		Column("address", "address_array", "first_usage", "insertion_date", "updated_at")

	if opts.PIT != nil && !opts.PIT.IsZero() {
		ret = ret.Where("accounts.first_usage <= ?", opts.PIT)
	}

	if h.store.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") && opts.PIT != nil && !opts.PIT.IsZero() {
		selectDistinctAccountMetadataHistories := h.store.newScopedSelect().
			DistinctOn("accounts_address").
			ModelTableExpr(h.store.GetPrefixedRelationName("accounts_metadata")).
			Column("accounts_address").
			ColumnExpr("first_value(metadata) over (partition by accounts_address order by revision desc) as metadata").
			Where("date <= ?", opts.PIT)

		ret = ret.
			Join(
				`left join (?) accounts_metadata on accounts_metadata.accounts_address = accounts.address`,
				selectDistinctAccountMetadataHistories,
			).
			ColumnExpr("coalesce(accounts_metadata.metadata, '{}'::jsonb) as metadata")
	} else {
		ret = ret.ColumnExpr("accounts.metadata")
	}

	return ret, nil
}

func (h accountsResourceHandler) ResolveFilter(ctx context.Context, opts common.ResourceQuery[any], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "address":
		fallthrough
	case property == "account":
		switch operator {
		case queries.OperatorIn:
			addresses, err := assetAddressArray(value)
			if err != nil {
				return "", nil, err
			}

			return "address IN (?)", []any{bun.In(addresses)}, nil
		default:
			return filterAccountAddress(value.(string), "address"), nil, nil
		}

	case property == "first_usage" || property == "insertion_date" || property == "updated_at":
		value, err := common.NormalizeDateFilterValue(value)
		if err != nil {
			return "", nil, err
		}
		return fmt.Sprintf("%s %s ?", property, common.ConvertOperatorToSQL(operator)), []any{value}, nil
	case balanceRegex.MatchString(property) || property == "balance":
		// The relation itself is not asked of the engine - see holders. What
		// the statement carries is the addresses the fold selected, which every
		// engine tests the same way.
		asset := ""
		if balanceRegex.MatchString(property) {
			asset = balanceRegex.FindAllStringSubmatch(property, 2)[0][1]
		}

		value, err := number(value)
		if err != nil {
			return "", nil, err
		}

		addresses, err := h.store.holders(ctx, opts.PIT, asset, operator, value)
		if err != nil {
			return "", nil, err
		}
		if len(addresses) == 0 {
			return "1 = 0", nil, nil
		}

		return "address in (?)", []any{bun.In(addresses)}, nil
	case property == "metadata":
		has := h.store.dialect.Has("metadata", value.(string))
		return has.SQL, has.Args, nil

	case common.MetadataRegex.Match([]byte(property)):
		match := common.MetadataRegex.FindAllStringSubmatch(property, 3)

		holds := h.store.dialect.Holds("metadata", map[string]any{match[0][1]: value})
		return holds.SQL, holds.Args, nil
	default:
		return "", nil, common.NewErrInvalidQuery("invalid filter property %s", property)
	}
}

func (h accountsResourceHandler) Project(_ common.ResourceQuery[any], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error) {
	return selectQuery.ColumnExpr("*"), nil
}

func (h accountsResourceHandler) Expand(opts common.ResourceQuery[any], property string) (*bun.SelectQuery, *common.JoinCondition, error) {
	d := h.store.dialect
	switch property {
	case "volumes":
		if !h.store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
			return nil, nil, common.NewErrInvalidQuery("feature %s must be 'ON' to use volumes", features.FeatureMovesHistory)
		}
	case "effectiveVolumes":
		if !h.store.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
			return nil, nil, common.NewErrInvalidQuery("feature %s must be 'SYNC' to use effectiveVolumes", features.FeatureMovesHistoryPostCommitEffectiveVolumes)
		}
	default:
		// The property becomes the column alias below, which bun renders as raw
		// SQL, so only the expansions named here may reach it.
		return nil, nil, common.NewErrInvalidQuery("invalid expand property %s", property)
	}

	selectRowsQuery := h.store.newScopedSelect().
		Where("accounts_address in (select address from dataset)")
	if opts.UsePIT() {
		selectRowsQuery = selectRowsQuery.
			ModelTableExpr(h.store.GetPrefixedRelationName("moves")).
			DistinctOn("accounts_address, asset").
			Column("accounts_address", "asset")
		if property == "volumes" {
			selectRowsQuery = selectRowsQuery.
				ColumnExpr("first_value(post_commit_volumes) over (partition by accounts_address, asset order by seq desc) as volumes").
				Where("insertion_date <= ?", opts.PIT)
		} else {
			selectRowsQuery = selectRowsQuery.
				ColumnExpr("first_value(post_commit_effective_volumes) over (partition by accounts_address, asset order by effective_date desc, seq desc) as volumes").
				Where("effective_date <= ?", opts.PIT)
		}
	} else {
		selectRowsQuery = selectRowsQuery.
			ModelTableExpr(h.store.GetPrefixedRelationName("accounts_volumes")).
			Column("asset", "accounts_address").
			ColumnExpr(d.Pair(h.store.ledger.Bucket, "input", "output") + " as volumes")
	}

	return h.store.db.NewSelect().
			With("rows", selectRowsQuery).
			ModelTableExpr("rows").
			Column("accounts_address").
			ColumnExpr(d.Gather("asset", d.Volumes("volumes")) + " as " + strcase.SnakeCase(property)).
			Group("accounts_address"), &common.JoinCondition{
			Left:  "address",
			Right: "accounts_address",
		}, nil
}

var _ common.RepositoryHandler[any] = accountsResourceHandler{}
