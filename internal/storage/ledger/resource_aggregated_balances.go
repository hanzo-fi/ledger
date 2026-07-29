package ledger

import (
	"context"
	"database/sql"
	"errors"

	"github.com/uptrace/bun"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/queries"
	"github.com/hanzo-fi/ledger/internal/storage/common"
	"github.com/hanzo-fi/ledger/pkg/features"
)

type aggregatedBalancesResourceRepositoryHandler struct {
	store *Store
}

func (h aggregatedBalancesResourceRepositoryHandler) Schema() queries.EntitySchema {
	return queries.AggregatedBalanceSchema
}

func (h aggregatedBalancesResourceRepositoryHandler) BuildDataset(query common.RepositoryHandlerBuildContext[ledger.GetAggregatedVolumesOptions]) (*bun.SelectQuery, error) {

	allAddresses, needAddressSegments := collectAddressFilters(query)
	canPushLateral := canPushAddressFilterToLateral(query.Builder)

	if query.UsePIT() {
		ret := h.store.newScopedSelect().
			ModelTableExpr(h.store.GetPrefixedRelationName("moves")).
			DistinctOn("accounts_address, asset").
			Column("accounts_address", "asset")
		if query.Opts.UseInsertionDate {
			if !h.store.ledger.HasFeature(features.FeatureMovesHistory, "ON") {
				return nil, NewErrMissingFeature(features.FeatureMovesHistory)
			}

			ret = ret.
				ColumnExpr("first_value(post_commit_volumes) over (partition by accounts_address, asset order by seq desc) as volumes").
				Where("insertion_date <= ?", query.PIT)
		} else {
			if !h.store.ledger.HasFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "SYNC") {
				return nil, NewErrMissingFeature(features.FeatureMovesHistoryPostCommitEffectiveVolumes)
			}

			ret = ret.
				ColumnExpr("first_value(post_commit_effective_volumes) over (partition by accounts_address, asset order by effective_date desc, seq desc) as volumes").
				Where("effective_date <= ?", query.PIT)
		}

		if needAddressSegments {
			subQuery := h.store.newScopedSelect().
				TableExpr(h.store.GetPrefixedRelationName("accounts")).
				Column("address_array").
				Where("accounts.address = accounts_address")

			subQuery = applyLateralAddressFilter(subQuery, allAddresses, canPushLateral)

			ret = ret.
				ColumnExpr("accounts.address_array as accounts_address_array").
				Join(`join lateral (?) accounts on true`, subQuery)
		}

		if query.UseFilter("metadata") {
			if h.store.ledger.HasFeature(features.FeatureAccountMetadataHistory, "SYNC") {
				subQuery := h.store.newScopedSelect().
					DistinctOn("accounts_address").
					ModelTableExpr(h.store.GetPrefixedRelationName("accounts_metadata")).
					ColumnExpr("first_value(metadata) over (partition by accounts_address order by revision desc) as metadata").
					Where("accounts_metadata.accounts_address = moves.accounts_address").
					Where("date <= ?", query.PIT)

				ret = ret.
					Join(`left join lateral (?) accounts_metadata on true`, subQuery).
					Column("metadata")
			} else {
				subQuery := h.store.newScopedSelect().
					TableExpr(h.store.GetPrefixedRelationName("accounts")).
					ColumnExpr("metadata").
					Where("accounts.address = moves.accounts_address")

				ret = ret.
					Join(`left join lateral (?) accounts_metadata on true`, subQuery).
					Column("metadata")
			}
		}

		return ret, nil
	} else {
		ret := h.store.newScopedSelect().
			ModelTableExpr(h.store.GetPrefixedRelationName("accounts_volumes")).
			Column("asset", "accounts_address").
			ColumnExpr(h.store.dialect.Pair(h.store.ledger.Bucket, "input", "output") + " as volumes")

		if query.UseFilter("metadata") || needAddressSegments {
			subQuery := h.store.newScopedSelect().
				TableExpr(h.store.GetPrefixedRelationName("accounts")).
				Column("address").
				Where("accounts.address = accounts_address")

			if query.UseFilter("address") {
				subQuery = subQuery.ColumnExpr("address_array as accounts_address_array")
				ret = ret.Column("accounts_address_array")
				subQuery = applyLateralAddressFilter(subQuery, allAddresses, canPushLateral)
			}
			if query.UseFilter("metadata") {
				subQuery = subQuery.ColumnExpr("metadata")
				ret = ret.Column("metadata")
			}

			ret = ret.
				Join(`join lateral (?) accounts on true`, subQuery)
		}

		return ret, nil
	}
}

func (h aggregatedBalancesResourceRepositoryHandler) ResolveFilter(_ context.Context, _ common.ResourceQuery[ledger.GetAggregatedVolumesOptions], operator, property string, value any) (string, []any, error) {
	switch {
	case property == "address":
		switch operator {
		case queries.OperatorIn:
			addresses, err := assetAddressArray(value)
			if err != nil {
				return "", nil, err
			}

			return "accounts_address IN (?)", []any{bun.In(addresses)}, nil
		default:
			return filterAccountAddress(value.(string), "accounts_address"), nil, nil
		}
	case common.MetadataRegex.Match([]byte(property)) || property == "metadata":
		if property == "metadata" {
			has := h.store.dialect.Has("metadata", value.(string))
			return has.SQL, has.Args, nil
		} else {
			match := common.MetadataRegex.FindAllStringSubmatch(property, 3)

			holds := h.store.dialect.Holds("metadata", map[string]any{match[0][1]: value})
			return holds.SQL, holds.Args, nil
		}
	default:
		return "", nil, common.NewErrInvalidQuery("unknown key '%s' when building query", property)
	}
}

func (h aggregatedBalancesResourceRepositoryHandler) Expand(_ common.ResourceQuery[ledger.GetAggregatedVolumesOptions], property string) (*bun.SelectQuery, *common.JoinCondition, error) {
	return nil, nil, errors.New("no expand available for aggregated balances")
}

// Project yields the rows to be totalled: an asset and the pair standing
// against it. The total is not asked of the engine - see totalVolumes.
func (h aggregatedBalancesResourceRepositoryHandler) Project(
	_ common.ResourceQuery[ledger.GetAggregatedVolumesOptions],
	selectQuery *bun.SelectQuery,
) (*bun.SelectQuery, error) {
	return selectQuery.Column("asset", "volumes"), nil
}

// totalVolumes adds the rows up, one pair per asset.
//
// A ledger's volumes are arbitrary precision integers and no engine adds those:
// one holds 64 bit integers and saturates, the other holds a numeric. So the
// addition is Go's, over big.Int, and a total reads back the same whichever
// engine the ledger is stored on. A row with no pair adds nothing, as an
// aggregate over a null does.
func totalVolumes(rows *sql.Rows) (ledger.AggregatedVolumes, error) {
	total := ledger.VolumesByAssets{}
	for rows.Next() {
		var (
			asset   string
			volumes sql.Null[ledger.Volumes]
		)
		if err := rows.Scan(&asset, &volumes); err != nil {
			return ledger.AggregatedVolumes{}, err
		}
		if !volumes.Valid {
			continue
		}

		sum, held := total[asset]
		if !held {
			sum = ledger.NewEmptyVolumes()
		}
		sum.Input.Add(sum.Input, volumes.V.Input)
		sum.Output.Add(sum.Output, volumes.V.Output)
		total[asset] = sum
	}

	return ledger.AggregatedVolumes{Aggregated: total}, nil
}

var _ common.RepositoryHandler[ledger.GetAggregatedVolumesOptions] = aggregatedBalancesResourceRepositoryHandler{}
