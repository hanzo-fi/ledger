package v2

import (
	"net/http"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/hanzo-fi/go-libs/v5/pkg/transport/api"

	"github.com/hanzo-fi/ledger/internal/api/common"
	"github.com/hanzo-fi/ledger/internal/controller/system"
	storagecommon "github.com/hanzo-fi/ledger/internal/storage/common"
	systemstore "github.com/hanzo-fi/ledger/internal/storage/system"
)

// listLedgers constructs an HTTP handler that lists ledgers with pagination.
// The handler applies the provided pagination configuration (sorted by "id" ascending),
// reads the "includeDeleted" query parameter to include deleted ledgers when set,
// invokes the controller's ListLedgers, and renders the resulting paginated cursor.
func listLedgers(b system.Controller, paginationConfig storagecommon.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		rq, err := getPaginatedQuery[systemstore.ListLedgersQueryPayload](
			r,
			paginationConfig,
			"id",
			paginate.OrderAsc,
			func(resourceQuery *storagecommon.ResourceQuery[systemstore.ListLedgersQueryPayload]) {
				// Extract includeDeleted query parameter
				includeDeleted := api.QueryParamBool(r, "includeDeleted")
				resourceQuery.Opts.IncludeDeleted = includeDeleted
			},
		)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		ledgers, err := b.ListLedgers(r.Context(), rq)
		if err != nil {
			common.HandleCommonPaginationErrors(w, r, err)
			return
		}

		api.RenderCursor(w, *ledgers)
	}
}
