package v2

import (
	"net/http"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/hanzo-fi/go-libs/v5/pkg/transport/api"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/api/common"
	storagecommon "github.com/hanzo-fi/ledger/internal/storage/common"
)

func listLogs(paginationConfig storagecommon.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())

		rq, err := getPaginatedQuery[any](r, paginationConfig, "id", paginate.OrderDesc)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		cursor, err := l.ListLogs(r.Context(), rq)
		if err != nil {
			common.HandleCommonPaginationErrors(w, r, err)
			return
		}

		api.RenderCursor(w, *paginate.MapCursor(cursor, func(log ledger.Log) any {
			return renderLog(r, log)
		}))
	}
}
