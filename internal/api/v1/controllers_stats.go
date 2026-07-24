package v1

import (
	"net/http"

	"github.com/hanzo-fi/go-libs/v5/pkg/transport/api"

	"github.com/hanzo-fi/ledger/internal/api/common"
)

func getStats(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	stats, err := l.GetStats(r.Context())
	if err != nil {
		common.HandleCommonErrors(w, r, err)
		return
	}

	api.Ok(w, stats)
}
