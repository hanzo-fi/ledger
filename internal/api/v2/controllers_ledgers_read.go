package v2

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/postgres"
	"github.com/hanzo-fi/go-libs/v5/pkg/transport/api"

	"github.com/hanzo-fi/ledger/internal/api/common"
	"github.com/hanzo-fi/ledger/internal/controller/system"
)

func readLedger(b system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ledger, err := b.GetLedger(r.Context(), chi.URLParam(r, "ledger"))
		if err != nil {
			switch {
			case postgres.IsNotFoundError(err):
				api.NotFound(w, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}
		api.Ok(w, ledger)
	}
}
