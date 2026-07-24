package v2

import (
	"net/http"

	"github.com/pkg/errors"

	"github.com/hanzo-fi/go-libs/v5/pkg/transport/api"

	ledger "github.com/hanzo-fi/ledger/internal"
	ledgercontroller "github.com/hanzo-fi/ledger/internal/controller/ledger"
	systemcontroller "github.com/hanzo-fi/ledger/internal/controller/system"
)

func resetPipeline(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := systemController.ResetPipeline(r.Context(), getPipelineID(r)); err != nil {
			switch {
			case errors.Is(err, ledger.ErrPipelineNotFound("")):
				api.NotFound(w, err)
			case errors.Is(err, ledgercontroller.ErrInUsePipeline("")):
				api.BadRequest(w, "VALIDATION", err)
			default:
				api.InternalServerError(w, r, err)
			}
			return
		}

		w.WriteHeader(http.StatusAccepted)
	}

}
