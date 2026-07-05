package v2

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v5/pkg/transport/api"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/api/common"
	systemcontroller "github.com/hanzo-fi/ledger/internal/controller/system"
)

func createExporter(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		common.WithBody[ledger.ExporterConfiguration](w, r, func(req ledger.ExporterConfiguration) {
			exporter, err := systemController.CreateExporter(r.Context(), req)
			if err != nil {
				switch {
				case errors.Is(err, systemcontroller.ErrInvalidDriverConfiguration{}):
					api.BadRequest(w, "VALIDATION", err)
				default:
					api.InternalServerError(w, r, err)
				}
				return
			}

			api.Created(w, exporter)
		})
	}
}
