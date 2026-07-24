package v2

import (
	"net/http"

	"github.com/hanzo-fi/go-libs/v5/pkg/transport/api"

	systemcontroller "github.com/hanzo-fi/ledger/internal/controller/system"
)

func listExporters(systemController systemcontroller.Controller) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		exporters, err := systemController.ListExporters(r.Context())
		if err != nil {
			api.InternalServerError(w, r, err)
			return
		}

		api.RenderCursor(w, *exporters)
	}
}
