package v2

import (
	"net/http"

	"github.com/hanzo-fi/go-libs/v5/pkg/transport/api"

	"github.com/hanzo-fi/ledger/internal/api/common"
	"github.com/hanzo-fi/ledger/internal/controller/system"
)

func deleteLedgerMetadata(b system.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := b.DeleteLedgerMetadata(r.Context(), common.URLParam(r, "ledger"), common.URLParam(r, "key")); err != nil {
			common.HandleCommonWriteErrors(w, r, err)
			return
		}

		api.NoContent(w)
	}
}
