package v2

import (
	"context"
	"encoding/json"
	"net/http"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/api/common"
	ledgercontroller "github.com/hanzo-fi/ledger/internal/controller/ledger"
)

func exportLogs(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := common.LedgerFromContext(r.Context()).Export(r.Context(), ledgercontroller.ExportWriterFn(func(ctx context.Context, log ledger.Log) error {
		return enc.Encode(log)
	})); err != nil {
		common.HandleCommonErrors(w, r, err)
		return
	}
}
