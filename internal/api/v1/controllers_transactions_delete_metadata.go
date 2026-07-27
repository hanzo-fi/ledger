package v1

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/hanzo-fi/go-libs/v5/pkg/transport/api"

	"github.com/hanzo-fi/ledger/internal/api/common"
	ledgercontroller "github.com/hanzo-fi/ledger/internal/controller/ledger"
)

func deleteTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	transactionID, err := strconv.ParseUint(common.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, errors.New("invalid transaction ID"))
		return
	}

	metadataKey := common.URLParam(r, "key")

	_, idempotencyHit, err := l.DeleteTransactionMetadata(r.Context(), getCommandParameters(r, ledgercontroller.DeleteTransactionMetadata{
		TransactionID: transactionID,
		Key:           metadataKey,
	}))
	if err != nil {
		common.HandleCommonWriteErrors(w, r, err)
		return
	}
	if idempotencyHit {
		w.Header().Set("Idempotency-Hit", "true")
	}

	api.NoContent(w)
}
