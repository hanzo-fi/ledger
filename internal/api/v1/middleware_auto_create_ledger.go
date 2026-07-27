package v1

import (
	"errors"
	"net/http"

	"github.com/hanzo-fi/go-libs/v5/pkg/storage/postgres"
	"github.com/hanzo-fi/go-libs/v5/pkg/transport/api"
	"go.opentelemetry.io/otel/trace"

	ledger "github.com/hanzo-fi/ledger/internal"
	"github.com/hanzo-fi/ledger/internal/api/common"
	"github.com/hanzo-fi/ledger/internal/controller/system"
)

func autoCreateMiddleware(backend system.Controller, tracer trace.Tracer) func(handler http.Handler) http.Handler {
	return func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {

			ctx, span := tracer.Start(r.Context(), "AutomaticLedgerCreate")
			defer span.End()

			ledgerName := common.URLParam(r, "ledger")
			if _, err := backend.GetLedger(ctx, ledgerName); err != nil {
				if !postgres.IsNotFoundError(err) {
					common.InternalServerError(w, r, err)
					return
				}

				if err := backend.CreateLedger(ctx, ledgerName, ledger.Configuration{
					Bucket: ledgerName,
				}); err != nil {
					switch {
					case errors.Is(err, ledger.ErrInvalidLedgerName{}):
						api.BadRequest(w, common.ErrValidation, err)
					default:
						common.InternalServerError(w, r, err)
					}
					return
				}
			}
			span.End()

			handler.ServeHTTP(w, r)
		})
	}
}
