package v1

import (
	"net/http"

	"github.com/hanzo-fi/go-libs/v5/pkg/authn/jwt"
	"github.com/zap-proto/zip"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"github.com/hanzo-fi/ledger/internal/api/common"
	"github.com/hanzo-fi/ledger/internal/controller/system"
)

// Register mounts the v1 API on router, with base wrapped around every route.
func Register(
	router zip.Router,
	base common.Chain,
	systemController system.Controller,
	authenticator jwt.Authenticator,
	version string,
	debug bool,
	opts ...RouterOption,
) {
	routerOptions := &routerOptions{}
	for _, opt := range append(defaultRouterOptions, opts...) {
		opt(routerOptions)
	}

	router.Get("/_info", base.Adapt(GetInfo(systemController, version, routerOptions.experimentalFeatures)))

	// Every ledger-scoped route runs the same chain: authenticate, create the
	// ledger on first use, then resolve it and check its schema — except on
	// /_info, which has to stay reachable on an outdated schema to report it.
	scoped := base.With(
		jwt.Middleware(authenticator),
		autoCreateMiddleware(systemController, routerOptions.tracer),
		common.LedgerMiddleware(systemController, func(r *http.Request) string {
			return common.URLParam(r, "ledger")
		}, routerOptions.tracer, "/_info"),
	)

	ledger := router.Group("/:ledger")

	// LedgerController
	ledger.Get("/_info", scoped.Adapt(getLedgerInfo))
	ledger.Get("/stats", scoped.Adapt(getStats))
	ledger.Get("/logs", scoped.Adapt(getLogs))

	// AccountController
	ledger.Get("/accounts", scoped.Adapt(listAccounts))
	ledger.Head("/accounts", scoped.Adapt(countAccounts))
	ledger.Get("/accounts/:address", scoped.Adapt(getAccount))
	ledger.Post("/accounts/:address/metadata", scoped.Adapt(addAccountMetadata))
	ledger.Delete("/accounts/:address/metadata/:key", scoped.Adapt(deleteAccountMetadata))

	// TransactionController
	ledger.Get("/transactions", scoped.Adapt(listTransactions))
	ledger.Head("/transactions", scoped.Adapt(countTransactions))

	ledger.Post("/transactions", scoped.Adapt(createTransaction))
	ledger.Post("/transactions/batch", scoped.Adapt(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not supported", http.StatusBadRequest)
	}))

	ledger.Get("/transactions/:id", scoped.Adapt(readTransaction))
	ledger.Post("/transactions/:id/revert", scoped.Adapt(revertTransaction))
	ledger.Post("/transactions/:id/metadata", scoped.Adapt(addTransactionMetadata))
	ledger.Delete("/transactions/:id/metadata/:key", scoped.Adapt(deleteTransactionMetadata))

	ledger.Get("/balances", scoped.Adapt(getBalances))
	ledger.Get("/aggregate/balances", scoped.Adapt(getBalancesAggregated))

	// A ledger-scoped path with no route of its own still runs the chain and
	// then 404s, exactly as it did when this was a sub-router mounted under
	// /{ledger}. Specificity, not registration order, keeps this from
	// shadowing anything above.
	ledger.All("/*", scoped.Adapt(http.NotFound))
}

// NewRouter returns the v1 API as a standalone net/http handler.
func NewRouter(
	systemController system.Controller,
	authenticator jwt.Authenticator,
	version string,
	debug bool,
	opts ...RouterOption,
) http.Handler {
	app := common.NewApp()
	Register(app.Group(""), nil, systemController, authenticator, version, debug, opts...)
	return common.Handler(app)
}

type routerOptions struct {
	tracer               trace.Tracer
	experimentalFeatures []string
}

type RouterOption func(ro *routerOptions)

func WithTracer(tracer trace.Tracer) RouterOption {
	return func(ro *routerOptions) {
		ro.tracer = tracer
	}
}

func WithExperimentalFeatures(features []string) RouterOption {
	return func(ro *routerOptions) {
		ro.experimentalFeatures = features
	}
}

var defaultRouterOptions = []RouterOption{
	WithTracer(nooptracer.Tracer{}),
}
