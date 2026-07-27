package v2

import (
	"net/http"

	"github.com/hanzo-fi/go-libs/v5/pkg/authn/jwt"
	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/zap-proto/zip"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"github.com/hanzo-fi/ledger/internal/api/bulking"
	"github.com/hanzo-fi/ledger/internal/api/common"
	v1 "github.com/hanzo-fi/ledger/internal/api/v1"
	systemcontroller "github.com/hanzo-fi/ledger/internal/controller/system"
	storagecommon "github.com/hanzo-fi/ledger/internal/storage/common"
)

// Register mounts the v2 API under prefix on router, with base wrapped around
// every route.
//
// It registers authentication-protected top-level endpoints (including
// /_info), an "/_" group that may expose exporter management and bucket
// operations, and ledger-scoped routes (ledger creation, metadata, and nested
// subroutes such as bulk operations, info, stats, pipelines when enabled, logs,
// accounts, transactions, aggregated balances, and volumes), tagging
// ledger-scoped spans with the selected ledger. Tracing, bulking, bulk handler
// factories, pagination, and whether exporter-related endpoints are mounted are
// controlled via RouterOption arguments.
func Register(
	router zip.Router,
	prefix string,
	base common.Chain,
	systemController systemcontroller.Controller,
	authenticator jwt.Authenticator,
	version string,
	opts ...RouterOption,
) {
	routerOptions := routerOptions{}
	for _, opt := range append(defaultRouterOptions, opts...) {
		opt(&routerOptions)
	}

	// Handlers and middleware below see paths relative to the API root, as they
	// did when this was a sub-router mounted under a stripped prefix. An empty
	// prefix strips nothing.
	authenticated := base.With(
		func(handler http.Handler) http.Handler { return http.StripPrefix(prefix, handler) },
		jwt.Middleware(authenticator),
	)

	router.Get("/_info", authenticated.Adapt(v1.GetInfo(systemController, version, routerOptions.experimentalFeatures)))

	if routerOptions.exporters {
		exporters := router.Group("/_/exporters")
		exporters.Get("/", authenticated.Adapt(listExporters(systemController)))
		exporters.Get("/:exporterID", authenticated.Adapt(getExporter(systemController)))
		exporters.Put("/:exporterID", authenticated.Adapt(updateExporter(systemController)))
		exporters.Delete("/:exporterID", authenticated.Adapt(deleteExporter(systemController)))
		exporters.Post("/", authenticated.Adapt(createExporter(systemController)))
	}

	buckets := router.Group("/_/buckets")
	buckets.Delete("/:bucket", authenticated.Adapt(deleteBucket(systemController)))
	buckets.Post("/:bucket/restore", authenticated.Adapt(restoreBucket(systemController)))

	router.Get("/", authenticated.Adapt(listLedgers(systemController, routerOptions.paginationConfig)))

	named := authenticated.With(func(handler http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			trace.
				SpanFromContext(r.Context()).
				SetAttributes(attribute.String("ledger", common.URLParam(r, "ledger")))
			handler.ServeHTTP(w, r)
		})
	})

	ledger := router.Group("/:ledger")
	ledger.Post("/", named.Adapt(createLedger(systemController)))
	ledger.Get("/", named.Adapt(readLedger(systemController)))
	ledger.Put("/metadata", named.Adapt(updateLedgerMetadata(systemController)))
	ledger.Delete("/metadata/:key", named.Adapt(deleteLedgerMetadata(systemController)))

	// Everything past this point needs the ledger itself resolved, and its
	// schema up to date — except /_info, which has to stay reachable on an
	// outdated schema to report it.
	scoped := named.With(common.LedgerMiddleware(systemController, func(r *http.Request) string {
		return common.URLParam(r, "ledger")
	}, routerOptions.tracer, "/_info"))

	ledger.Post("/_bulk", scoped.Adapt(bulkHandler(
		routerOptions.bulkerFactory,
		routerOptions.bulkHandlerFactories,
	)))
	ledger.Get("/_info", scoped.Adapt(getLedgerInfo))
	ledger.Get("/stats", scoped.Adapt(readStats))
	ledger.Post("/schemas/:version", scoped.Adapt(insertSchema))
	ledger.Get("/schemas/:version", scoped.Adapt(readSchema))
	ledger.Get("/schemas", scoped.Adapt(listSchemas(routerOptions.paginationConfig)))

	if routerOptions.exporters {
		pipelines := ledger.Group("/pipelines")
		pipelines.Get("/", scoped.Adapt(listPipelines(systemController)))
		pipelines.Post("/", scoped.Adapt(createPipeline(systemController)))

		pipeline := pipelines.Group("/:pipelineID")
		pipeline.Get("/", scoped.Adapt(readPipeline(systemController)))
		pipeline.Delete("/", scoped.Adapt(deletePipeline(systemController)))
		pipeline.Post("/start", scoped.Adapt(startPipeline(systemController)))
		pipeline.Post("/stop", scoped.Adapt(stopPipeline(systemController)))
		pipeline.Post("/reset", scoped.Adapt(resetPipeline(systemController)))
	}

	logs := ledger.Group("/logs")
	logs.Get("/", scoped.Adapt(listLogs(routerOptions.paginationConfig)))
	logs.Post("/import", scoped.Adapt(importLogs))
	logs.Post("/export", scoped.Adapt(exportLogs))

	accounts := ledger.Group("/accounts")
	accounts.Get("/", scoped.Adapt(listAccounts(routerOptions.paginationConfig)))
	accounts.Head("/", scoped.Adapt(countAccounts))
	accounts.Get("/:address", scoped.Adapt(readAccount))
	accounts.Post("/:address/metadata", scoped.Adapt(addAccountMetadata))
	accounts.Delete("/:address/metadata/:key", scoped.Adapt(deleteAccountMetadata))

	transactions := ledger.Group("/transactions")
	transactions.Get("/", scoped.Adapt(listTransactions(routerOptions.paginationConfig)))
	transactions.Head("/", scoped.Adapt(countTransactions))
	transactions.Post("/", scoped.Adapt(createTransaction))
	transactions.Get("/:id", scoped.Adapt(readTransaction))
	transactions.Post("/:id/revert", scoped.Adapt(revertTransaction))
	transactions.Post("/:id/metadata", scoped.Adapt(addTransactionMetadata))
	transactions.Delete("/:id/metadata/:key", scoped.Adapt(deleteTransactionMetadata))

	ledger.Get("/aggregate/balances", scoped.Adapt(readBalancesAggregated))

	ledger.Get("/volumes", scoped.Adapt(readVolumes(routerOptions.paginationConfig)))

	ledger.Post("/queries/:id/run", scoped.Adapt(runQuery(routerOptions.paginationConfig)))

	// A v2 path with no route of its own still authenticates and then 404s,
	// exactly as it did when this was a sub-router mounted under the prefix.
	// "/+" and not "/*": the root itself is a route here, and a catch-all that
	// also matched the empty remainder would take it.
	router.All("/+", authenticated.Adapt(http.NotFound))
}

// NewRouter returns the v2 API as a standalone net/http handler.
func NewRouter(
	systemController systemcontroller.Controller,
	authenticator jwt.Authenticator,
	version string,
	opts ...RouterOption,
) http.Handler {
	app := common.NewApp()
	Register(app.Group(""), "", nil, systemController, authenticator, version, opts...)
	return common.Handler(app)
}

type routerOptions struct {
	tracer               trace.Tracer
	bulkerFactory        bulking.BulkerFactory
	bulkHandlerFactories map[string]bulking.HandlerFactory
	paginationConfig     storagecommon.PaginationConfig
	exporters            bool
	experimentalFeatures []string
}

type RouterOption func(ro *routerOptions)

func WithTracer(tracer trace.Tracer) RouterOption {
	return func(ro *routerOptions) {
		ro.tracer = tracer
	}
}

func WithBulkHandlerFactories(bulkHandlerFactories map[string]bulking.HandlerFactory) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkHandlerFactories = bulkHandlerFactories
	}
}

func WithBulkerFactory(bulkerFactory bulking.BulkerFactory) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkerFactory = bulkerFactory
	}
}

func WithPaginationConfig(paginationConfig storagecommon.PaginationConfig) RouterOption {
	return func(ro *routerOptions) {
		ro.paginationConfig = paginationConfig
	}
}

func WithExporters(v bool) RouterOption {
	return func(ro *routerOptions) {
		ro.exporters = v
	}
}

func WithExperimentalFeatures(features []string) RouterOption {
	return func(ro *routerOptions) {
		ro.experimentalFeatures = features
	}
}

func WithDefaultBulkHandlerFactories(bulkMaxSize int) RouterOption {
	return WithBulkHandlerFactories(map[string]bulking.HandlerFactory{
		"application/json": bulking.NewJSONBulkHandlerFactory(bulkMaxSize),
		"application/vnd.formance.ledger.api.v2.bulk+script-stream": bulking.NewTextStreamBulkHandlerFactory(),
		"application/vnd.formance.ledger.api.v2.bulk+json-stream":   bulking.NewJSONStreamBulkHandlerFactory(),
	})
}

var defaultRouterOptions = []RouterOption{
	WithTracer(nooptracer.Tracer{}),
	WithBulkerFactory(bulking.NewDefaultBulkerFactory()),
	WithDefaultBulkHandlerFactories(100),
	WithPaginationConfig(storagecommon.PaginationConfig{
		DefaultPageSize: paginate.QueryDefaultPageSize,
		MaxPageSize:     paginate.MaxPageSize,
	}),
}
