package api

import (
	"fmt"
	"net/http"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
	"github.com/hanzo-fi/go-libs/v5/pkg/audit/httpaudit"
	"github.com/hanzo-fi/go-libs/v5/pkg/authn/jwt"
	"github.com/hanzo-fi/go-libs/v5/pkg/observe"
	logging "github.com/hanzo-fi/go-libs/v5/pkg/observe/log"
	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/hanzo-fi/go-libs/v5/pkg/transport/api"
	"github.com/hanzo-fi/go-libs/v5/pkg/transport/httpserver"
	otelchimetric "github.com/riandyrn/otelchi/metric"
	"github.com/zap-proto/zip"
	"go.opentelemetry.io/otel/metric"
	noopmetrics "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"

	"github.com/hanzo-fi/ledger/internal/api/bulking"
	"github.com/hanzo-fi/ledger/internal/api/common"
	v1 "github.com/hanzo-fi/ledger/internal/api/v1"
	v2 "github.com/hanzo-fi/ledger/internal/api/v2"
	"github.com/hanzo-fi/ledger/internal/controller/system"
	storagecommon "github.com/hanzo-fi/ledger/internal/storage/common"
)

// ServiceMiddleware is what every route the ledger serves — API and operational
// alike — runs inside: a JSON content-type default and the service logger,
// staged before anything else so a handler can override both.
func ServiceMiddleware(logger logging.Logger) common.Chain {
	return common.Chain{
		func(handler http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")

				handler.ServeHTTP(w, r.WithContext(logging.ContextWithLogger(r.Context(), logger)))
			})
		},
	}
}

// todo: refine textual errors

// NewRouter returns the app the ledger's API is routed by. Routing is zip's;
// the handlers and the observability stack around them are net/http, and each
// route is registered as the whole chain applied to its handler — so the chain
// still runs outside the handler, sees the response the handler wrote, and
// reads the route the router matched.
func NewRouter(
	logger logging.Logger,
	systemController system.Controller,
	authenticator jwt.Authenticator,
	publisher message.Publisher,
	version string,
	debug bool,
	opts ...RouterOption,
) *zip.App {

	routerOptions := routerOptions{}
	for _, opt := range append(defaultRouterOptions, opts...) {
		opt(&routerOptions)
	}

	baseCfg := otelchimetric.NewBaseConfig(
		"ledger",
		otelchimetric.WithMeterProvider(routerOptions.meterProvider),
	)

	app := common.NewApp()
	base := ServiceMiddleware(logger).With(
		cors.New(cors.Options{
			AllowOriginFunc: func(r *http.Request, origin string) bool {
				return true
			},
			AllowCredentials: true,
			AllowedHeaders:   []string{"*"},
			ExposedHeaders:   []string{"Count"},
		}).Handler,
		common.LogID(),
		middleware.RequestLogger(api.NewLogFormatter()),
		httpserver.OTLPMiddleware("ledger", debug),
		httpaudit.Middleware(publisher, auditEventTopic, auditAppName, nil, routerOptions.auditHTTPOptions...),
		otelchimetric.NewRequestDurationMillis(baseCfg),
		otelchimetric.NewRequestInFlight(baseCfg),
		otelchimetric.NewResponseSizeBytes(baseCfg),
		func(next http.Handler) http.Handler {
			fn := func(w http.ResponseWriter, r *http.Request) {
				defer func() {
					if rvr := recover(); rvr != nil {
						if rvr == http.ErrAbortHandler {
							// we don't recover http.ErrAbortHandler so the response
							// to the client is aborted, this should not be logged
							panic(rvr)
						}

						if debug {
							middleware.PrintPrettyStack(rvr)
						}

						observe.RecordError(r.Context(), fmt.Errorf("%s", rvr))

						w.WriteHeader(http.StatusInternalServerError)
					}
				}()

				next.ServeHTTP(w, r)
			}

			return http.HandlerFunc(fn)
		},
	)

	v2.Register(
		app.Group("/v2"),
		"/v2",
		base,
		systemController,
		authenticator,
		version,
		v2.WithTracer(routerOptions.tracer),
		v2.WithBulkerFactory(routerOptions.bulkerFactory),
		v2.WithDefaultBulkHandlerFactories(routerOptions.bulkMaxSize),
		v2.WithPaginationConfig(routerOptions.paginationConfig),
		v2.WithExporters(routerOptions.exporters),
		v2.WithExperimentalFeatures(routerOptions.experimentalFeatures),
	)
	v1.Register(
		app.Group(""),
		base,
		systemController,
		authenticator,
		version,
		debug,
		v1.WithTracer(routerOptions.tracer),
		v1.WithExperimentalFeatures(routerOptions.experimentalFeatures),
	)

	// A path matching neither version 404s without authenticating, as it did
	// when v1 was the mounted fallback and had no route for it either.
	app.All("/*", base.Adapt(http.NotFound))

	return app
}

type routerOptions struct {
	tracer               trace.Tracer
	meterProvider        metric.MeterProvider
	bulkMaxSize          int
	bulkerFactory        bulking.BulkerFactory
	paginationConfig     storagecommon.PaginationConfig
	exporters            bool
	experimentalFeatures []string
	auditHTTPOptions     []httpaudit.HTTPOption
}

type RouterOption func(ro *routerOptions)

func WithTracer(tracer trace.Tracer) RouterOption {
	return func(ro *routerOptions) {
		ro.tracer = tracer
	}
}

func WithBulkMaxSize(bulkMaxSize int) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkMaxSize = bulkMaxSize
	}
}

func WithBulkerFactory(bf bulking.BulkerFactory) RouterOption {
	return func(ro *routerOptions) {
		ro.bulkerFactory = bf
	}
}

func WithPaginationConfiguration(paginationConfig storagecommon.PaginationConfig) RouterOption {
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

func WithAuditHTTPOptions(options ...httpaudit.HTTPOption) RouterOption {
	return func(ro *routerOptions) {
		ro.auditHTTPOptions = options
	}
}

func WithMeterProvider(mp metric.MeterProvider) RouterOption {
	return func(ro *routerOptions) {
		ro.meterProvider = mp
	}
}

var defaultRouterOptions = []RouterOption{
	WithTracer(nooptracer.Tracer{}),
	WithMeterProvider(noopmetrics.MeterProvider{}),
	WithBulkMaxSize(DefaultBulkMaxSize),
	WithPaginationConfiguration(storagecommon.PaginationConfig{
		MaxPageSize:     paginate.MaxPageSize,
		DefaultPageSize: paginate.QueryDefaultPageSize,
	}),
}

const DefaultBulkMaxSize = 100
