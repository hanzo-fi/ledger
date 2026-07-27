package cmd

import (
	"context"
	"fmt"
	"net/http"
	"net/http/pprof"
	"time"

	"github.com/spf13/cobra"
	"github.com/zap-proto/zip"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/hanzo-fi/go-libs/v5/pkg/audit"
	"github.com/hanzo-fi/go-libs/v5/pkg/authn/jwt"
	"github.com/hanzo-fi/go-libs/v5/pkg/cloud/aws/iam"
	"github.com/hanzo-fi/go-libs/v5/pkg/fx/authnfx"
	"github.com/hanzo-fi/go-libs/v5/pkg/fx/messagingfx"
	"github.com/hanzo-fi/go-libs/v5/pkg/fx/observefx"
	"github.com/hanzo-fi/go-libs/v5/pkg/fx/transportfx"
	"github.com/hanzo-fi/go-libs/v5/pkg/messaging/publish"
	"github.com/hanzo-fi/go-libs/v5/pkg/observe"
	logging "github.com/hanzo-fi/go-libs/v5/pkg/observe/log"
	"github.com/hanzo-fi/go-libs/v5/pkg/observe/metrics"
	"github.com/hanzo-fi/go-libs/v5/pkg/observe/traces"
	"github.com/hanzo-fi/go-libs/v5/pkg/service"
	"github.com/hanzo-fi/go-libs/v5/pkg/service/health"
	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/connect"
	apilib "github.com/hanzo-fi/go-libs/v5/pkg/transport/api"
	"github.com/hanzo-fi/go-libs/v5/pkg/transport/httpserver"

	"github.com/hanzo-fi/ledger/internal/api"
	"github.com/hanzo-fi/ledger/internal/api/common"
	"github.com/hanzo-fi/ledger/internal/bus"
	ledgercontroller "github.com/hanzo-fi/ledger/internal/controller/ledger"
	systemcontroller "github.com/hanzo-fi/ledger/internal/controller/system"
	"github.com/hanzo-fi/ledger/internal/replication"
	"github.com/hanzo-fi/ledger/internal/replication/drivers"
	"github.com/hanzo-fi/ledger/internal/replication/drivers/alldrivers"
	"github.com/hanzo-fi/ledger/internal/storage"
	"github.com/hanzo-fi/ledger/internal/storage/bunconnect"
	storagecommon "github.com/hanzo-fi/ledger/internal/storage/common"
	systemstore "github.com/hanzo-fi/ledger/internal/storage/system"
	"github.com/hanzo-fi/ledger/internal/tracing"
	"github.com/hanzo-fi/ledger/internal/worker"
)

type ServeCommandConfig struct {
	commonConfig        `mapstructure:",squash"`
	WorkerConfiguration `mapstructure:",squash"`

	Bind                    string `mapstructure:"bind"`
	BallastSizeInBytes      uint   `mapstructure:"ballast-size"`
	NumscriptCacheMaxCount  uint   `mapstructure:"numscript-cache-max-count"`
	AutoUpgrade             bool   `mapstructure:"auto-upgrade"`
	BulkMaxSize             int    `mapstructure:"bulk-max-size"`
	BulkParallel            int    `mapstructure:"bulk-parallel"`
	DefaultPageSize         uint64 `mapstructure:"default-page-size"`
	MaxPageSize             uint64 `mapstructure:"max-page-size"`
	WorkerEnabled           bool   `mapstructure:"worker"`
	WorkerAddress           string `mapstructure:"worker-grpc-address"`
	AuditEnabled            bool   `mapstructure:"audit-enabled"`
	AuditAsyncEnabled       bool   `mapstructure:"audit-async-enabled"`
	AuditAsyncQueueCapacity int    `mapstructure:"audit-async-queue-capacity"`
	AuditAsyncWorkerCount   int    `mapstructure:"audit-async-worker-count"`

	DisableLedgerScopeOptimization bool `mapstructure:"disable-ledger-scope-optimization"`
}

const (
	BindFlag                   = "bind"
	BallastSizeInBytesFlag     = "ballast-size"
	NumscriptCacheMaxCountFlag = "numscript-cache-max-count"
	AutoUpgradeFlag            = "auto-upgrade"
	BulkMaxSizeFlag            = "bulk-max-size"
	BulkParallelFlag           = "bulk-parallel"

	DefaultPageSizeFlag   = "default-page-size"
	MaxPageSizeFlag       = "max-page-size"
	WorkerEnabledFlag     = "worker"
	SemconvMetricsNames   = "semconv-metrics-names"
	SchemaEnforcementMode = "schema-enforcement-mode"

	AuditAsyncEnabledFlag       = "audit-async-enabled"
	AuditAsyncQueueCapacityFlag = "audit-async-queue-capacity"
	AuditAsyncWorkerCountFlag   = "audit-async-worker-count"

	DisableLedgerScopeOptimizationFlag = "disable-ledger-scope-optimization"
)

func NewServeCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "serve",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {

			cfg, err := LoadConfig[ServeCommandConfig](cmd)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			if err := cfg.Validate(); err != nil {
				return err
			}

			connectionOptions, err := connect.ConnectionOptionsFromFlags(cmd.Flags(), cmd.Context())
			if err != nil {
				return err
			}

			storageDriver, sqliteDSN, err := bunconnect.FromFlags(cmd.Flags())
			if err != nil {
				return err
			}

			options := []fx.Option{
				fx.NopLogger,
				otlpModule(cmd, cfg.commonConfig),
				messagingfx.PublishModuleFromFlags(cmd, service.IsDebug(cmd)),
				authnfx.JWTModuleFromFlags(cmd),
				fx.Supply(connectionOptions),
				bunconnect.Module(storageDriver, *connectionOptions, sqliteDSN, service.IsDebug(cmd)),
				storage.NewFXModule(storage.ModuleConfig{
					AutoUpgrade:                     cfg.AutoUpgrade,
					DisableScopedSelectOptimization: cfg.DisableLedgerScopeOptimization,
				}),
				drivers.NewFXModule(),
				fx.Invoke(alldrivers.Register),
				systemcontroller.NewFXModule(systemcontroller.ModuleConfiguration{
					NumscriptInterpreter:      cfg.NumscriptInterpreter,
					NumscriptInterpreterFlags: cfg.NumscriptInterpreterFlags,
					NSCacheConfiguration: ledgercontroller.CacheConfiguration{
						MaxCount: cfg.NumscriptCacheMaxCount,
					},
					DatabaseRetryConfiguration: systemcontroller.DatabaseRetryConfiguration{
						MaxRetry: 10,
						Delay:    time.Millisecond * 100,
					},
					EnableFeatures:        cfg.ExperimentalFeaturesEnabled,
					SchemaEnforcementMode: cfg.commonConfig.SchemaEnforcementMode,
				}),
				bus.NewFxModule(),
				ballastModule(cfg.BallastSizeInBytes),
				api.Module(api.Config{
					Version: Version,
					Debug:   service.IsDebug(cmd),
					Bulk: api.BulkConfig{
						MaxSize:  cfg.BulkMaxSize,
						Parallel: cfg.BulkParallel,
					},
					Pagination: storagecommon.PaginationConfig{
						MaxPageSize:     cfg.MaxPageSize,
						DefaultPageSize: cfg.DefaultPageSize,
					},
					Exporters:            cfg.ExperimentalExporters,
					ExperimentalFeatures: experimentalFeatures(cfg),
					Audit: api.AuditConfig{
						Enabled:            cfg.AuditEnabled,
						AsyncEnabled:       cfg.AuditAsyncEnabled,
						AsyncQueueCapacity: cfg.AuditAsyncQueueCapacity,
						AsyncWorkerCount:   cfg.AuditAsyncWorkerCount,
					},
				}),
				fx.Provide(func(
					params struct {
						fx.In

						App              *zip.App
						HealthController *health.HealthController
						Logger           logging.Logger

						MeterProvider *metric.MeterProvider     `optional:"true"`
						Exporter      *metrics.InMemoryExporter `optional:"true"`
					},
				) http.Handler {
					return assembleFinalRouter(
						service.IsDebug(cmd),
						params.MeterProvider,
						params.Exporter,
						params.HealthController,
						params.Logger,
						params.App,
					)
				}),
				fx.Invoke(func(lc fx.Lifecycle, h http.Handler) {
					lc.Append(transportfx.FXHook(httpserver.NewHook(h, httpserver.WithAddress(cfg.Bind))))
				}),
			}

			if cfg.WorkerEnabled {
				options = append(options,
					newWorkerModule(cfg.WorkerConfiguration),
					replication.NewFXEmbeddedClientModule(),
				)
			} else {
				options = append(options,
					worker.NewGRPCClientFxModule(
						cfg.WorkerAddress,
						grpc.WithTransportCredentials(insecure.NewCredentials()),
					),
					replication.NewFXGRPCClientModule(),
				)
			}

			return service.New(cmd.OutOrStdout(), options...).Run(cmd)
		},
	}
	cmd.Flags().Uint(BallastSizeInBytesFlag, 0, "Ballast size in bytes, default to 0")
	cmd.Flags().Uint(NumscriptCacheMaxCountFlag, 1024, "Numscript cache max count")
	cmd.Flags().Bool(AutoUpgradeFlag, false, "Automatically upgrade all schemas")
	cmd.Flags().String(BindFlag, "0.0.0.0:3068", "API bind address")
	cmd.Flags().Int(BulkMaxSizeFlag, api.DefaultBulkMaxSize, "Bulk max size (default 100)")
	cmd.Flags().Int(BulkParallelFlag, 10, "Bulk max parallelism")
	cmd.Flags().Uint64(MaxPageSizeFlag, 100, "Max page size")
	cmd.Flags().Uint64(DefaultPageSizeFlag, 15, "Default page size")
	cmd.Flags().Bool(WorkerEnabledFlag, false, "Enable worker")
	cmd.Flags().Bool(ExperimentalFeaturesFlag, false, "Enable features configurability")
	cmd.Flags().Bool(NumscriptInterpreterFlag, false, "Enable experimental numscript rewrite")
	cmd.Flags().StringSlice(NumscriptInterpreterFlagsToPass, nil, "Feature flags to pass to the experimental numscript interpreter")
	cmd.Flags().String(WorkerGRPCAddressFlag, "localhost:8081", "GRPC address")
	cmd.Flags().Bool(SemconvMetricsNames, false, "Use semconv metrics names (recommended)")
	cmd.Flags().String(SchemaEnforcementMode, "audit", "Schema enforcement mode. Values: `audit`, `strict`")
	cmd.Flags().Bool(audit.AuditEnabledFlag, true, "Enable HTTP audit")
	cmd.Flags().Bool(AuditAsyncEnabledFlag, true, "Publish HTTP audit events asynchronously")
	cmd.Flags().Int(AuditAsyncQueueCapacityFlag, api.DefaultAuditAsyncQueueCapacity, "HTTP audit async publish queue capacity")
	cmd.Flags().Int(AuditAsyncWorkerCountFlag, api.DefaultAuditAsyncWorkerCount, "HTTP audit async publish worker count")
	cmd.Flags().Bool(DisableLedgerScopeOptimizationFlag, false, "Always emit the `ledger = ?` predicate on read queries, disabling the alone-in-bucket optimization that skips it when a ledger is the only one in its bucket")

	addWorkerFlags(cmd)
	connect.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())
	observe.AddFlags(cmd.Flags())
	metrics.AddFlags(cmd.Flags())
	traces.AddFlags(cmd.Flags())
	jwt.AddFlags(cmd.Flags())
	publish.AddFlags(ServiceName, cmd.Flags(), func(cd *publish.ConfigDefault) {
		cd.PublisherCircuitBreakerSchema = systemstore.SchemaSystem
	})
	iam.AddFlags(cmd.Flags())

	return cmd
}

// assembleFinalRouter adds the operational endpoints to the API's router and
// returns the whole thing as the handler the server serves. One router answers
// both: an /_ path is answered here and never falls through to the API, where a
// leading segment is read as a ledger name.
func assembleFinalRouter(
	exportPProf bool,
	meterProvider *metric.MeterProvider,
	exporter *metrics.InMemoryExporter,
	healthController *health.HealthController,
	logger logging.Logger,
	app *zip.App,
) http.Handler {
	base := api.ServiceMiddleware(logger)

	ops := app.Group("/_")
	if exporter != nil {
		ops.All("/metrics", base.Adapt(metrics.NewInMemoryExporterHandler(
			meterProvider,
			exporter,
		).ServeHTTP))
	}
	if exportPProf {
		ops.All("/debug/pprof/*", base.Adapt(http.StripPrefix(
			"/_",
			http.HandlerFunc(pprof.Index),
		).ServeHTTP))
	}
	ops.All("/healthcheck", base.Adapt(healthController.Check))
	ops.Get("/info", base.Adapt(func(w http.ResponseWriter, r *http.Request) {
		apilib.RawOk(w, struct {
			Server  string `json:"server"`
			Version string `json:"version"`
		}{
			Server:  "ledger",
			Version: Version,
		})
	}))
	ops.All("/*", base.Adapt(http.NotFound))

	app.Get("/_healthcheck", base.Adapt(healthController.Check))

	return common.Handler(app)
}

func ballastModule(sizeInBytes uint) fx.Option {
	if sizeInBytes == 0 {
		return fx.Options()
	}
	return fx.Invoke(func(lc fx.Lifecycle) {
		var ballast []byte
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				ballast = make([]byte, 0, sizeInBytes)
				_ = ballast
				return nil
			},
			OnStop: func(ctx context.Context) error {
				ballast = nil
				return nil
			},
		})
	})
}

func experimentalFeatures(cfg *ServeCommandConfig) []string {
	var features []string
	if cfg.ExperimentalFeaturesEnabled {
		features = append(features, ExperimentalFeaturesFlag)
	}
	if cfg.ExperimentalExporters {
		features = append(features, ExperimentalExporters)
	}
	if cfg.NumscriptInterpreter {
		features = append(features, NumscriptInterpreterFlag)
	}
	return features
}

func otlpModule(cmd *cobra.Command, cfg commonConfig) fx.Option {
	return fx.Options(
		observefx.ResourceModuleFromFlags(cmd, observe.WithServiceVersion(Version)),
		observefx.TracesModuleFromFlags(cmd),
		observefx.ProvideMetricsProviderOption(func() metric.Option {
			return metric.WithView(func(instrument metric.Instrument) (metric.Stream, bool) {
				if cfg.SemconvMetricsNames {
					return metric.Stream{}, false
				}
				return metric.Stream{
					Name:        tracing.LegacyMetricsName(instrument.Name),
					Description: instrument.Description,
					Unit:        instrument.Unit,
				}, true
			})
		}),
		observefx.MetricsModuleFromFlags(cmd),
	)
}
