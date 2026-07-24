package cmd

import (
	"github.com/spf13/cobra"
	"go.uber.org/fx"

	"github.com/hanzo-fi/go-libs/v5/pkg/fx/observefx"
	"github.com/hanzo-fi/go-libs/v5/pkg/observe"
	logging "github.com/hanzo-fi/go-libs/v5/pkg/observe/log"
	"github.com/hanzo-fi/go-libs/v5/pkg/service"
	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/connect"

	"github.com/hanzo-fi/ledger/internal/storage"
	"github.com/hanzo-fi/ledger/internal/storage/bunconnect"
	"github.com/hanzo-fi/ledger/internal/storage/driver"
)

func NewBucketUpgrade() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "upgrade",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return withStorageDriver(cmd, func(driver *driver.Driver) error {
				if args[0] == "*" {
					return driver.UpgradeAllBuckets(cmd.Context())
				}

				return driver.UpgradeBucket(cmd.Context(), args[0])
			})
		},
	}

	service.AddFlags(cmd.Flags())
	connect.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())

	return cmd
}

func withStorageDriver(cmd *cobra.Command, fn func(driver *driver.Driver) error) error {

	logger := logging.NewDefaultLogger(cmd.OutOrStdout(), service.IsDebug(cmd), false, false)

	connectionOptions, err := connect.ConnectionOptionsFromFlags(cmd.Flags(), cmd.Context())
	if err != nil {
		return err
	}

	storageDriver, sqliteDSN, err := bunconnect.FromFlags(cmd.Flags())
	if err != nil {
		return err
	}

	var d *driver.Driver
	app := fx.New(
		fx.NopLogger,
		observefx.ResourceModuleFromFlags(cmd, observe.WithServiceVersion(Version)),
		observefx.TracesModuleFromFlags(cmd),
		bunconnect.Module(storageDriver, *connectionOptions, sqliteDSN, service.IsDebug(cmd)),
		storage.NewFXModule(storage.ModuleConfig{}),
		fx.Supply(fx.Annotate(logger, fx.As(new(logging.Logger)))),
		fx.Populate(&d),
	)
	err = app.Start(cmd.Context())
	if err != nil {
		return err
	}
	defer func() {
		_ = app.Stop(cmd.Context())
	}()

	return fn(d)
}
