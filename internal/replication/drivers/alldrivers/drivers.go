package alldrivers

import (
	"github.com/hanzo-fi/ledger/internal/replication/drivers"
	"github.com/hanzo-fi/ledger/internal/replication/drivers/clickhouse"
	"github.com/hanzo-fi/ledger/internal/replication/drivers/elasticsearch"
	"github.com/hanzo-fi/ledger/internal/replication/drivers/http"
	"github.com/hanzo-fi/ledger/internal/replication/drivers/noop"
	"github.com/hanzo-fi/ledger/internal/replication/drivers/stdout"
)

func Register(driversRegistry *drivers.Registry) {
	driversRegistry.RegisterDriver("elasticsearch", elasticsearch.NewDriver)
	driversRegistry.RegisterDriver("clickhouse", clickhouse.NewDriver)
	driversRegistry.RegisterDriver("stdout", stdout.NewDriver)
	driversRegistry.RegisterDriver("http", http.NewDriver)
	driversRegistry.RegisterDriver("noop", noop.NewDriver)
}
