package ginkgo

import (
	"github.com/hanzo-fi/go-libs/v5/pkg/storage/bun/connect"
	"github.com/hanzo-fi/go-libs/v5/pkg/testing/deferred"
	"github.com/hanzo-fi/go-libs/v5/pkg/testing/testservice"
	. "github.com/hanzo-fi/go-libs/v5/pkg/testing/testservice/ginkgo"

	"github.com/hanzo-fi/ledger/cmd"
	"github.com/hanzo-fi/ledger/pkg/testserver"
)

func DeferTestServer(postgresConnectionOptions *deferred.Deferred[connect.ConnectionOptions], options ...testservice.Option) *deferred.Deferred[*testservice.Service] {
	return DeferNew(
		cmd.NewRootCommand,
		append([]testservice.Option{
			testserver.GetTestServerOptions(postgresConnectionOptions),
		}, options...)...,
	)
}

func DeferTestWorker(postgresConnectionOptions *deferred.Deferred[connect.ConnectionOptions], options ...testservice.Option) *deferred.Deferred[*testservice.Service] {
	return DeferNew(
		cmd.NewRootCommand,
		append([]testservice.Option{
			testservice.WithInstruments(
				testservice.GRPCServerInstrumentation(),
				testservice.AppendArgsInstrumentation("worker"),
				testservice.PostgresInstrumentation(postgresConnectionOptions),
				testserver.GRPCAddressInstrumentation(":0"),
			),
		}, options...)...,
	)
}
