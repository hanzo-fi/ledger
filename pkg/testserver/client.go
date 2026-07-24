package testserver

import (
	"github.com/hanzo-fi/go-libs/v5/pkg/testing/deferred"
	"github.com/hanzo-fi/go-libs/v5/pkg/testing/testservice"

	ledgerclient "github.com/hanzo-fi/ledger/pkg/client"
)

func Client(srv *testservice.Service) *ledgerclient.Formance {
	return ledgerclient.New(
		ledgerclient.WithServerURL(testservice.GetServerURL(srv).String()),
	)
}

func DeferClient(srv *deferred.Deferred[*testservice.Service]) *deferred.Deferred[*ledgerclient.Formance] {
	return deferred.Map(srv, Client)
}
