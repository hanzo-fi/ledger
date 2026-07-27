package api

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/hanzo-fi/go-libs/v5/pkg/authn/jwt"
	logging "github.com/hanzo-fi/go-libs/v5/pkg/observe/log"
	"github.com/stretchr/testify/require"
	"github.com/zap-proto/zip"

	"github.com/hanzo-fi/ledger/internal/api/common"
)

// apiSurface is the URL surface the ledger serves, method by method. It is the
// contract this package exists to hold still: a route that moves, gains or
// loses a method, or picks up a stray path segment breaks a client, so it has
// to break a test first.
//
// The fallback patterns that answer everything else are asserted separately,
// in TestFallbackRoutes.
var apiSurface = []string{
	// v1
	"GET     /_info",
	"GET     /:ledger/_info",
	"GET     /:ledger/stats",
	"GET     /:ledger/logs",
	"GET     /:ledger/accounts",
	"HEAD    /:ledger/accounts",
	"GET     /:ledger/accounts/:address",
	"POST    /:ledger/accounts/:address/metadata",
	"DELETE  /:ledger/accounts/:address/metadata/:key",
	"GET     /:ledger/transactions",
	"HEAD    /:ledger/transactions",
	"POST    /:ledger/transactions",
	"POST    /:ledger/transactions/batch",
	"GET     /:ledger/transactions/:id",
	"POST    /:ledger/transactions/:id/revert",
	"POST    /:ledger/transactions/:id/metadata",
	"DELETE  /:ledger/transactions/:id/metadata/:key",
	"GET     /:ledger/balances",
	"GET     /:ledger/aggregate/balances",

	// v2
	"GET     /v2/_info",
	"DELETE  /v2/_/buckets/:bucket",
	"POST    /v2/_/buckets/:bucket/restore",
	"GET     /v2/",
	"POST    /v2/:ledger/",
	"GET     /v2/:ledger/",
	"PUT     /v2/:ledger/metadata",
	"DELETE  /v2/:ledger/metadata/:key",
	"POST    /v2/:ledger/_bulk",
	"GET     /v2/:ledger/_info",
	"GET     /v2/:ledger/stats",
	"POST    /v2/:ledger/schemas/:version",
	"GET     /v2/:ledger/schemas/:version",
	"GET     /v2/:ledger/schemas",
	"GET     /v2/:ledger/logs/",
	"POST    /v2/:ledger/logs/import",
	"POST    /v2/:ledger/logs/export",
	"GET     /v2/:ledger/accounts/",
	"HEAD    /v2/:ledger/accounts/",
	"GET     /v2/:ledger/accounts/:address",
	"POST    /v2/:ledger/accounts/:address/metadata",
	"DELETE  /v2/:ledger/accounts/:address/metadata/:key",
	"GET     /v2/:ledger/transactions/",
	"HEAD    /v2/:ledger/transactions/",
	"POST    /v2/:ledger/transactions/",
	"GET     /v2/:ledger/transactions/:id",
	"POST    /v2/:ledger/transactions/:id/revert",
	"POST    /v2/:ledger/transactions/:id/metadata",
	"DELETE  /v2/:ledger/transactions/:id/metadata/:key",
	"GET     /v2/:ledger/aggregate/balances",
	"GET     /v2/:ledger/volumes",
	"POST    /v2/:ledger/queries/:id/run",
}

// exporterSurface is mounted only when the exporters feature is on.
var exporterSurface = []string{
	"GET     /v2/_/exporters/",
	"GET     /v2/_/exporters/:exporterID",
	"PUT     /v2/_/exporters/:exporterID",
	"DELETE  /v2/_/exporters/:exporterID",
	"POST    /v2/_/exporters/",
	"GET     /v2/:ledger/pipelines/",
	"POST    /v2/:ledger/pipelines/",
	"GET     /v2/:ledger/pipelines/:pipelineID/",
	"DELETE  /v2/:ledger/pipelines/:pipelineID/",
	"POST    /v2/:ledger/pipelines/:pipelineID/start",
	"POST    /v2/:ledger/pipelines/:pipelineID/stop",
	"POST    /v2/:ledger/pipelines/:pipelineID/reset",
}

// fallbacks answer any path the surface above has no route for. They are
// registered for every method, and they exist so that a miss under a ledger
// still runs that ledger's middleware before it 404s, and a miss under /v2 is
// still answered by v2 rather than being read as a v1 ledger name.
var fallbacks = []string{"/*", "/:ledger/*", "/v2/+"}

func TestAPISurface(t *testing.T) {
	t.Parallel()

	t.Run("with exporters", func(t *testing.T) {
		t.Parallel()
		require.ElementsMatch(t, append(slices.Clone(apiSurface), exporterSurface...), routesOf(t, true))
	})

	t.Run("without exporters", func(t *testing.T) {
		t.Parallel()
		require.ElementsMatch(t, apiSurface, routesOf(t, false))
	})
}

func TestFallbackRoutes(t *testing.T) {
	t.Parallel()

	methods := map[string][]string{}
	for _, route := range newTestRouter(t, true).Fiber().GetRoutes() {
		if slices.Contains(fallbacks, route.Path) {
			methods[route.Path] = append(methods[route.Path], route.Method)
		}
	}

	require.Len(t, methods, len(fallbacks), "every fallback must be registered")
	for path, registered := range methods {
		require.Subset(t, registered, []string{
			http.MethodGet, http.MethodHead, http.MethodPost,
			http.MethodPut, http.MethodPatch, http.MethodDelete,
		}, "fallback %s must answer every method", path)
	}
}

// TestRouting pins which route answers a request, on the real route table. The
// table is full of patterns that overlap — a ledger name is a bare path
// segment, so it competes with /_info, with /v2, and with every fallback — and
// which one wins is the whole difference between an API and a 404.
func TestRouting(t *testing.T) {
	t.Parallel()

	router := matcherFor(t, newTestRouter(t, true))

	for _, tc := range []struct {
		method, path, expect string
	}{
		// A version prefix beats a ledger name; a reserved name beats both.
		{http.MethodGet, "/_info", "/_info"},
		{http.MethodGet, "/v2/_info", "/v2/_info"},
		{http.MethodGet, "/v2/", "/v2/"},
		{http.MethodGet, "/v2", "/v2/"},
		{http.MethodGet, "/v2/xxx", "/v2/:ledger/"},
		{http.MethodGet, "/v2/xxx/", "/v2/:ledger/"},
		{http.MethodPost, "/v2/xxx", "/v2/:ledger/"},
		{http.MethodGet, "/v2/_/exporters", "/v2/_/exporters/"},
		{http.MethodGet, "/v2/_/exporters/", "/v2/_/exporters/"},
		{http.MethodGet, "/v2/_/exporters/e1", "/v2/_/exporters/:exporterID"},
		{http.MethodDelete, "/v2/_/buckets/b1", "/v2/_/buckets/:bucket"},

		// A collection answers with or without its trailing slash.
		{http.MethodGet, "/xxx/accounts", "/:ledger/accounts"},
		{http.MethodGet, "/xxx/accounts/", "/:ledger/accounts"},
		{http.MethodHead, "/xxx/accounts", "/:ledger/accounts"},
		{http.MethodGet, "/v2/xxx/accounts", "/v2/:ledger/accounts/"},
		{http.MethodGet, "/v2/xxx/accounts/", "/v2/:ledger/accounts/"},
		{http.MethodHead, "/v2/xxx/transactions", "/v2/:ledger/transactions/"},

		// A parameter never swallows a sibling literal.
		{http.MethodGet, "/xxx/accounts/bob", "/:ledger/accounts/:address"},
		{http.MethodPost, "/xxx/transactions/batch", "/:ledger/transactions/batch"},
		{http.MethodGet, "/xxx/transactions/0", "/:ledger/transactions/:id"},
		{http.MethodGet, "/xxx/aggregate/balances", "/:ledger/aggregate/balances"},
		{http.MethodGet, "/v2/xxx/schemas", "/v2/:ledger/schemas"},
		{http.MethodGet, "/v2/xxx/schemas/1.2.3", "/v2/:ledger/schemas/:version"},
		{http.MethodPost, "/v2/xxx/pipelines/p1/start", "/v2/:ledger/pipelines/:pipelineID/start"},

		// An encoded separator stays inside one segment.
		{http.MethodGet, "/xxx/accounts/foo%2Fbar", "/:ledger/accounts/:address"},
		{http.MethodDelete, "/xxx/accounts/foo%2Fbar/metadata/k", "/:ledger/accounts/:address/metadata/:key"},

		// Everything else falls through, and never past its own version.
		{http.MethodGet, "/", "/*"},
		{http.MethodGet, "/xxx", "/:ledger/*"},
		{http.MethodGet, "/xxx/nope", "/:ledger/*"},
		{http.MethodPost, "/xxx/nope/deeper", "/:ledger/*"},
		{http.MethodGet, "/v2/xxx/nope", "/v2/+"},
		{http.MethodGet, "/v2/nope/deeper", "/v2/+"},
	} {
		t.Run(tc.method+" "+tc.path, func(t *testing.T) {
			require.Equal(t, tc.expect, router(tc.method, tc.path))
		})
	}
}

func newTestRouter(t *testing.T, exporters bool) *zip.App {
	t.Helper()

	return NewRouter(
		logging.Testing(),
		nil,
		jwt.NewNoAuth(),
		nil,
		"develop",
		false,
		WithExporters(exporters),
	)
}

// routesOf renders an app's API routes the way apiSurface writes them.
func routesOf(t *testing.T, exporters bool) []string {
	t.Helper()

	var routes []string
	for _, route := range newTestRouter(t, exporters).Fiber().GetRoutes() {
		if slices.Contains(fallbacks, route.Path) {
			continue
		}
		routes = append(routes, fmt.Sprintf("%-7s %s", route.Method, route.Path))
	}
	return routes
}

// matcherFor rebuilds an app's route table over handlers that report which
// pattern they were reached through, so a test can ask the real table what it
// matches without standing up a ledger behind every route.
func matcherFor(t *testing.T, app *zip.App) func(method, path string) string {
	t.Helper()

	mirror := common.NewApp()
	for _, route := range app.Fiber().GetRoutes() {
		pattern := route.Path
		report := common.Adapt(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = w.Write([]byte(pattern))
		})
		switch route.Method {
		case http.MethodGet:
			mirror.Get(pattern, report)
		case http.MethodHead:
			mirror.Head(pattern, report)
		case http.MethodPost:
			mirror.Post(pattern, report)
		case http.MethodPut:
			mirror.Put(pattern, report)
		case http.MethodPatch:
			mirror.Patch(pattern, report)
		case http.MethodDelete:
			mirror.Delete(pattern, report)
		}
	}

	serve := common.Handler(mirror)
	return func(method, path string) string {
		request := httptest.NewRequest(method, "/", nil)
		request.URL.Path = path
		recorder := httptest.NewRecorder()
		serve.ServeHTTP(recorder, request)
		return recorder.Body.String()
	}
}
