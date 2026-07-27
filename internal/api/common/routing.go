package common

import (
	"context"
	"net/http"
	"strings"
	"sync"

	"github.com/go-chi/chi/v5"
	luxlog "github.com/luxfi/log"
	"github.com/valyala/fasthttp"
	"github.com/zap-proto/zip"
)

// The API is routed by zip. Its handlers and middleware are net/http. This file
// is the only seam between the two, and it is deliberately narrow: zip owns
// path and method matching, and nothing else.
//
// A matched request is handed to its handler as the request the server parsed,
// writing to the writer the server opened — the router is asked which handler,
// not asked to rebuild the request. That is what keeps a streaming export
// streaming, keeps cancellation, flushing and the raw request path intact, and
// keeps a route's middleware chain composing and running exactly as it did
// under the router this replaces.

// NewApp constructs the zip app the API is routed by. zip's own logger is
// silenced: the ledger logs through go-libs, and a second logging stack on the
// same requests is noise, not observability.
func NewApp() *zip.App {
	return zip.New(zip.Config{
		AppName:               "ledger",
		Logger:                luxlog.NewNoOpLogger(),
		DisableStartupMessage: true,
		ServerHeader:          "-",
	})
}

// Handler serves an app's routes over net/http.
//
// go-libs' httpserver hook owns the listener, TLS and graceful shutdown, so the
// app answers a request at a time rather than through zip.Listen. Only the
// method and the request target are handed to the router; the request itself
// travels past it untouched, and is picked up by the matched route in adapt.
func Handler(app *zip.App) http.Handler {
	route := app.Fiber().Handler()

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		target := fasthttp.AcquireRequest()
		defer fasthttp.ReleaseRequest(target)

		target.Header.SetMethod(r.Method)
		target.SetRequestURI(requestTarget(r))

		ctx := contexts.Get().(*fasthttp.RequestCtx)
		defer func() {
			// Init does not clear these, and they hold the exchange: a pooled
			// context must not outlive the request it carried.
			ctx.ResetUserValues()
			contexts.Put(ctx)
		}()
		ctx.Request.Reset()
		ctx.Response.Reset()
		ctx.Init(target, nil, silent{})

		pending := &request{w: w, r: r}
		ctx.SetUserValue(requestKey, pending)

		route(ctx)

		if !pending.handled {
			// No route claimed it, so the router answered on its own.
			writeResponse(w, &ctx.Response)
		}
	})
}

// Chain is a net/http middleware stack, outermost first. Every route is
// registered as a chain applied to a handler, which is how the router this
// replaces composed them too — the difference is only that the composition is
// now explicit.
type Chain []func(http.Handler) http.Handler

// With returns the chain extended by mw, leaving c untouched so a broader chain
// can be reused as the base of several narrower ones.
func (c Chain) With(mw ...func(http.Handler) http.Handler) Chain {
	return append(append(make(Chain, 0, len(c)+len(mw)), c...), mw...)
}

// Adapt composes the chain around h and mounts the result on a zip route.
func (c Chain) Adapt(h http.HandlerFunc) zip.Handler {
	var wrapped http.Handler = h
	for i := len(c) - 1; i >= 0; i-- {
		wrapped = c[i](wrapped)
	}
	return adapt(wrapped)
}

// Adapt mounts a handler on a zip route with no middleware around it.
func Adapt(h http.HandlerFunc) zip.Handler { return Chain(nil).Adapt(h) }

// URLParam returns the path parameter name captured by the router for r, or ""
// when the matched route declares no such parameter. Handlers ask here and
// nowhere else.
func URLParam(r *http.Request, name string) string {
	return chi.URLParam(r, name)
}

// adapt hands the request waiting behind the router to h, with the matched
// route's parameters and pattern attached.
//
// The carrier is a chi route context because go-libs' observability middleware
// (otelchi, and the request metrics built on it) reads the route label from
// there — it is the one route-context type already required to be present, so
// URLParam reads it too rather than shadowing it with a second mechanism.
func adapt(h http.Handler) zip.Handler {
	// A handler is bound to one route, so its pattern is fixed after the first
	// request; rendering it per request would be pure garbage.
	var (
		once    sync.Once
		pattern string
	)

	return func(c *zip.Ctx) error {
		fc := c.Fiber()
		route := fc.Route()
		once.Do(func() { pattern = chiPattern(route.Path) })

		rctx := chi.NewRouteContext()
		rctx.RoutePatterns = []string{pattern}
		for _, name := range route.Params {
			rctx.URLParams.Add(name, fc.Params(name))
		}

		pending := fc.RequestCtx().UserValue(requestKey).(*request)
		pending.handled = true

		r := pending.r
		h.ServeHTTP(pending.w, r.WithContext(context.WithValue(r.Context(), chi.RouteCtxKey, rctx)))

		return nil
	}
}

// request is the live exchange, parked on the routing context for the matched
// route to pick up.
type request struct {
	w       http.ResponseWriter
	r       *http.Request
	handled bool
}

type requestKeyType struct{}

var requestKey = requestKeyType{}

// contexts pools the throwaway routing contexts: one per request, holding a
// method and a target and nothing else.
var contexts = sync.Pool{New: func() any { return new(fasthttp.RequestCtx) }}

// silent discards the router's internal logging, which has nothing to say about
// a request that never touches its transport.
type silent struct{}

func (silent) Printf(string, ...any) {}

// requestTarget renders what the router matches on: the escaped path when the
// request carries one, the path otherwise, plus the query verbatim. That is the
// rule the router this replaces used, and it is what lets a handler see a path
// segment exactly as the client wrote it — an account address holding an
// encoded separator stays one segment rather than becoming two.
func requestTarget(r *http.Request) string {
	target := escapeTarget(r.URL.RawPath)
	if target == "" {
		target = escapeTarget(r.URL.Path)
	}
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}
	return target
}

// escapeTarget percent-encodes the bytes a request target cannot carry —
// controls, space, and the query and fragment delimiters — and nothing else.
// '%' in particular is left alone, so an address holding a bad escape still
// reaches its handler to be rejected as invalid instead of being repaired on
// the way in. A path off the wire holds none of these bytes, so the ordinary
// case allocates nothing.
func escapeTarget(path string) string {
	i := 0
	for ; i < len(path); i++ {
		if mustEscape(path[i]) {
			break
		}
	}
	if i == len(path) {
		return path
	}

	escaped := strings.Builder{}
	escaped.Grow(len(path) + 8)
	escaped.WriteString(path[:i])
	for ; i < len(path); i++ {
		c := path[i]
		if !mustEscape(c) {
			escaped.WriteByte(c)
			continue
		}
		escaped.WriteByte('%')
		escaped.WriteByte(upperhex[c>>4])
		escaped.WriteByte(upperhex[c&0xf])
	}
	return escaped.String()
}

const upperhex = "0123456789ABCDEF"

func mustEscape(c byte) bool {
	return c <= ' ' || c == 0x7f || c == '?' || c == '#'
}

// writeResponse copies a response the router produced itself onto the wire.
func writeResponse(w http.ResponseWriter, resp *fasthttp.Response) {
	header := w.Header()
	written := make(map[string]struct{}, resp.Header.Len())
	for k, v := range resp.Header.All() {
		name := string(k)
		if _, ok := written[name]; ok {
			header.Add(name, string(v))
			continue
		}
		written[name] = struct{}{}
		header.Set(name, string(v))
	}
	w.WriteHeader(resp.StatusCode())
	_, _ = w.Write(resp.Body())
}

// chiPattern renders a zip route path in the {param} form the chi-typed
// observability middleware expects, so a span name and a metric's http.route
// attribute stay the low-cardinality pattern rather than the request path.
func chiPattern(path string) string {
	if !strings.Contains(path, ":") {
		return path
	}
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		if strings.HasPrefix(segment, ":") {
			segments[i] = "{" + strings.TrimSuffix(segment[1:], "?") + "}"
		}
	}
	return strings.Join(segments, "/")
}
