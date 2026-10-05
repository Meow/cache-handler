package httpcache

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

func init() {
	caddy.RegisterModule(new(Handler))
	httpcaddyfile.RegisterHandlerDirective(moduleName, parseCaddyfileHandlerDirective)
	httpcaddyfile.RegisterDirectiveOrder(moduleName, httpcaddyfile.Before, "rewrite")
}

// Handler is an HTTP cache. Responses are stored as files in a directory
// bounded by max_size and the most requested ones are also kept in memory,
// bounded by max_memory. Bodies are streamed to the client and to the cache
// at the same time and are never buffered whole in memory.
type Handler struct {
	Options

	cfg    *config
	store  *Store
	logger *zap.Logger
}

// CaddyModule returns the Caddy module information.
func (*Handler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.cache",
		New: func() caddy.Module { return new(Handler) },
	}
}

// Provision resolves the options against the global ones and opens the
// store, or joins the one already open on the same directory.
func (h *Handler) Provision(ctx caddy.Context) error {
	h.logger = ctx.Logger()

	appModule, err := ctx.App(moduleName)
	if err != nil {
		return err
	}
	app := appModule.(*App)

	cfg, err := h.Options.inherit(app.Options).resolve()
	if err != nil {
		return err
	}
	if err := app.claim(cfg.path, cfg.limits); err != nil {
		return err
	}

	store, _, err := stores.LoadOrNew(cfg.path, func() (caddy.Destructor, error) {
		return OpenStore(cfg.path, cfg.limits, h.logger)
	})
	if err != nil {
		return err
	}

	h.cfg = cfg
	h.store = store.(*Store)
	// A store inherited from the previous configuration gets its new limits
	// when this one starts.
	app.use(h.store, cfg.path, cfg.limits)

	return nil
}

// Cleanup releases the store, which is closed once no configuration uses it.
func (h *Handler) Cleanup() error {
	if h.store == nil {
		return nil
	}
	_, err := stores.Delete(h.cfg.path)

	return err
}

// UnmarshalCaddyfile sets up the handler from the cache directive.
func (h *Handler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	return parseOptions(d, &h.Options)
}

func parseCaddyfileHandlerDirective(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
	handler := new(Handler)

	return handler, handler.UnmarshalCaddyfile(h.Dispenser)
}

// ServeHTTP implements caddyhttp.MiddlewareHandler.
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	c := h.cfg
	x := &exchange{h: h, c: c, w: w, r: r, next: next, start: time.Now()}

	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		return x.serveUncached()
	}
	if r.Header.Get("Upgrade") != "" || (c.exclude != nil && c.exclude.MatchString(r.RequestURI)) {
		return x.bypass("EXCLUDED")
	}

	x.key = c.buildKey(r, http.MethodGet)
	if len(x.key) > maxKeyLen/2 {
		x.key = ""
		return x.bypass("KEY-TOO-LONG")
	}

	if c.strict {
		x.reqCC = parseDirectives(r.Header.Values("Cache-Control"))
		if len(x.reqCC) == 0 && r.Header.Get("Pragma") == "no-cache" {
			x.reqCC["no-cache"] = ""
		}
		if x.reqCC.has("no-store") {
			return x.bypass("REQUEST-NO-STORE")
		}
	}

	return x.serve()
}

// exchange is one request going through the cache.
type exchange struct {
	h     *Handler
	c     *config
	w     http.ResponseWriter
	r     *http.Request
	next  caddyhttp.Handler
	key   string
	start time.Time
	// reqCC holds the request directives, in strict mode only.
	reqCC directives

	// The flight this exchange leads, if any.
	flightID    ID
	flight      *flight
	flightEnded atomic.Bool
}

// status builds the Cache-Status value of this cache.
func (x *exchange) status(params string) string {
	s := x.c.name + "; " + params
	if x.key != "" && !x.c.key.Hide {
		s += "; key=" + formatKey(x.key)
	}

	return s
}

// bypass sends the request to the upstream without involving the cache.
func (x *exchange) bypass(detail string) error {
	x.w.Header().Add("Cache-Status", x.status("fwd=bypass; detail="+detail))

	return x.next.ServeHTTP(x.w, x.r)
}

// pass sends the request to the upstream as the client made it and relays
// the response without storing it.
func (x *exchange) pass(detail string) error {
	x.w.Header().Add("Cache-Status", x.status("fwd=uri-miss; detail="+detail))

	return x.next.ServeHTTP(x.w, x.r)
}

// serveUncached handles the methods that are never cached. A successful
// unsafe request invalidates what is stored for its URI.
func (x *exchange) serveUncached() error {
	r := x.r
	x.w.Header().Add("Cache-Status", x.status("fwd=bypass; detail=UNSUPPORTED-METHOD"))

	switch r.Method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
	default:
		return x.next.ServeHTTP(x.w, r)
	}

	// The handlers below may rewrite the request.
	key := x.c.buildKey(r, http.MethodGet)

	sw := &statusWriter{ResponseWriterWrapper: &caddyhttp.ResponseWriterWrapper{ResponseWriter: x.w}}
	err := x.next.ServeHTTP(sw, r)
	if err == nil && sw.status < http.StatusBadRequest {
		x.h.store.Purge(key)
	}

	return err
}

// statusWriter records the status code of a response.
type statusWriter struct {
	*caddyhttp.ResponseWriterWrapper
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	if w.status == 0 {
		w.status = code
	}
	w.ResponseWriterWrapper.WriteHeader(code)
}

// serve answers a GET or HEAD request from the cache, fetching the response
// first when it is missing or stale.
func (x *exchange) serve() error {
	s := x.h.store

	var (
		joined   *flight
		deadline time.Time
	)
	for {
		id, hit := s.Lookup(x.key, x.r.Header)
		now := time.Now()

		if hit != nil && x.usable(hit.rec, now, joined) {
			if joined != nil {
				return x.serveHit(hit, now, "fwd=uri-miss; collapsed")
			}

			return x.serveHit(hit, now, x.hitParams(hit, now))
		}

		if x.reqCC.has("only-if-cached") {
			hit.Close()
			x.w.Header().Add("Cache-Status", x.status("fwd=bypass; detail=ONLY-IF-CACHED"))

			return caddyhttp.Error(http.StatusGatewayTimeout, errors.New("the response is not in the cache"))
		}

		// The cache is filled by GET requests only.
		if x.r.Method == http.MethodHead {
			hit.Close()
			return x.pass("HEAD")
		}

		// Responses known not to be cacheable are not worth waiting for
		// each other.
		if s.Uncacheable(id) {
			return x.fetch(id, nil, hit)
		}

		fl, leader := s.BeginFlight(id)
		if leader {
			return x.fetch(id, fl, hit)
		}

		// Another request is fetching the response.
		if hit != nil && x.staleUsable(hit.rec, now, hit.rec.swr) {
			return x.serveHit(hit, now, x.hitParams(hit, now)+"; detail=UPDATING")
		}
		hit.Close()

		if deadline.IsZero() {
			deadline = x.start.Add(x.c.lockTimeout)
		}
		timer := time.NewTimer(time.Until(deadline))
		select {
		case <-fl.done:
			timer.Stop()
			joined = fl
		case <-timer.C:
			return x.pass("LOCK-TIMEOUT")
		case <-x.r.Context().Done():
			timer.Stop()
			return x.r.Context().Err()
		}
	}
}

// usable tells whether a stored response can answer the request as is.
// joined is the fetch the request waited for, if it did.
func (x *exchange) usable(rec *record, now time.Time, joined *flight) bool {
	// A response fetched while the request was waiting is as current as
	// what the request would get by asking the upstream itself.
	if joined != nil && joined.stored && rec.stored >= x.start.UnixMilli() {
		return true
	}

	remaining := time.Duration(rec.fresh-now.UnixMilli()) * time.Millisecond
	if x.c.strict {
		cc := x.reqCC
		if cc.has("no-cache") {
			return false
		}
		if d, ok := cc.seconds("max-age"); ok && age(rec, now) > d {
			return false
		}
		if d, ok := cc.seconds("min-fresh"); ok && remaining < d {
			return false
		}
		if remaining <= 0 && cc.has("max-stale") && rec.flags&flagMustRevalidate == 0 {
			if d, _ := cc.seconds("max-stale"); cc["max-stale"] == "" || -remaining <= d {
				return true
			}
		}
	}

	return remaining > 0
}

// staleUsable tells whether a stale response may still be served, window
// being the seconds it is allowed to after its freshness ended.
func (x *exchange) staleUsable(rec *record, now time.Time, window uint32) bool {
	if rec.flags&flagMustRevalidate != 0 || x.reqCC.has("no-cache") {
		return false
	}

	return now.UnixMilli() < rec.fresh+int64(window)*1000
}

// age is the time since the response left the origin.
func age(rec *record, now time.Time) time.Duration {
	resident := max(now.UnixMilli()-rec.stored, 0)

	return time.Duration(resident)*time.Millisecond + time.Duration(rec.age)*time.Second
}

func (x *exchange) hitParams(hit *Hit, now time.Time) string {
	remaining := hit.rec.fresh - now.UnixMilli()
	if remaining <= 0 {
		// A stale response has a negative ttl, however recently it expired.
		return "hit; ttl=" + strconv.FormatInt(min(remaining/1000, -1), 10)
	}

	tier := "DISK"
	if hit.InMemory() {
		tier = "MEMORY"
	}

	return "hit; ttl=" + strconv.FormatInt(remaining/1000, 10) + "; detail=" + tier
}

// serveHit writes a stored response to the client and closes hit.
func (x *exchange) serveHit(hit *Hit, now time.Time, params string) error {
	defer hit.Close()

	rec := hit.rec
	header := x.w.Header()
	applyHeaders(header, rec.header)
	header.Set("Age", strconv.FormatInt(int64(age(rec, now)/time.Second), 10))
	header.Add("Cache-Status", x.status(params))

	// Ranges and preconditions are evaluated against the stored response by
	// the standard library.
	if rec.status == http.StatusOK && (x.r.Header.Get("Range") != "" || conditional(x.r)) {
		var modified time.Time
		if t, err := http.ParseTime(header.Get("Last-Modified")); err == nil {
			modified = t
		}
		http.ServeContent(x.w, x.r, "", modified, hit.Body())

		return nil
	}

	header.Set("Content-Length", strconv.FormatInt(rec.bodyLen, 10))
	x.w.WriteHeader(rec.status)
	if x.r.Method == http.MethodHead || rec.bodyLen == 0 {
		return nil
	}

	if err := hit.WriteBody(x.w); err != nil {
		if errors.Is(err, syscall.EIO) {
			// The file cannot be read back: stop serving it.
			hit.Discard()
			x.h.logger.Error("reading a cached response failed", zap.String("key", x.key), zap.Error(err))
		}
		// The response is cut short, which the client must be able to tell.
		panic(http.ErrAbortHandler)
	}

	return nil
}

// endFlight releases the requests waiting for this exchange, once.
func (x *exchange) endFlight(stored bool) {
	if x.flight != nil && x.flightEnded.CompareAndSwap(false, true) {
		x.h.store.EndFlight(x.flightID, x.flight, stored)
	}
}

// fetch asks the upstream for the response and, when it may be stored, puts
// it in the cache. stale is the expired response the cache holds, if any; it
// is closed here. fl is nil when the request does not hold the flight, which
// is the case for responses recently found uncacheable.
func (x *exchange) fetch(id ID, fl *flight, stale *Hit) error {
	defer stale.Close()

	s, r := x.h.store, x.r
	x.flightID, x.flight = id, fl

	fw := newFetchWriter(x, id, stale)
	// Whatever happens below, the waiters are released and an unfinished
	// file is removed.
	defer fw.close()
	defer x.endFlight(false)

	// The fetch does not stop with the client: a response worth storing is
	// worth finishing, for the requests to come.
	ctx, cancel := context.WithCancel(context.WithoutCancel(r.Context()))
	defer cancel()
	fw.cancel = cancel
	stop := context.AfterFunc(r.Context(), fw.clientGone)

	restore := fw.prepareRequest(r)
	err, aborted := callNext(x.next, fw, r.WithContext(ctx))
	restore()
	stop()
	fw.finish(err == nil && !aborted)

	now := time.Now()
	failed := err != nil || aborted

	switch fw.mode {
	case modeRevalidated:
		hit := x.refresh(stale, fw, now)
		x.endFlight(true)

		return x.serveHit(hit, now, "fwd=stale; fwd-status=304; detail=REVALIDATED")

	case modeStaleError:
		x.endFlight(false)

		return x.serveHit(stale, now, fmt.Sprintf("fwd=stale; fwd-status=%d; detail=STALE", fw.status))

	case modeStream:
		committed := !failed && fw.commit()
		if committed {
			s.SetUncacheable(id, false)
		}
		x.endFlight(committed)

	case modeSilent:
		if !failed && fw.commit() {
			s.SetUncacheable(id, false)
			x.endFlight(true)
			if _, hit := s.Lookup(x.key, r.Header); hit != nil {
				return x.serveHit(hit, now, fw.forward+"; stored")
			}
		}

		return x.fallback(fw, stale, now, failed)

	case modeAbort:
		return x.fallback(fw, stale, now, failed)

	case modeUndecided:
		// The upstream failed before answering.
		x.endFlight(false)
		if stale != nil && x.staleUsable(stale.rec, now, stale.rec.sie) {
			return x.serveHit(stale, now, "fwd=stale; detail=STALE")
		}
	}

	if aborted {
		panic(http.ErrAbortHandler)
	}

	return err
}

// fallback answers a request whose fetch produced nothing to serve from and
// wrote nothing to the client yet.
func (x *exchange) fallback(fw *fetchWriter, stale *Hit, now time.Time, failed bool) error {
	x.endFlight(false)

	if err := x.r.Context().Err(); err != nil {
		return err
	}
	if failed && stale != nil && x.staleUsable(stale.rec, now, stale.rec.sie) {
		return x.serveHit(stale, now, "fwd=stale; detail=STALE")
	}

	return x.pass(fw.reason)
}

// callNext runs the next handlers. A reverse proxy that loses its upstream
// while relaying the body aborts by panicking, which is reported here rather
// than left to unwind, so that the fetch can be cleaned up or retried.
func callNext(next caddyhttp.Handler, w http.ResponseWriter, r *http.Request) (err error, aborted bool) {
	defer func() {
		if v := recover(); v != nil {
			if v != http.ErrAbortHandler {
				panic(v)
			}
			aborted = true
		}
	}()

	return next.ServeHTTP(w, r), false
}

// refresh updates a stored response the upstream confirmed with a 304 and
// returns the response to serve.
func (x *exchange) refresh(stale *Hit, fw *fetchWriter, now time.Time) *Hit {
	old := stale.rec

	header := old.header.Clone()
	// The response dates from its confirmation.
	header.Set("Date", now.UTC().Format(http.TimeFormat))
	for name, values := range headerDiff(fw.base, fw.hdr) {
		switch name {
		// These describe the body, which the 304 does not carry.
		case "Content-Encoding", "Content-Range", "Content-Type":
		default:
			header[name] = values
		}
	}

	// As when it was first stored, the response is judged on what the
	// upstream said, which is what the stored headers add to those every
	// response gets.
	final := fw.base.Clone()
	applyHeaders(final, header)

	v := x.c.evaluate(x.r, old.status, ownHeaders(fw.base, final), now)
	if !v.store {
		// Confirmed for this request, but not to be kept any longer.
		stale.Discard()
		return stale
	}

	rec := &record{
		key:    old.key,
		stored: now.UnixMilli(),
		fresh:  now.Add(v.lifetime).UnixMilli(),
		swr:    seconds(v.swr),
		sie:    seconds(v.sie),
		age:    seconds(v.age),
		status: old.status,
		header: header,
	}
	if v.mustRevalidate {
		rec.flags |= flagMustRevalidate
	}

	if err := x.h.store.Rewrite(stale, rec); err != nil {
		return stale
	}
	if _, hit := x.h.store.Lookup(x.key, x.r.Header); hit != nil {
		return hit
	}

	return stale
}

func seconds(d time.Duration) uint32 {
	return uint32(min(max(d/time.Second, 0), 1<<32-1))
}

// Interface guards
var (
	_ caddy.CleanerUpper          = (*Handler)(nil)
	_ caddy.Provisioner           = (*Handler)(nil)
	_ caddyhttp.MiddlewareHandler = (*Handler)(nil)
	_ caddyfile.Unmarshaler       = (*Handler)(nil)
)
