package httpcache

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
	"go.uber.org/zap"
)

// cacheTest is a cache handler with a store of its own, called directly
// rather than through Caddy. It is for what takes a client that fails, an
// upstream that does something at a precise moment, or a look at the store:
// what a server and a network in between do not let a test decide.
type cacheTest struct {
	t *testing.T
	h *Handler
	s *Store
}

func newCacheTest(t *testing.T, o Options) *cacheTest {
	t.Helper()

	o.Path = t.TempDir()
	c, err := o.resolve()
	if err != nil {
		t.Fatal(err)
	}

	// What Caddy does with the app when it loads a configuration.
	app := &App{logger: zap.NewNop(), ready: make(chan struct{})}
	if err := app.claim(c.path, c.limits); err != nil {
		t.Fatal(err)
	}
	if err := app.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = app.Stop() })

	return &cacheTest{
		t: t,
		h: &Handler{Options: o, cfg: c, app: app, logger: zap.NewNop()},
		s: app.store(context.Background(), c.path),
	}
}

// newFetch returns the writer of a fetch for key that no request runs, for
// what is only seen of a fetch by calling for it.
func (c *cacheTest) newFetch(key string) *fetchWriter {
	x := &exchange{h: c.h, c: c.h.cfg, store: c.s, w: httptest.NewRecorder(), r: newRequest(http.MethodGet, "/a"), key: key}
	fw := newFetchWriter(x, makeID(key), nil)
	fw.cancel = func() {}

	return fw
}

// outcome is how the handling of a request ended.
type outcome struct {
	// err is what the handler returned. aborted tells that it cut the
	// response short, and panicked what else it panicked with.
	err      error
	aborted  bool
	panicked any
}

// reply is what became of a request.
type reply struct {
	*httptest.ResponseRecorder
	outcome
}

func (r *reply) status() string {
	return r.Header().Get("Cache-Status")
}

func (r *reply) body() string {
	return r.Body.String()
}

// newRequest returns a request for a path of example.com, with the given
// "Name: value" headers.
func newRequest(method, path string, headers ...string) *http.Request {
	r := httptest.NewRequest(method, "http://example.com"+path, nil)
	// As a server reads it from a request that is not made to a proxy.
	r.RequestURI = r.URL.RequestURI()
	for _, h := range headers {
		name, value, _ := strings.Cut(h, ": ")
		r.Header.Add(name, value)
	}

	return r
}

// keyOf returns the key of a GET request for a path of example.com.
func keyOf(path string) string {
	return "GET-http-example.com-" + path
}

// serve runs a request through the cache, next standing for the handlers
// behind it.
func (c *cacheTest) serve(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) (out outcome) {
	defer func() {
		switch v := recover(); v {
		case nil:
		case http.ErrAbortHandler:
			out.aborted = true
		default:
			out.panicked = v
		}
	}()

	out.err = c.h.ServeHTTP(w, r, next)

	return out
}

func (c *cacheTest) do(r *http.Request, next caddyhttp.Handler) *reply {
	rep := &reply{ResponseRecorder: httptest.NewRecorder()}
	rep.outcome = c.serve(rep.ResponseRecorder, r, next)

	return rep
}

func (c *cacheTest) get(path string, next caddyhttp.Handler, headers ...string) *reply {
	return c.do(newRequest(http.MethodGet, path, headers...), next)
}

// expect checks the Cache-Status of a reply to a request for path, given
// without the name of the cache and the key.
func (c *cacheTest) expect(rep *reply, path, want string) {
	c.t.Helper()

	if rep.err != nil || rep.aborted || rep.panicked != nil {
		c.t.Errorf("%s: error %v, aborted %v, panic %v", path, rep.err, rep.aborted, rep.panicked)
	}
	if got := rep.status(); got != "Caddy; "+want+"; key="+formatKey(keyOf(path)) {
		c.t.Errorf("Cache-Status: %s\n         want: Caddy; %s; key=%s", got, want, formatKey(keyOf(path)))
	}
}

// expectHit checks that a reply was served from the cache, fresh for ttl
// seconds or one less.
func (c *cacheTest) expectHit(rep *reply, path string, ttl int) {
	c.t.Helper()

	if rep.err != nil || rep.aborted || rep.panicked != nil {
		c.t.Errorf("%s: error %v, aborted %v, panic %v", path, rep.err, rep.aborted, rep.panicked)
	}
	m := hitPattern.FindStringSubmatch(rep.status())
	if m == nil || m[3] != formatKey(keyOf(path)) || (m[1] != fmt.Sprint(ttl) && m[1] != fmt.Sprint(ttl-1)) {
		c.t.Errorf("Cache-Status: %s\n         want: a hit for %s with ttl=%d", rep.status(), keyOf(path), ttl)
	}
}

// backend stands for the handlers behind the cache: an upstream, of which
// it counts the requests.
type backend struct {
	calls  atomic.Int32
	handle func(w http.ResponseWriter, r *http.Request, call int) error
}

func (b *backend) ServeHTTP(w http.ResponseWriter, r *http.Request) error {
	return b.handle(w, r, int(b.calls.Add(1)))
}

func upstreamFunc(handle func(w http.ResponseWriter, r *http.Request, call int) error) *backend {
	return &backend{handle: handle}
}

// respond returns an upstream that answers every request alike.
func respond(status int, body string, headers ...string) *backend {
	return upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
		send(w, status, body, headers...)
		return nil
	})
}

// sendHeader writes the header of a response with the given "Name: value"
// headers. It has a type, without which the standard library reads the
// beginning of a body to find one before it serves any of it.
func sendHeader(w http.ResponseWriter, status int, headers ...string) {
	for _, h := range headers {
		name, value, _ := strings.Cut(h, ": ")
		w.Header().Add(name, value)
	}
	if w.Header().Get("Content-Type") == "" {
		w.Header().Set("Content-Type", "text/plain")
	}
	w.WriteHeader(status)
}

// send writes a response of known length.
func send(w http.ResponseWriter, status int, body string, headers ...string) {
	sendHeader(w, status, append(headers, fmt.Sprintf("Content-Length: %d", len(body)))...)
	_, _ = io.WriteString(w, body)
}

// brokenClient is a client that fails once it was sent a number of bytes.
type brokenClient struct {
	// takes is how many bytes of body the client accepts, and err what
	// writing more to it returns.
	takes int
	err   error

	mu     sync.Mutex
	header http.Header
	status int
	got    []byte
}

func (c *brokenClient) Header() http.Header {
	if c.header == nil {
		c.header = make(http.Header)
	}

	return c.header
}

func (c *brokenClient) WriteHeader(code int) {
	c.status = code
}

func (c *brokenClient) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if room := c.takes - len(c.got); len(p) > room {
		c.got = append(c.got, p[:room]...)
		return room, c.err
	}
	c.got = append(c.got, p...)

	return len(p), nil
}

// watchedContext tells how far the request it belongs to got, by the number
// of times it was asked for its Done channel: a request asks once to find
// out whether the cache is ready, and again each time it starts waiting for
// the fetch of another request.
type watchedContext struct {
	context.Context

	mu    sync.Mutex
	asked int
	marks map[int]chan struct{}
}

func watch(ctx context.Context) *watchedContext {
	return &watchedContext{Context: ctx, marks: make(map[int]chan struct{})}
}

// askedFor returns a channel that is closed when the Done channel is asked
// for the nth time.
func (c *watchedContext) askedFor(n int) <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.marks[n] == nil {
		c.marks[n] = make(chan struct{})
		if c.asked >= n {
			close(c.marks[n])
		}
	}

	return c.marks[n]
}

func (c *watchedContext) Done() <-chan struct{} {
	c.mu.Lock()
	c.asked++
	if mark := c.marks[c.asked]; mark != nil {
		close(mark)
	}
	c.mu.Unlock()

	return c.Context.Done()
}

// waiting is the number of times a request asked its context when it waits
// for the fetch of another one, and waitingAgain when it does again after
// it found that the response being fetched is not for it.
const (
	waiting      = 2
	waitingAgain = 3
)

// waitGone blocks until the fetch w belongs to knows that its client left.
func waitGone(t *testing.T, w http.ResponseWriter) {
	fw := w.(*fetchWriter)
	waitFor(t, "the fetch to be told that the client left", func() bool {
		fw.mu.Lock()
		defer fw.mu.Unlock()

		return fw.gone
	})
}

const lastModified = "Mon, 02 Jan 2006 15:04:05 GMT"

func TestCacheUnavailable(t *testing.T) {
	c, err := Options{Path: t.TempDir()}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	app := &App{logger: zap.NewNop(), ready: make(chan struct{})}
	h := &Handler{cfg: c, app: app, logger: zap.NewNop()}
	up := respond(http.StatusOK, "from the upstream")

	request := func(ctx context.Context) {
		t.Helper()

		rec := httptest.NewRecorder()
		if err := h.ServeHTTP(rec, newRequest(http.MethodGet, "/a").WithContext(ctx), up); err != nil {
			t.Error(err)
		}
		if got := rec.Header().Get("Cache-Status"); got != "Caddy; fwd=bypass; detail=UNAVAILABLE" || rec.Body.String() != "from the upstream" {
			t.Errorf("Cache-Status: %s, body %q", got, rec.Body)
		}
	}

	// The request is over before the configuration is started.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	request(ctx)

	// The configuration is not started after all: the requests that were
	// waiting for it are let through.
	if err := app.Cleanup(); err != nil {
		t.Fatal(err)
	}
	request(context.Background())

	if n := up.calls.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

func TestAppStart(t *testing.T) {
	limits := Limits{MaxSize: 1 << 20}
	newApp := func(paths ...string) *App {
		app := &App{logger: zap.NewNop(), ready: make(chan struct{})}
		for _, path := range paths {
			if err := app.claim(path, limits); err != nil {
				t.Fatal(err)
			}
		}

		return app
	}

	// One of the directories cannot be made, for a file being in the way:
	// the other is not left open.
	good := t.TempDir()
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	app := newApp(good, filepath.Join(file, "cache"))
	if err := app.Start(); err == nil {
		t.Fatal("started with a directory that cannot be made")
	}
	if n := len(openStores(good)); n != 0 {
		t.Errorf("%d stores left open", n)
	}
	if s := app.store(context.Background(), good); s != nil {
		t.Error("a configuration that failed to start has a store")
	}
	if err := app.Stop(); err != nil {
		t.Error(err)
	}

	// A store outlives the configuration it was opened by when the next one
	// uses it, and takes the limits of that one.
	first := newApp(good)
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	limits.MaxSize = 2 << 20
	second := newApp(good)
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	if err := first.Stop(); err != nil {
		t.Error(err)
	}
	s := second.store(context.Background(), good)
	if s == nil || s != first.opened[good] || s.Limits() != limits {
		t.Fatalf("the store was not kept with the new limits: %+v", s)
	}
	if err := storePut(t, s, "key", nil, nil, []byte("body")); err != nil {
		t.Errorf("storing after the first configuration stopped: %v", err)
	}

	if err := second.Stop(); err != nil {
		t.Error(err)
	}
	if n := len(openStores(good)); n != 0 {
		t.Errorf("%d stores left open", n)
	}
	if err := storePut(t, s, "key", nil, nil, []byte("body")); !errors.Is(err, errStoreClosed) {
		t.Errorf("storing after the last configuration stopped: %v", err)
	}
}

// TestProvision covers the handler being set up by Caddy, without a server
// being started.
func TestProvision(t *testing.T) {
	provision := func(global string) (caddy.Context, error) {
		return caddy.ProvisionContext(&caddy.Config{AppsRaw: caddy.ModuleMap{moduleName: json.RawMessage(global)}})
	}

	// The options set globally are wrong: no handler gets to inherit them.
	ctx, err := provision(`{"max_size": -1}`)
	if err == nil {
		t.Fatal("a negative max_size was accepted")
	}
	if err := new(Handler).Provision(ctx); err == nil || !strings.Contains(err.Error(), "max_size must be positive") {
		t.Errorf("a handler was provisioned without its app: %v", err)
	}

	dir := t.TempDir()
	if ctx, err = provision(fmt.Sprintf(`{"path": %q, "ttl": "1h"}`, dir)); err != nil {
		t.Fatal(err)
	}
	h := &Handler{Options: Options{Stale: caddy.Duration(time.Minute)}}
	if err := h.Provision(ctx); err != nil {
		t.Fatal(err)
	}
	if h.cfg.path != dir || h.cfg.ttl != time.Hour || h.cfg.stale != time.Minute {
		t.Errorf("unexpected configuration: %+v", h.cfg)
	}
	// Nothing is opened before the configuration is started.
	if n := len(openStores(dir)); n != 0 {
		t.Errorf("%d stores opened by a configuration that was not started", n)
	}

	if err := (&Handler{Options: Options{Mode: "relaxed"}}).Provision(ctx); err == nil || !strings.Contains(err.Error(), "unknown cache mode") {
		t.Errorf("a handler with an unknown mode was provisioned: %v", err)
	}
	if err := (&Handler{Options: Options{MaxSize: 1 << 30}}).Provision(ctx); err == nil || !strings.Contains(err.Error(), "configured with different") {
		t.Errorf("a handler with other limits for the same directory was provisioned: %v", err)
	}
}

// TestAdminAPIRequests covers the admin endpoint, without one.
func TestAdminAPIRequests(t *testing.T) {
	api := new(adminAPI)
	if routes := api.Routes(); len(routes) != 2 || routes[0].Pattern != "/cache/stats" || routes[1].Pattern != "/cache/purge" {
		t.Errorf("unexpected routes: %+v", routes)
	}

	status := func(err error) int {
		var apiErr caddy.APIError
		if !errors.As(err, &apiErr) {
			t.Errorf("unexpected result: %v", err)
		}

		return apiErr.HTTPStatus
	}
	post := func(target string) error {
		return api.handlePurge(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, target, nil))
	}

	if got := status(api.handleStats(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/cache/stats", nil))); got != http.StatusMethodNotAllowed {
		t.Errorf("posting to the stats: status %d", got)
	}
	if got := status(api.handlePurge(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/cache/purge?all=true", nil))); got != http.StatusMethodNotAllowed {
		t.Errorf("purging with a GET request: status %d", got)
	}
	if got := status(post("/cache/purge?regex=(")); got != http.StatusBadRequest {
		t.Errorf("purging with an invalid expression: status %d", got)
	}
	if got := status(post("/cache/purge")); got != http.StatusBadRequest {
		t.Errorf("purging nothing in particular: status %d", got)
	}

	c := newCacheTest(t, Options{})
	paths := []string{"/a/1", "/a/2", "/b/1", "/b/2", "/c"}
	for _, path := range paths {
		c.expect(c.get(path, respond(http.StatusOK, "body")), path, "fwd=uri-miss; stored")
	}

	rec := httptest.NewRecorder()
	var stats struct {
		Caches []StoreStats `json:"caches"`
	}
	if err := api.handleStats(rec, httptest.NewRequest(http.MethodGet, "/cache/stats", nil)); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &stats); err != nil || len(stats.Caches) != 1 || stats.Caches[0].Path != c.s.dir || stats.Caches[0].Entries != len(paths) {
		t.Errorf("unexpected stats: %s, %v", rec.Body, err)
	}

	// The purges, in the order they are made.
	for _, purge := range []struct {
		query string
		want  int
	}{
		{"key=nothing", 0},
		{"key=" + keyOf("/c"), 1},
		{"prefix=" + keyOf("/a/"), 2},
		{`regex=/b/\d$`, 2},
		{"all=true", 0},
	} {
		rec := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodDelete, "/cache/purge", nil)
		r.URL.RawQuery = "path=" + c.s.dir + "&" + purge.query
		if err := api.handlePurge(rec, r); err != nil || strings.TrimSpace(rec.Body.String()) != fmt.Sprintf(`{"purged":%d}`, purge.want) {
			t.Errorf("purging %s: %s, %v; want %d", purge.query, rec.Body, err, purge.want)
		}
	}
	if st := c.s.Stats(); st.Entries != 0 {
		t.Errorf("%d entries left", st.Entries)
	}
}

// TestCaddyfileAdapters covers what Caddy is handed for the directive and
// for the global option.
func TestCaddyfileAdapters(t *testing.T) {
	const block = "cache {\n ttl 1h\n}"

	handler, err := parseCaddyfileHandlerDirective(httpcaddyfile.Helper{Dispenser: caddyfile.NewTestDispenser(block)})
	if err != nil {
		t.Fatal(err)
	}
	if h, ok := handler.(*Handler); !ok || h.TTL != caddy.Duration(time.Hour) {
		t.Errorf("unexpected handler: %+v", handler)
	}

	option, err := parseCaddyfileGlobalOption(caddyfile.NewTestDispenser(block), nil)
	if err != nil {
		t.Fatal(err)
	}
	app, ok := option.(httpcaddyfile.App)
	var options Options
	if !ok || app.Name != moduleName || json.Unmarshal(app.Value, &options) != nil || options.TTL != caddy.Duration(time.Hour) {
		t.Errorf("unexpected app: %+v", option)
	}
}

func TestRequestsThatBypass(t *testing.T) {
	c := newCacheTest(t, Options{})
	up := respond(http.StatusOK, "from the upstream")

	// A method that neither reads nor changes what is stored.
	rep := c.do(newRequest(http.MethodOptions, "/a"), up)
	if got := rep.status(); got != "Caddy; fwd=bypass; detail=UNSUPPORTED-METHOD" || rep.body() != "from the upstream" {
		t.Errorf("Cache-Status: %s, body %q", got, rep.body())
	}

	// A key too long to be stored.
	rep = c.get("/"+strings.Repeat("a", maxKeyLen), up)
	if got := rep.status(); got != "Caddy; fwd=bypass; detail=INVALID-KEY" || rep.body() != "from the upstream" {
		t.Errorf("Cache-Status: %s, body %q", got, rep.body())
	}

	if st := c.s.Stats(); st.Entries != 0 || st.Hits+st.Misses != 0 {
		t.Errorf("the cache was involved: %+v", st)
	}

	// What the configuration keeps out of the cache, and what is no request
	// for a response.
	c = newCacheTest(t, Options{Regex: &RegexOptions{Exclude: "^/private/"}})
	for _, r := range []*http.Request{newRequest(http.MethodGet, "/private/a"), newRequest(http.MethodGet, "/a", "Upgrade: websocket")} {
		if rep := c.do(r, up); rep.status() != "Caddy; fwd=bypass; detail=EXCLUDED" || rep.body() != "from the upstream" {
			t.Errorf("%s: Cache-Status: %s, body %q", r.URL.Path, rep.status(), rep.body())
		}
	}

	// What the client does not want stored, where it has a say.
	c = newCacheTest(t, Options{Mode: "strict"})
	for range 2 {
		if rep := c.get("/a", up, "Cache-Control: no-store"); rep.status() != "Caddy; fwd=bypass; detail=REQUEST-NO-STORE; key="+keyOf("/a") {
			t.Errorf("Cache-Status: %s", rep.status())
		}
	}
	if st := c.s.Stats(); st.Entries != 0 {
		t.Errorf("%d entries", st.Entries)
	}
}

func TestUnsafeMethods(t *testing.T) {
	c := newCacheTest(t, Options{})
	up := respond(http.StatusOK, "body")
	unsafe := func(method string, up caddyhttp.Handler) *reply {
		rep := c.do(newRequest(method, "/a"), up)
		if got := rep.status(); got != "Caddy; fwd=bypass; detail=UNSUPPORTED-METHOD" {
			t.Errorf("%s: Cache-Status: %s", method, got)
		}

		return rep
	}

	// A request that changed something invalidates what is stored.
	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete} {
		c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")
		c.expectHit(c.get("/a", up), "/a", 120)
		if rep := unsafe(method, respond(http.StatusNoContent, "")); rep.err != nil || rep.Code != http.StatusNoContent {
			t.Errorf("%s: status %d, %v", method, rep.Code, rep.err)
		}
	}
	c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")

	// One that failed does not.
	unsafe(http.MethodPost, respond(http.StatusForbidden, "no"))
	if rep := unsafe(http.MethodPost, upstreamFunc(func(http.ResponseWriter, *http.Request, int) error { return errInjected })); !errors.Is(rep.err, errInjected) {
		t.Errorf("returned %v", rep.err)
	}
	c.expectHit(c.get("/a", up), "/a", 120)

	// So does one of a method the cache does not know to be safe, while
	// the safe ones leave the cache alone (RFC 9111, section 4.4).
	unsafe("LOCK", respond(http.StatusOK, "locked"))
	c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")
	unsafe(http.MethodOptions, respond(http.StatusNoContent, ""))
	c.expectHit(c.get("/a", up), "/a", 120)

	// What the response points to on the same host goes too, be it
	// relative or absolute; what is on another host stays.
	store := func(paths ...string) {
		for _, path := range paths {
			c.expect(c.get(path, up), path, "fwd=uri-miss; stored")
			c.expectHit(c.get(path, up), path, 120)
		}
	}
	store("/b", "/dir/c", "/d", "/e")
	unsafe(http.MethodPost, respond(http.StatusCreated, "", "Location: /b", "Content-Location: http://EXAMPLE.com/dir/c"))
	c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")
	c.expect(c.get("/b", up), "/b", "fwd=uri-miss; stored")
	c.expect(c.get("/dir/c", up), "/dir/c", "fwd=uri-miss; stored")
	unsafe(http.MethodPost, respond(http.StatusCreated, "", "Location: http://other.example.com/d", "Content-Location: e"))
	c.expectHit(c.get("/d", up), "/d", 120)
	c.expect(c.get("/e", up), "/e", "fwd=uri-miss; stored")
	// A Location that cannot be read is not an error.
	unsafe(http.MethodPost, respond(http.StatusCreated, "", "Location: ::not a url"))
}

// TestAgeFromDate checks that a response's age counts from its Date, not
// from its arrival (RFC 9111, section 4.2.3).
func TestAgeFromDate(t *testing.T) {
	c := newCacheTest(t, Options{})
	up := respond(http.StatusOK, "body", "Cache-Control: max-age=60", "Date: "+time.Now().Add(-30*time.Second).UTC().Format(http.TimeFormat))

	c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")
	hit := c.get("/a", up)
	c.expectHit(hit, "/a", 30)
	if age, _ := strconv.Atoi(hit.Header().Get("Age")); age < 30 || age > 31 {
		t.Errorf("Age: %q, want 30", hit.Header().Get("Age"))
	}
}

// TestRequestsNothingIsFetchedFor covers the requests that are answered
// from the cache or not by it at all.
func TestRequestsNothingIsFetchedFor(t *testing.T) {
	c := newCacheTest(t, Options{Mode: "strict"})
	up := respond(http.StatusOK, "0123456789")
	head := func(path string, up caddyhttp.Handler) *reply {
		return c.do(newRequest(http.MethodHead, path), up)
	}

	// Nothing is stored.
	rep := c.get("/a", up, "Cache-Control: only-if-cached")
	if statusOf(rep.err) != http.StatusGatewayTimeout || rep.status() != "Caddy; fwd=bypass; detail=ONLY-IF-CACHED; key="+keyOf("/a") {
		t.Errorf("returned %v, Cache-Status: %s", rep.err, rep.status())
	}
	c.expect(head("/a", up), "/a", "fwd=uri-miss; detail=HEAD")
	if st := c.s.Stats(); st.Entries != 0 || up.calls.Load() != 1 {
		t.Errorf("%d entries after %d requests to the upstream", st.Entries, up.calls.Load())
	}

	// The response is on its way in.
	halfway, release := make(chan struct{}), make(chan struct{})
	first := make(chan *reply)
	go func() {
		first <- c.get("/a", upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
			sendHeader(w, http.StatusOK, "Content-Length: 10")
			_, _ = io.WriteString(w, "01234")
			close(halfway)
			<-release
			_, _ = io.WriteString(w, "56789")

			return nil
		}))
	}()
	<-halfway
	rep = head("/a", up)
	c.expect(rep, "/a", "fwd=uri-miss; collapsed")
	if rep.Header().Get("Content-Length") != "10" || rep.Body.Len() != 0 {
		t.Errorf("body %q, headers %v", rep.body(), rep.Header())
	}
	close(release)
	c.expect(<-first, "/a", "fwd=uri-miss; stored")

	// The response is stored.
	rep = head("/a", up)
	c.expectHit(rep, "/a", 120)
	if rep.Header().Get("Content-Length") != "10" || rep.Body.Len() != 0 {
		t.Errorf("body %q, headers %v", rep.body(), rep.Header())
	}
	rep = c.get("/a", up, "Cache-Control: only-if-cached")
	c.expectHit(rep, "/a", 120)
	if rep.body() != "0123456789" || up.calls.Load() != 1 {
		t.Errorf("body %q after %d requests to the upstream", rep.body(), up.calls.Load())
	}

	// The response is known not to be stored: a request for part of it is
	// the upstream's to answer.
	private := respond(http.StatusOK, "0123456789", "Cache-Control: private")
	c.expect(c.get("/private", private), "/private", "fwd=uri-miss; detail=PRIVATE")
	c.expect(c.get("/private", private, "Range: bytes=0-1"), "/private", "fwd=uri-miss; detail=UNCACHEABLE")
	c.expect(c.get("/private", private), "/private", "fwd=uri-miss; detail=PRIVATE")
}

// TestHitFromMemory covers the response that has a copy in memory, made
// here without waiting for it to be requested enough.
func TestHitFromMemory(t *testing.T) {
	c := newCacheTest(t, Options{})
	up := respond(http.StatusOK, "0123456789")
	c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")

	c.s.mu.Lock()
	e := c.s.index[makeID(keyOf("/a"))]
	c.s.mu.Unlock()
	c.s.promote(e)

	rep := c.get("/a", up)
	c.expectHit(rep, "/a", 120)
	if rep.body() != "0123456789" || !strings.Contains(rep.status(), "detail=MEMORY") {
		t.Errorf("body %q, Cache-Status: %s", rep.body(), rep.status())
	}
	if rep = c.get("/a", up, "Range: bytes=-3"); rep.Code != http.StatusPartialContent || rep.body() != "789" {
		t.Errorf("status %d, body %q", rep.Code, rep.body())
	}
	if out := c.serve(&brokenClient{takes: 5, err: errInjected}, newRequest(http.MethodGet, "/a"), up); !out.aborted {
		t.Error("a response the client did not take was not cut short")
	}
}

// TestResponseHeaders covers what the cache makes of the headers that are
// not all the upstream's, or not all there when the response starts.
func TestResponseHeaders(t *testing.T) {
	c := newCacheTest(t, Options{DefaultCacheControl: "max-age=60"})

	// front runs a request for which the handlers in front of the cache
	// set headers.
	front := func(path string, up caddyhttp.Handler) *reply {
		rep := &reply{ResponseRecorder: httptest.NewRecorder()}
		rep.Header().Set("Server", "front")
		rep.Header().Set("Alt-Svc", "h3")
		rep.outcome = c.serve(rep.ResponseRecorder, newRequest(http.MethodGet, path), up)

		return rep
	}

	// The upstream removes one of them, and says nothing of how long its
	// response is fresh.
	up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
		w.Header().Del("Server")
		send(w, http.StatusOK, "body")

		return nil
	})
	rep := front("/a", up)
	c.expect(rep, "/a", "fwd=uri-miss; stored")
	again := front("/a", up)
	c.expectHit(again, "/a", 60)
	for _, rep := range []*reply{rep, again} {
		if h := rep.Header(); len(h.Values("Server")) != 0 || h.Get("Alt-Svc") != "h3" || h.Get("Cache-Control") != "max-age=60" || rep.body() != "body" {
			t.Errorf("body %q, headers %v", rep.body(), h)
		}
	}

	// Trailers are passed on, with the response they keep out of the cache.
	up = upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
		sendHeader(w, http.StatusOK, "Trailer: X-Sum, X-Unset")
		_, _ = io.WriteString(w, "body")
		w.Header().Set("X-Sum", "4")
		w.Header().Set(http.TrailerPrefix+"X-Late", "yes")
		w.Header().Set("X-Not-Announced", "yes")

		return nil
	})
	rep = c.get("/trailers", up)
	c.expect(rep, "/trailers", "fwd=uri-miss; detail=TRAILER")
	if h := rep.Header(); h.Get("X-Sum") != "4" || h.Get(http.TrailerPrefix+"X-Late") != "yes" || h.Get("X-Not-Announced") != "" {
		t.Errorf("headers %v", h)
	}
}

// TestRequestIsPutBack checks that a request the cache sends to the
// upstream a second time, as the client made it, is no longer what the
// first attempt made of it.
func TestRequestIsPutBack(t *testing.T) {
	c := newCacheTest(t, Options{})

	// What Caddy's router notes of the routes it took.
	groups := map[string]struct{}{"taken": {}}
	r := newRequest(http.MethodGet, "/a?x=1", "Range: bytes=0-1", "X-Kept: yes")
	r = r.WithContext(context.WithValue(r.Context(), routeGroupCtxKey, groups))

	up := upstreamFunc(func(w http.ResponseWriter, r *http.Request, call int) error {
		if call == 1 {
			if r.Header.Get("Range") != "" {
				t.Errorf("the upstream was asked for %s", r.Header.Get("Range"))
			}
			r.Header.Set("X-Added", "yes")
			r.Header.Del("X-Kept")
			r.URL.Path = "/rewritten"
			groups["rewrite"] = struct{}{}
			send(w, http.StatusOK, "private", "Cache-Control: private")

			return nil
		}

		if h := r.Header; h.Get("Range") != "bytes=0-1" || h.Get("X-Kept") != "yes" || h.Get("X-Added") != "" || r.URL.String() != "http://example.com/a?x=1" {
			t.Errorf("the request was not put back: %s, %v", r.URL, h)
		}
		if _, rewritten := groups["rewrite"]; rewritten || len(groups) != 1 {
			t.Errorf("the routes taken were not put back: %v", groups)
		}
		send(w, http.StatusOK, "asked again")

		return nil
	})

	rep := c.do(r, up)
	c.expect(rep, "/a?x=1", "fwd=uri-miss; detail=PRIVATE")
	if rep.body() != "asked again" || up.calls.Load() != 2 {
		t.Errorf("body %q after %d requests to the upstream", rep.body(), up.calls.Load())
	}
}

func TestPragma(t *testing.T) {
	c := newCacheTest(t, Options{Mode: "strict"})
	up := respond(http.StatusOK, "body")

	c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")
	c.expectHit(c.get("/a", up), "/a", 120)
	// It is what a client that knows nothing of Cache-Control says.
	c.expect(c.get("/a", up, "Pragma: no-cache"), "/a", "fwd=stale; stored")
	c.expectHit(c.get("/a", up, "Pragma: no-cache", "Cache-Control: max-age=60"), "/a", 120)
	if n := up.calls.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

// TestWaitingForAFetch covers the requests that find another one fetching
// the response they want, of which they cannot be sent anything before it
// is whole: it does not say how long it is.
func TestWaitingForAFetch(t *testing.T) {
	// slow returns an upstream whose first answer waits for release, once
	// it told that it was asked.
	slow := func(release <-chan struct{}) (*backend, <-chan struct{}) {
		entered := make(chan struct{})

		return upstreamFunc(func(w http.ResponseWriter, _ *http.Request, call int) error {
			if call > 1 {
				send(w, http.StatusOK, "asked again")
				return nil
			}

			sendHeader(w, http.StatusOK)
			close(entered)
			<-release
			_, _ = io.WriteString(w, "body")

			return nil
		}), entered
	}

	t.Run("served", func(t *testing.T) {
		c := newCacheTest(t, Options{})
		ctx := watch(context.Background())
		up, entered := slow(ctx.askedFor(waiting))

		first := make(chan *reply)
		go func() { first <- c.get("/a", up) }()
		<-entered

		rep := c.do(newRequest(http.MethodGet, "/a").WithContext(ctx), up)
		c.expect(rep, "/a", "fwd=uri-miss; collapsed")
		c.expect(<-first, "/a", "fwd=uri-miss; stored")
		if rep.body() != "body" || up.calls.Load() != 1 {
			t.Errorf("body %q after %d requests to the upstream", rep.body(), up.calls.Load())
		}
	})

	t.Run("for too long", func(t *testing.T) {
		c := newCacheTest(t, Options{LockTimeout: caddy.Duration(20 * time.Millisecond)})
		release := make(chan struct{})
		up, entered := slow(release)

		first := make(chan *reply)
		go func() { first <- c.get("/a", up) }()
		<-entered

		rep := c.get("/a", up)
		c.expect(rep, "/a", "fwd=uri-miss; detail=LOCK-TIMEOUT")
		if rep.body() != "asked again" {
			t.Errorf("body %q", rep.body())
		}
		close(release)
		c.expect(<-first, "/a", "fwd=uri-miss; stored")
	})

	t.Run("by a client that leaves", func(t *testing.T) {
		c := newCacheTest(t, Options{})
		release := make(chan struct{})
		up, entered := slow(release)

		first := make(chan *reply)
		go func() { first <- c.get("/a", up) }()
		<-entered

		ctx, cancel := context.WithCancel(context.Background())
		watched := watch(ctx)
		second := make(chan *reply)
		go func() { second <- c.do(newRequest(http.MethodGet, "/a").WithContext(watched), up) }()
		<-watched.askedFor(waiting)
		cancel()
		if rep := <-second; !errors.Is(rep.err, context.Canceled) || rep.Body.Len() != 0 {
			t.Errorf("a request whose client left returned %v after %d bytes", rep.err, rep.Body.Len())
		}

		close(release)
		c.expect(<-first, "/a", "fwd=uri-miss; stored")
		if n := up.calls.Load(); n != 1 {
			t.Errorf("the upstream got %d requests, want 1", n)
		}
	})
}

// TestJoiningADownload covers the requests served from a response another
// request is still receiving, when that response varies.
func TestJoiningADownload(t *testing.T) {
	c := newCacheTest(t, Options{})

	// The variant being received is the one the request selects: the
	// range it asks for is sent as soon as it is there.
	halfway, release := make(chan struct{}), make(chan struct{})
	up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
		sendHeader(w, http.StatusOK, "Vary: Accept-Language", "Content-Length: 10", "Last-Modified: "+lastModified)
		_, _ = io.WriteString(w, "01234")
		close(halfway)
		<-release
		_, _ = io.WriteString(w, "56789")

		return nil
	})
	first := make(chan *reply)
	go func() { first <- c.get("/a", up, "Accept-Language: fr") }()
	<-halfway

	rep := c.get("/a", up, "Accept-Language: fr", "Range: bytes=1-3")
	c.expect(rep, "/a", "fwd=uri-miss; collapsed")
	if rep.Code != http.StatusPartialContent || rep.body() != "123" || rep.Header().Get("Content-Range") != "bytes 1-3/10" {
		t.Errorf("status %d, body %q, headers %v", rep.Code, rep.body(), rep.Header())
	}
	rep = c.get("/a", up, "Accept-Language: fr", "If-Modified-Since: "+lastModified)
	if rep.Code != http.StatusNotModified {
		t.Errorf("status %d for a request that has the response already", rep.Code)
	}

	close(release)
	if rep := <-first; rep.body() != "0123456789" || up.calls.Load() != 1 {
		t.Errorf("body %q after %d requests to the upstream", rep.body(), up.calls.Load())
	}

	// The variant being received is another one, which could not be told
	// when the request started waiting: it goes on waiting, then fetches
	// its own.
	ctx := watch(context.Background())
	entered := make(chan struct{})
	up = upstreamFunc(func(w http.ResponseWriter, r *http.Request, call int) error {
		if call > 1 {
			send(w, http.StatusOK, "hallo", "Vary: Accept-Language")
			return nil
		}

		close(entered)
		<-ctx.askedFor(waiting)
		sendHeader(w, http.StatusOK, "Vary: Accept-Language", "Content-Length: 7")
		<-ctx.askedFor(waitingAgain)
		_, _ = io.WriteString(w, "bonjour")

		return nil
	})
	go func() { first <- c.get("/b", up, "Accept-Language: fr") }()
	<-entered

	rep = c.do(newRequest(http.MethodGet, "/b", "Accept-Language: de").WithContext(ctx), up)
	c.expect(rep, "/b", "fwd=uri-miss; stored")
	if rep.body() != "hallo" || (<-first).body() != "bonjour" || up.calls.Load() != 2 {
		t.Errorf("body %q after %d requests to the upstream", rep.body(), up.calls.Load())
	}
}

// TestJoinedDownloadThatFails checks that a request served from a response
// another request is receiving is not told that it got all of it when the
// download fails.
func TestJoinedDownloadThatFails(t *testing.T) {
	c := newCacheTest(t, Options{})
	client := watchClient(5)
	halfway := make(chan struct{})
	up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
		sendHeader(w, http.StatusOK, "Content-Length: 10")
		_, _ = io.WriteString(w, "01234")
		close(halfway)
		<-client.received

		return errInjected
	})

	first := make(chan *reply)
	go func() { first <- c.get("/a", up) }()
	<-halfway

	if out := c.serve(client, newRequest(http.MethodGet, "/a"), up); out.err != nil || !out.aborted || client.Body.String() != "01234" {
		t.Errorf("a response that was not whole was delivered: %v, %q", out.err, client.Body)
	}
	// The request the response was fetched for may not even have been sent
	// what there was of it.
	if rep := <-first; !rep.aborted {
		t.Errorf("a response that was not whole was delivered: %v, %q", rep.err, rep.body())
	}
	if st := c.s.Stats(); st.Entries != 0 || tempFiles(c.s) != 0 || up.calls.Load() != 1 {
		t.Errorf("%d entries, %d temporary files, %d requests to the upstream", st.Entries, tempFiles(c.s), up.calls.Load())
	}
}

// TestStaleResponseWhileUpdating checks that a request is served a stale
// response while another one updates it, where that is allowed.
func TestStaleResponseWhileUpdating(t *testing.T) {
	c := newCacheTest(t, Options{Stale: caddy.Duration(time.Hour)})
	asked, release := make(chan struct{}), make(chan struct{})
	up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, call int) error {
		if call == 1 {
			send(w, http.StatusOK, "body", "Cache-Control: max-age=0", `Etag: "v1"`)
			return nil
		}

		close(asked)
		<-release
		sendHeader(w, http.StatusNotModified)

		return nil
	})
	c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")

	first := make(chan *reply)
	go func() { first <- c.get("/a", up) }()
	<-asked

	rep := c.get("/a", up)
	c.expect(rep, "/a", "hit; ttl=-1; detail=UPDATING")
	if rep.body() != "body" {
		t.Errorf("body %q", rep.body())
	}

	close(release)
	c.expect(<-first, "/a", "fwd=stale; fwd-status=304; detail=REVALIDATED")
	if n := up.calls.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

// TestTailThatCannotBeServed covers the responses in progress a request is
// not served from, which only happens here by calling for it: the requests
// are never offered such a response.
func TestTailThatCannotBeServed(t *testing.T) {
	c := newCacheTest(t, Options{})

	exchange := func(key string, headers ...string) *exchange {
		return &exchange{h: c.h, c: c.h.cfg, store: c.s, w: httptest.NewRecorder(), r: newRequest(http.MethodGet, "/a", headers...), key: key}
	}

	// A range of a body whose length is not known.
	w, err := c.s.Create("unsized", nil, nil, testRecord(), 0, -1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Abort()
	if exchange("unsized", "Range: bytes=0-1").serveTail(w) {
		t.Error("served a range of a body of unknown length")
	}

	// A response that is no longer in progress.
	if w, err = c.s.Create("stored", nil, nil, testRecord(), 0, 4, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Write([]byte("body")); err != nil {
		t.Fatal(err)
	}
	if err := w.Commit(); err != nil {
		t.Fatal(err)
	}
	x := exchange("stored")
	if x.serveTail(w) || len(x.w.Header()) != 0 {
		t.Errorf("served a response that is not in progress anymore: %v", x.w.Header())
	}
}

func TestRangeOfAStoredResponse(t *testing.T) {
	c := newCacheTest(t, Options{})
	up := respond(http.StatusOK, "0123456789", "Last-Modified: "+lastModified)
	c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")

	rep := c.get("/a", up, "Range: bytes=2-4")
	if rep.Code != http.StatusPartialContent || rep.body() != "234" {
		t.Errorf("status %d, body %q", rep.Code, rep.body())
	}
	if rep = c.get("/a", up, "If-Modified-Since: "+lastModified); rep.Code != http.StatusNotModified || rep.Body.Len() != 0 {
		t.Errorf("status %d, body %q", rep.Code, rep.body())
	}
	// A date in the future says nothing.
	later := time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)
	if rep = c.get("/a", up, "If-Unmodified-Since: "+later, "If-Range: "+later, "Range: bytes=2-4"); rep.Code != http.StatusOK || rep.body() != "0123456789" {
		t.Errorf("status %d, body %q", rep.Code, rep.body())
	}
}

// TestStoredResponseThatCannotBeSent covers the client that stops reading a
// stored response, and the file that cannot be read back.
func TestStoredResponseThatCannotBeSent(t *testing.T) {
	c := newCacheTest(t, Options{})
	up := respond(http.StatusOK, string(bodyFor("/a", 10_000)))
	c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")

	client := &brokenClient{takes: 100, err: errInjected}
	if out := c.serve(client, newRequest(http.MethodGet, "/a"), up); out.err != nil || !out.aborted {
		t.Errorf("a response the client did not take was not cut short: %v", out.err)
	}
	if n := c.s.Stats().Entries; n != 1 {
		t.Errorf("%d entries after a client left", n)
	}

	// An input/output error is taken for one of the file, which is then not
	// served anymore.
	client = &brokenClient{takes: 100, err: fmt.Errorf("read: %w", syscall.EIO)}
	if out := c.serve(client, newRequest(http.MethodGet, "/a"), up); out.err != nil || !out.aborted {
		t.Errorf("a response that could not be read was not cut short: %v", out.err)
	}
	if n := c.s.Stats().Entries; n != 0 {
		t.Errorf("%d entries after a file could not be read", n)
	}
}

// TestFetchThatFails covers the upstream that fails once the cache decided
// to store its response without sending it to the client meanwhile, as it
// does for a request with a precondition.
func TestFetchThatFails(t *testing.T) {
	failing := func(first *backend) *backend {
		return upstreamFunc(func(w http.ResponseWriter, r *http.Request, call int) error {
			switch call {
			case 1:
				return first.ServeHTTP(w, r)
			case 2:
				sendHeader(w, http.StatusOK, "Content-Length: 10")
				_, _ = io.WriteString(w, "01234")

				return errInjected
			}
			send(w, http.StatusOK, "asked again")

			return nil
		})
	}

	// Without anything to serve in its place, the request is sent to the
	// upstream as the client made it.
	c := newCacheTest(t, Options{Stale: caddy.Duration(time.Hour)})
	up := failing(respond(http.StatusNotFound, "nothing yet"))
	c.expect(c.get("/a", up), "/a", "fwd=uri-miss; detail=UNCACHEABLE-STATUS")
	// The response is known not to be cacheable for now, which only a
	// request for all of it could find out to have changed.
	c.s.SetUncacheable(makeID(keyOf("/a")), false)
	rep := c.get("/a", up, `If-None-Match: "other"`)
	c.expect(rep, "/a", "fwd=uri-miss; detail=UPSTREAM-ERROR")
	if rep.body() != "asked again" || up.calls.Load() != 3 {
		t.Errorf("body %q after %d requests to the upstream", rep.body(), up.calls.Load())
	}

	// With a stale response that may stand in for an error, that one.
	up = failing(respond(http.StatusOK, "stale", "Cache-Control: max-age=0", `Etag: "v1"`))
	c.expect(c.get("/b", up), "/b", "fwd=uri-miss; stored")
	rep = c.get("/b", up, `If-None-Match: "other"`)
	c.expect(rep, "/b", "fwd=stale; detail=STALE")
	if rep.body() != "stale" || up.calls.Load() != 2 {
		t.Errorf("body %q after %d requests to the upstream", rep.body(), up.calls.Load())
	}
	if n := c.s.Stats().Entries; n != 1 || tempFiles(c.s) != 0 {
		t.Errorf("%d entries, %d temporary files", n, tempFiles(c.s))
	}

	// The same goes for the upstream that fails before it stores anything:
	// with an error of its own, or with none to show.
	up = upstreamFunc(func(w http.ResponseWriter, _ *http.Request, call int) error {
		switch call {
		case 1:
			send(w, http.StatusOK, "stale", "Cache-Control: max-age=0", `Etag: "v1"`)
		case 2:
			send(w, http.StatusBadGateway, "the upstream of the upstream is down")
		case 3:
			return errInjected
		default:
			// Unless it has a response after all, which is not to be stored:
			// the one that was is not served anymore.
			send(w, http.StatusOK, "private", "Cache-Control: private")
		}

		return nil
	})
	c.expect(c.get("/c", up), "/c", "fwd=uri-miss; stored")
	for _, want := range []string{"fwd=stale; fwd-status=502; detail=STALE", "fwd=stale; detail=STALE"} {
		rep = c.get("/c", up)
		c.expect(rep, "/c", want)
		if rep.Code != http.StatusOK || rep.body() != "stale" {
			t.Errorf("status %d, body %q", rep.Code, rep.body())
		}
	}
	rep = c.get("/c", up)
	c.expect(rep, "/c", "fwd=stale; detail=PRIVATE")
	if rep.body() != "private" || c.s.Stats().Entries != 1 {
		t.Errorf("body %q, %d entries", rep.body(), c.s.Stats().Entries)
	}
}

// TestUpstreamThatDoesNotAnswer covers the fetch that ends before the
// upstream said anything.
func TestUpstreamThatDoesNotAnswer(t *testing.T) {
	grace := clientGoneGrace
	clientGoneGrace = 10 * time.Millisecond
	defer func() { clientGoneGrace = grace }()

	c := newCacheTest(t, Options{})

	// The client leaves, and the upstream is not waited for much longer. A
	// reverse proxy returns without an error when its request is canceled.
	ctx, cancel := context.WithCancel(context.Background())
	rep := c.do(newRequest(http.MethodGet, "/a").WithContext(ctx), upstreamFunc(func(_ http.ResponseWriter, r *http.Request, _ int) error {
		cancel()
		<-r.Context().Done()

		return nil
	}))
	if !errors.Is(rep.err, context.Canceled) || rep.aborted {
		t.Errorf("a request whose client left returned %v", rep.err)
	}

	// The upstream is lost: the client is not to take that for a response.
	rep = c.get("/a", upstreamFunc(func(http.ResponseWriter, *http.Request, int) error {
		panic(http.ErrAbortHandler)
	}))
	if !rep.aborted {
		t.Errorf("a request whose upstream was lost returned %v", rep.err)
	}
	// And neither when it is lost in the middle of a response that is not
	// stored.
	rep = c.get("/a", upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
		sendHeader(w, http.StatusOK, "Cache-Control: no-store", "Content-Length: 10")
		_, _ = io.WriteString(w, "01234")
		panic(http.ErrAbortHandler)
	}))
	if !rep.aborted || rep.body() != "01234" {
		t.Errorf("a response cut short was returned as %v after %q", rep.err, rep.body())
	}

	if st := c.s.Stats(); st.Entries != 0 || tempFiles(c.s) != 0 {
		t.Errorf("%d entries, %d temporary files", st.Entries, tempFiles(c.s))
	}
}

// TestClientThatLeaves covers the client that is gone when the cache has
// something to tell it, or to decide for it.
func TestClientThatLeaves(t *testing.T) {
	c := newCacheTest(t, Options{})

	// A response the cache cannot use, to a request that would have to be
	// made again: there is nobody to make it for.
	ctx, cancel := context.WithCancel(context.Background())
	up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
		cancel()
		send(w, http.StatusOK, "body", "Cache-Control: no-store")

		return nil
	})
	rep := c.do(newRequest(http.MethodGet, "/a", "Range: bytes=0-1").WithContext(ctx), up)
	if !errors.Is(rep.err, context.Canceled) || up.calls.Load() != 1 {
		t.Errorf("returned %v after %d requests to the upstream", rep.err, up.calls.Load())
	}

	// A response that does not say where it ends is not stored for a client
	// that is gone: it could go on forever.
	ctx, cancel = context.WithCancel(context.Background())
	// stopped is why the fetch was stopped, if it was when the upstream was
	// done.
	var stopped error
	up = upstreamFunc(func(w http.ResponseWriter, r *http.Request, _ int) error {
		cancel()
		waitGone(t, w)
		sendHeader(w, http.StatusOK)
		_, _ = io.WriteString(w, "body")
		stopped = r.Context().Err()

		return nil
	})
	rep = c.do(newRequest(http.MethodGet, "/b").WithContext(ctx), up)
	if got := rep.status(); got != "Caddy; fwd=uri-miss; detail=CLIENT-GONE; key="+keyOf("/b") || stopped == nil {
		t.Errorf("Cache-Status: %s, fetch stopped: %v", got, stopped)
	}
	// Which says nothing of the response.
	c.expect(c.get("/b", respond(http.StatusOK, "body")), "/b", "fwd=uri-miss; stored")

	// A response that is not stored is not fetched for a client that is
	// gone.
	ctx, cancel = context.WithCancel(context.Background())
	up = upstreamFunc(func(w http.ResponseWriter, r *http.Request, _ int) error {
		sendHeader(w, http.StatusOK, "Cache-Control: no-store")
		_, _ = io.WriteString(w, "01234")
		cancel()
		waitGone(t, w)
		stopped = r.Context().Err()

		return stopped
	})
	rep = c.do(newRequest(http.MethodGet, "/c").WithContext(ctx), up)
	if !errors.Is(rep.err, context.Canceled) {
		t.Errorf("returned %v, fetch stopped: %v", rep.err, stopped)
	}

	// Nor is one that does not say where it ends, once the client it was
	// being sent to is gone.
	ctx, cancel = context.WithCancel(context.Background())
	up = upstreamFunc(func(w http.ResponseWriter, r *http.Request, _ int) error {
		sendHeader(w, http.StatusOK)
		_, _ = io.WriteString(w, "01234")
		cancel()
		waitGone(t, w)
		stopped = r.Context().Err()

		return stopped
	})
	rep = c.do(newRequest(http.MethodGet, "/endless").WithContext(ctx), up)
	if !rep.aborted || stopped == nil || c.s.Stats().Entries != 1 {
		t.Errorf("returned %v, fetch stopped: %v, %d entries", rep.err, stopped, c.s.Stats().Entries)
	}

	// One that does goes on being received, for the requests to come.
	ctx, cancel = context.WithCancel(context.Background())
	up = upstreamFunc(func(w http.ResponseWriter, r *http.Request, _ int) error {
		sendHeader(w, http.StatusOK, "Content-Length: 10")
		_, _ = io.WriteString(w, "01234")
		cancel()
		waitGone(t, w)
		_, _ = io.WriteString(w, "56789")
		stopped = r.Context().Err()

		return nil
	})
	c.do(newRequest(http.MethodGet, "/sized").WithContext(ctx), up)
	if stopped != nil {
		t.Errorf("the fetch of a response to store was stopped: %v", stopped)
	}
	rep = c.get("/sized", up)
	c.expectHit(rep, "/sized", 120)
	if rep.body() != "0123456789" {
		t.Errorf("body %q", rep.body())
	}

	// A response stored without being sent meanwhile goes on being
	// received, for as long as the upstream goes on sending it.
	ctx, cancel = context.WithCancel(context.Background())
	up = upstreamFunc(func(w http.ResponseWriter, r *http.Request, _ int) error {
		sendHeader(w, http.StatusOK, "Content-Length: 10")
		_, _ = io.WriteString(w, "01234")
		cancel()
		waitGone(t, w)
		_, _ = io.WriteString(w, "56789")
		stopped = r.Context().Err()

		return nil
	})
	c.do(newRequest(http.MethodGet, "/d", `If-None-Match: "other"`).WithContext(ctx), up)
	if stopped != nil {
		t.Errorf("the fetch of a response to store was stopped: %v", stopped)
	}
	rep = c.get("/d", up)
	c.expectHit(rep, "/d", 120)
	if rep.body() != "0123456789" {
		t.Errorf("body %q", rep.body())
	}
}

// TestConfirmedResponse covers what a stored response becomes when the
// upstream confirms it.
func TestConfirmedResponse(t *testing.T) {
	const confirmed = "fwd=stale; fwd-status=304; detail=REVALIDATED"

	t.Run("by its date", func(t *testing.T) {
		c := newCacheTest(t, Options{})
		var asked http.Header
		up := upstreamFunc(func(w http.ResponseWriter, r *http.Request, call int) error {
			asked = r.Header.Clone()
			switch call {
			case 1:
				send(w, http.StatusOK, "body", "Cache-Control: no-cache", "Last-Modified: "+lastModified)
			case 2:
				// The type is that of the body, which this is not.
				sendHeader(w, http.StatusNotModified, `Etag: "v2"`, "Content-Type: text/html", "X-Confirmed: once")
			default:
				// It comes from a cache, which had it for a while.
				sendHeader(w, http.StatusNotModified, `Etag: "v3"`, "X-Confirmed: twice", "Age: 30")
			}

			return nil
		})

		c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")
		rep := c.get("/a", up)
		c.expect(rep, "/a", confirmed)
		if asked.Get("If-Modified-Since") != lastModified || asked.Get("If-None-Match") != "" {
			t.Errorf("the upstream was asked with %v", asked)
		}
		if h := rep.Header(); rep.body() != "body" || h.Get("Etag") != `"v2"` || h.Get("Content-Type") != "text/plain" || h.Get("X-Confirmed") != "once" {
			t.Errorf("body %q, headers %v", rep.body(), h)
		}

		// The tag the clients were given stays.
		rep = c.get("/a", up)
		c.expect(rep, "/a", confirmed)
		if asked.Get("If-Modified-Since") != lastModified || asked.Get("If-None-Match") != `"v2"` {
			t.Errorf("the upstream was asked with %v", asked)
		}
		if h := rep.Header(); h.Get("Etag") != `"v2"` || h.Get("X-Confirmed") != "twice" || h.Get("Age") != "30" {
			t.Errorf("headers %v", h)
		}
	})

	t.Run("not to be kept", func(t *testing.T) {
		c := newCacheTest(t, Options{})
		up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, call int) error {
			if call == 1 {
				send(w, http.StatusOK, "body", "Cache-Control: no-cache", `Etag: "v1"`)
			} else {
				sendHeader(w, http.StatusNotModified, "Cache-Control: no-store")
			}

			return nil
		})

		c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")
		rep := c.get("/a", up)
		c.expect(rep, "/a", confirmed)
		if rep.body() != "body" || c.s.Stats().Entries != 0 {
			t.Errorf("body %q, %d entries", rep.body(), c.s.Stats().Entries)
		}
	})

	t.Run("in a store that is closed", func(t *testing.T) {
		c := newCacheTest(t, Options{})
		up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, call int) error {
			if call == 1 {
				send(w, http.StatusOK, "body", "Cache-Control: no-cache", `Etag: "v1"`)
			} else {
				_ = c.s.Close()
				sendHeader(w, http.StatusNotModified)
			}

			return nil
		})

		c.expect(c.get("/a", up), "/a", "fwd=uri-miss; stored")
		rep := c.get("/a", up)
		c.expect(rep, "/a", confirmed)
		if rep.body() != "body" {
			t.Errorf("body %q", rep.body())
		}
	})

	t.Run("while what leads to it is replaced", func(t *testing.T) {
		c := newCacheTest(t, Options{})
		up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, call int) error {
			if call == 1 {
				send(w, http.StatusOK, "bonjour", "Vary: Accept-Language", "Cache-Control: no-cache", `Etag: "v1"`)
				return nil
			}

			// Another request gets a response that varies on more, which
			// makes all that was stored for the URL unreachable.
			err := storePut(t, c.s, keyOf("/a"), []string{"accept-language", "x-kind"}, http.Header{"Accept-Language": {"de"}}, []byte("hallo"))
			if err != nil {
				t.Error(err)
			}
			sendHeader(w, http.StatusNotModified)

			return nil
		})

		c.expect(c.get("/a", up, "Accept-Language: fr"), "/a", "fwd=uri-miss; stored")
		rep := c.get("/a", up, "Accept-Language: fr")
		c.expect(rep, "/a", confirmed)
		if rep.body() != "bonjour" {
			t.Errorf("body %q", rep.body())
		}
		c.expect(c.get("/a", respond(http.StatusOK, "bonjour", "Vary: Accept-Language, X-Kind"), "Accept-Language: fr"), "/a", "fwd=uri-miss; stored")
	})
}

// TestResponsesThatAreNotStored covers the responses the cache finds it
// cannot store when their header comes.
func TestResponsesThatAreNotStored(t *testing.T) {
	// One that says it is larger than what is stored.
	c := newCacheTest(t, Options{MaxBodyBytes: 10})
	up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
		// What the upstream handlers ask of the connection is asked of the
		// client's.
		if err := http.NewResponseController(w).EnableFullDuplex(); err == nil {
			t.Error("the test client supports full duplex")
		}
		send(w, http.StatusOK, "more than ten bytes")
		// And what they flush goes to the client.
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Error(err)
		}

		return nil
	})
	for range 2 {
		rep := c.get("/a", up)
		c.expect(rep, "/a", "fwd=uri-miss; detail=TOO-LARGE")
		if rep.body() != "more than ten bytes" || !rep.Flushed {
			t.Errorf("body %q, flushed: %v", rep.body(), rep.Flushed)
		}
	}

	// One the store has no room for: another download holds the one file
	// the cache may have.
	c = newCacheTest(t, Options{MaxFileCount: 1})
	other, err := c.s.Create("other", nil, nil, testRecord(), 0, -1, 1)
	if err != nil {
		t.Fatal(err)
	}
	rep := c.get("/a", respond(http.StatusOK, "body"))
	c.expect(rep, "/a", "fwd=uri-miss; detail=STORAGE-ERROR")
	if rep.body() != "body" {
		t.Errorf("body %q", rep.body())
	}
	// Which says nothing of the response.
	other.Abort()
	c.expect(c.get("/a", respond(http.StatusOK, "body")), "/a", "fwd=uri-miss; stored")
}

// TestRangeOfWhatIsNotABody checks that a range is only served of a 200
// response.
func TestRangeOfWhatIsNotABody(t *testing.T) {
	c := newCacheTest(t, Options{})
	up := respond(http.StatusMovedPermanently, "moved", "Location: /b")

	for range 2 {
		rep := c.get("/a", up, "Range: bytes=0-1")
		if rep.Code != http.StatusMovedPermanently || rep.body() != "moved" || rep.Header().Get("Location") != "/b" {
			t.Errorf("status %d, body %q, headers %v", rep.Code, rep.body(), rep.Header())
		}
	}
	c.expectHit(c.get("/a", up), "/a", 120)
	if n := up.calls.Load(); n != 1 {
		t.Errorf("the upstream got %d requests, want 1", n)
	}
}

// TestResponseThatCannotBeReadBack covers the response that is stored but
// cannot be sent to the client from where it is stored, which only happens
// here by taking its file away.
func TestResponseThatCannotBeReadBack(t *testing.T) {
	c := newCacheTest(t, Options{})

	fetch := func(key string, declared int64) *fetchWriter {
		fw := c.newFetch(key)

		var err error
		if fw.w, err = c.s.Create(key, nil, nil, testRecord(), 0, declared, 1); err != nil {
			t.Fatal(err)
		}

		return fw
	}
	check := func(fw *fetchWriter) {
		t.Helper()

		if fw.w != nil || fw.pump != nil || fw.reason != "STORAGE-ERROR" || tempFiles(c.s) != 0 {
			t.Errorf("the response is still being stored: reason %s, %d temporary files", fw.reason, tempFiles(c.s))
		}
	}

	// The file is gone.
	fw := fetch("gone", -1)
	if err := os.Remove(fw.w.tmp); err != nil {
		t.Fatal(err)
	}
	fw.startPumpLocked(-1)
	check(fw)

	// The range to send starts in a body of unknown length.
	fw = fetch("unsized", -1)
	fw.first = 5
	fw.startPumpLocked(10)
	check(fw)
}

// TestClientThatFailsDuringAFetch covers the response that turns out too
// large to store while a client that fails is being sent it.
func TestClientThatFailsDuringAFetch(t *testing.T) {
	c := newCacheTest(t, Options{MaxBodyBytes: 10})

	// The second half is one byte too many for the cache.
	var second error
	up := upstreamFunc(func(w http.ResponseWriter, r *http.Request, _ int) error {
		sendHeader(w, http.StatusOK)
		if _, err := io.WriteString(w, "01234567"); err != nil {
			t.Errorf("writing what can be stored: %v", err)
		}
		_, second = io.WriteString(w, "89a")

		return second
	})

	// The client did not take what was stored: the rest is not for it.
	client := &brokenClient{err: errInjected}
	out := c.serve(client, newRequest(http.MethodGet, "/a"), up)
	if !errors.Is(second, errClientGone) || !errors.Is(out.err, errClientGone) || out.aborted {
		t.Errorf("the upstream was told %v, the server %v", second, out.err)
	}

	// The client took what was stored, and fails when sent the rest.
	c.s.SetUncacheable(makeID(keyOf("/a")), false)
	client = &brokenClient{takes: 9, err: errInjected}
	out = c.serve(client, newRequest(http.MethodGet, "/a"), up)
	if !errors.Is(second, errInjected) || !errors.Is(out.err, errInjected) || out.aborted || string(client.got) != "012345678" {
		t.Errorf("the upstream was told %v, the server %v, the client got %q", second, out.err, client.got)
	}

	if st := c.s.Stats(); st.Entries != 0 || tempFiles(c.s) != 0 {
		t.Errorf("%d entries, %d temporary files", st.Entries, tempFiles(c.s))
	}
}

// TestResponseThatCannotBeStoredAfterAll covers the response the cache
// gives up storing while it sends it to a client that is still there.
func TestResponseThatCannotBeStoredAfterAll(t *testing.T) {
	// One that turns out too large: the client gets the rest as it comes.
	c := newCacheTest(t, Options{MaxBodyBytes: 10})
	up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
		sendHeader(w, http.StatusOK)
		for _, part := range []string{"01234567", "89abcdef", "ghij"} {
			if _, err := io.WriteString(w, part); err != nil {
				return err
			}
		}

		return nil
	})
	// Nothing tells the next request that the response is as large again.
	for range 2 {
		rep := c.get("/a", up)
		c.expect(rep, "/a", "fwd=uri-miss; stored")
		if rep.body() != "0123456789abcdefghij" {
			t.Errorf("body %q", rep.body())
		}
	}
	if st := c.s.Stats(); st.Entries != 0 || tempFiles(c.s) != 0 {
		t.Errorf("%d entries, %d temporary files", st.Entries, tempFiles(c.s))
	}

	// One the disk has no room for, other downloads having taken it, of
	// which the client asked for a range: once it has that range, the rest
	// is of no use to anyone.
	c = newCacheTest(t, Options{MaxSize: 64 << 10})
	for _, key := range []string{"first", "second"} {
		other, err := c.s.Create(key, nil, nil, testRecord(), 0, -1, 1)
		if err != nil {
			t.Fatal(err)
		}
		defer other.Abort()
		if _, err := other.Write(bodyFor(key, 25_000)); err != nil {
			t.Fatal(err)
		}
	}
	content := string(sliceContent("range", 20_000))
	var second error
	up = upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
		sendHeader(w, http.StatusOK, fmt.Sprintf("Content-Length: %d", len(content)))
		if _, err := io.WriteString(w, content[:5000]); err != nil {
			t.Errorf("writing what can be stored: %v", err)
		}
		_, second = io.WriteString(w, content[5000:])

		return second
	})
	rep := c.get("/a", up, "Range: bytes=2-5")
	c.expect(rep, "/a", "fwd=uri-miss; stored")
	if rep.Code != http.StatusPartialContent || rep.body() != content[2:6] || !errors.Is(second, errClientGone) {
		t.Errorf("status %d, body %q, the upstream was told %v", rep.Code, rep.body(), second)
	}
	if st := c.s.Stats(); st.Entries != 0 || tempFiles(c.s) != 2 {
		t.Errorf("%d entries, %d temporary files", st.Entries, tempFiles(c.s))
	}
}

// TestClientLeavingAsTheFetchEnds checks that the fetch that is over when
// it is told that the client left is not stopped: there is nothing to stop.
func TestClientLeavingAsTheFetchEnds(t *testing.T) {
	c := newCacheTest(t, Options{})

	fw := c.newFetch("key")
	stopped := false
	fw.cancel = func() { stopped = true }

	fw.finish(false)
	fw.clientGone()
	if stopped || !fw.gone {
		t.Errorf("stopped: %v, client known gone: %v", stopped, fw.gone)
	}
	if err := fw.close(); err != nil {
		t.Error(err)
	}
}

// TestResponseTooLargeForAConditionalRequest covers the response found too
// large to store when nothing of it was sent to the client, whose request
// can therefore be made again.
func TestResponseTooLargeForAConditionalRequest(t *testing.T) {
	c := newCacheTest(t, Options{MaxBodyBytes: 10})

	var written error
	up := upstreamFunc(func(w http.ResponseWriter, r *http.Request, call int) error {
		if r.Header.Get("If-None-Match") != "" {
			sendHeader(w, http.StatusNotModified)
			return nil
		}

		sendHeader(w, http.StatusOK, `Etag: "v1"`)
		_, written = io.WriteString(w, "more than ten bytes")

		return written
	})

	rep := c.get("/a", up, `If-None-Match: "v1"`)
	c.expect(rep, "/a", "fwd=uri-miss; detail=TOO-LARGE")
	if rep.Code != http.StatusNotModified || !errors.Is(written, errFetchAborted) || up.calls.Load() != 2 {
		t.Errorf("status %d after %d requests to the upstream, the first of which was told %v", rep.Code, up.calls.Load(), written)
	}
	if st := c.s.Stats(); st.Entries != 0 || tempFiles(c.s) != 0 {
		t.Errorf("%d entries, %d temporary files", st.Entries, tempFiles(c.s))
	}
}

// TestResponseShorterThanItSays checks that a response is not stored, nor
// delivered as complete, when the upstream handlers return before they
// wrote all of it.
func TestResponseShorterThanItSays(t *testing.T) {
	c := newCacheTest(t, Options{})
	up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
		sendHeader(w, http.StatusOK, "Content-Length: 10")
		_, _ = io.WriteString(w, "01234")

		return nil
	})

	rep := c.get("/a", up)
	if !rep.aborted {
		t.Errorf("a response cut short was delivered: %v, %q", rep.err, rep.body())
	}
	if st := c.s.Stats(); st.Entries != 0 || tempFiles(c.s) != 0 {
		t.Errorf("%d entries, %d temporary files", st.Entries, tempFiles(c.s))
	}
}

// TestSliceOrNothing checks that the fetch of a slice that follows others
// hands over nothing but a slice.
func TestSliceOrNothing(t *testing.T) {
	c := newCacheTest(t, Options{})
	if err := storePut(t, c.s, "key", nil, nil, []byte("whole")); err != nil {
		t.Fatal(err)
	}
	_, hit := c.s.Lookup("key", nil)
	if hit == nil {
		t.Fatal("not found")
	}

	rec := httptest.NewRecorder()
	x := &exchange{h: c.h, c: c.h.cfg, store: c.s, w: rec, r: newRequest(http.MethodGet, "/a"), key: "key", slice: new(sliceFetch)}
	if err := x.deliver(hit, time.Now(), "hit"); !errors.Is(err, errSliceLost) || !hit.closed || rec.Body.Len() != 0 {
		t.Errorf("a whole response was delivered in the middle of another: %v, %q", err, rec.Body)
	}

	// Nor is it written anything.
	var sink http.ResponseWriter = &sliceSink{header: make(http.Header)}
	sink.WriteHeader(http.StatusOK)
	if n, err := sink.Write([]byte("body")); n != 4 || err != nil {
		t.Errorf("wrote %d bytes, %v", n, err)
	}
}

// What follows covers the slice option.

// testSlice is the size of the slices in these tests, and slicedBody a body
// of three slices, the last one shorter than the others.
const testSlice = minSlice

var slicedBody = string(sliceContent("v1", 10_000))

// sliced returns an upstream that has content and sends the range of it
// that it is asked for, with the given headers. If answer is set, it is
// given each request first, with the first byte of the range asked for or
// -1 when the request is for all of the content, and tells whether it
// answered the request itself.
func sliced(content string, answer func(w http.ResponseWriter, r *http.Request, first, call int) (bool, error), headers ...string) *backend {
	return upstreamFunc(func(w http.ResponseWriter, r *http.Request, call int) error {
		first, last := -1, len(content)-1
		if spec := r.Header.Get("Range"); spec != "" {
			if _, err := fmt.Sscanf(spec, "bytes=%d-%d", &first, &last); err != nil {
				return fmt.Errorf("unexpected range %q", spec)
			}
			last = min(last, len(content)-1)
		}

		if answer != nil {
			if answered, err := answer(w, r, first, call); answered {
				return err
			}
		}

		switch {
		case first < 0:
			send(w, http.StatusOK, content, headers...)
		case first >= len(content):
			sendHeader(w, http.StatusRequestedRangeNotSatisfiable, fmt.Sprintf("Content-Range: bytes */%d", len(content)))
		default:
			send(w, http.StatusPartialContent, content[first:last+1], append(headers, contentRange(first, last, len(content)))...)
		}

		return nil
	})
}

func contentRange(first, last, total int) string {
	return fmt.Sprintf("Content-Range: bytes %d-%d/%d", first, last, total)
}

// watchedClient is a client that tells when it received a number of bytes.
type watchedClient struct {
	*httptest.ResponseRecorder
	want     int
	received chan struct{}

	mu  sync.Mutex
	got int
}

func watchClient(want int) *watchedClient {
	return &watchedClient{ResponseRecorder: httptest.NewRecorder(), want: want, received: make(chan struct{})}
}

func (c *watchedClient) Write(p []byte) (int, error) {
	n, err := c.ResponseRecorder.Write(p)

	c.mu.Lock()
	before := c.got
	c.got += n
	if before < c.want && c.got >= c.want {
		close(c.received)
	}
	c.mu.Unlock()

	return n, err
}

// statusOf returns the status code of the error a handler returned.
func statusOf(err error) int {
	var herr caddyhttp.HandlerError
	if errors.As(err, &herr) {
		return herr.StatusCode
	}

	return 0
}

func TestSlicesWithDefaults(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice, DefaultCacheControl: "max-age=60"})
	up := sliced(slicedBody, nil, "Last-Modified: "+lastModified)

	rep := c.get("/a", up)
	c.expect(rep, "/a", "fwd=uri-miss; stored")
	if h := rep.Header(); rep.body() != slicedBody || h.Get("Cache-Control") != "max-age=60" || h.Get("Last-Modified") != lastModified || h.Get("Content-Range") != "" {
		t.Errorf("body %q, headers %v", abbreviate(rep.body()), h)
	}
	rep = c.get("/a", up, "Range: bytes=5000-5009")
	c.expectHit(rep, "/a", 60)
	if rep.Code != http.StatusPartialContent || rep.body() != slicedBody[5000:5010] {
		t.Errorf("status %d, body %q", rep.Code, rep.body())
	}
	if rep = c.get("/a", up, "If-Modified-Since: "+lastModified); rep.Code != http.StatusNotModified {
		t.Errorf("status %d for a request that has the response already", rep.Code)
	}
	if n := up.calls.Load(); n != 3 {
		t.Errorf("the upstream got %d requests, want one per slice", n)
	}
	// The response ends before the range: its first slice tells where.
	rep = c.get("/a", up, "Range: bytes=20000-")
	if rep.Code != http.StatusRequestedRangeNotSatisfiable || rep.Header().Get("Content-Range") != "bytes */10000" || up.calls.Load() != 4 {
		t.Errorf("status %d, headers %v after %d requests to the upstream", rep.Code, rep.Header(), up.calls.Load())
	}

	// A response larger than what is stored is not stored in slices either.
	c = newCacheTest(t, Options{Slice: testSlice, MaxBodyBytes: 5000})
	up = sliced(slicedBody, nil)
	for _, want := range []string{"TOO-LARGE", "UNCACHEABLE"} {
		rep := c.get("/a", up)
		c.expect(rep, "/a", "fwd=uri-miss; detail="+want)
		if rep.body() != slicedBody {
			t.Errorf("body %q", abbreviate(rep.body()))
		}
	}
	if n := up.calls.Load(); n != 3 {
		t.Errorf("the upstream got %d requests, want 3", n)
	}
}

// TestUpstreamThatAnswersAnotherRange covers the upstream that sends a
// range, but not the one it was asked for: its responses are stored whole.
func TestUpstreamThatAnswersAnotherRange(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice})

	for path, header := range map[string]string{
		// All of it, whatever is asked.
		"/all": contentRange(0, len(slicedBody)-1, len(slicedBody)),
		// The slice, of a response it does not tell the size of.
		"/unsized": fmt.Sprintf("Content-Range: bytes 0-%d/*", testSlice-1),
		// The slice, it says, with a body that is not.
		"/longer": contentRange(0, testSlice-1, len(slicedBody)),
	} {
		up := sliced(slicedBody, func(w http.ResponseWriter, _ *http.Request, first, _ int) (bool, error) {
			if first < 0 {
				return false, nil
			}
			send(w, http.StatusPartialContent, slicedBody, header)

			return true, nil
		})

		rep := c.get(path, up)
		c.expect(rep, path, "fwd=uri-miss; stored")
		if rep.Code != http.StatusOK || rep.body() != slicedBody {
			t.Errorf("%s: status %d, body %q", path, rep.Code, abbreviate(rep.body()))
		}
		c.expectHit(c.get(path, up), path, 120)
		if n := up.calls.Load(); n != 2 {
			t.Errorf("%s: the upstream got %d requests, want one for a slice and one for the response", path, n)
		}
	}
}

// TestSliceThatIsNotSent covers the upstream that answers something else
// than a slice in the middle of a response.
func TestSliceThatIsNotSent(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice})

	// failing returns an upstream that answers the request for the second
	// slice with the given status, until it is told to stop.
	failing := func(status int, healthy *atomic.Bool) *backend {
		return sliced(slicedBody, func(w http.ResponseWriter, _ *http.Request, first, _ int) (bool, error) {
			if first != testSlice || healthy.Load() {
				return false, nil
			}
			send(w, status, "no such thing")

			return true, nil
		})
	}

	// The response is gone: what the cache has of it goes too.
	var healthy atomic.Bool
	up := failing(http.StatusNotFound, &healthy)
	rep := c.get("/gone", up)
	if !rep.aborted || rep.body() != slicedBody[:testSlice] {
		t.Errorf("a response whose second slice is gone was not cut short: %v, %q", rep.err, abbreviate(rep.body()))
	}
	healthy.Store(true)
	rep = c.get("/gone", up)
	c.expect(rep, "/gone", "fwd=uri-miss; stored")
	if rep.body() != slicedBody || up.calls.Load() != 5 {
		t.Errorf("body %q after %d requests to the upstream", abbreviate(rep.body()), up.calls.Load())
	}

	// The upstream fails, which says nothing of the response.
	healthy.Store(false)
	up = failing(http.StatusBadGateway, &healthy)
	rep = c.get("/failing", up)
	if !rep.aborted || rep.body() != slicedBody[:testSlice] {
		t.Errorf("a response whose second slice failed was not cut short: %v, %q", rep.err, abbreviate(rep.body()))
	}
	healthy.Store(true)
	rep = c.get("/failing", up)
	c.expectHit(rep, "/failing", 120)
	if rep.body() != slicedBody || up.calls.Load() != 4 {
		t.Errorf("body %q after %d requests to the upstream", abbreviate(rep.body()), up.calls.Load())
	}
}

// TestSliceThatCannotBeStored covers the slices the cache has to give up
// when their header comes.
func TestSliceThatCannotBeStored(t *testing.T) {
	// The store has no room: another download holds the one file the cache
	// may have.
	c := newCacheTest(t, Options{Slice: testSlice, MaxFileCount: 1})
	other, err := c.s.Create("other", nil, nil, testRecord(), 0, -1, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Abort()

	up := sliced(slicedBody, nil)
	rep := c.get("/a", up)
	c.expect(rep, "/a", "fwd=uri-miss; detail=STORAGE-ERROR")
	if rep.body() != slicedBody || up.calls.Load() != 2 {
		t.Errorf("body %q after %d requests to the upstream", abbreviate(rep.body()), up.calls.Load())
	}

	// The response is not to be stored anymore: the slice the cache has of
	// it is not served anymore either.
	c = newCacheTest(t, Options{Slice: testSlice})
	var private atomic.Bool
	up = upstreamFunc(func(w http.ResponseWriter, r *http.Request, call int) error {
		control := "Cache-Control: no-cache"
		if private.Load() {
			control = "Cache-Control: no-store"
		}

		return sliced(slicedBody, nil, control, `Etag: "v1"`).ServeHTTP(w, r)
	})
	c.expect(c.get("/a", up, "Range: bytes=0-99"), "/a", "fwd=uri-miss; stored")
	private.Store(true)
	rep = c.get("/a", up, "Range: bytes=0-99")
	c.expect(rep, "/a", "fwd=uri-miss; detail=NO-STORE")
	if rep.Code != http.StatusPartialContent || rep.body() != slicedBody[:100] {
		t.Errorf("status %d, body %q", rep.Code, abbreviate(rep.body()))
	}
	if _, hit := c.s.Lookup(keyOf("/a"), http.Header{sliceSelector: {sliceRange(0, testSlice-1)}}); hit != nil {
		hit.Close()
		t.Error("the slice of a response that is not to be stored is still there")
	}
}

// TestSlicedResponseThatIsReplaced covers the upstream that has another
// response than the one the cache holds slices of.
func TestSlicedResponseThatIsReplaced(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice})

	type version struct{ content, tag string }
	var current atomic.Value
	up := upstreamFunc(func(w http.ResponseWriter, r *http.Request, _ int) error {
		v := current.Load().(version)

		return sliced(v.content, nil, "Etag: "+v.tag).ServeHTTP(w, r)
	})

	// With another tag: the client is not sent the slices of both as one,
	// and those of the first go.
	current.Store(version{slicedBody, `"v1"`})
	c.expect(c.get("/a", up, "Range: bytes=0-0"), "/a", "fwd=uri-miss; stored")
	current.Store(version{slicedBody, `"v2"`})
	rep := c.get("/a", up)
	if !rep.aborted || rep.body() != slicedBody[:testSlice] {
		t.Errorf("a response made of two was not cut short: %v, %q", rep.err, abbreviate(rep.body()))
	}
	rep = c.get("/a", up)
	c.expect(rep, "/a", "fwd=uri-miss; stored")
	if rep.body() != slicedBody || rep.Header().Get("Etag") != `"v2"` {
		t.Errorf("body %q, headers %v", abbreviate(rep.body()), rep.Header())
	}

	// With the same tag, and shorter.
	c.expect(c.get("/b", up, "Range: bytes=0-0"), "/b", "fwd=uri-miss; stored")
	current.Store(version{slicedBody[:testSlice], `"v2"`})
	if rep = c.get("/b", up); !rep.aborted || rep.body() != slicedBody[:testSlice] {
		t.Errorf("a response that ends before it said was not cut short: %v, %q", rep.err, abbreviate(rep.body()))
	}
	rep = c.get("/b", up)
	c.expect(rep, "/b", "fwd=uri-miss; stored")
	if rep.body() != slicedBody[:testSlice] {
		t.Errorf("body %q", abbreviate(rep.body()))
	}
}

// TestSmallResponsesWithSlices covers the responses that fit in their
// first slice, which are stored whole.
func TestSmallResponsesWithSlices(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice})
	small := slicedBody[:1000]

	// One that varies is found beside the slices of the others.
	up := sliced(small, nil, "Vary: Accept-Language")
	c.expect(c.get("/a", up, "Accept-Language: fr"), "/a", "fwd=uri-miss; stored")
	c.expectHit(c.get("/a", up, "Accept-Language: fr"), "/a", 120)
	rep := c.get("/a", up, "Accept-Language: de")
	c.expect(rep, "/a", "fwd=uri-miss; stored")
	if rep.Code != http.StatusOK || rep.body() != small || up.calls.Load() != 2 {
		t.Errorf("status %d, body %q after %d requests to the upstream", rep.Code, abbreviate(rep.body()), up.calls.Load())
	}

	// One that grows into slices: the stale response that was stored whole
	// is not found before them anymore.
	var content atomic.Value
	content.Store(small)
	up = upstreamFunc(func(w http.ResponseWriter, r *http.Request, _ int) error {
		body := content.Load().(string)

		return sliced(body, nil, "Cache-Control: max-age=0", fmt.Sprintf(`Etag: "%d"`, len(body))).ServeHTTP(w, r)
	})
	c.expect(c.get("/b", up), "/b", "fwd=uri-miss; stored")
	content.Store(slicedBody)
	rep = c.get("/b", up)
	c.expect(rep, "/b", "fwd=stale; stored")
	if rep.body() != slicedBody || rep.Header().Get("Etag") != `"10000"` {
		t.Errorf("body %q, headers %v", abbreviate(rep.body()), rep.Header())
	}
	if _, hit := c.s.Lookup(keyOf("/b"), nil); hit != nil {
		hit.Close()
		t.Errorf("a whole response is still stored: status %d", hit.rec.status)
	}
}

// TestSliceLargerThanItSays covers the upstream that sends more than the
// slice it announced, and more than a file may hold.
func TestSliceLargerThanItSays(t *testing.T) {
	const slice = 2 * testSlice
	c := newCacheTest(t, Options{Slice: slice, MaxSize: 2 * slice})
	content := string(sliceContent("large", 20_000))

	var first, second error
	up := sliced(content, func(w http.ResponseWriter, _ *http.Request, _, _ int) (bool, error) {
		sendHeader(w, http.StatusPartialContent, contentRange(0, slice-1, len(content)), fmt.Sprintf("Content-Length: %d", slice))
		_, first = io.WriteString(w, content[:slice+1000])
		_, second = io.WriteString(w, content[slice+1000:])

		return true, nil
	})

	// Depending on whether the request got to read the slice before it was
	// given up, the response is cut short or not started at all.
	rep := c.get("/a", up)
	if !rep.aborted && rep.err == nil {
		t.Errorf("a slice that was given up was delivered: %q", abbreviate(rep.body()))
	}
	if !errors.Is(first, errFetchAborted) || !errors.Is(second, errFetchAborted) {
		t.Errorf("the upstream was told %v, then %v", first, second)
	}
	if tempFiles(c.s) != 0 {
		t.Errorf("%d temporary files left", tempFiles(c.s))
	}
}

// TestSliceOutlivesTheClientItIsFetchedFor checks that a slice goes on
// being received when the client leaves, for the requests to come.
func TestSliceOutlivesTheClientItIsFetchedFor(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice})

	ctx, cancel := context.WithCancel(context.Background())
	stopped := errors.New("the upstream was not asked")
	var asked atomic.Int32
	up := sliced(slicedBody, func(w http.ResponseWriter, r *http.Request, first, _ int) (bool, error) {
		if first != 0 {
			return false, nil
		}
		asked.Add(1)

		sendHeader(w, http.StatusPartialContent, contentRange(0, testSlice-1, len(slicedBody)), fmt.Sprintf("Content-Length: %d", testSlice))
		_, _ = io.WriteString(w, slicedBody[:1000])
		cancel()
		waitGone(t, w)
		_, _ = io.WriteString(w, slicedBody[1000:testSlice])
		stopped = r.Context().Err()

		return true, nil
	})

	// What becomes of the request depends on where it was when its client
	// left. The fetch is over when it returns in any case.
	c.do(newRequest(http.MethodGet, "/a").WithContext(ctx), up)
	if stopped != nil {
		t.Errorf("the fetch of a slice was stopped: %v", stopped)
	}

	rep := c.get("/a", up, "Range: bytes=0-1999")
	c.expectHit(rep, "/a", 120)
	if rep.body() != slicedBody[:2000] || asked.Load() != 1 {
		t.Errorf("body %q after %d requests for the slice", abbreviate(rep.body()), asked.Load())
	}
}

// TestPanicInTheFetchOfASlice checks that the panic of an upstream handler
// is the request's, whenever it happens.
func TestPanicInTheFetchOfASlice(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice})

	// Before the client was sent anything.
	rep := c.get("/first", upstreamFunc(func(http.ResponseWriter, *http.Request, int) error {
		panic("no slice")
	}))
	if rep.panicked != "no slice" || rep.Body.Len() != 0 {
		t.Errorf("panicked with %v after %d bytes", rep.panicked, rep.Body.Len())
	}

	// Once the slice was sent whole, which the request finds out when it
	// goes on to the next one.
	client := watchClient(testSlice)
	up := sliced(slicedBody, func(w http.ResponseWriter, _ *http.Request, first, _ int) (bool, error) {
		send(w, http.StatusPartialContent, slicedBody[:testSlice], contentRange(0, testSlice-1, len(slicedBody)))
		<-client.received
		panic("slice sent")
	})
	panicked := c.serve(client, newRequest(http.MethodGet, "/late"), up).panicked
	if panicked != "slice sent" || client.Body.String() != slicedBody[:testSlice] || up.calls.Load() != 1 {
		t.Errorf("panicked with %v after %d bytes and %d requests to the upstream", panicked, client.Body.Len(), up.calls.Load())
	}
}

// plantSlice stores a response where slice n of path is looked for.
func (c *cacheTest) plantSlice(path string, n, status int, contentRange, body string) *Writer {
	c.t.Helper()

	rec := testRecord()
	rec.status = status
	rec.header = http.Header{"Content-Type": {"text/plain"}}
	if contentRange != "" {
		rec.header.Set("Content-Range", contentRange)
	}

	selector := http.Header{sliceSelector: {sliceRange(int64(n)*testSlice, int64(n+1)*testSlice-1)}}
	w, err := c.s.Create(keyOf(path), []string{sliceSelector}, selector, rec, 0, int64(len(body)), 1)
	if err != nil {
		c.t.Fatal(err)
	}
	if _, err := w.Write([]byte(body)); err != nil {
		c.t.Fatal(err)
	}

	return w
}

// TestSliceThatIsNotOne covers what is found where a slice is looked for
// without being that slice. The cache stores no such thing: it is put
// there.
func TestSliceThatIsNotOne(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice})
	up := sliced(slicedBody, nil, "Cache-Control: max-age=60")

	// Another range than the one of the slice: dropped, and fetched.
	c.expect(c.get("/range", up, "Range: bytes=0-0"), "/range", "fwd=uri-miss; stored")
	if err := c.plantSlice("/range", 1, http.StatusPartialContent, "bytes 0-99/10000", slicedBody[:100]).Commit(); err != nil {
		t.Fatal(err)
	}
	rep := c.get("/range", up)
	c.expectHit(rep, "/range", 60)
	if rep.body() != slicedBody || up.calls.Load() != 3 {
		t.Errorf("body %q after %d requests to the upstream", abbreviate(rep.body()), up.calls.Load())
	}

	// A whole response, which is of no use in the middle of another.
	c.expect(c.get("/whole", up, "Range: bytes=0-0"), "/whole", "fwd=uri-miss; stored")
	if err := c.plantSlice("/whole", 1, http.StatusOK, "", "whole").Commit(); err != nil {
		t.Fatal(err)
	}
	if rep = c.get("/whole", up); !rep.aborted || rep.body() != slicedBody[:testSlice] {
		t.Errorf("a whole response in the middle of another was not cut short: %v, %q", rep.err, abbreviate(rep.body()))
	}

	// Another range again, in a response being received.
	w := c.plantSlice("/tail", 1, http.StatusPartialContent, "bytes 0-99/10000", slicedBody[:100])
	defer w.Abort()
	sr := &sliceReader{
		x:      &exchange{h: c.h, c: c.h.cfg, store: c.s, key: keyOf("/tail")},
		size:   testSlice,
		ctx:    context.Background(),
		header: http.Header{sliceSelector: {sliceRange(testSlice, 2*testSlice-1)}},
	}
	if part := sr.tailPart(1, w); part != nil {
		part.close()
		t.Error("a response being received was taken for a slice it is not")
	}

	// A slice the upstream confirms, which is made another one meanwhile:
	// the fetch is the only one to hold it.
	up = upstreamFunc(func(w http.ResponseWriter, r *http.Request, _ int) error {
		if r.Header.Get("If-None-Match") == "" {
			return sliced(slicedBody, nil, "Cache-Control: no-cache", `Etag: "v1"`).ServeHTTP(w, r)
		}

		w.(*fetchWriter).stale.rec.header.Set("Content-Range", "bytes 0-99/10000")
		sendHeader(w, http.StatusNotModified)

		return nil
	})
	c.expect(c.get("/confirmed", up, "Range: bytes=0-0"), "/confirmed", "fwd=uri-miss; stored")
	if rep = c.get("/confirmed", up, "Range: bytes=0-0"); statusOf(rep.err) != http.StatusBadGateway || rep.Body.Len() != 0 {
		t.Errorf("a slice that is not one was delivered: %v, %q", rep.err, rep.body())
	}
}

// TestSliceStoredBeforeItIsRead covers the slice that is whole in the cache
// by the time the request it was fetched for gets to read it.
func TestSliceStoredBeforeItIsRead(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice})
	waitLoaded(t, c.s)
	up := sliced(slicedBody, nil)
	defer func() { openFile = os.Open }()

	// stored makes the request find the slice it wants to read as it
	// arrives stored already, and then does what it is told.
	stored := func(n int64, then func()) {
		openFile = func(name string) (*os.File, error) {
			openFile = os.Open
			waitFor(t, "the slice to be stored", func() bool { return c.s.Stats().Stored == n })
			then()

			return os.Open(name)
		}
	}

	// It is read from the cache then.
	stored(1, func() {})
	rep := c.get("/a", up, "Range: bytes=0-9")
	c.expect(rep, "/a", "fwd=uri-miss; stored")
	if rep.Code != http.StatusPartialContent || rep.body() != slicedBody[:10] {
		t.Errorf("status %d, body %q", rep.Code, rep.body())
	}

	// Unless it is no longer there, which leaves the request with nothing.
	stored(2, func() { c.s.Purge(keyOf("/b")) })
	rep = c.get("/b", up, "Range: bytes=0-9")
	if statusOf(rep.err) != http.StatusBadGateway || rep.Body.Len() != 0 {
		t.Errorf("a slice that was purged was delivered: %v, %q", rep.err, rep.body())
	}
	if n := up.calls.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

// TestSliceBeingReceivedThatCannotBeOpened covers the request that finds
// the slice it wants being fetched for another one, and fails to open the
// file it is received in: it waits for the slice to be stored.
func TestSliceBeingReceivedThatCannotBeOpened(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice})
	waitLoaded(t, c.s)
	release := make(chan struct{})
	up, halfway := stalling(slicedBody, 0, release)

	// The request the slice is fetched for opens the file, the next one to
	// try does not get to.
	var opens atomic.Int32
	opened, refused := make(chan struct{}), make(chan struct{})
	openFile = func(name string) (*os.File, error) {
		switch opens.Add(1) {
		case 1:
			defer close(opened)
		case 2:
			close(refused)
			return nil, errInjected
		}

		return os.Open(name)
	}
	defer func() { openFile = os.Open }()

	first := make(chan *reply)
	go func() { first <- c.get("/a", up, "Range: bytes=0-9") }()
	<-halfway
	<-opened

	looked := c.s.Stats().Misses
	second := make(chan *reply)
	go func() { second <- c.get("/a", up, "Range: bytes=0-9") }()
	<-refused
	// It looks in the cache again before it goes back to waiting.
	waitFor(t, "the second request to look in the cache again", func() bool { return c.s.Stats().Misses >= looked+2 })
	close(release)

	c.expect(<-first, "/a", "fwd=uri-miss; stored")
	rep := <-second
	c.expect(rep, "/a", "fwd=uri-miss; collapsed")
	if rep.body() != slicedBody[:10] || up.calls.Load() != 1 {
		t.Errorf("body %q after %d requests to the upstream", rep.body(), up.calls.Load())
	}
}

func TestSlicesOnlyIfCached(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice, Mode: "strict"})
	up := sliced(slicedBody, nil)

	// Nothing of the response is in the cache.
	rep := c.get("/a", up, "Cache-Control: only-if-cached")
	if statusOf(rep.err) != http.StatusGatewayTimeout || rep.status() != "Caddy; fwd=bypass; detail=ONLY-IF-CACHED; key="+keyOf("/a") {
		t.Errorf("returned %v, Cache-Status: %s", rep.err, rep.status())
	}

	// Its beginning is.
	c.expect(c.get("/a", up, "Range: bytes=0-0"), "/a", "fwd=uri-miss; stored")
	rep = c.get("/a", up, "Cache-Control: only-if-cached", "Range: bytes=10-19")
	c.expectHit(rep, "/a", 120)
	if rep.body() != slicedBody[10:20] {
		t.Errorf("body %q", rep.body())
	}
	if rep = c.get("/a", up, "Cache-Control: only-if-cached"); !rep.aborted || rep.body() != slicedBody[:testSlice] {
		t.Errorf("a response the cache has the beginning of was not cut short: %v, %q", rep.err, abbreviate(rep.body()))
	}
	if n := up.calls.Load(); n != 1 {
		t.Errorf("the upstream got %d requests, want 1", n)
	}
}

// stalling returns an upstream like sliced, whose first answer to a request
// for the range starting at the given byte stops after its header and the
// first half of its body until release is closed. It closes the returned
// channel when it gets there.
func stalling(content string, at int, release <-chan struct{}, headers ...string) (*backend, <-chan struct{}) {
	halfway := make(chan struct{})
	var once sync.Once

	return sliced(content, func(w http.ResponseWriter, r *http.Request, first, _ int) (bool, error) {
		stall := false
		if first == at {
			once.Do(func() { stall = true })
		}
		if !stall {
			return false, nil
		}

		last := min(first+testSlice, len(content)) - 1
		half := first + (last-first+1)/2
		sendHeader(w, http.StatusPartialContent, append(headers, contentRange(first, last, len(content)), fmt.Sprintf("Content-Length: %d", last-first+1))...)
		_, _ = io.WriteString(w, content[first:half])
		close(halfway)
		<-release
		_, _ = io.WriteString(w, content[half:last+1])

		return true, nil
	}, headers...), halfway
}

// TestHeadAndSlices covers the HEAD requests, which the cache does not
// fetch anything for.
func TestHeadAndSlices(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice})
	head := func(path string, up *backend) *reply {
		return c.do(newRequest(http.MethodHead, path), up)
	}

	// Nothing is in the cache, nor on its way there.
	up := sliced(slicedBody, nil)
	c.expect(head("/a", up), "/a", "fwd=uri-miss; detail=HEAD")
	if n := c.s.Stats().Entries; n != 0 {
		t.Errorf("%d entries after a HEAD request", n)
	}

	// The first slice is on its way.
	release := make(chan struct{})
	up, halfway := stalling(slicedBody, 0, release)
	first := make(chan *reply)
	go func() { first <- c.get("/a", up) }()
	<-halfway
	rep := head("/a", up)
	c.expect(rep, "/a", "fwd=uri-miss; collapsed")
	if rep.Code != http.StatusOK || rep.Header().Get("Content-Length") != fmt.Sprint(len(slicedBody)) || rep.Body.Len() != 0 {
		t.Errorf("status %d, body %q, headers %v", rep.Code, abbreviate(rep.body()), rep.Header())
	}
	close(release)
	if rep := <-first; rep.body() != slicedBody {
		t.Errorf("body %q", abbreviate(rep.body()))
	}

	// A response that fits in its first slice, and is stored whole, is on
	// its way.
	small := slicedBody[:1000]
	release = make(chan struct{})
	up, halfway = stalling(small, 0, release)
	go func() { first <- c.get("/small", up) }()
	<-halfway
	rep = head("/small", up)
	c.expect(rep, "/small", "fwd=uri-miss; collapsed")
	if rep.Code != http.StatusOK || rep.Header().Get("Content-Length") != "1000" || rep.Body.Len() != 0 {
		t.Errorf("status %d, body %q, headers %v", rep.Code, abbreviate(rep.body()), rep.Header())
	}
	close(release)
	if rep := <-first; rep.body() != small || up.calls.Load() != 1 {
		t.Errorf("body %q after %d requests to the upstream", abbreviate(rep.body()), up.calls.Load())
	}
}

// TestStaleSlicesWhileUpdating covers the requests that find a stale
// response another request is updating, and may be served it meanwhile.
func TestStaleSlicesWhileUpdating(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice, Stale: caddy.Duration(time.Hour)})

	// updating returns an upstream whose responses are stale at once, and
	// which takes its time to confirm the beginning of one, once.
	updating := func(content string, release <-chan struct{}) (*backend, <-chan struct{}) {
		asked := make(chan struct{})
		var once sync.Once

		return sliced(content, func(w http.ResponseWriter, r *http.Request, first, _ int) (bool, error) {
			if r.Header.Get("If-None-Match") == "" {
				return false, nil
			}
			if first == 0 {
				once.Do(func() {
					close(asked)
					<-release
				})
			}
			sendHeader(w, http.StatusNotModified)

			return true, nil
		}, "Cache-Control: max-age=0", `Etag: "v1"`), asked
	}

	for path, content := range map[string]string{"/sliced": slicedBody, "/whole": slicedBody[:1000]} {
		release := make(chan struct{})
		up, asked := updating(content, release)
		c.expect(c.get(path, up), path, "fwd=uri-miss; stored")

		first := make(chan *reply)
		go func() { first <- c.get(path, up) }()
		<-asked

		rep := c.get(path, up)
		c.expect(rep, path, "hit; ttl=-1; detail=UPDATING")
		if rep.body() != content {
			t.Errorf("%s: body %q", path, abbreviate(rep.body()))
		}

		close(release)
		rep = <-first
		c.expect(rep, path, "fwd=stale; fwd-status=304; detail=REVALIDATED")
		if rep.body() != content {
			t.Errorf("%s: body %q", path, abbreviate(rep.body()))
		}
	}
}

// TestWaitingForASlice covers the requests that wait for a fetch another
// request started, and what they make of what that fetch brings.
func TestWaitingForASlice(t *testing.T) {
	small := slicedBody[:1000]

	t.Run("the slice", func(t *testing.T) {
		c := newCacheTest(t, Options{Slice: testSlice})
		client := watchClient(testSlice / 2)
		up, halfway := stalling(slicedBody, 0, client.received)

		first := make(chan *reply)
		go func() { first <- c.get("/a", up) }()
		<-halfway

		// It is served as it arrives.
		out := c.serve(client, newRequest(http.MethodGet, "/a"), up)
		if out.err != nil || out.aborted || client.Body.String() != slicedBody || client.Header().Get("Cache-Status") != "Caddy; fwd=uri-miss; collapsed; key="+keyOf("/a") {
			t.Errorf("returned %v, body %q, headers %v", out.err, abbreviate(client.Body.String()), client.Header())
		}
		rep := <-first
		c.expect(rep, "/a", "fwd=uri-miss; stored")
		if rep.body() != slicedBody || up.calls.Load() != 3 {
			t.Errorf("body %q after %d requests to the upstream", abbreviate(rep.body()), up.calls.Load())
		}
	})

	t.Run("a whole response", func(t *testing.T) {
		c := newCacheTest(t, Options{Slice: testSlice})
		client := watchClient(len(small) / 2)
		up, halfway := stalling(small, 0, client.received)

		first := make(chan *reply)
		go func() { first <- c.get("/a", up) }()
		<-halfway

		// It is served as it arrives, like the slice would be.
		out := c.serve(client, newRequest(http.MethodGet, "/a"), up)
		if out.err != nil || out.aborted || client.Body.String() != small || client.Header().Get("Cache-Status") != "Caddy; fwd=uri-miss; collapsed; key="+keyOf("/a") {
			t.Errorf("returned %v, body %q, headers %v", out.err, abbreviate(client.Body.String()), client.Header())
		}
		c.expect(<-first, "/a", "fwd=uri-miss; stored")
		if n := up.calls.Load(); n != 1 {
			t.Errorf("the upstream got %d requests, want 1", n)
		}
	})

	t.Run("a whole response in the middle of another", func(t *testing.T) {
		c := newCacheTest(t, Options{Slice: testSlice})
		c.expect(c.get("/a", sliced(slicedBody, nil), "Range: bytes=0-0"), "/a", "fwd=uri-miss; stored")

		// The upstream stops sending ranges.
		release, entered := make(chan struct{}), make(chan struct{})
		up := upstreamFunc(func(w http.ResponseWriter, _ *http.Request, _ int) error {
			sendHeader(w, http.StatusOK, fmt.Sprintf("Content-Length: %d", len(slicedBody)))
			close(entered)
			<-release
			_, _ = io.WriteString(w, slicedBody)

			return nil
		})
		first := make(chan *reply)
		go func() { first <- c.get("/a", up, fmt.Sprintf("Range: bytes=%d-%d", testSlice, testSlice+9)) }()
		<-entered

		// A client that was sent the beginning of the response in slices has
		// no use for it.
		rep := c.get("/a", up)
		if !rep.aborted || rep.body() != slicedBody[:testSlice] {
			t.Errorf("a response that is no longer sliced was not cut short: %v, %q", rep.err, abbreviate(rep.body()))
		}

		close(release)
		rep = <-first
		c.expect(rep, "/a", "fwd=uri-miss; stored")
		if rep.Code != http.StatusPartialContent || rep.body() != slicedBody[testSlice:testSlice+10] || up.calls.Load() != 1 {
			t.Errorf("status %d, body %q after %d requests to the upstream", rep.Code, rep.body(), up.calls.Load())
		}
	})

	t.Run("for too long", func(t *testing.T) {
		c := newCacheTest(t, Options{Slice: testSlice, LockTimeout: caddy.Duration(20 * time.Millisecond)})
		c.expect(c.get("/b", sliced(slicedBody, nil), "Range: bytes=0-0"), "/b", "fwd=uri-miss; stored")

		// slow returns an upstream that takes its time to answer its first
		// request.
		slow := func(release <-chan struct{}) (*backend, <-chan struct{}) {
			entered := make(chan struct{})

			return sliced(slicedBody, func(_ http.ResponseWriter, _ *http.Request, _, call int) (bool, error) {
				if call == 1 {
					close(entered)
					<-release
				}

				return false, nil
			}), entered
		}

		// Before the client was sent anything, its request goes to the
		// upstream as it is.
		release := make(chan struct{})
		up, entered := slow(release)
		first := make(chan *reply)
		go func() { first <- c.get("/a", up) }()
		<-entered
		rep := c.get("/a", up)
		c.expect(rep, "/a", "fwd=uri-miss; detail=LOCK-TIMEOUT")
		if rep.body() != slicedBody {
			t.Errorf("body %q", abbreviate(rep.body()))
		}
		close(release)
		c.expect(<-first, "/a", "fwd=uri-miss; stored")

		// After, it has no use for anything but the slice.
		release = make(chan struct{})
		up, entered = slow(release)
		go func() { first <- c.get("/b", up, fmt.Sprintf("Range: bytes=%d-%d", testSlice, testSlice+9)) }()
		<-entered
		if rep = c.get("/b", up); !rep.aborted || rep.body() != slicedBody[:testSlice] {
			t.Errorf("a response whose second slice did not come was not cut short: %v, %q", rep.err, abbreviate(rep.body()))
		}
		close(release)
		c.expect(<-first, "/b", "fwd=uri-miss; stored")
	})

	t.Run("by a client that leaves", func(t *testing.T) {
		c := newCacheTest(t, Options{Slice: testSlice})
		release, entered := make(chan struct{}), make(chan struct{})
		up := sliced(slicedBody, func(_ http.ResponseWriter, _ *http.Request, _, call int) (bool, error) {
			if call == 1 {
				close(entered)
				<-release
			}

			return false, nil
		})
		first := make(chan *reply)
		go func() { first <- c.get("/a", up) }()
		<-entered

		// Once the request looked in the cache, it is waiting or about to.
		looked := c.s.Stats().Misses
		ctx, cancel := context.WithCancel(context.Background())
		second := make(chan *reply)
		go func() { second <- c.do(newRequest(http.MethodGet, "/a").WithContext(ctx), up) }()
		waitFor(t, "the second request to look in the cache", func() bool { return c.s.Stats().Misses > looked })
		cancel()
		if rep := <-second; !errors.Is(rep.err, context.Canceled) || rep.Body.Len() != 0 {
			t.Errorf("a request whose client left returned %v after %d bytes", rep.err, rep.Body.Len())
		}

		close(release)
		c.expect(<-first, "/a", "fwd=uri-miss; stored")
	})
}

// brokenBody is a body that cannot be read or moved in.
type brokenBody struct {
	seek, read error
}

func (b brokenBody) Seek(offset int64, _ int) (int64, error) { return offset, b.seek }
func (b brokenBody) Read([]byte) (int, error)                { return 0, b.read }

// TestSliceReader covers the reading of the slices where it depends on
// nothing but the slice being read.
func TestSliceReader(t *testing.T) {
	c := newCacheTest(t, Options{Slice: testSlice})
	if err := storePut(t, c.s, "slice", nil, nil, []byte("body")); err != nil {
		t.Fatal(err)
	}
	_, hit := c.s.Lookup("slice", nil)
	if hit == nil {
		t.Fatal("not found")
	}
	defer hit.Close()

	// reader returns the body of a response of three slices, with the
	// first one open.
	reader := func(body io.ReadSeeker) *sliceReader {
		sr := &sliceReader{
			x:     &exchange{h: c.h, c: c.h.cfg, store: c.s, key: "slice"},
			size:  testSlice,
			total: int64(len(slicedBody)),
			part:  &slicePart{length: testSlice, body: body, hit: hit},
		}
		sr.ctx, sr.cancel = context.WithCancel(context.Background())

		return sr
	}
	buf := make([]byte, 100)

	sr := reader(strings.NewReader(slicedBody[:testSlice]))
	for _, seek := range []struct {
		offset int64
		whence int
		want   int64
	}{{100, io.SeekStart, 100}, {50, io.SeekCurrent, 150}, {-1, io.SeekEnd, int64(len(slicedBody)) - 1}, {1, io.SeekCurrent, int64(len(slicedBody))}} {
		if got, err := sr.Seek(seek.offset, seek.whence); err != nil || got != seek.want {
			t.Errorf("Seek(%d, %d) = %d, %v; want %d", seek.offset, seek.whence, got, err, seek.want)
		}
	}
	if n, err := sr.Read(buf); n != 0 || err != io.EOF {
		t.Errorf("read %d bytes at the end of the body: %v", n, err)
	}
	if _, err := sr.Seek(-1, io.SeekStart); err == nil {
		t.Error("seeked before the beginning")
	}
	if _, err := sr.Seek(150, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	if n, err := sr.Read(buf); n != len(buf) || err != nil || string(buf) != slicedBody[150:250] {
		t.Errorf("read %d bytes at offset 150: %v", n, err)
	}
	if err := sr.shut(); err != nil {
		t.Errorf("the reading ended with %v", err)
	}
	if n, err := sr.Read(buf); n != 0 || !errors.Is(err, errSliceClosed) {
		t.Errorf("read %d bytes once the reading was over: %v", n, err)
	}

	// A body that cannot be moved in. What failed once is not tried again.
	sr = reader(brokenBody{seek: errInjected})
	if _, err := sr.Seek(10, io.SeekStart); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if n, err := sr.Read(buf); n != 0 || !errors.Is(err, errInjected) {
			t.Errorf("read %d bytes of a body that cannot be seeked: %v", n, err)
		}
	}
	if err := sr.shut(); !errors.Is(err, errInjected) {
		t.Errorf("the reading ended with %v", err)
	}

	// A slice shorter than it says.
	sr = reader(brokenBody{read: io.EOF})
	if n, err := sr.Read(buf); n != 0 || !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("read %d bytes of an empty slice: %v", n, err)
	}

	// A file that cannot be read back is not served anymore.
	sr = reader(brokenBody{read: fmt.Errorf("read: %w", syscall.EIO)})
	if n, err := sr.Read(buf); n != 0 || !errors.Is(err, syscall.EIO) {
		t.Errorf("read %d bytes of a file that cannot be read: %v", n, err)
	}
	if n := c.s.Stats().Entries; n != 0 {
		t.Errorf("%d entries after a file could not be read", n)
	}
}
