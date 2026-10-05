package httpcache

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2/caddytest"
)

const (
	testURL  = "http://localhost:9080"
	adminURL = "http://localhost:2999"
)

// startCaddy loads a configuration made of a global cache block with the
// given options, stored in dir, and of a site with the given directives.
func startCaddy(t *testing.T, dir, cacheOptions, site string) *caddytest.Tester {
	t.Helper()

	tester := caddytest.NewTester(t)
	tester.InitServer(fmt.Sprintf(`
	{
		admin localhost:2999
		http_port 9080
		https_port 9443
		cache {
			path %s
			%s
		}
	}
	localhost:9080 {
		%s
	}`, dir, cacheOptions, site), "caddyfile")

	return tester
}

// upstream is an origin server that counts the requests it gets.
type upstream struct {
	*httptest.Server
	hits atomic.Int64
}

func newUpstream(t *testing.T, handler http.HandlerFunc) *upstream {
	t.Helper()

	u := new(upstream)
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.hits.Add(1)
		handler(w, r)
	}))
	t.Cleanup(u.Close)

	return u
}

func (u *upstream) addr() string {
	return strings.TrimPrefix(u.URL, "http://")
}

// fetch performs a request, given as a path and "Name: value" headers.
func fetch(t *testing.T, tester *caddytest.Tester, method, path string, headers ...string) (*http.Response, string) {
	t.Helper()

	req, err := http.NewRequest(method, testURL+path, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range headers {
		name, value, _ := strings.Cut(h, ": ")
		req.Header.Set(name, value)
	}

	resp, err := tester.Client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("%s %s: reading the body: %v", method, path, err)
	}

	return resp, string(body)
}

func get(t *testing.T, tester *caddytest.Tester, path string, headers ...string) (*http.Response, string) {
	t.Helper()

	return fetch(t, tester, http.MethodGet, path, headers...)
}

func expectStatus(t *testing.T, resp *http.Response, want string) {
	t.Helper()

	if got := resp.Header.Get("Cache-Status"); got != want {
		t.Errorf("Cache-Status: %s\n         want: %s", got, want)
	}
}

var hitPattern = regexp.MustCompile(`^Caddy; hit; ttl=(\d+); detail=(DISK|MEMORY); key=(.+)$`)

// expectHit checks that the response is a fresh one from the cache and
// returns the tier it came from.
func expectHit(t *testing.T, resp *http.Response, key string, ttl int) string {
	t.Helper()

	m := hitPattern.FindStringSubmatch(resp.Header.Get("Cache-Status"))
	if m == nil || m[3] != key || (m[1] != fmt.Sprint(ttl) && m[1] != fmt.Sprint(ttl-1)) {
		t.Errorf("Cache-Status: %s\n         want: a hit for %s with ttl=%d", resp.Header.Get("Cache-Status"), key, ttl)
		return ""
	}
	if resp.Header.Get("Age") == "" {
		t.Error("a response served from the cache has no Age")
	}

	return m[2]
}

func expectBody(t *testing.T, got, want string) {
	t.Helper()

	if got != want {
		t.Errorf("body: %q, want %q", abbreviate(got), abbreviate(want))
	}
}

func abbreviate(s string) string {
	if len(s) > 80 {
		return fmt.Sprintf("%s… (%d bytes)", s[:80], len(s))
	}

	return s
}

func cacheStats(t *testing.T) StoreStats {
	t.Helper()

	resp, err := http.Get(adminURL + "/cache/stats")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	var stats struct {
		Caches []StoreStats `json:"caches"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&stats); err != nil || len(stats.Caches) != 1 {
		t.Fatalf("reading the stats: %v, %d caches", err, len(stats.Caches))
	}

	return stats.Caches[0]
}

func purge(t *testing.T, query string) int {
	t.Helper()

	resp, err := http.Post(adminURL+"/cache/purge?"+query, "", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		Purged int `json:"purged"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("purging %s: status %d, %v", query, resp.StatusCode, err)
	}

	return result.Purged
}

func TestMissThenHit(t *testing.T) {
	tester := startCaddy(t, t.TempDir(), "", `
		route /hello {
			cache
			respond "Hello, cache!"
		}`)
	const key = "GET-http-localhost:9080-/hello"

	resp, body := get(t, tester, "/hello")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)
	expectBody(t, body, "Hello, cache!")
	if resp.Header.Get("Age") != "" {
		t.Error("a response from the upstream has an Age")
	}

	resp, body = get(t, tester, "/hello")
	if tier := expectHit(t, resp, key, 120); tier != "DISK" {
		t.Errorf("first hit served from %s, want DISK", tier)
	}
	expectBody(t, body, "Hello, cache!")
	if resp.Header.Get("Content-Length") != "13" {
		t.Errorf("Content-Length: %q", resp.Header.Get("Content-Length"))
	}

	// A response that keeps being requested is served from memory.
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, body = get(t, tester, "/hello")
		expectBody(t, body, "Hello, cache!")
		if expectHit(t, resp, key, 120) == "MEMORY" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the response never moved to memory")
		}
		time.Sleep(10 * time.Millisecond)
	}

	time.Sleep(2 * time.Second)
	resp, _ = get(t, tester, "/hello")
	expectHit(t, resp, key, 118)
	if age := resp.Header.Get("Age"); age != "2" && age != "3" {
		t.Errorf("Age: %s, want 2", age)
	}
}

func TestProxiedResponseIsFetchedOnce(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("X-Upstream", "yes")
		_, _ = io.WriteString(w, "from upstream "+r.URL.Path)
	})
	tester := startCaddy(t, t.TempDir(), "ttl 1h", `
		cache
		reverse_proxy `+up.addr())

	var first http.Header
	for i := range 5 {
		resp, body := get(t, tester, "/a")
		expectBody(t, body, "from upstream /a")
		if resp.Header.Get("X-Upstream") != "yes" || resp.Header.Get("Content-Type") != "text/plain" {
			t.Errorf("request %d: upstream headers lost: %v", i, resp.Header)
		}

		// A hit must look like the response it was stored from.
		comparable := resp.Header.Clone()
		for _, name := range []string{"Cache-Status", "Age"} {
			comparable.Del(name)
		}
		if i == 0 {
			first = comparable
		} else if fmt.Sprint(comparable) != fmt.Sprint(first) {
			t.Errorf("request %d: headers %v differ from the first response %v", i, comparable, first)
		}
	}
	resp, body := get(t, tester, "/b")
	expectBody(t, body, "from upstream /b")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key=GET-http-localhost:9080-/b")

	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

func TestRevalidation(t *testing.T) {
	var conditional atomic.Int64
	version := atomic.Value{}
	version.Store("v1")

	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		v := version.Load().(string)
		etag := `"` + v + `"`
		w.Header().Set("Etag", etag)
		w.Header().Set("X-Served", fmt.Sprint(time.Now().UnixNano()))
		if r.Header.Get("If-None-Match") == etag {
			conditional.Add(1)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = io.WriteString(w, "body "+v)
	})
	tester := startCaddy(t, t.TempDir(), "ttl 1s", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/doc"

	resp, body := get(t, tester, "/doc")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)
	expectBody(t, body, "body v1")
	served := resp.Header.Get("X-Served")

	time.Sleep(1100 * time.Millisecond)

	// Expired: the upstream confirms the response without sending it again.
	resp, body = get(t, tester, "/doc")
	expectStatus(t, resp, "Caddy; fwd=stale; fwd-status=304; detail=REVALIDATED; key="+key)
	expectBody(t, body, "body v1")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d, want 200", resp.StatusCode)
	}
	if conditional.Load() != 1 {
		t.Errorf("the upstream got %d conditional requests, want 1", conditional.Load())
	}
	// The headers of the confirmation replace the stored ones.
	if resp.Header.Get("X-Served") == served {
		t.Error("the headers were not updated by the revalidation")
	}

	// And it is fresh again.
	resp, body = get(t, tester, "/doc")
	expectHit(t, resp, key, 1)
	expectBody(t, body, "body v1")
	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}

	// When the response changed, the new one replaces it.
	version.Store("v2")
	time.Sleep(1100 * time.Millisecond)
	resp, body = get(t, tester, "/doc")
	expectStatus(t, resp, "Caddy; fwd=stale; stored; key="+key)
	expectBody(t, body, "body v2")
	resp, body = get(t, tester, "/doc")
	expectHit(t, resp, key, 1)
	expectBody(t, body, "body v2")
}

// TestRevalidationCountsTheAge checks that a response confirmed by a cache
// upstream of this one is not taken for fresher than that cache said.
func TestRevalidationCountsTheAge(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Etag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.Header().Set("Cache-Control", "max-age=120")
			w.Header().Set("Age", "100")
			w.WriteHeader(http.StatusNotModified)
			return
		}
		w.Header().Set("Cache-Control", "max-age=1")
		_, _ = io.WriteString(w, "body")
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/doc"

	get(t, tester, "/doc")
	time.Sleep(1100 * time.Millisecond)

	resp, body := get(t, tester, "/doc")
	expectStatus(t, resp, "Caddy; fwd=stale; fwd-status=304; detail=REVALIDATED; key="+key)
	expectBody(t, body, "body")
	if resp.Header.Get("Age") != "100" {
		t.Errorf("Age: %q, want 100", resp.Header.Get("Age"))
	}

	// Of the 120 seconds the response is fresh for, 100 are already spent.
	resp, _ = get(t, tester, "/doc")
	expectHit(t, resp, key, 20)
}

// TestRevalidationKeepsTheEntityTag checks that the clients keep being
// answered on the entity tag they were given once the response is confirmed
// by the upstream, which knows it under another one when encode changed it.
func TestRevalidationKeepsTheEntityTag(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Etag", `"v1"`)
		w.Header().Set("Content-Type", "text/plain")
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = io.WriteString(w, strings.Repeat("compress me ", 200))
	})
	tester := startCaddy(t, t.TempDir(), "ttl 1s", `
		cache
		encode gzip
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/doc"

	resp, _ := get(t, tester, "/doc", "Accept-Encoding: gzip")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)
	etag := resp.Header.Get("Etag")
	if resp.Header.Get("Content-Encoding") != "gzip" || etag == `"v1"` {
		t.Fatalf("Content-Encoding %q, Etag %s: want a compressed response with a tag of its own", resp.Header.Get("Content-Encoding"), etag)
	}

	time.Sleep(1100 * time.Millisecond)
	resp, _ = get(t, tester, "/doc", "Accept-Encoding: gzip")
	expectStatus(t, resp, "Caddy; fwd=stale; fwd-status=304; detail=REVALIDATED; key="+key)
	if got := resp.Header.Get("Etag"); got != etag {
		t.Errorf("Etag %s once revalidated, want %s", got, etag)
	}

	resp, _ = get(t, tester, "/doc", "Accept-Encoding: gzip", "If-None-Match: "+etag)
	if resp.StatusCode != http.StatusNotModified {
		t.Errorf("status %d for a client that has the response, want 304", resp.StatusCode)
	}
	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

func TestRangeAndConditionalRequests(t *testing.T) {
	const content = "0123456789abcdefghijklmnopqrstuvwxyz"
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" || r.Header.Get("If-None-Match") != "" {
			t.Errorf("the cache forwarded the client's range or precondition: %v", r.Header)
		}
		w.Header().Set("Etag", `"abc"`)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, content)
	})
	tester := startCaddy(t, t.TempDir(), "ttl 1h", `
		cache
		reverse_proxy `+up.addr())

	// The first request is for a range: the whole response is stored and
	// the range served from it.
	resp, body := get(t, tester, "/file", "Range: bytes=10-15")
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 10-15/36" {
		t.Errorf("status %d, Content-Range %q", resp.StatusCode, resp.Header.Get("Content-Range"))
	}
	expectBody(t, body, "abcdef")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key=GET-http-localhost:9080-/file")

	resp, body = get(t, tester, "/file")
	expectHit(t, resp, "GET-http-localhost:9080-/file", 3600)
	expectBody(t, body, content)

	resp, body = get(t, tester, "/file", "Range: bytes=-4")
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("suffix range: status %d", resp.StatusCode)
	}
	expectBody(t, body, "wxyz")

	resp, _ = get(t, tester, "/file", "Range: bytes=100-200")
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable {
		t.Errorf("unsatisfiable range: status %d", resp.StatusCode)
	}

	resp, body = get(t, tester, "/file", `If-None-Match: "abc"`)
	if resp.StatusCode != http.StatusNotModified || body != "" {
		t.Errorf("matching If-None-Match: status %d, body %q", resp.StatusCode, body)
	}
	resp, body = get(t, tester, "/file", `If-None-Match: "other"`)
	if resp.StatusCode != http.StatusOK {
		t.Errorf("non-matching If-None-Match: status %d", resp.StatusCode)
	}
	expectBody(t, body, content)

	// A conditional request for a response not in the cache yet.
	resp, _ = get(t, tester, "/other", `If-None-Match: "abc"`)
	if resp.StatusCode != http.StatusNotModified {
		t.Errorf("conditional miss: status %d", resp.StatusCode)
	}

	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

func TestUncacheableRangeRequestKeepsItsRange(t *testing.T) {
	const content = "0123456789abcdefghijklmnopqrstuvwxyz"
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		http.ServeContent(w, r, "file.txt", time.Time{}, strings.NewReader(content))
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	// What the cache cannot store, the upstream answers itself, range included.
	for i, detail := range []string{"NO-STORE", "UNCACHEABLE", "UNCACHEABLE"} {
		resp, body := get(t, tester, "/file", "Range: bytes=10-15")
		if resp.StatusCode != http.StatusPartialContent {
			t.Errorf("status %d, want 206", resp.StatusCode)
		}
		expectBody(t, body, "abcdef")
		expectStatus(t, resp, "Caddy; fwd=uri-miss; detail="+detail+"; key=GET-http-localhost:9080-/file")

		// Finding out takes one more request to the upstream, once: from
		// then on the client's request is all it gets.
		if want := int64(i + 2); up.hits.Load() != want {
			t.Errorf("the upstream got %d requests after %d from the client, want %d", up.hits.Load(), i+1, want)
		}
	}

	// The same goes for a conditional request.
	before := up.hits.Load()
	get(t, tester, "/file", `If-None-Match: "x"`)
	if n := up.hits.Load() - before; n != 1 {
		t.Errorf("the upstream got %d requests for one conditional request", n)
	}
}

func TestVary(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Vary", "X-Lang")
		_, _ = io.WriteString(w, "lang="+r.Header.Get("X-Lang"))
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	for _, lang := range []string{"fr", "en", "fr", "en", ""} {
		_, body := get(t, tester, "/page", "X-Lang: "+lang)
		expectBody(t, body, "lang="+lang)
	}
	if n := up.hits.Load(); n != 3 {
		t.Errorf("the upstream got %d requests, want 3", n)
	}
}

// TestVaryIsSelectedByTheRequestAsReceived checks that a response is stored
// as the variant the next request for it will look up: the one the headers
// the client sent select, whatever the handlers after the cache made of them.
func TestVaryIsSelectedByTheRequestAsReceived(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Vary", "X-Lang")
		_, _ = io.WriteString(w, "lang="+r.Header.Get("X-Lang"))
	})
	tester := startCaddy(t, t.TempDir(), "", `
		route {
			cache
			request_header X-Lang any
			reverse_proxy `+up.addr()+`
		}`)

	for _, lang := range []string{"fr", "fr", "en", "en", "fr"} {
		_, body := get(t, tester, "/page", "X-Lang: "+lang)
		expectBody(t, body, "lang=any")
	}
	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

func TestVaryIgnored(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Vary", "Origin")
		_, _ = io.WriteString(w, "image")
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache {
			key {
				disable_vary
			}
		}
		reverse_proxy `+up.addr())

	for _, origin := range []string{"https://a.example", "https://b.example", "https://c.example"} {
		get(t, tester, "/image", "Origin: "+origin)
	}
	if n := up.hits.Load(); n != 1 {
		t.Errorf("the upstream got %d requests, want 1", n)
	}
}

func TestCompressedVariants(t *testing.T) {
	text := strings.Repeat("Hello, gzip. ", 200)
	tester := startCaddy(t, t.TempDir(), "", `
		route /gzip {
			cache
			encode gzip
			header Content-Type text/plain
			respond "`+text+`"
		}`)

	// The test client does not ask for compression by itself.
	resp, body := get(t, tester, "/gzip", "Accept-Encoding: gzip")
	if resp.Header.Get("Content-Encoding") != "gzip" {
		t.Fatalf("not compressed: %v", resp.Header)
	}
	compressed := body

	// The same capabilities spelled differently select the same response.
	resp, body = get(t, tester, "/gzip", "Accept-Encoding: GZIP;q=1.0, identity")
	if !strings.Contains(resp.Header.Get("Cache-Status"), "; hit; ") || body != compressed {
		t.Errorf("Cache-Status: %s", resp.Header.Get("Cache-Status"))
	}

	// A client that does not accept gzip gets its own.
	resp, body = get(t, tester, "/gzip")
	if resp.Header.Get("Content-Encoding") != "" {
		t.Errorf("compressed response sent to a client that did not ask for it")
	}
	expectBody(t, body, text)
}

func TestUncacheableResponses(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/cookie":
			w.Header().Set("Set-Cookie", "session=1")
		case "/private":
			w.Header().Set("Cache-Control", "private, max-age=60")
		case "/no-store":
			w.Header().Set("Cache-Control", "no-store")
		case "/error":
			w.WriteHeader(http.StatusInternalServerError)
		case "/not-found":
			w.WriteHeader(http.StatusNotFound)
		}
		_, _ = io.WriteString(w, "body of "+r.URL.Path)
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	cases := map[string]string{
		"/cookie":    "SET-COOKIE",
		"/private":   "PRIVATE",
		"/no-store":  "NO-STORE",
		"/error":     "UNCACHEABLE-STATUS",
		"/not-found": "UNCACHEABLE-STATUS",
	}
	for path, reason := range cases {
		for range 2 {
			resp, body := get(t, tester, path)
			expectStatus(t, resp, "Caddy; fwd=uri-miss; detail="+reason+"; key=GET-http-localhost:9080-"+path)
			expectBody(t, body, "body of "+path)
		}
	}
	if n := up.hits.Load(); n != int64(2*len(cases)) {
		t.Errorf("the upstream got %d requests, want %d", n, 2*len(cases))
	}
	if n := cacheStats(t).Entries; n != 0 {
		t.Errorf("%d responses stored", n)
	}
}

func TestConcurrentRequestsShareOneFetch(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond)
		_, _ = io.WriteString(w, "slow response")
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	const clients = 30
	statuses := make([]string, clients)
	var wg sync.WaitGroup
	for i := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, body := get(t, tester, "/slow")
			expectBody(t, body, "slow response")
			statuses[i] = resp.Header.Get("Cache-Status")
		}()
	}
	wg.Wait()

	if n := up.hits.Load(); n != 1 {
		t.Errorf("the upstream got %d requests, want 1", n)
	}
	leaders, followers := 0, 0
	for _, status := range statuses {
		switch {
		case strings.Contains(status, "fwd=uri-miss; stored"):
			leaders++
		case strings.Contains(status, "fwd=uri-miss; collapsed"), strings.Contains(status, "; hit; "):
			followers++
		default:
			t.Errorf("unexpected Cache-Status: %s", status)
		}
	}
	if leaders != 1 || followers != clients-1 {
		t.Errorf("%d requests fetched and %d waited, want 1 and %d", leaders, followers, clients-1)
	}
}

// TestRequestsJoinADownloadInProgress checks that the requests arriving
// while a response is being downloaded are served from it as it arrives,
// rather than once it is complete.
func TestRequestsJoinADownloadInProgress(t *testing.T) {
	first := string(bodyFor("first", 100_000))
	second := string(bodyFor("second", 100_000))
	release := make(chan struct{})
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(first)+len(second)))
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = io.WriteString(w, first)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
		_, _ = io.WriteString(w, second)
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/video"

	leader := make(chan string)
	go func() {
		_, body := get(t, tester, "/video")
		leader <- body
	}()
	waitFor(t, "the download to start", func() bool { return up.hits.Load() == 1 })

	// A request for the whole response gets its beginning at once.
	resp, err := tester.Client.Get(testURL + "/video")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()
	expectStatus(t, resp, "Caddy; fwd=uri-miss; collapsed; key="+key)
	if resp.Header.Get("Content-Length") != "200000" {
		t.Errorf("Content-Length: %q", resp.Header.Get("Content-Length"))
	}
	head := make([]byte, len(first))
	if _, err := io.ReadFull(resp.Body, head); err != nil || string(head) != first {
		t.Fatalf("reading the part already downloaded: %v", err)
	}

	// A request for a range that is already there is answered in full,
	// while the download is still going on.
	ranged, body := get(t, tester, "/video", "Range: bytes=1000-1999")
	if ranged.StatusCode != http.StatusPartialContent || ranged.Header.Get("Content-Range") != "bytes 1000-1999/200000" {
		t.Errorf("status %d, Content-Range %q", ranged.StatusCode, ranged.Header.Get("Content-Range"))
	}
	expectBody(t, body, first[1000:2000])

	// So is a HEAD request.
	headResp, _ := fetch(t, tester, http.MethodHead, "/video")
	if headResp.Header.Get("Content-Length") != "200000" || headResp.Header.Get("Content-Type") != "application/octet-stream" {
		t.Errorf("HEAD during the download: %v", headResp.Header)
	}

	// A range that is still to come waits for it.
	late := make(chan string)
	go func() {
		_, body := get(t, tester, "/video", "Range: bytes=150000-")
		late <- body
	}()
	select {
	case <-late:
		t.Fatal("got a range that was not downloaded yet")
	case <-time.After(200 * time.Millisecond):
	}

	close(release)

	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(rest) != second {
		t.Errorf("reading the rest: %d bytes, %v", len(rest), err)
	}
	expectBody(t, <-late, second[50_000:])
	expectBody(t, <-leader, first+second)

	if n := up.hits.Load(); n != 1 {
		t.Errorf("the upstream got %d requests, want 1", n)
	}
	hit, body := get(t, tester, "/video")
	expectHit(t, hit, key, 120)
	expectBody(t, body, first+second)
}

// TestRangeRequestIsStreamedOnAMiss checks that the request that triggers a
// download gets the range it asked for as it arrives, while the whole
// response is stored.
func TestRangeRequestIsStreamedOnAMiss(t *testing.T) {
	first := string(bodyFor("first", 100_000))
	second := string(bodyFor("second", 100_000))
	release := make(chan struct{})
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			t.Errorf("the range was forwarded: %s", r.Header.Get("Range"))
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(first)+len(second)))
		_, _ = io.WriteString(w, first)
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
		_, _ = io.WriteString(w, second)
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/video"

	req, _ := http.NewRequest(http.MethodGet, testURL+"/video", nil)
	req.Header.Set("Range", "bytes=20000-")
	resp, err := tester.Client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 20000-199999/200000" || resp.Header.Get("Content-Length") != "180000" {
		t.Errorf("status %d, headers %v", resp.StatusCode, resp.Header)
	}
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)

	// Most of what the upstream sent so far is already here: the server
	// may hold back a last few kilobytes until more comes.
	head := make([]byte, 64_000)
	if _, err := io.ReadFull(resp.Body, head); err != nil || string(head) != first[20_000:84_000] {
		t.Fatalf("reading the range while it is downloaded: %v", err)
	}

	close(release)
	rest, err := io.ReadAll(resp.Body)
	if err != nil || string(head)+string(rest) != (first + second)[20_000:] {
		t.Errorf("reading the rest: %d bytes, %v", len(rest), err)
	}

	// The whole response was stored.
	hit, body := get(t, tester, "/video")
	expectHit(t, hit, key, 120)
	expectBody(t, body, first+second)

	// A range that stops before the end is relayed too.
	ranged, body := get(t, tester, "/other", "Range: bytes=100-199")
	if ranged.StatusCode != http.StatusPartialContent {
		t.Errorf("status %d", ranged.StatusCode)
	}
	expectBody(t, body, first[100:200])
	hit, body = get(t, tester, "/other")
	expectHit(t, hit, "GET-http-localhost:9080-/other", 120)
	expectBody(t, body, first+second)
}

// TestFailedDownloadFailsThoseWhoJoinedIt checks that a download cut short
// is not mistaken for a complete response by anyone.
func TestFailedDownloadFailsThoseWhoJoinedIt(t *testing.T) {
	release := make(chan struct{})
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "200000")
		_, _ = w.Write(bodyFor("first", 100_000))
		w.(http.Flusher).Flush()
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
		panic(http.ErrAbortHandler)
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	download := func() error {
		resp, err := tester.Client.Get(testURL + "/video")
		if err != nil {
			return err
		}
		defer func() { _ = resp.Body.Close() }()
		_, err = io.ReadAll(resp.Body)
		return err
	}

	results := make(chan error, 2)
	go func() { results <- download() }()
	waitFor(t, "the download to start", func() bool { return up.hits.Load() == 1 })
	go func() { results <- download() }()
	time.Sleep(200 * time.Millisecond)
	close(release)

	for range 2 {
		if err := <-results; err == nil {
			t.Error("a truncated response was delivered as complete")
		}
	}
	st := cacheStats(t)
	if st.Entries != 0 {
		t.Error("a truncated response was stored")
	}
	if left, _ := os.ReadDir(filepath.Join(st.Path, tmpDirName)); len(left) != 0 {
		t.Errorf("%d temporary files left", len(left))
	}
}

// TestStatusOfAStoredResponse checks that a response stored with another
// status than 200 reaches the client that triggered its download with it.
func TestStatusOfAStoredResponse(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "max-age=60")
		switch r.URL.Path {
		case "/moved":
			w.Header().Set("Location", "/new")
			w.WriteHeader(http.StatusMovedPermanently)
		case "/gone":
			w.WriteHeader(http.StatusGone)
			_, _ = io.WriteString(w, "gone")
		}
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())
	tester.Client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	for path, status := range map[string]int{"/moved": http.StatusMovedPermanently, "/gone": http.StatusGone} {
		for i, want := range []string{"fwd=uri-miss; stored", "hit; ttl="} {
			resp, _ := get(t, tester, path)
			if resp.StatusCode != status {
				t.Errorf("%s, request %d: status %d, want %d", path, i, resp.StatusCode, status)
			}
			if !strings.Contains(resp.Header.Get("Cache-Status"), want) {
				t.Errorf("%s, request %d: Cache-Status %s", path, i, resp.Header.Get("Cache-Status"))
			}
		}
	}
}

// TestNothingIsStoredForAnUpstreamThatNeverAnswered covers a client giving
// up on an upstream that hangs: the fetch is then given up too, and must not
// leave an empty response in the cache.
func TestNothingIsStoredForAnUpstreamThatNeverAnswered(t *testing.T) {
	grace := clientGoneGrace
	clientGoneGrace = 200 * time.Millisecond
	defer func() { clientGoneGrace = grace }()

	var hang atomic.Bool
	hang.Store(true)
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if hang.Load() {
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, "finally")
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, testURL+"/slow", nil)
	if resp, err := tester.Client.Do(req); err == nil {
		_ = resp.Body.Close()
		t.Fatal("got a response from an upstream that does not answer")
	}

	// Leave the abandoned fetch the time to be given up.
	time.Sleep(600 * time.Millisecond)
	if n := cacheStats(t).Entries; n != 0 {
		t.Fatalf("%d responses stored for an upstream that never answered", n)
	}

	hang.Store(false)
	resp, body := get(t, tester, "/slow")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key=GET-http-localhost:9080-/slow")
	expectBody(t, body, "finally")
}

// TestSlowClientDoesNotHoldUpTheOthers checks that the client whose request
// started a download does not set its pace: one that stops reading delays
// neither the download nor the requests served from it.
func TestSlowClientDoesNotHoldUpTheOthers(t *testing.T) {
	const size = 16 << 20
	content := bodyFor("large", size)
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(size))
		_, _ = w.Write(content)
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	// A client that asks and then reads nothing.
	conn, err := net.Dial("tcp", "localhost:9080")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err := io.WriteString(conn, "GET /large HTTP/1.1\r\nHost: localhost:9080\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the download to start", func() bool { return up.hits.Load() == 1 })

	done := make(chan string, 2)
	for _, headers := range [][]string{nil, {"Range: bytes=-1000"}} {
		go func() {
			_, body := get(t, tester, "/large", headers...)
			done <- body
		}()
	}
	for range 2 {
		select {
		case body := <-done:
			if body != string(content) && body != string(content[size-1000:]) {
				t.Errorf("got %d bytes that are not what was asked for", len(body))
			}
		case <-time.After(4 * time.Second):
			t.Fatal("a request is held up by a client that does not read")
		}
	}

	// The download itself completed.
	resp, _ := get(t, tester, "/large", "Range: bytes=0-9")
	if !strings.Contains(resp.Header.Get("Cache-Status"), "; hit; ") {
		t.Errorf("Cache-Status: %s", resp.Header.Get("Cache-Status"))
	}
	if n := up.hits.Load(); n != 1 {
		t.Errorf("the upstream got %d requests, want 1", n)
	}
}

// TestResponseTooLargeToStoreIsStillDelivered covers a response that turns
// out too large while it is downloaded: every client gets all of it.
func TestResponseTooLargeToStoreIsStillDelivered(t *testing.T) {
	content := string(bodyFor("big", 400_000))
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		// No length announced.
		for off := 0; off < len(content); off += 50_000 {
			_, _ = io.WriteString(w, content[off:off+50_000])
			w.(http.Flusher).Flush()
			time.Sleep(10 * time.Millisecond)
		}
	})
	tester := startCaddy(t, t.TempDir(), "max_cacheable_body_bytes 100k", `
		cache
		reverse_proxy `+up.addr())

	bodies := make(chan string, 3)
	for range 3 {
		go func() {
			resp, err := tester.Client.Get(testURL + "/big")
			if err != nil {
				bodies <- err.Error()
				return
			}
			defer func() { _ = resp.Body.Close() }()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				bodies <- err.Error()
				return
			}
			bodies <- string(body)
		}()
		time.Sleep(20 * time.Millisecond)
	}
	for range 3 {
		expectBody(t, <-bodies, content)
	}

	st := cacheStats(t)
	if st.Entries != 0 {
		t.Error("a response over the size limit was stored")
	}
	if left, _ := os.ReadDir(filepath.Join(st.Path, tmpDirName)); len(left) != 0 {
		t.Errorf("%d temporary files left", len(left))
	}
}

// TestRangeIsDeliveredWhenTheResponseCannotBeStored checks that the request
// which triggered a download gets the range it asked for when the response
// can no longer be stored midway, like it would get a whole response.
func TestRangeIsDeliveredWhenTheResponseCannotBeStored(t *testing.T) {
	const size = 32 << 10
	content := string(bodyFor("range", size))
	release := make(chan struct{})
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(size))
		if r.URL.Path == "/hog" {
			_, _ = io.WriteString(w, content[:size-1])
			w.(http.Flusher).Flush()
			select {
			case <-release:
			case <-time.After(10 * time.Second):
			}
			_, _ = io.WriteString(w, content[size-1:])
			return
		}
		_, _ = io.WriteString(w, content[:size/2])
		w.(http.Flusher).Flush()
		time.Sleep(20 * time.Millisecond)
		_, _ = io.WriteString(w, content[size/2:])
	})
	// Each response takes half of the cache, before its headers are counted:
	// a second one cannot be stored while the first is still downloading.
	tester := startCaddy(t, t.TempDir(), "max_size 64Ki", `
		cache
		reverse_proxy `+up.addr())

	hog, err := tester.Client.Get(testURL + "/hog")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = hog.Body.Close() }()
	head := make([]byte, size-1)
	if _, err := io.ReadFull(hog.Body, head); err != nil {
		t.Fatal(err)
	}

	for path, r := range map[string][2]int{
		// Sent before, across and after the point where storing fails.
		"/before": {100, 299},
		"/across": {1000, size - 100},
		"/after":  {size - 68, size - 1},
	} {
		resp, body := get(t, tester, path, fmt.Sprintf("Range: bytes=%d-%d", r[0], r[1]))
		if want := fmt.Sprintf("bytes %d-%d/%d", r[0], r[1], size); resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != want {
			t.Errorf("%s: status %d, Content-Range %q, want 206 and %q", path, resp.StatusCode, resp.Header.Get("Content-Range"), want)
		}
		expectBody(t, body, content[r[0]:r[1]+1])
	}
	if st := cacheStats(t); st.Entries != 0 {
		t.Errorf("%d responses were stored, where none could be", st.Entries)
	}

	close(release)
	rest, err := io.ReadAll(hog.Body)
	if err != nil || string(head)+string(rest) != content {
		t.Errorf("the download in progress was disturbed: %d more bytes, %v", len(rest), err)
	}
	resp, body := get(t, tester, "/hog")
	expectHit(t, resp, "GET-http-localhost:9080-/hog", 120)
	expectBody(t, body, content)
}

// TestStaleIsNotServedForAResponseTheCacheGaveUp checks that the stale
// response only stands in for an upstream that failed.
func TestStaleIsNotServedForAResponseTheCacheGaveUp(t *testing.T) {
	var private atomic.Bool
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if private.Load() {
			w.Header().Set("Cache-Control", "private")
			http.ServeContent(w, r, "", time.Time{}, strings.NewReader("NEW PRIVATE CONTENT"))
			return
		}
		_, _ = io.WriteString(w, "OLD PUBLIC CONTENT")
	})
	tester := startCaddy(t, t.TempDir(), `
			ttl 1s
			stale 1m`, `
		cache
		reverse_proxy `+up.addr())

	get(t, tester, "/page")
	private.Store(true)
	time.Sleep(1100 * time.Millisecond)

	resp, body := get(t, tester, "/page", "Range: bytes=0-10")
	expectBody(t, body, "NEW PRIVATE")
	if strings.Contains(resp.Header.Get("Cache-Status"), "STALE") {
		t.Errorf("Cache-Status: %s", resp.Header.Get("Cache-Status"))
	}
	_, body = get(t, tester, "/page")
	expectBody(t, body, "NEW PRIVATE CONTENT")
}

// TestWhatIsNotAPlainResponse covers responses that are relayed without
// being stored because storing would lose part of them.
func TestWhatIsNotAPlainResponse(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/trailers":
			w.Header().Set("Trailer", "X-Checksum")
			_, _ = io.WriteString(w, "body")
			w.Header().Set("X-Checksum", "abc123")
		case "/events":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = io.WriteString(w, "data: hello\n\n")
		}
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	for range 2 {
		resp, body := get(t, tester, "/trailers")
		expectStatus(t, resp, "Caddy; fwd=uri-miss; detail=TRAILER; key=GET-http-localhost:9080-/trailers")
		expectBody(t, body, "body")
		if got := resp.Trailer.Get("X-Checksum"); got != "abc123" {
			t.Errorf("trailer %q, want abc123", got)
		}

		resp, body = get(t, tester, "/events")
		expectStatus(t, resp, "Caddy; fwd=uri-miss; detail=EVENT-STREAM; key=GET-http-localhost:9080-/events")
		expectBody(t, body, "data: hello\n\n")
	}
}

func TestUncacheableResponsesAreNotSerialized(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Header().Set("Cache-Control", "private")
		_, _ = io.WriteString(w, "personal")
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	// The first request finds out the response is private.
	get(t, tester, "/me")

	start := time.Now()
	var wg sync.WaitGroup
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, body := get(t, tester, "/me")
			expectBody(t, body, "personal")
		}()
	}
	wg.Wait()

	// One after the other they would take two seconds.
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("10 requests for a private response took %v", elapsed)
	}
}

func TestStaleIfError(t *testing.T) {
	var failing atomic.Bool
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = io.WriteString(w, "down")
			return
		}
		_, _ = io.WriteString(w, "healthy")
	})
	tester := startCaddy(t, t.TempDir(), `
			ttl 1s
			stale 4s`, `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/svc"

	_, body := get(t, tester, "/svc")
	expectBody(t, body, "healthy")

	failing.Store(true)
	time.Sleep(1100 * time.Millisecond)

	// The upstream answers with an error: the stale response is served.
	resp, body := get(t, tester, "/svc")
	expectStatus(t, resp, "Caddy; fwd=stale; fwd-status=503; detail=STALE; key="+key)
	expectBody(t, body, "healthy")
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d, want 200", resp.StatusCode)
	}

	// The upstream does not answer at all.
	up.CloseClientConnections()
	up.Close()
	resp, body = get(t, tester, "/svc")
	expectStatus(t, resp, "Caddy; fwd=stale; detail=STALE; key="+key)
	expectBody(t, body, "healthy")

	// Past the stale period the error gets through.
	time.Sleep(4 * time.Second)
	resp, _ = get(t, tester, "/svc")
	if resp.StatusCode != http.StatusBadGateway {
		t.Errorf("status %d once the stale period is over, want 502", resp.StatusCode)
	}
}

func TestStaleWhileUpdating(t *testing.T) {
	var slow atomic.Bool
	var version atomic.Int64
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if slow.Load() {
			time.Sleep(600 * time.Millisecond)
		}
		_, _ = fmt.Fprintf(w, "version %d", version.Add(1))
	})
	tester := startCaddy(t, t.TempDir(), `
			ttl 1s
			stale 10s`, `
		cache
		reverse_proxy `+up.addr())

	_, body := get(t, tester, "/page")
	expectBody(t, body, "version 1")

	slow.Store(true)
	time.Sleep(1100 * time.Millisecond)

	// The first request after the expiry updates the response…
	updated := make(chan string)
	go func() {
		_, body := get(t, tester, "/page")
		updated <- body
	}()
	time.Sleep(150 * time.Millisecond)

	// …and the others are served the stale one meanwhile, without waiting.
	start := time.Now()
	resp, body := get(t, tester, "/page")
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Errorf("the stale response took %v", elapsed)
	}
	expectBody(t, body, "version 1")
	if status := resp.Header.Get("Cache-Status"); !strings.HasPrefix(status, "Caddy; hit; ttl=-") || !strings.Contains(status, "detail=UPDATING") {
		t.Errorf("Cache-Status: %s", status)
	}

	expectBody(t, <-updated, "version 2")
	_, body = get(t, tester, "/page")
	expectBody(t, body, "version 2")
	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

func TestMustRevalidateIsNeverServedStale(t *testing.T) {
	var failing atomic.Bool
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if failing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Cache-Control", "max-age=1, must-revalidate")
		_, _ = io.WriteString(w, "strict")
	})
	tester := startCaddy(t, t.TempDir(), "stale 1h", `
		cache
		reverse_proxy `+up.addr())

	get(t, tester, "/strict")
	resp, _ := get(t, tester, "/strict")
	expectHit(t, resp, "GET-http-localhost:9080-/strict", 1)

	failing.Store(true)
	time.Sleep(1100 * time.Millisecond)
	resp, _ = get(t, tester, "/strict")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status %d, want the 503 of the upstream", resp.StatusCode)
	}
}

// TestClientDisconnect checks that a response keeps being stored when the
// client that triggered its fetch goes away.
func TestClientDisconnect(t *testing.T) {
	endless := make(chan struct{})
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/endless" {
			// A response with no announced end, that goes on for as long
			// as it is listened to.
			for r.Context().Err() == nil {
				_, _ = io.WriteString(w, "0123456789")
				w.(http.Flusher).Flush()
				time.Sleep(10 * time.Millisecond)
			}
			close(endless)
			return
		}

		w.Header().Set("Content-Length", "20")
		_, _ = io.WriteString(w, "0123456789")
		w.(http.Flusher).Flush()
		time.Sleep(400 * time.Millisecond)
		_, _ = io.WriteString(w, "abcdefghij")
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	// leave requests a URL and goes away once it got the first bytes.
	leave := func(path string) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, testURL+path, nil)
		resp, err := tester.Client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()

		head := make([]byte, 10)
		if _, err := io.ReadFull(resp.Body, head); err != nil || string(head) != "0123456789" {
			t.Fatalf("read %q, %v", head, err)
		}
	}

	leave("/download")
	time.Sleep(800 * time.Millisecond)

	resp, body := get(t, tester, "/download")
	expectHit(t, resp, "GET-http-localhost:9080-/download", 120)
	expectBody(t, body, "0123456789abcdefghij")
	if n := up.hits.Load(); n != 1 {
		t.Errorf("the upstream got %d requests, want 1", n)
	}

	// A response that does not say where it ends is not followed once
	// nobody is listening: it could go on forever.
	leave("/endless")
	select {
	case <-endless:
	case <-time.After(5 * time.Second):
		t.Error("an endless response is still being downloaded after its only client left")
	}
	if st := cacheStats(t); st.Entries != 1 {
		t.Errorf("%d entries, want only the complete download", st.Entries)
	}
}

func TestTruncatedUpstreamResponseIsNotStored(t *testing.T) {
	var broken atomic.Bool
	broken.Store(true)
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if !broken.Load() {
			_, _ = io.WriteString(w, strings.Repeat("x", 1000))
			return
		}
		w.Header().Set("Content-Length", "1000")
		_, _ = io.WriteString(w, strings.Repeat("x", 500))
		w.(http.Flusher).Flush()
		panic(http.ErrAbortHandler)
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	req, _ := http.NewRequest(http.MethodGet, testURL+"/file", nil)
	resp, err := tester.Client.Do(req)
	if err == nil {
		_, err = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Error("the client was not told the response is incomplete")
	}
	if n := cacheStats(t).Entries; n != 0 {
		t.Fatalf("a truncated response was stored")
	}

	broken.Store(false)
	resp, body := get(t, tester, "/file")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key=GET-http-localhost:9080-/file")
	expectBody(t, body, strings.Repeat("x", 1000))

	dir := cacheStats(t).Path
	if left, _ := os.ReadDir(filepath.Join(dir, tmpDirName)); len(left) != 0 {
		t.Errorf("%d temporary files left", len(left))
	}
}

// TestLargeResponseIsStreamed is the reason this cache exists: a response
// much larger than the memory budget goes to the client and to the disk as
// it arrives, without being held in memory, on a miss as on a hit.
func TestLargeResponseIsStreamed(t *testing.T) {
	const (
		chunk  = 1 << 20
		chunks = 96
	)
	block := bodyFor("large", chunk)
	want := sha256.New()
	for range chunks {
		want.Write(block)
	}

	firstChunkRead := make(chan struct{})
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(block)
		w.(http.Flusher).Flush()
		// The client must get the beginning while the rest is still to come.
		select {
		case <-firstChunkRead:
		case <-time.After(5 * time.Second):
			t.Error("the response was not streamed: the client got nothing before its end")
		}
		for range chunks - 1 {
			_, _ = w.Write(block)
		}
	})
	tester := startCaddy(t, t.TempDir(), `
			max_size 512Mi
			max_memory 1Mi`, `
		cache
		reverse_proxy `+up.addr())
	tester.Client.Timeout = time.Minute

	download := func(signal chan struct{}) (http.Header, uint64) {
		runtime.GC()
		var before runtime.MemStats
		runtime.ReadMemStats(&before)

		var peak atomic.Uint64
		stop := make(chan struct{})
		sampled := make(chan struct{})
		go func() {
			defer close(sampled)
			var m runtime.MemStats
			for {
				select {
				case <-stop:
					return
				case <-time.After(5 * time.Millisecond):
					runtime.ReadMemStats(&m)
					if m.HeapAlloc > peak.Load() {
						peak.Store(m.HeapAlloc)
					}
				}
			}
		}()

		resp, err := tester.Client.Get(testURL + "/large")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()

		got := sha256.New()
		if signal != nil {
			if _, err := io.CopyN(got, resp.Body, chunk); err != nil {
				t.Fatalf("reading the first chunk: %v", err)
			}
			close(signal)
		}
		n, err := io.Copy(got, resp.Body)
		if err != nil {
			t.Fatalf("after %d bytes: %v", n, err)
		}
		close(stop)
		<-sampled

		if !bytes.Equal(got.Sum(nil), want.Sum(nil)) {
			t.Error("the body is damaged")
		}

		return resp.Header, max(peak.Load(), before.HeapAlloc) - before.HeapAlloc
	}

	header, grew := download(firstChunkRead)
	if !strings.Contains(header.Get("Cache-Status"), "fwd=uri-miss; stored") {
		t.Errorf("Cache-Status: %s", header.Get("Cache-Status"))
	}
	t.Logf("heap grew by %d MiB while relaying %d MiB from the upstream", grew>>20, chunks)
	if grew > 32<<20 {
		t.Errorf("the heap grew by %d MiB while relaying a %d MiB response", grew>>20, chunks)
	}

	for range 3 {
		header, grew = download(nil)
		if !strings.Contains(header.Get("Cache-Status"), "hit; ttl=") {
			t.Errorf("Cache-Status: %s", header.Get("Cache-Status"))
		}
		if grew > 32<<20 {
			t.Errorf("the heap grew by %d MiB while serving a %d MiB response from the cache", grew>>20, chunks)
		}
	}
	t.Logf("heap grew by %d MiB while serving it from the cache", grew>>20)

	if n := up.hits.Load(); n != 1 {
		t.Errorf("the upstream got %d requests, want 1", n)
	}
	st := cacheStats(t)
	if st.MemoryBytes > 1<<20 {
		t.Errorf("the cache uses %d bytes of memory, over its limit", st.MemoryBytes)
	}
	if st.DiskBytes < chunks*chunk {
		t.Errorf("%d bytes on disk, less than the response", st.DiskBytes)
	}
}

func TestLimitsAreEnforced(t *testing.T) {
	const size = 100_000
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bodyFor(r.URL.Path, size))
	})
	dir := t.TempDir()
	tester := startCaddy(t, dir, `
			max_size 2Mi
			max_memory 1Mi`, `
		cache
		reverse_proxy `+up.addr())

	for round := range 4 {
		for i := range 60 {
			path := fmt.Sprintf("/object/%d", i)
			_, body := get(t, tester, path)
			if body != string(bodyFor(path, size)) {
				t.Fatalf("round %d: %s is damaged", round, path)
			}
		}
		st := cacheStats(t)
		if st.DiskBytes > 2<<20 || st.MemoryBytes > 1<<20 {
			t.Fatalf("round %d: limits exceeded: %+v", round, st)
		}
	}

	var onDisk int64
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			onDisk += info.Size()
		}
		return nil
	})
	if onDisk > 2<<20 {
		t.Errorf("%d bytes on disk, over the limit", onDisk)
	}
	st := cacheStats(t)
	if st.Evicted == 0 || st.Entries == 0 {
		t.Errorf("unexpected stats: %+v", st)
	}

	// A response larger than what may be stored is relayed all the same.
	up.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bodyFor("big", 3<<20))
	})
	for range 2 {
		resp, body := get(t, tester, "/big")
		if body != string(bodyFor("big", 3<<20)) {
			t.Error("the oversized response is damaged")
		}
		if strings.Contains(resp.Header.Get("Cache-Status"), "hit") {
			t.Errorf("Cache-Status: %s", resp.Header.Get("Cache-Status"))
		}
	}
	if st := cacheStats(t); st.DiskBytes > 2<<20 {
		t.Errorf("limits exceeded by an oversized response: %+v", st)
	}
}

func TestMaxFileCount(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "object "+r.URL.Path)
	})
	dir := t.TempDir()
	tester := startCaddy(t, dir, "max_file_count 5", `
		cache
		reverse_proxy `+up.addr())

	for i := range 20 {
		path := fmt.Sprintf("/object/%d", i)
		_, body := get(t, tester, path)
		expectBody(t, body, "object "+path)

		if st := cacheStats(t); st.Entries > 5 || st.MaxFiles != 5 {
			t.Fatalf("after %d objects: %+v", i+1, st)
		}
	}

	files := 0
	_ = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() && len(info.Name()) == 32 {
			files++
		}
		return nil
	})
	if files != 5 {
		t.Errorf("%d cache files on disk, want 5", files)
	}

	// The most recent ones are those that were kept.
	resp, _ := get(t, tester, "/object/19")
	expectHit(t, resp, "GET-http-localhost:9080-/object/19", 120)
	resp, _ = get(t, tester, "/object/0")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key=GET-http-localhost:9080-/object/0")
}

func TestCacheSurvivesReloadAndRestart(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "persistent")
	})
	dir := t.TempDir()
	site := `
		cache
		reverse_proxy ` + up.addr()
	const key = "GET-http-localhost:9080-/keep"

	tester := startCaddy(t, dir, "ttl 1h", site)
	get(t, tester, "/keep")

	// A reload with other settings keeps the cache as it is.
	tester = startCaddy(t, dir, `
			ttl 1h
			max_size 1Gi
			max_memory 64Mi`, site)
	resp, body := get(t, tester, "/keep")
	expectHit(t, resp, key, 3600)
	expectBody(t, body, "persistent")
	if st := cacheStats(t); st.MaxSize != 1<<30 || st.MaxMemory != 64<<20 {
		t.Errorf("the new limits were not applied: %+v", st)
	}

	// Moving the cache elsewhere closes the store, as stopping Caddy does…
	tester = startCaddy(t, t.TempDir(), "ttl 1h", site)
	resp, _ = get(t, tester, "/keep")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)

	// …and coming back to it finds the responses on disk.
	tester = startCaddy(t, dir, "ttl 1h", site)
	resp, body = get(t, tester, "/keep")
	if tier := expectHit(t, resp, key, 3600); tier != "DISK" {
		t.Errorf("served from %s after a restart, want DISK", tier)
	}
	expectBody(t, body, "persistent")

	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

// TestMinUses checks that with min_uses a response is kept in memory until
// it is requested again, and only then written to disk.
func TestMinUses(t *testing.T) {
	const size = 50_000
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/varied/"):
			w.Header().Set("Vary", "Accept-Language")
			_, _ = io.WriteString(w, r.Header.Get("Accept-Language")+" "+r.URL.Path)
		case strings.HasPrefix(r.URL.Path, "/stream/"):
			// No announced length, and more than memory is to hold of one
			// response.
			body := bodyFor(r.URL.Path, 300_000)
			for off := 0; off < len(body); off += 30_000 {
				_, _ = w.Write(body[off : off+30_000])
				w.(http.Flusher).Flush()
			}
		default:
			w.Header().Set("Content-Length", fmt.Sprint(size))
			_, _ = w.Write(bodyFor(r.URL.Path, size))
		}
	})
	dir := t.TempDir()
	options := `
			ttl 1h
			max_memory 1Mi
			min_uses 2`
	site := `
		cache
		reverse_proxy ` + up.addr()
	tester := startCaddy(t, dir, options, site)
	const prefix = "GET-http-localhost:9080-"

	// The first request stores the response, in memory only.
	for _, path := range []string{"/twice", "/once"} {
		resp, body := get(t, tester, path)
		expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+prefix+path)
		expectBody(t, body, string(bodyFor(path, size)))
	}
	_, body := get(t, tester, "/varied/page", "Accept-Language: fr")
	expectBody(t, body, "fr /varied/page")
	if st := cacheStats(t); cacheFiles(dir) != 0 || st.Entries != 4 || st.TransientEntries != 4 || st.DiskBytes != 0 {
		t.Fatalf("%d files after the first requests: %+v", cacheFiles(dir), st)
	}

	// The second is served from memory, and has the response written.
	resp, body := get(t, tester, "/twice")
	if tier := expectHit(t, resp, prefix+"/twice", 3600); tier != "MEMORY" {
		t.Errorf("second request served from %s, want MEMORY", tier)
	}
	expectBody(t, body, string(bodyFor("/twice", size)))
	waitFor(t, "the response requested twice to be written", func() bool { return cacheFiles(dir) == 1 })

	// So does it for a response that varies, along with what tells its
	// variants apart.
	resp, body = get(t, tester, "/varied/page", "Accept-Language: fr")
	expectHit(t, resp, prefix+"/varied/page", 3600)
	expectBody(t, body, "fr /varied/page")
	waitFor(t, "the variant requested twice to be written", func() bool { return cacheFiles(dir) == 3 })
	waitFor(t, "the counters to tell", func() bool {
		st := cacheStats(t)
		return st.Persisted == 2 && st.TransientEntries == 1
	})

	// A response memory cannot hold is written as it arrives, as without
	// min_uses.
	large := string(bodyFor("/stream/large", 300_000))
	resp, body = get(t, tester, "/stream/large")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+prefix+"/stream/large")
	expectBody(t, body, large)
	waitFor(t, "the large response to be stored", func() bool { return cacheFiles(dir) == 4 })
	resp, body = get(t, tester, "/stream/large")
	if tier := expectHit(t, resp, prefix+"/stream/large", 3600); tier != "DISK" {
		t.Errorf("large response served from %s, want DISK", tier)
	}
	expectBody(t, body, large)
	if st := cacheStats(t); st.Persisted != 2 || st.MemoryBytes > 1<<20 {
		t.Errorf("unexpected stats: %+v", st)
	}

	// Closing the store, as stopping Caddy does, loses what was requested
	// once and nothing else.
	startCaddy(t, t.TempDir(), options, site)
	tester = startCaddy(t, dir, options, site)
	for _, path := range []string{"/twice", "/varied/page", "/stream/large"} {
		resp, _ = get(t, tester, path, "Accept-Language: fr")
		if status := resp.Header.Get("Cache-Status"); !strings.Contains(status, "; hit; ") {
			t.Errorf("%s after a restart: %s, want a hit", path, status)
		}
	}
	resp, _ = get(t, tester, "/once")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+prefix+"/once")

	if n := up.hits.Load(); n != 5 {
		t.Errorf("the upstream got %d requests, want 5", n)
	}
}

func TestAdminAPI(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, r.URL.Path)
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	paths := []string{"/a/1", "/a/2", "/b/1", "/b/2", "/c"}
	for _, path := range paths {
		get(t, tester, path)
	}
	if st := cacheStats(t); st.Entries != len(paths) || st.Stored != int64(len(paths)) || st.DiskBytes == 0 {
		t.Errorf("unexpected stats: %+v", st)
	}

	isHit := func(path string) bool {
		resp, _ := get(t, tester, path)
		return strings.Contains(resp.Header.Get("Cache-Status"), "; hit; ")
	}

	if n := purge(t, "key=GET-http-localhost:9080-/c"); n != 1 {
		t.Errorf("purged %d responses by key, want 1", n)
	}
	if isHit("/c") {
		t.Error("/c is still cached after its purge")
	}

	if n := purge(t, "prefix=GET-http-localhost:9080-/a/"); n != 2 {
		t.Errorf("purged %d responses by prefix, want 2", n)
	}
	if isHit("/a/1") || !isHit("/b/1") {
		t.Error("the purge by prefix removed the wrong responses")
	}

	if n := purge(t, `regex=/b/\d$`); n != 2 {
		t.Errorf("purged %d responses by regex, want 2", n)
	}
	if isHit("/b/2") {
		t.Error("/b/2 is still cached after its purge")
	}

	purge(t, "all=true")
	if st := cacheStats(t); st.Entries != 0 || st.DiskBytes != 0 {
		t.Errorf("the cache is not empty after purging everything: %+v", st)
	}

	resp, err := http.Post(adminURL+"/cache/purge", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("a purge without target got status %d, want 400", resp.StatusCode)
	}
}

func TestUnsafeMethodsInvalidate(t *testing.T) {
	var version atomic.Int64
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost {
			version.Add(1)
			if r.URL.Query().Has("fail") {
				w.WriteHeader(http.StatusBadRequest)
			}
			return
		}
		_, _ = fmt.Fprintf(w, "version %d", version.Load())
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())

	_, body := get(t, tester, "/item")
	expectBody(t, body, "version 0")

	resp, _ := fetch(t, tester, http.MethodPost, "/item")
	expectStatus(t, resp, "Caddy; fwd=bypass; detail=UNSUPPORTED-METHOD")
	_, body = get(t, tester, "/item")
	expectBody(t, body, "version 1")

	// A failed request changed nothing.
	get(t, tester, "/other")
	fetch(t, tester, http.MethodPost, "/other?fail")
	resp, _ = get(t, tester, "/other")
	expectHit(t, resp, "GET-http-localhost:9080-/other", 120)
}

func TestHead(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, "the body")
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/doc"

	// A HEAD request does not fill the cache…
	resp, _ := fetch(t, tester, http.MethodHead, "/doc")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; detail=HEAD; key="+key)

	// …but is answered from it.
	get(t, tester, "/doc")
	resp, body := fetch(t, tester, http.MethodHead, "/doc")
	expectHit(t, resp, key, 120)
	if body != "" || resp.Header.Get("Content-Length") != "8" || resp.Header.Get("Content-Type") != "text/plain" {
		t.Errorf("HEAD from the cache: body %q, headers %v", body, resp.Header)
	}
	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

func TestModes(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/bypass") {
			w.Header().Set("Cache-Control", "no-store")
		}
		w.Header().Set("Etag", `"v1"`)
		if r.Header.Get("If-None-Match") == `"v1"` {
			w.WriteHeader(http.StatusNotModified)
			return
		}
		_, _ = io.WriteString(w, "content")
	})
	tester := startCaddy(t, t.TempDir(), "", `
		route /default {
			cache
			reverse_proxy `+up.addr()+`
		}
		route /strict {
			cache {
				mode strict
			}
			reverse_proxy `+up.addr()+`
		}
		route /bypass {
			cache {
				mode bypass_response
			}
			reverse_proxy `+up.addr()+`
		}`)

	// By default clients cannot force their way past the cache.
	get(t, tester, "/default")
	for _, header := range []string{"Cache-Control: no-cache", "Pragma: no-cache", "Cache-Control: max-age=0", "Cache-Control: no-store"} {
		resp, _ := get(t, tester, "/default", header)
		expectHit(t, resp, "GET-http-localhost:9080-/default", 120)
	}

	// In strict mode they are obeyed.
	get(t, tester, "/strict")
	resp, _ := get(t, tester, "/strict")
	expectHit(t, resp, "GET-http-localhost:9080-/strict", 120)
	resp, body := get(t, tester, "/strict", "Cache-Control: no-cache")
	expectStatus(t, resp, "Caddy; fwd=stale; fwd-status=304; detail=REVALIDATED; key=GET-http-localhost:9080-/strict")
	expectBody(t, body, "content")
	resp, _ = get(t, tester, "/strict", "Cache-Control: no-store")
	expectStatus(t, resp, "Caddy; fwd=bypass; detail=REQUEST-NO-STORE; key=GET-http-localhost:9080-/strict")
	resp, _ = get(t, tester, "/strict?absent", "Cache-Control: only-if-cached")
	if resp.StatusCode != http.StatusGatewayTimeout {
		t.Errorf("only-if-cached for a response not in the cache: status %d, want 504", resp.StatusCode)
	}

	// The response directives can be ignored too.
	get(t, tester, "/bypass")
	resp, _ = get(t, tester, "/bypass")
	expectHit(t, resp, "GET-http-localhost:9080-/bypass", 120)
}

// TestKeyTemplate covers the case of many URLs for one upstream object: the
// key is built from what identifies the object, so they share one response.
func TestKeyTemplate(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "object at "+r.URL.Path)
	})
	tester := startCaddy(t, t.TempDir(), "", `
		@image path_regexp image ^/img/download/(.+)/([0-9]+).*\.([A-Za-z0-9]+)$
		cache @image {
			key {
				template {re.image.1}/{re.image.2}.{re.image.3}
			}
		}
		rewrite @image /bucket/images/{re.image.1}/{re.image.2}/full.{re.image.3}
		reverse_proxy `+up.addr())

	resp, body := get(t, tester, "/img/download/2024/5/17/123__safe_cute.png")
	expectStatus(t, resp, `Caddy; fwd=uri-miss; stored; key="2024/5/17/123.png"`)
	expectBody(t, body, "object at /bucket/images/2024/5/17/123/full.png")

	for _, url := range []string{"/img/download/2024/5/17/123.png", "/img/download/2024/5/17/123__other_tags.png?download=1"} {
		resp, body = get(t, tester, url)
		expectHit(t, resp, `"2024/5/17/123.png"`, 120)
		expectBody(t, body, "object at /bucket/images/2024/5/17/123/full.png")
	}

	_, body = get(t, tester, "/img/download/2024/5/17/124.png")
	expectBody(t, body, "object at /bucket/images/2024/5/17/124/full.png")

	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

func TestDirectiveOptions(t *testing.T) {
	tester := startCaddy(t, t.TempDir(), "", `
		route /named {
			cache {
				cache_name Edge
				ttl 10s
				key {
					hide
				}
			}
			respond "named"
		}
		route /control {
			cache {
				default_cache_control "public, max-age=30"
			}
			respond "control"
		}
		route /excluded/* {
			cache {
				regex {
					exclude ^/excluded/
				}
			}
			respond "excluded"
		}
		route /status {
			cache {
				allowed_additional_status_codes 404
			}
			respond "nope" 404
		}
		route /query {
			cache {
				key {
					disable_query
				}
			}
			respond "query"
		}`)

	get(t, tester, "/named")
	resp, _ := get(t, tester, "/named")
	if status := resp.Header.Get("Cache-Status"); status != "Edge; hit; ttl=10; detail=DISK" && status != "Edge; hit; ttl=9; detail=DISK" {
		t.Errorf("Cache-Status: %s", status)
	}

	resp, _ = get(t, tester, "/control")
	if cc := resp.Header.Get("Cache-Control"); cc != "public, max-age=30" {
		t.Errorf("Cache-Control: %q", cc)
	}
	resp, _ = get(t, tester, "/control")
	expectHit(t, resp, "GET-http-localhost:9080-/control", 30)
	if cc := resp.Header.Get("Cache-Control"); cc != "public, max-age=30" {
		t.Errorf("Cache-Control of the hit: %q", cc)
	}

	for range 2 {
		resp, _ = get(t, tester, "/excluded/page")
		expectStatus(t, resp, "Caddy; fwd=bypass; detail=EXCLUDED")
	}

	get(t, tester, "/status")
	resp, body := get(t, tester, "/status")
	expectHit(t, resp, "GET-http-localhost:9080-/status", 120)
	if resp.StatusCode != http.StatusNotFound || body != "nope" {
		t.Errorf("cached 404: status %d, body %q", resp.StatusCode, body)
	}

	get(t, tester, "/query?a=1")
	resp, _ = get(t, tester, "/query?b=2")
	expectHit(t, resp, "GET-http-localhost:9080-/query", 120)
}

// TestHeadersSetBeforeTheCache covers the headers a directive in front of
// the cache gives every response, such as a long max-age for the browsers:
// they neither decide how long the cache keeps a response nor get stored.
func TestHeadersSetBeforeTheCache(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/own" {
			w.Header().Set("Cache-Control", "max-age=60")
		}
		_, _ = io.WriteString(w, "image")
	})
	tester := startCaddy(t, t.TempDir(), "ttl 5s", `
		header Cache-Control "public, max-age=31536000"
		header Content-Disposition attachment
		cache
		reverse_proxy `+up.addr())

	for i := range 2 {
		resp, _ := get(t, tester, "/plain")
		if got := resp.Header.Values("Cache-Control"); len(got) != 1 || got[0] != "public, max-age=31536000" {
			t.Errorf("request %d: Cache-Control %q", i, got)
		}
		if got := resp.Header.Values("Content-Disposition"); len(got) != 1 || got[0] != "attachment" {
			t.Errorf("request %d: Content-Disposition %q", i, got)
		}
		if i == 1 {
			// The upstream said nothing: the configured ttl applies.
			expectHit(t, resp, "GET-http-localhost:9080-/plain", 5)
		}
	}

	for i := range 2 {
		resp, _ := get(t, tester, "/own")
		if got := resp.Header.Values("Cache-Control"); len(got) != 2 || got[1] != "max-age=60" {
			t.Errorf("request %d: Cache-Control %q", i, got)
		}
		if i == 1 {
			// What the upstream says is obeyed.
			expectHit(t, resp, "GET-http-localhost:9080-/own", 60)
		}
	}
}

func TestHandlerOverridesGlobalOptions(t *testing.T) {
	tester := startCaddy(t, t.TempDir(), `
			ttl 50s
			cache_name Global`, `
		route /global {
			cache
			respond "global"
		}
		route /local {
			cache {
				ttl 5s
			}
			respond "local"
		}`)

	get(t, tester, "/global")
	resp, _ := get(t, tester, "/global")
	if !strings.HasPrefix(resp.Header.Get("Cache-Status"), "Global; hit; ttl=5") && !strings.HasPrefix(resp.Header.Get("Cache-Status"), "Global; hit; ttl=49") {
		t.Errorf("Cache-Status: %s", resp.Header.Get("Cache-Status"))
	}

	get(t, tester, "/local")
	resp, _ = get(t, tester, "/local")
	if !strings.HasPrefix(resp.Header.Get("Cache-Status"), "Global; hit; ttl=5;") && !strings.HasPrefix(resp.Header.Get("Cache-Status"), "Global; hit; ttl=4;") {
		t.Errorf("Cache-Status: %s", resp.Header.Get("Cache-Status"))
	}
}

func TestJSONConfiguration(t *testing.T) {
	dir := t.TempDir()
	tester := caddytest.NewTester(t)
	tester.InitServer(fmt.Sprintf(`{
		"admin": {"listen": "localhost:2999"},
		"apps": {
			"cache": {
				"path": %q,
				"max_size": "64Mi",
				"max_memory": 8388608,
				"max_file_count": 1000,
				"ttl": "1h"
			},
			"http": {
				"http_port": 9080,
				"servers": {
					"test": {
						"listen": [":9080"],
						"routes": [{
							"handle": [
								{"handler": "cache", "stale": "30s", "key": {"disable_host": true}},
								{"handler": "static_response", "body": "from json"}
							]
						}]
					}
				}
			}
		}
	}`, dir), "json")

	get(t, tester, "/json")
	resp, body := get(t, tester, "/json")
	expectHit(t, resp, "GET-http-/json", 3600)
	expectBody(t, body, "from json")

	if st := cacheStats(t); st.Path != dir || st.MaxSize != 64<<20 || st.MaxMemory != 8<<20 || st.MaxFiles != 1000 {
		t.Errorf("unexpected stats: %+v", st)
	}
}

// TestRequestIsHandledTwiceAsReceived covers the responses the cache finds
// out it cannot use after asking for them: the client's request then goes to
// the upstream as it was received, not as the first attempt left it.
func TestRequestIsHandledTwiceAsReceived(t *testing.T) {
	var paths sync.Map
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		paths.Store(r.URL.Path+" "+r.Header.Get("X-Added"), true)
		w.Header().Set("Cache-Control", "no-store")
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader("path="+r.URL.Path))
	})
	tester := startCaddy(t, t.TempDir(), "", `
		route {
			cache
			uri strip_prefix /api
			request_header +X-Added once
			reverse_proxy `+up.addr()+`
		}`)

	resp, body := get(t, tester, "/api/api/x", "Range: bytes=0-10")
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("status %d, want 206", resp.StatusCode)
	}
	expectBody(t, body, "path=/api/x")

	paths.Range(func(key, _ any) bool {
		if key != "/api/x once" {
			t.Errorf("the upstream got a request for %q", key)
		}
		return true
	})
}

// TestRewriteIsAppliedToARequestHandledTwice covers the directives of which
// Caddy applies only the first that matches, rewrite and handle: the router
// remembers in the request that one was applied, which must not keep it from
// applying it again to a request the cache starts over.
func TestRewriteIsAppliedToARequestHandledTwice(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/real/x" {
			t.Errorf("the upstream got a request for %q", r.URL.Path)
		}
		w.Header().Set("Cache-Control", "no-store")
		http.ServeContent(w, r, "", time.Time{}, strings.NewReader("path="+r.URL.Path))
	})
	tester := startCaddy(t, t.TempDir(), "", `
		cache
		rewrite * /real{path}
		reverse_proxy `+up.addr())

	resp, body := get(t, tester, "/x", "Range: bytes=5-")
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("status %d, want 206", resp.StatusCode)
	}
	expectBody(t, body, "/real/x")
	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

func TestFailedReloadLeavesTheCacheAlone(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(bodyFor(r.URL.Path, 300_000))
	})
	dir := t.TempDir()
	tester := startCaddy(t, dir, "max_size 100Mi", `
		cache
		reverse_proxy `+up.addr())

	for i := range 10 {
		get(t, tester, fmt.Sprintf("/object/%d", i))
	}
	before := cacheStats(t)

	// This configuration shrinks the cache to less than it holds, but is
	// refused once its first site is set up, for the limits of its second.
	loadError(t, fmt.Sprintf(`
	{
		admin localhost:2999
		http_port 9080
		cache {
			path %s
			max_size 1Mi
		}
	}
	localhost:9080 {
		cache
		reverse_proxy %s
	}
	localhost:9081 {
		cache {
			max_size 2Mi
		}
	}`, dir, up.addr()))

	after := cacheStats(t)
	if after.MaxSize != 100<<20 || after.Entries != before.Entries {
		t.Errorf("a refused configuration changed the cache: %+v, was %+v", after, before)
	}
	resp, _ := get(t, tester, "/object/0")
	expectHit(t, resp, "GET-http-localhost:9080-/object/0", 120)
}

// loadError submits a configuration that must be refused and returns why.
func loadError(t *testing.T, config string) string {
	t.Helper()

	resp, err := http.Post(adminURL+"/load", "text/caddyfile", strings.NewReader(config))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, _ := io.ReadAll(resp.Body)
	// The admin API starts answering with the warnings of the adapter, so a
	// configuration refused afterwards still has a 200 status.
	if resp.StatusCode == http.StatusOK && !bytes.Contains(body, []byte(`"error"`)) {
		t.Errorf("configuration accepted:\n%s", config)
	}

	return string(body)
}

func TestInvalidConfigurations(t *testing.T) {
	// Something to submit the configurations to.
	dir := t.TempDir()
	startCaddy(t, dir, "", "")

	site := func(cache string) string {
		return `
		{
			admin localhost:2999
			http_port 9080
		}
		localhost:9080 {
			cache {
				` + cache + `
			}
		}`
	}

	cases := map[string]string{
		"redis {\n url 127.0.0.1:6379\n }": "storage backends were removed",
		"storers badger":                   "storage backends were removed",
		"max_size lots":                    "invalid size",
		"max_size 0":                       "max_size must be positive",
		"max_memory 4k":                    "max_memory must be at least",
		"max_file_count lots":              "invalid max_file_count",
		"max_file_count 0":                 "invalid max_file_count",
		"min_uses 0":                       "invalid min_uses",
		"min_uses 5000":                    "min_uses must be between",
		"min_uses 2\n max_memory off":      "max_memory off does not allow",
		"slice lots":                       "invalid size",
		"slice 1k":                         "slice must be at least",
		"slice 64Mi\n max_size 100Mi":      "slice cannot be larger than half of max_size",
		"mode relaxed":                     "unknown cache mode",
		"ttl":                              "wrong argument count",
		"ttl soon":                         "invalid duration",
		"surprise":                         "unsupported cache option",
		"regex {\n exclude ( \n }":         "regex exclude",
	}
	for options, want := range cases {
		if got := loadError(t, site(options)); !strings.Contains(got, want) {
			t.Errorf("%q refused with %s, want an error about %q", options, got, want)
		}
	}

	// One directory is one cache, with one set of limits.
	got := loadError(t, fmt.Sprintf(`
	{
		admin localhost:2999
		http_port 9080
		cache {
			path %s
		}
	}
	localhost:9080 {
		route /a {
			cache {
				max_size 1Gi
			}
		}
		route /b {
			cache {
				max_size 2Gi
			}
		}
	}`, dir))
	if !strings.Contains(got, "configured with different") {
		t.Errorf("conflicting limits refused with %s", got)
	}
}
