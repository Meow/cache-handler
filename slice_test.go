package httpcache

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"github.com/caddyserver/caddy/v2/modules/caddyhttp"
)

func init() {
	caddy.RegisterModule(new(panicHandler))
	httpcaddyfile.RegisterHandlerDirective("test_panic", func(h httpcaddyfile.Helper) (caddyhttp.MiddlewareHandler, error) {
		handler := new(panicHandler)

		return handler, handler.UnmarshalCaddyfile(h.Dispenser)
	})
}

// panicHandler is a handler with a bug: it panics when it is asked for a
// given range. It exists for the tests only, as the test_panic directive.
type panicHandler struct {
	Range string `json:"range,omitempty"`
}

func (*panicHandler) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "http.handlers.test_panic",
		New: func() caddy.Module { return new(panicHandler) },
	}
}

func (h *panicHandler) UnmarshalCaddyfile(d *caddyfile.Dispenser) error {
	d.Next()
	if !d.AllArgs(&h.Range) {
		return d.ArgErr()
	}

	return nil
}

func (h *panicHandler) ServeHTTP(w http.ResponseWriter, r *http.Request, next caddyhttp.Handler) error {
	if r.Header.Get("Range") == h.Range {
		panic("test_panic: asked for " + h.Range)
	}

	return next.ServeHTTP(w, r)
}

// sliceContent returns a body in which no two places look alike, unlike the
// one of bodyFor, which repeats every 32 bytes: a slice put at the place of
// another one must not go unnoticed.
func sliceContent(seed string, size int) []byte {
	body := make([]byte, 0, size+sha256.Size)
	for i := 0; len(body) < size; i++ {
		sum := sha256.Sum256(fmt.Appendf(nil, "%s-%d", seed, i))
		body = append(body, sum[:]...)
	}

	return body[:size]
}

// origin is an upstream that serves one body per version, ranges included,
// and remembers the ranges it was asked for.
type origin struct {
	*upstream
	// version selects the body and its entity tag, size its length.
	version atomic.Value
	size    atomic.Int64
	// header is added to the responses.
	header http.Header
	// before, if set, is called with each request before it is answered.
	before func(r *http.Request)

	mu     sync.Mutex
	ranges []string
}

func newOrigin(t *testing.T, size int) *origin {
	t.Helper()

	o := &origin{header: make(http.Header)}
	o.version.Store("v1")
	o.size.Store(int64(size))
	o.upstream = newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		o.mu.Lock()
		o.ranges = append(o.ranges, r.Header.Get("Range"))
		o.mu.Unlock()
		if o.before != nil {
			o.before(r)
		}

		version := o.version.Load().(string)
		for name, values := range o.header {
			w.Header()[name] = values
		}
		w.Header().Set("Etag", `"`+version+`"`)
		w.Header().Set("Content-Type", "application/octet-stream")
		http.ServeContent(w, r, "", time.Time{}, bytes.NewReader(o.content(version)))
	})

	return o
}

func (o *origin) content(version string) []byte {
	return sliceContent(version, int(o.size.Load()))
}

// asked returns the ranges the origin was asked for so far, sorted.
func (o *origin) asked() []string {
	o.mu.Lock()
	defer o.mu.Unlock()

	ranges := slices.Clone(o.ranges)
	slices.Sort(ranges)

	return ranges
}

// rangeHandler answers requests for one range of content, like an upstream
// that supports them does. When stall returns a channel for the first byte
// of a range, the second half of that range is only sent once the channel
// is closed.
func rangeHandler(t *testing.T, content []byte, stall func(first int) <-chan struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var first, last int
		if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &first, &last); err != nil {
			t.Errorf("unexpected range %q", r.Header.Get("Range"))
			return
		}
		last = min(last, len(content)-1)

		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", first, last, len(content)))
		w.Header().Set("Content-Length", fmt.Sprint(last-first+1))
		w.WriteHeader(http.StatusPartialContent)

		half := first + (last-first+1)/2
		_, _ = w.Write(content[first:half])
		if wait := stall(first); wait != nil {
			w.(http.Flusher).Flush()
			select {
			case <-wait:
			case <-time.After(10 * time.Second):
			}
		}
		_, _ = w.Write(content[half : last+1])
	}
}

func TestSlices(t *testing.T) {
	up := newOrigin(t, 40_000)
	content := string(up.content("v1"))
	tester := startCaddy(t, t.TempDir(), "ttl 1h\nslice 4Ki", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/video"

	// A range far into a response that is not in the cache: the slice it is
	// in is all the upstream is asked for.
	resp, body := get(t, tester, "/video", "Range: bytes=30000-30099")
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 30000-30099/40000" {
		t.Errorf("status %d, Content-Range %q", resp.StatusCode, resp.Header.Get("Content-Range"))
	}
	expectBody(t, body, content[30000:30100])
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)
	if resp.Header.Get("Etag") != `"v1"` || resp.Header.Get("Content-Type") != "application/octet-stream" {
		t.Errorf("upstream headers lost: %v", resp.Header)
	}
	if asked := up.asked(); !slices.Equal(asked, []string{"bytes=28672-32767"}) {
		t.Errorf("the upstream was asked for %v", asked)
	}

	// The whole response is put together from its slices, of which the
	// missing ones are fetched, once each.
	resp, body = get(t, tester, "/video")
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") != "40000" || resp.Header.Get("Content-Range") != "" {
		t.Errorf("status %d, headers %v", resp.StatusCode, resp.Header)
	}
	expectBody(t, body, content)
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)
	if n := up.hits.Load(); n != 10 {
		t.Errorf("the upstream got %d requests, want one for each of the 10 slices: %v", n, up.asked())
	}
	// The last slice is as long as what is left.
	if asked := up.asked(); !slices.Contains(asked, "bytes=0-4095") || !slices.Contains(asked, "bytes=36864-40959") {
		t.Errorf("the upstream was asked for %v", asked)
	}

	// From now on everything comes from the cache.
	resp, body = get(t, tester, "/video")
	expectHit(t, resp, key, 3600)
	expectBody(t, body, content)

	resp, body = get(t, tester, "/video", "Range: bytes=4000-9000")
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 4000-9000/40000" {
		t.Errorf("range over three slices: status %d, Content-Range %q", resp.StatusCode, resp.Header.Get("Content-Range"))
	}
	expectBody(t, body, content[4000:9001])

	resp, body = get(t, tester, "/video", "Range: bytes=-100")
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("suffix range: status %d", resp.StatusCode)
	}
	expectBody(t, body, content[39900:])

	resp, body = get(t, tester, "/video", "Range: bytes=10-19,20000-20009")
	if resp.StatusCode != http.StatusPartialContent || !strings.HasPrefix(resp.Header.Get("Content-Type"), "multipart/byteranges") {
		t.Errorf("two ranges: status %d, Content-Type %q", resp.StatusCode, resp.Header.Get("Content-Type"))
	}
	if !strings.Contains(body, content[10:20]) || !strings.Contains(body, content[20000:20010]) {
		t.Error("two ranges: a range is missing from the body")
	}

	resp, body = get(t, tester, "/video", `If-None-Match: "v1"`)
	if resp.StatusCode != http.StatusNotModified || body != "" {
		t.Errorf("matching If-None-Match: status %d, body %q", resp.StatusCode, abbreviate(body))
	}

	resp, body = fetch(t, tester, http.MethodHead, "/video")
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Length") != "40000" || body != "" {
		t.Errorf("HEAD: status %d, headers %v", resp.StatusCode, resp.Header)
	}

	if n := up.hits.Load(); n != 10 {
		t.Errorf("the upstream got %d requests, want 10", n)
	}
	// The slices, and the entry that leads to them.
	if st := cacheStats(t); st.Entries != 11 || st.Stored != 10 {
		t.Errorf("unexpected stats: %+v", st)
	}

	// Purging the key leaves none of its slices to be found.
	if n := purge(t, "key="+key); n != 1 {
		t.Errorf("purged %d entries by key, want 1", n)
	}
	resp, body = get(t, tester, "/video", "Range: bytes=30000-30099")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)
	expectBody(t, body, content[30000:30100])
	if n := up.hits.Load(); n != 11 {
		t.Errorf("the upstream got %d requests, want 11", n)
	}

	// A purge by prefix finds the slices themselves: the new one and what
	// leads to it, and the ten the first purge left for eviction to remove.
	if n := purge(t, "prefix=GET-http-localhost:9080-/vid"); n != 12 {
		t.Errorf("purged %d entries by prefix, want 12", n)
	}
	if st := cacheStats(t); st.Entries != 0 || st.DiskBytes != 0 {
		t.Errorf("the cache is not empty after the purge: %+v", st)
	}
}

// TestSeekIntoADownloadInProgress is what slices are for: a request for the
// end of a response does not wait for the download of its beginning.
func TestSeekIntoADownloadInProgress(t *testing.T) {
	content := string(sliceContent("seek", 40_000))
	release := make(chan struct{})
	var (
		mu    sync.Mutex
		asked []string
	)
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		asked = append(asked, r.Header.Get("Range"))
		mu.Unlock()

		rangeHandler(t, []byte(content), func(first int) <-chan struct{} {
			// The first slice takes its time.
			if first == 0 {
				return release
			}
			return nil
		})(w, r)
	})
	tester := startCaddy(t, t.TempDir(), "slice 4Ki", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/video"

	whole := make(chan string, 2)
	for range 2 {
		go func() {
			_, body := get(t, tester, "/video")
			whole <- body
		}()
	}
	waitFor(t, "the download to start", func() bool { return up.hits.Load() == 1 })

	seek := make(chan string, 1)
	go func() {
		resp, body := get(t, tester, "/video", "Range: bytes=30000-")
		if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 30000-39999/40000" {
			t.Errorf("status %d, Content-Range %q", resp.StatusCode, resp.Header.Get("Content-Range"))
		}
		expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)
		seek <- body
	}()
	select {
	case body := <-seek:
		expectBody(t, body, content[30000:])
	case <-time.After(5 * time.Second):
		t.Fatal("a request for the end of the response waited for its beginning")
	}

	close(release)
	for range 2 {
		expectBody(t, <-whole, content)
	}

	// Each slice was fetched once, whoever wanted it.
	slices.Sort(asked)
	if len(asked) != 10 || len(slices.Compact(slices.Clone(asked))) != 10 {
		t.Errorf("the upstream was asked for %v, want each of the 10 slices once", asked)
	}
}

func TestSliceIsServedWhileItDownloads(t *testing.T) {
	content := sliceContent("slow", 20_000)
	release := make(chan struct{})
	// Half of the slice, then the rest when the test says so.
	up := newUpstream(t, rangeHandler(t, content, func(int) <-chan struct{} { return release }))
	tester := startCaddy(t, t.TempDir(), "slice 8Ki", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/video"

	open := func() *http.Response {
		req, _ := http.NewRequest(http.MethodGet, testURL+"/video", nil)
		req.Header.Set("Range", "bytes=8192-16383")
		resp, err := tester.Client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })

		return resp
	}

	// The request that triggers the download of a slice gets what there is
	// of it, and so does one that comes meanwhile.
	leader := open()
	expectStatus(t, leader, "Caddy; fwd=uri-miss; stored; key="+key)
	follower := open()
	expectStatus(t, follower, "Caddy; fwd=uri-miss; collapsed; key="+key)
	for _, resp := range []*http.Response{leader, follower} {
		head := make([]byte, 4096)
		if _, err := io.ReadFull(resp.Body, head); err != nil || !bytes.Equal(head, content[8192:12288]) {
			t.Fatalf("reading the half of the slice that was downloaded: %v", err)
		}
	}

	close(release)
	for _, resp := range []*http.Response{leader, follower} {
		rest, err := io.ReadAll(resp.Body)
		if err != nil || !bytes.Equal(rest, content[12288:16384]) {
			t.Errorf("reading the rest of the slice: %d bytes, %v", len(rest), err)
		}
	}
	if n := up.hits.Load(); n != 1 {
		t.Errorf("the upstream got %d requests, want 1", n)
	}
}

// TestSmallResponseIsNotSliced checks that a response that fits in one slice
// is stored like it is without slices.
func TestSmallResponseIsNotSliced(t *testing.T) {
	up := newOrigin(t, 1000)
	content := string(up.content("v1"))
	tester := startCaddy(t, t.TempDir(), "ttl 1h\nslice 4Ki", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/small"

	resp, body := get(t, tester, "/small")
	if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Range") != "" || resp.Header.Get("Content-Length") != "1000" {
		t.Errorf("status %d, headers %v", resp.StatusCode, resp.Header)
	}
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)
	expectBody(t, body, content)

	resp, body = get(t, tester, "/small")
	expectHit(t, resp, key, 3600)
	expectBody(t, body, content)

	resp, body = get(t, tester, "/small", "Range: bytes=100-199")
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 100-199/1000" {
		t.Errorf("status %d, Content-Range %q", resp.StatusCode, resp.Header.Get("Content-Range"))
	}
	expectBody(t, body, content[100:200])

	if asked := up.asked(); !slices.Equal(asked, []string{"bytes=0-4095"}) {
		t.Errorf("the upstream was asked for %v", asked)
	}
	if st := cacheStats(t); st.Entries != 1 {
		t.Errorf("%d entries, want the response alone", st.Entries)
	}

	// A range on a response not in the cache yet is answered all the same.
	resp, body = get(t, tester, "/other", "Range: bytes=100-199")
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 100-199/1000" {
		t.Errorf("status %d, Content-Range %q", resp.StatusCode, resp.Header.Get("Content-Range"))
	}
	expectBody(t, body, content[100:200])
}

// TestUpstreamWithoutRangesIsNotSliced covers the upstream that answers a
// request for a slice with the whole response.
func TestUpstreamWithoutRangesIsNotSliced(t *testing.T) {
	content := string(sliceContent("whole", 20_000))
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/missing" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		_, _ = io.WriteString(w, content)
	})
	tester := startCaddy(t, t.TempDir(), "ttl 1h\nslice 4Ki\nallowed_additional_status_codes 404", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/file"

	resp, body := get(t, tester, "/file", "Range: bytes=10000-10099")
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 10000-10099/20000" {
		t.Errorf("status %d, Content-Range %q", resp.StatusCode, resp.Header.Get("Content-Range"))
	}
	expectBody(t, body, content[10000:10100])
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)

	resp, body = get(t, tester, "/file")
	expectHit(t, resp, key, 3600)
	expectBody(t, body, content)

	// So is any response that is not part of a larger one.
	resp, _ = get(t, tester, "/missing")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status %d, want 404", resp.StatusCode)
	}
	resp, _ = get(t, tester, "/missing", "Range: bytes=10000-")
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status %d, want 404", resp.StatusCode)
	}
	expectHit(t, resp, "GET-http-localhost:9080-/missing", 3600)

	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
	if st := cacheStats(t); st.Entries != 2 {
		t.Errorf("%d entries, want 2", st.Entries)
	}
}

func TestSliceBeyondTheEnd(t *testing.T) {
	up := newOrigin(t, 40_000)
	tester := startCaddy(t, t.TempDir(), "slice 4Ki", `
		cache
		reverse_proxy `+up.addr())

	// The upstream has no such slice. The first one tells the size of the
	// response, which the answer has to say.
	resp, _ := get(t, tester, "/video", "Range: bytes=50000-")
	if resp.StatusCode != http.StatusRequestedRangeNotSatisfiable || resp.Header.Get("Content-Range") != "bytes */40000" {
		t.Errorf("status %d, Content-Range %q", resp.StatusCode, resp.Header.Get("Content-Range"))
	}
	if asked := up.asked(); !slices.Equal(asked, []string{"bytes=0-4095", "bytes=49152-53247"}) {
		t.Errorf("the upstream was asked for %v", asked)
	}
}

// TestEmptyResponseIsNotSliced covers the upstream that has no range to
// give of a response, not even the first.
func TestEmptyResponseIsNotSliced(t *testing.T) {
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") != "" {
			w.Header().Set("Content-Range", "bytes */0")
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("X-Empty", "yes")
	})
	tester := startCaddy(t, t.TempDir(), "slice 4Ki", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/empty"

	resp, body := get(t, tester, "/empty")
	if resp.StatusCode != http.StatusOK || body != "" || resp.Header.Get("X-Empty") != "yes" {
		t.Errorf("status %d, body %q, headers %v", resp.StatusCode, body, resp.Header)
	}
	expectStatus(t, resp, "Caddy; fwd=uri-miss; stored; key="+key)

	resp, _ = get(t, tester, "/empty")
	expectHit(t, resp, key, 120)
	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}
}

// TestSlicedResponseThatChanges checks that a client is never sent the
// slices of two different responses as one.
func TestSlicedResponseThatChanges(t *testing.T) {
	up := newOrigin(t, 40_000)
	tester := startCaddy(t, t.TempDir(), "ttl 1h\nslice 4Ki", `
		cache
		reverse_proxy `+up.addr())

	download := func(path string) (*http.Response, string, error) {
		resp, err := tester.Client.Get(testURL + path)
		if err != nil {
			return nil, "", err
		}
		defer func() { _ = resp.Body.Close() }()
		body, err := io.ReadAll(resp.Body)

		return resp, string(body), err
	}

	// The beginning of the first version is in the cache when the upstream
	// gets another one.
	get(t, tester, "/video", "Range: bytes=0-99")
	up.version.Store("v2")

	if _, body, err := download("/video"); err == nil {
		t.Errorf("a response made of two versions was delivered as complete: %s", abbreviate(body))
	}

	// What was left of the first version went with it.
	resp, body, err := download("/video")
	if err != nil {
		t.Fatal(err)
	}
	if resp.Header.Get("Etag") != `"v2"` {
		t.Errorf("Etag: %s", resp.Header.Get("Etag"))
	}
	expectBody(t, body, string(up.content("v2")))

	// The same goes for a response that is no longer one to slice.
	get(t, tester, "/other", "Range: bytes=0-99")
	up.size.Store(1000)
	if _, body, err := download("/other"); err == nil {
		t.Errorf("a response made of two versions was delivered as complete: %s", abbreviate(body))
	}
	resp, body, err = download("/other")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status %d", resp.StatusCode)
	}
	expectBody(t, body, string(up.content("v2")))
}

func TestSliceRevalidation(t *testing.T) {
	up := newOrigin(t, 40_000)
	content := string(up.content("v1"))
	var conditional atomic.Int64
	up.before = func(r *http.Request) {
		if r.Header.Get("If-None-Match") != "" {
			conditional.Add(1)
			if r.Header.Get("Range") != "bytes=28672-32767" {
				t.Errorf("a slice was revalidated with the range %q", r.Header.Get("Range"))
			}
		}
	}
	tester := startCaddy(t, t.TempDir(), "ttl 1s\nslice 4Ki", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/video"

	get(t, tester, "/video", "Range: bytes=30000-30099")
	time.Sleep(1100 * time.Millisecond)

	// Expired: the upstream confirms the slice without sending it again.
	resp, body := get(t, tester, "/video", "Range: bytes=30000-30099")
	expectStatus(t, resp, "Caddy; fwd=stale; fwd-status=304; detail=REVALIDATED; key="+key)
	if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Range") != "bytes 30000-30099/40000" {
		t.Errorf("status %d, Content-Range %q", resp.StatusCode, resp.Header.Get("Content-Range"))
	}
	expectBody(t, body, content[30000:30100])
	if conditional.Load() != 1 {
		t.Errorf("the upstream got %d conditional requests, want 1", conditional.Load())
	}

	// And it is fresh again.
	resp, body = get(t, tester, "/video", "Range: bytes=30000-30099")
	expectHit(t, resp, key, 1)
	expectBody(t, body, content[30000:30100])
	if n := up.hits.Load(); n != 2 {
		t.Errorf("the upstream got %d requests, want 2", n)
	}

	// When the response changed, the new slice replaces the old one.
	up.version.Store("v2")
	time.Sleep(1100 * time.Millisecond)
	resp, body = get(t, tester, "/video", "Range: bytes=30000-30099")
	expectStatus(t, resp, "Caddy; fwd=stale; stored; key="+key)
	expectBody(t, body, string(up.content("v2")[30000:30100]))
	if resp.Header.Get("Etag") != `"v2"` {
		t.Errorf("Etag: %s", resp.Header.Get("Etag"))
	}
}

func TestSlicesOfAVaryingResponse(t *testing.T) {
	up := newOrigin(t, 20_000)
	up.header.Set("Vary", "X-Kind")
	up.before = func(r *http.Request) {
		// One version per value of the header.
		up.version.Store("kind-" + r.Header.Get("X-Kind"))
	}
	tester := startCaddy(t, t.TempDir(), "ttl 1h\nslice 4Ki", `
		cache
		reverse_proxy `+up.addr())

	for range 2 {
		for _, kind := range []string{"a", "b"} {
			content := string(up.content("kind-" + kind))

			resp, body := get(t, tester, "/varied", "X-Kind: "+kind, "Range: bytes=10000-10099")
			if resp.StatusCode != http.StatusPartialContent {
				t.Errorf("status %d", resp.StatusCode)
			}
			expectBody(t, body, content[10000:10100])

			_, body = get(t, tester, "/varied", "X-Kind: "+kind)
			expectBody(t, body, content)
		}
	}
	// Five slices for each of the two.
	if n := up.hits.Load(); n != 10 {
		t.Errorf("the upstream got %d requests, want 10: %v", n, up.asked())
	}
}

// gunzip inflates the body of a response that was compressed.
func gunzip(t *testing.T, body string) string {
	t.Helper()

	zr, err := gzip.NewReader(strings.NewReader(body))
	if err != nil {
		t.Fatalf("not gzip: %v", err)
	}
	plain, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("inflating: %v", err)
	}

	return string(plain)
}

// compressibleText returns a body that compression makes much smaller.
func compressibleText(size int) string {
	var text strings.Builder
	for i := 0; text.Len() < size; i++ {
		fmt.Fprintf(&text, "line %d of a text that compresses well\n", i)
	}

	return text.String()
}

// TestSlicesAndCompression covers the response that is transformed between
// the upstream and the cache: its ranges are not those of what the client
// gets, so it is stored whole, beside the slices of the variant that is
// left as it is.
func TestSlicesAndCompression(t *testing.T) {
	content := compressibleText(40_000)

	// What a handler that compresses on the way does to the response to a
	// request for a range, when it does not know better: the body is no
	// longer the range the headers say, and its length is not told.
	up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.Header().Set("Etag", `"text"`)
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			http.ServeContent(w, r, "", time.Time{}, strings.NewReader(content))
			return
		}

		plain := httptest.NewRecorder()
		http.ServeContent(plain, r, "", time.Time{}, strings.NewReader(content))
		for name, values := range plain.Header() {
			if name != "Content-Length" {
				w.Header()[name] = values
			}
		}
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Vary", "Accept-Encoding")
		w.WriteHeader(plain.Code)

		zw := gzip.NewWriter(w)
		_, _ = zw.Write(plain.Body.Bytes())
		_ = zw.Close()
	})
	tester := startCaddy(t, t.TempDir(), "ttl 1h\nslice 4Ki", `
		cache
		reverse_proxy `+up.addr())

	for i := range 2 {
		resp, body := get(t, tester, "/text", "Accept-Encoding: gzip")
		if resp.StatusCode != http.StatusOK || resp.Header.Get("Content-Encoding") != "gzip" {
			t.Fatalf("status %d, headers %v", resp.StatusCode, resp.Header)
		}
		expectBody(t, gunzip(t, body), content)
		if hit := strings.Contains(resp.Header.Get("Cache-Status"), "; hit; "); hit != (i == 1) {
			t.Errorf("request %d: Cache-Status: %s", i, resp.Header.Get("Cache-Status"))
		}

		// A client that does not accept gzip gets slices.
		resp, body = get(t, tester, "/text", "Range: bytes=30000-30099")
		if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Encoding") != "" {
			t.Errorf("status %d, headers %v", resp.StatusCode, resp.Header)
		}
		expectBody(t, body, content[30000:30100])
		if hit := strings.Contains(resp.Header.Get("Cache-Status"), "; hit; "); hit != (i == 1) {
			t.Errorf("request %d: Cache-Status: %s", i, resp.Header.Get("Cache-Status"))
		}
	}

	// One request to find out the response is compressed, one for all of
	// it, and one for the slice.
	if n := up.hits.Load(); n != 3 {
		t.Errorf("the upstream got %d requests, want 3", n)
	}
}

// TestSlicesAndEncode covers the encode directive on either side of the
// cache. It leaves the response to a request for a range alone, which is
// all it sees of an upstream that is asked for slices: placed after the
// cache it compresses nothing, and the response is stored in slices as the
// upstream sent it. Placed in front, it compresses what the cache serves.
func TestSlicesAndEncode(t *testing.T) {
	content := compressibleText(40_000)

	for _, test := range []struct {
		name, site string
		compressed bool
	}{
		{"after the cache", "cache\nencode gzip\nreverse_proxy %s", false},
		{"in front of the cache", "route {\nencode gzip\ncache\nreverse_proxy %s\n}", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			up := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Range") == "" {
					t.Errorf("the upstream was asked for the whole response")
				}
				w.Header().Set("Content-Type", "text/plain")
				w.Header().Set("Etag", `"text"`)
				http.ServeContent(w, r, "", time.Time{}, strings.NewReader(content))
			})
			tester := startCaddy(t, t.TempDir(), "ttl 1h\nslice 4Ki", fmt.Sprintf(test.site, up.addr()))

			for i := range 2 {
				resp, body := get(t, tester, "/text", "Accept-Encoding: gzip")
				if resp.StatusCode != http.StatusOK {
					t.Fatalf("status %d, headers %v", resp.StatusCode, resp.Header)
				}
				if compressed := resp.Header.Get("Content-Encoding") == "gzip"; compressed != test.compressed {
					t.Errorf("Content-Encoding: %q, want compressed: %v", resp.Header.Get("Content-Encoding"), test.compressed)
				} else if compressed {
					body = gunzip(t, body)
				}
				expectBody(t, body, content)
				if hit := strings.Contains(resp.Header.Get("Cache-Status"), "; hit; "); hit != (i == 1) {
					t.Errorf("request %d: Cache-Status: %s", i, resp.Header.Get("Cache-Status"))
				}

				// A range is one of the response as the upstream has it,
				// whoever asks.
				resp, body = get(t, tester, "/text", "Accept-Encoding: gzip", "Range: bytes=30000-30099")
				if resp.StatusCode != http.StatusPartialContent || resp.Header.Get("Content-Encoding") != "" {
					t.Errorf("status %d, headers %v", resp.StatusCode, resp.Header)
				}
				expectBody(t, body, content[30000:30100])
			}

			// The ten slices of the response, once each.
			if n := up.hits.Load(); n != 10 {
				t.Errorf("the upstream got %d requests, want 10", n)
			}
		})
	}
}

func TestUncacheableResponseIsNotSliced(t *testing.T) {
	up := newOrigin(t, 40_000)
	up.header.Set("Cache-Control", "no-store")
	content := string(up.content("v1"))
	tester := startCaddy(t, t.TempDir(), "slice 4Ki", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/private"

	// What the cache cannot store, the upstream answers itself.
	for i, detail := range []string{"NO-STORE", "UNCACHEABLE", "UNCACHEABLE"} {
		resp, body := get(t, tester, "/private", "Range: bytes=30000-30099")
		if resp.StatusCode != http.StatusPartialContent {
			t.Errorf("status %d, want 206", resp.StatusCode)
		}
		expectBody(t, body, content[30000:30100])
		expectStatus(t, resp, "Caddy; fwd=uri-miss; detail="+detail+"; key="+key)

		// Finding out takes one more request to the upstream, once.
		if want := int64(i + 2); up.hits.Load() != want {
			t.Errorf("the upstream got %d requests after %d from the client, want %d", up.hits.Load(), i+1, want)
		}
	}

	resp, body := get(t, tester, "/private")
	expectStatus(t, resp, "Caddy; fwd=uri-miss; detail=UNCACHEABLE; key="+key)
	expectBody(t, body, content)
	if st := cacheStats(t); st.Entries != 0 {
		t.Errorf("%d entries, want none", st.Entries)
	}
}

// TestSliceOutlivesItsClient checks that the slice a client was reading
// when it left is stored, and that the ones after it are not fetched.
func TestSliceOutlivesItsClient(t *testing.T) {
	content := sliceContent("left", 40_000)
	up := newUpstream(t, rangeHandler(t, content, func(int) <-chan struct{} {
		later := make(chan struct{})
		time.AfterFunc(400*time.Millisecond, func() { close(later) })

		return later
	}))
	tester := startCaddy(t, t.TempDir(), "slice 4Ki", `
		cache
		reverse_proxy `+up.addr())

	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, testURL+"/video", nil)
	resp, err := tester.Client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	head := make([]byte, 10)
	if _, err := io.ReadFull(resp.Body, head); err != nil || !bytes.Equal(head, content[:10]) {
		t.Fatalf("read %q, %v", head, err)
	}
	cancel()
	_ = resp.Body.Close()

	time.Sleep(800 * time.Millisecond)

	hit, body := get(t, tester, "/video", "Range: bytes=0-4095")
	expectHit(t, hit, "GET-http-localhost:9080-/video", 120)
	expectBody(t, body, string(content[:4096]))
	if n := up.hits.Load(); n != 1 {
		t.Errorf("the upstream got %d requests, want 1", n)
	}
}

func TestSlicesWithMinUses(t *testing.T) {
	up := newOrigin(t, 40_000)
	content := string(up.content("v1"))
	tester := startCaddy(t, t.TempDir(), "ttl 1h\nslice 4Ki\nmin_uses 2", `
		cache
		reverse_proxy `+up.addr())

	// Requested once, the slices are in memory only. The client has the last
	// one before the cache is done storing it.
	_, body := get(t, tester, "/video")
	expectBody(t, body, content)
	waitFor(t, "the last slice to be stored", func() bool { return cacheStats(t).Stored == 10 })
	if st := cacheStats(t); st.TransientEntries != 11 || st.DiskBytes != 0 {
		t.Errorf("unexpected stats after one request: %+v", st)
	}

	// Requested again, they are served from there, then written to disk.
	resp, body := get(t, tester, "/video")
	expectBody(t, body, content)
	if !strings.Contains(resp.Header.Get("Cache-Status"), "detail=MEMORY") {
		t.Errorf("Cache-Status: %s", resp.Header.Get("Cache-Status"))
	}
	waitFor(t, "the slices to be written to disk", func() bool { return cacheStats(t).TransientEntries == 0 })
	if n := up.hits.Load(); n != 10 {
		t.Errorf("the upstream got %d requests, want 10", n)
	}
}

// TestSlicesOfARewrittenRequest checks that each slice is asked of the
// upstream as the first one was: the request is rewritten once per fetch,
// starting over from what the client sent.
func TestSlicesOfARewrittenRequest(t *testing.T) {
	up := newOrigin(t, 40_000)
	content := string(up.content("v1"))
	up.before = func(r *http.Request) {
		if r.URL.Path != "/real/video" {
			t.Errorf("the upstream was asked for %s", r.URL.Path)
		}
	}
	tester := startCaddy(t, t.TempDir(), "slice 4Ki", `
		cache
		rewrite * /real{path}
		reverse_proxy `+up.addr())

	for range 2 {
		resp, body := get(t, tester, "/video", "Range: bytes=5000-34999")
		if resp.StatusCode != http.StatusPartialContent {
			t.Errorf("status %d", resp.StatusCode)
		}
		expectBody(t, body, content[5000:35000])
	}
	if n := up.hits.Load(); n != 8 {
		t.Errorf("the upstream got %d requests, want one for each of the 8 slices read", n)
	}
}

// TestSeveralRangesAndAClientThatLeaves covers what the standard library
// does for a request for several ranges: it has them read by a goroutine of
// its own, which is still at it when the client is gone and the request
// over.
func TestSeveralRangesAndAClientThatLeaves(t *testing.T) {
	const size = 2_000_000
	up := newOrigin(t, size)
	content := up.content("v1")
	tester := startCaddy(t, t.TempDir(), "slice 64Ki\nmin_uses 2", `
		cache
		reverse_proxy `+up.addr())

	// Twice each, so that the slices are read as they download, then from
	// memory, then from disk.
	for i := range 8 {
		ctx, cancel := context.WithCancel(context.Background())
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/video-%d", testURL, i%2), nil)
		req.Header.Set("Range", "bytes=0-999999,1000000-1999999")
		resp, err := tester.Client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != http.StatusPartialContent {
			t.Errorf("status %d", resp.StatusCode)
		}
		if _, err := io.ReadFull(resp.Body, make([]byte, 50_000)); err != nil {
			t.Errorf("reading the beginning: %v", err)
		}
		cancel()
		_ = resp.Body.Close()
	}

	// Nothing of it is in the way of the requests that follow.
	for i := range 2 {
		resp, body := get(t, tester, fmt.Sprintf("/video-%d", i), "Range: bytes=10-19,1900000-1900009")
		if resp.StatusCode != http.StatusPartialContent {
			t.Errorf("status %d", resp.StatusCode)
		}
		if !strings.Contains(body, string(content[10:20])) || !strings.Contains(body, string(content[1_900_000:1_900_010])) {
			t.Error("a range is missing from the body")
		}
	}
}

// TestResponseThatOutgrowsItsSlice covers the response that was stored
// whole and is now too large for that, where it varies: the slices are then
// stored beside what was there, which is not to be found before them.
// TestPanicWhileFetchingASlice covers a handler after the cache that panics
// while a slice is fetched for a request for several ranges, whose body
// http.ServeContent reads from a goroutine of its own. The panic is that of
// the request, as it is without a cache: the client is cut short and the
// server carries on, where a panic in that goroutine would end the process.
func TestPanicWhileFetchingASlice(t *testing.T) {
	up := newOrigin(t, 40_000)
	content := up.content("v1")
	tester := startCaddy(t, t.TempDir(), "ttl 1h\nslice 4Ki", `
		route {
			cache
			test_panic bytes=8192-12287
			reverse_proxy `+up.addr()+`
		}`)

	req, err := http.NewRequest(http.MethodGet, testURL+"/file", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Range", "bytes=0-9,9000-9009")
	resp, err := tester.Client.Do(req)
	if err == nil {
		_, err = io.ReadAll(resp.Body)
		_ = resp.Body.Close()
	}
	if err == nil {
		t.Error("a response whose second range could not be fetched was not cut short")
	}

	// The slices that can be fetched still are.
	resp, body := get(t, tester, "/file", "Range: bytes=20000-20009")
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("status %d", resp.StatusCode)
	}
	expectBody(t, body, string(content[20000:20010]))
}

func TestResponseThatOutgrowsItsSlice(t *testing.T) {
	up := newOrigin(t, 1000)
	up.header.Set("Vary", "X-Kind")
	tester := startCaddy(t, t.TempDir(), "ttl 1s\nstale 1h\nslice 4Ki", `
		cache
		reverse_proxy `+up.addr())
	const key = "GET-http-localhost:9080-/growing"

	_, body := get(t, tester, "/growing", "X-Kind: a")
	expectBody(t, body, string(up.content("v1")))

	up.size.Store(40_000)
	up.version.Store("v2")
	content := string(up.content("v2"))
	time.Sleep(1100 * time.Millisecond)

	resp, body := get(t, tester, "/growing", "X-Kind: a", "Range: bytes=100-199")
	expectStatus(t, resp, "Caddy; fwd=stale; stored; key="+key)
	expectBody(t, body, content[100:200])
	time.Sleep(1100 * time.Millisecond)

	// The slice is what there is to confirm now.
	resp, body = get(t, tester, "/growing", "X-Kind: a", "Range: bytes=100-199")
	expectStatus(t, resp, "Caddy; fwd=stale; fwd-status=304; detail=REVALIDATED; key="+key)
	expectBody(t, body, content[100:200])
	if n := up.hits.Load(); n != 3 {
		t.Errorf("the upstream got %d requests, want 3", n)
	}
}

// TestHeadersSetLateFromTheRequest covers the handler in front of the cache
// that looks at the request when the response headers are written, which is
// while the slice being sent may still be fetched, and the request with it
// changed into the one for that slice.
func TestHeadersSetLateFromTheRequest(t *testing.T) {
	up := newOrigin(t, 40_000)
	content := string(up.content("v1"))
	tester := startCaddy(t, t.TempDir(), "slice 4Ki", `
		header >X-Seen "{http.request.header.X-Tag} {http.request.uri.path}"
		cache
		rewrite * /real{path}
		reverse_proxy `+up.addr())

	for i := range 20 {
		path := fmt.Sprintf("/video-%d", i)
		resp, body := get(t, tester, path, "X-Tag: tagged", "Range: bytes=5000-5099")
		expectBody(t, body, content[5000:5100])
		// What the handler sees of the request is not the cache's to say:
		// the rewrite may be done or undone. It is to see one or the other.
		if seen := resp.Header.Get("X-Seen"); seen != "tagged "+path && seen != "tagged /real"+path {
			t.Errorf("X-Seen: %q", seen)
		}
	}
}

// TestSlicesUnderConcurrentRanges has many clients read ranges all over a
// response at once, each of which must get exactly what it asked for.
func TestSlicesUnderConcurrentRanges(t *testing.T) {
	const size = 300_000
	up := newOrigin(t, size)
	content := up.content("v1")
	tester := startCaddy(t, t.TempDir(), "slice 16Ki", `
		cache
		reverse_proxy `+up.addr())

	var wg sync.WaitGroup
	for client := range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			rng := rand.New(rand.NewPCG(uint64(client), 1))
			for range 12 {
				first := rng.IntN(size)
				last := min(first+rng.IntN(60_000), size-1)

				req, _ := http.NewRequest(http.MethodGet, testURL+"/video", nil)
				req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", first, last))
				resp, err := tester.Client.Do(req)
				if err != nil {
					t.Errorf("bytes=%d-%d: %v", first, last, err)
					return
				}
				body, err := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if err != nil || resp.StatusCode != http.StatusPartialContent || !bytes.Equal(body, content[first:last+1]) {
					t.Errorf("bytes=%d-%d: status %d, %d bytes, %v", first, last, resp.StatusCode, len(body), err)
				}
			}
		}()
	}
	wg.Wait()

	// Nearly every slice was fetched once: a request can miss a slice just
	// as another one is done storing it.
	if n, slices := up.hits.Load(), int64((size+16383)/16384); n > slices+slices/2 {
		t.Errorf("the upstream got %d requests for %d slices", n, slices)
	}

	resp, body := get(t, tester, "/video")
	if resp.StatusCode != http.StatusOK || !bytes.Equal([]byte(body), content) {
		t.Errorf("the whole response: status %d, %d bytes", resp.StatusCode, len(body))
	}
}

func TestSliceOffInADirective(t *testing.T) {
	up := newOrigin(t, 40_000)
	content := string(up.content("v1"))
	tester := startCaddy(t, t.TempDir(), "slice 4Ki", `
		cache {
			slice off
		}
		reverse_proxy `+up.addr())

	resp, body := get(t, tester, "/video", "Range: bytes=30000-30099")
	if resp.StatusCode != http.StatusPartialContent {
		t.Errorf("status %d", resp.StatusCode)
	}
	expectBody(t, body, content[30000:30100])
	if asked := up.asked(); !slices.Equal(asked, []string{""}) {
		t.Errorf("the upstream was asked for %v, want the whole response", asked)
	}
}

func TestParseContentRange(t *testing.T) {
	type result struct {
		first, last, total int64
		ok                 bool
	}
	cases := map[string]result{
		"bytes 0-4095/40000":      {0, 4095, 40000, true},
		"bytes 36864-39999/40000": {36864, 39999, 40000, true},
		"bytes 0-0/1":             {0, 0, 1, true},
		"bytes 0 - 9 / 10":        {0, 9, 10, true},
		"":                        {},
		"bytes 0-4095/*":          {},
		"bytes */40000":           {},
		"bytes 0-4095":            {},
		"bytes 10-5/40000":        {},
		"bytes 0-40000/40000":     {},
		"bytes -5/40000":          {},
		"items 0-4095/40000":      {},
	}
	for in, want := range cases {
		first, last, total, ok := parseContentRange(in)
		if got := (result{first, last, total, ok}); got != want {
			t.Errorf("parseContentRange(%q) = %+v, want %+v", in, got, want)
		}
	}
}

func TestFirstSlice(t *testing.T) {
	cases := map[string]int64{
		"":                           0,
		"bytes=0-":                   0,
		"bytes=4095-":                0,
		"bytes=4096-":                1,
		"bytes=30000-30099":          7,
		"bytes=30000-30099,0-10":     7,
		"bytes=-100":                 0,
		"bytes=x-":                   0,
		"items=30000-":               0,
		"bytes=9223372036854775807-": 0,
	}
	for in, want := range cases {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		if in != "" {
			r.Header.Set("Range", in)
		}
		if got := firstSlice(r, 4096); got != want {
			t.Errorf("firstSlice(%q) = %d, want %d", in, got, want)
		}
	}

	// A HEAD request reads nothing, wherever its range starts.
	r := httptest.NewRequest(http.MethodHead, "/", nil)
	r.Header.Set("Range", "bytes=30000-")
	if got := firstSlice(r, 4096); got != 0 {
		t.Errorf("firstSlice of a HEAD request = %d, want 0", got)
	}
}
