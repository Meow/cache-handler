package httpcache

import (
	"bytes"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
)

func TestParseSize(t *testing.T) {
	valid := map[string]Size{
		"0":      0,
		"4096":   4096,
		"8Gi":    8 << 30,
		"25Gi":   25 << 30,
		"25000m": 25000 << 20,
		"512MiB": 512 << 20,
		"1.5g":   3 << 29,
		"10k":    10 << 10,
		"500MB":  500e6,
		"2 TiB":  2 << 40,
		"off":    SizeOff,
	}
	for in, want := range valid {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}

	for _, in := range []string{"", "Gi", "-1", "12 parsecs", "1e3", "99999999999999Ti"} {
		if got, err := ParseSize(in); err == nil {
			t.Errorf("ParseSize(%q) = %d, want an error", in, got)
		}
	}

	var decoded struct {
		A, B Size
	}
	if err := json.Unmarshal([]byte(`{"A": 1024, "B": "8Gi"}`), &decoded); err != nil || decoded.A != 1024 || decoded.B != 8<<30 {
		t.Errorf("decoded %+v, %v", decoded, err)
	}
	if got := Size(8 << 30).String(); got != "8GiB" {
		t.Errorf("String() = %q", got)
	}
}

func TestParseDirectives(t *testing.T) {
	d := parseDirectives([]string{`Public, max-age=60, no-cache="Set-Cookie, X-Other"`, `s-maxage = "30" , stale-if-error=bogus`})

	if !d.has("public") || !d.has("no-cache") || d.has("set-cookie") || d.has("x-other") {
		t.Errorf("unexpected directives %v", d)
	}
	if v, ok := d.seconds("max-age"); !ok || v != time.Minute {
		t.Errorf("max-age = %v, %v", v, ok)
	}
	if v, ok := d.seconds("s-maxage"); !ok || v != 30*time.Second {
		t.Errorf("s-maxage = %v, %v", v, ok)
	}
	// What cannot be read counts as zero rather than as absent.
	if v, ok := d.seconds("stale-if-error"); !ok || v != 0 {
		t.Errorf("stale-if-error = %v, %v", v, ok)
	}
	if _, ok := d.seconds("min-fresh"); ok {
		t.Error("min-fresh reported present")
	}
}

func TestEvaluate(t *testing.T) {
	now := time.Now()
	get := httptest.NewRequest(http.MethodGet, "/", nil)
	authenticated := httptest.NewRequest(http.MethodGet, "/", nil)
	authenticated.Header.Set("Authorization", "Bearer token")

	base := Options{TTL: caddy.Duration(time.Hour), Stale: caddy.Duration(time.Minute)}
	resolve := func(o Options) *config {
		c, err := o.resolve()
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	standard := resolve(base)

	bypass := base
	bypass.Mode = "bypass_response"
	extra := base
	extra.AllowedAdditionalStatusCodes = []int{404}
	noVary := base
	noVary.Key = &KeyOptions{DisableVary: true}
	keyed := base
	keyed.Key = &KeyOptions{Headers: []string{"authorization"}}

	tests := []struct {
		name     string
		c        *config
		r        *http.Request
		status   int
		header   http.Header
		reason   string
		lifetime time.Duration
		check    func(*testing.T, verdict)
	}{
		{name: "default ttl", status: 200, lifetime: time.Hour},
		{name: "max-age", status: 200, header: http.Header{"Cache-Control": {"max-age=60"}}, lifetime: time.Minute},
		{name: "s-maxage wins", status: 200, header: http.Header{"Cache-Control": {"max-age=60, s-maxage=10"}}, lifetime: 10 * time.Second},
		{name: "expires", status: 200, header: http.Header{
			"Date":    {now.UTC().Format(http.TimeFormat)},
			"Expires": {now.Add(5 * time.Minute).UTC().Format(http.TimeFormat)},
		}, lifetime: 5 * time.Minute},
		{name: "age shortens", status: 200, header: http.Header{"Cache-Control": {"max-age=60"}, "Age": {"20"}}, lifetime: 40 * time.Second},
		{name: "no-store", status: 200, header: http.Header{"Cache-Control": {"no-store"}}, reason: "NO-STORE"},
		{name: "private", status: 200, header: http.Header{"Cache-Control": {"private, max-age=60"}}, reason: "PRIVATE"},
		{name: "set-cookie", status: 200, header: http.Header{"Set-Cookie": {"a=b"}}, reason: "SET-COOKIE"},
		{name: "vary star", status: 200, header: http.Header{"Vary": {"*"}}, reason: "VARY-STAR"},
		{name: "vary star ignored", c: resolve(noVary), status: 200, header: http.Header{"Vary": {"*"}}, lifetime: time.Hour},
		{name: "vary", status: 200, header: http.Header{"Vary": {"Origin, accept-encoding", "Origin"}}, lifetime: time.Hour, check: func(t *testing.T, v verdict) {
			if !reflect.DeepEqual(v.vary, []string{"accept-encoding", "origin"}) {
				t.Errorf("vary = %v", v.vary)
			}
		}},
		{name: "500", status: 500, reason: "UNCACHEABLE-STATUS"},
		{name: "206", status: 206, header: http.Header{"Cache-Control": {"max-age=60"}}, reason: "UNCACHEABLE-STATUS"},
		{name: "404 without freshness", status: 404, reason: "UNCACHEABLE-STATUS"},
		{name: "404 with freshness", status: 404, header: http.Header{"Cache-Control": {"max-age=60"}}, lifetime: time.Minute},
		{name: "404 allowed", c: resolve(extra), status: 404, lifetime: time.Hour},
		{name: "301", status: 301, lifetime: time.Hour},
		{name: "expired without validator", status: 200, header: http.Header{"Cache-Control": {"max-age=0"}}, reason: "EXPIRED"},
		{name: "expired with validator", status: 200, header: http.Header{"Cache-Control": {"max-age=0"}, "Etag": {`"v1"`}}, lifetime: 0},
		{name: "unreadable expires", status: 200, header: http.Header{"Expires": {"0"}}, reason: "EXPIRED"},
		{name: "no-cache", status: 200, header: http.Header{"Cache-Control": {"no-cache"}, "Last-Modified": {"Mon, 02 Jan 2006 15:04:05 GMT"}}, lifetime: 0, check: func(t *testing.T, v verdict) {
			if !v.mustRevalidate || v.swr != 0 || v.sie != 0 {
				t.Errorf("no-cache response may be served stale: %+v", v)
			}
		}},
		{name: "stale windows", status: 200, lifetime: time.Hour, check: func(t *testing.T, v verdict) {
			if v.swr != time.Minute || v.sie != time.Minute {
				t.Errorf("swr %v, sie %v; want the configured stale duration", v.swr, v.sie)
			}
		}},
		{name: "stale windows from the response", status: 200, header: http.Header{"Cache-Control": {"max-age=60, stale-while-revalidate=5, stale-if-error=600"}}, lifetime: time.Minute, check: func(t *testing.T, v verdict) {
			if v.swr != 5*time.Second || v.sie != 10*time.Minute {
				t.Errorf("swr %v, sie %v", v.swr, v.sie)
			}
		}},
		{name: "must-revalidate", status: 200, header: http.Header{"Cache-Control": {"max-age=60, must-revalidate"}}, lifetime: time.Minute, check: func(t *testing.T, v verdict) {
			if !v.mustRevalidate || v.swr != 0 || v.sie != 0 {
				t.Errorf("must-revalidate response may be served stale: %+v", v)
			}
		}},
		{name: "authorization", r: authenticated, status: 200, reason: "AUTHORIZATION"},
		{name: "authorization public", r: authenticated, status: 200, header: http.Header{"Cache-Control": {"public"}}, lifetime: time.Hour},
		{name: "authorization in key", c: resolve(keyed), r: authenticated, status: 200, lifetime: time.Hour},
		{name: "bypass ignores no-store", c: resolve(bypass), status: 200, header: http.Header{"Cache-Control": {"no-store, max-age=5"}}, lifetime: time.Hour},
		{name: "bypass keeps cookies out", c: resolve(bypass), status: 200, header: http.Header{"Set-Cookie": {"a=b"}}, reason: "SET-COOKIE"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, r, header := tt.c, tt.r, tt.header
			if c == nil {
				c = standard
			}
			if r == nil {
				r = get
			}
			if header == nil {
				header = http.Header{}
			}

			v := c.evaluate(r, tt.status, header, now)
			if tt.reason != "" {
				if v.store || v.reason != tt.reason {
					t.Fatalf("got %+v, want it rejected for %s", v, tt.reason)
				}
				return
			}
			if !v.store {
				t.Fatalf("rejected for %s", v.reason)
			}
			if d := v.lifetime - tt.lifetime; d < -time.Second || d > time.Second {
				t.Errorf("lifetime %v, want %v", v.lifetime, tt.lifetime)
			}
			if tt.check != nil {
				tt.check(t, v)
			}
		})
	}
}

func TestNormalizeAcceptEncoding(t *testing.T) {
	tests := map[string]string{
		"gzip, deflate, br, zstd":   "br,deflate,gzip,zstd",
		"zstd,br ,GZIP, deflate":    "br,deflate,gzip,zstd",
		"gzip;q=1.0, br;q=0, *;q=0": "gzip",
		"identity":                  "",
		"gzip, gzip, x-custom":      "gzip",
		"br; q=0.0, gzip; q=0.5":    "gzip",
	}
	for in, want := range tests {
		if got := normalizeAcceptEncoding(in); got != want {
			t.Errorf("normalizeAcceptEncoding(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSingleRange(t *testing.T) {
	tests := []struct {
		header      string
		first, last int64
		ok          bool
	}{
		{"bytes=0-", 0, 99, true},
		{"bytes=10-19", 10, 19, true},
		{"bytes=90-200", 90, 99, true},
		{"bytes=99-", 99, 99, true},
		{"bytes=-10", 90, 99, true},
		{"bytes=-500", 0, 99, true},
		{"bytes= 5 - 6 ", 5, 6, true},
		{"", 0, 0, false},
		{"bytes=100-", 0, 0, false},
		{"bytes=20-10", 0, 0, false},
		{"bytes=0-1,5-6", 0, 0, false},
		{"bytes=-0", 0, 0, false},
		{"bytes=a-b", 0, 0, false},
		{"bytes=-", 0, 0, false},
		{"items=0-5", 0, 0, false},
	}
	for _, tt := range tests {
		first, last, ok := singleRange(tt.header, 100)
		if ok != tt.ok || (ok && (first != tt.first || last != tt.last)) {
			t.Errorf("singleRange(%q) = %d, %d, %v; want %d, %d, %v", tt.header, first, last, ok, tt.first, tt.last, tt.ok)
		}
	}
	if _, _, ok := singleRange("bytes=0-", 0); ok {
		t.Error("a range of an empty body was accepted")
	}
}

func TestBuildKey(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "http://example.com/a/b?z=1&a=2", nil)
	r.Header.Set("X-Tenant", "blue")

	tests := []struct {
		key  KeyOptions
		want string
	}{
		{KeyOptions{}, "GET-http-example.com-/a/b?z=1&a=2"},
		{KeyOptions{SortQuery: true}, "GET-http-example.com-/a/b?a=2&z=1"},
		{KeyOptions{DisableQuery: true}, "GET-http-example.com-/a/b"},
		{KeyOptions{DisableHost: true, DisableScheme: true, DisableMethod: true, DisableQuery: true}, "/a/b"},
		{KeyOptions{Headers: []string{"x-tenant"}}, "GET-http-example.com-/a/b?z=1&a=2-blue"},
	}
	for _, tt := range tests {
		c, err := Options{Key: &tt.key}.resolve()
		if err != nil {
			t.Fatal(err)
		}
		if got := c.buildKey(r, http.MethodGet); got != tt.want {
			t.Errorf("%+v: key %q, want %q", tt.key, got, tt.want)
		}
	}
}

func TestFormatKey(t *testing.T) {
	tests := map[string]string{
		"GET-http-localhost:9080-/a": "GET-http-localhost:9080-/a",
		"GET-http-localhost-/a?b=c":  `"GET-http-localhost-/a?b=c"`,
		"/a":                         `"/a"`,
		`say "hi"\`:                  `"say \"hi\"\\"`,
		"caf\xc3\xa9":                `"caf??"`,
	}
	for in, want := range tests {
		if got := formatKey(in); got != want {
			t.Errorf("formatKey(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestHeaderDiff(t *testing.T) {
	base := http.Header{"Server": {"Caddy"}, "Alt-Svc": {"h3"}, "Vary": {"Origin"}}
	final := http.Header{
		"Alt-Svc":        {"h3"},
		"Vary":           {"Origin", "Accept-Encoding"},
		"Content-Type":   {"text/plain"},
		"Content-Length": {"12"},
		"Connection":     {"close, X-Hop"},
		"X-Hop":          {"1"},
		"Keep-Alive":     {"timeout=5"},
	}

	diff := headerDiff(base, final)
	want := http.Header{
		"Server":       nil,
		"Vary":         {"Origin", "Accept-Encoding"},
		"Content-Type": {"text/plain"},
	}
	if !reflect.DeepEqual(diff, want) {
		t.Fatalf("diff = %v, want %v", diff, want)
	}

	// Replaying the diff over the same starting point gives the response
	// back, minus what belongs to the connection.
	replayed := base.Clone()
	applyHeaders(replayed, diff)
	wantReplayed := http.Header{"Alt-Svc": {"h3"}, "Vary": {"Origin", "Accept-Encoding"}, "Content-Type": {"text/plain"}}
	if !reflect.DeepEqual(replayed, wantReplayed) {
		t.Errorf("replayed = %v, want %v", replayed, wantReplayed)
	}

	// The stored values must not be shared with the response.
	replayed["Vary"][0] = "changed"
	if diff["Vary"][0] != "Origin" {
		t.Error("the stored headers were modified through a response")
	}
}

func TestOwnHeaders(t *testing.T) {
	base := http.Header{"Server": {"Caddy"}, "Cache-Control": {"public, max-age=31536000"}, "Vary": {"Origin"}}
	final := http.Header{
		"Cache-Control": {"public, max-age=31536000", "max-age=60"},
		"Vary":          {"Origin"},
		"Server":        {"nginx"},
		"Etag":          {`"v1"`},
	}

	want := http.Header{"Cache-Control": {"max-age=60"}, "Server": {"nginx"}, "Etag": {`"v1"`}}
	if got := ownHeaders(base, final); !reflect.DeepEqual(got, want) {
		t.Errorf("own = %v, want %v", got, want)
	}
}

func TestRecordRoundTrip(t *testing.T) {
	rec := &record{
		key:    "GET-http-example.com-/a\x00salt\x00accept-encoding=gzip",
		flags:  flagMustRevalidate,
		stored: 1700000000123,
		fresh:  1700000060123,
		swr:    30,
		sie:    600,
		age:    7,
		status: 200,
		header: http.Header{"Content-Type": {"text/plain"}, "Vary": {"A", "B"}, "Server": nil, "X-Empty": {""}},
	}
	body := []byte("the body")

	head, err := encodeHead(rec)
	if err != nil {
		t.Fatal(err)
	}
	finalizeHead(head, int64(len(body)))
	file := append(bytes.Clone(head), body...)

	got, err := readRecord(bytes.NewReader(file), int64(len(file)))
	if err != nil {
		t.Fatal(err)
	}
	// A header to remove comes back with no value.
	rec.header["Server"] = []string{}
	rec.bodyLen, rec.bodyOff = int64(len(body)), int64(len(head))
	if !reflect.DeepEqual(got, rec) {
		t.Errorf("read back %+v\nwant      %+v", got, rec)
	}

	// Any damage to the head is detected, and none makes the reader panic.
	rng := rand.New(rand.NewPCG(1, 2))
	for range 5000 {
		damaged := bytes.Clone(file)
		switch rng.IntN(3) {
		case 0:
			damaged[rng.IntN(len(head))] ^= byte(1 + rng.IntN(255))
		case 1:
			damaged = damaged[:rng.IntN(len(damaged))]
		default:
			for range 8 {
				damaged[rng.IntN(len(head))] = byte(rng.IntN(256))
			}
		}

		// The only bytes not covered by the checksum are reserved ones, so
		// whatever is accepted must read the same.
		if again, err := readRecord(bytes.NewReader(damaged), int64(len(damaged))); err == nil && !reflect.DeepEqual(again, got) {
			t.Fatalf("damaged file accepted: %x", damaged[:min(len(damaged), headerSize)])
		}
	}

	// Headers too large for one read.
	rec.header.Set("X-Large", string(bodyFor("large", 10_000)))
	head, _ = encodeHead(rec)
	finalizeHead(head, 0)
	if got, err := readRecord(bytes.NewReader(head), int64(len(head))); err != nil || got.header.Get("X-Large") != rec.header.Get("X-Large") {
		t.Errorf("large head: %v", err)
	}
}

func FuzzReadRecord(f *testing.F) {
	head, _ := encodeHead(&record{key: "k", status: 200, header: http.Header{"A": {"b"}}})
	finalizeHead(head, 3)
	f.Add(append(head, "abc"...))
	f.Add([]byte(fileMagic))

	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = readRecord(bytes.NewReader(data), int64(len(data)))
	})
}
