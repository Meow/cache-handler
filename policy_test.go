package httpcache

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
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

	for _, in := range []string{"", "Gi", "-1", "12 parsecs", "1e3", "1.2.3", "99999999999999Ti"} {
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
	for _, in := range []string{`true`, `"lots"`, `1.5`} {
		if err := json.Unmarshal([]byte(in), &decoded.A); err == nil {
			t.Errorf("%s decoded to %d, want an error", in, decoded.A)
		}
	}

	for size, want := range map[Size]string{8 << 30: "8GiB", 3 << 19: "1.5MiB", 512: "512B", 0: "0B", SizeOff: "off"} {
		if got := size.String(); got != want {
			t.Errorf("Size(%d).String() = %q, want %q", int64(size), got, want)
		}
	}
}

// TestSliceIsCheckedAgainstTheInheritedSize covers a slice and the max_size
// it must fit in being set at different levels: while a block is parsed, it
// cannot tell what it inherits.
func TestSliceIsCheckedAgainstTheInheritedSize(t *testing.T) {
	var local, global Options
	if err := parseOptions(caddyfile.NewTestDispenser("cache {\n slice 6Gi\n}"), &local); err != nil {
		t.Fatalf("a slice that fits in the max_size set globally was refused: %v", err)
	}
	if err := parseOptions(caddyfile.NewTestDispenser("cache {\n max_size 100Gi\n}"), &global); err != nil {
		t.Fatal(err)
	}

	if c, err := local.inherit(global).resolve(); err != nil || c.slice != 6<<30 {
		t.Errorf("resolved to %+v, %v", c, err)
	}
	// Without it, the default size applies.
	if _, err := local.inherit(Options{}).resolve(); err == nil || !strings.Contains(err.Error(), "half of max_size") {
		t.Errorf("a slice larger than half of the default max_size was accepted: %v", err)
	}
	// What a block can check on its own, it still refuses as it is parsed.
	if err := parseOptions(caddyfile.NewTestDispenser("cache {\n slice 1k\n}"), new(Options)); err == nil {
		t.Error("a slice below the minimum was accepted")
	}
}

// TestParseOptions covers parsing a whole block, as a directive or as a
// global option. What Caddy makes of it is left to the tests that start one.
func TestParseOptions(t *testing.T) {
	var got Options
	err := parseOptions(caddyfile.NewTestDispenser(`cache {
		path /var/cache/caddy
		max_size 1Gi
		max_memory 0
		max_file_count 1000
		min_uses 1
		slice 0
		max_cacheable_body_bytes 10Mi
		inactive 24h
		ttl 1h
		stale 30s
		lock_timeout 2s
		mode bypass_response
		cache_name Edge
		default_cache_control public, max-age=60
		allowed_additional_status_codes 404 410
		key {
			disable_host
			disable_method
			disable_query
			disable_scheme
			disable_vary
			sort_query
			hide
			headers Accept-Language X-Tenant
			template {http.request.uri.path}
		}
		regex {
			exclude ^/private/
		}
	}`), &got)
	if err != nil {
		t.Fatal(err)
	}

	want := Options{
		Path:                         "/var/cache/caddy",
		MaxSize:                      1 << 30,
		MaxMemory:                    SizeOff,
		MaxFileCount:                 1000,
		MinUses:                      1,
		Slice:                        SizeOff,
		MaxBodyBytes:                 10 << 20,
		Inactive:                     caddy.Duration(24 * time.Hour),
		TTL:                          caddy.Duration(time.Hour),
		Stale:                        caddy.Duration(30 * time.Second),
		LockTimeout:                  caddy.Duration(2 * time.Second),
		Mode:                         "bypass_response",
		CacheName:                    "Edge",
		DefaultCacheControl:          "public, max-age=60",
		AllowedAdditionalStatusCodes: []int{404, 410},
		Key: &KeyOptions{
			DisableHost:   true,
			DisableMethod: true,
			DisableQuery:  true,
			DisableScheme: true,
			DisableVary:   true,
			SortQuery:     true,
			Hide:          true,
			Headers:       []string{"Accept-Language", "X-Tenant"},
			Template:      "{http.request.uri.path}",
		},
		Regex: &RegexOptions{Exclude: "^/private/"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("parsed %+v\nwant   %+v", got, want)
	}

	const count = "wrong argument count"
	for options, want := range map[string]string{
		"redis {\n url 127.0.0.1:6379\n }":       "storage backends were removed",
		"surprise":                               "unsupported cache option",
		"max_size 0":                             "max_size must be positive",
		"max_memory 4k":                          "max_memory must be at least",
		"max_file_count lots":                    "invalid max_file_count",
		"min_uses 0":                             "invalid min_uses",
		"min_uses 5000":                          "min_uses must be between",
		"min_uses 2\n max_memory off":            "max_memory off does not allow",
		"slice lots":                             "invalid size",
		"ttl soon":                               "invalid duration",
		"mode relaxed":                           "unknown cache mode",
		"regex {\n exclude ( \n }":               "regex exclude",
		"ttl":                                    count,
		"path":                                   count,
		"path /a /b":                             count,
		"max_size":                               count,
		"max_memory lots":                        "invalid size",
		"max_file_count":                         count,
		"min_uses":                               count,
		"max_cacheable_body_bytes lots":          "invalid size",
		"max_cacheable_body_bytes off":           "max_cacheable_body_bytes must be positive",
		"inactive soon":                          "invalid duration",
		"stale soon":                             "invalid duration",
		"stale -1s":                              "cannot be negative",
		"lock_timeout soon":                      "invalid duration",
		"mode":                                   count,
		"cache_name":                             count,
		"default_cache_control":                  count,
		"allowed_additional_status_codes":        count,
		"allowed_additional_status_codes teapot": "invalid status code",
		"allowed_additional_status_codes 206":    "status code 206 cannot be cached",
		"key {\n headers\n }":                    count,
		"key {\n template\n }":                   count,
		"key {\n surprise\n }":                   "unsupported key option",
		"regex {\n exclude\n }":                  count,
		"regex {\n surprise\n }":                 "unsupported regex option",
	} {
		err := parseOptions(caddyfile.NewTestDispenser("cache {\n"+options+"\n}"), new(Options))
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%q refused with %v, want an error about %q", options, err, want)
		}
	}

	// The directive itself takes no argument, as a directive or as a global
	// option.
	if err := parseOptions(caddyfile.NewTestDispenser("cache /api/*"), new(Options)); err == nil {
		t.Error("an argument to the directive was accepted")
	}
	if app, err := parseCaddyfileGlobalOption(caddyfile.NewTestDispenser("cache {\n surprise\n}"), nil); err == nil {
		t.Errorf("an unknown global option was accepted: %+v", app)
	}

	// A block that sets nothing inherits everything; one that sets everything
	// inherits nothing but the status codes, which add up.
	if inherited := (Options{}).inherit(want); !reflect.DeepEqual(inherited, want) {
		t.Errorf("inherited %+v\nwant      %+v", inherited, want)
	}
	parent := Options{Path: "/elsewhere", Slice: 1 << 20, Mode: "strict", AllowedAdditionalStatusCodes: []int{418}}
	want.AllowedAdditionalStatusCodes = []int{418, 404, 410}
	if inherited := got.inherit(parent); !reflect.DeepEqual(inherited, want) {
		t.Errorf("inherited %+v\nwant      %+v", inherited, want)
	}
}

// TestModeOption checks what each mode makes of the Cache-Control directives.
func TestModeOption(t *testing.T) {
	for mode, want := range map[string][2]bool{
		"":                {false, false},
		"bypass_request":  {false, false},
		"strict":          {true, false},
		"bypass":          {false, true},
		"bypass_response": {false, true},
	} {
		c, err := Options{Mode: mode}.resolve()
		if err != nil || c.strict != want[0] || c.ignoreResponse != want[1] {
			t.Errorf("mode %q: requests honored %v, responses ignored %v, %v", mode, c.strict, c.ignoreResponse, err)
		}
	}
}

// TestInvalidOptions covers what a JSON configuration can ask for that the
// Caddyfile has no way to spell.
func TestInvalidOptions(t *testing.T) {
	for want, o := range map[string]Options{
		"max_size must be positive":         {MaxSize: -1},
		"max_file_count must be positive":   {MaxFileCount: -1},
		"cannot be negative":                {TTL: caddy.Duration(-time.Second)},
		"status code 304 cannot be cached":  {AllowedAdditionalStatusCodes: []int{304}},
		"status code 1000 cannot be cached": {AllowedAdditionalStatusCodes: []int{1000}},
	} {
		if c, err := o.resolve(); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%+v resolved to %+v, %v; want an error about %q", o, c, err, want)
		}
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

	// A quoted value may hold a quote, and a list empty members.
	d = parseDirectives([]string{`, ext="a \" b, no-store" ,, max-age=99999999999,`})
	if len(d) != 2 || !d.has("ext") || d.has("no-store") {
		t.Errorf("unexpected directives %v", d)
	}
	// No lifetime is longer than the longest one.
	if v, ok := d.seconds("max-age"); !ok || v != maxLifetime {
		t.Errorf("max-age = %v, %v", v, ok)
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
	strict := base
	strict.Mode = "strict"
	strictKeyed := keyed
	strictKeyed.Mode = "strict"
	httpDate := func(t time.Time) string { return t.UTC().Format(http.TimeFormat) }

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
		{name: "absurd age", status: 200, header: http.Header{"Cache-Control": {"max-age=60"}, "Age": {"13835058055"}}, reason: "EXPIRED"},
		// RFC 9111, section 4.2.3: the age is at least the time since the Date.
		{name: "date shortens", status: 200, header: http.Header{"Cache-Control": {"max-age=60"}, "Date": {httpDate(now.Add(-20 * time.Second))}}, lifetime: 40 * time.Second, check: func(t *testing.T, v verdict) {
			if v.age != 20*time.Second {
				t.Errorf("age %v, want 20s", v.age)
			}
		}},
		{name: "date shortens the default ttl", status: 200, header: http.Header{"Date": {httpDate(now.Add(-time.Minute))}}, lifetime: 59 * time.Minute},
		{name: "age beats an earlier date", status: 200, header: http.Header{"Cache-Control": {"max-age=60"}, "Age": {"30"}, "Date": {httpDate(now.Add(-20 * time.Second))}}, lifetime: 30 * time.Second},
		{name: "date beats a smaller age", status: 200, header: http.Header{"Cache-Control": {"max-age=60"}, "Age": {"10"}, "Date": {httpDate(now.Add(-20 * time.Second))}}, lifetime: 40 * time.Second},
		{name: "date in the future", status: 200, header: http.Header{"Cache-Control": {"max-age=60"}, "Date": {httpDate(now.Add(time.Hour))}}, lifetime: time.Minute},
		{name: "expires counts from now", status: 200, header: http.Header{
			"Date":    {httpDate(now.Add(-time.Minute))},
			"Expires": {httpDate(now.Add(5 * time.Minute))},
		}, lifetime: 5 * time.Minute},
		{name: "bypass ignores the date", c: resolve(bypass), status: 200, header: http.Header{"Date": {httpDate(now.Add(-time.Minute))}}, lifetime: time.Hour},
		{name: "must-understand", status: 200, header: http.Header{"Cache-Control": {"must-understand, no-store, max-age=60"}}, lifetime: time.Minute},
		{name: "trailers", status: 200, header: http.Header{"Trailer": {"X-Checksum"}}, reason: "TRAILER"},
		{name: "event stream", status: 200, header: http.Header{"Content-Type": {"text/event-stream; charset=utf-8"}}, reason: "EVENT-STREAM"},
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
		{name: "authorization in vary", r: authenticated, status: 200, header: http.Header{"Vary": {"Authorization"}}, lifetime: time.Hour},
		// RFC 9111, section 3.5 allows no exception but response directives.
		{name: "strict authorization in key", c: resolve(strictKeyed), r: authenticated, status: 200, reason: "AUTHORIZATION"},
		{name: "strict authorization in vary", c: resolve(strict), r: authenticated, status: 200, header: http.Header{"Vary": {"Authorization"}}, reason: "AUTHORIZATION"},
		{name: "strict authorization s-maxage", c: resolve(strict), r: authenticated, status: 200, header: http.Header{"Cache-Control": {"s-maxage=60"}}, lifetime: time.Minute},
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
		"*":                         "*",
		"gzip, *;q=0.1":             "*,gzip",
		"gzip, gzip, x-custom":      "gzip",
		"br; q=0.0, gzip; q=0.5":    "gzip",
	}
	for in, want := range tests {
		if got := normalizeAcceptEncoding(in); got != want {
			t.Errorf("normalizeAcceptEncoding(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestStaleUsable checks when a response past its freshness may still be
// served.
func TestStaleUsable(t *testing.T) {
	now := time.Now()
	stale := &record{fresh: now.Add(-time.Second).UnixMilli(), swr: 60}
	fresh := &record{fresh: now.Add(time.Minute).UnixMilli(), swr: 60}

	plain := &exchange{}
	if !plain.staleUsable(stale, now, stale.swr) {
		t.Error("a stale response within its window was refused")
	}
	if plain.staleUsable(stale, now, 0) {
		t.Error("a stale response was accepted without a window")
	}
	if plain.staleUsable(&record{fresh: stale.fresh, swr: 60, flags: flagMustRevalidate}, now, 60) {
		t.Error("a must-revalidate response was accepted stale")
	}

	// In strict mode a request with terms of its own is never served stale.
	for _, directive := range []string{"no-cache", "max-age=0", "min-fresh=600"} {
		strict := &exchange{reqCC: parseDirectives([]string{directive})}
		for name, rec := range map[string]*record{"stale": stale, "fresh": fresh} {
			if strict.staleUsable(rec, now, rec.swr) {
				t.Errorf("a %s response was served in place of an update to a request with %s", name, directive)
			}
		}
	}
}

// TestUsable checks when a stored response answers a request as it is.
func TestUsable(t *testing.T) {
	now := time.Now()
	fresh := &record{stored: now.Add(-10 * time.Second).UnixMilli(), fresh: now.Add(time.Minute).UnixMilli()}
	stale := &record{stored: now.Add(-time.Minute).UnixMilli(), fresh: now.Add(-10 * time.Second).UnixMilli()}
	strictly := &record{stored: stale.stored, fresh: stale.fresh, flags: flagMustRevalidate}

	tests := []struct {
		directives string
		rec        *record
		want       bool
	}{
		{"", fresh, true},
		{"", stale, false},
		{"no-cache", fresh, false},
		{"max-age=5", fresh, false},
		{"max-age=60", fresh, true},
		{"min-fresh=120", fresh, false},
		{"min-fresh=30", fresh, true},
		{"max-stale", stale, true},
		{"max-stale=60", stale, true},
		{"max-stale=5", stale, false},
		{"max-stale", strictly, false},
		{"max-stale=60, max-age=30", stale, false},
	}
	for _, tt := range tests {
		x := &exchange{c: &config{strict: true}, reqCC: parseDirectives([]string{tt.directives})}
		if got := x.usable(tt.rec, now, nil); got != tt.want {
			t.Errorf("a request with %q was told %v of a response fresh for %ds",
				tt.directives, got, (tt.rec.fresh-now.UnixMilli())/1000)
		}
	}

	// A response another request fetched while this one waited for it is as
	// current as it gets, whatever its freshness.
	x := &exchange{c: new(config), start: now.Add(-2 * time.Minute)}
	if !x.usable(stale, now, &flight{stored: true}) {
		t.Error("the response a request waited for was refused")
	}
	if x.usable(stale, now, &flight{}) {
		t.Error("a stale response was accepted after a fetch that stored nothing")
	}
	x.start = now.Add(-30 * time.Second)
	if x.usable(stale, now, &flight{stored: true}) {
		t.Error("a stale response older than the request was taken for the one it waited for")
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
		{"bytes=5", 0, 0, false},
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
		{KeyOptions{Headers: []string{"x-tenant"}}, `GET-http-example.com-/a/b?z=1&a=2-X-Tenant="blue"`},
		{KeyOptions{Headers: []string{"x-tenant", "x-missing"}}, `GET-http-example.com-/a/b?z=1&a=2-X-Tenant="blue"-X-Missing=""`},
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

	// A query that cannot be parsed is kept as it is.
	c, err := Options{Key: &KeyOptions{SortQuery: true}}.resolve()
	if err != nil {
		t.Fatal(err)
	}
	odd := httptest.NewRequest(http.MethodGet, "http://example.com/a?z=%zz&a=2", nil)
	if got, want := c.buildKey(odd, http.MethodGet), "GET-http-example.com-/a?z=%zz&a=2"; got != want {
		t.Errorf("key %q, want %q", got, want)
	}

	secure := httptest.NewRequest(http.MethodGet, "https://example.com/a", nil)
	secure.TLS = new(tls.ConnectionState)
	if got, want := c.buildKey(secure, http.MethodGet), "GET-https-example.com-/a"; got != want {
		t.Errorf("key %q, want %q", got, want)
	}

	// A template is filled in by Caddy.
	if c, err = (Options{Key: &KeyOptions{Template: "{tenant}{missing}/page"}}).resolve(); err != nil {
		t.Fatal(err)
	}
	repl := caddy.NewReplacer()
	repl.Set("tenant", "blue")
	templated := r.WithContext(context.WithValue(r.Context(), caddy.ReplacerCtxKey, repl))
	if got, want := c.buildKey(templated, http.MethodGet), "blue/page"; got != want {
		t.Errorf("key %q, want %q", got, want)
	}

	// Without Caddy's replacer, a template is the key.
	if c, err = (Options{Key: &KeyOptions{Template: "{http.request.uri.path}"}}).resolve(); err != nil {
		t.Fatal(err)
	}
	if got, want := c.buildKey(r, http.MethodGet), "{http.request.uri.path}"; got != want {
		t.Errorf("key %q, want %q", got, want)
	}
}

// TestKeysCannotBeForged checks that a request cannot be given the key of
// another one by moving text between its URL and a header that is part of
// the key.
func TestKeysCannotBeForged(t *testing.T) {
	c, err := Options{Key: &KeyOptions{Headers: []string{"Accept-Language", "X-Tenant"}}}.resolve()
	if err != nil {
		t.Fatal(err)
	}

	request := func(target string, headers ...string) string {
		r := httptest.NewRequest(http.MethodGet, target, nil)
		for i := 0; i < len(headers); i += 2 {
			r.Header.Set(headers[i], headers[i+1])
		}
		return c.buildKey(r, http.MethodGet)
	}

	keys := map[string]string{}
	for name, key := range map[string]string{
		"plain":             request("http://example.com/page?q=1", "Accept-Language", "en-US"),
		"moved to the url":  request("http://example.com/page?q=1-en", "Accept-Language", "US"),
		"quoted in the url": request(`http://example.com/page?q=1-Accept-Language="en-US"`, "Accept-Language", ""),
		"moved to a header": request("http://example.com/page?q=1", "Accept-Language", `en-US"-X-Tenant="a`),
		"other header":      request("http://example.com/page?q=1", "Accept-Language", "en-US", "X-Tenant", "a"),
		"encoded slash":     request("http://example.com/page%2Fq", "Accept-Language", "en-US"),
		"slash":             request("http://example.com/page/q", "Accept-Language", "en-US"),
		"encoded nul":       request("http://example.com/page%00", "Accept-Language", "en-US"),
	} {
		if other, dup := keys[key]; dup {
			t.Errorf("%q and %q share the key %s", name, other, key)
		}
		keys[key] = name
		if strings.IndexByte(key, 0) >= 0 {
			t.Errorf("%q: the key contains a NUL", name)
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

// TestReadRecordRejects covers files whose checksum is right but whose
// content is not, which random damage never produces, and files that cannot
// be read at all.
func TestReadRecordRejects(t *testing.T) {
	// file returns the head of a cache file with the given meta block.
	file := func(meta []byte) []byte {
		head := make([]byte, headerSize, headerSize+1+len(meta))
		copy(head, fileMagic)
		binary.LittleEndian.PutUint32(head[36:], 1)
		binary.LittleEndian.PutUint32(head[40:], uint32(len(meta)))
		head = append(append(head, 'k'), meta...)
		finalizeHead(head, 0)

		return head
	}
	read := func(file []byte) (*record, error) {
		return readRecord(bytes.NewReader(file), int64(len(file)))
	}

	// A status, no vary specification and one header with one value.
	valid := appendString(appendString([]byte{200, 1, 0, 1}, "A"), "")
	valid = appendString(append(valid[:len(valid)-1], 1), "b")
	if rec, err := read(file(valid)); err != nil || rec.status != 200 || rec.header.Get("A") != "b" {
		t.Fatalf("read %+v, %v", rec, err)
	}

	for name, meta := range map[string][]byte{
		"empty":                    nil,
		"unfinished number":        {0x80},
		"string longer than meta":  {200, 1, 5, 'a'},
		"missing header":           append(bytes.Clone(valid[:3]), 2, 1, 'A', 0),
		"missing value":            valid[:len(valid)-2],
		"more than it says":        append(bytes.Clone(valid), 0),
		"number that never ends":   bytes.Repeat([]byte{0xff}, 12),
		"values without a header":  {200, 1, 0, 1},
		"vary longer than a block": append([]byte{200, 1}, bytes.Repeat([]byte{0xff}, 9)...),
	} {
		if rec, err := read(file(meta)); err != errCorrupt {
			t.Errorf("%s: read %+v, %v; want it found corrupt", name, rec, err)
		}
	}

	// A file shorter than it was said to be, in its first block and after.
	large := &record{key: "k", status: 200, header: http.Header{"X-Large": {string(bodyFor("large", 10_000))}}}
	head, err := encodeHead(large)
	if err != nil {
		t.Fatal(err)
	}
	finalizeHead(head, 0)
	for _, length := range []int{100, 5000} {
		if rec, err := readRecord(bytes.NewReader(head[:length]), int64(len(head))); err == nil {
			t.Errorf("read %+v from the first %d bytes of a file", rec, length)
		}
	}
}

func TestEncodeHeadLimits(t *testing.T) {
	for name, rec := range map[string]*record{
		"no key":        {status: 200},
		"long key":      {key: strings.Repeat("k", maxKeyLen+1), status: 200},
		"large headers": {key: "k", status: 200, header: http.Header{"X-Large": {strings.Repeat("v", maxMetaLen)}}},
	} {
		if _, err := encodeHead(rec); err == nil {
			t.Errorf("%s: encoded", name)
		}
	}
	if _, err := encodeHead(&record{key: strings.Repeat("k", maxKeyLen), status: 200}); err != nil {
		t.Errorf("the longest key was refused: %v", err)
	}
}

func TestParseID(t *testing.T) {
	id := makeID("key")
	if got, ok := parseID(id.String()); !ok || got != id {
		t.Errorf("parseID(%s) = %s, %v", id, got, ok)
	}
	for _, name := range []string{"", "w-123456", id.String()[1:], id.String() + "00", strings.Repeat("zz", len(id))} {
		if _, ok := parseID(name); ok {
			t.Errorf("%q passed for the name of a cache file", name)
		}
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
