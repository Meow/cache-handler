package httpcache

import (
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
)

// maxLifetime caps how long a response is considered fresh, whatever the
// upstream says.
const maxLifetime = 10 * 365 * 24 * time.Hour

// directives are the parsed members of a Cache-Control header.
type directives map[string]string

func parseDirectives(values []string) directives {
	d := make(directives)
	for _, line := range values {
		for _, part := range splitUnquoted(line, ',') {
			name, value, _ := strings.Cut(part, "=")
			name = strings.ToLower(strings.TrimSpace(name))
			if name == "" {
				continue
			}
			if _, dup := d[name]; !dup {
				d[name] = strings.Trim(strings.TrimSpace(value), `"`)
			}
		}
	}

	return d
}

// splitUnquoted splits s on sep, except inside double quotes.
func splitUnquoted(s string, sep byte) []string {
	var parts []string

	quoted := false
	start := 0
	for i := 0; i < len(s); i++ {
		switch {
		case s[i] == '\\' && quoted:
			i++
		case s[i] == '"':
			quoted = !quoted
		case s[i] == sep && !quoted:
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}

	return append(parts, s[start:])
}

func (d directives) has(name string) bool {
	_, ok := d[name]

	return ok
}

// seconds returns the duration a directive carries. A value that cannot be
// read counts as zero, the safe side for a freshness lifetime.
func (d directives) seconds(name string) (time.Duration, bool) {
	v, ok := d[name]
	if !ok {
		return 0, false
	}

	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n < 0 {
		return 0, true
	}
	if n > int64(maxLifetime/time.Second) {
		return maxLifetime, true
	}

	return time.Duration(n) * time.Second, true
}

// verdict is the decision taken on a response received from the upstream.
type verdict struct {
	store bool
	// reason says why the response is not stored.
	reason string
	// lifetime is how long the response stays fresh from now on.
	lifetime       time.Duration
	swr, sie       time.Duration
	mustRevalidate bool
	age            time.Duration
	// vary lists the request headers the response depends on.
	vary []string
}

func reject(reason string) verdict {
	return verdict{reason: reason}
}

// cacheableStatus lists the status codes a response may be stored with when
// it states how long it is fresh. The value tells whether the default TTL
// applies when it does not.
var cacheableStatus = map[int]bool{
	http.StatusOK:                   true,
	http.StatusNonAuthoritativeInfo: true,
	http.StatusNoContent:            true,
	http.StatusMultipleChoices:      true,
	http.StatusMovedPermanently:     true,
	http.StatusPermanentRedirect:    true,
	http.StatusFound:                false,
	http.StatusTemporaryRedirect:    false,
	http.StatusNotFound:             false,
	http.StatusMethodNotAllowed:     false,
	http.StatusGone:                 false,
	http.StatusRequestURITooLong:    false,
	http.StatusNotImplemented:       false,
}

// evaluate decides whether and for how long the response to r may be stored.
func (c *config) evaluate(r *http.Request, status int, h http.Header, now time.Time) verdict {
	defaultTTL, known := cacheableStatus[status]
	if c.extraStatus[status] {
		defaultTTL, known = true, true
	}
	if !known {
		return reject("UNCACHEABLE-STATUS")
	}

	var v verdict
	if !c.key.DisableVary {
		var star bool
		if v.vary, star = varyNames(h); star {
			return reject("VARY-STAR")
		}
	}
	if len(h["Set-Cookie"]) > 0 {
		return reject("SET-COOKIE")
	}

	var cc directives
	if !c.ignoreResponse {
		cc = parseDirectives(h.Values("Cache-Control"))
	}
	switch {
	case cc.has("no-store"):
		return reject("NO-STORE")
	case cc.has("private"):
		return reject("PRIVATE")
	}

	// A response to an authenticated request is only shared when it says so
	// or when the credentials are part of what selects it.
	if r.Header.Get("Authorization") != "" &&
		!cc.has("public") && !cc.has("s-maxage") && !cc.has("must-revalidate") &&
		!slices.Contains(v.vary, "authorization") && !slices.Contains(c.keyHeaders, "Authorization") {
		return reject("AUTHORIZATION")
	}

	if d, ok := cc.seconds("s-maxage"); ok {
		v.lifetime = d
	} else if d, ok := cc.seconds("max-age"); ok {
		v.lifetime = d
	} else if expires := h.Get("Expires"); expires != "" && !c.ignoreResponse {
		// An unreadable date means already expired.
		if t, err := http.ParseTime(expires); err == nil {
			date := now
			if d, err := http.ParseTime(h.Get("Date")); err == nil {
				date = d
			}
			v.lifetime = min(max(t.Sub(date), 0), maxLifetime)
		}
	} else if defaultTTL {
		v.lifetime = c.ttl
	} else {
		return reject("UNCACHEABLE-STATUS")
	}

	if age, err := strconv.ParseInt(h.Get("Age"), 10, 64); err == nil && age > 0 {
		v.age = min(time.Duration(age)*time.Second, maxLifetime)
		v.lifetime -= v.age
	}

	if cc.has("no-cache") {
		v.lifetime = 0
		v.mustRevalidate = true
	}
	if cc.has("must-revalidate") || cc.has("proxy-revalidate") {
		v.mustRevalidate = true
	}

	if v.lifetime <= 0 {
		v.lifetime = 0
		// Storing an expired response is only worth it when the upstream
		// can confirm it later without sending it again.
		if status != http.StatusOK || (h.Get("Etag") == "" && h.Get("Last-Modified") == "") {
			return reject("EXPIRED")
		}
	}

	if !v.mustRevalidate {
		v.swr, v.sie = c.stale, c.stale
		if d, ok := cc.seconds("stale-while-revalidate"); ok {
			v.swr = d
		}
		if d, ok := cc.seconds("stale-if-error"); ok {
			v.sie = d
		}
	}

	v.store = true

	return v
}

// varyNames returns the lowercase, sorted names listed by the Vary header
// and whether one of them is the wildcard.
func varyNames(h http.Header) (names []string, star bool) {
	for _, line := range h.Values("Vary") {
		for name := range strings.SplitSeq(line, ",") {
			name = strings.ToLower(strings.TrimSpace(name))
			switch {
			case name == "*":
				return nil, true
			case name != "" && !slices.Contains(names, name):
				names = append(names, name)
			}
		}
	}
	slices.Sort(names)

	return names, false
}

// varyValue returns the value of a request header as it takes part in the
// selection of a response.
func varyValue(h http.Header, name string) string {
	values := h.Values(name)
	if len(values) == 0 {
		return ""
	}

	v := strings.Join(values, ",")
	if name == "accept-encoding" {
		return normalizeAcceptEncoding(v)
	}

	return strings.TrimSpace(v)
}

// normalizeAcceptEncoding reduces an Accept-Encoding header to the sorted
// list of the common codings it accepts. Clients spell the same capabilities
// in many ways, and each spelling would otherwise be stored separately.
func normalizeAcceptEncoding(v string) string {
	var codings []string
	for part := range strings.SplitSeq(v, ",") {
		coding, params, _ := strings.Cut(part, ";")
		coding = strings.ToLower(strings.TrimSpace(coding))
		switch coding {
		case "gzip", "br", "zstd", "deflate":
		default:
			continue
		}

		if q, ok := strings.CutPrefix(strings.ReplaceAll(params, " ", ""), "q="); ok {
			if f, err := strconv.ParseFloat(q, 64); err == nil && f == 0 {
				continue
			}
		}
		if !slices.Contains(codings, coding) {
			codings = append(codings, coding)
		}
	}
	slices.Sort(codings)

	return strings.Join(codings, ",")
}

// buildKey computes the cache key of a request. method overrides the method
// of the request, since a HEAD request is answered from the GET response.
func (c *config) buildKey(r *http.Request, method string) string {
	var b strings.Builder

	if c.key.Template != "" {
		if repl, ok := r.Context().Value(caddy.ReplacerCtxKey).(*caddy.Replacer); ok {
			b.WriteString(repl.ReplaceAll(c.key.Template, ""))
		} else {
			b.WriteString(c.key.Template)
		}
	} else {
		if !c.key.DisableMethod {
			b.WriteString(method)
			b.WriteByte('-')
		}
		if !c.key.DisableScheme {
			if r.TLS != nil {
				b.WriteString("https-")
			} else {
				b.WriteString("http-")
			}
		}
		if !c.key.DisableHost {
			b.WriteString(r.Host)
			b.WriteByte('-')
		}
		b.WriteString(r.URL.Path)
		if !c.key.DisableQuery && r.URL.RawQuery != "" {
			b.WriteByte('?')
			if c.key.SortQuery {
				b.WriteString(sortQuery(r.URL.RawQuery))
			} else {
				b.WriteString(r.URL.RawQuery)
			}
		}
	}

	for _, name := range c.keyHeaders {
		b.WriteByte('-')
		b.WriteString(strings.Join(r.Header.Values(name), ","))
	}

	// A NUL separates the key from what selects a variant of it.
	return strings.ReplaceAll(b.String(), "\x00", "")
}

func sortQuery(raw string) string {
	values, err := url.ParseQuery(raw)
	if err != nil {
		return raw
	}

	// Encode sorts by name.
	return values.Encode()
}

// conditional tells whether the request asks for the response only under a
// condition on its validators.
func conditional(r *http.Request) bool {
	for _, name := range []string{"If-None-Match", "If-Modified-Since", "If-Match", "If-Unmodified-Since"} {
		if r.Header.Get(name) != "" {
			return true
		}
	}

	return false
}

// hopHeaders are the response headers that describe one connection and are
// therefore not stored, along with the ones recomputed when serving.
var hopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
	"Content-Length",
	"Age",
}

// headerDiff returns what a handler changed in the response headers: the
// headers it added or modified with their final values, and the ones it
// removed with no value. Replaying it over the same starting headers gives
// the same response, without storing what the server itself sets on every
// response.
func headerDiff(base, final http.Header) http.Header {
	diff := make(http.Header)
	for name, values := range final {
		if !slices.Equal(values, base[name]) {
			diff[name] = slices.Clone(values)
		}
	}
	for name := range base {
		if _, ok := final[name]; !ok {
			diff[name] = nil
		}
	}

	for _, name := range strings.Split(strings.Join(final.Values("Connection"), ","), ",") {
		if name = strings.TrimSpace(name); name != "" {
			delete(diff, http.CanonicalHeaderKey(name))
		}
	}
	for _, name := range hopHeaders {
		delete(diff, name)
	}

	return diff
}

// ownHeaders returns the headers a handler gave a response itself, leaving
// out the values that were set before it ran. Those come from the handlers
// in front of the cache, which set them on every response, served from the
// cache or not: they say nothing about the response of the upstream.
func ownHeaders(base, final http.Header) http.Header {
	own := make(http.Header, len(final))
	for name, values := range final {
		before := base[name]
		if len(values) >= len(before) && slices.Equal(values[:len(before)], before) {
			values = values[len(before):]
		}
		if len(values) > 0 {
			own[name] = values
		}
	}

	return own
}

// applyHeaders replays a header diff over the headers of a response.
func applyHeaders(dst, diff http.Header) {
	for name, values := range diff {
		if len(values) == 0 {
			delete(dst, name)
		} else {
			// The values are shared by every request served from memory.
			dst[name] = slices.Clone(values)
		}
	}
}

// formatKey renders a cache key as a Cache-Status parameter value.
func formatKey(key string) string {
	token := key != ""
	for i := 0; i < len(key) && token; i++ {
		c := key[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z':
		case i > 0 && (c >= '0' && c <= '9' || strings.IndexByte("!#$%&'*+-.^_`|~:/", c) >= 0):
		default:
			token = false
		}
	}
	if token {
		return key
	}

	var b strings.Builder
	b.WriteByte('"')
	for i := 0; i < len(key); i++ {
		switch c := key[i]; {
		case c == '"' || c == '\\':
			b.WriteByte('\\')
			b.WriteByte(c)
		case c < 0x20 || c > 0x7e:
			b.WriteByte('?')
		default:
			b.WriteByte(c)
		}
	}
	b.WriteByte('"')

	return b.String()
}
