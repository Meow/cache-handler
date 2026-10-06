// This file has been modified from the one of caddyserver/cache-handler it
// replaces, see the NOTICE file.

package httpcache

import (
	"fmt"
	"math"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
)

const (
	defaultTTL         = 120 * time.Second
	defaultLockTimeout = 5 * time.Second
	defaultMaxSize     = 10 << 30
	defaultMaxMemory   = 256 << 20
	defaultCacheName   = "Caddy"

	// minMaxMemory is the smallest memory budget accepted: below it the
	// index could not even describe a useful number of files.
	minMaxMemory = 1 << 20
	// maxMinUses is the largest number of requests a response can be made
	// to wait for before it is written to disk.
	maxMinUses = 1000
	// minSlice is the smallest slice accepted: each slice is a file and a
	// request to the upstream.
	minSlice = 4 << 10
)

// KeyOptions tunes how the cache key of a request is built. By default the
// key is METHOD-SCHEME-HOST-PATH?QUERY.
type KeyOptions struct {
	// Leave the host out of the key.
	DisableHost bool `json:"disable_host,omitempty"`
	// Leave the method out of the key.
	DisableMethod bool `json:"disable_method,omitempty"`
	// Leave the query string out of the key.
	DisableQuery bool `json:"disable_query,omitempty"`
	// Leave the scheme out of the key.
	DisableScheme bool `json:"disable_scheme,omitempty"`
	// Ignore the Vary header of the responses.
	DisableVary bool `json:"disable_vary,omitempty"`
	// Sort the query parameters so their order does not matter.
	SortQuery bool `json:"sort_query,omitempty"`
	// Do not reveal the key in the Cache-Status header.
	Hide bool `json:"hide,omitempty"`
	// Request headers whose value is added to the key.
	Headers []string `json:"headers,omitempty"`
	// Replaces the default key. Caddy placeholders are supported.
	Template string `json:"template,omitempty"`
}

// RegexOptions excludes requests from the cache.
type RegexOptions struct {
	// Requests whose URI matches are not cached.
	Exclude string `json:"exclude,omitempty"`
}

// Options are the settings shared by the global cache option and the cache
// handler. A handler inherits from the global option whatever it leaves
// unset.
type Options struct {
	// Directory the responses are stored in. Handlers using the same
	// directory share one cache and its limits.
	// Default: the "cache" directory of Caddy's data directory.
	Path string `json:"path,omitempty"`
	// Disk space the cache may take. Default: 10GiB.
	MaxSize Size `json:"max_size,omitempty"`
	// RAM the cache may take, for its index and for the most requested
	// responses, which are served from memory. "off" keeps the responses on
	// disk only. Default: 256MiB.
	MaxMemory Size `json:"max_memory,omitempty"`
	// Number of files the cache may hold: one per stored response, one per
	// URL whose responses vary, and one per download in progress.
	// Default: no limit other than max_size.
	MaxFileCount int64 `json:"max_file_count,omitempty"`
	// Remove the responses that were not requested for that long.
	// Default: keep them until the space is needed.
	Inactive caddy.Duration `json:"inactive,omitempty"`
	// Number of requests after which a response is written to disk. Until
	// then it is kept in memory, within half of max_memory, and dropped from
	// there by the responses that follow if it is not requested again.
	// Default: 1, every response is written to disk as it is received.
	MinUses int `json:"min_uses,omitempty"`
	// Size of the ranges the responses are asked of the upstream in, each
	// range being stored by itself: a request for the middle of a large
	// response fetches what it reads and nothing else. The upstream has to
	// support range requests. Default: off, responses are fetched whole.
	Slice Size `json:"slice,omitempty"`
	// How long a response is fresh when the upstream does not say.
	// Default: 120s.
	TTL caddy.Duration `json:"ttl,omitempty"`
	// How long past its freshness a response may still be served while it is
	// being updated or when the upstream fails. Default: 0.
	Stale caddy.Duration `json:"stale,omitempty"`
	// How long a request waits for another one that is already fetching the
	// same response before going to the upstream itself. Default: 5s.
	LockTimeout caddy.Duration `json:"lock_timeout,omitempty"`
	// Which Cache-Control directives are honoured: by default those of the
	// responses only, with "strict" also those of the requests, with
	// "bypass_response" or "bypass" none of the responses. "strict" is the
	// mode that conforms to RFC 9111, see README.md.
	Mode string `json:"mode,omitempty"`
	// Name of the cache in the Cache-Status header. Default: Caddy.
	CacheName string `json:"cache_name,omitempty"`
	// Cache-Control given to the responses that have none.
	DefaultCacheControl string `json:"default_cache_control,omitempty"`
	// Responses with a larger body are not stored. Default: half of max_size.
	MaxBodyBytes Size `json:"max_cacheable_body_bytes,omitempty"`
	// Status codes to cache with the default TTL besides the standard ones.
	AllowedAdditionalStatusCodes []int `json:"allowed_additional_status_codes,omitempty"`
	// Cache key tuning.
	Key *KeyOptions `json:"key,omitempty"`
	// Requests to keep out of the cache.
	Regex *RegexOptions `json:"regex,omitempty"`
}

// inherit fills the unset options of o with those of parent.
func (o Options) inherit(parent Options) Options {
	if o.Path == "" {
		o.Path = parent.Path
	}
	if o.MaxSize == 0 {
		o.MaxSize = parent.MaxSize
	}
	if o.MaxMemory == 0 {
		o.MaxMemory = parent.MaxMemory
	}
	if o.MaxFileCount == 0 {
		o.MaxFileCount = parent.MaxFileCount
	}
	if o.Inactive == 0 {
		o.Inactive = parent.Inactive
	}
	if o.MinUses == 0 {
		o.MinUses = parent.MinUses
	}
	if o.Slice == 0 {
		o.Slice = parent.Slice
	}
	if o.TTL == 0 {
		o.TTL = parent.TTL
	}
	if o.Stale == 0 {
		o.Stale = parent.Stale
	}
	if o.LockTimeout == 0 {
		o.LockTimeout = parent.LockTimeout
	}
	if o.Mode == "" {
		o.Mode = parent.Mode
	}
	if o.CacheName == "" {
		o.CacheName = parent.CacheName
	}
	if o.DefaultCacheControl == "" {
		o.DefaultCacheControl = parent.DefaultCacheControl
	}
	if o.MaxBodyBytes == 0 {
		o.MaxBodyBytes = parent.MaxBodyBytes
	}
	if o.Key == nil {
		o.Key = parent.Key
	}
	if o.Regex == nil {
		o.Regex = parent.Regex
	}
	o.AllowedAdditionalStatusCodes = append(append([]int{}, parent.AllowedAdditionalStatusCodes...), o.AllowedAdditionalStatusCodes...)

	return o
}

// config is what a handler works with once its options are resolved.
type config struct {
	path   string
	limits Limits

	name           string
	ttl            time.Duration
	stale          time.Duration
	lockTimeout    time.Duration
	strict         bool
	ignoreResponse bool
	defaultCC      string
	maxBody        int64
	minUses        int
	slice          int64
	extraStatus    map[int]bool
	key            KeyOptions
	keyHeaders     []string
	exclude        *regexp.Regexp
}

// resolve validates the options and applies the defaults.
func (o Options) resolve() (*config, error) {
	c := &config{
		path:        o.Path,
		name:        o.CacheName,
		ttl:         time.Duration(o.TTL),
		stale:       time.Duration(o.Stale),
		lockTimeout: time.Duration(o.LockTimeout),
		defaultCC:   o.DefaultCacheControl,
		maxBody:     int64(o.MaxBodyBytes),
		minUses:     o.MinUses,
		extraStatus: make(map[int]bool),
		limits: Limits{
			MaxSize:   int64(o.MaxSize),
			MaxMemory: int64(o.MaxMemory),
			Inactive:  time.Duration(o.Inactive),
			MaxFiles:  o.MaxFileCount,
		},
	}

	if c.path == "" {
		c.path = filepath.Join(caddy.AppDataDir(), "cache")
	}
	path, err := filepath.Abs(c.path)
	if err != nil {
		return nil, fmt.Errorf("cache path: %w", err)
	}
	c.path = path

	switch {
	case c.limits.MaxSize == 0:
		c.limits.MaxSize = defaultMaxSize
	case c.limits.MaxSize < 0:
		return nil, fmt.Errorf("max_size must be positive")
	}
	switch {
	case c.limits.MaxMemory == 0:
		c.limits.MaxMemory = defaultMaxMemory
	case c.limits.MaxMemory < 0:
		c.limits.MaxMemory = 0
	case c.limits.MaxMemory < minMaxMemory:
		return nil, fmt.Errorf("max_memory must be at least %s, or off", Size(minMaxMemory))
	}

	if c.limits.MaxFiles < 0 {
		return nil, fmt.Errorf("max_file_count must be positive")
	}

	switch {
	case c.minUses == 0:
		c.minUses = 1
	case c.minUses < 1 || c.minUses > maxMinUses:
		return nil, fmt.Errorf("min_uses must be between 1 and %d", maxMinUses)
	case c.minUses > 1 && c.limits.MaxMemory == 0:
		return nil, fmt.Errorf("min_uses keeps the responses in memory until they are requested again, which max_memory off does not allow")
	}

	switch slice := int64(o.Slice); {
	case slice <= 0:
	case slice < minSlice:
		return nil, fmt.Errorf("slice must be at least %s, or off", Size(minSlice))
	case slice > c.limits.MaxSize/2:
		// A slice is stored like a response, and is no more allowed to take
		// most of the cache.
		return nil, fmt.Errorf("slice cannot be larger than half of max_size")
	default:
		c.slice = slice
	}

	if c.ttl < 0 || c.stale < 0 || c.lockTimeout < 0 || c.limits.Inactive < 0 || c.maxBody < 0 {
		return nil, fmt.Errorf("cache durations and sizes cannot be negative")
	}
	if c.ttl == 0 {
		c.ttl = defaultTTL
	}
	if c.lockTimeout == 0 {
		c.lockTimeout = defaultLockTimeout
	}
	if c.name == "" {
		c.name = defaultCacheName
	}

	switch o.Mode {
	case "", "bypass_request":
	case "strict":
		c.strict = true
	case "bypass", "bypass_response":
		c.ignoreResponse = true
	default:
		return nil, fmt.Errorf("unknown cache mode %q: expected strict, bypass, bypass_request or bypass_response", o.Mode)
	}

	for _, code := range o.AllowedAdditionalStatusCodes {
		if code < 200 || code > 599 || code == http.StatusPartialContent || code == http.StatusNotModified {
			return nil, fmt.Errorf("status code %d cannot be cached", code)
		}
		c.extraStatus[code] = true
	}

	if o.Key != nil {
		c.key = *o.Key
		for _, name := range o.Key.Headers {
			c.keyHeaders = append(c.keyHeaders, http.CanonicalHeaderKey(name))
		}
	}
	if o.Regex != nil && o.Regex.Exclude != "" {
		if c.exclude, err = regexp.Compile(o.Regex.Exclude); err != nil {
			return nil, fmt.Errorf("regex exclude: %w", err)
		}
	}

	return c, nil
}

const storageRemoved = "storage backends were removed, the cache now stores on disk and in memory by itself: use path, max_size, max_memory and max_file_count"

// removedOptions are the options of the Souin based versions of this module
// that no longer exist, with what to do instead.
var removedOptions = map[string]string{
	"badger":                    storageRemoved,
	"etcd":                      storageRemoved,
	"nats":                      storageRemoved,
	"nuts":                      storageRemoved,
	"olric":                     storageRemoved,
	"otter":                     storageRemoved,
	"redis":                     storageRemoved,
	"simplefs":                  storageRemoved,
	"storers":                   storageRemoved,
	"api":                       "the cache API is always served by the admin endpoint, under /cache/",
	"cdn":                       "CDN purge propagation is not supported",
	"cache_keys":                "use one cache directive per matcher instead",
	"headers":                   "use key { headers ... } instead",
	"timeout":                   "set the timeouts on reverse_proxy instead",
	"allowed_http_verbs":        "only GET and HEAD requests are cached",
	"log_level":                 "the cache logs through Caddy's logger",
	"disable_coalescing":        "use lock_timeout to bound how long requests wait for each other",
	"disable_surrogate_key":     "surrogate keys are not supported",
	"mapping_eviction_interval": "it is no longer needed",
}

// parseOptions reads the block of the global option or of the directive,
// which share their syntax.
func parseOptions(d *caddyfile.Dispenser, o *Options) error {
	for d.Next() {
		if d.NextArg() {
			return d.ArgErr()
		}

		for nesting := d.Nesting(); d.NextBlock(nesting); {
			option := d.Val()

			if hint, removed := removedOptions[option]; removed {
				return d.Errf("the %s option is not supported anymore: %s", option, hint)
			}

			switch option {
			case "path":
				if !d.AllArgs(&o.Path) {
					return d.ArgErr()
				}
			case "max_size":
				if err := parseSizeArg(d, &o.MaxSize); err != nil {
					return err
				}
				if o.MaxSize <= 0 {
					return d.Err("max_size must be positive")
				}
			case "max_memory":
				if err := parseSizeArg(d, &o.MaxMemory); err != nil {
					return err
				}
				// An unset size is zero, so zero is spelled off.
				if o.MaxMemory == 0 {
					o.MaxMemory = SizeOff
				}
			case "max_file_count":
				var arg string
				if !d.AllArgs(&arg) {
					return d.ArgErr()
				}
				count, err := strconv.ParseInt(arg, 10, 64)
				if err != nil || count <= 0 {
					return d.Errf("invalid max_file_count %q: expected a positive number of files", arg)
				}
				o.MaxFileCount = count
			case "min_uses":
				var arg string
				if !d.AllArgs(&arg) {
					return d.ArgErr()
				}
				uses, err := strconv.Atoi(arg)
				if err != nil || uses < 1 {
					return d.Errf("invalid min_uses %q: expected a positive number of requests", arg)
				}
				o.MinUses = uses
			case "slice":
				if err := parseSizeArg(d, &o.Slice); err != nil {
					return err
				}
				// An unset size is zero, so zero is spelled off.
				if o.Slice == 0 {
					o.Slice = SizeOff
				}
			case "max_cacheable_body_bytes":
				if err := parseSizeArg(d, &o.MaxBodyBytes); err != nil {
					return err
				}
				if o.MaxBodyBytes < 0 {
					return d.Err("max_cacheable_body_bytes must be positive")
				}
			case "inactive":
				if err := parseDurationArg(d, &o.Inactive); err != nil {
					return err
				}
			case "ttl":
				if err := parseDurationArg(d, &o.TTL); err != nil {
					return err
				}
			case "stale":
				if err := parseDurationArg(d, &o.Stale); err != nil {
					return err
				}
			case "lock_timeout":
				if err := parseDurationArg(d, &o.LockTimeout); err != nil {
					return err
				}
			case "mode":
				if !d.AllArgs(&o.Mode) {
					return d.ArgErr()
				}
			case "cache_name":
				if !d.AllArgs(&o.CacheName) {
					return d.ArgErr()
				}
			case "default_cache_control":
				args := d.RemainingArgs()
				if len(args) == 0 {
					return d.ArgErr()
				}
				o.DefaultCacheControl = strings.Join(args, " ")
			case "allowed_additional_status_codes":
				args := d.RemainingArgs()
				if len(args) == 0 {
					return d.ArgErr()
				}
				for _, arg := range args {
					code, err := strconv.Atoi(arg)
					if err != nil {
						return d.Errf("invalid status code %q", arg)
					}
					o.AllowedAdditionalStatusCodes = append(o.AllowedAdditionalStatusCodes, code)
				}
			case "key":
				key := new(KeyOptions)
				for nesting := d.Nesting(); d.NextBlock(nesting); {
					switch d.Val() {
					case "disable_host":
						key.DisableHost = true
					case "disable_method":
						key.DisableMethod = true
					case "disable_query":
						key.DisableQuery = true
					case "disable_scheme":
						key.DisableScheme = true
					case "disable_vary":
						key.DisableVary = true
					case "sort_query":
						key.SortQuery = true
					case "hide":
						key.Hide = true
					case "headers":
						key.Headers = d.RemainingArgs()
						if len(key.Headers) == 0 {
							return d.ArgErr()
						}
					case "template":
						if !d.AllArgs(&key.Template) {
							return d.ArgErr()
						}
					default:
						return d.Errf("unsupported key option: %s", d.Val())
					}
				}
				o.Key = key
			case "regex":
				regex := new(RegexOptions)
				for nesting := d.Nesting(); d.NextBlock(nesting); {
					switch d.Val() {
					case "exclude":
						if !d.AllArgs(&regex.Exclude) {
							return d.ArgErr()
						}
					default:
						return d.Errf("unsupported regex option: %s", d.Val())
					}
				}
				o.Regex = regex
			default:
				return d.Errf("unsupported cache option: %s", option)
			}
		}
	}

	// Report mistakes where they are written rather than when the
	// configuration is loaded. Only those that can be told from here: a
	// slice is checked against the size of the cache, which may be set where
	// this block inherits from, or where it is inherited. That is left to
	// when the handler is provisioned, and knows both.
	check := *o
	if check.MaxSize == 0 {
		check.MaxSize = math.MaxInt64
	}
	if _, err := check.resolve(); err != nil {
		return d.Err(err.Error())
	}

	return nil
}

func parseSizeArg(d *caddyfile.Dispenser, size *Size) error {
	var arg string
	if !d.AllArgs(&arg) {
		return d.ArgErr()
	}

	v, err := ParseSize(arg)
	if err != nil {
		return d.Err(err.Error())
	}
	*size = v

	return nil
}

func parseDurationArg(d *caddyfile.Dispenser, duration *caddy.Duration) error {
	var arg string
	if !d.AllArgs(&arg) {
		return d.ArgErr()
	}

	v, err := caddy.ParseDuration(arg)
	if err != nil {
		return d.Errf("invalid duration %q: %v", arg, err)
	}
	*duration = caddy.Duration(v)

	return nil
}
