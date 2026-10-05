Caddy Module: http.handlers.cache
================================

An HTTP cache for Caddy that works the way nginx's `proxy_cache` does: responses are stored as files in a directory of bounded size, and the most requested ones are also kept in memory, within a strict budget.

> [!NOTE]
> Earlier versions of this module were an adapter for [Souin](https://github.com/darkweak/souin) and its pluggable storage backends. This version is self-contained: it has one storage, built in, and no dependency on Souin. See [Migrating from the Souin based versions](#migrating-from-the-souin-based-versions).

## Features

* Two storage tiers, both bounded: `max_size` on disk, `max_memory` in RAM.
* Responses are streamed to the client and to the cache at the same time. A body is never buffered whole in memory, whatever its size.
* The cache survives restarts and crashes: files are written atomically and the index is rebuilt from them in the background.
* Concurrent requests for a missing response share one upstream request.
* A response keeps being stored when the client that asked for it disconnects.
* Expired responses are revalidated with `If-None-Match` / `If-Modified-Since` instead of being downloaded again, and can be served stale while they are updated or when the upstream fails.
* `Range`, `If-None-Match`, `If-Modified-Since` and `HEAD` requests are answered from the cache.
* `Vary` support, with `Accept-Encoding` normalized so that compressed variants are not multiplied.
* Sets the [`Cache-Status`](https://www.rfc-editor.org/rfc/rfc9211) and `Age` response headers.
* Statistics and purge on Caddy's admin endpoint.

## Building

```sh
xcaddy build --with github.com/caddyserver/cache-handler
```

## Minimal configuration

```caddy
{
    cache {
        ttl 24h
        max_memory 8Gi
        max_size 25Gi
    }
}

example.com {
    cache
    reverse_proxy your-app:8080
}
```

The `cache` directive is ordered before `rewrite`. The global `cache` block is optional: it sets the defaults every `cache` directive inherits.

## Options

The global option and the directive take the same options. A directive inherits from the global option whatever it does not set itself.

```caddy
cache [<matcher>] {
    path /var/cache/caddy
    max_size 25Gi
    max_memory 8Gi
    inactive 30d

    ttl 24h
    stale 1h
    lock_timeout 5s
    mode strict
    cache_name Caddy
    default_cache_control "public, max-age=3600"
    max_cacheable_body_bytes 1Gi
    allowed_additional_status_codes 404 410

    key {
        disable_host
        disable_method
        disable_query
        disable_scheme
        disable_vary
        sort_query
        headers Authorization X-Tenant
        template {http.request.uri.path}
        hide
    }
    regex {
        exclude ^/admin/
    }
}
```

| Option | Default | Description |
|---|---|---|
| `path` | `cache` in Caddy's data directory | Directory the responses are stored in. Directives using the same directory share one cache. The directory must be used by one Caddy process only. |
| `max_size` | `10Gi` | Disk space the cache may take. The least recently used responses are removed to stay under it. |
| `max_memory` | `256Mi` | RAM the cache may take, for its index and for the most requested responses. `off` keeps the responses on disk only. |
| `inactive` | none | Removes the responses that were not requested for this long, fresh or not. |
| `ttl` | `120s` | How long a response is fresh when the upstream does not say (no `Cache-Control: max-age` / `s-maxage`, no `Expires`). |
| `stale` | `0` | How long past its freshness a response may still be served while it is being updated, or when the upstream fails. `Cache-Control: stale-while-revalidate` and `stale-if-error` in a response override it. |
| `lock_timeout` | `5s` | How long a request waits for another one that is already fetching the same response, before going to the upstream itself. |
| `mode` | | Which `Cache-Control` directives are honoured. By default those of the responses, but not those of the requests: a client cannot force its way past the cache. `strict` also honours the requests' (`no-cache`, `no-store`, `max-age`, `min-fresh`, `max-stale`, `only-if-cached`). `bypass_response` ignores the responses' and caches everything for `ttl`. `bypass` and `bypass_request` are accepted as aliases of `bypass_response` and of the default. |
| `cache_name` | `Caddy` | Name of the cache in the `Cache-Status` header. |
| `default_cache_control` | | `Cache-Control` given to the responses that have none. |
| `max_cacheable_body_bytes` | half of `max_size` | Responses with a larger body are relayed without being stored. |
| `allowed_additional_status_codes` | | Status codes to cache for `ttl`, besides 200, 203, 204, 300, 301 and 308. |
| `key` | | Tunes the cache key, see below. |
| `regex` `exclude` | | Requests whose URI matches are not cached. |

`max_size`, `max_memory` and `inactive` belong to the directory: two directives using the same `path` cannot disagree on them. Set them once in the global option, or give each cache its own `path`.

Sizes are a number of bytes or a number with a unit: `k`, `m`, `g`, `t` and `Ki`, `Mi`, `Gi`, `Ti` are powers of 1024, `KB`, `MB`, `GB`, `TB` powers of 1000.

In JSON, the handler is `{"handler": "cache", ...}` and the global options are the `cache` app, with the same option names.

## How it works

### Storage

Every stored response is one file under `path`. It is written to a temporary file while it arrives and renamed into place when it is complete, so that a file is always whole: a crash or an interrupted download leaves nothing behind but a temporary file that is deleted on the next start. Files are never modified afterwards.

An index of the files is kept in memory. On start it is rebuilt by reading the directory in the background; meanwhile, requests find the files that are not indexed yet directly on disk, so the cache is warm immediately.

A response requested at least twice in a few minutes is copied to memory, provided it is requested more than the response it would push out. Responses in memory are still on disk: memory is an accelerator, never the only copy.

### Limits

`max_size` covers the cache files and the downloads in progress. When a response does not fit, the least recently used ones are deleted to make room. A response larger than half of `max_size` (or than `max_cacheable_body_bytes`) is not stored.

`max_memory` covers the index (about 200 bytes plus the key per file) and the bodies and headers kept in memory. The bodies live outside the Go heap, in memory mapped for that purpose and returned to the system when the budget shrinks, so they do not weigh on the garbage collector. When the index alone approaches the budget, which takes millions of files, the least recently used files are removed.

What is not counted is what serving requests takes: a few tens of kilobytes of buffers per request in progress, whatever the size of the response.

### Fetching

On a miss, the response is relayed to the client as it arrives from the upstream while it is written to the cache. If the client disconnects, the download goes on for the benefit of the next requests, as long as the upstream keeps sending.

Other requests for the same response wait for the first one, up to `lock_timeout`, and are then served from the cache. When the response turns out not to be cacheable, they are released at once and, for a minute, requests for it are not made to wait at all.

When the request that triggers a fetch asks for a range or carries a precondition, the whole response is fetched and stored first, and the request is answered from it.

### Expiry

A response past its freshness is not discarded. The next request for it revalidates it: the upstream is sent the `ETag` and `Last-Modified` of the stored response and, if it answers `304 Not Modified`, the stored response is fresh again without having been transferred. Otherwise the new response replaces it.

With `stale` set, the other requests arriving during this update are served the stale response immediately, and a failure of the upstream (an error before the response, or a 5xx) is answered with the stale response too. Responses marked `must-revalidate`, `proxy-revalidate` or `no-cache` are never served stale.

### What is cached

Only `GET` requests fill the cache; `HEAD` requests are answered from it. A response is stored unless:

* its status is not one of 200, 203, 204, 300, 301, 302, 307, 308, 404, 405, 410, 414, 501 or of `allowed_additional_status_codes`;
* it does not say how long it is fresh and its status is not one `ttl` applies to;
* it has `Cache-Control: no-store` or `private`;
* it has a `Set-Cookie` header, or `Vary: *`;
* the request had an `Authorization` header, and the response has none of `public`, `s-maxage`, `must-revalidate`, and `Authorization` is neither in `Vary` nor in the `key` `headers`;
* it is already expired and has no `ETag` or `Last-Modified` to revalidate it with;
* its body is larger than what may be stored.

A successful `POST`, `PUT`, `PATCH` or `DELETE` request removes the response stored for its URI.

The cache stores what the handlers after it produce. Headers set by a directive placed before it, such as `header Cache-Control "public, max-age=31536000"` for the browsers, are given to every response, served from the cache or not: they are not stored and do not decide how long a response is kept.

## Cache key

The default key is `METHOD-SCHEME-HOST-PATH?QUERY`, for instance `GET-https-example.com-/logo.png?v=2`. A different key means a different stored response, so the key should contain what makes the response different and nothing else.

| `key` option | Effect |
|---|---|
| `disable_host`, `disable_method`, `disable_scheme` | Leaves that part out of the key. |
| `disable_query` | Leaves the query string out: `/a?x=1` and `/a?x=2` are the same response. |
| `sort_query` | Sorts the query parameters: `/a?x=1&y=2` and `/a?y=2&x=1` are the same response. |
| `headers` | Adds the value of these request headers to the key. |
| `template` | Replaces the key altogether. Caddy placeholders are supported. |
| `disable_vary` | Ignores the `Vary` header of the responses. |
| `hide` | Does not show the key in the `Cache-Status` header. |

The key is computed before the request is rewritten. When several URLs are rewritten to the same upstream object, build the key from what identifies the object so that they share one stored response:

```caddy
@image path_regexp image ^/img/download/(.+)/([0-9]+).*\.([A-Za-z0-9]+)$
cache @image {
    key {
        template {re.image.1}/{re.image.2}.{re.image.3}
    }
}
rewrite @image /bucket/images/{re.image.1}/{re.image.2}/full.{re.image.3}
reverse_proxy s3.example.com
```

Responses with a `Vary` header are stored once per combination of the request headers they list. An upstream that sends `Vary: Origin` to every browser, as object storages with CORS enabled do, makes one copy per origin: use `disable_vary` if the response does not actually depend on it.

## The Cache-Status header

| Value | Meaning |
|---|---|
| `Caddy; hit; ttl=3541; detail=MEMORY` | Served from memory, fresh for 3541 more seconds. |
| `Caddy; hit; ttl=3541; detail=DISK` | Served from disk. |
| `Caddy; hit; ttl=-12; detail=UPDATING` | Served stale while another request updates it. |
| `Caddy; fwd=uri-miss; stored` | Fetched from the upstream and stored. |
| `Caddy; fwd=uri-miss; collapsed` | Waited for another request fetching the same response. |
| `Caddy; fwd=uri-miss; detail=<REASON>` | Fetched from the upstream and not stored: `NO-STORE`, `PRIVATE`, `SET-COOKIE`, `VARY-STAR`, `AUTHORIZATION`, `UNCACHEABLE-STATUS`, `EXPIRED`, `TOO-LARGE`, `HEAD`, `LOCK-TIMEOUT`, `STORAGE-ERROR`. |
| `Caddy; fwd=stale; fwd-status=304; detail=REVALIDATED` | Expired, confirmed by the upstream, served from the cache. |
| `Caddy; fwd=stale; stored` | Expired, replaced by a new response from the upstream. |
| `Caddy; fwd=stale; fwd-status=503; detail=STALE` | Expired and served anyway because the upstream failed. |
| `Caddy; fwd=bypass; detail=<REASON>` | Not handled by the cache: `UNSUPPORTED-METHOD`, `EXCLUDED`, `KEY-TOO-LONG`, `REQUEST-NO-STORE`. |

The key follows as `; key=...` unless it is hidden.

## Admin API

The cache is observed and purged through [Caddy's admin endpoint](https://caddyserver.com/docs/api), `localhost:2019` by default.

```sh
# State of the caches: entries, bytes on disk and in memory, hits, misses, evictions…
curl localhost:2019/cache/stats

# Remove the response stored for a key, as shown in Cache-Status
curl -X POST 'localhost:2019/cache/purge?key=GET-https-example.com-/logo.png'

# Remove the responses whose key starts with a prefix, or matches a regular expression
curl -X POST 'localhost:2019/cache/purge?prefix=GET-https-example.com-/img/'
curl -X POST --data-urlencode 'regex=\.css$' -G 'localhost:2019/cache/purge'

# Empty the caches
curl -X POST 'localhost:2019/cache/purge?all=true'
```

With several caches, add `path=<directory>` to purge one of them only.

## Coming from nginx

| nginx | Here |
|---|---|
| `proxy_cache_path /var/www/cache` | `path /var/www/cache` |
| `max_size=25000m` | `max_size 25000m` |
| `keys_zone=name:8m` | Not needed: the index is part of `max_memory`. |
| `inactive=720m` | `inactive 720m` |
| `levels=1:2`, `use_temp_path=off` | Not needed. |
| `proxy_cache_valid 6h` | `ttl 6h`. Add `allowed_additional_status_codes 302` to cover the same statuses. |
| `proxy_cache_key $request_filename` | `key { template ... }`, see [Cache key](#cache-key). |
| `proxy_cache_lock on` | Always on. `proxy_cache_lock_timeout` is `lock_timeout`. |
| `proxy_cache_revalidate on` | Always on. |
| `proxy_cache_use_stale updating error timeout http_5xx` | `stale <duration>` |
| `proxy_cache_background_update on` | With `stale`, one request waits for the update and the others are served stale meanwhile. |
| `proxy_ignore_headers Cache-Control Expires` | `mode bypass_response` |
| `proxy_cache_bypass`, `proxy_no_cache` | A matcher on the `cache` directive, or `regex { exclude }`. |
| `$upstream_cache_status` | The `Cache-Status` response header. |

What nginx leaves to the page cache of the kernel, serving hot files from RAM, is done here explicitly and within `max_memory`. Files that are not in memory are still served through the page cache like nginx does.

## Migrating from the Souin based versions

The storage backends are gone, and with them the need to build Caddy with a storage module: remove `badger`, `etcd`, `nats`, `nuts`, `olric`, `otter`, `redis`, `simplefs` and `storers` from your configuration and set `path`, `max_size` and `max_memory` instead. A configuration that still uses a removed option is refused with a message saying what to use instead.

| Removed | Instead |
|---|---|
| Storage providers, `storers` | `path`, `max_size`, `max_memory`. The cache is local to one Caddy instance. |
| `api` | The API is always available on the admin endpoint, under `/cache/`. |
| `cache_keys` | One `cache` directive per matcher, each with its `key` block. |
| `headers` | `key { headers ... }` |
| `timeout` | The timeouts of `reverse_proxy`. |
| `allowed_http_verbs`, `key { disable_body }` | Only `GET` and `HEAD` are cached. |
| `cdn`, surrogate keys, ESI | Not supported. |
| `log_level` | Caddy's `log` option. |
| `key { hash }` | Not needed: keys are always hashed on disk. |

Other differences:

* Request `Cache-Control` directives are ignored unless `mode strict` is set.
* Responses with a 404, 405, 410, 414 or 501 status are only cached when they say for how long, or when listed in `allowed_additional_status_codes`.
* The `Cache-Status` header names the cache `Caddy` and has different details, see above.
* The admin API moved from `/souin-api` to `/cache`.

## Platform notes

Linux, macOS and the BSDs are supported. On Linux, memory the cache gives up is returned to the system at once; on macOS and the BSDs the system takes it back when it needs it, so the resident size of the process may stay above what the cache uses for a while.

On other systems the module builds and works, but the bodies kept in memory are on the Go heap, where freed memory is only returned by the garbage collector, and the cache directory is not protected against being used by two processes.
