# Changelog

## 1.0.0

First release of this module, forked from [caddyserver/cache-handler](https://github.com/caddyserver/cache-handler) 0.17.0. Version numbers start over: this is a different module, with a different configuration, not version 0.18 of the original.

### Changed from caddyserver/cache-handler 0.17.0

* The module path is `github.com/Meow/cache-handler`.
* The cache no longer relies on [Souin](https://github.com/darkweak/souin). It has its own engine and one built-in storage: files in a directory bounded by `max_size` and optionally `max_file_count`, with the most requested responses also kept in memory, bounded by `max_memory`.
* Response bodies are streamed to the client and to the cache at once and are never held whole in memory.
* Requests arriving while a response is being downloaded are served from it as it arrives.
* A `Range` request that triggers a download is relayed its range as the response arrives.
* A download continues when the client that triggered it disconnects.
* Expired responses are revalidated with a conditional request.
* Request `Cache-Control` directives are ignored unless `mode strict` is set.
* Responses with a 404, 405, 410, 414 or 501 status are only cached when they say for how long, or when listed in `allowed_additional_status_codes`.
* The `Cache-Status` header names the cache `Caddy` and uses different details.
* The admin API is always enabled and moved from `/souin-api` to `/cache`: `GET /cache/stats`, `POST /cache/purge`.

### Added

* Options `path`, `max_size`, `max_memory`, `max_file_count`, `inactive`, `lock_timeout`.
* Option `min_uses`: a response is kept in memory only until it has been requested that many times, and written to disk then. Responses that are requested once cost no disk write.
* `key { sort_query }` and Caddy placeholders in `key { template }`.

### Removed

* Storage backends and their options: `badger`, `etcd`, `nats`, `nuts`, `olric`, `otter`, `redis`, `simplefs`, `storers`. The cache is local to one Caddy instance.
* Surrogate keys, CDN purge propagation (`cdn`), ESI.
* Caching of methods other than `GET` (`allowed_http_verbs`, `key { disable_body }`).
* Options `api`, `cache_keys`, `headers`, `timeout`, `log_level`, `disable_coalescing`, `disable_surrogate_key`, `mapping_eviction_interval`, `key { hash }`.

A configuration that uses a removed option is refused with a message saying what to use instead.
