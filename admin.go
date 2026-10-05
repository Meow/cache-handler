// This file has been modified from the one of caddyserver/cache-handler it
// replaces, see the NOTICE file.

package httpcache

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/caddyserver/caddy/v2"
)

func init() {
	caddy.RegisterModule(new(adminAPI))
}

// adminAPI serves the state of the caches and purge requests on Caddy's
// admin endpoint:
//
//	GET  /cache/stats                  the state of every cache
//	POST /cache/purge?key=<key>        removes the response stored for a key
//	POST /cache/purge?prefix=<prefix>  removes the responses whose key has the prefix
//	POST /cache/purge?regex=<regex>    removes the responses whose key matches
//	POST /cache/purge?all=true         empties the caches
//
// Keys are the ones shown in the Cache-Status header. A purge applies to the
// cache stored in the directory given by the path parameter, or to all of
// them without it.
type adminAPI struct{}

// CaddyModule returns the Caddy module information.
func (*adminAPI) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  "admin.api.cache",
		New: func() caddy.Module { return new(adminAPI) },
	}
}

// Routes returns the admin routes.
func (a *adminAPI) Routes() []caddy.AdminRoute {
	return []caddy.AdminRoute{
		{Pattern: "/cache/stats", Handler: caddy.AdminHandlerFunc(a.handleStats)},
		{Pattern: "/cache/purge", Handler: caddy.AdminHandlerFunc(a.handlePurge)},
	}
}

// openStores returns the stores in use, limited to the one at path if given.
func openStores(path string) []*Store {
	var found []*Store
	stores.Range(func(key, value any) bool {
		if s, ok := value.(*Store); ok && (path == "" || key == path) {
			found = append(found, s)
		}
		return true
	})

	return found
}

func (a *adminAPI) handleStats(w http.ResponseWriter, r *http.Request) error {
	if r.Method != http.MethodGet {
		return caddy.APIError{HTTPStatus: http.StatusMethodNotAllowed, Err: fmt.Errorf("method not allowed")}
	}

	stats := []StoreStats{}
	for _, s := range openStores("") {
		stats = append(stats, s.Stats())
	}

	return writeJSON(w, map[string]any{"caches": stats})
}

func (a *adminAPI) handlePurge(w http.ResponseWriter, r *http.Request) error {
	switch r.Method {
	case http.MethodPost, http.MethodDelete, "PURGE":
	default:
		return caddy.APIError{HTTPStatus: http.StatusMethodNotAllowed, Err: fmt.Errorf("method not allowed")}
	}

	query := r.URL.Query()
	targets := openStores(query.Get("path"))

	var purge func(*Store) int
	switch {
	case query.Has("key"):
		key := query.Get("key")
		purge = func(s *Store) int {
			if s.Purge(key) {
				return 1
			}
			return 0
		}
	case query.Has("prefix"):
		prefix := query.Get("prefix")
		purge = func(s *Store) int {
			return s.PurgeMatch(func(key string) bool { return strings.HasPrefix(key, prefix) })
		}
	case query.Has("regex"):
		re, err := regexp.Compile(query.Get("regex"))
		if err != nil {
			return caddy.APIError{HTTPStatus: http.StatusBadRequest, Err: err}
		}
		purge = func(s *Store) int { return s.PurgeMatch(re.MatchString) }
	case query.Get("all") == "true":
		purge = (*Store).PurgeAll
	default:
		return caddy.APIError{
			HTTPStatus: http.StatusBadRequest,
			Err:        fmt.Errorf("expected one of the key, prefix, regex or all=true parameters"),
		}
	}

	purged := 0
	for _, s := range targets {
		purged += purge(s)
	}

	return writeJSON(w, map[string]any{"purged": purged})
}

func writeJSON(w http.ResponseWriter, v any) error {
	w.Header().Set("Content-Type", "application/json")

	return json.NewEncoder(w).Encode(v)
}

var (
	_ caddy.Module      = (*adminAPI)(nil)
	_ caddy.AdminRouter = (*adminAPI)(nil)
)
