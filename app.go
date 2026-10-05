package httpcache

import (
	"fmt"
	"sync"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
)

const moduleName = "cache"

// stores holds the open stores by directory. It outlives the configurations,
// so that reloading Caddy keeps the index and the responses held in memory.
var stores = caddy.NewUsagePool()

func init() {
	caddy.RegisterModule(new(App))
	httpcaddyfile.RegisterGlobalOption(moduleName, parseCaddyfileGlobalOption)
}

// App holds the cache options set globally, which every cache handler
// inherits.
type App struct {
	Options

	mu sync.Mutex
	// claims records the store and the limits each directory is used with
	// in this configuration.
	claims map[string]claim
}

type claim struct {
	store  *Store
	limits Limits
}

// CaddyModule implements caddy.Module.
func (*App) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  moduleName,
		New: func() caddy.Module { return new(App) },
	}
}

// Provision implements caddy.Provisioner.
func (a *App) Provision(caddy.Context) error {
	_, err := a.resolve()

	return err
}

// Start implements caddy.App. It runs once the whole configuration is
// accepted, which is when the limits it sets may be applied to the stores
// inherited from the previous one: a configuration that fails to load must
// not shrink the cache of the one that keeps running.
func (a *App) Start() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, c := range a.claims {
		c.store.SetLimits(c.limits)
	}

	return nil
}

// Stop implements caddy.App.
func (*App) Stop() error {
	return nil
}

// claim checks that the handlers of this configuration agree on the limits
// of the cache held in a directory: a directory is one cache.
func (a *App) claim(path string, limits Limits) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if other, ok := a.claims[path]; ok && other.limits != limits {
		return fmt.Errorf("the cache directory %s is configured with different max_size, max_memory or inactive values: give each cache its own path or set these options once, globally", path)
	}

	return nil
}

// use records the store a handler of this configuration works with.
func (a *App) use(store *Store, path string, limits Limits) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.claims == nil {
		a.claims = make(map[string]claim)
	}
	a.claims[path] = claim{store: store, limits: limits}
}

func parseCaddyfileGlobalOption(d *caddyfile.Dispenser, _ any) (any, error) {
	app := new(App)
	if err := parseOptions(d, &app.Options); err != nil {
		return nil, err
	}

	return httpcaddyfile.App{
		Name:  moduleName,
		Value: caddyconfig.JSON(app, nil),
	}, nil
}

var (
	_ caddy.App         = (*App)(nil)
	_ caddy.Module      = (*App)(nil)
	_ caddy.Provisioner = (*App)(nil)
)
