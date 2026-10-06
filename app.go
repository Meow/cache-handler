// This file has been modified from the one of caddyserver/cache-handler it
// replaces, see the NOTICE file.

package httpcache

import (
	"context"
	"fmt"
	"sync"

	"github.com/caddyserver/caddy/v2"
	"github.com/caddyserver/caddy/v2/caddyconfig"
	"github.com/caddyserver/caddy/v2/caddyconfig/caddyfile"
	"github.com/caddyserver/caddy/v2/caddyconfig/httpcaddyfile"
	"go.uber.org/zap"
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
// inherits, and the stores the handlers of its configuration work with.
type App struct {
	Options

	logger *zap.Logger

	mu sync.Mutex
	// claims records the limits each directory is used with in this
	// configuration.
	claims map[string]Limits
	// opened holds the stores once the app is started. It is not written
	// after ready is closed.
	opened map[string]*Store
	// held lists the directories whose store this app keeps open.
	held []string
	// ready is closed when the stores are open, or never will be.
	ready     chan struct{}
	readyOnce sync.Once
}

// CaddyModule implements caddy.Module.
func (*App) CaddyModule() caddy.ModuleInfo {
	return caddy.ModuleInfo{
		ID:  moduleName,
		New: func() caddy.Module { return new(App) },
	}
}

// Provision implements caddy.Provisioner.
func (a *App) Provision(ctx caddy.Context) error {
	a.logger = ctx.Logger()
	a.ready = make(chan struct{})

	_, err := a.resolve()

	return err
}

// Start implements caddy.App. The stores are opened here rather than at
// Provision because Start only runs for a configuration that is accepted and
// about to serve: caddy validate and a refused reload provision every module,
// and must neither touch the running server's cache nor apply new limits to
// it.
func (a *App) Start() error {
	defer a.readyOnce.Do(func() { close(a.ready) })

	a.mu.Lock()
	defer a.mu.Unlock()

	a.opened = make(map[string]*Store, len(a.claims))
	for path, limits := range a.claims {
		store, loaded, err := stores.LoadOrNew(path, func() (caddy.Destructor, error) {
			return OpenStore(path, limits, a.logger)
		})
		if err != nil {
			a.releaseLocked()
			a.opened = nil

			return fmt.Errorf("opening the cache in %s: %w", path, err)
		}

		a.held = append(a.held, path)
		a.opened[path] = store.(*Store)
		if loaded {
			// The store comes from the previous configuration.
			a.opened[path].SetLimits(limits)
		}
	}

	return nil
}

// Stop implements caddy.App. A store is closed once no configuration uses
// it anymore, so a reload keeps it.
func (a *App) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.releaseLocked()

	return nil
}

// Cleanup implements caddy.CleanerUpper. It releases the requests waiting
// for an app that was never started.
func (a *App) Cleanup() error {
	a.readyOnce.Do(func() { close(a.ready) })

	return nil
}

func (a *App) releaseLocked() {
	for _, path := range a.held {
		_, _ = stores.Delete(path)
	}
	a.held = nil
}

// claim registers that a handler of this configuration uses the directory
// with the given limits. A directory is one cache, so it has one set of them.
func (a *App) claim(path string, limits Limits) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.claims == nil {
		a.claims = make(map[string]Limits)
	}
	if other, ok := a.claims[path]; ok && other != limits {
		return fmt.Errorf("the cache directory %s is configured with different max_size, max_memory, max_file_count or inactive values: give each cache its own path or set these options once, globally", path)
	}
	a.claims[path] = limits

	return nil
}

// store returns the store of a directory, waiting for the app to be started
// if it is not yet. It returns nil if the cache is not available.
func (a *App) store(ctx context.Context, path string) *Store {
	select {
	case <-a.ready:
		return a.opened[path]
	case <-ctx.Done():
		return nil
	}
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
	_ caddy.App          = (*App)(nil)
	_ caddy.Module       = (*App)(nil)
	_ caddy.Provisioner  = (*App)(nil)
	_ caddy.CleanerUpper = (*App)(nil)
)
