//go:build ignore

// Caddy with the cache module of the working tree, and nothing else that is
// not in a standard build. bench.sh builds it with "go build caddy.go": the
// versions are those of go.mod, the ones the tests run with, where xcaddy
// would take the latest release of Caddy. The build constraint keeps the
// file out of the package builds, tests and lint of the repository.
package main

import (
	caddycmd "github.com/caddyserver/caddy/v2/cmd"

	_ "github.com/Meow/cache-handler"
	_ "github.com/caddyserver/caddy/v2/modules/standard"
)

func main() {
	caddycmd.Main()
}
