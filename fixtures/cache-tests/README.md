# Cache-Tests

Runs the [http-tests/cache-tests](https://github.com/http-tests/cache-tests) conformance suite against this module, with the Caddyfile in this directory.

1. Start the suite's server: `npm run server`
2. Run the tests: `NODE_TLS_REJECT_UNAUTHORIZED=0 npm run cli --base=http://localhost --silent > results/caddy-cache-handler.json`
3. To see the results, add this to `results/index.mjs`:

```
  {
    file: 'caddy-cache-handler.json',
    name: 'Caddy',
    type: 'rev-proxy',
    version: 'dev'
  }
```

4. Open https://localhost
