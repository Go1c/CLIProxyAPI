# Codex HTTP/2 Keepalive

This example is a Codex OAuth executor plugin with an in-plugin HTTP/2 transport pool, `uTLS` client hello, and HTTP/2 liveness pings.

It is opt-in only. When the plugin is disabled or not routed by `ModelRouter`, it has no effect on other services.

## Build

```bash
make -C examples/plugin bin/codex-http2-keepalive-go.dylib
```

Use `bin/codex-http2-keepalive-go.so` on Linux and `.dll` on Windows.

## Config

Load the shared library through `plugins.path` and configure the plugin under `plugins.configs.codex-http2-keepalive`:

```yaml
plugins:
  path:
    - /absolute/path/to/examples/plugin/bin/codex-http2-keepalive-go.dylib
  configs:
    codex-http2-keepalive:
      enabled: true
      model_prefixes:
        - gpt-
        - codex-
      base_url: https://chatgpt.com/backend-api/codex/responses
      read_idle_timeout: 15s
      ping_timeout: 15s
      max_idle_connections: 16
      retry_network_errors: true
      management_enabled: true
```

The plugin reads `access_token`, `account_id`, `proxy_url`, and `base_url` from the selected OAuth auth data. If the auth provides `base_url`, it overrides the configured default.

## Management

When `management_enabled` is true, the plugin exposes:

- `GET /v0/management/plugins/codex-http2-keepalive/status`
- `POST /v0/management/plugins/codex-http2-keepalive/close-idle`

Both routes use Management API authentication. The status JSON shows the pool key, upstream host, masked proxy, HTTP/2 state, `ReadIdleTimeout`, `PingTimeout`, request counters, active streams, and the last connection or PING error.

## Notes

This plugin does not use `HostHTTPClient`. It owns the outbound network stack and logs its own request lifecycle with secrets redacted.

The executor provider identifier is `codex`, so OAuth credentials are selected from the native Codex credential pool. An auth record may override the configured response path, but not the configured HTTPS origin.
