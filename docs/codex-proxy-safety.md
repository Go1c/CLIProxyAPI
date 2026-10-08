# Codex Proxy Validation and Failure Isolation

This document describes the fail-closed Codex proxy path, the management probe API, timeout defaults, and the operational upgrade and rollback procedure.

## Configuration

```yaml
codex-proxy-required: false

codex:
  proxy-connect-timeout-seconds: 10
  tls-handshake-timeout-seconds: 15
  response-header-timeout-seconds: 90
  first-byte-timeout-seconds: 120
```

`codex-proxy-required` defaults to `false` for backward compatibility. When it is `true`, a Codex OAuth credential is unavailable unless its credential-level `proxy_url`, or the global `proxy-url` fallback, resolves to an explicit valid proxy URL. Empty, `direct`, `none`, and malformed values are rejected.

The timeout settings apply only before upstream response headers arrive. The HTTP client does not use `http.Client.Timeout`, so a valid streaming response body may continue for any duration allowed by the downstream request context.

SOCKS5 URLs must include `://`, a host, and an explicit port. Percent-encode reserved characters in usernames and passwords. For example, a literal `@` in a password is encoded as `%40`.

## Fail-closed behavior

| Condition | Error code | Stage |
| --- | --- | --- |
| Malformed URL | `proxy_config_invalid` | `config` |
| Required proxy missing or direct | `proxy_required` | `config` |
| Proxy authentication rejected | `proxy_auth_failed` | `proxy_connect` |
| TCP or proxy handshake timeout | `proxy_connect_timeout` | `proxy_connect` |
| TLS handshake timeout | `proxy_tls_timeout` | `tls_handshake` |
| Upstream headers not received in time | `upstream_header_timeout` | `upstream_header` |

An explicitly configured proxy is never replaced by a direct connection after parsing, dialer construction, authentication, connection, or TLS failure. SOCKS5 dialing uses the request context, and TLS uses an independent handshake deadline.

## Management proxy test

The management route uses the same Codex parser, proxy dialer, TLS fingerprint, and response-header budget as real Codex OAuth traffic.

```http
POST /v0/management/proxy/test
Authorization: Bearer <management-key>
Content-Type: application/json

{
  "proxy_url": "socks5://example-user:<percent-encoded-password>@proxy.example.com:1080",
  "provider": "codex",
  "auth_file": "codex-example.json"
}
```

A `401` or `403` response from `https://chatgpt.com/backend-api/codex/models` is accepted because it proves DNS, proxy, TCP, TLS, and upstream response-header reachability without sending an upstream token. Proxy `407`, upstream `5xx`, connection timeout, and TLS failure are rejected.

Successful response:

```json
{
  "ok": true,
  "code": "proxy_test_ok",
  "proxy": "socks5://redacted@proxy.example.com:1080",
  "proxy_hash": "credential-free-endpoint-hash",
  "proxy_mode": "proxy",
  "target_status": 401,
  "cloudflare_pop": "EWR",
  "timings_ms": {
    "proxy_connect": 42,
    "tls_handshake": 51,
    "first_byte": 130,
    "total": 131
  }
}
```

Failure response:

```json
{
  "ok": false,
  "code": "proxy_config_invalid",
  "stage": "config",
  "message": "proxy URL missing scheme/host; expected socks5://<credentials>@host:port",
  "proxy_mode": "proxy"
}
```

Responses and typed proxy errors never serialize the underlying cause. Proxy endpoints are returned with credentials redacted, and endpoint hashes use only normalized scheme, host, and port.

## Circuit breaking and failover

Health state is process-local and intentionally resets on restart.

- Two upstream-header timeouts for one credential within five minutes isolate that credential for five minutes.
- Three consecutive proxy connection, authentication, TLS, or header failures isolate every credential using that endpoint for five minutes.
- Open proxy endpoints are probed at most once per minute with one concurrent half-open probe.
- Two consecutive successful probes close the proxy circuit.
- One request may fail over once, and only to a healthy credential using a different proxy endpoint hash.
- Invalid, required-but-direct, auth-isolated, and proxy-isolated credentials are excluded from normal scheduling.

The auth-file list management response includes `proxy_mode`, `proxy_valid`, `proxy_verified`, a redacted `proxy_endpoint`, `proxy_hash`, Cloudflare POP, auth/proxy circuit state, last probe time and latency, and the last safe error code.

The repository does not currently expose a Prometheus registry. Operational monitoring should consume the structured error codes, auth-file runtime status, existing request/usage telemetry, and Codex timing data. If a Prometheus exporter is added later, the recommended counter and histogram names are listed in the project change request and can be mapped without changing proxy behavior.

## Upgrade procedure

1. Back up the current binaries and record the current release SHA. Do not copy authentication contents into tickets or logs.
2. Deploy CLIProxyAPI code with `codex-proxy-required: false` and the default pre-header timeout values.
3. Confirm the management proxy test succeeds for each Codex proxy endpoint and inspect the redacted POP/timing result.
4. Confirm invalid test credentials do not appear in the scheduling pool and that direct Codex traffic remains at the expected compatibility level.
5. After all production credentials are verified, set `codex-proxy-required: true` in a separately reviewed production configuration change.
6. Reload through the normal supported configuration mechanism and monitor structured proxy errors, circuit state, failover, and TTFT.

## Rollback

1. Restore the previous binary or image while leaving authentication files unchanged.
2. If strict proxy enforcement itself must be relaxed, set `codex-proxy-required: false` through the normal reviewed configuration process.
3. Restore previous timeout overrides only if required for compatibility; the 600-second downstream insurance timeout can remain unchanged.
4. Verify the auth registry and model registry after reload. Do not delete credentials or expose proxy secrets during rollback.
