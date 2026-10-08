# gmsv_rhttp

Async HTTP for Garry's Mod servers. Rust, reqwest + rustls. Doesn't block Lua

## Use

`rhttp(options)` queues a request. Returns `true, id` on success, `false` otherwise

```lua
rhttp({
    url = "https://api.example.com/v1/users/42",
    headers = { ["Accept"] = "application/json" },
    timeout = 10,
    success = function(status, body, headers)
        print(status, body)
    end,
    failed = function(reason)
        print("failed: " .. reason)
    end,
})
```

HTTP errors (4xx/5xx) go to `success` check `status` yourself. Only network
and timeout failures go to `failed`

```lua
rhttp({
    url = "https://api.example.com/v1/events",
    method = "POST",
    headers = { ["Content-Type"] = "application/json" },
    body = util.TableToJSON({ event = "round_started", map = game.GetMap() }),
})
```

`rhttp_cancel(id)` cancels a queued request `rhttp_stats()` returns counters

## Options

| Field | Default |
| --- | --- |
| `url` | Required. `http`/`https` only, no `user:pass@` |
| `method` | `GET` |
| `parameters` | Query params, or form body for POST without `body` |
| `headers` | Request headers. Transport headers (`Host`, `Content-Length`, …) are rejected |
| `body` | Binary safe string, max 20 MB |
| `type` | Content type when the body came from `parameters`, else `text/plain; charset=utf-8` |
| `timeout` | 30 s, clamped to 1 s 86400 s. Covers queueing and retries |
| `retries` | Safe methods default 2, POST/PATCH default 0 max 5 |
| `retry_delay` | Base backoff, 0.25 s, clamped to 0.05-30 s |
| `success` | `function(status, body, headers)` without it the body isnt downloaded |
| `failed` | `function(reason)` |

Retries cover connection errors, timeouts and 408/429/500/502/503/504 with
exponential backoff. `Retry-After` is honoured. POST doesnt retry by default

Bad arguments call `failed` immediately and return `false`

## Limits

- 20 MB per body, 64 MB shared buffer; 256 concurrent, 1024 in flight
- Response headers reaching Lua: 128 headers / 64 KB max
- One `Think` drains at most 128 callbacks / 1 MB
- 10 redirects, `http`/`https` only. No proxy env is honoured
- Response header names arrive lowercase; repeats keep the last value

## Notes

Server-side only (`gmsv_`). Dont pass player-controlled URLs or headers
without an allowlist. Custom secret headers are forwarded on redirect
(reqwest strips only `Authorization`/`Cookie` cross-host), so don't mix them
with untrusted targets

Callbacks pump on `Think` and a repeating timer, so they arrive even while
the server hibernates. Private PKI: `RHTTP_CAINFO` points at a PEM bundle
appended to the native roots; unreadable files are skipped

## Build

```bash
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --locked --release --target x86_64-unknown-linux-gnu
```

Copy `target/x86_64-unknown-linux-gnu/release/libgmsv_rhttp.so` to
`garrysmod/lua/bin/gmsv_rhttp_linux64.dll`. Windows:
`--target x86_64-pc-windows-msvc`, copy the `.dll` to
`garrysmod\lua\bin\gmsv_rhttp_win64.dll`

## License

MIT. See [LICENSE](LICENSE)
