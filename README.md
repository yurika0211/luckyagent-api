# LuckyAgent API

An independent Go module owning LuckyAgent's wire protocols and reusable server
transport. Application operations are supplied by the embedding host.

```text
github.com/yurika0211/luckyagent-api
```

The root package is named `transport` and provides:

- HTTP defaults, a typed route-handler catalog and server construction.
- Header-based API-key authentication, local-only behavior when no key is
  configured, an optional host-owned pairing-key validator, CORS, rate limiting,
  panic recovery and request logging.
- JSON/error responses and SSE error framing with flushing support.
- `websocket`: wire messages, clients, session broadcast, keepalive and connection
  management. Run persistence, tool approval and Agent execution stay in the host.
- `health`: health check registration and reports.
- `grpc`: protobuf messages, client, service contract and gateway registration.

It never imports the LuckyAgent runtime. The `luckyagent-gateway/turn` package
supplies shared neutral event/usage DTOs; importing it does not compile every
platform adapter.

## Embed transport in a Go application

```go
package main

import (
    "net/http"

    transport "github.com/yurika0211/luckyagent-api"
)

func main() {
    cfg := transport.DefaultConfig()
    limiter := transport.NewLimiter(cfg.RateLimit)
    defer limiter.Stop()
    policy := transport.NewMiddleware(cfg, limiter, nil, nil)
    mux := http.NewServeMux()
    mux.HandleFunc("/api/v1/health/live", func(w http.ResponseWriter, r *http.Request) {
        transport.SendJSON(w, http.StatusOK, map[string]string{"status": "ok"})
    })
    handler := policy.Log(policy.Authenticate(policy.RateLimit(policy.Recover(mux))))
    if err := transport.NewHTTPServer(cfg.Addr, handler).ListenAndServe(); err != nil {
        panic(err)
    }
}
```

Hosts exposing the complete LuckyAgent API use `RegisterRoutes` with the typed
`Handlers` structure. Each supplied callback implements an application
operation. Session, memory, model, tool and task policy remain outside this
module. `NewHTTPServer` does not start a goroutine: the host owns listening,
resource cleanup and graceful shutdown.

## Compatibility and tests

HTTP paths, JSON field names, WebSocket event names, protobuf field numbers and
the gRPC wire service name `luckyagent.LuckyAgentService` are preserved. Generated
Go clients and descriptors use this module's import path. Protocol sources and
their generator are in `proto/`; regenerate and version them here, rather than
keeping a second schema in the embedding application.

```bash
go test ./...
```

Tests cover authentication, CORS, SSE flushing, recovery, rate limiting,
WebSocket keepalive/disconnect behavior, protobuf identity, and the absence of
LuckyAgent runtime imports. LuckyAgent's application-level API tests remain in
its `internal/apiapp` and `internal/channelapp/websocket` packages.

During local extraction, keep this checkout next to `luckyagent-gateway` and
use an untracked `go.work` with both directories. Before publication, add a
workspace-only version replacement:

```bash
go work init . ../luckyagent-gateway
go work edit -replace=github.com/yurika0211/luckyagent-gateway@v0.1.0=../luckyagent-gateway
```

The committed `go.mod` contains release dependencies and no local replacement.
After both repositories and `v0.1.0` tags are published, verify a clean downstream
build with `GOWORK=off`. The initial extraction retains the MIT license and
attribution from LuckyAgent.
