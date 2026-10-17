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
- `websocket`: wire messages, clients, session broadcast, keepalive, connection
  management, durable run storage and reconnect replay. The host executes runs.
- `resources`: task/tree/feedback, approvals, cron, autonomy, Skill installation
  and collaboration controllers, using injected public tool-runtime ports.
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
operation. `resources.New` accepts a `Runtime` composed of task, approval, cron,
autonomy and Skill ports, plus optional collaboration managers and host policy
hooks. Its methods can be passed directly to the corresponding route fields.
JSON/error writers default to this module's standard responses. The host owns
storage opening, Agent execution, configuration checks and orchestrator session
provisioning. `NewHTTPServer` does not start a goroutine: the host owns listening,
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
go work init . ../luckyagent-gateway ../luckyagent-tools
go work edit -replace=github.com/yurika0211/luckyagent-gateway@v0.2.1=../luckyagent-gateway
go work edit -replace=github.com/yurika0211/luckyagent-tools@v0.2.1=../luckyagent-tools
```

The committed `go.mod` contains release dependencies and no local replacement.
After the repositories and `v0.2.1` tags are published, verify a clean downstream
build with `GOWORK=off`. The initial extraction retains the MIT license and
attribution from LuckyAgent.
