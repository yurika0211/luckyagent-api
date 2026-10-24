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
  and collaboration controllers, using injected public orchestration/tool ports.
  Cron list stays `GET /api/v1/cron`. Create is `POST /api/v1/cron`; pause,
  resume, get and delete use `/api/v1/cron/{id}`. Writes call the host
  `CronJobWriter` port so this module never builds the runnable task itself.
- `health`: health check registration and reports.
- `grpc`: protobuf messages, client, service contract and gateway registration.
- `workflow`: optional Gin handlers over the public tool-module DAG engine;
  the DAG implementation does not depend on Gin or the API module.

See [WebSocket run recovery](docs/websocket-recovery.md) for startup scanning,
lazy event loading and recovery compatibility.

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
    handler := policy.Log(policy.Protect(mux))
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

Task, collaboration and autonomy engines now come from the independent
`luckyagent-orchestrator` module. HTTP contracts stay here. See
[orchestration resource adapters](docs/orchestration.md) for migration details
and the `v0.1.0` engine dependency.

## Compatibility and tests

Use `Middleware.Protect` for the complete admission, authentication, credential
quota and request-body policy. Logging and optional CORS wrap that handler.
The lower-level middleware methods remain available for existing embedders.
See [HTTP and realtime reliability](docs/reliability.md) for local authentication,
body limits, atomic request acceptance, event replay and journal compatibility.

`workflow.NewServerHandlers` accepts a `luckyagent-tools/workflow.WorkflowEngine`
whose executor is configured by the host. `RegisterRoutes` preserves the
`/workflows` and `/workflow-instances` paths, methods and JSON fields. The root
transport package does not import Gin. The host retains action registration,
authentication, listening and shutdown ownership.

This adapter uses execution packages introduced after tools v0.2.2. For
joint changes, publish the orchestrator first, then tools and API, and pin their
merged commits or releases before upgrading the host. The committed module
requirement includes the workflow package and contains no local filesystem
replacement. A workspace is optional for joint development.

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
LuckyAgent runtime imports. Application-level API tests live in the
`luckyagent-app` module's `apiapp` and `channelapp/websocket` packages.

During local extraction, use an untracked `go.work` with all module roots
(adjust these paths to the actual checkout locations):

```bash
go work init . ../luckyagent-gateway ../luckyagent-tools ../luckyagent-orchestrator
```

The committed `go.mod` pins published dependency versions and contains no local
replacement. Downstream builds use `GOWORK=off`; workspaces are only needed for
local changes. The extraction retains the MIT license and attribution from
LuckyAgent.
