# Orchestration resource adapters

Resource controllers import `task`, `collab` and `autonomy` from
`github.com/yurika0211/luckyagent-orchestrator`. The scheduling engines are
maintained in that independent module. The API owns HTTP request parsing,
response rendering and route contracts; the host provides actual Agent
execution and session provisioning.

The `resources.Options.Registry` and `Delegates` fields use orchestrator
collaboration types. Runtime task/autonomy ports use the same orchestrator
types. Old tools subpackage paths remain aliases to these types, so consumers
can migrate imports without maintaining duplicate state implementations.

Routes, JSON fields, task tree relationships, queue snapshots and WebSocket
events are unchanged. No configuration keys are added.

The initial orchestrator dependency is `v0.1.0`. The module requirements and
checksums pin downloadable source, and normal builds do not require a workspace.
For joint development, use an ignored workspace with matching API, tools,
gateway and orchestrator checkouts. Publish orchestrator before updating
tools/API releases and the host pins; do not commit local paths in `go.mod`.
