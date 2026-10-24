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

## Cron writes

`GET /api/v1/cron` keeps the existing list envelope. Each job gains an optional
`expression` field. `schedule` remains `cron.DescribeSchedule` and is still the
human text Android already displays.

| Method | Path | Status | Body |
| --- | --- | --- | --- |
| GET | `/api/v1/cron` | 200 | `{"running":bool,"count":int,"jobs":[Job]}` |
| POST | `/api/v1/cron` | 201 | one `Job` |
| GET | `/api/v1/cron/{id}` | 200 | one `Job` |
| PATCH | `/api/v1/cron/{id}` | 200 | one `Job` |
| DELETE | `/api/v1/cron/{id}` | 204 | empty |

Create body:

```json
{"name":"","description":"","schedule":"每天9点","mode":"agent|shell","command":"","metadata":{"session_id":"","platform":"android"},"delete_after_run":false}
```

`schedule` is parsed with `cron.ParseNaturalLanguage` first, then a 5-field
cron expression. Neither match returns 400 with the parser error. `mode`
defaults to `shell`. `command`, `session_id` and `platform` are required so the
host can deliver the result back to the session. The server generates the job
id with the same slug-plus-unix-seconds shape as the cron tool. The raw request
string is stored as metadata `schedule_text`.

PATCH body is `{"action":"pause"}` or `{"action":"resume"}`.

```json
{"id":"","name":"","description":"","schedule":"每天09:00","expression":"0 9 * * *","status":"idle","last_run":null,"next_run":"","run_count":0,"error_count":0,"last_error":"","created_at":"","updated_at":"","delete_after_run":false,"metadata":{}}
```

`schedule` stays `cron.DescribeSchedule`. For a parsed 5-field expression that
uses a step, that human field is the expanded field list, for example
`*/15 * * * *` displays as `0,15,30,45 * * * *`. That display is unchanged.

`expression` is the raw expression Android can show on a detail page:

- a 5-field request, including steps such as `*/15 * * * *`, is returned
  unchanged from metadata `schedule_text`;
- `每天9点` has no original 5-field text, so `expression` is the persisted
  reconstruction `minute hour * * *`, for example `0 9 * * *`;
- interval and once schedules omit `expression`.

Empty `last_run` and `next_run` are omitted, matching the previous list response.

Missing job is 404. Illegal schedule, mode, command, session or action is 400.
A duplicate id is 409. Engine or writer missing is 503. Other methods are 405.
Authentication is the mux-level `Middleware.Protect` used by every other route;
these handlers do not add a second check.

The API module does not import the Agent. `resources.CronJobWriter` is the host
port. `luckyagent-app` implements it by calling `Agent.AddCronJob`,
`PauseCronJob`, `ResumeCronJob` and `RemoveCronJob`, which build the task and
persist the job. A host that only implements `CronEngine()` can still list jobs.

The initial orchestrator dependency is `v0.1.0`. The module requirements and
checksums pin downloadable source, and normal builds do not require a workspace.
For joint development, use an ignored workspace with matching API, tools,
gateway and orchestrator checkouts. Publish orchestrator before updating
tools/API releases and the host pins; do not commit local paths in `go.mod`.
