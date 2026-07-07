# zot-loop

Small automation helpers for zot: interval reminders, background command monitors, and a lightweight task list. It runs directly from Go source through `go run .`, so there is no build step for normal use.

## Install

```bash
zot ext install .
```

For local iteration, load it without installing:

```bash
zot --ext .
```

## Quick examples

```text
LoopCreate trigger="5m" prompt="Check if the build passed"
LoopCreate trigger="tool_call" triggerType="event" prompt="Review the latest tool use"
LoopCreate trigger="1h" triggerType="hybrid" eventSource="turn_end" prompt="Review progress" maxFires=6
LoopList
LoopDelete id="1"
```

```text
MonitorCreate command="tail -n0 -f build.log" description="Watch build output"
MonitorCreate command="go test ./..." onDone="Summarize the test result"
MonitorList
MonitorOutput monitorId="1" lines=50
MonitorStop monitorId="1"
```

```text
TaskCreate subject="Fix deploy polling" description="Replace polling with a scheduled reminder"
TaskList
TaskUpdate id="1" status="in_progress"
TaskPrune
TaskDelete id="1"
```

## Slash commands

`/loop [interval] [prompt]` creates or lists reminders.

```text
/loop
/loop 5m check the deploy
```

When opened without arguments, `/loop` shows an interactive panel:

- `↑/↓` select a loop
- `enter` or `r` run the selected loop now
- `p` pause or resume
- `d` delete
- `esc` close

`/tasks` shows tasks. With text after the command, it creates a task.

```text
/tasks
/tasks Write README updates
```

When opened without arguments, `/tasks` shows an interactive panel:

- `↑/↓` select a task
- `enter` ask the agent to work on it
- `s` mark in progress
- `x` mark done
- `d` delete
- `esc` close

## Tools

| Tool | Purpose |
|---|---|
| `LoopCreate` | Schedule a prompt on an interval such as `30s`, `5m`, `2h`, `1d`, on a zot event, or as a hybrid interval plus event loop |
| `LoopList` | Show configured reminders with IDs, status, fire count, and next-fire time |
| `LoopDelete` | Delete, pause, or resume a reminder |
| `MonitorCreate` | Run a background shell command and retain recent output lines |
| `MonitorList` | Show monitors with status, uptime, and output line count |
| `MonitorOutput` | Read recent captured monitor output |
| `MonitorStop` | Stop a running monitor with SIGTERM, then SIGKILL after 5 seconds if needed |
| `TaskCreate` | Add a lightweight task |
| `TaskList` | List tasks, optionally filtered by status |
| `TaskUpdate` | Change task status, subject, or description |
| `TaskDelete` | Remove a task |
| `TaskPrune` | Delete completed tasks |

## Storage

The extension stores `state.json` in its zot extension data directory. Reminder and task state persists across extension reloads. Monitor processes are runtime-only and are stopped on shutdown.

## Docker

Docker is not needed here. zot launches extensions as subprocesses, and this manifest runs:

```json
"exec": "go",
"args": ["run", "."]
```

That is enough as long as Go is installed and available on `PATH`. Docker would only be useful if you wanted an isolated, reproducible development shell or CI environment.

## Development

```bash
go test ./...
zot --ext .
```

Use `/reload-ext` inside zot after editing the Go files.

## Limits

- 25 active reminders
- 25 monitors
- intervals use Go duration-style values plus days: `30s`, `5m`, `2h`, `1d`
- five-field cron expressions are supported, for example `0 9 * * 1-5`
- event loops can listen to `turn_start`, `turn_end`, `tool_call`, `assistant_message`, and `monitor:done`
- loop fires submit prompts back into zot through the extension host, so scheduled and event loops can re-wake the agent

## License

MIT
