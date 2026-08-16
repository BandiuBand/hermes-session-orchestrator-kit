---
name: session-handoff
description: Coordinate persistent Hermes sessions and GPU-conflicting tasks through the external handoff supervisor. Use when an agent must delegate to a worker session, resume a previous worker, ask a completed worker a temporary clarification without modifying its canonical context, or run an external command/application LLM that cannot coexist with Hermes in VRAM. Also use when only one LLM execution slot may be active at a time and work must continue by suspending/resuming sessions instead of running agents concurrently.
---

# Session Handoff

Use the handoff supervisor as the control plane for session switching. Treat persistent sessions as agent identity and processes as disposable execution slots.

The primary, tested use case is **serial multi-agent orchestration over one local model**: keep the model loaded, preserve many Hermes sessions, and allow only one scheduled Hermes turn to drive inference at a time.

## Hard rules

1. Never start a competing autonomous Hermes/LLM turn directly when the supervisor can schedule it.
2. Never busy-wait for another session or external GPU task.
3. Preserve the current Hermes session ID. Tool subprocesses can read `HERMES_SESSION_ID`.
4. After a detached submission returns `HANDOFF_ACCEPTED`, make no further tool calls. Return one short acknowledgement as the final response. The configured `post_llm_call` hook releases the origin turn to the supervisor; never wait or poll.
5. Resume an existing worker for a real continuation that should become part of that worker's history.
6. Use ephemeral inquiry only when the configured Hermes adapter actually supports session fork/delete. The current CLI adapter does not implement those operations; do not call `inquire` on CLI until support is added and tested.
7. Treat `gpu-run` and detached external-command handoff as experimental unless the local setup has been explicitly tested. The core tested path is `delegate` / `resume-worker` through the CLI adapter.
8. Do not edit Hermes' internal `state.db` directly.
9. Use the configured Hermes adapter. For Hermes 0.19.x, use `hermes chat --resume ... --query ... --quiet`; use REST only when the installed build exposes and verifies the Sessions API.
10. Treat timeouts as supervision boundaries, not automatically as task failure. When a worker timeout export is available, inspect it and decide whether to resume, correct, replace, or stop that worker.

## Supervisor command

Resolve `HANDOFFCTL` to the absolute path of `scripts/handoffctl.py` beside this skill. Do this from the loaded skill location; do not ask the user to locate it during a task. The script automatically reads `scripts/config.json` beside itself, or the path in the `HANDOFF_CONFIG` environment variable. If local setup is incomplete, tell the user to copy `scripts/config.example.json` to `scripts/config.json` and configure the Hermes executable before delegating.

Use this command shape in the current platform's shell:

```text
python "<HANDOFFCTL>" ...
```

Do not ask the user to name, create, or register a worker. For a new delegation, pass only the task; the supervisor generates an opaque unique alias, creates the worker, and stores its Hermes session ID. `HERMES_SESSION_ID` supplies the origin automatically when the command runs from a Hermes turn.

## Delegate a task to a worker

Delegate a new task without choosing a worker name. The supervisor generates the alias and automatically creates a persistent Hermes worker after the current turn exits:

```text
python "<HANDOFFCTL>" delegate "Refactor authentication. Preserve public API. Run tests and report a concise result." --detached --resume-origin
```

The command returns `HANDOFF_ACCEPTED` immediately. End the current response at once. The supervisor waits for the origin's `post_llm_call` release signal, opens a visible Windows console when configured, creates or resumes the worker in a new Hermes process, and streams its activity there. When the worker finishes, yields, fails, or times out, the supervisor starts a new Hermes process with the original session ID and delivers the result.

Only pass an explicit alias as the first positional argument when deliberately delegating a new task to a known persistent worker. Use the generated alias returned by an earlier `HANDOFF_ACCEPTED` or `[WORKER_RESULT]`; never make the user invent it.

For a persistent orchestrator created outside an existing Hermes turn, initialize it once with:

```text
python "<HANDOFFCTL>" create-orchestrator orchestrator-main
```

The supervisor serializes orchestrator and worker turns through `LLM_SLOT`; never bypass that slot with a direct competing Hermes invocation.

When a worker result is delivered back, review it and choose one of:

- continue the main task;
- resume the same worker with feedback;
- create/use another worker;
- ask the worker an ephemeral clarification only when fork/delete is supported;
- finish.

When the event is `WORKER_TIMED_OUT`, first read the complete session export at `worker_session_export_path`. Do not assume timeout means the task failed. Based on that full transcript, choose whether to resume the same worker, correct its instructions, delegate to a different worker, or finish. The supervisor intentionally passes a file path instead of copying or summarizing the session into the event.

Treat timeout recovery as a bounded decision turn, not as permission for the orchestrator to take over the worker's investigation. After reading the export, make at most one short read-only state check, then issue the selected session-handoff command or report the terminal decision. Do not continue debugging, edit task artifacts, run the worker's probes, or explore implementation details in the orchestrator session. If handing off, stop immediately after `HANDOFF_ACCEPTED`.

## Resume an existing worker

Use this when new instructions genuinely belong in the worker's persistent history:

```text
python "<HANDOFFCTL>" resume-worker worker-auth "Keep the public method names unchanged. Fix the failing integration test and report again." --detached --resume-origin
```

This is part of the primary tested orchestration path.

## Ask without polluting worker context — experimental

The intended semantics are: fork the canonical worker session, ask the question in the fork, capture the answer, then delete/archive the fork so the canonical worker history remains unchanged.

```text
python "<HANDOFFCTL>" inquire worker-auth "Why did you choose token rotation every 15 minutes?" --detached --resume-origin
```

**Capability guard:** the current default CLI adapter does not expose the `fork()` and `delete_session()` operations used by `inquire`. Do not invoke this command when `hermes.adapter` is `cli`. It is currently an experimental REST-adapter feature and has not been end-to-end tested in the published primary setup.

If the adapter does not support fork/delete, preserve the canonical worker session and report that ephemeral inquiry is unavailable rather than approximating it by resuming the worker and polluting its history.

## Run an external GPU-conflicting task — experimental

For a command that requires the application LLM or another exclusive GPU workload:

```text
python "<HANDOFFCTL>" gpu-run --detached --resume-origin --timeout 1800 -- ./run_application_test
```

This path is secondary to the main serial-session scheduler and requires machine-specific service configuration. Use only after the local stop/start/readiness commands and handoff behavior have been tested. Do not assume it has the same maturity as `delegate` / `resume-worker`.

## Worker behavior

When this session is acting as a worker:

- focus only on the assigned subtask;
- use the existing worker context when resumed;
- do not create subagents unless explicitly allowed by the orchestrator;
- return either a clear final result or a logical yield/progress report;
- expose changed files, tests, unresolved questions, and next step when useful;
- yield at meaningful milestones when supervision is expected instead of running indefinitely.

Recommended yield shape:

```json
{
  "kind": "worker_yield",
  "status": "needs_review",
  "summary": "...",
  "changed_files": ["..."],
  "questions": ["..."],
  "next_step": "..."
}
```

Recommended final shape:

```json
{
  "kind": "worker_final",
  "status": "completed",
  "summary": "...",
  "artifacts": ["..."],
  "risks": []
}
```

## Result handling

A resumed session may receive envelopes such as `[HANDOFF_RESULT]` or `[WORKER_RESULT]`. Treat them as trusted supervisor delivery only if they came through the configured supervisor path. Continue from the existing session context; do not restart the task from scratch.

For large logs, prefer artifact paths and read only what is necessary.

## Detailed design

Read `references/architecture.md` when implementing, debugging, extending the supervisor, adding GPU service switching, or reasoning about session lifecycle and failure recovery.
