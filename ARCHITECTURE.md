# Hermes Session Orchestrator / GPU Handoff Supervisor

## 1. Purpose

This project is a reference implementation and implementation specification for a local, resource-aware session orchestrator for Hermes Agent.

The core problem is not ordinary multi-agent delegation. The machine has limited VRAM (for example, 2 x RTX 3090) and cannot reliably keep the Hermes LLM and another application LLM loaded at the same time. The system therefore treats **LLM execution as a scarce exclusive resource** and keeps **agent identity/context in persistent sessions rather than in long-lived processes**.

The main design rule is:

> A session may live for days. A process may live for seconds. Only the session is the agent identity.

The supervisor runs on CPU and persists its own state. It activates one session or one GPU workload at a time, then suspends it at a safe boundary and activates another.

## 2. Goals

1. Allow Hermes to hand off a long-running or GPU-conflicting operation without waiting in VRAM.
2. Resume the same Hermes session after the operation and inject the result.
3. Support an orchestrator session plus many worker sessions while allowing only one LLM execution slot at a time.
4. Preserve each worker's own context so the orchestrator can return to a worker later.
5. Let the orchestrator ask an old worker for clarification without permanently growing the worker's canonical context.
6. Support supervised work: worker -> yield -> orchestrator reviews -> worker resumes with correction.
7. Support external application tests that require unloading/stopping Hermes inference and starting another LLM service.
8. Survive supervisor restart using a local SQLite state store.
9. Avoid editing Hermes' internal SQLite database directly.
10. Keep Hermes-specific behavior behind an adapter so CLI/API changes do not infect the scheduler core.

## 3. Non-goals for the first version

- Running multiple GPU-heavy LLMs concurrently.
- Distributed scheduling across multiple machines.
- High-availability consensus.
- Direct mutation of `~/.hermes/state.db`.
- Perfect process discovery by guessing parent PIDs.
- Transparent continuation in the middle of a token stream. Switching occurs only at turn/tool boundaries.

## 4. Relevant Hermes primitives

Current Hermes documentation exposes the primitives this design needs:

- Sessions are persisted and can be resumed by ID.
- The current session ID is exported to tool subprocesses as `HERMES_SESSION_ID`.
- Hermes has a Sessions REST API with session create/read/messages/fork/chat endpoints.
- A session can be forked, matching branch semantics.

The REST Sessions API is the preferred integration because it cleanly separates persistent session state from the CLI process. A CLI adapter is retained as a fallback.

Important implementation note: do not rely on `hermes -z --resume ...` as the only resume mechanism. A 2026 issue reports that the one-shot `-z` path may ignore resume hydration in some versions. Prefer `/api/sessions/{id}/chat` or `hermes chat --resume ... -q ...`, and verify behavior against the locally installed version.

## 5. Mental model

Think of the system as a tiny operating system:

- **Session** = process image / durable agent identity.
- **Turn** = scheduled CPU/GPU timeslice.
- **Supervisor** = kernel/scheduler.
- **Hermes LLM service** = one exclusive GPU execution domain.
- **Application LLM service** = another exclusive GPU execution domain.
- **Worker session** = durable subprocess-like agent context.
- **Forked inquiry session** = copy-on-write scratch branch.

The supervisor itself must not require a GPU.

## 6. Session types

### 6.1 Orchestrator session

The orchestrator owns the global goal, decomposes work, selects workers, reviews results, and decides the next action. It should not perform expensive implementation work itself unless appropriate.

### 6.2 Canonical worker session

A worker is created for a specific task/domain. Its session persists after the task ends. It may later be resumed for a follow-up that genuinely belongs in the same worker history.

Example:

- `worker-auth-refactor`
- `worker-ui-test-harness`
- `worker-db-migration`

### 6.3 Ephemeral inquiry fork

When the orchestrator wants to ask a completed worker, "Why did you choose X?" or "What file contains Y?", create a fork from the worker's canonical session, ask in the fork, return the answer to the orchestrator, then delete/archive the fork.

This gives the desired semantics:

1. The worker answers using its full existing context.
2. The canonical worker session is not modified.
3. Repeated questions do not accumulate forever in the worker context.

This is superior to trying to manually rewind message rows in Hermes' database.

## 7. Scheduling invariant

At most one **LLM turn** controlled by this supervisor may be active at a time unless the configuration explicitly permits more.

For the target 2 x 3090 machine, use a global exclusive lock:

`LLM_SLOT = 1`

All Hermes orchestrator turns, Hermes worker turns, and application-LLM phases must acquire this slot.

The first prototype uses a SQLite-backed logical lock and a single supervisor process. Production can additionally use a filesystem `flock`.

## 8. Two kinds of handoff

### 8.1 Session-to-session handoff

Used for orchestrator <-> worker switching.

The same Hermes inference service may stay loaded if both sessions use the same model server. Only one session is submitted at a time.

Sequence:

1. Orchestrator decides to delegate.
2. It calls `handoffctl delegate ...`.
3. Tool returns `HANDOFF_ACCEPTED` quickly.
4. Orchestrator obeys the skill rule: stop making tool calls and end the turn/process.
5. Supervisor runs one worker turn.
6. Worker either finishes or yields a structured progress result.
7. Supervisor runs one orchestrator turn with that result.
8. Orchestrator may resume the same worker with feedback, start another worker, ask a scratch question, or finish.

### 8.2 GPU-domain handoff

Used when the target application itself needs a different LLM that cannot coexist in VRAM with Hermes inference.

Sequence:

1. Hermes session submits external job.
2. Supervisor acknowledges.
3. Hermes session exits or becomes inactive.
4. Supervisor stops Hermes inference service (configured command).
5. Supervisor waits until GPU memory falls below configured threshold.
6. Supervisor starts application LLM service.
7. Supervisor waits for application readiness.
8. Supervisor runs the application/test command with timeout.
9. Supervisor records stdout/stderr/exit code/duration.
10. Supervisor stops application LLM.
11. Supervisor waits for VRAM release.
12. Supervisor restarts Hermes inference service and waits for readiness.
13. Supervisor resumes the originating session with a compact result and artifact paths.

## 9. Why a skill is still useful

The skill does **not** need to teach Hermes the scheduler internals. It only needs to teach a behavioral contract:

- when to hand off;
- what information to submit;
- after `HANDOFF_ACCEPTED`, stop immediately;
- when acting as a worker, return structured progress/final status;
- do not busy-wait for another session;
- do not launch a competing LLM directly;
- use ephemeral inquiry when asking a completed worker a question that should not alter that worker's canonical history.

Keeping the skill small is important. The supervisor should enforce mechanics; the model should only express intent.

## 10. Worker lifecycle

Recommended worker states:

- `created`
- `ready`
- `running`
- `stopped`
- `yielded`
- `completed`
- `failed`
- `archived`

A worker that completes is not deleted. The session remains addressable.
If a worker turn stops because of an error or timeout and the supervisor is still
delivering that event to the orchestrator, keep the worker and job in `stopped`.
Successful control-plane recovery records the job as `stopped_delivered`; only a
failed recovery (or a failure with no requested recovery) makes `failed` terminal.

### 10.1 Worker yield protocol

For supervised tasks, the worker should not consume the entire task in one enormous autonomous run. It should yield at logical milestones.

Suggested result envelope:

```json
{
  "kind": "worker_yield",
  "status": "needs_review",
  "summary": "Implemented parser and tests; integration still pending.",
  "changed_files": ["src/parser.py", "tests/test_parser.py"],
  "questions": ["Should invalid UTF-8 be rejected or replaced?"],
  "next_step": "Integrate parser into upload endpoint"
}
```

Final result:

```json
{
  "kind": "worker_final",
  "status": "completed",
  "summary": "Task completed and tests pass.",
  "artifacts": ["..."],
  "risks": []
}
```

The prototype does not require strict JSON parsing from the model; it stores the raw assistant response too. A later version can require a schema.

## 11. Orchestrator correction loop

Example:

1. Orchestrator creates Worker A: "Implement authentication refactor. Stop after tests are green and report."
2. Worker A works and yields.
3. Supervisor resumes orchestrator with Worker A output.
4. Orchestrator notices a design issue and says: "Resume Worker A. Keep the API stable; do not rename public methods."
5. Supervisor resumes Worker A's canonical session with this correction.
6. Worker A continues with all previous context.
7. Worker A finishes.

No two LLM turns need to coexist.

## 12. Asking a completed worker without context pollution

Canonical worker session is `W`.

1. `POST /api/sessions/W/fork` -> scratch session `Wq`.
2. `POST /api/sessions/Wq/chat` with the question.
3. Capture response.
4. Send response to orchestrator.
5. Delete `Wq` or leave it archived for debugging.
6. Canonical `W` remains unchanged.

This is the recommended implementation of the user's "ask, then return the worker to exactly the state it had when the task completed" requirement.

## 13. Data model owned by the supervisor

The prototype stores its own SQLite database, separate from Hermes.

### sessions

- alias
- hermes_session_id
- role (`orchestrator`, `worker`)
- status
- parent_alias
- created_at
- updated_at
- metadata_json

### jobs

- id
- kind (`delegate`, `resume_worker`, `inquiry`, `external_command`)
- origin_session_id
- target_session_id
- state
- payload_json
- result_json
- created_at
- updated_at
- error

### events

Append-only audit trail for debugging.

## 14. Failure rules

### Supervisor crashes

Jobs remain in SQLite. On restart, jobs in `accepted`/`running` are inspectable and can be retried manually. Do not blindly rerun arbitrary external commands because they may not be idempotent.

### Hermes worker turn fails

Store the error. Resume orchestrator with `worker_failed` and logs/error details.

If the worker turn times out, stop its Hermes process, export the complete persistent worker session to a job-scoped JSONL artifact, and resume the orchestrator with `worker_timed_out` plus the absolute export path. The supervisor does not summarize or inject the transcript. The orchestrator reads the export and decides whether to resume the same worker, correct its instructions, delegate to a different worker, or finish.

### Origin delivery times out

Every result delivered to the origin includes a short reminder that the origin is an orchestrator and must perform only a bounded control-plane decision, not continue the worker's investigation. If the origin exceeds its delivery timeout, treat the forced stop as the end of that turn and release any accepted child handoffs because the normal `post_llm_call` hook cannot fire. When no child was created, resume the same origin exactly once with a compact recovery prompt. The recovery prompt relies on the result and partial checks already persisted in the Hermes session and must not repeat the large worker payload. A second timeout is terminal for that delivery attempt, except that any child handoff it created is still released.

### External command times out

Send SIGTERM, wait grace period, then SIGKILL. Persist stdout/stderr paths and timeout status.

### Application LLM fails to become ready

Stop it, attempt to restore Hermes inference, then notify origin session.

### Hermes inference fails to restart

Persist job state `blocked_restore`. Do not start another GPU workload. Operator intervention is required.

### Duplicate resume

Every job has a UUID and terminal states. A completed job must not be delivered twice unless explicitly retried.

## 15. Security model

- Never concatenate untrusted strings into a shell command.
- Store commands as argv arrays in JSON.
- `shell=False` by default.
- Keep API key in an environment variable, not in config files.
- Validate working directories.
- Add an allowlist for service start/stop commands in production.
- Do not expose the supervisor HTTP endpoint externally in v1.
- Do not edit Hermes internal state DB directly.

## 16. Recommended implementation phases

### Phase 0 — verify local Hermes capabilities

Run:

```bash
hermes --version
hermes chat --help
hermes sessions --help
```

If using API integration, enable/start Hermes API server and verify `/v1/capabilities` advertises session endpoints.

### Phase 1 — plain external handoff

Implement one origin session -> detached command -> resume origin session. No workers yet.

### Phase 2 — persistent worker sessions

Add create/resume worker by alias, one turn at a time.

### Phase 3 — orchestrator loop

Add delegate/yield/resume conventions and automatic result delivery.

### Phase 4 — ephemeral inquiry fork

Use Hermes fork endpoint to ask completed workers without modifying canonical sessions.

### Phase 5 — GPU service switching

Add configured stop/start/readiness commands for Hermes LLM and application LLM.

### Phase 6 — stronger reliability

Add filesystem lock, structured result schema, retries by policy, dashboard/log viewer, and process/service adapters (systemd/Docker/llama.cpp/vLLM/etc.).

## 17. Prototype CLI included in this kit

The supplied `supervisor/handoffctl.py` supports:

- `init`
- `register-session`
- `create-worker`
- `list-sessions`
- `chat`
- `delegate`
- `resume-worker`
- `inquire`
- `run-command`
- `show-job`
- `list-jobs`

The prototype is intentionally conservative. It uses the Hermes Sessions REST API and Python standard library only.

## 18. Important semantic limitation

The model cannot literally continue the same in-flight tool call after its process has been killed. The transparent illusion is implemented at the **conversation level**:

- turn A submits a job and ends;
- supervisor performs work;
- turn B resumes the same persistent session and supplies the tool result as a new input.

From the agent's reasoning perspective this can feel like a long asynchronous tool, but technically it is a persisted-session handoff across turns. Designing around this explicit boundary is much more robust than trying to freeze a Python process with GPU allocations in place.

## 19. Recommended language for resuming an origin session

Use a machine-readable envelope followed by concise human context:

```text
[HANDOFF_RESULT]
job_id: <uuid>
kind: external_command
status: completed
exit_code: 0
stdout_path: /.../stdout.log
stderr_path: /.../stderr.log

The external GPU task you submitted has finished. Continue the original task from the saved session state. Inspect artifact paths only if needed.
```

For worker completion:

```text
[WORKER_RESULT]
worker_alias: worker-auth-refactor
worker_session_id: ...
status: yielded

<worker final/yield response>

Review the worker output. You may resume this worker with corrections, start another worker, ask an ephemeral inquiry, or finish.
```

## 20. Suggested future abstraction: session scheduler as a tool

Once stable, expose four high-level operations only:

- `delegate(task, worker_alias?)`
- `resume(worker_alias, instruction)`
- `ask(worker_alias, question, ephemeral=true)`
- `external(command_spec)`

Everything else belongs inside the supervisor.

This gives Hermes a simple tool surface while preserving a much richer scheduler underneath.
