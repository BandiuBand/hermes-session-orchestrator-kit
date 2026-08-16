# Hermes Session Orchestrator Kit

Run a Hermes orchestrator and persistent worker sessions on one local LLM without making them compete for VRAM.

Hermes normally persists conversations, but a long autonomous task can still lose its place after context compaction, or multiple agent processes can accidentally invoke the same local model at once. This project adds a small external supervisor that treats a Hermes session as durable agent identity and a Hermes process as a disposable execution slot.

The orchestrator delegates a task and ends its turn. The supervisor then starts one worker turn, captures its result, stops that process, and resumes the original orchestrator session. Only one scheduled LLM turn owns the global `LLM_SLOT` at a time.

## Why use it?

- Delegate work without asking the user to invent or manage worker names.
- Preserve separate, resumable Hermes histories for the orchestrator and workers.
- Prevent two Hermes processes from sending competing requests to one local model.
- Resume the orchestrator automatically after a worker completes, fails, or times out.
- Export the complete timed-out worker session to JSONL and give the orchestrator its path.
- Keep LM Studio in charge of model variants, context length, GPU offload, and other load parameters.
- Show live worker/orchestrator activity in a visible console on Windows.
- Optionally switch mutually exclusive GPU services around an external command.

## Current status

The CLI-adapter path has been tested end-to-end with Hermes Agent 0.19.0 on Windows and LM Studio. The repository includes 31 automated tests covering session release, exclusive scheduling, detached delegation, automatic worker creation, timeout export/recovery, model selection, and failure delivery.

This is community software, not an official Nous Research project. Read [ARCHITECTURE.md](ARCHITECTURE.md) before extending the control plane.

## How it works

1. The orchestrator invokes `delegate "task" --detached --resume-origin`.
2. The supervisor stores an `accepted` job and immediately returns `HANDOFF_ACCEPTED`.
3. The orchestrator ends its response. A Hermes `post_llm_call` shell hook releases that exact job.
4. A detached supervisor process acquires the SQLite-leased `LLM_SLOT` and starts or resumes the worker.
5. After the worker finishes, the worker process exits and the supervisor resumes the persistent origin session with `[WORKER_RESULT]`.
6. The orchestrator validates the result and may continue, resume that worker with feedback, or delegate another task.

If a worker exceeds its wall timeout, the supervisor stops it, exports the complete session to `artifacts/<job-id>/worker-session-<session-id>.jsonl`, and resumes the orchestrator with the export path. The transcript is not duplicated into the prompt. If an orchestrator turn itself is forcibly stopped after creating another handoff, the supervisor emits the missing release so the accepted child job is not stranded.

## Requirements

- Python 3.10 or newer
- Hermes Agent with persistent CLI sessions and shell hooks
- `pytest` only for development/testing
- Optional: LM Studio CLI (`lms`) when using `"model": "auto"`
- Windows for visible `CREATE_NEW_CONSOLE` windows; the scheduling core is otherwise platform-neutral

Hermes references: [sessions](https://hermes-agent.nousresearch.com/docs/user-guide/sessions), [event hooks](https://hermes-agent.nousresearch.com/docs/user-guide/features/hooks/), and [skills](https://hermes-agent.nousresearch.com/docs/guides/work-with-skills/).

## Install the supervisor

Clone the repository and create a local configuration. Never commit `supervisor/config.json`; it is intentionally ignored.

### Windows PowerShell

```powershell
git clone https://github.com/BandiuBand/hermes-session-orchestrator-kit.git
cd hermes-session-orchestrator-kit
Copy-Item supervisor/config.example.json supervisor/config.json
python supervisor/handoffctl.py init
python -m pytest -q
```

### Linux/macOS

```bash
git clone https://github.com/BandiuBand/hermes-session-orchestrator-kit.git
cd hermes-session-orchestrator-kit
cp supervisor/config.example.json supervisor/config.json
python supervisor/handoffctl.py init
python -m pytest -q
```

Edit `supervisor/config.json` if `hermes` or `lms` is not on `PATH`. Use argv arrays, for example:

```json
{
  "hermes": {
    "adapter": "cli",
    "cli_argv": ["C:/path/to/hermes.exe"],
    "model": "auto",
    "lm_studio_cli_argv": ["C:/path/to/lms.exe"]
  }
}
```

## Model selection: no saved model settings

The supervisor supports exactly two policies:

- Set `hermes.model` to a concrete model key such as `publisher/model-name`.
- Set it to `"auto"`; the supervisor asks LM Studio for the single currently loaded LLM and passes only that model's `modelKey` to Hermes.

The supervisor does **not** save or replay the LM Studio variant, context length, quantization, GPU offload, parallelism, or other load parameters. Change those freely in LM Studio. Auto mode refuses to guess when zero or multiple LLMs are loaded, which prevents Hermes from silently loading a remembered model over the one you selected.

## Connect it to Hermes

The short version is:

1. Install/copy the `session-handoff` skill.
2. Give its bundled script a local `config.json`.
3. Add one `post_llm_call` hook to Hermes.
4. Validate the hook.
5. Start a fresh Hermes session and ask it to use `session-handoff`.

The complete Windows and POSIX procedure is in [docs/HERMES_SETUP.md](docs/HERMES_SETUP.md).

After setup, a normal request can be:

```text
Inspect this repository, split the remaining work into appropriate worker tasks,
supervise their results, and continue until the acceptance criteria pass.
Use session-handoff.
```

The user does not create a worker. The skill instructs the orchestrator to run:

```text
handoffctl.py delegate "the complete worker task" --detached --resume-origin
```

The supervisor generates an opaque alias and persistent Hermes session automatically.

## Useful commands

Run these from the repository root; omit `--config` when `config.json` is beside the script, or set `HANDOFF_CONFIG`.

```bash
python supervisor/handoffctl.py delegate "Run the test suite and fix the first failure" --detached --resume-origin
python supervisor/handoffctl.py resume-worker worker-20260101-120000-abcd1234 "Apply the review feedback" --detached --resume-origin
python supervisor/handoffctl.py inquire worker-20260101-120000-abcd1234 "Why this design?" --detached --resume-origin
python supervisor/handoffctl.py list-jobs
python supervisor/handoffctl.py list-sessions
python supervisor/handoffctl.py list-locks
```

## GPU service switching

`gpu-run` can stop the Hermes inference service, wait for a GPU memory guard, start an application LLM, run a command, reverse the services, and resume the origin. Service commands are deliberately configuration-driven because deployments may use LM Studio, Docker, systemd, llama.cpp, or vLLM.

Keep `gpu.enabled` false until the `services` argv arrays have been adapted and tested for your machine.

## Repository layout

```text
ARCHITECTURE.md                 Design specification and lifecycle details
docs/HERMES_SETUP.md            Hermes skill + post-turn hook setup
session-handoff/                Installable Hermes skill source
supervisor/handoffctl.py        Supervisor and CLI
supervisor/config.example.json  Portable configuration template
tests/test_supervisor.py        Automated tests
```

Runtime databases, logs, exported sessions, local configuration, caches, and generated archives are excluded from Git.

## Safety

- Commands are executed as argv arrays with `shell=False`.
- Shell hooks run with the current user's permissions; review the exact hook command before approving it.
- Do not publish `.state`, `artifacts`, session JSONL exports, `config.json`, `.env`, or Hermes credentials.
- Test GPU stop/start commands manually before enabling `gpu-run`.
- Keep backups of important workspaces. This supervisor schedules agents; it does not sandbox their filesystem tools.

## Development

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
```

When changing the supervisor, keep `supervisor/handoffctl.py` and `session-handoff/scripts/handoffctl.py` byte-identical.

## License

[MIT](LICENSE)
