# Connecting Hermes Agent

This guide connects Hermes Agent to the session supervisor through an installable skill and one `post_llm_call` shell hook.

The hook matters because a detached worker must not begin while the orchestrator is still generating or using tools. `HANDOFF_ACCEPTED` is stored immediately, but the worker is released only after Hermes finishes the origin turn.

## 1. Confirm your Hermes installation

```bash
hermes --version
hermes config path
hermes hooks doctor
```

The tested version is Hermes Agent 0.19.0. Newer versions should work while these interfaces remain available:

- `hermes chat --resume <session> --query <prompt>`
- `hermes sessions list`
- the `post_llm_call` shell-hook event

## 2. Configure the supervisor

From the cloned repository:

```powershell
# Windows
Copy-Item supervisor/config.example.json supervisor/config.json
python supervisor/handoffctl.py init
```

```bash
# Linux/macOS
cp supervisor/config.example.json supervisor/config.json
python supervisor/handoffctl.py init
```

Edit the local `supervisor/config.json`:

- `hermes.cli_argv`: Hermes executable argv, normally `["hermes"]`.
- `hermes.model`: one explicit model key or `"auto"`.
- `hermes.lm_studio_cli_argv`: normally `["lms"]` when auto mode is used.
- `supervisor.show_console`: `true` opens a live supervisor window on Windows.
- timeout values: increase them for large local models with slow prompt ingestion.

Do not add LM Studio load arguments to the supervisor. It intentionally uses only the model key and leaves the loaded variant and parameters under LM Studio's control.

## 3. Install the skill

```bash
hermes skills install https://raw.githubusercontent.com/BandiuBand/hermes-session-orchestrator-kit/main/session-handoff/SKILL.md --category workflow --yes
```

Hermes installs `SKILL.md` and its referenced `scripts/` and `references/` files. Skills take effect in a new session.

For a development copy, copy the entire `session-handoff` directory into:

```text
<Hermes home>/skills/workflow/session-handoff/
```

The exact Hermes home depends on the installation/profile. `hermes config path` shows the active configuration location; the `skills` directory is under the same Hermes home in standard installations.

## 4. Give the installed skill its local config

The bundled script looks for `config.json` beside itself unless `HANDOFF_CONFIG` is set.

Copy `session-handoff/scripts/config.example.json` to:

```text
<installed session-handoff>/scripts/config.json
```

Then adapt the executable argv and timeouts in that copy. Alternatively set `HANDOFF_CONFIG` to the absolute path of the repository's `supervisor/config.json` and keep one shared configuration.

Verify the installed entry point:

```bash
python <installed-session-handoff>/scripts/handoffctl.py init
python <installed-session-handoff>/scripts/handoffctl.py list-locks
```

## 5. Add the Hermes post-turn hook

Open the active config:

```bash
hermes config edit
```

Merge one of these blocks into the YAML, using absolute paths. Keep the command on one line.

### Windows

```yaml
hooks:
  post_llm_call:
    - command: 'python C:\absolute\path\session-handoff\scripts\handoffctl.py --config C:\absolute\path\session-handoff\scripts\config.json turn-complete'
      timeout: 30
```

If a path contains spaces, quote each path inside the command:

```yaml
hooks:
  post_llm_call:
    - command: 'python "D:\My Tools\session-handoff\scripts\handoffctl.py" --config "D:\My Tools\session-handoff\scripts\config.json" turn-complete'
      timeout: 30
```

### Linux/macOS

```yaml
hooks:
  post_llm_call:
    - command: 'python /absolute/path/session-handoff/scripts/handoffctl.py --config /absolute/path/session-handoff/scripts/config.json turn-complete'
      timeout: 30
```

Hermes sends the hook event as JSON on stdin. `turn-complete` reads its `session_id` and releases only jobs accepted by that origin session.

## 6. Validate and approve the hook

```bash
hermes config check
hermes hooks list
hermes hooks doctor
hermes hooks test post_llm_call
```

Hermes may ask for first-use approval for the exact shell-hook command. Review and approve that command. If you later change its path or arguments, re-run the checks because the allowlist is command-specific.

## 7. Start the orchestrator

Open a fresh Hermes session in the actual project working directory. Give it the high-level goal and explicitly request the skill:

```text
You are the orchestrator for this workspace. Break the goal into worker tasks,
delegate them, validate their results, and redirect them when needed.
Use session-handoff.
```

Hermes should invoke `delegate` with the full task and no manually invented worker name. After it receives `HANDOFF_ACCEPTED`, it must end the current response immediately. The post-turn hook releases the detached supervisor, which creates the worker automatically.

## 8. Observe and troubleshoot

```bash
python <installed-session-handoff>/scripts/handoffctl.py list-jobs
python <installed-session-handoff>/scripts/handoffctl.py list-sessions
python <installed-session-handoff>/scripts/handoffctl.py list-locks
hermes sessions list --source tool --limit 10
hermes hooks doctor
```

On Windows, `show_console: true` opens a console containing live Hermes output for each detached job.

Common symptoms:

- **Job remains `accepted`:** check the `post_llm_call` hook, its approval, and `origin_release_timeout_seconds`.
- **A second model loads in LM Studio:** use `"model": "auto"` or the exact loaded model key. Confirm exactly one LLM is loaded. The supervisor never restores variants or parameters.
- **Large model times out during prompt ingestion:** increase `worker_turn_timeout_seconds`, `delivery_timeout_seconds`, and keep `origin_release_timeout_seconds` longer than the maximum origin turn.
- **Worker finished but origin did not resume:** inspect `show-job <job-id>` and verify the origin still exists in `hermes sessions list`.
- **Timeout recovery:** read the complete file at `worker_session_export_path`; do not rely on a short summary.

## Uninstall

Remove the `post_llm_call` entry from the Hermes config, then remove the installed skill:

```bash
hermes skills uninstall session-handoff
```

The supervisor state database and exported session artifacts are not deleted automatically. Remove those only after confirming they are no longer needed.
