#!/usr/bin/env python3
"""Hermes session handoff/orchestration prototype.

Design priorities:
- persistent Hermes sessions, short-lived processes;
- one scheduled LLM turn at a time;
- no direct writes to Hermes' internal state.db;
- Hermes REST Sessions API preferred;
- ephemeral inquiry uses a fork, preserving canonical worker history.

This is a reference implementation, not a production daemon. Commands with
`--detached` launch an independent child and return a job id immediately.
All Hermes turns share a SQLite-leased global LLM slot.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import queue
import re
import shlex
import signal
import sqlite3
import subprocess
import sys
import time
import threading
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable, Optional


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def expand(path: str) -> str:
    return os.path.abspath(os.path.expanduser(path))


def load_json(path: str) -> dict[str, Any]:
    with open(expand(path), "r", encoding="utf-8") as f:
        return json.load(f)


def default_config_path() -> str:
    configured = os.environ.get("HANDOFF_CONFIG")
    if configured:
        return configured
    adjacent = pathlib.Path(__file__).resolve().with_name("config.json")
    if adjacent.exists():
        return str(adjacent)
    return "config.json"


@dataclass
class Config:
    raw: dict[str, Any]

    @property
    def db_path(self) -> str:
        return expand(self.raw.get("state_db", "~/.local/state/hermes-handoff/state.db"))

    @property
    def artifact_dir(self) -> str:
        return expand(self.raw.get("artifact_dir", "~/.local/state/hermes-handoff/artifacts"))

    @property
    def api_url(self) -> str:
        return self.raw.get("hermes", {}).get("api_url", "http://127.0.0.1:8642").rstrip("/")

    @property
    def api_key(self) -> str:
        env_name = self.raw.get("hermes", {}).get("api_key_env", "API_SERVER_KEY")
        key = os.environ.get(env_name, "")
        if not key:
            raise RuntimeError(f"Hermes API key environment variable {env_name!r} is not set")
        return key

    @property
    def hermes_adapter(self) -> str:
        return str(self.raw.get("hermes", {}).get("adapter", "cli")).lower()

    @property
    def hermes_cli_argv(self) -> list[str]:
        value = self.raw.get("hermes", {}).get("cli_argv", ["hermes"])
        if not isinstance(value, list) or not value or not all(isinstance(x, str) and x for x in value):
            raise RuntimeError("hermes.cli_argv must be a non-empty JSON argv array")
        return value


class StateStore:
    def __init__(self, path: str):
        self.path = path
        pathlib.Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(path, timeout=30)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.init_schema()

    def init_schema(self) -> None:
        self.db.executescript(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                alias TEXT PRIMARY KEY,
                hermes_session_id TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ready',
                parent_alias TEXT,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                origin_session_id TEXT,
                target_session_id TEXT,
                state TEXT NOT NULL,
                payload_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT,
                error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                event TEXT NOT NULL,
                detail_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS resource_locks (
                resource TEXT PRIMARY KEY,
                owner TEXT NOT NULL,
                acquired_at REAL NOT NULL,
                lease_until REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS worker_reservations (
                alias TEXT PRIMARY KEY,
                job_id TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS turn_releases (
                job_id TEXT PRIMARY KEY,
                origin_session_id TEXT NOT NULL,
                released_at TEXT NOT NULL
            );
            """
        )
        self.db.commit()

    def close(self) -> None:
        self.db.close()

    def upsert_session(self, alias: str, sid: str, role: str, parent_alias: Optional[str] = None,
                       metadata: Optional[dict[str, Any]] = None) -> None:
        ts = now_iso()
        self.db.execute(
            """
            INSERT INTO sessions(alias, hermes_session_id, role, status, parent_alias, metadata_json, created_at, updated_at)
            VALUES (?, ?, ?, 'ready', ?, ?, ?, ?)
            ON CONFLICT(alias) DO UPDATE SET
              hermes_session_id=excluded.hermes_session_id,
              role=excluded.role,
              status='ready',
              parent_alias=excluded.parent_alias,
              metadata_json=excluded.metadata_json,
              updated_at=excluded.updated_at
            """,
            (alias, sid, role, parent_alias, json.dumps(metadata or {}), ts, ts),
        )
        self.db.commit()

    def session(self, alias_or_id: str) -> Optional[sqlite3.Row]:
        cur = self.db.execute(
            "SELECT * FROM sessions WHERE alias=? OR hermes_session_id=?", (alias_or_id, alias_or_id)
        )
        return cur.fetchone()

    def sessions(self) -> list[sqlite3.Row]:
        return list(self.db.execute("SELECT * FROM sessions ORDER BY created_at"))

    def set_session_status(self, alias_or_id: str, status: str) -> None:
        cur = self.db.execute(
            "UPDATE sessions SET status=?, updated_at=? WHERE alias=? OR hermes_session_id=?",
            (status, now_iso(), alias_or_id, alias_or_id),
        )
        if cur.rowcount != 1:
            self.db.rollback()
            raise RuntimeError(f"session not found: {alias_or_id}")
        self.db.commit()

    def create_job(self, kind: str, origin: Optional[str], target: Optional[str], payload: dict[str, Any]) -> str:
        job_id = str(uuid.uuid4())
        ts = now_iso()
        self.db.execute(
            "INSERT INTO jobs(id,kind,origin_session_id,target_session_id,state,payload_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",
            (job_id, kind, origin, target, "accepted", json.dumps(payload), ts, ts),
        )
        self.db.commit()
        self.event(job_id, "accepted", payload)
        return job_id

    def set_job(self, job_id: str, state: str, result: Optional[dict[str, Any]] = None,
                error: Optional[str] = None) -> None:
        self.db.execute(
            "UPDATE jobs SET state=?, result_json=?, error=?, updated_at=? WHERE id=?",
            (state, json.dumps(result) if result is not None else None, error, now_iso(), job_id),
        )
        self.db.commit()
        self.event(job_id, state, result or ({"error": error} if error else {}))

    def set_job_target(self, job_id: str, target_session_id: str) -> None:
        cur = self.db.execute(
            "UPDATE jobs SET target_session_id=?, updated_at=? WHERE id=?",
            (target_session_id, now_iso(), job_id),
        )
        if cur.rowcount != 1:
            self.db.rollback()
            raise RuntimeError(f"job not found: {job_id}")
        self.db.commit()

    def reserve_worker(self, alias: str, job_id: str) -> None:
        try:
            self.db.execute(
                "INSERT INTO worker_reservations(alias,job_id,created_at) VALUES(?,?,?)",
                (alias, job_id, now_iso()),
            )
            self.db.commit()
        except sqlite3.IntegrityError:
            self.db.rollback()
            row = self.worker_reservation(alias)
            owner = row["job_id"] if row else "another job"
            raise RuntimeError(f"worker alias is already being created: {alias} (job {owner})")

    def worker_reservation(self, alias: str) -> Optional[sqlite3.Row]:
        return self.db.execute(
            "SELECT * FROM worker_reservations WHERE alias=?", (alias,)
        ).fetchone()

    def release_worker_reservation(self, alias: str, job_id: str) -> None:
        self.db.execute(
            "DELETE FROM worker_reservations WHERE alias=? AND job_id=?", (alias, job_id)
        )
        self.db.commit()

    def mark_origin_turn_complete(self, origin_session_id: str) -> list[str]:
        rows = self.db.execute(
            "SELECT id FROM jobs WHERE origin_session_id=? AND state='accepted'",
            (origin_session_id,),
        ).fetchall()
        released: list[str] = []
        for row in rows:
            self.db.execute(
                "INSERT OR IGNORE INTO turn_releases(job_id,origin_session_id,released_at) VALUES(?,?,?)",
                (row["id"], origin_session_id, now_iso()),
            )
            released.append(row["id"])
        self.db.commit()
        return released

    def origin_turn_released(self, job_id: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM turn_releases WHERE job_id=?", (job_id,)
        ).fetchone() is not None

    def wait_origin_turn_release(self, job_id: str, timeout_seconds: float,
                                 poll_seconds: float = 0.2) -> None:
        if timeout_seconds <= 0:
            return
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            if self.origin_turn_released(job_id):
                self.event(job_id, "origin_turn_released", {})
                return
            time.sleep(poll_seconds)
        raise TimeoutError(
            f"origin turn did not emit post_llm_call release for job {job_id}"
        )

    def job(self, job_id: str) -> Optional[sqlite3.Row]:
        return self.db.execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()

    def jobs(self) -> list[sqlite3.Row]:
        return list(self.db.execute("SELECT * FROM jobs ORDER BY created_at DESC, rowid DESC"))

    def event(self, job_id: Optional[str], event: str, detail: dict[str, Any]) -> None:
        self.db.execute(
            "INSERT INTO events(job_id,event,detail_json,created_at) VALUES(?,?,?,?)",
            (job_id, event, json.dumps(detail), now_iso()),
        )
        self.db.commit()

    def acquire_resource(self, resource: str, owner: str, wait_timeout: float, lease_seconds: float,
                         poll_seconds: float = 0.25) -> None:
        deadline = time.monotonic() + wait_timeout
        while True:
            now = time.time()
            acquired = False
            try:
                self.db.execute("BEGIN IMMEDIATE")
                row = self.db.execute(
                    "SELECT owner, lease_until FROM resource_locks WHERE resource=?", (resource,)
                ).fetchone()
                if row is None or row["owner"] == owner or float(row["lease_until"]) <= now:
                    self.db.execute(
                        """
                        INSERT INTO resource_locks(resource,owner,acquired_at,lease_until) VALUES(?,?,?,?)
                        ON CONFLICT(resource) DO UPDATE SET
                          owner=excluded.owner, acquired_at=excluded.acquired_at, lease_until=excluded.lease_until
                        """,
                        (resource, owner, now, now + lease_seconds),
                    )
                    acquired = True
                self.db.commit()
            except Exception:
                if self.db.in_transaction:
                    self.db.rollback()
                raise
            if acquired:
                self.event(owner, "resource_acquired", {"resource": resource, "lease_seconds": lease_seconds})
                return
            if time.monotonic() >= deadline:
                raise TimeoutError(f"timed out waiting for exclusive resource {resource!r}")
            time.sleep(poll_seconds)

    def release_resource(self, resource: str, owner: str) -> None:
        cur = self.db.execute(
            "DELETE FROM resource_locks WHERE resource=? AND owner=?", (resource, owner)
        )
        self.db.commit()
        if cur.rowcount:
            self.event(owner, "resource_released", {"resource": resource})

    def resource_locks(self) -> list[sqlite3.Row]:
        return list(self.db.execute("SELECT * FROM resource_locks ORDER BY resource"))


class HermesAPI:
    def __init__(self, cfg: Config):
        self.base = cfg.api_url
        self.key = cfg.api_key

    def _request(self, method: str, path: str, body: Optional[dict[str, Any]] = None,
                 timeout: int = 3600) -> Any:
        data = None if body is None else json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            self.base + path,
            data=data,
            method=method,
            headers={
                "Authorization": f"Bearer {self.key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                raw = resp.read()
                if not raw:
                    return None
                return json.loads(raw.decode("utf-8"))
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Hermes API HTTP {e.code}: {detail}") from e
        except urllib.error.URLError as e:
            raise RuntimeError(f"Hermes API unreachable at {self.base}: {e}") from e

    def capabilities(self) -> Any:
        return self._request("GET", "/v1/capabilities")

    def create_session(self, title: Optional[str] = None, role: str = "worker") -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if title:
            payload["title"] = title
        out = self._request("POST", "/api/sessions", payload)
        if not isinstance(out, dict):
            raise RuntimeError(f"Unexpected create-session response: {out!r}")
        return out

    def chat(self, sid: str, prompt: str) -> dict[str, Any]:
        out = self._request("POST", f"/api/sessions/{sid}/chat", {"input": prompt})
        if not isinstance(out, dict):
            return {"raw": out}
        return out

    def chat_with_timeout(self, sid: str, prompt: str, timeout_seconds: int) -> dict[str, Any]:
        out = self._request(
            "POST", f"/api/sessions/{sid}/chat", {"input": prompt}, timeout=timeout_seconds
        )
        return out if isinstance(out, dict) else {"raw": out}

    def fork(self, sid: str, title: Optional[str] = None) -> dict[str, Any]:
        payload: dict[str, Any] = {}
        if title:
            payload["title"] = title
        out = self._request("POST", f"/api/sessions/{sid}/fork", payload)
        if not isinstance(out, dict):
            raise RuntimeError(f"Unexpected fork response: {out!r}")
        return out

    def delete_session(self, sid: str) -> Any:
        return self._request("DELETE", f"/api/sessions/{sid}")


def lm_studio_loaded_llms(cli: list[str]) -> list[dict[str, Any]]:
    cp = subprocess.run(
        cli + ["ps", "--json"], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, encoding="utf-8", errors="replace", timeout=30,
        creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
    )
    if cp.returncode != 0:
        raise RuntimeError(f"LM Studio model discovery failed: {cp.stderr[-2000:]}")
    try:
        return [m for m in json.loads(cp.stdout) if m.get("type") == "llm"]
    except (json.JSONDecodeError, TypeError) as e:
        raise RuntimeError(f"Invalid `lms ps --json` response: {cp.stdout[-2000:]}") from e


class HermesCLI:
    """Adapter for Hermes 0.19.x, whose durable resume path is `hermes chat`."""

    def __init__(self, cfg: Config):
        self.argv = cfg.hermes_cli_argv
        hermes = cfg.raw.get("hermes", {})
        self.timeout = int(hermes.get("cli_timeout_seconds", 3600))
        self.source = str(hermes.get("default_source", "tool"))
        self.no_restore_cwd = bool(hermes.get("no_restore_cwd", False))
        self.model = hermes.get("model")
        self.visible = os.environ.get("HANDOFF_VISIBLE_CONSOLE") == "1"
        self.visible_exit_drain_seconds = float(hermes.get("visible_exit_drain_seconds", 2))
        self.max_turns = int(hermes.get("worker_max_turns", 80))
        self.chat_extra_argv = hermes.get("cli_chat_extra_argv", [])
        if not isinstance(self.chat_extra_argv, list) or not all(
            isinstance(item, str) and item for item in self.chat_extra_argv
        ):
            raise RuntimeError("hermes.cli_chat_extra_argv must be an argv array of non-empty strings")
        self.lm_studio_cli = hermes.get("lm_studio_cli_argv", ["lms"])
        if not isinstance(self.lm_studio_cli, list) or not self.lm_studio_cli:
            raise RuntimeError("hermes.lm_studio_cli_argv must be a non-empty argv array")

    def _model_args(self) -> list[str]:
        model = self.model
        if model == "auto":
            loaded = lm_studio_loaded_llms(self.lm_studio_cli)
            if len(loaded) != 1 or not loaded[0].get("modelKey"):
                names = [m.get("modelKey") for m in loaded]
                raise RuntimeError(f"Hermes model auto selection requires exactly one loaded LM Studio LLM; found {names}")
            model = loaded[0]["modelKey"]
        return ["--model", str(model)] if model else []

    def _run(self, argv: list[str], operation: str,
             timeout_seconds: Optional[int] = None) -> subprocess.CompletedProcess[str]:
        if self.visible and operation in {"create-session", "resume"}:
            return self._run_visible(argv, operation, timeout_seconds)
        cp = subprocess.run(
            argv,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=self.timeout if timeout_seconds is None else timeout_seconds,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        if cp.returncode != 0:
            raise RuntimeError(
                f"Hermes CLI {operation} failed ({cp.returncode}): {shlex.join(argv[:4])}\n"
                f"stdout: {cp.stdout[-2000:]}\nstderr: {cp.stderr[-2000:]}"
            )
        return cp

    def _run_visible(self, argv: list[str], operation: str,
                     timeout_seconds: Optional[int]) -> subprocess.CompletedProcess[str]:
        timeout = self.timeout if timeout_seconds is None else timeout_seconds
        proc = subprocess.Popen(
            argv, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True, encoding="utf-8", errors="replace", bufsize=1,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0,
        )
        lines: list[str] = []
        pending: queue.Queue[Optional[str]] = queue.Queue()

        def read_output() -> None:
            try:
                assert proc.stdout is not None
                for line in proc.stdout:
                    pending.put(line)
            except (OSError, ValueError):
                # The process return code remains authoritative; Windows may
                # tear down the redirected pipe before the reader sees EOF.
                pass
            finally:
                # A broken/never-closed Windows pipe must not strand the
                # supervisor after the Hermes process has already exited.
                pending.put(None)

        threading.Thread(target=read_output, daemon=True).start()
        deadline = time.monotonic() + timeout
        stream_done = False
        exit_drain_deadline: Optional[float] = None
        try:
            while not stream_done:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise subprocess.TimeoutExpired(argv, timeout)
                if proc.poll() is not None and exit_drain_deadline is None:
                    exit_drain_deadline = time.monotonic() + self.visible_exit_drain_seconds
                if exit_drain_deadline is not None and time.monotonic() >= exit_drain_deadline:
                    break
                queue_wait = min(0.2, remaining)
                if exit_drain_deadline is not None:
                    queue_wait = min(queue_wait, max(0.001, exit_drain_deadline - time.monotonic()))
                try:
                    line = pending.get(timeout=queue_wait)
                except queue.Empty:
                    continue
                if line is None:
                    stream_done = True
                else:
                    lines.append(line)
                    print(line, end="", flush=True)
            rc = proc.wait(timeout=max(0.1, deadline - time.monotonic()))
        except subprocess.TimeoutExpired:
            print(f"\n[SUPERVISOR] {operation} timed out after {timeout}s; stopping Hermes.", flush=True)
            if os.name == "nt":
                try:
                    proc.send_signal(signal.CTRL_BREAK_EVENT)
                    proc.wait(timeout=5)
                except Exception:
                    proc.kill()
            else:
                proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()
            raise
        cp = subprocess.CompletedProcess(argv, rc, "".join(lines), "")
        if cp.returncode != 0:
            raise RuntimeError(
                f"Hermes CLI {operation} failed ({cp.returncode}): {shlex.join(argv[:4])}\n"
                f"output: {cp.stdout[-4000:]}"
            )
        return cp

    def _last_assistant_response(self, sid: str, fallback: str) -> str:
        if not self.visible:
            return fallback.strip()
        cp = subprocess.run(
            self.argv + ["sessions", "export", "-", "--format", "jsonl", "--session-id", sid, "--yes"],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", errors="replace", timeout=30,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        if cp.returncode == 0:
            for line in reversed(cp.stdout.splitlines()):
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if str(obj.get("id")) != sid:
                    continue
                for message in reversed(obj.get("messages") or []):
                    if message.get("role") == "assistant" and not message.get("tool_calls"):
                        return str(message.get("content") or "").strip()
        return fallback.strip()

    def export_session(self, sid: str, output_path: str) -> str:
        """Export the complete durable Hermes session to one JSONL artifact."""
        absolute_path = os.path.abspath(output_path)
        pathlib.Path(absolute_path).parent.mkdir(parents=True, exist_ok=True)
        cp = subprocess.run(
            self.argv + [
                "sessions", "export", absolute_path, "--format", "jsonl",
                "--session-id", sid, "--yes",
            ],
            stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", errors="replace", timeout=60,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        if cp.returncode != 0 or not os.path.isfile(absolute_path):
            raise RuntimeError(
                f"Hermes session export failed ({cp.returncode}) for {sid}: "
                f"{(cp.stderr or cp.stdout)[-2000:]}"
            )
        return absolute_path

    def create_session(self, title: Optional[str] = None, role: str = "worker") -> dict[str, Any]:
        session_name = title or f"persistent-{role}"
        if role == "orchestrator":
            prompt = (
                "[ORCHESTRATED_ORCHESTRATOR_INITIALIZE]\n"
                f"orchestrator_alias: {session_name}\n\n"
                "This is a persistent orchestrator session managed by an external supervisor. "
                "Review future WORKER_RESULT envelopes and decide whether to resume a worker, delegate elsewhere, or finish. "
                "Reply exactly ORCHESTRATOR_READY now. Do not use tools."
            )
        else:
            prompt = (
                "[ORCHESTRATED_WORKER_INITIALIZE]\n"
                f"worker_alias: {session_name}\n\n"
                "This is a persistent worker session managed by an external supervisor. "
                "Reply exactly WORKER_READY. Do not use tools and do not start other agents."
            )
        argv = self.argv + ["chat", "--query", prompt]
        if not self.visible:
            argv.append("--quiet")
        argv += ["--source", self.source, "--pass-session-id"]
        argv += self.chat_extra_argv + self._model_args()
        cp = self._run(argv, "create-session")
        session_output = cp.stdout + "\n" + cp.stderr
        match = re.search(r"(?mi)^\s*(?:session_id|Session):\s*(\S+)\s*$", session_output)
        if not match:
            raise RuntimeError(f"Hermes CLI did not report a session_id: {session_output[-2000:]}")
        sid = match.group(1)
        response = self._last_assistant_response(sid, cp.stdout)
        if title:
            self._run(self.argv + ["sessions", "rename", sid, title], "rename-session")
        return {"session_id": sid, "response": response}

    def chat(self, sid: str, prompt: str) -> dict[str, Any]:
        return self.chat_with_timeout(sid, prompt, self.timeout)

    def chat_with_timeout(self, sid: str, prompt: str, timeout_seconds: int) -> dict[str, Any]:
        argv = self.argv + ["chat", "--resume", sid, "--query", prompt]
        if not self.visible:
            argv.append("--quiet")
        else:
            argv += ["--max-turns", str(self.max_turns)]
        argv += ["--source", self.source]
        if self.no_restore_cwd:
            argv.append("--no-restore-cwd")
        argv += self.chat_extra_argv + self._model_args()
        cp = self._run(argv, "resume", timeout_seconds=timeout_seconds)
        return {"response": self._last_assistant_response(sid, cp.stdout)}


def hermes_client(cfg: Config) -> Any:
    if cfg.hermes_adapter == "cli":
        return HermesCLI(cfg)
    if cfg.hermes_adapter == "rest":
        return HermesAPI(cfg)
    raise RuntimeError("hermes.adapter must be either 'cli' or 'rest'")


def export_worker_session_checkpoint(
    cfg: Config, api: Any, worker_sid: str, job_id: str,
) -> str:
    exporter = getattr(api, "export_session", None)
    if not callable(exporter):
        raise RuntimeError("configured Hermes adapter does not support full session export")
    output_path = os.path.join(
        cfg.artifact_dir, job_id, f"worker-session-{worker_sid}.jsonl",
    )
    return str(exporter(worker_sid, output_path))


@contextmanager
def llm_slot(cfg: Config, store: StateStore, owner: str) -> Iterable[None]:
    scheduler = cfg.raw.get("scheduler", {})
    wait_timeout = float(scheduler.get("lock_wait_timeout_seconds", 3600))
    lease_seconds = float(scheduler.get("lock_lease_seconds", 7200))
    poll_seconds = float(scheduler.get("lock_poll_seconds", 0.25))
    store.acquire_resource("LLM_SLOT", owner, wait_timeout, lease_seconds, poll_seconds)
    try:
        yield
    finally:
        store.release_resource("LLM_SLOT", owner)


def scheduled_chat(cfg: Config, store: StateStore, owner: str, client: Any,
                   sid: str, prompt: str, timeout_seconds: Optional[int] = None) -> dict[str, Any]:
    with llm_slot(cfg, store, owner):
        if timeout_seconds is not None and hasattr(client, "chat_with_timeout"):
            return client.chat_with_timeout(sid, prompt, timeout_seconds)
        return client.chat(sid, prompt)


ORCHESTRATOR_CONTROL_RULE = """

[ORCHESTRATOR_CONTROL_RULE]
You are the orchestrator, not the worker. Keep this control turn short.
Do not continue the worker's investigation or repeat a full audit. Perform at most
one bounded check of status/schema/primary outputs, then accept or reject the
result, update task state, and either create the next handoff, return a correction
to a worker, or finish. After HANDOFF_ACCEPTED, end the turn immediately.
"""

ORIGIN_DELIVERY_RECOVERY = """[ORIGIN_DELIVERY_RECOVERY]
Your previous control turn exceeded its timeout. The handoff event, worker result,
and checks you already performed are preserved in this session. Do not repeat them
and do not re-read the full worker payload. Perform only the next control-plane
action: accept/reject and update state, create the next handoff or correction, or
finish. After HANDOFF_ACCEPTED, end the turn immediately.
"""

ORCHESTRATOR_WATCHDOG_RECOVERY = """[ORCHESTRATOR_WATCHDOG_RECOVERY]
The control plane has been idle with no LLM slot owner and no accepted/running
job. Resume orchestration from durable files and supervisor state. You are the
orchestrator, not a worker: perform at most one bounded status check, then create
exactly the next required handoff or finish the phase. Do not redo completed work.
After HANDOFF_ACCEPTED, end the turn immediately.
"""


def scheduled_origin_chat(
    cfg: Config, store: StateStore, owner: str, client: Any,
    origin_sid: str, prompt: str, timeout_seconds: int,
    retry_once_override: Optional[bool] = None,
) -> dict[str, Any]:
    """Run an origin turn and release its handoffs if Hermes is forcibly stopped.

    A detached handoff created during the origin turn normally receives its release
    from the post_llm_call hook. If the CLI exceeds its hard timeout, subprocess
    termination makes that turn definitively over but the hook cannot run. Emit
    the equivalent release here so accepted child work is not stranded. If no
    child was created, optionally run one bounded control-only recovery turn.
    """
    current_prompt = prompt.rstrip() + ORCHESTRATOR_CONTROL_RULE
    retry_once = (
        bool(cfg.raw.get("supervisor", {}).get("origin_delivery_retry_once", True))
        if retry_once_override is None else retry_once_override
    )
    attempt = 0
    while True:
        try:
            return scheduled_chat(
                cfg, store, owner, client, origin_sid, current_prompt,
                timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            released = store.mark_origin_turn_complete(origin_sid)
            store.event(owner, "origin_forced_stop_released", {
                "origin_session_id": origin_sid,
                "released_job_ids": released,
                "attempt": attempt + 1,
            })
            if released or attempt >= 1 or not retry_once:
                raise
            attempt += 1
            current_prompt = ORIGIN_DELIVERY_RECOVERY + ORCHESTRATOR_CONTROL_RULE
            store.event(owner, "origin_delivery_retrying", {
                "origin_session_id": origin_sid,
                "attempt": attempt + 1,
            })


def delivery_timeout(cfg: Config) -> int:
    return int(cfg.raw.get("hermes", {}).get("delivery_timeout_seconds", 180))


def worker_turn_timeout(cfg: Config) -> int:
    return int(cfg.raw.get("hermes", {}).get("worker_turn_timeout_seconds", 900))


def origin_release_timeout(cfg: Config) -> float:
    return float(cfg.raw.get("supervisor", {}).get("origin_release_timeout_seconds", 180))


def orchestration_busy(store: StateStore, origin_session_id: Optional[str] = None) -> bool:
    """True only while supervisor-owned work can legitimately make progress."""
    if any(row["resource"] == "LLM_SLOT" for row in store.resource_locks()):
        return True
    if origin_session_id:
        row = store.db.execute(
            "SELECT 1 FROM jobs WHERE origin_session_id=? "
            "AND state IN ('accepted','running') LIMIT 1",
            (origin_session_id,),
        ).fetchone()
    else:
        row = store.db.execute(
            "SELECT 1 FROM jobs WHERE state IN ('accepted','running') LIMIT 1"
        ).fetchone()
    return row is not None


def durable_idle_age_seconds(store: StateStore) -> float:
    row = store.db.execute("SELECT MAX(updated_at) AS updated_at FROM jobs").fetchone()
    if not row or not row["updated_at"]:
        return 0.0
    updated = datetime.fromisoformat(str(row["updated_at"]))
    return max(0.0, (datetime.now(timezone.utc) - updated).total_seconds())


def run_orchestrator_watchdog(
    cfg: Config, store: StateStore, orchestrator: str, idle_seconds: float,
    poll_seconds: float, until_file: Optional[str], recovery_prompt: Optional[str],
    rotate_after: int = 2, max_cycles: Optional[int] = None,
) -> None:
    sid = resolve_sid(store, orchestrator)
    owner = f"WATCHDOG:{orchestrator}"
    resource = f"ORCHESTRATOR_WATCHDOG:{orchestrator}"
    store.acquire_resource(resource, owner, 0.01, 86400, 0.01)
    idle_since: Optional[float] = None
    idle_resumes = 0
    cycles = 0
    try:
        print(
            f"[WATCHDOG] orchestrator={orchestrator} session={sid} "
            f"idle_seconds={idle_seconds:g}", flush=True,
        )
        while True:
            if until_file and os.path.isfile(until_file):
                print(f"[WATCHDOG] completion file exists: {until_file}", flush=True)
                return
            if orchestration_busy(store, sid):
                idle_since = None
            elif idle_since is None:
                prior_idle = min(idle_seconds, durable_idle_age_seconds(store))
                idle_since = time.monotonic() - prior_idle
                print("[WATCHDOG] control plane idle; grace timer started.", flush=True)
            elif time.monotonic() - idle_since >= idle_seconds:
                prompt = recovery_prompt or ORCHESTRATOR_WATCHDOG_RECOVERY
                attempt_owner = f"watchdog:{orchestrator}:{uuid.uuid4()}"
                before_jobs = {
                    row["id"] for row in store.db.execute(
                        "SELECT id FROM jobs WHERE origin_session_id=?", (sid,)
                    )
                }
                print("[WATCHDOG] idle threshold reached; resuming orchestrator.", flush=True)
                store.event(owner, "orchestrator_watchdog_resume", {
                    "orchestrator": orchestrator, "session_id": sid,
                    "idle_seconds": idle_seconds,
                })
                try:
                    scheduled_origin_chat(
                        cfg, store, attempt_owner, hermes_client(cfg), sid,
                        prompt, delivery_timeout(cfg), retry_once_override=False,
                    )
                except Exception as e:
                    # A timeout after creating a child is expected: scheduled_origin_chat
                    # releases that child before raising. Keep watching durable state.
                    print(f"[WATCHDOG] recovery turn ended: {type(e).__name__}: {e}", flush=True)
                    store.event(owner, "orchestrator_watchdog_turn_ended", {"error": str(e)})
                new_jobs = {
                    row["id"] for row in store.db.execute(
                        "SELECT id FROM jobs WHERE origin_session_id=?", (sid,)
                    )
                } - before_jobs
                idle_resumes = 0 if new_jobs else idle_resumes + 1
                if until_file and os.path.isfile(until_file):
                    print(f"[WATCHDOG] completion file exists: {until_file}", flush=True)
                    return
                if rotate_after > 0 and idle_resumes >= rotate_after:
                    old_sid = sid
                    title = f"{orchestrator}-recovered-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
                    print(
                        f"[WATCHDOG] {idle_resumes} idle resumes made no handoff; "
                        "rotating orchestrator session.", flush=True,
                    )
                    created = scheduled_create_session(
                        cfg, store, f"watchdog-rotate:{uuid.uuid4()}",
                        hermes_client(cfg), title, "orchestrator",
                    )
                    sid = extract_session_id(created)
                    store.upsert_session(
                        orchestrator, sid, "orchestrator",
                        metadata={"watchdog_replaced_session_id": old_sid},
                    )
                    store.event(owner, "orchestrator_watchdog_rotated", {
                        "orchestrator": orchestrator,
                        "old_session_id": old_sid, "new_session_id": sid,
                    })
                    print(f"[WATCHDOG] new orchestrator session={sid}", flush=True)
                    idle_resumes = 0
                    idle_since = time.monotonic() - idle_seconds
                else:
                    idle_since = time.monotonic()
            cycles += 1
            if max_cycles is not None and cycles >= max_cycles:
                return
            time.sleep(poll_seconds)
    finally:
        store.release_resource(resource, owner)


def launch_detached(cfg: Config, argv: list[str], job_id: str, role: str) -> subprocess.Popen[Any]:
    env = os.environ.copy()
    visible = bool(cfg.raw.get("supervisor", {}).get("show_console", True)) and os.name == "nt"
    if visible:
        env["HANDOFF_VISIBLE_CONSOLE"] = "1"
        env["HANDOFF_JOB_ID"] = job_id
        env["HANDOFF_ROLE"] = role
        return subprocess.Popen(
            argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            env=env, creationflags=subprocess.CREATE_NEW_CONSOLE | subprocess.CREATE_NEW_PROCESS_GROUP,
            close_fds=True,
        )
    return subprocess.Popen(
        argv, stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        env=env, start_new_session=True, close_fds=True,
    )


def attach_visible_console() -> None:
    if os.name != "nt" or os.environ.get("HANDOFF_VISIBLE_CONSOLE") != "1":
        return
    try:
        sys.stdout = open("CONOUT$", "w", encoding="utf-8", errors="replace", buffering=1)
        sys.stderr = open("CONOUT$", "w", encoding="utf-8", errors="replace", buffering=1)
        import ctypes
        role = os.environ.get("HANDOFF_ROLE", "handoff")
        job_id = os.environ.get("HANDOFF_JOB_ID", "")
        ctypes.windll.kernel32.SetConsoleTitleW(f"Hermes Supervisor - {role} - {job_id[:8]}")
        print("=" * 78)
        print(f"HERMES SUPERVISOR  role={role}  job={job_id}")
        print("Do not close this window while the job is active.")
        print("=" * 78, flush=True)
    except Exception:
        pass


def scheduled_create_session(cfg: Config, store: StateStore, owner: str, client: Any,
                             title: Optional[str], role: str) -> dict[str, Any]:
    with llm_slot(cfg, store, owner):
        return client.create_session(title=title, role=role)


def extract_session_id(obj: dict[str, Any]) -> str:
    for key in ("id", "session_id", "sessionId"):
        if obj.get(key):
            return str(obj[key])
    if isinstance(obj.get("session"), dict):
        return extract_session_id(obj["session"])
    raise RuntimeError(f"Cannot find session id in response: {obj!r}")


def extract_text(obj: Any) -> str:
    if isinstance(obj, str):
        return obj
    if isinstance(obj, dict):
        for key in ("response", "output", "text", "content", "message"):
            val = obj.get(key)
            if isinstance(val, str):
                return val
            if isinstance(val, dict):
                nested = extract_text(val)
                if nested:
                    return nested
        # Keep full response if schema differs across versions.
        return json.dumps(obj, ensure_ascii=False, indent=2)
    return json.dumps(obj, ensure_ascii=False)


def parse_worker_envelope(text: str) -> Optional[dict[str, Any]]:
    candidate = text.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", candidate, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        candidate = fenced.group(1)
    try:
        value = json.loads(candidate)
    except json.JSONDecodeError:
        return None
    if not isinstance(value, dict):
        return None
    kind = value.get("kind") or value.get("envelope")
    if kind not in {"worker_yield", "worker_final"}:
        return None
    normalized = dict(value)
    normalized["kind"] = kind
    return normalized


def classify_worker_response(text: str) -> tuple[str, Optional[dict[str, Any]]]:
    envelope = parse_worker_envelope(text)
    if envelope is None:
        return "completed", None
    if envelope["kind"] == "worker_yield":
        return "yielded", envelope
    if str(envelope.get("status", "completed")).lower() in {"failed", "error"}:
        return "failed", envelope
    return "completed", envelope


def resolve_sid(store: StateStore, value: str) -> str:
    row = store.session(value)
    return row["hermes_session_id"] if row else value


def run_argv(argv: list[str], cwd: Optional[str], timeout: Optional[int], stdout_path: str,
             stderr_path: str) -> dict[str, Any]:
    if not argv:
        raise ValueError("argv must not be empty")
    if cwd is not None and not pathlib.Path(cwd).is_dir():
        raise ValueError(f"cwd is not an existing directory: {cwd}")
    pathlib.Path(stdout_path).parent.mkdir(parents=True, exist_ok=True)
    start = time.monotonic()
    with open(stdout_path, "wb") as out, open(stderr_path, "wb") as err:
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0
        proc = subprocess.Popen(
            argv,
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            stdout=out,
            stderr=err,
            start_new_session=os.name != "nt",
            creationflags=creationflags,
        )
        timed_out = False
        try:
            rc = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            try:
                if os.name == "nt":
                    proc.terminate()
                else:
                    os.killpg(proc.pid, signal.SIGTERM)
            except (ProcessLookupError, OSError):
                pass
            try:
                rc = proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                try:
                    if os.name == "nt":
                        proc.kill()
                    else:
                        os.killpg(proc.pid, signal.SIGKILL)
                except (ProcessLookupError, OSError):
                    pass
                rc = proc.wait()
    return {
        "exit_code": rc,
        "timed_out": timed_out,
        "duration_seconds": round(time.monotonic() - start, 3),
        "stdout_path": stdout_path,
        "stderr_path": stderr_path,
    }


def cmd_init(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    pathlib.Path(cfg.artifact_dir).mkdir(parents=True, exist_ok=True)
    print(json.dumps({"state_db": store.path, "artifact_dir": cfg.artifact_dir}, indent=2))


def cmd_register_session(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    store.upsert_session(args.alias, args.session_id, args.role, args.parent_alias)
    print(f"registered {args.alias} -> {args.session_id}")


def cmd_create_worker(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    if store.session(args.alias):
        raise RuntimeError(f"worker alias already exists: {args.alias}")
    api = hermes_client(cfg)
    obj = scheduled_create_session(
        cfg, store, f"create-worker:{uuid.uuid4()}", api, args.title or args.alias, "worker"
    )
    sid = extract_session_id(obj)
    store.upsert_session(args.alias, sid, "worker", args.parent_alias)
    print(json.dumps({"alias": args.alias, "session_id": sid, "api": obj}, ensure_ascii=False, indent=2))


def cmd_create_orchestrator(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    if store.session(args.alias):
        raise RuntimeError(f"session alias already exists: {args.alias}")
    api = hermes_client(cfg)
    obj = scheduled_create_session(
        cfg, store, f"create-orchestrator:{uuid.uuid4()}", api,
        args.title or args.alias, "orchestrator",
    )
    sid = extract_session_id(obj)
    store.upsert_session(args.alias, sid, "orchestrator")
    print(json.dumps({"alias": args.alias, "session_id": sid, "adapter": obj}, ensure_ascii=False, indent=2))


def cmd_list_sessions(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    for r in store.sessions():
        print(f"{r['alias']}\t{r['role']}\t{r['status']}\t{r['hermes_session_id']}")


def cmd_chat(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    sid = resolve_sid(store, args.session)
    client = hermes_client(cfg)
    out = scheduled_chat(cfg, store, f"chat:{uuid.uuid4()}", client, sid, args.prompt)
    print(extract_text(out))


def cmd_delegate(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    if args.task is None:
        args.task = args.worker
        args.worker = f"worker-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"

    origin = args.origin or os.environ.get("HERMES_SESSION_ID")
    if not origin:
        raise RuntimeError("origin session missing; pass --origin or run from Hermes with HERMES_SESSION_ID")

    worker_row = store.session(args.worker)
    if worker_row and worker_row["status"] in {"queued", "running"} and not args._worker_job:
        raise RuntimeError(f"worker is already running: {args.worker}")
    worker_sid = worker_row["hermes_session_id"] if worker_row else None

    job_id = args._worker_job or store.create_job("delegate", origin, worker_sid, {"worker": args.worker, "task": args.task})
    if args.detached and not args._worker_job:
        previous_status = worker_row["status"] if worker_row else None
        if worker_row:
            store.set_session_status(args.worker, "queued")
        else:
            try:
                store.reserve_worker(args.worker, job_id)
            except Exception as e:
                store.set_job(job_id, "failed", error=str(e))
                raise
        child = [sys.executable, os.path.abspath(__file__), "--config", args.config, "delegate",
                 args.worker, args.task, "--origin", origin, "--resume-origin", "--_worker-job", job_id]
        try:
            launch_detached(cfg, child, job_id, "delegate")
        except Exception as e:
            if worker_row:
                store.set_session_status(args.worker, previous_status)
            else:
                store.release_worker_reservation(args.worker, job_id)
            store.set_job(job_id, "failed", error=f"failed to launch detached worker: {e}")
            raise
        print(json.dumps({"status": "HANDOFF_ACCEPTED", "job_id": job_id, "worker": args.worker, "worker_session_id": worker_sid}))
        return
    if args._worker_job:
        print(f"[SUPERVISOR] WAITING_FOR_ORIGIN_RELEASE origin={origin}", flush=True)
        store.wait_origin_turn_release(job_id, origin_release_timeout(cfg))
        print("[SUPERVISOR] ORIGIN_RELEASED; starting worker process.", flush=True)

    worker_completed = False
    result: dict[str, Any] = {}
    api: Any = None
    try:
        api = hermes_client(cfg)
        store.set_job(job_id, "running")
        if worker_row:
            store.set_session_status(args.worker, "running")
        else:
            if not args._worker_job:
                store.reserve_worker(args.worker, job_id)
            reservation = store.worker_reservation(args.worker)
            if not reservation or reservation["job_id"] != job_id:
                raise RuntimeError(f"worker alias reservation is missing or owned by another job: {args.worker}")
            created = scheduled_create_session(
                cfg, store, job_id, api, args.worker, "worker"
            )
            worker_sid = extract_session_id(created)
            store.upsert_session(args.worker, worker_sid, "worker")
            store.set_session_status(args.worker, "running")
            store.set_job_target(job_id, worker_sid)
            store.release_worker_reservation(args.worker, job_id)

        worker_prompt = (
            "[ORCHESTRATED_WORKER_TASK]\n"
            f"job_id: {job_id}\n"
            f"task: {args.task}\n\n"
            "Work on this task using your existing session context. Prefer a JSON worker_yield or worker_final envelope "
            "matching the session-handoff protocol; plain text remains supported. "
            "Do not wait for the orchestrator and do not launch another Hermes worker yourself."
        )
        print(f"[SUPERVISOR] WORKER_RUNNING alias={args.worker} session={worker_sid}", flush=True)
        worker_out = scheduled_chat(
            cfg, store, job_id, api, worker_sid, worker_prompt, worker_turn_timeout(cfg)
        )
        worker_text = extract_text(worker_out)
        worker_state, structured = classify_worker_response(worker_text)
        result = {
            "worker_alias": args.worker, "worker_session_id": worker_sid,
            "worker_state": worker_state, "worker_response": worker_text,
        }
        if structured is not None:
            result["structured_response"] = structured
        store.set_session_status(args.worker, worker_state)
        worker_completed = True
        store.set_job(job_id, "completed", result)

        if args.resume_origin:
            print(f"[SUPERVISOR] ORIGIN_RESTARTING session={origin}", flush=True)
            origin_prompt = (
                "[WORKER_RESULT]\n"
                f"job_id: {job_id}\nworker_alias: {args.worker}\nworker_session_id: {worker_sid}\n"
                f"status: {worker_state}\n\n"
                f"{worker_text}\n\n"
                "Review this worker output. You may resume the same worker with corrections, create another worker, ask an ephemeral inquiry, or continue the main task."
            )
            origin_out = scheduled_origin_chat(
                cfg, store, job_id, api, origin, origin_prompt, delivery_timeout(cfg)
            )
            result["origin_response"] = extract_text(origin_out)
            store.set_job(job_id, "delivered", result)
        print(json.dumps({"status": "HANDOFF_COMPLETE", "job_id": job_id, **result}, ensure_ascii=False, indent=2))
    except Exception as e:
        store.release_worker_reservation(args.worker, job_id)
        if worker_completed:
            store.set_job(job_id, "delivery_failed", result=result, error=str(e))
        else:
            if args.resume_origin:
                failure_result = {
                    "worker_alias": args.worker,
                    "worker_session_id": worker_sid,
                    "worker_state": "failed",
                    "error": str(e),
                }
                if store.session(args.worker):
                    store.set_session_status(args.worker, "stopped")
                store.set_job(job_id, "stopped", failure_result, error=str(e))
                timed_out = isinstance(e, subprocess.TimeoutExpired)
                if timed_out and worker_sid and api is not None:
                    try:
                        failure_result["worker_session_export_path"] = export_worker_session_checkpoint(
                            cfg, api, worker_sid, job_id,
                        )
                    except Exception as export_error:
                        failure_result["worker_session_export_error"] = str(export_error)
                try:
                    print(f"[SUPERVISOR] WORKER_FAILED; ORIGIN_RESTARTING session={origin}", flush=True)
                    event_name = "WORKER_TIMED_OUT" if timed_out else "WORKER_FAILED"
                    checkpoint_line = ""
                    if failure_result.get("worker_session_export_path"):
                        checkpoint_line = (
                            f"worker_session_export_path: {failure_result['worker_session_export_path']}\n"
                            "Read that complete session export before deciding what to do next.\n"
                        )
                    origin_out = scheduled_origin_chat(
                        cfg, store, job_id, hermes_client(cfg), origin,
                        f"[{event_name}]\n"
                        f"job_id: {job_id}\nworker_alias: {args.worker}\n"
                        f"worker_session_id: {worker_sid}\nerror: {e}\n"
                        f"{checkpoint_line}\n"
                        "Decide whether to resume this worker, correct its instructions, delegate to a new worker, or finish.",
                        delivery_timeout(cfg),
                    )
                    failure_result["origin_response"] = extract_text(origin_out)
                    store.set_job(job_id, "stopped_delivered", failure_result, error=str(e))
                    print(json.dumps({"status": "HANDOFF_STOPPED_DELIVERED", "job_id": job_id, **failure_result}, ensure_ascii=False, indent=2))
                    return
                except Exception as delivery_error:
                    if store.session(args.worker):
                        store.set_session_status(args.worker, "failed")
                    store.set_job(
                        job_id, "failed", result=failure_result,
                        error=f"worker error: {e}; origin restart error: {delivery_error}",
                    )
            else:
                if store.session(args.worker):
                    store.set_session_status(args.worker, "failed")
                store.set_job(job_id, "failed", error=str(e))
        raise


def cmd_resume_worker(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    row = store.session(args.worker)
    if not row:
        raise RuntimeError(f"unknown worker alias: {args.worker}")
    if row["status"] in {"queued", "running"} and not args._worker_job:
        raise RuntimeError(f"worker is already running: {args.worker}")
    sid = row["hermes_session_id"]
    origin = args.origin or os.environ.get("HERMES_SESSION_ID")
    job_id = args._worker_job or store.create_job("resume_worker", origin, sid, {"worker": args.worker, "instruction": args.instruction})
    if args.detached and not args._worker_job:
        previous_status = row["status"]
        store.set_session_status(args.worker, "queued")
        child = [sys.executable, os.path.abspath(__file__), "--config", args.config, "resume-worker",
                 args.worker, args.instruction, "--_worker-job", job_id]
        if origin:
            child += ["--origin", origin, "--resume-origin"]
        try:
            launch_detached(cfg, child, job_id, "resume-worker")
        except Exception as e:
            store.set_session_status(args.worker, previous_status)
            store.set_job(job_id, "failed", error=f"failed to launch detached worker: {e}")
            raise
        print(json.dumps({"status": "HANDOFF_ACCEPTED", "job_id": job_id, "worker": args.worker, "worker_session_id": sid}))
        return
    if args._worker_job:
        print(f"[SUPERVISOR] WAITING_FOR_ORIGIN_RELEASE origin={origin}", flush=True)
        store.wait_origin_turn_release(job_id, origin_release_timeout(cfg))
        print("[SUPERVISOR] ORIGIN_RELEASED; restarting worker process.", flush=True)
    else:
        store.set_session_status(args.worker, "running")
    if args._worker_job:
        store.set_session_status(args.worker, "running")
    store.set_job(job_id, "running")
    worker_completed = False
    result: dict[str, Any] = {}
    api: Any = None
    try:
        api = hermes_client(cfg)
        out = scheduled_chat(
            cfg, store, job_id, api, sid,
            "[ORCHESTRATOR_FEEDBACK]\n" + args.instruction
            + "\n\nPrefer a JSON worker_yield or worker_final envelope; plain text remains supported.",
            worker_turn_timeout(cfg),
        )
        text = extract_text(out)
        worker_state, structured = classify_worker_response(text)
        result = {
            "worker_alias": args.worker, "worker_session_id": sid,
            "worker_state": worker_state, "worker_response": text,
        }
        if structured is not None:
            result["structured_response"] = structured
        store.set_session_status(args.worker, worker_state)
        worker_completed = True
        store.set_job(job_id, "completed", result)
        if args.resume_origin and origin:
            origin_out = scheduled_origin_chat(
                cfg, store, job_id, api, origin,
                f"[WORKER_RESULT]\njob_id: {job_id}\nworker_alias: {args.worker}\nstatus: {worker_state}\n\n{text}",
                delivery_timeout(cfg),
            )
            result["origin_response"] = extract_text(origin_out)
            store.set_job(job_id, "delivered", result)
        print(json.dumps({"job_id": job_id, **result}, ensure_ascii=False, indent=2))
    except Exception as e:
        if worker_completed:
            store.set_job(job_id, "delivery_failed", result=result, error=str(e))
        else:
            if args.resume_origin and origin:
                failure_result = {
                    "worker_alias": args.worker,
                    "worker_session_id": sid,
                    "worker_state": "failed",
                    "error": str(e),
                }
                store.set_session_status(args.worker, "stopped")
                store.set_job(job_id, "stopped", failure_result, error=str(e))
                timed_out = isinstance(e, subprocess.TimeoutExpired)
                if timed_out and api is not None:
                    try:
                        failure_result["worker_session_export_path"] = export_worker_session_checkpoint(
                            cfg, api, sid, job_id,
                        )
                    except Exception as export_error:
                        failure_result["worker_session_export_error"] = str(export_error)
                try:
                    print(f"[SUPERVISOR] WORKER_FAILED; ORIGIN_RESTARTING session={origin}", flush=True)
                    event_name = "WORKER_TIMED_OUT" if timed_out else "WORKER_FAILED"
                    checkpoint_line = ""
                    if failure_result.get("worker_session_export_path"):
                        checkpoint_line = (
                            f"worker_session_export_path: {failure_result['worker_session_export_path']}\n"
                            "Read that complete session export before deciding what to do next.\n"
                        )
                    origin_out = scheduled_origin_chat(
                        cfg, store, job_id, hermes_client(cfg), origin,
                        f"[{event_name}]\n"
                        f"job_id: {job_id}\nworker_alias: {args.worker}\n"
                        f"worker_session_id: {sid}\nerror: {e}\n"
                        f"{checkpoint_line}\n"
                        "Decide whether to resume this worker, correct its instructions, delegate to a new worker, or finish.",
                        delivery_timeout(cfg),
                    )
                    failure_result["origin_response"] = extract_text(origin_out)
                    store.set_job(job_id, "stopped_delivered", failure_result, error=str(e))
                    print(json.dumps({"status": "HANDOFF_STOPPED_DELIVERED", "job_id": job_id, **failure_result}, ensure_ascii=False, indent=2))
                    return
                except Exception as delivery_error:
                    store.set_session_status(args.worker, "failed")
                    store.set_job(
                        job_id, "failed", result=failure_result,
                        error=f"worker error: {e}; origin restart error: {delivery_error}",
                    )
            else:
                store.set_session_status(args.worker, "failed")
                store.set_job(job_id, "failed", error=str(e))
        raise


def cmd_inquire(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    row = store.session(args.worker)
    if not row:
        raise RuntimeError(f"unknown worker alias: {args.worker}")
    canonical_sid = row["hermes_session_id"]
    api = hermes_client(cfg)
    origin = args.origin or os.environ.get("HERMES_SESSION_ID")
    job_id = args._worker_job or store.create_job("inquiry", origin, canonical_sid, {"worker": args.worker, "question": args.question})
    if args.detached and not args._worker_job:
        child = [sys.executable, os.path.abspath(__file__), "--config", args.config, "inquire",
                 args.worker, args.question, "--_worker-job", job_id]
        if origin:
            child += ["--origin", origin, "--resume-origin"]
        if args.keep_fork:
            child += ["--keep-fork"]
        launch_detached(cfg, child, job_id, "inquire")
        print(json.dumps({"status": "HANDOFF_ACCEPTED", "job_id": job_id, "worker": args.worker}))
        return
    if args._worker_job:
        time.sleep(float(cfg.raw.get("handoff_grace_seconds", 3)))
    store.set_job(job_id, "running")
    fork_sid: Optional[str] = None
    try:
        fork_obj = api.fork(canonical_sid, title=f"inquiry-{args.worker}-{job_id[:8]}")
        fork_sid = extract_session_id(fork_obj)
        answer_obj = scheduled_chat(
            cfg, store, job_id, api, fork_sid,
            "[EPHEMERAL_INQUIRY]\n"
            + args.question
            + "\nAnswer from the context you already have. This branch is temporary; do not alter the canonical task state.",
        )
        answer = extract_text(answer_obj)
        result = {
            "worker_alias": args.worker,
            "canonical_session_id": canonical_sid,
            "scratch_session_id": fork_sid,
            "answer": answer,
        }
        if not args.keep_fork:
            api.delete_session(fork_sid)
            result["scratch_deleted"] = True
        store.set_job(job_id, "completed", result)
        if args.resume_origin and origin:
            delivered = scheduled_origin_chat(
                cfg, store, job_id, api, origin,
                "[WORKER_INQUIRY_RESULT]\n"
                f"job_id: {job_id}\nworker_alias: {args.worker}\ncanonical_session_id: {canonical_sid}\n\n"
                f"{answer}\n\nThe answer came from a temporary fork. The canonical worker session was not changed.",
                delivery_timeout(cfg),
            )
            result["origin_response"] = extract_text(delivered)
            store.set_job(job_id, "delivered", result)
        if not args._worker_job:
            print(json.dumps({"job_id": job_id, **result}, ensure_ascii=False, indent=2))
    except Exception as e:
        store.set_job(job_id, "failed", error=str(e))
        raise


def cmd_run_command(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    origin = args.origin or os.environ.get("HERMES_SESSION_ID")
    argv = args.command
    if args.resume_origin and not origin:
        raise RuntimeError("origin session missing; pass --origin or run from Hermes with HERMES_SESSION_ID")
    payload = {"argv": argv, "cwd": args.cwd, "timeout": args.timeout}
    job_id = args._worker_job or store.create_job("external_command", origin, None, payload)

    if args.detached and not args._worker_job:
        child_argv = [sys.executable, os.path.abspath(__file__), "--config", args.config, "run-command",
                      "--origin", origin or "", "--timeout", str(args.timeout), "--_worker-job", job_id]
        if args.cwd:
            child_argv += ["--cwd", args.cwd]
        if args.resume_origin:
            child_argv += ["--resume-origin"]
        child_argv += ["--"] + argv
        launch_detached(cfg, child_argv, job_id, "external-command")
        print(json.dumps({"status": "HANDOFF_ACCEPTED", "job_id": job_id}))
        return

    job_dir = os.path.join(cfg.artifact_dir, job_id)
    pathlib.Path(job_dir).mkdir(parents=True, exist_ok=True)
    stdout_path = os.path.join(job_dir, "stdout.log")
    stderr_path = os.path.join(job_dir, "stderr.log")
    store.set_job(job_id, "running")
    try:
        result = run_argv(argv, args.cwd, args.timeout, stdout_path, stderr_path)
        state = "timed_out" if result["timed_out"] else "completed"
        store.set_job(job_id, state, result)
        if args.resume_origin and origin:
            api = hermes_client(cfg)
            prompt = (
                "[HANDOFF_RESULT]\n"
                f"job_id: {job_id}\nkind: external_command\nstatus: {state}\n"
                f"exit_code: {result['exit_code']}\nstdout_path: {stdout_path}\nstderr_path: {stderr_path}\n"
                f"duration_seconds: {result['duration_seconds']}\n\n"
                "The external task has finished. Continue the original task from this saved session. Read logs only if needed."
            )
            delivered = scheduled_origin_chat(
                cfg, store, job_id, api, origin, prompt, delivery_timeout(cfg)
            )
            result["origin_response"] = extract_text(delivered)
            store.set_job(job_id, "delivered", result)
        if not args._worker_job:
            print(json.dumps({"job_id": job_id, **result}, ensure_ascii=False, indent=2))
    except Exception as e:
        store.set_job(job_id, "failed", error=str(e))
        raise


def cmd_exclusive_run(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    """Run one external argv command under LLM_SLOT without switching services."""
    origin = args.origin or os.environ.get("HERMES_SESSION_ID")
    argv = args.command
    if args.resume_origin and not origin:
        raise RuntimeError("origin session missing; pass --origin or run from Hermes with HERMES_SESSION_ID")
    payload = {
        "argv": argv, "cwd": args.cwd, "timeout": args.timeout,
        "exclusive_llm_slot": True, "service_switch": False,
    }
    job_id = args._worker_job or store.create_job(
        "exclusive_external_command", origin, None, payload,
    )

    if args.detached and not args._worker_job:
        child = [
            sys.executable, os.path.abspath(__file__), "--config", args.config,
            "exclusive-run", "--timeout", str(args.timeout), "--_worker-job", job_id,
        ]
        if origin:
            child += ["--origin", origin]
        if args.cwd:
            child += ["--cwd", args.cwd]
        if args.resume_origin:
            child += ["--resume-origin"]
        child += ["--"] + argv
        launch_detached(cfg, child, job_id, "exclusive-run")
        print(json.dumps({"status": "HANDOFF_ACCEPTED", "job_id": job_id}))
        return

    if args._worker_job:
        print(f"[SUPERVISOR] WAITING_FOR_ORIGIN_RELEASE origin={origin}", flush=True)
        store.wait_origin_turn_release(job_id, origin_release_timeout(cfg))
        print("[SUPERVISOR] ORIGIN_RELEASED; waiting for exclusive LLM slot.", flush=True)

    job_dir = os.path.join(cfg.artifact_dir, job_id)
    pathlib.Path(job_dir).mkdir(parents=True, exist_ok=True)
    stdout_path = os.path.join(job_dir, "stdout.log")
    stderr_path = os.path.join(job_dir, "stderr.log")
    store.set_job(job_id, "running")
    try:
        with llm_slot(cfg, store, job_id):
            result = run_argv(argv, args.cwd, args.timeout, stdout_path, stderr_path)
        state = "timed_out" if result["timed_out"] else "completed"
        store.set_job(job_id, state, result)
        if args.resume_origin and origin:
            delivered = scheduled_origin_chat(
                cfg, store, job_id, hermes_client(cfg), origin,
                "[HANDOFF_RESULT]\n"
                f"job_id: {job_id}\nkind: exclusive_external_command\nstatus: {state}\n"
                f"exit_code: {result['exit_code']}\nstdout_path: {stdout_path}\n"
                f"stderr_path: {stderr_path}\nduration_seconds: {result['duration_seconds']}\n\n"
                "The exclusive same-model probe has finished. The existing LM Studio model "
                "was kept loaded. Continue the phase from this saved session.",
                delivery_timeout(cfg),
            )
            result["origin_response"] = extract_text(delivered)
            store.set_job(job_id, "delivered", result)
        if not args._worker_job:
            print(json.dumps({"job_id": job_id, **result}, ensure_ascii=False, indent=2))
    except Exception as e:
        failure_result = {
            "argv": argv, "cwd": args.cwd, "timeout": args.timeout,
            "error": str(e),
        }
        if args.resume_origin and origin:
            store.set_job(job_id, "stopped", failure_result, error=str(e))
            try:
                delivered = scheduled_origin_chat(
                    cfg, store, job_id, hermes_client(cfg), origin,
                    "[EXCLUSIVE_RUN_FAILED]\n"
                    f"job_id: {job_id}\nkind: exclusive_external_command\n"
                    f"argv: {json.dumps(argv, ensure_ascii=False)}\nerror: {e}\n\n"
                    "The exclusive command did not complete. Correct the argv or probe and "
                    "schedule exactly one replacement exclusive-run, or record a terminal blocker.",
                    delivery_timeout(cfg),
                )
                failure_result["origin_response"] = extract_text(delivered)
                store.set_job(job_id, "stopped_delivered", failure_result, error=str(e))
                print(json.dumps({
                    "status": "EXCLUSIVE_RUN_FAILURE_DELIVERED", "job_id": job_id,
                    **failure_result,
                }, ensure_ascii=False, indent=2))
                return
            except Exception as delivery_error:
                store.set_job(
                    job_id, "failed", failure_result,
                    error=f"exclusive command error: {e}; origin restart error: {delivery_error}",
                )
        else:
            store.set_job(job_id, "failed", failure_result, error=str(e))
        raise


def service_argv(cfg: Config, service: str, action: str) -> list[str]:
    val = cfg.raw.get("services", {}).get(service, {}).get(action, [])
    if val is None:
        return []
    if not isinstance(val, list) or not all(isinstance(x, str) for x in val):
        raise RuntimeError(f"services.{service}.{action} must be a JSON argv array")
    return val


def lm_studio_restore_spec(cfg: Config) -> Optional[dict[str, Any]]:
    settings = cfg.raw.get("services", {}).get("hermes_llm", {})
    if settings.get("adapter") != "lm_studio":
        return None
    cli = settings.get("cli_argv", ["lms"])
    if not isinstance(cli, list) or not cli or not all(isinstance(x, str) and x for x in cli):
        raise RuntimeError("services.hermes_llm.cli_argv must be a non-empty argv array")
    loaded = lm_studio_loaded_llms(cli)
    requested = str(settings.get("model", "auto"))
    if requested == "auto":
        if len(loaded) != 1:
            names = [m.get("identifier") or m.get("modelKey") for m in loaded]
            raise RuntimeError(f"LM Studio auto selection requires exactly one loaded LLM; found {names}")
        model_key = loaded[0].get("modelKey")
        if not model_key:
            raise RuntimeError("The running LM Studio model has no modelKey")
    else:
        model_key = requested
    # Deliberately keep only the model key in memory for this gpu-run process.
    # LM Studio remains the source of truth for variant and load parameters.
    return {"cli": cli, "model_key": model_key}


def hermes_service_control(cfg: Config, action: str, restore: Optional[dict[str, Any]]) -> None:
    if restore is None:
        run_control(service_argv(cfg, "hermes_llm", action))
        return
    if action == "stop":
        run_control(restore["cli"] + ["unload", "--all"])
    elif action == "start":
        run_control(restore["cli"] + ["load", restore["model_key"], "--yes"])
    else:
        raise ValueError(f"unknown Hermes service action: {action}")


def wait_hermes_service_ready(cfg: Config, restore: Optional[dict[str, Any]]) -> None:
    timeout = int(cfg.raw.get("service_ready_timeout_seconds", 180))
    if restore is None:
        wait_ready(service_argv(cfg, "hermes_llm", "ready"), timeout)
        return
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        cp = subprocess.run(
            restore["cli"] + ["ps", "--json"], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, encoding="utf-8", errors="replace", timeout=30,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        if cp.returncode == 0:
            try:
                loaded = json.loads(cp.stdout)
                if any(m.get("modelKey") == restore["model_key"] and m.get("status") != "loading" for m in loaded):
                    return
            except json.JSONDecodeError:
                pass
        time.sleep(2)
    raise RuntimeError(f"LM Studio model {restore['model_key']!r} did not become ready")


def run_control(argv: list[str], timeout: int = 120) -> None:
    if not argv:
        return
    cp = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, text=True)
    if cp.returncode != 0:
        raise RuntimeError(
            f"control command failed ({cp.returncode}): {shlex.join(argv)}\n"
            f"stdout: {cp.stdout[-2000:]}\nstderr: {cp.stderr[-2000:]}"
        )


def wait_ready(argv: list[str], timeout: int, poll: float = 2.0) -> None:
    if not argv:
        return
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        cp = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        if cp.returncode == 0:
            return
        last = (cp.stdout + "\n" + cp.stderr)[-2000:]
        time.sleep(poll)
    raise RuntimeError(f"readiness check timed out: {shlex.join(argv)}\n{last}")


def gpu_used_memory_mb() -> Optional[int]:
    try:
        cp = subprocess.run(
            ["nvidia-smi", "--query-gpu=memory.used", "--format=csv,noheader,nounits"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if cp.returncode != 0:
        return None
    values = []
    for line in cp.stdout.splitlines():
        line = line.strip()
        if line:
            try:
                values.append(int(line))
            except ValueError:
                return None
    return sum(values) if values else None


def wait_gpu_below(cfg: Config) -> None:
    g = cfg.raw.get("gpu", {})
    if not g.get("enabled", False):
        return
    threshold = int(g.get("free_memory_threshold_mb", 1500))
    timeout = int(g.get("wait_timeout_seconds", 180))
    poll = float(g.get("poll_seconds", 2))
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        used = gpu_used_memory_mb()
        if used is None:
            raise RuntimeError("GPU guard enabled but nvidia-smi memory query failed")
        if used <= threshold:
            return
        time.sleep(poll)
    raise RuntimeError(f"GPU memory did not fall below {threshold} MiB total used within {timeout}s")


def cmd_gpu_run(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    origin = args.origin or os.environ.get("HERMES_SESSION_ID")
    argv = args.command
    payload = {"argv": argv, "cwd": args.cwd, "timeout": args.timeout, "gpu_switch": True}
    job_id = args._worker_job or store.create_job("gpu_external_command", origin, None, payload)

    if args.detached and not args._worker_job:
        child = [sys.executable, os.path.abspath(__file__), "--config", args.config, "gpu-run",
                 "--timeout", str(args.timeout), "--_worker-job", job_id]
        if origin:
            child += ["--origin", origin]
        if args.cwd:
            child += ["--cwd", args.cwd]
        if args.resume_origin:
            child += ["--resume-origin"]
        child += ["--"] + argv
        launch_detached(cfg, child, job_id, "gpu-run")
        print(json.dumps({"status": "HANDOFF_ACCEPTED", "job_id": job_id}))
        return

    job_dir = os.path.join(cfg.artifact_dir, job_id)
    pathlib.Path(job_dir).mkdir(parents=True, exist_ok=True)
    stdout_path = os.path.join(job_dir, "stdout.log")
    stderr_path = os.path.join(job_dir, "stderr.log")
    store.set_job(job_id, "running")
    grace = float(cfg.raw.get("handoff_grace_seconds", 3))
    if grace > 0:
        time.sleep(grace)
    app_started = False
    hermes_restore = lm_studio_restore_spec(cfg)
    try:
        # Release Hermes inference VRAM, then activate the application LLM.
        hermes_service_control(cfg, "stop", hermes_restore)
        wait_gpu_below(cfg)
        run_control(service_argv(cfg, "application_llm", "start"))
        app_started = True
        wait_ready(service_argv(cfg, "application_llm", "ready"), int(cfg.raw.get("service_ready_timeout_seconds", 180)))

        result = run_argv(argv, args.cwd, args.timeout, stdout_path, stderr_path)
        state = "timed_out" if result["timed_out"] else "completed"

        # Tear down application inference before restoring Hermes.
        run_control(service_argv(cfg, "application_llm", "stop"))
        app_started = False
        wait_gpu_below(cfg)
        hermes_service_control(cfg, "start", hermes_restore)
        wait_hermes_service_ready(cfg, hermes_restore)

        store.set_job(job_id, state, result)
        if args.resume_origin and origin:
            api = hermes_client(cfg)
            delivered = scheduled_origin_chat(
                cfg, store, job_id, api, origin,
                "[HANDOFF_RESULT]\n"
                f"job_id: {job_id}\nkind: gpu_external_command\nstatus: {state}\n"
                f"exit_code: {result['exit_code']}\nstdout_path: {stdout_path}\nstderr_path: {stderr_path}\n"
                f"duration_seconds: {result['duration_seconds']}\n\n"
                "The exclusive GPU task is finished and Hermes inference has been restored. Continue the original task.",
                delivery_timeout(cfg),
            )
            result["origin_response"] = extract_text(delivered)
            store.set_job(job_id, "delivered", result)
        if not args._worker_job:
            print(json.dumps({"job_id": job_id, **result}, ensure_ascii=False, indent=2))
    except Exception as e:
        restore_error = None
        try:
            if app_started:
                run_control(service_argv(cfg, "application_llm", "stop"))
            hermes_service_control(cfg, "start", hermes_restore)
            wait_hermes_service_ready(cfg, hermes_restore)
        except Exception as restore_exc:
            restore_error = str(restore_exc)
        msg = str(e) if not restore_error else f"{e}; additionally failed to restore Hermes: {restore_error}"
        store.set_job(job_id, "failed", error=msg)
        raise RuntimeError(msg) from e


def cmd_show_job(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    row = store.job(args.job_id)
    if not row:
        raise RuntimeError("job not found")
    print(json.dumps(dict(row), ensure_ascii=False, indent=2))


def cmd_list_jobs(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    for r in store.jobs():
        print(f"{r['id']}\t{r['kind']}\t{r['state']}\t{r['created_at']}")


def cmd_list_locks(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    now = time.time()
    for r in store.resource_locks():
        remaining = max(0.0, float(r["lease_until"]) - now)
        print(f"{r['resource']}\t{r['owner']}\tlease_remaining={remaining:.1f}s")


def cmd_turn_complete(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    try:
        payload = json.load(sys.stdin)
    except Exception as e:
        raise RuntimeError(f"post_llm_call hook payload is not valid JSON: {e}") from e
    session_id = str(payload.get("session_id") or "").strip()
    if not session_id:
        raise RuntimeError("post_llm_call hook payload has no session_id")
    store.mark_origin_turn_complete(session_id)


def cmd_watch_orchestrator(cfg: Config, store: StateStore, args: argparse.Namespace) -> None:
    until_file = expand(args.until_file) if args.until_file else None
    run_orchestrator_watchdog(
        cfg, store, args.orchestrator, args.idle_seconds, args.poll_seconds,
        until_file, args.prompt, args.rotate_after,
    )


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Hermes session handoff/orchestration prototype")
    p.add_argument("--config", default=default_config_path())
    sub = p.add_subparsers(dest="cmd", required=True)

    s = sub.add_parser("init")
    s.set_defaults(fn=cmd_init)

    s = sub.add_parser("register-session")
    s.add_argument("alias")
    s.add_argument("session_id")
    s.add_argument("--role", choices=["orchestrator", "worker"], default="worker")
    s.add_argument("--parent-alias")
    s.set_defaults(fn=cmd_register_session)

    s = sub.add_parser("create-worker")
    s.add_argument("alias")
    s.add_argument("--title")
    s.add_argument("--parent-alias")
    s.set_defaults(fn=cmd_create_worker)

    s = sub.add_parser("create-orchestrator")
    s.add_argument("alias")
    s.add_argument("--title")
    s.set_defaults(fn=cmd_create_orchestrator)

    s = sub.add_parser("list-sessions")
    s.set_defaults(fn=cmd_list_sessions)

    s = sub.add_parser("chat")
    s.add_argument("session")
    s.add_argument("prompt")
    s.set_defaults(fn=cmd_chat)

    s = sub.add_parser("delegate")
    s.add_argument("worker", metavar="TASK_OR_WORKER",
                   help="task text, or a known worker alias when TASK is also supplied")
    s.add_argument("task", metavar="TASK", nargs="?",
                   help="task text when deliberately reusing a known worker")
    s.add_argument("--origin")
    s.add_argument("--resume-origin", action="store_true")
    s.add_argument("--detached", action="store_true")
    s.add_argument("--_worker-job", help=argparse.SUPPRESS)
    s.set_defaults(fn=cmd_delegate)

    s = sub.add_parser("resume-worker")
    s.add_argument("worker")
    s.add_argument("instruction")
    s.add_argument("--origin")
    s.add_argument("--resume-origin", action="store_true")
    s.add_argument("--detached", action="store_true")
    s.add_argument("--_worker-job", help=argparse.SUPPRESS)
    s.set_defaults(fn=cmd_resume_worker)

    s = sub.add_parser("inquire")
    s.add_argument("worker")
    s.add_argument("question")
    s.add_argument("--origin")
    s.add_argument("--keep-fork", action="store_true")
    s.add_argument("--resume-origin", action="store_true")
    s.add_argument("--detached", action="store_true")
    s.add_argument("--_worker-job", help=argparse.SUPPRESS)
    s.set_defaults(fn=cmd_inquire)

    s = sub.add_parser("run-command")
    s.add_argument("--origin")
    s.add_argument("--cwd")
    s.add_argument("--timeout", type=int, default=1800)
    s.add_argument("--detached", action="store_true")
    s.add_argument("--resume-origin", action="store_true")
    s.add_argument("--_worker-job", help=argparse.SUPPRESS)
    s.add_argument("command", nargs=argparse.REMAINDER)
    s.set_defaults(fn=cmd_run_command)

    s = sub.add_parser(
        "exclusive-run",
        help="run one argv command under LLM_SLOT without unloading the current model",
    )
    s.add_argument("--origin")
    s.add_argument("--cwd")
    s.add_argument("--timeout", type=int, default=600)
    s.add_argument("--detached", action="store_true")
    s.add_argument("--resume-origin", action="store_true")
    s.add_argument("--_worker-job", help=argparse.SUPPRESS)
    s.add_argument("command", nargs=argparse.REMAINDER)
    s.set_defaults(fn=cmd_exclusive_run)

    s = sub.add_parser("gpu-run")
    s.add_argument("--origin")
    s.add_argument("--cwd")
    s.add_argument("--timeout", type=int, default=1800)
    s.add_argument("--detached", action="store_true")
    s.add_argument("--resume-origin", action="store_true")
    s.add_argument("--_worker-job", help=argparse.SUPPRESS)
    s.add_argument("command", nargs=argparse.REMAINDER)
    s.set_defaults(fn=cmd_gpu_run)

    s = sub.add_parser("show-job")
    s.add_argument("job_id")
    s.set_defaults(fn=cmd_show_job)

    s = sub.add_parser("list-jobs")
    s.set_defaults(fn=cmd_list_jobs)

    s = sub.add_parser("list-locks")
    s.set_defaults(fn=cmd_list_locks)

    s = sub.add_parser(
        "watch-orchestrator",
        help="resume an idle orchestrator while preserving active jobs and LLM_SLOT",
    )
    s.add_argument("orchestrator", help="registered orchestrator alias or Hermes session id")
    s.add_argument("--idle-seconds", type=float, default=120)
    s.add_argument("--poll-seconds", type=float, default=5)
    s.add_argument("--until-file", help="stop when this phase verdict/artifact exists")
    s.add_argument("--prompt", help="custom compact recovery prompt")
    s.add_argument(
        "--rotate-after", type=int, default=2,
        help="replace the durable orchestrator session after N idle resumes with no handoff; 0 disables",
    )
    s.set_defaults(fn=cmd_watch_orchestrator)

    s = sub.add_parser("turn-complete", help=argparse.SUPPRESS)
    s.set_defaults(fn=cmd_turn_complete)
    return p


def main() -> int:
    attach_visible_console()
    if os.name == "nt":
        for stream in (sys.stdout, sys.stderr):
            if hasattr(stream, "reconfigure"):
                stream.reconfigure(encoding="utf-8", errors="replace")
    p = build_parser()
    args = p.parse_args()
    # argparse.REMAINDER keeps a literal leading "--" on some invocations.
    if getattr(args, "command", None) and args.command[0] == "--":
        args.command = args.command[1:]
    cfg = Config(load_json(args.config))
    store = StateStore(cfg.db_path)
    try:
        args.fn(cfg, store, args)
        return 0
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        return 2
    finally:
        store.close()
        if os.environ.get("HANDOFF_VISIBLE_CONSOLE") == "1":
            delay = float(cfg.raw.get("supervisor", {}).get("console_close_delay_seconds", 10))
            print(f"\n[SUPERVISOR] Job process finished. Window closes in {delay:.0f}s.", flush=True)
            if delay > 0:
                time.sleep(delay)


if __name__ == "__main__":
    raise SystemExit(main())
