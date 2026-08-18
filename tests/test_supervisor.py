import importlib.util
import io
import json
import os
import pathlib
import pytest
import sqlite3
import subprocess
import sys
import tempfile
import time
from argparse import Namespace
from unittest import mock

ROOT = pathlib.Path(__file__).resolve().parents[1]
MOD = ROOT / "supervisor" / "handoffctl.py"
spec = importlib.util.spec_from_file_location("handoffctl", MOD)
h = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = h
spec.loader.exec_module(h)


def test_state_store_roundtrip():
    with tempfile.TemporaryDirectory() as td:
        db = os.path.join(td, "state.db")
        s = h.StateStore(db)
        s.upsert_session("worker-a", "sid-1", "worker")
        assert s.session("worker-a")["hermes_session_id"] == "sid-1"
        jid = s.create_job("delegate", "origin", "sid-1", {"task": "x"})
        s.set_job(jid, "completed", {"ok": True})
        assert s.job(jid)["state"] == "completed"
        s.close()


def test_post_llm_hook_releases_only_accepted_origin_job():
    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        accepted = store.create_job("delegate", "origin-one", None, {"task": "x"})
        running = store.create_job("delegate", "origin-one", None, {"task": "y"})
        store.set_job(running, "running")
        with mock.patch.object(h.sys, "stdin", io.StringIO('{"session_id":"origin-one"}')):
            h.cmd_turn_complete(cfg, store, Namespace())
        assert store.origin_turn_released(accepted)
        assert not store.origin_turn_released(running)
        store.wait_origin_turn_release(accepted, timeout_seconds=0.1, poll_seconds=0.001)
        store.close()


def test_origin_release_wait_times_out_without_hook_signal():
    with tempfile.TemporaryDirectory() as td:
        store = h.StateStore(os.path.join(td, "state.db"))
        job_id = store.create_job("delegate", "origin-one", None, {"task": "x"})
        with pytest.raises(TimeoutError, match="post_llm_call"):
            store.wait_origin_turn_release(job_id, timeout_seconds=0.01, poll_seconds=0.001)
        store.close()


@pytest.mark.skipif(os.name != "nt", reason="Windows console behavior")
def test_detached_supervisor_opens_visible_windows_console():
    cfg = h.Config({"supervisor": {"show_console": True}})
    with mock.patch.object(h.subprocess, "Popen", return_value=mock.Mock()) as popen:
        h.launch_detached(cfg, ["python", "handoffctl.py"], "job-visible", "delegate")
    kwargs = popen.call_args.kwargs
    assert kwargs["creationflags"] & subprocess.CREATE_NEW_CONSOLE
    assert kwargs["env"]["HANDOFF_VISIBLE_CONSOLE"] == "1"
    assert kwargs["env"]["HANDOFF_JOB_ID"] == "job-visible"


def test_llm_slot_is_exclusive_across_store_connections():
    with tempfile.TemporaryDirectory() as td:
        db_path = os.path.join(td, "state.db")
        first = h.StateStore(db_path)
        second = h.StateStore(db_path)
        first.acquire_resource("LLM_SLOT", "job-one", wait_timeout=1, lease_seconds=60, poll_seconds=0.005)
        with pytest.raises(TimeoutError, match="LLM_SLOT"):
            second.acquire_resource("LLM_SLOT", "job-two", wait_timeout=0.03, lease_seconds=60, poll_seconds=0.005)
        first.release_resource("LLM_SLOT", "job-one")
        second.acquire_resource("LLM_SLOT", "job-two", wait_timeout=1, lease_seconds=60, poll_seconds=0.005)
        second.release_resource("LLM_SLOT", "job-two")
        assert first.resource_locks() == []
        first.close()
        second.close()


def test_llm_slot_releases_when_chat_fails():
    class BrokenHermes:
        def chat(self, sid, prompt):
            raise RuntimeError("model failed")

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        with pytest.raises(RuntimeError, match="model failed"):
            h.scheduled_chat(cfg, store, "job-broken", BrokenHermes(), "sid", "prompt")
        assert store.resource_locks() == []
        store.close()


def test_orchestration_busy_tracks_jobs_and_llm_slot():
    with tempfile.TemporaryDirectory() as td:
        store = h.StateStore(os.path.join(td, "state.db"))
        assert not h.orchestration_busy(store)
        job_id = store.create_job("delegate", "origin", None, {"task": "x"})
        assert h.orchestration_busy(store)
        assert h.orchestration_busy(store, "origin")
        assert not h.orchestration_busy(store, "another-origin")
        store.set_job(job_id, "delivered", {"ok": True})
        assert not h.orchestration_busy(store)
        store.acquire_resource("LLM_SLOT", "probe", 1, 60, 0.001)
        assert h.orchestration_busy(store)
        store.release_resource("LLM_SLOT", "probe")
        assert not h.orchestration_busy(store)
        store.close()


def test_durable_idle_age_uses_latest_job_update():
    with tempfile.TemporaryDirectory() as td:
        store = h.StateStore(os.path.join(td, "state.db"))
        assert h.durable_idle_age_seconds(store) == 0
        job_id = store.create_job("delegate", "origin", None, {"task": "x"})
        assert 0 <= h.durable_idle_age_seconds(store) < 2
        store.set_job(job_id, "delivered")
        assert 0 <= h.durable_idle_age_seconds(store) < 2
        store.close()


def test_watchdog_stops_when_phase_verdict_exists_without_resuming():
    with tempfile.TemporaryDirectory() as td:
        verdict = os.path.join(td, "P2.json")
        pathlib.Path(verdict).write_text("{}", encoding="utf-8")
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        store.upsert_session("orchestrator", "origin-sid", "orchestrator")
        with mock.patch.object(h, "scheduled_origin_chat") as resume:
            h.run_orchestrator_watchdog(
                cfg, store, "orchestrator", 0, 0.001, verdict, None,
                rotate_after=2, max_cycles=1,
            )
        resume.assert_not_called()
        assert store.resource_locks() == []
        store.close()


def test_forced_origin_timeout_releases_handoffs_created_during_turn():
    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        child = store.create_job("delegate", "origin-sid", None, {"task": "next"})
        unrelated = store.create_job("delegate", "other-origin", None, {"task": "other"})
        with mock.patch.object(
            h, "scheduled_chat", side_effect=subprocess.TimeoutExpired("hermes chat", 9),
        ) as scheduled:
            with pytest.raises(subprocess.TimeoutExpired):
                h.scheduled_origin_chat(
                    cfg, store, "parent-job", mock.Mock(), "origin-sid", "review", 9,
                )
        assert scheduled.call_count == 1
        assert "[ORCHESTRATOR_CONTROL_RULE]" in scheduled.call_args.args[5]
        assert store.origin_turn_released(child)
        assert not store.origin_turn_released(unrelated)
        store.close()


def test_origin_timeout_without_child_retries_once_with_control_only_prompt():
    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        recovered = {"response": "next handoff accepted"}
        with mock.patch.object(
            h, "scheduled_chat",
            side_effect=[subprocess.TimeoutExpired("hermes chat", 9), recovered],
        ) as scheduled:
            result = h.scheduled_origin_chat(
                cfg, store, "parent-job", mock.Mock(), "origin-sid",
                "[WORKER_RESULT]\nvery large worker payload", 9,
            )
        assert result == recovered
        assert scheduled.call_count == 2
        first_prompt = scheduled.call_args_list[0].args[5]
        retry_prompt = scheduled.call_args_list[1].args[5]
        assert "very large worker payload" in first_prompt
        assert "[ORCHESTRATOR_CONTROL_RULE]" in first_prompt
        assert "[ORIGIN_DELIVERY_RECOVERY]" in retry_prompt
        assert "very large worker payload" not in retry_prompt
        store.close()


def test_origin_chat_applies_console_idle_timeout_and_restores_client():
    class IdleAwareClient:
        visible_idle_timeout_seconds = None

        def chat_with_timeout(self, sid, prompt, timeout_seconds):
            assert sid == "origin-sid"
            assert self.visible_idle_timeout_seconds == 17
            return {"response": "done"}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({
            "state_db": os.path.join(td, "state.db"),
            "supervisor": {"origin_idle_timeout_seconds": 17},
        })
        store = h.StateStore(cfg.db_path)
        client = IdleAwareClient()
        out = h.scheduled_origin_chat(
            cfg, store, "origin-job", client, "origin-sid", "review", 900,
        )
        assert out == {"response": "done"}
        assert client.visible_idle_timeout_seconds is None
        store.close()


def test_origin_delivery_recovery_is_bounded_to_one_retry():
    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        with mock.patch.object(
            h, "scheduled_chat", side_effect=subprocess.TimeoutExpired("hermes chat", 9),
        ) as scheduled:
            with pytest.raises(subprocess.TimeoutExpired):
                h.scheduled_origin_chat(
                    cfg, store, "parent-job", mock.Mock(), "origin-sid", "review", 9,
                )
        assert scheduled.call_count == 2
        store.close()


def test_second_origin_timeout_releases_handoff_created_by_recovery_turn():
    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        calls = 0
        child = None

        def time_out_and_create_child(*_args, **_kwargs):
            nonlocal calls, child
            calls += 1
            if calls == 2:
                child = store.create_job(
                    "delegate", "origin-sid", None, {"task": "next"},
                )
            raise subprocess.TimeoutExpired("hermes chat", 9)

        with mock.patch.object(h, "scheduled_chat", side_effect=time_out_and_create_child):
            with pytest.raises(subprocess.TimeoutExpired):
                h.scheduled_origin_chat(
                    cfg, store, "parent-job", mock.Mock(), "origin-sid", "review", 9,
                )
        assert calls == 2
        assert child is not None
        assert store.origin_turn_released(child)
        store.close()


def test_run_argv_success():
    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "out")
        err = os.path.join(td, "err")
        r = h.run_argv([sys.executable, "-c", "print('ok')"], None, 5, out, err)
        assert r["exit_code"] == 0
        assert not r["timed_out"]
        assert pathlib.Path(out).read_text().strip() == "ok"


def test_run_argv_timeout():
    with tempfile.TemporaryDirectory() as td:
        out = os.path.join(td, "out")
        err = os.path.join(td, "err")
        r = h.run_argv([sys.executable, "-c", "import time; time.sleep(5)"], None, 1, out, err)
        assert r["timed_out"]


def test_cli_adapter_uses_supported_resume_command():
    cfg = h.Config({"hermes": {"adapter": "cli", "cli_argv": ["hermes.exe"], "default_source": "tool"}})
    completed = subprocess.CompletedProcess([], 0, stdout="continued\n", stderr="")
    with mock.patch.object(h.subprocess, "run", return_value=completed) as run:
        assert h.HermesCLI(cfg).chat("session-1", "result")["response"] == "continued"
    argv = run.call_args.args[0]
    assert argv == [
        "hermes.exe", "chat", "--resume", "session-1", "--query", "result", "--quiet", "--source", "tool"
    ]
    assert run.call_args.kwargs["creationflags"] == (
        subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    )


def test_cli_adapter_auto_selects_only_running_lm_studio_model():
    cfg = h.Config({"hermes": {
        "adapter": "cli", "cli_argv": ["hermes.exe"], "model": "auto",
        "lm_studio_cli_argv": ["lms.exe"],
    }})
    discovered = subprocess.CompletedProcess(
        [], 0, stdout=json.dumps([{"type": "llm", "modelKey": "qwen/qwen3.8-27b"}]), stderr=""
    )
    chatted = subprocess.CompletedProcess([], 0, stdout="continued\n", stderr="")
    with mock.patch.object(h.subprocess, "run", side_effect=[discovered, chatted]) as run:
        assert h.HermesCLI(cfg).chat("session-1", "result")["response"] == "continued"
    assert run.call_args_list[1].args[0][-2:] == ["--model", "qwen/qwen3.8-27b"]


def test_cli_adapter_persists_restricted_toolsets_on_every_resume():
    extra = [
        "--toolsets", "terminal,file,skills", "--skills", "session-handoff", "--yolo",
    ]
    cfg = h.Config({"hermes": {
        "adapter": "cli", "cli_argv": ["hermes.exe"],
        "model": "qwen/qwen3.8-27b", "cli_chat_extra_argv": extra,
    }})
    completed = subprocess.CompletedProcess([], 0, stdout="continued\n", stderr="")
    with mock.patch.object(h.subprocess, "run", return_value=completed) as run:
        h.HermesCLI(cfg).chat("origin-sid", "continue phase")
    argv = run.call_args.args[0]
    assert argv[argv.index("--source") + 2:-2] == extra
    assert "delegation" not in argv[argv.index("--toolsets") + 1].split(",")


def test_cli_adapter_creates_and_renames_persistent_session():
    extra = ["--toolsets", "terminal,file,skills", "--skills", "session-handoff"]
    cfg = h.Config({"hermes": {
        "adapter": "cli", "cli_argv": ["hermes.exe"], "model": "qwen/qwen3.8-27b",
        "cli_chat_extra_argv": extra,
    }})
    created = subprocess.CompletedProcess(
        [], 0, stdout="WORKER_READY\n", stderr="\nsession_id: 20260816_worker_abc123\n"
    )
    renamed = subprocess.CompletedProcess([], 0, stdout="renamed\n", stderr="")
    with mock.patch.object(h.subprocess, "run", side_effect=[created, renamed]) as run:
        out = h.HermesCLI(cfg).create_session("worker-phase2")
    assert out == {"session_id": "20260816_worker_abc123", "response": "WORKER_READY"}
    create_argv = run.call_args_list[0].args[0]
    assert create_argv[:2] == ["hermes.exe", "chat"]
    assert create_argv[create_argv.index("--pass-session-id") + 1:-2] == extra
    assert create_argv[-2:] == ["--model", "qwen/qwen3.8-27b"]
    assert run.call_args_list[1].args[0] == [
        "hermes.exe", "sessions", "rename", "20260816_worker_abc123", "worker-phase2"
    ]


def test_visible_cli_adapter_accepts_human_session_summary_label():
    cfg = h.Config({"hermes": {
        "adapter": "cli", "cli_argv": ["hermes.exe"], "model": "qwen/qwen3.8-27b",
    }})
    visible_output = subprocess.CompletedProcess(
        [], 0, stdout="WORKER_READY\n\nSession:        20260816_visible_abc123\n", stderr=""
    )
    renamed = subprocess.CompletedProcess([], 0, stdout="renamed\n", stderr="")
    with mock.patch.dict(h.os.environ, {"HANDOFF_VISIBLE_CONSOLE": "1"}), \
         mock.patch.object(h.HermesCLI, "_run", side_effect=[visible_output, renamed]), \
         mock.patch.object(h.HermesCLI, "_last_assistant_response", return_value="WORKER_READY"):
        out = h.HermesCLI(cfg).create_session("worker-visible")
    assert out["session_id"] == "20260816_visible_abc123"


def test_visible_cli_reader_failure_does_not_strand_supervisor():
    class BrokenOutput:
        def __iter__(self):
            raise OSError("simulated broken Windows pipe")

    proc = mock.Mock()
    proc.stdout = BrokenOutput()
    proc.poll.return_value = 0
    proc.wait.return_value = 0
    cfg = h.Config({"hermes": {
        "adapter": "cli", "cli_argv": ["hermes.exe"],
        "visible_exit_drain_seconds": 0.01,
    }})
    with mock.patch.object(h.subprocess, "Popen", return_value=proc):
        completed = h.HermesCLI(cfg)._run_visible(["hermes.exe", "chat"], "resume", 1)
    assert completed.returncode == 0
    assert completed.stdout == ""


def test_phase2_worker_alias_keeps_same_session_across_delegate_and_resume():
    class FakeHermes:
        def __init__(self):
            self.chats = []

        def create_session(self, title=None, role="worker"):
            return {"session_id": "worker-sid-1", "response": "WORKER_READY"}

        def chat(self, sid, prompt):
            self.chats.append((sid, prompt))
            if sid == "origin-sid":
                return {"response": "origin-reviewed"}
            return {"response": f"worker-turn-{sum(1 for call in self.chats if call[0] == sid)}"}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db"), "artifact_dir": os.path.join(td, "artifacts")})
        store = h.StateStore(cfg.db_path)
        fake = FakeHermes()
        create_args = Namespace(alias="worker-phase2", title=None, parent_alias=None)
        delegate_args = Namespace(
            worker="worker-phase2", task="first task", origin="origin-sid", resume_origin=True,
            detached=False, _worker_job=None, config="unused",
        )
        resume_args = Namespace(
            worker="worker-phase2", instruction="follow-up", origin=None, resume_origin=False,
            detached=False, _worker_job=None, config="unused",
        )
        with mock.patch.object(h, "hermes_client", return_value=fake):
            h.cmd_create_worker(cfg, store, create_args)
            h.cmd_delegate(cfg, store, delegate_args)
            h.cmd_resume_worker(cfg, store, resume_args)
        worker = store.session("worker-phase2")
        assert worker["hermes_session_id"] == "worker-sid-1"
        assert worker["status"] == "completed"
        worker_calls = [call for call in fake.chats if call[0] == "worker-sid-1"]
        assert len(worker_calls) == 2
        assert "[ORCHESTRATED_WORKER_TASK]" in worker_calls[0][1]
        assert "[ORCHESTRATOR_FEEDBACK]" in worker_calls[1][1]
        assert [job["kind"] for job in reversed(store.jobs())] == ["delegate", "resume_worker"]
        assert all(job["state"] in {"completed", "delivered"} for job in store.jobs())
        store.close()


def test_delegate_detached_accepts_unknown_alias_and_reserves_creation():
    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        args = Namespace(
            worker="worker-auto", task="do the work", origin="origin-sid", resume_origin=True,
            detached=True, _worker_job=None, config="config.json",
        )
        with mock.patch.object(h.subprocess, "Popen") as popen:
            h.cmd_delegate(cfg, store, args)
        job = store.jobs()[0]
        assert job["state"] == "accepted"
        assert job["target_session_id"] is None
        assert store.session("worker-auto") is None
        assert store.worker_reservation("worker-auto")["job_id"] == job["id"]
        assert "--_worker-job" in popen.call_args.args[0]
        store.close()


def test_delegate_with_only_task_generates_worker_alias(capsys):
    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        args = h.build_parser().parse_args([
            "delegate", "inspect whatever task the user supplies", "--origin", "origin-sid",
            "--detached", "--resume-origin",
        ])
        args.config = "config.json"
        with mock.patch.object(h.subprocess, "Popen"):
            h.cmd_delegate(cfg, store, args)
        accepted = json.loads(capsys.readouterr().out)
        assert accepted["status"] == "HANDOFF_ACCEPTED"
        assert accepted["worker"].startswith("worker-")
        assert accepted["worker"] != "inspect whatever task the user supplies"
        job = store.jobs()[0]
        payload = json.loads(job["payload_json"])
        assert payload["task"] == "inspect whatever task the user supplies"
        assert payload["worker"] == accepted["worker"]
        assert store.worker_reservation(accepted["worker"])["job_id"] == job["id"]
        store.close()


def test_delegate_child_auto_creates_unknown_persistent_worker():
    class FakeHermes:
        def __init__(self):
            self.created = []
            self.chats = []

        def create_session(self, title=None, role="worker"):
            self.created.append((title, role))
            return {"session_id": "auto-worker-sid", "response": "WORKER_READY"}

        def chat(self, sid, prompt):
            self.chats.append((sid, prompt))
            return {"response": '{"kind":"worker_final","status":"completed","summary":"done"}'}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({
            "state_db": os.path.join(td, "state.db"),
            "handoff_grace_seconds": 0,
            "supervisor": {"origin_release_timeout_seconds": 1},
        })
        store = h.StateStore(cfg.db_path)
        job_id = store.create_job(
            "delegate", "origin-sid", None, {"worker": "worker-auto", "task": "do the work"}
        )
        store.reserve_worker("worker-auto", job_id)
        store.mark_origin_turn_complete("origin-sid")
        args = Namespace(
            worker="worker-auto", task="do the work", origin="origin-sid", resume_origin=False,
            detached=False, _worker_job=job_id, config="unused",
        )
        fake = FakeHermes()
        with mock.patch.object(h, "hermes_client", return_value=fake):
            h.cmd_delegate(cfg, store, args)
        worker = store.session("worker-auto")
        assert worker["hermes_session_id"] == "auto-worker-sid"
        assert worker["status"] == "completed"
        assert store.job(job_id)["target_session_id"] == "auto-worker-sid"
        assert store.job(job_id)["state"] == "completed"
        assert store.worker_reservation("worker-auto") is None
        assert fake.created == [("worker-auto", "worker")]
        assert fake.chats[0][0] == "auto-worker-sid"
        store.close()


def test_delegate_rejects_second_auto_creation_for_same_alias():
    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        first = Namespace(
            worker="worker-auto", task="first", origin="origin-one", resume_origin=True,
            detached=True, _worker_job=None, config="config.json",
        )
        second = Namespace(
            worker="worker-auto", task="second", origin="origin-two", resume_origin=True,
            detached=True, _worker_job=None, config="config.json",
        )
        with mock.patch.object(h.subprocess, "Popen"):
            h.cmd_delegate(cfg, store, first)
            with pytest.raises(RuntimeError, match="already being created"):
                h.cmd_delegate(cfg, store, second)
        assert [job["state"] for job in store.jobs()] == ["failed", "accepted"]
        store.close()


def test_phase2_detached_reserves_worker_and_rejects_second_turn():
    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db"), "artifact_dir": os.path.join(td, "artifacts")})
        store = h.StateStore(cfg.db_path)
        store.upsert_session("worker-busy", "worker-sid-busy", "worker")
        args = Namespace(
            worker="worker-busy", instruction="first", origin=None, resume_origin=False,
            detached=True, _worker_job=None, config="config.json",
        )
        with mock.patch.object(h.subprocess, "Popen"):
            h.cmd_resume_worker(cfg, store, args)
        assert store.session("worker-busy")["status"] == "queued"
        with pytest.raises(RuntimeError, match="already running"):
            h.cmd_resume_worker(cfg, store, args)
        store.close()


def test_phase2_reserved_child_can_consume_its_own_job():
    class FakeHermes:
        def chat(self, sid, prompt):
            return {"response": "reserved-child-finished"}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({
            "state_db": os.path.join(td, "state.db"), "artifact_dir": os.path.join(td, "artifacts"),
            "handoff_grace_seconds": 0,
            "supervisor": {"origin_release_timeout_seconds": 0},
        })
        store = h.StateStore(cfg.db_path)
        store.upsert_session("worker-reserved", "worker-sid", "worker")
        job_id = store.create_job("resume_worker", None, "worker-sid", {"worker": "worker-reserved"})
        store.set_session_status("worker-reserved", "running")
        args = Namespace(
            worker="worker-reserved", instruction="continue", origin=None, resume_origin=False,
            detached=False, _worker_job=job_id, config="unused",
        )
        with mock.patch.object(h, "hermes_client", return_value=FakeHermes()):
            h.cmd_resume_worker(cfg, store, args)
        assert store.job(job_id)["state"] == "completed"
        assert store.session("worker-reserved")["status"] == "completed"
        store.close()


def test_phase2_delivery_failure_does_not_mark_completed_worker_failed():
    class FakeHermes:
        def chat(self, sid, prompt):
            if sid == "origin-sid":
                raise RuntimeError("origin unavailable")
            return {"response": "worker finished"}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db"), "artifact_dir": os.path.join(td, "artifacts")})
        store = h.StateStore(cfg.db_path)
        store.upsert_session("worker-delivery", "worker-sid", "worker")
        args = Namespace(
            worker="worker-delivery", task="task", origin="origin-sid", resume_origin=True,
            detached=False, _worker_job=None, config="unused",
        )
        with mock.patch.object(h, "hermes_client", return_value=FakeHermes()):
            with pytest.raises(RuntimeError, match="origin unavailable"):
                h.cmd_delegate(cfg, store, args)
        assert store.session("worker-delivery")["status"] == "completed"
        assert store.jobs()[0]["state"] == "delivery_failed"
        store.close()


def test_worker_failure_restarts_origin_and_delivers_failure():
    class FakeHermes:
        def __init__(self):
            self.calls = []

        def chat(self, sid, prompt):
            self.calls.append((sid, prompt))
            if sid == "worker-sid":
                raise RuntimeError("worker process timed out")
            return {"response": "origin reviewed worker failure"}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        store.upsert_session("worker-fails", "worker-sid", "worker")
        args = Namespace(
            worker="worker-fails", task="task", origin="origin-sid", resume_origin=True,
            detached=False, _worker_job=None, config="unused",
        )
        fake = FakeHermes()
        with mock.patch.object(h, "hermes_client", return_value=fake):
            h.cmd_delegate(cfg, store, args)
        job = store.jobs()[0]
        assert job["state"] == "stopped_delivered"
        assert store.session("worker-fails")["status"] == "stopped"
        assert fake.calls[-1][0] == "origin-sid"
        assert "[WORKER_FAILED]" in fake.calls[-1][1]
        store.close()


def test_resume_worker_failure_restarts_origin_and_delivers_failure():
    class FakeHermes:
        def __init__(self):
            self.calls = []

        def chat(self, sid, prompt):
            self.calls.append((sid, prompt))
            if sid == "worker-sid":
                raise RuntimeError("worker process timed out")
            return {"response": "origin reviewed resumed worker failure"}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        store.upsert_session("worker-fails", "worker-sid", "worker")
        args = Namespace(
            worker="worker-fails", instruction="continue", origin="origin-sid",
            resume_origin=True, detached=False, _worker_job=None, config="unused",
        )
        fake = FakeHermes()
        with mock.patch.object(h, "hermes_client", return_value=fake):
            h.cmd_resume_worker(cfg, store, args)
        job = store.jobs()[0]
        assert job["state"] == "stopped_delivered"
        assert store.session("worker-fails")["status"] == "stopped"
        assert fake.calls[-1][0] == "origin-sid"
        assert "[WORKER_FAILED]" in fake.calls[-1][1]
        store.close()


def test_worker_failure_is_stopped_during_recovery_and_failed_if_recovery_fails():
    class FakeHermes:
        def __init__(self, state_store):
            self.store = state_store

        def chat(self, sid, prompt):
            if sid == "worker-sid":
                raise RuntimeError("worker crashed")
            assert self.store.jobs()[0]["state"] == "stopped"
            assert self.store.session("worker-fails")["status"] == "stopped"
            raise RuntimeError("origin unavailable")

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        store.upsert_session("worker-fails", "worker-sid", "worker")
        args = Namespace(
            worker="worker-fails", task="task", origin="origin-sid", resume_origin=True,
            detached=False, _worker_job=None, config="unused",
        )
        with mock.patch.object(h, "hermes_client", return_value=FakeHermes(store)):
            with pytest.raises(RuntimeError, match="worker crashed"):
                h.cmd_delegate(cfg, store, args)
        job = store.jobs()[0]
        assert job["state"] == "failed"
        assert store.session("worker-fails")["status"] == "failed"
        assert "origin unavailable" in job["error"]
        store.close()


def test_resume_worker_timeout_exports_full_session_path_for_origin_decision():
    class FakeHermes:
        def __init__(self):
            self.calls = []
            self.exports = []

        def chat(self, sid, prompt):
            self.calls.append((sid, prompt))
            if sid == "worker-sid":
                raise subprocess.TimeoutExpired(["hermes", "chat"], 900)
            return {"response": "origin chose next action"}

        def export_session(self, sid, output_path):
            self.exports.append((sid, output_path))
            return os.path.abspath(output_path)

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db"), "artifact_dir": os.path.join(td, "artifacts")})
        store = h.StateStore(cfg.db_path)
        store.upsert_session("worker-times-out", "worker-sid", "worker")
        args = Namespace(
            worker="worker-times-out", instruction="continue", origin="origin-sid",
            resume_origin=True, detached=False, _worker_job=None, config="unused",
        )
        fake = FakeHermes()
        with mock.patch.object(h, "hermes_client", return_value=fake):
            h.cmd_resume_worker(cfg, store, args)
        job = store.jobs()[0]
        result = json.loads(job["result_json"])
        export_path = result["worker_session_export_path"]
        assert job["state"] == "stopped_delivered"
        assert fake.exports == [("worker-sid", export_path)]
        assert export_path.endswith("worker-session-worker-sid.jsonl")
        assert "[WORKER_TIMED_OUT]" in fake.calls[-1][1]
        assert f"worker_session_export_path: {export_path}" in fake.calls[-1][1]
        assert "Read that complete session export" in fake.calls[-1][1]
        store.close()


def test_phase3_worker_envelope_classification():
    yielded = """```json
    {"kind":"worker_yield","status":"needs_review","summary":"half done","next_step":"continue"}
    ```"""
    final = '{"kind":"worker_final","status":"completed","summary":"done"}'
    hermes_alias = '{"envelope":"worker_yield","status":"needs_review","summary":"real shape"}'
    assert h.classify_worker_response(yielded)[0] == "yielded"
    assert h.classify_worker_response(final)[0] == "completed"
    alias_state, alias_envelope = h.classify_worker_response(hermes_alias)
    assert alias_state == "yielded"
    assert alias_envelope["kind"] == "worker_yield"
    assert h.classify_worker_response("plain response") == ("completed", None)


def test_phase3_create_orchestrator_registers_role_and_uses_slot():
    class FakeHermes:
        def __init__(self):
            self.role = None

        def create_session(self, title=None, role="worker"):
            self.role = role
            return {"session_id": "orchestrator-sid", "response": "ORCHESTRATOR_READY"}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db")})
        store = h.StateStore(cfg.db_path)
        fake = FakeHermes()
        args = Namespace(alias="orchestrator-e2e", title=None)
        with mock.patch.object(h, "hermes_client", return_value=fake):
            h.cmd_create_orchestrator(cfg, store, args)
        row = store.session("orchestrator-e2e")
        assert row["role"] == "orchestrator"
        assert row["hermes_session_id"] == "orchestrator-sid"
        assert fake.role == "orchestrator"
        assert store.resource_locks() == []
        store.close()


def test_phase3_yield_status_is_persisted_for_orchestrator_review():
    envelope = {
        "kind": "worker_yield", "status": "needs_review", "summary": "parser ready",
        "questions": ["keep API?"], "next_step": "integration",
    }

    class FakeHermes:
        def chat(self, sid, prompt):
            return {"response": json.dumps(envelope)}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db"), "handoff_grace_seconds": 0})
        store = h.StateStore(cfg.db_path)
        store.upsert_session("worker-yield", "worker-yield-sid", "worker")
        args = Namespace(
            worker="worker-yield", instruction="continue until review", origin=None,
            resume_origin=False, detached=False, _worker_job=None, config="unused",
        )
        with mock.patch.object(h, "hermes_client", return_value=FakeHermes()):
            h.cmd_resume_worker(cfg, store, args)
        job = store.jobs()[0]
        result = json.loads(job["result_json"])
        assert store.session("worker-yield")["status"] == "yielded"
        assert result["worker_state"] == "yielded"
        assert result["structured_response"] == envelope
        assert store.resource_locks() == []
        store.close()


def test_lm_studio_auto_selection_keeps_only_running_model_key():
    cfg = h.Config({"services": {"hermes_llm": {
        "adapter": "lm_studio", "cli_argv": ["lms.exe"], "model": "auto"
    }}})
    running = [{
        "type": "llm", "modelKey": "qwen/qwen3.8-27b", "selectedVariant": "qwen/qwen3.8-27b@q8_0",
        "identifier": "qwen/qwen3.8-27b", "contextLength": 64000, "parallel": 4,
    }]
    completed = subprocess.CompletedProcess([], 0, stdout=json.dumps(running), stderr="")
    with mock.patch.object(h.subprocess, "run", return_value=completed):
        spec = h.lm_studio_restore_spec(cfg)
    assert spec == {"cli": ["lms.exe"], "model_key": "qwen/qwen3.8-27b"}


def test_lm_studio_load_does_not_replay_model_parameters():
    cfg = h.Config({})
    spec = {"cli": ["lms.exe"], "model_key": "qwen/qwen3.8-27b"}
    with mock.patch.object(h, "run_control") as run:
        h.hermes_service_control(cfg, "stop", spec)
        h.hermes_service_control(cfg, "start", spec)
    assert run.call_args_list == [
        mock.call(["lms.exe", "unload", "--all"]),
        mock.call(["lms.exe", "load", "qwen/qwen3.8-27b", "--yes"]),
    ]


def test_phase1_synchronous_command_delivers_result():
    class FakeHermes:
        def __init__(self):
            self.calls = []

        def chat(self, sid, prompt):
            self.calls.append((sid, prompt))
            return {"response": "origin continued"}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({"state_db": os.path.join(td, "state.db"), "artifact_dir": os.path.join(td, "artifacts")})
        store = h.StateStore(cfg.db_path)
        fake = FakeHermes()
        args = Namespace(
            origin="origin-1", command=[sys.executable, "-c", "print('external-ok')"], cwd=None,
            timeout=5, detached=False, resume_origin=True, _worker_job=None, config="unused",
        )
        with mock.patch.object(h, "hermes_client", return_value=fake):
            h.cmd_run_command(cfg, store, args)
        job = store.jobs()[0]
        result = json.loads(job["result_json"])
        assert job["state"] == "delivered"
        assert pathlib.Path(result["stdout_path"]).read_text().strip() == "external-ok"
        assert fake.calls[0][0] == "origin-1"
        assert "[HANDOFF_RESULT]" in fake.calls[0][1]
        store.close()


def test_exclusive_run_waits_for_release_holds_slot_and_never_switches_services():
    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({
            "state_db": os.path.join(td, "state.db"),
            "artifact_dir": os.path.join(td, "artifacts"),
            "supervisor": {"origin_release_timeout_seconds": 5},
        })
        store = h.StateStore(cfg.db_path)
        job_id = store.create_job("exclusive_external_command", "origin-1", None, {})
        release_seen = False

        def wait_for_release(wait_job_id, timeout):
            nonlocal release_seen
            assert wait_job_id == job_id
            assert timeout == 5
            assert store.job(job_id)["state"] == "accepted"
            release_seen = True

        def run_under_slot(argv, cwd, timeout, stdout_path, stderr_path):
            assert release_seen
            locks = store.resource_locks()
            assert [(row["resource"], row["owner"]) for row in locks] == [("LLM_SLOT", job_id)]
            return {
                "exit_code": 0, "timed_out": False, "duration_seconds": 0.01,
                "stdout_path": stdout_path, "stderr_path": stderr_path,
            }

        args = Namespace(
            origin="origin-1", command=["probe.exe"], cwd=None, timeout=30,
            detached=False, resume_origin=False, _worker_job=job_id, config="unused",
        )
        with (
            mock.patch.object(store, "wait_origin_turn_release", side_effect=wait_for_release),
            mock.patch.object(h, "run_argv", side_effect=run_under_slot),
            mock.patch.object(h, "hermes_service_control") as service_control,
            mock.patch.object(h, "run_control") as run_control,
        ):
            h.cmd_exclusive_run(cfg, store, args)
        assert store.job(job_id)["state"] == "completed"
        assert store.resource_locks() == []
        service_control.assert_not_called()
        run_control.assert_not_called()
        store.close()


def test_exclusive_run_timeout_is_recorded_without_service_switching():
    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({
            "state_db": os.path.join(td, "state.db"),
            "artifact_dir": os.path.join(td, "artifacts"),
        })
        store = h.StateStore(cfg.db_path)
        args = Namespace(
            origin=None, command=["slow-probe.exe"], cwd=None, timeout=1,
            detached=False, resume_origin=False, _worker_job=None, config="unused",
        )
        fake_result = {
            "exit_code": -1, "timed_out": True, "duration_seconds": 1.0,
            "stdout_path": "stdout.log", "stderr_path": "stderr.log",
        }
        with mock.patch.object(h, "run_argv", return_value=fake_result):
            h.cmd_exclusive_run(cfg, store, args)
        assert store.jobs()[0]["state"] == "timed_out"
        assert store.resource_locks() == []
        store.close()


def test_exclusive_run_execution_error_is_delivered_to_origin_for_correction():
    class FakeHermes:
        def chat(self, sid, prompt):
            assert sid == "origin-1"
            assert "[EXCLUSIVE_RUN_FAILED]" in prompt
            assert "not a valid Win32 application" in prompt
            assert store.jobs()[0]["state"] == "stopped"
            return {"response": "retry with python interpreter"}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({
            "state_db": os.path.join(td, "state.db"),
            "artifact_dir": os.path.join(td, "artifacts"),
        })
        store = h.StateStore(cfg.db_path)
        args = Namespace(
            origin="origin-1", command=["probe.py"], cwd=None, timeout=5,
            detached=False, resume_origin=True, _worker_job=None, config="unused",
        )
        with (
            mock.patch.object(
                h, "run_argv",
                side_effect=OSError(193, "%1 is not a valid Win32 application"),
            ),
            mock.patch.object(h, "hermes_client", return_value=FakeHermes()),
        ):
            h.cmd_exclusive_run(cfg, store, args)
        job = store.jobs()[0]
        result = json.loads(job["result_json"])
        assert job["state"] == "stopped_delivered"
        assert result["argv"] == ["probe.py"]
        assert result["origin_response"] == "retry with python interpreter"
        assert store.resource_locks() == []
        store.close()


def test_exclusive_run_releases_probe_slot_before_origin_delivery():
    class FakeHermes:
        def chat(self, sid, prompt):
            assert sid == "origin-1"
            assert "kind: exclusive_external_command" in prompt
            return {"response": "phase continued"}

    with tempfile.TemporaryDirectory() as td:
        cfg = h.Config({
            "state_db": os.path.join(td, "state.db"),
            "artifact_dir": os.path.join(td, "artifacts"),
        })
        store = h.StateStore(cfg.db_path)
        args = Namespace(
            origin="origin-1", command=[sys.executable, "-c", "print('probe-ok')"],
            cwd=None, timeout=5, detached=False, resume_origin=True,
            _worker_job=None, config="unused",
        )
        with mock.patch.object(h, "hermes_client", return_value=FakeHermes()):
            h.cmd_exclusive_run(cfg, store, args)
        job = store.jobs()[0]
        events = [
            row["event"] for row in store.db.execute(
                "SELECT event FROM events WHERE job_id=? ORDER BY id", (job["id"],)
            )
        ]
        assert job["state"] == "delivered"
        assert events.count("resource_acquired") == 2
        assert events.count("resource_released") == 2
        first_release = events.index("resource_released")
        assert first_release < events.index("completed") < events.index("resource_acquired", first_release + 1)
        assert store.resource_locks() == []
        store.close()


def test_exclusive_run_is_exposed_in_cli_help():
    assert "exclusive-run" in h.build_parser().format_help()


def test_phase1_detached_end_to_end_with_fake_hermes():
    with tempfile.TemporaryDirectory() as td:
        td_path = pathlib.Path(td)
        fake_hermes = td_path / "fake_hermes.py"
        delivery = td_path / "delivery.json"
        fake_hermes.write_text(
            "import json, pathlib, sys\n"
            f"pathlib.Path({str(delivery)!r}).write_text(json.dumps(sys.argv), encoding='utf-8')\n"
            "print('origin continued')\n",
            encoding="utf-8",
        )
        config = td_path / "config.json"
        db_path = td_path / "state.db"
        artifact_dir = td_path / "artifacts"
        config.write_text(json.dumps({
            "state_db": str(db_path),
            "artifact_dir": str(artifact_dir),
            "handoff_grace_seconds": 0.05,
            "supervisor": {"show_console": False},
            "hermes": {
                "adapter": "cli",
                "cli_argv": [sys.executable, str(fake_hermes)],
                "default_source": "tool",
            },
        }), encoding="utf-8")

        cp = subprocess.run(
            [sys.executable, str(MOD), "--config", str(config), "run-command", "--detached",
             "--resume-origin", "--origin", "origin-e2e", "--timeout", "5", "--",
             sys.executable, "-c", "print('detached-ok')"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10,
        )
        assert cp.returncode == 0, cp.stderr
        accepted = json.loads(cp.stdout)
        assert accepted["status"] == "HANDOFF_ACCEPTED"

        deadline = time.monotonic() + 10
        state = None
        row = None
        while time.monotonic() < deadline:
            if db_path.exists():
                db = sqlite3.connect(db_path)
                try:
                    row = db.execute("SELECT state, result_json, error FROM jobs WHERE id=?", (accepted["job_id"],)).fetchone()
                finally:
                    db.close()
                if row and row[0] in {"delivered", "failed"}:
                    state = row[0]
                    break
            time.sleep(0.05)
        assert state == "delivered", row
        result = json.loads(row[1])
        assert pathlib.Path(result["stdout_path"]).read_text().strip() == "detached-ok"
        assert delivery.exists()
        delivered_argv = json.loads(delivery.read_text(encoding="utf-8"))
        assert delivered_argv[1:4] == ["chat", "--resume", "origin-e2e"]
        # The detached child marks delivery immediately before its finally block
        # closes SQLite. Wait for that Windows file handle to be released.
        close_deadline = time.monotonic() + 5
        probe_path = td_path / "state-unlocked.db"
        while time.monotonic() < close_deadline:
            try:
                db_path.rename(probe_path)
                probe_path.rename(db_path)
                break
            except PermissionError:
                time.sleep(0.05)
        else:
            raise AssertionError("detached supervisor did not release its SQLite file")
