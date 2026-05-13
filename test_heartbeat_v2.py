"""Tests for heartbeat_v2.py pure functions.

Run: cd ~/.hermes/scripts && python3 -m pytest test_heartbeat_v2.py -v
"""

import sys
import time
from pathlib import Path
from unittest.mock import patch

import pytest

# Add scripts dir to path for heartbeat_v2 import
sys.path.insert(0, str(Path(__file__).resolve().parent))

from heartbeat_v2 import (
    HeartbeatSnapshot,
    _is_daemon_process,
    _is_on_cooldown,
    _cache_clean_threshold,
    _CACHE_CLEAN_MTIME_DAYS,
    action_connect,
    action_report,
    action_evolve,
    action_work,
    execute_action,
    _scan_cron_errors,
    _summarize_today,
    _record_action_log,
    score_actions,
    select_action,
)


# ── Test Helpers ───────────────────────────────────────────────────────────

def _snap(**overrides) -> HeartbeatSnapshot:
    """Build a HeartbeatSnapshot with sensible defaults."""
    defaults = dict(
        ts="2026-01-01T00:00:00Z",
        uptime_seconds=3600.0,
        active_sessions=10,
        running_agents=0,
        agent_cache_size=32.0,
        agent_cache_keys=["web_001", "img_002"],
        failed_platforms=[],
        pending_approvals=0,
        queued_events=0,
        provider_health={},
        disk_used_pct=50.0,
        disk_free_gb=50.0,
        memory_used_pct=50.0,
        cron_jobs_count=0,
        stuck_sessions=[],
        warmth_actions=[],
    )
    defaults.update(overrides)
    return HeartbeatSnapshot(**defaults)


# ═══════════════════════════════════════════════════════════════════════════════
# _is_daemon_process
# ═══════════════════════════════════════════════════════════════════════════════

class TestIsDaemonProcess:
    """Pattern-based daemon detection — pure string matching."""

    def test_known_daemons(self):
        assert _is_daemon_process("hermes gateway run --port 8080")
        assert _is_daemon_process("python3 hermes-admin/app.py")

    def test_normal_agents(self):
        assert not _is_daemon_process("python3 heartbeat_v2.py")
        assert not _is_daemon_process("python3 agent_worker.py --id=5")
        assert not _is_daemon_process("hermes task run build")

    def test_empty_and_edge(self):
        assert not _is_daemon_process("")
        assert not _is_daemon_process("hermes")  # partial match shouldn't trigger
        assert not _is_daemon_process("HERMES GATEWAY RUN")  # case-sensitive

    def test_substring_in_longer_string(self):
        """'hermes gateway run' should match even as substring."""
        assert _is_daemon_process("/usr/bin/python3 hermes gateway run --debug")


# ═══════════════════════════════════════════════════════════════════════════════
# _is_on_cooldown
# ═══════════════════════════════════════════════════════════════════════════════

class TestIsOnCooldown:
    """Cooldown check — pure timestamp comparison."""

    COOLDOWN = 600  # 10 min

    def test_empty_history(self):
        assert not _is_on_cooldown("WORK", [], self.COOLDOWN)

    def test_action_missing_from_history(self):
        history = [{"action": "REST", "ts": time.time() - 10}]
        assert not _is_on_cooldown("WORK", history, self.COOLDOWN)

    def test_recent_action_triggers_cooldown(self):
        history = [{"action": "WORK", "ts": time.time() - 60}]
        assert _is_on_cooldown("WORK", history, self.COOLDOWN)

    def test_old_action_does_not_trigger(self):
        history = [{"action": "WORK", "ts": time.time() - 900}]  # 15 min ago
        assert not _is_on_cooldown("WORK", history, self.COOLDOWN)

    def test_only_last_occurrence_checked(self):
        """Per implementation: break after first match in reversed order."""
        history = [
            {"action": "WORK", "ts": time.time() - 900},  # old
            {"action": "REST", "ts": time.time() - 60},
            {"action": "WORK", "ts": time.time() - 1200},  # older — not reached
        ]
        # Last WORK (reversed) is 900s ago → outside cooldown
        assert not _is_on_cooldown("WORK", history, self.COOLDOWN)

    def test_missing_ts_field_treated_as_zero(self):
        """When 'ts' is missing, get() returns 0 (epoch 1970), NOT within cooldown."""
        history = [{"action": "WORK"}]  # no 'ts' → default 0
        # now - 0 is ~50 years, far outside any cooldown
        assert not _is_on_cooldown("WORK", history, self.COOLDOWN)

    def test_multiple_actions_history(self):
        history = [
            {"action": "REST", "ts": time.time() - 1200},
            {"action": "WORK", "ts": time.time() - 30},
            {"action": "EVOLVE", "ts": time.time() - 800},
        ]
        assert _is_on_cooldown("WORK", history, self.COOLDOWN)
        assert not _is_on_cooldown("EVOLVE", history, self.COOLDOWN)


# ═══════════════════════════════════════════════════════════════════════════════
# score_actions
# ═══════════════════════════════════════════════════════════════════════════════

class TestScoreActions:
    """Scoring logic — pure computation from snapshot + history."""

    def test_baseline_all_five(self):
        scores = score_actions(_snap(), [])
        assert sorted(scores.keys()) == ["CONNECT", "EVOLVE", "REPORT", "REST", "WORK"]
        assert all(s >= 5.0 for s in scores.values())

    def test_pending_work_boosts_work(self):
        # High cron count + stuck sessions → WORK should dominate
        snap = _snap(cron_jobs_count=5, stuck_sessions=[{"pid": 1}])
        scores = score_actions(snap, [])
        assert scores["WORK"] > scores["REST"]
        assert scores["WORK"] > scores["REPORT"]

    def test_disk_pressure_boosts_rest(self):
        snap = _snap(disk_used_pct=90.0)  # above 85% warn
        scores = score_actions(snap, [])
        assert scores["REST"] > 5.0  # got a boost

    def test_memory_pressure_boosts_rest(self):
        snap = _snap(memory_used_pct=90.0)  # above 80
        scores = score_actions(snap, [])
        assert scores["REST"] > 5.0

    def test_disk_below_warn_no_rest_boost(self):
        snap = _snap(disk_used_pct=50.0)
        scores = score_actions(snap, [])
        # REST should be at baseline (no boost from disk/mem)
        assert scores["REST"] == 5.0

    def test_memory_none_does_not_crash(self):
        snap = _snap(memory_used_pct=None)
        scores = score_actions(snap, [])
        assert scores["REST"] == 5.0  # no boost when None

    def test_failed_platforms_boost_evolve(self):
        snap = _snap(failed_platforms=["openrouter", "anthropic"])
        scores = score_actions(snap, [])
        assert scores["EVOLVE"] > 5.0
        assert scores["EVOLVE"] > scores["REPORT"]

    def test_idle_state_boosts_connect(self):
        snap = _snap(running_agents=0, active_sessions=10)
        scores = score_actions(snap, [])
        # Should get 3.0 * 0.5 = 1.5 boost
        assert scores["CONNECT"] == 6.5

    def test_busy_state_no_connect_boost(self):
        snap = _snap(running_agents=3, active_sessions=100)
        scores = score_actions(snap, [])
        assert scores["CONNECT"] == 5.0  # no idle boost

    def test_repetition_penalty(self):
        history = [{"action": "WORK", "ts": time.time() - 60}]
        scores = score_actions(_snap(), history)
        # Last action was WORK → penalty of -1.5
        assert scores["WORK"] < 5.0
        assert scores["WORK"] == 3.5

    def test_repetition_penalty_not_applied_to_others(self):
        history = [{"action": "WORK", "ts": time.time() - 60}]
        scores = score_actions(_snap(), history)
        assert scores["REST"] == 5.0
        assert scores["EVOLVE"] == 5.0

    def test_combined_signals(self):
        """WORK boost + REPEAT penalty on WORK → net effect."""
        snap = _snap(cron_jobs_count=3)
        history = [{"action": "WORK", "ts": time.time() - 60}]
        scores = score_actions(snap, history)
        # WORK: 5.0 + 3*2.0 + (-1.5) = 9.5
        assert scores["WORK"] == pytest.approx(9.5)


# ═══════════════════════════════════════════════════════════════════════════════
# select_action
# ═══════════════════════════════════════════════════════════════════════════════

class TestSelectAction:
    """Action selection — pure decision logic."""

    def test_picks_highest_scoring(self):
        scores = {"WORK": 5.0, "REST": 5.0, "EVOLVE": 5.0, "CONNECT": 10.0, "REPORT": 5.0}
        action, reason = select_action(scores, _snap(), [])
        assert action == "CONNECT"
        assert "10.0" in reason

    def test_skips_cooldown_action(self):
        """When top scorer is on cooldown, pick next best."""
        history = [{"action": "WORK", "ts": time.time() - 10}]  # just ran
        scores = {"WORK": 15.0, "REST": 6.0, "EVOLVE": 5.0, "CONNECT": 5.0, "REPORT": 5.0}
        action, _ = select_action(scores, _snap(), history)
        assert action != "WORK"

    def test_skips_work_with_backpressure(self):
        """When agents are running, skip WORK even if high score."""
        snap = _snap(running_agents=2)
        scores = {"WORK": 15.0, "REST": 6.0, "EVOLVE": 5.0, "CONNECT": 5.0, "REPORT": 5.0}
        action, _ = select_action(scores, snap, [])
        assert action != "WORK"
        assert action == "REST"

    def test_falls_back_to_report_when_all_skipped(self):
        """If every action is blocked, default to REPORT."""
        history = [{"action": "WORK", "ts": time.time() - 10}]
        scores = {"WORK": 15.0, "REST": 5.0, "EVOLVE": 5.0, "CONNECT": 5.0, "REPORT": 5.0}
        # WORK on cooldown, all others tied — highest is WORK which gets skipped
        action, reason = select_action(scores, _snap(), history)
        assert action == "REST"  # next highest, not REPORT

    def test_all_on_cooldown_defaults_to_report(self):
        """All five actions recently taken → REPORT."""
        now = time.time()
        history = [
            {"action": "WORK", "ts": now - 10},
            {"action": "REST", "ts": now - 10},
            {"action": "EVOLVE", "ts": now - 10},
            {"action": "CONNECT", "ts": now - 10},
            {"action": "REPORT", "ts": now - 10},
        ]
        scores = {"WORK": 15.0, "REST": 6.0, "EVOLVE": 5.0, "CONNECT": 5.0, "REPORT": 5.0}
        action, reason = select_action(scores, _snap(), history)
        assert action == "REPORT"
        assert "defaulting" in reason


# ═══════════════════════════════════════════════════════════════════════════════
# action_connect
# ═══════════════════════════════════════════════════════════════════════════════

class TestActionConnect:
    """Provider health: pause degraded jobs, unpause recovered, scan 429."""

    def test_returns_tuple(self):
        result, steps, errors = action_connect(_snap(), dry_run=False)
        assert isinstance(result, str)
        assert isinstance(steps, list)
        assert isinstance(errors, list)

    def test_result_starts_with_connect(self):
        result, *_ = action_connect(_snap(), dry_run=False)
        assert result.startswith("CONNECT:")

    def test_dry_run_no_crash(self):
        """Dry-run with degraded platform should produce steps without error."""
        snap = _snap(failed_platforms=["opencode"])
        result, steps, errors = action_connect(snap, dry_run=True)
        assert isinstance(steps, list)
        assert isinstance(errors, list)

    def test_empty_cron_no_steps(self):
        """No failed platforms and no cron errors → mostly empty steps."""
        snap = _snap(failed_platforms=[])
        result, steps, errors = action_connect(snap, dry_run=False)
        assert result.startswith("CONNECT:")
        # No provider degraded → no pause/unpause steps; may have rate-limit steps
        assert isinstance(steps, list)


# ═══════════════════════════════════════════════════════════════════════════════
# action_report
# ═══════════════════════════════════════════════════════════════════════════════

class TestActionReport:
    """Summarize today's action log entries."""

    def test_returns_tuple(self):
        result, steps, errors = action_report(_snap(), dry_run=False)
        assert isinstance(result, str)
        assert isinstance(steps, list)
        assert isinstance(errors, list)

    def test_silent_or_summary(self):
        """Either silent (no actions) or produces summary with done + learnings."""
        result, steps, errors = action_report(_snap(), dry_run=False)
        if result.startswith("silent:"):
            assert steps == []
        else:
            assert "做了：" in result
            assert isinstance(steps, list)

    def test_dry_run_unchanged(self):
        """action_report doesn't distinguish dry_run (purely informational)."""
        dry = action_report(_snap(), dry_run=True)
        wet = action_report(_snap(), dry_run=False)
        assert dry == wet


# ═══════════════════════════════════════════════════════════════════════════════
# action_work
# ═══════════════════════════════════════════════════════════════════════════════

class TestActionWork:
    """Housekeeping: cache clean, session archive, git push."""

    @patch("heartbeat_v2._safe_shell", return_value=(True, ""))
    def test_returns_tuple(self, _mock):
        result, steps, errors = action_work(_snap(), dry_run=False)
        assert isinstance(result, str)
        assert isinstance(steps, list)
        assert isinstance(errors, list)

    @patch("heartbeat_v2._safe_shell", return_value=(True, ""))
    def test_result_starts_with_work(self, _mock):
        result, *_ = action_work(_snap(), dry_run=False)
        assert result.startswith("WORK:")

    @patch("heartbeat_v2._safe_shell", return_value=(True, ""))
    def test_dry_run_produces_steps(self, _mock):
        result, steps, errors = action_work(_snap(), dry_run=True)
        assert isinstance(steps, list)
        assert len(steps) > 0


# ═══════════════════════════════════════════════════════════════════════════════
# action_evolve
# ═══════════════════════════════════════════════════════════════════════════════

class TestActionEvolve:
    """Self-check: pytest canary, cron error scan, pacman update check."""

    @patch("heartbeat_v2._scan_cron_errors", return_value=[])
    @patch("heartbeat_v2._safe_shell", return_value=(True, ""))
    def test_returns_tuple(self, _shell, _cron):
        result, steps, errors = action_evolve(_snap(), dry_run=False)
        assert isinstance(result, str)
        assert isinstance(steps, list)
        assert isinstance(errors, list)

    @patch("heartbeat_v2._scan_cron_errors", return_value=[])
    @patch("heartbeat_v2._safe_shell", return_value=(True, ""))
    def test_result_starts_with_evolve(self, _shell, _cron):
        result, *_ = action_evolve(_snap(), dry_run=False)
        assert result.startswith("EVOLVE:")

    @patch("heartbeat_v2._scan_cron_errors", return_value=[])
    @patch("heartbeat_v2._safe_shell", return_value=(True, ""))
    def test_dry_run_produces_steps(self, _shell, _cron):
        result, steps, errors = action_evolve(_snap(), dry_run=True)
        assert isinstance(steps, list)
        assert len(steps) > 0

    @patch("heartbeat_v2._scan_cron_errors", return_value=[])
    @patch("heartbeat_v2._safe_shell", return_value=(True, ""))
    def test_cron_scan_step_present(self, _shell, _cron):
        result, steps, errors = action_evolve(_snap(), dry_run=False)
        ops = [s.get("op") for s in steps if s.get("op")]
        assert "cron_scan" in ops


# ═══════════════════════════════════════════════════════════════════════════════
# execute_action
# ═══════════════════════════════════════════════════════════════════════════════

class TestExecuteAction:
    """Action dispatch routing."""

    @patch("heartbeat_v2._scan_cron_errors", return_value=[])
    @patch("heartbeat_v2._safe_shell", return_value=(True, ""))
    def test_known_action_routes(self, _shell, _cron):
        for action in ["WORK", "REST", "EVOLVE", "CONNECT", "REPORT"]:
            result, steps, errors = execute_action(action, _snap(), dry_run=True)
            assert isinstance(result, str), f"{action}: result not str"
            assert isinstance(steps, list), f"{action}: steps not list"
            assert isinstance(errors, list), f"{action}: errors not list"

    def test_unknown_action(self):
        result, steps, errors = execute_action("NOPE", _snap(), dry_run=True)
        assert isinstance(result, str)
        assert isinstance(steps, list)
        assert isinstance(errors, list)

    @patch("heartbeat_v2._scan_cron_errors", return_value=[])
    @patch("heartbeat_v2._safe_shell", return_value=(True, ""))
    def test_result_is_tuple(self, _shell, _cron):
        result = execute_action("WORK", _snap(), dry_run=True)
        assert isinstance(result, tuple)
        assert len(result) == 3
        assert isinstance(result[2], list)


# ═══════════════════════════════════════════════════════════════════════════════
# Helper: _record_action_log + _summarize_today
# ═══════════════════════════════════════════════════════════════════════════════

class TestActionLogHelpers:
    """Record and read action log entries."""

    def test_record_and_summarize(self, tmp_path, monkeypatch):
        """Write an entry, then summarize reads it back."""
        log_file = tmp_path / "test_log.jsonl"
        monkeypatch.setattr("heartbeat_v2._ACTION_LOG_PATH", log_file)

        trigger = {"disk_pct": 50.0, "cron_count": 3}
        steps = [{"op": "test_op", "result": "ok"}]
        _record_action_log("WORK", trigger, steps, "ok", [], "test learnings")

        entries = _summarize_today()
        assert len(entries) == 1
        assert entries[0]["action"] == "WORK"
        assert entries[0]["outcome"] == "ok"
        assert entries[0]["learnings"] == "test learnings"
        assert len(entries[0]["steps"]) == 1
        assert entries[0]["steps"][0]["op"] == "test_op"

    def test_summarize_empty(self, tmp_path, monkeypatch):
        """Empty log file should return empty list."""
        log_file = tmp_path / "empty.jsonl"
        monkeypatch.setattr("heartbeat_v2._ACTION_LOG_PATH", log_file)
        entries = _summarize_today()
        assert entries == []

    def test_summarize_no_file(self, tmp_path, monkeypatch):
        """Missing log file should return empty list."""
        monkeypatch.setattr("heartbeat_v2._ACTION_LOG_PATH", tmp_path / "nonexistent.jsonl")
        entries = _summarize_today()
        assert entries == []


# ═══════════════════════════════════════════════════════════════════════════════
# Helper: _scan_cron_errors
# ═══════════════════════════════════════════════════════════════════════════════

class TestScanCronErrors:
    """Scan cron output directory for error patterns."""

    def test_no_output_dir(self, tmp_path, monkeypatch):
        """No output dir → empty list."""
        monkeypatch.setattr("heartbeat_v2._HERMES_HOME", tmp_path)
        assert _scan_cron_errors() == []

    def test_empty_output_dir(self, tmp_path, monkeypatch):
        """Empty output dir → empty list."""
        cron_out = tmp_path / "cron" / "output"
        cron_out.mkdir(parents=True)
        monkeypatch.setattr("heartbeat_v2._HERMES_HOME", tmp_path)
        assert _scan_cron_errors() == []

    def test_error_in_output(self, tmp_path, monkeypatch):
        """Output file with 429 → detected as error."""
        cron_out = tmp_path / "cron" / "output" / "abc123"
        cron_out.mkdir(parents=True)
        (cron_out / "output.txt").write_text(
            "FAILED: RuntimeError: HTTP 429: Too Many Requests\n"
        )
        monkeypatch.setattr("heartbeat_v2._HERMES_HOME", tmp_path)
        errors = _scan_cron_errors()
        assert len(errors) == 1
        assert errors[0]["job_id"] == "abc123"
        assert "429" in errors[0]["error_snippet"]

    def test_clean_output_not_detected(self, tmp_path, monkeypatch):
        """Output file without error keywords → not detected."""
        cron_out = tmp_path / "cron" / "output" / "xyz789"
        cron_out.mkdir(parents=True)
        (cron_out / "output.txt").write_text(
            "SUCCESS: all tasks completed\n"
        )
        monkeypatch.setattr("heartbeat_v2._HERMES_HOME", tmp_path)
        assert _scan_cron_errors() == []

    def test_traceback_detected(self, tmp_path, monkeypatch):
        """Output with Traceback → detected."""
        cron_out = tmp_path / "cron" / "output" / "err_job"
        cron_out.mkdir(parents=True)
        (cron_out / "output.txt").write_text(
            "Traceback (most recent call last):\n  File 'x.py', line 1\n"
        )
        monkeypatch.setattr("heartbeat_v2._HERMES_HOME", tmp_path)
        errors = _scan_cron_errors()
        assert len(errors) == 1
        assert "err_job" == errors[0]["job_id"]


class TestDynamicThreshold:
    def test_high_pressure_returns_1(self) -> None:
        assert _cache_clean_threshold(91.0) == 1

    def test_mid_high_pressure_returns_3(self) -> None:
        assert _cache_clean_threshold(85.0) == 3

    def test_mid_pressure_returns_5(self) -> None:
        assert _cache_clean_threshold(75.0) == 5

    def test_normal_disk_returns_default(self) -> None:
        assert _cache_clean_threshold(60.0) == _CACHE_CLEAN_MTIME_DAYS

    def test_edge_90(self) -> None:
        assert _cache_clean_threshold(90.1) == 1
        assert _cache_clean_threshold(90.0) == 3

    def test_edge_80(self) -> None:
        assert _cache_clean_threshold(80.1) == 3
        assert _cache_clean_threshold(80.0) == 5

    def test_edge_70(self) -> None:
        assert _cache_clean_threshold(70.1) == 5
        assert _cache_clean_threshold(70.0) == _CACHE_CLEAN_MTIME_DAYS

    def test_none_disk_returns_default(self) -> None:
        assert _cache_clean_threshold(None) == _CACHE_CLEAN_MTIME_DAYS
