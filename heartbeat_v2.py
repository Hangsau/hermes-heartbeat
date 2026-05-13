#!/usr/bin/env python3
"""Heartbeat v2 — autonomic + cognitive layers for Hermes agent health.

Run standalone or invoked by the internal-heartbeat cron.
  --dry-run    Build snapshot and decisions, do not execute actions.
  --action=X   Execute only action X (WORK|REST|EVOLVE|CONNECT|REPORT).
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import os
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

# ── Paths ──────────────────────────────────────────────────────────
_HERMES_HOME = Path.home() / ".hermes"
_STATE_PATH = _HERMES_HOME / "heartbeat_state.json"
_DECISIONS_PATH = _HERMES_HOME / "heartbeat_decisions.jsonl"
_CRON_JOBS_PATH = _HERMES_HOME / "cron" / "jobs.json"
_GATEWAY_PID_PATH = _HERMES_HOME / "gateway.pid"
_HEALTH_LOG_DIR = _HERMES_HOME / "health_logs"

# ── Timing ─────────────────────────────────────────────────────────
_AUTONOMIC_INTERVAL_SEC = 30
_COGNITIVE_INTERVAL_SEC = 300
_STUCK_THRESHOLD_MIN = 30
_STUCK_RECOVERY_MIN = 45
_ACTION_TIMEOUT_SEC = 300
_DECISION_COOLDOWN_SEC = 600
_WARMTH_IDLE_HOURS = 24

# ── Thresholds ─────────────────────────────────────────────────────
_CACHE_BLOAT_THRESHOLD = 128
_DISK_WARN_PCT = 85
_DISK_CRIT_PCT = 95
_QUEUED_EVENTS_WARN = 10

# ── Scoring weights ────────────────────────────────────────────────
_WEIGHT_PENDING_WORK = 2.0
_WEIGHT_CACHE_BLOAT = 1.5
_WEIGHT_FAILED_PLATFORMS = 2.0
_WEIGHT_IDLE_TIME = 0.5
_WEIGHT_REPETITION_PENALTY = -1.5


# ── Safe helpers ───────────────────────────────────────────────────
def _safe_json_read(path: Path, default: Any = None) -> Any:
    """Read JSON file; return default on any error."""
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _safe_json_write(path: Path, data: Any) -> bool:
    """Atomic JSON write (tmp + rename). Returns success."""
    try:
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        tmp.replace(path)
        return True
    except Exception:
        return False


def _safe_shell(cmd: list[str], timeout: int = 10, workdir: str | None = None) -> tuple[bool, str]:
    """Run shell command safely; returns (ok, stdout_or_err)."""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, check=False, cwd=workdir
        )
        return result.returncode == 0, (result.stdout.strip() or result.stderr.strip())
    except Exception as exc:
        return False, str(exc)


def _safe_file_mtime(path: Path) -> float | None:
    """Return file mtime epoch, or None."""
    try:
        return path.stat().st_mtime
    except Exception:
        return None


# ── Snapshot ───────────────────────────────────────────────────────
@dataclasses.dataclass(slots=True)
class HeartbeatSnapshot:
    ts: str
    uptime_seconds: float
    active_sessions: int
    running_agents: int
    agent_cache_size: int
    agent_cache_keys: list[str]
    failed_platforms: list[str]
    pending_approvals: int
    queued_events: int
    provider_health: dict[str, Any]
    disk_used_pct: float
    disk_free_gb: float
    memory_used_pct: float | None
    cron_jobs_count: int
    stuck_sessions: list[dict[str, Any]]
    warmth_actions: list[dict[str, Any]]


def _disk_usage() -> tuple[float, float]:
    """Return (used_pct, free_gb) for Hermes home partition."""
    try:
        usage = shutil.disk_usage(_HERMES_HOME)
        used_pct = 100.0 - (usage.free / usage.total * 100.0)
        free_gb = usage.free / (1024 ** 3)
        return used_pct, free_gb
    except Exception:
        return 0.0, 0.0


def _memory_usage() -> float | None:
    """Return memory used percent, or None if unavailable."""
    ok, out = _safe_shell(["free", "-m"])
    if not ok:
        return None
    try:
        lines = out.splitlines()
        for line in lines:
            if line.startswith("Mem:"):
                parts = line.split()
                total = int(parts[1])
                used = int(parts[2])
                return (used / total) * 100.0
    except Exception:
        pass
    return None


def _cron_jobs_count() -> int:
    jobs = _safe_json_read(_CRON_JOBS_PATH, default={})
    return len(jobs.get("jobs", []))


def _list_hermes_processes() -> list[dict[str, Any]]:
    """Find python processes that look like Hermes agents."""
    ok, out = _safe_shell(
        ["pgrep", "-a", "-f", "python.*hermes"], timeout=5
    )
    if not ok:
        return []
    procs = []
    for line in out.splitlines():
        parts = line.split(None, 1)
        if len(parts) >= 2:
            try:
                procs.append({"pid": int(parts[0]), "cmd": parts[1]})
            except ValueError:
                continue
    return procs


def _system_uptime() -> float:
    """Return system uptime in seconds."""
    try:
        with open("/proc/uptime", "r") as f:
            return float(f.read().split()[0])
    except Exception:
        return 0.0


def _count_active_sessions() -> int:
    """Count session log files as proxy for historical sessions."""
    try:
        sess_dir = _HERMES_HOME / "sessions"
        return len(list(sess_dir.glob("*.jsonl")))
    except Exception:
        return 0


_STUCK_DAEMON_WHITELIST = ["hermes gateway run", "hermes-admin/app.py"]


def _is_daemon_process(cmd: str) -> bool:
    """Return True if this is a known long-running daemon, not a stuck user agent."""
    for pattern in _STUCK_DAEMON_WHITELIST:
        if pattern in cmd:
            return True
    return False


def _detect_stuck_sessions(threshold_min: int) -> list[dict[str, Any]]:
    """Find python hermes processes running longer than threshold, excluding daemons."""
    procs = _list_hermes_processes()
    if not procs:
        return []
    ok, out = _safe_shell(
        ["ps", "-o", "pid=,etime=,cmd=", "-p"]
        + [str(p["pid"]) for p in procs],
        timeout=5,
    )
    if not ok:
        return []
    stuck = []
    for line in out.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 3:
            continue
        pid_str, etime, cmd = parts
        if _is_daemon_process(cmd):
            continue
        minutes = 0
        try:
            if "-" in etime:
                days, rest = etime.split("-", 1)
                minutes += int(days) * 1440
                etime = rest
            chunks = etime.split(":")
            if len(chunks) == 3:
                minutes += int(chunks[0]) * 60 + int(chunks[1])
            elif len(chunks) == 2:
                minutes += int(chunks[0])
            elif len(chunks) == 1:
                minutes += int(chunks[0]) // 60
        except ValueError:
            continue
        if minutes >= threshold_min:
            stuck.append({"pid": int(pid_str), "etime": etime, "cmd": cmd[:80], "minutes": minutes})
    return stuck


def _scan_cold_sessions(hours: int = _WARMTH_IDLE_HOURS) -> list[dict[str, Any]]:
    """Find session files idle longer than threshold hours."""
    session_dir = _HERMES_HOME / "sessions"
    if not session_dir.exists():
        return []
    cutoff = time.time() - hours * 3600
    cold = []
    for path in session_dir.glob("*.jsonl"):
        try:
            mtime = path.stat().st_mtime
            if mtime < cutoff:
                cold.append({
                    "file": path.name,
                    "idle_hours": round((time.time() - mtime) / 3600, 1),
                })
        except Exception:
            continue
    # Sort by idle time descending, cap at 10
    cold.sort(key=lambda x: x["idle_hours"], reverse=True)
    return cold[:10]


def _kanban_ready_tasks(limit: int = 5) -> list[dict[str, Any]]:
    """Query kanban.db for tasks with status='ready'."""
    db_path = _HERMES_HOME / "kanban.db"
    if not db_path.exists():
        return []
    try:
        import sqlite3
        with sqlite3.connect(str(db_path), timeout=5) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT id, title, assignee, priority, created_at FROM tasks WHERE status = 'ready' ORDER BY priority DESC, created_at ASC LIMIT ?",
                (limit,),
            )
            rows = cur.fetchall()
            return [dict(row) for row in rows]
    except Exception:
        return []


def _cache_size_mb() -> float:
    """Approximate cache size on disk in MB."""
    cache_dir = _HERMES_HOME / "cache"
    if not cache_dir.exists():
        return 0.0
    total = 0
    for p in cache_dir.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except Exception:
            continue
    return round(total / (1024 * 1024), 2)


def _provider_health_from_logs() -> dict[str, Any]:
    """Lightweight provider health: inspect recent errors.log for provider keywords."""
    err_log = _HERMES_HOME / "logs" / "errors.log"
    if not err_log.exists():
        return {}
    try:
        # Read last 50KB
        size = err_log.stat().st_size
        with open(err_log, "rb") as f:
            f.seek(max(0, size - 50 * 1024))
            tail = f.read().decode("utf-8", errors="ignore")
    except Exception:
        return {}
    # Count provider-related errors
    counts = {}
    for kw in ["anthropic", "openrouter", "openai", "gemini", "ollama", "opencode"]:
        c = tail.lower().count(kw)
        if c:
            counts[kw] = {"errors_in_tail": c, "status": "degraded" if c > 5 else "ok"}
    return counts


def build_heartbeat_snapshot() -> HeartbeatSnapshot:
    """Gather current system state."""
    disk_used, disk_free = _disk_usage()
    mem_used = _memory_usage()
    cron_count = _cron_jobs_count()
    procs = _list_hermes_processes()
    stuck = _detect_stuck_sessions(_STUCK_THRESHOLD_MIN)
    provider_health = _provider_health_from_logs()

    cache_dir = _HERMES_HOME / "cache"
    cache_keys = []
    if cache_dir.exists():
        try:
            cache_keys = [p.name for p in cache_dir.iterdir() if p.is_dir()][:10]
        except Exception:
            pass

    return HeartbeatSnapshot(
        ts=datetime.now(timezone.utc).isoformat(),
        uptime_seconds=_system_uptime(),
        active_sessions=_count_active_sessions(),
        running_agents=len(procs),
        agent_cache_size=_cache_size_mb(),
        agent_cache_keys=cache_keys,
        failed_platforms=[k for k, v in provider_health.items() if v.get("status") != "ok"],
        pending_approvals=0,
        queued_events=0,
        provider_health=provider_health,
        disk_used_pct=disk_used,
        disk_free_gb=disk_free,
        memory_used_pct=mem_used,
        cron_jobs_count=cron_count,
        stuck_sessions=stuck,
        warmth_actions=_scan_cold_sessions(),
    )



# ── Cognitive Layer ─────────────────────────────────────────────────────
def _read_decision_history(limit: int = 20) -> list[dict[str, Any]]:
    """Read last N decisions from jsonl."""
    if not _DECISIONS_PATH.exists():
        return []
    try:
        with open(_DECISIONS_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
        records = []
        for line in lines[-limit:]:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return records
    except Exception:
        return []


def _is_on_cooldown(action: str, history: list[dict[str, Any]], cooldown_sec: int) -> bool:
    """Check if same action was taken within cooldown window."""
    now = time.time()
    for rec in reversed(history):
        if rec.get("action") == action:
            ts = rec.get("ts", 0)
            if now - ts < cooldown_sec:
                return True
            break
    return False


def score_actions(snap: HeartbeatSnapshot, history: list[dict[str, Any]]) -> dict[str, float]:
    """Score five possible actions based on current state."""
    scores: dict[str, float] = {
        "WORK": 5.0,
        "REST": 5.0,
        "EVOLVE": 5.0,
        "CONNECT": 5.0,
        "REPORT": 5.0,
    }

    # Pending work: high cron count, stuck sessions
    pending = snap.cron_jobs_count + len(snap.stuck_sessions)
    scores["WORK"] += pending * _WEIGHT_PENDING_WORK

    # Cache bloat: disk / memory pressure
    bloat = 0.0
    if snap.disk_used_pct > _DISK_WARN_PCT:
        bloat += (snap.disk_used_pct - _DISK_WARN_PCT) / 10.0
    if snap.memory_used_pct and snap.memory_used_pct > 80.0:
        bloat += (snap.memory_used_pct - 80.0) / 10.0
    scores["REST"] += bloat * _WEIGHT_CACHE_BLOAT

    # Failed platforms
    failed = len(snap.failed_platforms)
    scores["EVOLVE"] += failed * _WEIGHT_FAILED_PLATFORMS

    # Idle time: low running agents + low sessions = more CONNECT
    if snap.running_agents == 0 and snap.active_sessions < 50:
        scores["CONNECT"] += 3.0 * _WEIGHT_IDLE_TIME

    # Repetition penalty
    if history:
        last_action = history[-1].get("action")
        if last_action in scores:
            scores[last_action] += _WEIGHT_REPETITION_PENALTY

    return scores


def select_action(
    scores: dict[str, float], snap: HeartbeatSnapshot, history: list[dict[str, Any]]
) -> tuple[str, str]:
    """Pick highest-scoring action respecting cooldown. Returns (action, reason)."""
    sorted_actions = sorted(scores.items(), key=lambda x: x[1], reverse=True)

    for action, score in sorted_actions:
        skips = []
        if _is_on_cooldown(action, history, _DECISION_COOLDOWN_SEC):
            skips.append("cooldown")
        # Backpressure: skip WORK if busy
        if snap.running_agents > 0 and action == "WORK":
            skips.append("backpressure")
        if skips:
            continue
        reason = f"score={score:.1f}, pending={snap.cron_jobs_count}, stuck={len(snap.stuck_sessions)}, disk={snap.disk_used_pct:.1f}%"
        return action, reason

    return "REPORT", "all viable actions skipped (cooldown/backpressure), defaulting to REPORT"


def record_decision(action: str, reason: str, scores: dict[str, float]) -> None:
    """Append decision to jsonl."""
    rec = {
        "ts": time.time(),
        "action": action,
        "reason": reason,
        "scores": scores,
    }
    try:
        with open(_DECISIONS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


# ── Action log ──────────────────────────────────────────────────────
_ACTION_LOG_PATH = _HERMES_HOME / "heartbeat_action_log.jsonl"
_ACTION_LOG_ARCHIVE_PATH = _HERMES_HOME / "heartbeat_action_log_archive.jsonl"
_PATTERNS_PATH = _HERMES_HOME / "heartbeat_patterns.json"
_LEARNING_SCRIPT = _HERMES_HOME / "scripts" / "heartbeat_learning.py"
_ACTION_LOG_ROTATE_DAYS = 30
_SESSION_ARCHIVE_DIR = _HERMES_HOME / "sessions" / "archive"
_SESSION_ARCHIVE_IDLE_HOURS = 168  # 7 days
_SESSION_ARCHIVE_MIN_IDLE_MINUTES = 60
_CACHE_CLEAN_MTIME_DAYS = 7

def _cache_clean_threshold(disk_pct: float | None) -> int:
    """Return cache mtime threshold in days, based on disk pressure.
    Higher pressure → more aggressive cleanup (lower threshold)."""
    if disk_pct is None:
        return _CACHE_CLEAN_MTIME_DAYS
    if disk_pct > 90:
        return 1
    if disk_pct > 80:
        return 3
    if disk_pct > 70:
        return 5
    return _CACHE_CLEAN_MTIME_DAYS
_GIT_REPOS = [
    Path.home() / "managed-agents-research",
]


def _record_action_log(
    action: str, trigger: dict, steps: list[dict],
    outcome: str, errors: list[str], learnings: str = ""
) -> None:
    """Append structured action record to heartbeat_action_log.jsonl."""
    try:
        rec = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "action": action,
            "trigger": trigger,
            "steps": steps,
            "outcome": outcome,
            "errors": errors,
            "learnings": learnings,
        }
        with open(_ACTION_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _rotate_action_log() -> tuple[int, int]:
    """Archive entries older than _ACTION_LOG_ROTATE_DAYS to the archive file.
    Returns (archived_count, kept_count). Called from main() after recording."""
    if not _ACTION_LOG_PATH.exists():
        return 0, 0
    cutoff_ts = datetime.now(timezone.utc)
    cutoff = (cutoff_ts.replace(hour=0, minute=0, second=0, microsecond=0)
              - timedelta(days=_ACTION_LOG_ROTATE_DAYS)).isoformat()
    kept: list[str] = []
    archived: list[str] = []
    try:
        with open(_ACTION_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line_s = line.strip()
                if not line_s:
                    continue
                try:
                    rec = json.loads(line_s)
                    ts = rec.get("ts", "")
                except json.JSONDecodeError:
                    kept.append(line_s)
                    continue
                if ts and ts < cutoff:
                    archived.append(line_s)
                else:
                    kept.append(line_s)
    except Exception:
        return 0, 0

    if not archived:
        return 0, len(kept)

    # Append to archive
    try:
        with open(_ACTION_LOG_ARCHIVE_PATH, "a", encoding="utf-8") as f:
            for line in archived:
                f.write(line + "\n")
    except Exception:
        return 0, len(kept)

    # Write back kept entries
    try:
        with open(_ACTION_LOG_PATH, "w", encoding="utf-8") as f:
            for line in kept:
                f.write(line + "\n")
    except Exception:
        return 0, len(kept)

    return len(archived), len(kept)


def _scan_cron_errors() -> list[dict[str, Any]]:
    """Scan cron output dirs for recent errors (429, timeout, Traceback)."""
    output_dir = _HERMES_HOME / "cron" / "output"
    if not output_dir.exists():
        return []
    errors: list[dict[str, Any]] = []
    for job_dir in sorted(output_dir.iterdir()):
        if not job_dir.is_dir():
            continue
        try:
            files = sorted(job_dir.iterdir(), key=lambda p: p.stat().st_mtime, reverse=True)
        except Exception:
            continue
        if not files:
            continue
        latest = files[0]
        try:
            text = latest.read_text(errors="ignore")[:4096]
        except Exception:
            continue
        if any(kw in text for kw in ["FAILED", "Error", "429", "timeout", "Traceback"]):
            errors.append({
                "job_id": job_dir.name,
                "error_snippet": text[-300:].strip(),
                "file": str(latest),
            })
    return errors


def _summarize_today() -> list[dict[str, Any]]:
    """Read today's action log entries. Returns list of actions done today."""
    if not _ACTION_LOG_PATH.exists():
        return []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    entries: list[dict[str, Any]] = []
    try:
        with open(_ACTION_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                ts = rec.get("ts", "")
                if ts.startswith(today):
                    entries.append(rec)
    except Exception:
        return []
    return entries


# ── Action functions ─────────────────────────────────────────────────
def action_work(snap: HeartbeatSnapshot, dry_run: bool) -> tuple[str, list[dict], list[str]]:
    """System maintenance: clean caches, archive sessions, git push."""
    steps: list[dict] = []
    errors: list[str] = []

    # 1. Clean ~/.cache/ files older than N days (dynamic threshold based on disk pressure)
    days = _cache_clean_threshold(snap.disk_used_pct)
    ok, out = _safe_shell(
        ["find", str(Path.home() / ".cache"), "-type", "f", "-mtime", f"+{days}", "-delete"],
        timeout=30,
    )
    if dry_run:
        steps.append({"op": "cache_clean", "target": "~/.cache/", "result": "dry-run skipped"})
    elif ok:
        steps.append({"op": "cache_clean", "target": "~/.cache/", "result": "cleaned", "ok": True})
    else:
        errors.append(f"cache_clean failed: {out}")
        steps.append({"op": "cache_clean", "target": "~/.cache/", "result": out, "ok": False})

    # 2. Clean /tmp hermes remnants
    ok, out = _safe_shell(
        ["find", "/tmp", "-name", "hermes_*", "-mmin", "+60", "-delete"],
        timeout=10,
    )
    if dry_run:
        steps.append({"op": "tmp_clean", "target": "/tmp/hermes_*", "result": "dry-run skipped"})
    elif ok:
        steps.append({"op": "tmp_clean", "target": "/tmp/hermes_*", "result": "cleaned", "ok": True})
    else:
        errors.append(f"tmp_clean failed: {out}")
        steps.append({"op": "tmp_clean", "result": out, "ok": False})

    # 3. Archive idle sessions
    _SESSION_ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
    session_dir = _HERMES_HOME / "sessions"
    cutoff = time.time() - _SESSION_ARCHIVE_IDLE_HOURS * 3600
    min_idle = _SESSION_ARCHIVE_MIN_IDLE_MINUTES * 60
    archived = 0
    if session_dir.exists() and not dry_run:
        for p in session_dir.glob("*.jsonl"):
            try:
                mtime = p.stat().st_mtime
                if mtime < cutoff and (time.time() - mtime) > min_idle:
                    p.rename(_SESSION_ARCHIVE_DIR / p.name)
                    archived += 1
            except Exception as exc:
                errors.append(f"archive session {p.name}: {exc}")
        if archived:
            steps.append({"op": "archive_sessions", "count": archived, "result": f"moved {archived} to archive/", "ok": True})
        else:
            steps.append({"op": "archive_sessions", "result": "nothing to archive"})
    elif dry_run:
        steps.append({"op": "archive_sessions", "result": "dry-run skipped"})

    # 4. Git push managed-agents-research
    for repo_path in _GIT_REPOS:
        if not repo_path.exists():
            steps.append({"op": "git_push", "repo": str(repo_path), "result": "repo not found", "ok": False})
            continue
        ok_fetch, out_fetch = _safe_shell(["git", "fetch", "origin"], timeout=30, workdir=str(repo_path))
        if not ok_fetch:
            errors.append(f"git fetch {repo_path}: {out_fetch}")
            steps.append({"op": "git_push", "repo": str(repo_path), "result": f"fetch failed: {out_fetch}", "ok": False})
            continue
        # Check if ahead of origin/master
        ok_diff, out_diff = _safe_shell(
            ["git", "rev-list", "--count", "origin/master..HEAD"], timeout=10, workdir=str(repo_path)
        )
        ahead = 0
        if ok_diff:
            try:
                ahead = int(out_diff.strip())
            except ValueError:
                pass
        if ahead == 0:
            steps.append({"op": "git_push", "repo": str(repo_path), "result": "no unpushed commits", "ok": True})
        elif dry_run:
            steps.append({"op": "git_push", "repo": str(repo_path), "result": f"[DRY] would push {ahead} commits"})
        else:
            ok_push, out_push = _safe_shell(["git", "push", "origin", "master"], timeout=30, workdir=str(repo_path))
            if ok_push:
                steps.append({"op": "git_push", "repo": str(repo_path), "result": f"pushed {ahead} commits", "ok": True})
            else:
                errors.append(f"git push {repo_path}: {out_push}")
                steps.append({"op": "git_push", "repo": str(repo_path), "result": f"push failed: {out_push}", "ok": False})

    result = f"WORK: {len(steps)} steps performed" + (f", {len(errors)} errors" if errors else "")
    return result, steps, errors


def action_connect(snap: HeartbeatSnapshot, dry_run: bool) -> tuple[str, list[dict], list[str]]:
    """Provider health: pause degraded jobs, unpause recovered ones + direct 429 cron errors."""
    steps: list[dict] = []
    errors: list[str] = []
    jobs_data = _safe_json_read(_CRON_JOBS_PATH, default={})
    jobs = jobs_data.get("jobs", [])

    # Provider mapping: provider keyword → model name substrings
    provider_map = {
        "opencode": ["opencode"],
        "openrouter": ["openrouter", "kimi", "k2"],
        "anthropic": ["claude", "anthropic"],
        "openai": ["gpt", "openai"],
        "gemini": ["gemini"],
        "ollama": ["ollama"],
    }

    degraded = set(snap.failed_platforms)

    # Pause jobs whose mapped provider is degraded
    for job in jobs:
        job_name = job.get("name", "?")
        job_model = job.get("model", "") or ""
        if not job.get("enabled", True):
            continue

        matched = None
        for provider, keywords in provider_map.items():
            if provider in degraded and any(kw.lower() in job_model.lower() for kw in keywords):
                matched = provider
                break

        if matched:
            if dry_run:
                steps.append({"op": "pause_job", "job": job_name, "reason": f"provider {matched} degraded", "result": "dry-run skipped"})
            else:
                job["enabled"] = False
                job["paused_at"] = datetime.now(timezone.utc).isoformat()
                job["paused_reason"] = f"heartbeat: provider {matched} degraded"
                if _safe_json_write(_CRON_JOBS_PATH, jobs_data):
                    steps.append({"op": "pause_job", "job": job_name, "reason": f"provider {matched} degraded", "result": "paused", "ok": True})
                else:
                    errors.append(f"pause {job_name}: write failed")
                    steps.append({"op": "pause_job", "job": job_name, "result": "write failed", "ok": False})

    # Unpause jobs whose heartbeat-paused provider is now healthy
    for job in jobs:
        paused_reason = job.get("paused_reason", "")
        if job.get("enabled", True) or not paused_reason.startswith("heartbeat:"):
            continue
        job_name = job.get("name", "?")
        job_model = job.get("model", "") or ""
        for provider, keywords in provider_map.items():
            if provider in paused_reason and provider not in degraded and any(kw.lower() in job_model.lower() for kw in keywords):
                if dry_run:
                    steps.append({"op": "unpause_job", "job": job_name, "result": "dry-run skipped"})
                else:
                    job["enabled"] = True
                    job["paused_at"] = None
                    job["paused_reason"] = None
                    if _safe_json_write(_CRON_JOBS_PATH, jobs_data):
                        steps.append({"op": "unpause_job", "job": job_name, "result": "unpaused", "ok": True})
                    else:
                        errors.append(f"unpause {job_name}: write failed")
                        steps.append({"op": "unpause_job", "job": job_name, "result": "write failed", "ok": False})

    # Direct cron error scan: pause jobs with 429 rate-limit
    cron_errors = _scan_cron_errors()
    rate_limited = [e for e in cron_errors if "429" in e["error_snippet"]]
    for rl in rate_limited:
        rl_job_id = rl["job_id"]
        for job in jobs:
            if job.get("id") == rl_job_id and job.get("enabled", True):
                if dry_run:
                    steps.append({"op": "pause_rate_limited", "job": job.get("name", rl_job_id), "result": "dry-run skipped"})
                else:
                    job["enabled"] = False
                    job["paused_at"] = datetime.now(timezone.utc).isoformat()
                    job["paused_reason"] = "heartbeat: cron error 429 rate-limit"
                    if _safe_json_write(_CRON_JOBS_PATH, jobs_data):
                        steps.append({"op": "pause_rate_limited", "job": job.get("name", rl_job_id), "result": "paused", "ok": True})
                    else:
                        errors.append(f"pause_rate_limited {rl_job_id}: write failed")
                break

    result = f"CONNECT: {len(steps)} steps" + (f", {len(errors)} errors" if errors else "")
    return result, steps, errors


def action_evolve(snap: HeartbeatSnapshot, dry_run: bool) -> tuple[str, list[dict], list[str]]:
    """Self-check: pytest canary, cron error scan, pacman update check."""
    steps: list[dict] = []
    errors: list[str] = []

    # 1. Pytest canary
    test_path = _HERMES_HOME / "scripts" / "test_heartbeat_v2.py"
    if test_path.exists():
        if dry_run:
            steps.append({"op": "pytest_canary", "result": "dry-run skipped"})
        else:
            ok, out = _safe_shell(
                ["python3", "-m", "pytest", str(test_path), "-v", "--tb=short"],
                timeout=60,
                workdir=str(_HERMES_HOME / "scripts"),
            )
            if ok:
                steps.append({"op": "pytest_canary", "result": "PASS", "ok": True})
            else:
                errors.append("pytest canary failed")
                steps.append({"op": "pytest_canary", "result": out[-200:], "ok": False})
    else:
        steps.append({"op": "pytest_canary", "result": "test file not found"})

    # 2. Scan cron errors
    cron_errors = _scan_cron_errors()
    if cron_errors:
        steps.append({"op": "cron_scan", "count": len(cron_errors), "jobs": [e["job_id"][:12] for e in cron_errors], "result": f"{len(cron_errors)} jobs with errors"})
    else:
        steps.append({"op": "cron_scan", "result": "no errors"})

    # 3. Pacman update check
    if dry_run:
        steps.append({"op": "pacman_check", "result": "dry-run skipped"})
    else:
        ok, out = _safe_shell(["pacman", "-Sy", "--dry-run"], timeout=30, workdir="/tmp")
        if ok:
            upgrades = [line for line in out.splitlines() if "->" in line and "::" not in line]
            if upgrades:
                steps.append({"op": "pacman_check", "count": len(upgrades), "result": f"{len(upgrades)} packages upgradable"})
            else:
                steps.append({"op": "pacman_check", "result": "system up to date"})
        else:
            errors.append(f"pacman -Sy: {out[:100]}")
            steps.append({"op": "pacman_check", "result": out[:100], "ok": False})

    # 4. Learning extraction: run heartbeat_learning.py if action log has data
    if _LEARNING_SCRIPT.exists() and _ACTION_LOG_PATH.exists():
        if dry_run:
            steps.append({"op": "learn_extract", "result": "dry-run skipped"})
        else:
            ok, out = _safe_shell(
                ["python3", str(_LEARNING_SCRIPT)],
                timeout=30,
                workdir=str(_HERMES_HOME / "scripts"),
            )
            if ok:
                # Parse output for pattern count
                extracted = 0
                for line in out.splitlines():
                    if "Extracted" in line and "new candidate patterns" in line:
                        try:
                            extracted = int(line.split("Extracted")[1].split()[0])
                        except (ValueError, IndexError):
                            pass
                steps.append({"op": "learn_extract", "count": extracted, "result": f"extracted {extracted} patterns", "ok": True})
            else:
                steps.append({"op": "learn_extract", "result": "extraction skipped (not enough data)", "ok": True})

    result = f"EVOLVE: {len(steps)} steps" + (f", {len(errors)} errors" if errors else "")
    return result, steps, errors


def action_rest(snap: HeartbeatSnapshot, dry_run: bool) -> tuple[str, list[dict], list[str]]:
    """Prune old health logs and idle sessions."""
    steps: list[dict] = []
    errors: list[str] = []
    results = []

    if dry_run:
        return "[DRY] Would prune old logs and sessions", [{"op": "rest", "result": "dry-run skipped"}], []

    # Prune health logs older than 30 days
    cutoff = time.time() - 30 * 86400
    for p in _HEALTH_LOG_DIR.glob("health_*.json"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
                results.append(f"removed {p.name}")
        except Exception:
            pass

    if results:
        steps.append({"op": "prune_health_logs", "count": len(results), "files": results})
    else:
        steps.append({"op": "prune_health_logs", "result": "nothing to prune"})

    result = "; ".join(results) if results else "nothing to prune"
    return result, steps, errors


def action_report(snap: HeartbeatSnapshot, dry_run: bool) -> tuple[str, list[dict], list[str]]:
    """Summarize today's action log for Telegram delivery."""
    entries = _summarize_today()
    if not entries:
        return "silent: no actions today", [], []

    done: list[str] = []
    learnings: list[str] = []
    for e in entries:
        action = e.get("action", "?")
        for step in e.get("steps", []):
            op = step.get("op", "?")
            result = step.get("result", "?")
            done.append(f"- [{action}] {op}: {result}")
        if e.get("learnings"):
            learnings.append(f"- [{action}] {e['learnings']}")

    lines = ["🫀 心跳今日摘要", "", "做了："] + done
    if learnings:
        lines += ["", "學到："] + learnings

    # Include detected patterns (only new ones since last report)
    if _PATTERNS_PATH.exists():
        try:
            patterns_data = json.loads(_PATTERNS_PATH.read_text(encoding="utf-8"))
            pats = patterns_data.get("patterns", [])
            if pats:
                recent = [p for p in pats if p.get("frequency", 0) >= 2]
                if recent:
                    lines += ["", "📊 近期模式："]
                    for p in recent[:5]:
                        ptype = p.get("type", "?")
                        freq = p.get("frequency", 0)
                        desc = p.get("description", p.get("error", str(p)[:80]))
                        lines.append(f"- [{ptype}] ×{freq}: {desc}")
        except Exception:
            pass

    # Build displayable steps from entries for dry-run output
    report_steps = []
    for e in entries:
        a = e.get("action", "?")
        for s in e.get("steps", []):
            report_steps.append({"op": f"{a} {s.get('op','?')}", "result": s.get("result","?")})

    return "\n".join(lines), report_steps, []


# ── Action dispatch ──────────────────────────────────────────────────
def execute_action(
    action: str, snap: HeartbeatSnapshot, dry_run: bool
) -> tuple[str, list[dict], list[str]]:
    """Dispatch to the correct action function. Returns (result, steps, errors)."""
    actions = {
        "WORK": action_work,
        "REST": action_rest,
        "EVOLVE": action_evolve,
        "CONNECT": action_connect,
        "REPORT": action_report,
    }
    if action in actions:
        return actions[action](snap, dry_run)
    return f"unknown action: {action}", [], [f"unknown action: {action}"]


# ── Main ─────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes Heartbeat v2")
    parser.add_argument("--dry-run", action="store_true", help="Build snapshot and decide, do not execute actions")
    parser.add_argument("--action", type=str, default=None, help="Execute only action X (WORK|REST|EVOLVE|CONNECT|REPORT)")
    args = parser.parse_args()

    # Snapshot
    snap = build_heartbeat_snapshot()

    # Decision history
    history = _read_decision_history(limit=20)

    # Decide
    scores = score_actions(snap, history)
    action, reason = select_action(scores, snap, history)

    # Override if --action specified
    if args.action:
        action = args.action.upper()
        reason = f"--action override to {action}"

    # Record decision
    record_decision(action, reason, scores)

    # Execute
    trigger = {
        "disk_pct": snap.disk_used_pct,
        "memory_pct": snap.memory_used_pct,
        "cron_count": snap.cron_jobs_count,
        "stuck_sessions": len(snap.stuck_sessions),
        "failed_platforms": snap.failed_platforms,
    }
    result, steps, errors = execute_action(action, snap, args.dry_run)

    # Print status
    if args.dry_run:
        print(f"[DRY] Action: {action} | Reason: {reason}")
        print(f"       Scores: {', '.join(f'{k}={v:.1f}' for k, v in scores.items())}")
        for step in steps:
            print(f"       → {step.get('op','?')}: {step.get('result','?')}")
        print(f"       Result: {result}")
    else:
        # Record action log only for real executions
        outcome = "error" if errors else "ok"
        learnings = ""
        if errors:
            learnings = f"errors: {', '.join(errors[:3])}"
        _record_action_log(action, trigger, steps, outcome, errors, learnings)
        # Rotate action log (archive entries > 30 days)
        archived, kept = _rotate_action_log()
        if archived:
            print(f"Log rotation: archived {archived} entries, {kept} kept")
        print(f"Action: {action} | Result: {result}")
        if errors:
            print(f"Errors: {len(errors)}")
            for e in errors[:3]:
                print(f"  - {e}")


if __name__ == "__main__":
    main()
