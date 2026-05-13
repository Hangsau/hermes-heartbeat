"""Heartbeat snapshot — system state collection."""

from __future__ import annotations

import dataclasses, shutil, time, sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from heartbeat.config import (
    _HERMES_HOME, _CRON_JOBS_PATH, _STUCK_THRESHOLD_MIN, _WARMTH_IDLE_HOURS,
    _STUCK_DAEMON_WHITELIST,
)
from heartbeat.utils import _safe_json_read, _safe_shell


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
    try:
        usage = shutil.disk_usage(_HERMES_HOME)
        used_pct = 100.0 - (usage.free / usage.total * 100.0)
        free_gb = usage.free / (1024 ** 3)
        return used_pct, free_gb
    except Exception:
        return 0.0, 0.0


def _memory_usage() -> float | None:
    ok, out = _safe_shell(["free", "-m"])
    if not ok: return None
    try:
        for line in out.splitlines():
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
    ok, out = _safe_shell(["pgrep", "-a", "-f", "python.*hermes"], timeout=5)
    if not ok: return []
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
    try:
        with open("/proc/uptime", "r") as f:
            return float(f.read().split()[0])
    except Exception:
        return 0.0


def _count_active_sessions() -> int:
    try:
        sess_dir = _HERMES_HOME / "sessions"
        return len(list(sess_dir.glob("*.jsonl")))
    except Exception:
        return 0


def _is_daemon_process(cmd: str) -> bool:
    for pattern in _STUCK_DAEMON_WHITELIST:
        if pattern in cmd:
            return True
    return False


def _detect_stuck_sessions(threshold_min: int) -> list[dict[str, Any]]:
    procs = _list_hermes_processes()
    if not procs: return []
    ok, out = _safe_shell(
        ["ps", "-o", "pid=,etime=,cmd=", "-p"] + [str(p["pid"]) for p in procs],
        timeout=5,
    )
    if not ok: return []
    stuck = []
    for line in out.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 3: continue
        pid_str, etime, cmd = parts
        if _is_daemon_process(cmd): continue
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
    session_dir = _HERMES_HOME / "sessions"
    if not session_dir.exists(): return []
    cutoff = time.time() - hours * 3600
    cold = []
    for path in session_dir.glob("*.jsonl"):
        try:
            mtime = path.stat().st_mtime
            if mtime < cutoff:
                cold.append({"file": path.name, "idle_hours": round((time.time() - mtime) / 3600, 1)})
        except Exception:
            continue
    cold.sort(key=lambda x: x["idle_hours"], reverse=True)
    return cold[:10]


def _kanban_ready_tasks(limit: int = 5) -> list[dict[str, Any]]:
    db_path = _HERMES_HOME / "kanban.db"
    if not db_path.exists(): return []
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
    cache_dir = _HERMES_HOME / "cache"
    if not cache_dir.exists(): return 0.0
    total = 0
    for p in cache_dir.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except Exception:
            continue
    return round(total / (1024 * 1024), 2)


def _provider_health_from_logs() -> dict[str, Any]:
    err_log = _HERMES_HOME / "logs" / "errors.log"
    if not err_log.exists(): return {}
    try:
        size = err_log.stat().st_size
        with open(err_log, "rb") as f:
            f.seek(max(0, size - 50 * 1024))
            tail = f.read().decode("utf-8", errors="ignore")
    except Exception:
        return {}
    counts = {}
    for kw in ["anthropic", "openrouter", "openai", "gemini", "ollama", "opencode"]:
        c = tail.lower().count(kw)
        if c:
            counts[kw] = {"errors_in_tail": c, "status": "degraded" if c > 5 else "ok"}
    return counts


def build_heartbeat_snapshot() -> HeartbeatSnapshot:
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
