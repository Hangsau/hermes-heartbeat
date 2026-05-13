"""Heartbeat action functions — WORK, REST, EVOLVE, CONNECT, REPORT, EXPLORE."""

from __future__ import annotations

import json, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from heartbeat.config import (
    _HERMES_HOME, _CRON_JOBS_PATH, _STATE_PATH, _HEALTH_LOG_DIR,
    _GIT_REPOS, _DECISION_COOLDOWN_SEC,
    _ACTION_LOG_PATH, _ACTION_LOG_ARCHIVE_PATH, _ACTION_LOG_ROTATE_DAYS,
    _PATTERNS_PATH, _LEARNING_SCRIPT, _COVERAGE_PATH,
    _SESSION_ARCHIVE_DIR, _SESSION_ARCHIVE_IDLE_HOURS,
    _SESSION_ARCHIVE_MIN_IDLE_MINUTES, _CACHE_CLEAN_MTIME_DAYS,
)
from heartbeat.snapshot import HeartbeatSnapshot
from heartbeat.utils import (
    _safe_json_read, _safe_json_write, _safe_shell,
    _probe_provider, _parse_coverage_pct, _track_coverage,
    _scan_cron_errors, _cache_clean_threshold,
)

# ── Action log ──

def _record_action_log(
    action: str, trigger: dict, steps: list[dict],
    outcome: str, errors: list[str], learnings: str = ""
) -> None:
    try:
        rec = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "action": action, "trigger": trigger, "steps": steps,
            "outcome": outcome, "errors": errors, "learnings": learnings,
        }
        with open(_ACTION_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass


def _rotate_action_log() -> tuple[int, int]:
    if not _ACTION_LOG_PATH.exists(): return 0, 0
    cutoff_ts = datetime.now(timezone.utc)
    cutoff = (cutoff_ts.replace(hour=0, minute=0, second=0, microsecond=0)
              - timedelta(days=_ACTION_LOG_ROTATE_DAYS)).isoformat()
    kept, archived = [], []
    try:
        with open(_ACTION_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line_s = line.strip()
                if not line_s: continue
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
    if not archived: return 0, len(kept)
    try:
        with open(_ACTION_LOG_ARCHIVE_PATH, "a", encoding="utf-8") as f:
            for line in archived: f.write(line + "\n")
    except Exception:
        return 0, len(kept)
    try:
        with open(_ACTION_LOG_PATH, "w", encoding="utf-8") as f:
            for line in kept: f.write(line + "\n")
    except Exception:
        return 0, len(kept)
    return len(archived), len(kept)


def _summarize_today() -> list[dict[str, Any]]:
    if not _ACTION_LOG_PATH.exists(): return []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    entries = []
    try:
        with open(_ACTION_LOG_PATH, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line: continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if rec.get("ts", "").startswith(today):
                    entries.append(rec)
    except Exception:
        return []
    return entries


# ── Action implementations ──

def action_work(snap: HeartbeatSnapshot, dry_run: bool) -> tuple[str, list[dict], list[str]]:
    steps, errors = [], []
    days = _cache_clean_threshold(snap.disk_used_pct)
    # 1. Clean ~/.cache/
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

    # 2. Clean /tmp
    ok, out = _safe_shell(["find", "/tmp", "-name", "hermes_*", "-mmin", "+60", "-delete"], timeout=10)
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

    # 4. Git push
    for repo_path in _GIT_REPOS:
        if not repo_path.exists():
            steps.append({"op": "git_push", "repo": str(repo_path), "result": "repo not found", "ok": False})
            continue
        ok_fetch, out_fetch = _safe_shell(["git", "fetch", "origin"], timeout=30, workdir=str(repo_path))
        if not ok_fetch:
            errors.append(f"git fetch {repo_path}: {out_fetch}")
            steps.append({"op": "git_push", "repo": str(repo_path), "result": f"fetch: {out_fetch}", "ok": False})
            continue
        ok_diff, out_diff = _safe_shell(["git", "rev-list", "--count", "origin/master..HEAD"], timeout=10, workdir=str(repo_path))
        ahead = 0
        if ok_diff:
            try: ahead = int(out_diff.strip())
            except ValueError: pass
        if ahead == 0:
            steps.append({"op": "git_push", "repo": str(repo_path), "result": "no unpushed commits", "ok": True})
        elif dry_run:
            steps.append({"op": "git_push", "repo": str(repo_path), "result": f"[DRY] push {ahead} commits"})
        else:
            ok_push, out_push = _safe_shell(["git", "push", "origin", "master"], timeout=30, workdir=str(repo_path))
            if ok_push:
                steps.append({"op": "git_push", "repo": str(repo_path), "result": f"pushed {ahead} commits", "ok": True})
            else:
                errors.append(f"git push {repo_path}: {out_push}")
                steps.append({"op": "git_push", "repo": str(repo_path), "result": f"push failed: {out_push}", "ok": False})

    result = f"WORK: {len(steps)} steps" + (f", {len(errors)} errors" if errors else "")
    return result, steps, errors


def action_connect(snap: HeartbeatSnapshot, dry_run: bool) -> tuple[str, list[dict], list[str]]:
    steps, errors = [], []
    jobs_data = _safe_json_read(_CRON_JOBS_PATH, default={})
    jobs = jobs_data.get("jobs", [])
    provider_map = {
        "opencode": ["opencode"], "openrouter": ["openrouter", "kimi", "k2"],
        "anthropic": ["claude", "anthropic"], "openai": ["gpt", "openai"],
        "gemini": ["gemini"], "ollama": ["ollama"],
    }
    degraded = set(snap.failed_platforms)
    recovered = []
    for prov in list(degraded):
        alive, detail = _probe_provider(prov)
        if alive:
            recovered.append(prov)
            degraded.discard(prov)
            steps.append({"op": "probe_recovery", "provider": prov, "result": f"recovered ({detail})", "ok": True})
        else:
            steps.append({"op": "probe_degraded", "provider": prov, "result": detail, "ok": False})

    for job in jobs:
        job_name = job.get("name", "?")
        job_model = job.get("model", "") or ""
        if not job.get("enabled", True): continue
        matched = None
        for provider, keywords in provider_map.items():
            if provider in degraded and any(kw.lower() in job_model.lower() for kw in keywords):
                matched = provider; break
        if matched:
            if dry_run:
                steps.append({"op": "pause_job", "job": job_name, "reason": f"provider {matched} degraded", "result": "dry-run skipped"})
            else:
                job["enabled"] = False
                job["paused_at"] = datetime.now(timezone.utc).isoformat()
                job["paused_reason"] = f"heartbeat: provider {matched} degraded"
                steps.append({"op": "pause_job", "job": job_name, "result": "paused" if _safe_json_write(_CRON_JOBS_PATH, jobs_data) else "write failed", "ok": True})

    for job in jobs:
        paused_reason = job.get("paused_reason", "")
        if job.get("enabled", True) or not paused_reason.startswith("heartbeat:"): continue
        job_name = job.get("name", "?")
        job_model = job.get("model", "") or ""
        for provider, keywords in provider_map.items():
            if provider in paused_reason and provider not in degraded and any(kw.lower() in job_model.lower() for kw in keywords):
                if dry_run:
                    steps.append({"op": "unpause_job", "job": job_name, "result": "dry-run skipped"})
                else:
                    job["enabled"] = True; job["paused_at"] = None; job["paused_reason"] = None
                    steps.append({"op": "unpause_job", "job": job_name, "result": "unpaused" if _safe_json_write(_CRON_JOBS_PATH, jobs_data) else "write failed", "ok": True})

    cron_errors = _scan_cron_errors()
    for rl in [e for e in cron_errors if "429" in e["error_snippet"]]:
        rl_job_id = rl["job_id"]
        for job in jobs:
            if job.get("id") == rl_job_id and job.get("enabled", True):
                if dry_run:
                    steps.append({"op": "pause_rate_limited", "job": job.get("name", rl_job_id), "result": "dry-run skipped"})
                else:
                    job["enabled"] = False
                    job["paused_at"] = datetime.now(timezone.utc).isoformat()
                    job["paused_reason"] = "heartbeat: cron error 429 rate-limit"
                    steps.append({"op": "pause_rate_limited", "job": job.get("name", rl_job_id), "result": "paused" if _safe_json_write(_CRON_JOBS_PATH, jobs_data) else "write failed", "ok": True})
                break

    result = f"CONNECT: {len(steps)} steps" + (f", {len(errors)} errors" if errors else "")
    return result, steps, errors


def action_evolve(snap: HeartbeatSnapshot, dry_run: bool) -> tuple[str, list[dict], list[str]]:
    steps, errors = [], []

    # 1. Pytest canary + coverage
    test_path = _HERMES_HOME / "scripts" / "test_heartbeat_v2.py"
    if test_path.exists():
        if dry_run:
            steps.append({"op": "pytest_canary", "result": "dry-run skipped"})
        else:
            ok, out = _safe_shell(
                ["python3", "-m", "pytest", str(test_path), "--cov=heartbeat_v2", "--cov-report=term", "--tb=short", "-q"],
                timeout=90, workdir=str(_HERMES_HOME / "scripts"),
            )
            if ok:
                pct = _parse_coverage_pct(out)
                if pct is not None:
                    cov = _track_coverage(pct)
                    delta = cov.get("delta")
                    steps.append({"op": "pytest_canary", "result": "PASS", "coverage": pct, "delta": delta, "ok": True})
                else:
                    steps.append({"op": "pytest_canary", "result": "PASS (cov parse failed)", "ok": True})
            else:
                errors.append("pytest canary failed")
                steps.append({"op": "pytest_canary", "result": out[-200:], "ok": False})
    else:
        steps.append({"op": "pytest_canary", "result": "test file not found"})

    # 2. Cron scan
    cron_errors = _scan_cron_errors()
    steps.append({"op": "cron_scan", "count": len(cron_errors), "result": f"{len(cron_errors)} jobs with errors" if cron_errors else "no errors"})

    # 3. Pacman
    if dry_run:
        steps.append({"op": "pacman_check", "result": "dry-run skipped"})
    else:
        ok, out = _safe_shell(["pacman", "-Sy", "--dry-run"], timeout=30, workdir="/tmp")
        if ok:
            upgrades = [line for line in out.splitlines() if "->" in line and "::" not in line]
            steps.append({"op": "pacman_check", "count": len(upgrades), "result": f"{len(upgrades)} pkgs" if upgrades else "up to date"})
        else:
            errors.append(f"pacman: {out[:100]}")
            steps.append({"op": "pacman_check", "result": out[:100], "ok": False})

    # 4. Learning extraction
    if _LEARNING_SCRIPT.exists() and _ACTION_LOG_PATH.exists():
        if dry_run:
            steps.append({"op": "learn_extract", "result": "dry-run skipped"})
        else:
            ok, out = _safe_shell(["python3", str(_LEARNING_SCRIPT)], timeout=30, workdir=str(_HERMES_HOME / "scripts"))
            extracted = 0
            for line in out.splitlines():
                if "Extracted" in line and "new candidate patterns" in line:
                    try: extracted = int(line.split("Extracted")[1].split()[0])
                    except (ValueError, IndexError): pass
            steps.append({"op": "learn_extract", "count": extracted, "result": f"extracted {extracted} patterns" if extracted else "no new patterns", "ok": True})

    result = f"EVOLVE: {len(steps)} steps" + (f", {len(errors)} errors" if errors else "")
    return result, steps, errors


def action_rest(snap: HeartbeatSnapshot, dry_run: bool) -> tuple[str, list[dict], list[str]]:
    steps, errors = [], []
    if dry_run:
        return "[DRY] prune logs", [{"op": "rest", "result": "dry-run skipped"}], []

    cutoff = time.time() - 30 * 86400
    results = []
    for p in _HEALTH_LOG_DIR.glob("health_*.json"):
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink(); results.append(f"removed {p.name}")
        except Exception:
            pass
    if results:
        steps.append({"op": "prune_health_logs", "count": len(results), "ok": True})
    else:
        steps.append({"op": "prune_health_logs", "result": "nothing to prune"})
    return "; ".join(results) if results else "nothing to prune", steps, errors


def action_report(snap: HeartbeatSnapshot, dry_run: bool) -> tuple[str, list[dict], list[str]]:
    entries = _summarize_today()
    if not entries:
        return "silent: no actions today", [], []

    done, learnings = [], []
    for e in entries:
        action = e.get("action", "?")
        for step in e.get("steps", []):
            done.append(f"- [{action}] {step.get('op','?')}: {step.get('result','?')}")
        if e.get("learnings"):
            learnings.append(f"- [{action}] {e['learnings']}")

    lines = ["🫀 心跳今日摘要", "", "做了："] + done
    if learnings:
        lines += ["", "學到："] + learnings
    if _PATTERNS_PATH.exists():
        try:
            patterns_data = json.loads(_PATTERNS_PATH.read_text(encoding="utf-8"))
            pats = patterns_data.get("patterns", [])
            if pats:
                recent = [p for p in pats if p.get("frequency", 0) >= 2]
                if recent:
                    lines += ["", "📊 近期模式："]
                    for p in recent[:5]:
                        lines.append(f"- [{p.get('type','?')}] ×{p.get('frequency',0)}: {p.get('description', str(p)[:80])}")
        except Exception:
            pass

    report_steps = []
    for e in entries:
        a = e.get("action", "?")
        for s in e.get("steps", []):
            report_steps.append({"op": f"{a} {s.get('op','?')}", "result": s.get("result","?")})
    return "\n".join(lines), report_steps, []


# ── Explore: autonomous exploration ──
_EXPLORE_MENU = [
    {"name": "check_research", "desc": "檢查 research repo 是否有未同步變更"},
    {"name": "prune_logs", "desc": "清理過期日誌和暫存"},
    {"name": "self_review", "desc": "自檢：上次錯誤是否已消失，健康狀態是否改善"},
    {"name": "kanban_check", "desc": "檢查 kanban 是否有 pending tasks"},
]

def action_explore(snap: HeartbeatSnapshot, dry_run: bool) -> tuple[str, list[dict], list[str]]:
    """Autonomous exploration: pick one thing from menu when idle."""
    steps, errors = [], []
    if dry_run:
        return "[DRY] explore menu", [{"op": "explore", "result": "menu shown, skipped"}], []

    # Rotate through menu items (using state to track)
    state = _safe_json_read(_STATE_PATH, default={})
    last_explore_idx = state.get("last_explore_idx", -1)
    next_idx = (last_explore_idx + 1) % len(_EXPLORE_MENU)
    choice = _EXPLORE_MENU[next_idx]
    state["last_explore_idx"] = next_idx
    _safe_json_write(_STATE_PATH, state)

    name = choice["name"]
    if name == "check_research":
        for repo_path in _GIT_REPOS:
            if not repo_path.exists(): continue
            ok, out = _safe_shell(["git", "status", "--porcelain"], timeout=10, workdir=str(repo_path))
            if ok and out.strip():
                steps.append({"op": "research_status", "repo": str(repo_path), "result": f"uncommitted: {out[:200]}"})
            else:
                steps.append({"op": "research_status", "repo": str(repo_path), "result": "clean"})

    elif name == "prune_logs":
        log_dir = _HERMES_HOME / "logs"
        cutoff = time.time() - 14 * 86400
        count = 0
        if log_dir.exists():
            for p in log_dir.glob("*.log"):
                try:
                    if p.stat().st_mtime < cutoff:
                        p.unlink(); count += 1
                except Exception:
                    pass
        steps.append({"op": "prune_logs", "count": count, "result": f"removed {count} old logs"})

    elif name == "self_review":
        # Check if previously reported errors are gone
        cron_errors = _scan_cron_errors()
        prev = state.get("last_error_count", 0)
        now_count = len(cron_errors)
        if now_count < prev:
            steps.append({"op": "error_trend", "prev": prev, "now": now_count, "result": f"errors decreased: {prev} → {now_count}"})
        elif now_count > prev:
            steps.append({"op": "error_trend", "prev": prev, "now": now_count, "result": f"errors increased: {prev} → {now_count}"})
        else:
            steps.append({"op": "error_trend", "prev": prev, "now": now_count, "result": "unchanged"})
        state["last_error_count"] = now_count
        _safe_json_write(_STATE_PATH, state)

    elif name == "kanban_check":
        from heartbeat.snapshot import _kanban_ready_tasks
        tasks = _kanban_ready_tasks(limit=5)
        if tasks:
            for t in tasks:
                steps.append({"op": "kanban_ready", "task": t.get("title","?")[:60], "priority": t.get("priority",0)})
        else:
            steps.append({"op": "kanban_ready", "result": "no ready tasks"})

    result = f"EXPLORE: {name} — {choice['desc']}"
    return result, steps, errors


# ── Dispatch ──

def execute_action(
    action: str, snap: HeartbeatSnapshot, dry_run: bool
) -> tuple[str, list[dict], list[str]]:
    actions = {
        "WORK": action_work, "REST": action_rest,
        "EVOLVE": action_evolve, "CONNECT": action_connect,
        "REPORT": action_report, "EXPLORE": action_explore,
    }
    if action in actions:
        return actions[action](snap, dry_run)
    return f"unknown action: {action}", [], [f"unknown action: {action}"]
