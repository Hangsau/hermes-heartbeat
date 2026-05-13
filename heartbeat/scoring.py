"""Heartbeat cognitive layer — decision scoring and selection."""

from __future__ import annotations

import json, time
from typing import Any

from heartbeat.config import (
    _DECISIONS_PATH, _DECISION_COOLDOWN_SEC, _WEIGHT_PENDING_WORK,
    _WEIGHT_CACHE_BLOAT, _WEIGHT_FAILED_PLATFORMS, _WEIGHT_IDLE_TIME,
    _WEIGHT_EXPLORE_IDLE, _WEIGHT_REPETITION_PENALTY,
    _DISK_WARN_PCT,
)
from heartbeat.snapshot import HeartbeatSnapshot


def _read_decision_history(limit: int = 20) -> list[dict[str, Any]]:
    if not _DECISIONS_PATH.exists():
        return []
    try:
        with open(_DECISIONS_PATH, "r", encoding="utf-8") as f:
            lines = f.readlines()
        records = []
        for line in lines[-limit:]:
            line = line.strip()
            if not line: continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return records
    except Exception:
        return []


def _is_on_cooldown(action: str, history: list[dict[str, Any]], cooldown_sec: int) -> bool:
    now = time.time()
    for rec in reversed(history):
        if rec.get("action") == action:
            ts = rec.get("ts", 0)
            if now - ts < cooldown_sec:
                return True
            break
    return False


def score_actions(snap: HeartbeatSnapshot, history: list[dict[str, Any]]) -> dict[str, float]:
    """Score six possible actions based on current state."""
    scores: dict[str, float] = {
        "WORK": 5.0, "REST": 5.0, "EVOLVE": 5.0,
        "CONNECT": 5.0, "REPORT": 5.0, "EXPLORE": 5.0,
    }

    # Pending work boosts WORK
    pending = snap.cron_jobs_count + len(snap.stuck_sessions)
    scores["WORK"] += pending * _WEIGHT_PENDING_WORK

    # Cache bloat boosts REST
    bloat = 0.0
    if snap.disk_used_pct > _DISK_WARN_PCT:
        bloat += (snap.disk_used_pct - _DISK_WARN_PCT) / 10.0
    if snap.memory_used_pct and snap.memory_used_pct > 80.0:
        bloat += (snap.memory_used_pct - 80.0) / 10.0
    scores["REST"] += bloat * _WEIGHT_CACHE_BLOAT

    # Failed platforms boost EVOLVE
    failed = len(snap.failed_platforms)
    scores["EVOLVE"] += failed * _WEIGHT_FAILED_PLATFORMS

    # Idle time boosts CONNECT (but EXPLORE wins in deep idle)
    if snap.running_agents == 0 and snap.active_sessions < 50:
        scores["CONNECT"] += 3.0 * _WEIGHT_IDLE_TIME

    # Deep idle (no agents, no failures, no disk pressure) → EXPLORE
    if (snap.running_agents == 0 and not snap.failed_platforms
            and snap.disk_used_pct < _DISK_WARN_PCT
            and snap.cron_jobs_count < 5):
        scores["EXPLORE"] += _WEIGHT_EXPLORE_IDLE

    # Repetition penalty
    if history:
        last_action = history[-1].get("action")
        if last_action in scores:
            scores[last_action] += _WEIGHT_REPETITION_PENALTY

    return scores


def select_action(
    scores: dict[str, float], snap: HeartbeatSnapshot, history: list[dict[str, Any]]
) -> tuple[str, str]:
    """Pick highest-scoring action respecting cooldown and backpressure."""
    sorted_actions = sorted(scores.items(), key=lambda x: x[1], reverse=True)
    for action, score in sorted_actions:
        skips = []
        if _is_on_cooldown(action, history, _DECISION_COOLDOWN_SEC):
            skips.append("cooldown")
        if snap.running_agents > 0 and action == "WORK":
            skips.append("backpressure")
        if skips:
            continue
        reason = f"score={score:.1f}, pending={snap.cron_jobs_count}, stuck={len(snap.stuck_sessions)}, disk={snap.disk_used_pct:.1f}%"
        return action, reason
    return "REPORT", "all viable actions skipped (cooldown/backpressure), defaulting to REPORT"


def record_decision(action: str, reason: str, scores: dict[str, float]) -> None:
    rec = {"ts": time.time(), "action": action, "reason": reason, "scores": scores}
    try:
        with open(_DECISIONS_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass
