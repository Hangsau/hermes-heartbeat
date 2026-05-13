"""Heartbeat v2 main entry point."""

from __future__ import annotations

import argparse, sys

from heartbeat.snapshot import build_heartbeat_snapshot
from heartbeat.scoring import _read_decision_history, score_actions, select_action, record_decision
from heartbeat.actions import execute_action, _record_action_log, _rotate_action_log


def main() -> None:
    parser = argparse.ArgumentParser(description="Hermes Heartbeat v2")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--action", type=str, default=None,
                        help="Execute only action X (WORK|REST|EVOLVE|CONNECT|REPORT|EXPLORE)")
    args = parser.parse_args()

    snap = build_heartbeat_snapshot()
    history = _read_decision_history(limit=20)
    scores = score_actions(snap, history)
    action, reason = select_action(scores, snap, history)

    if args.action:
        action = args.action.upper()
        reason = f"--action override to {action}"

    record_decision(action, reason, scores)

    trigger = {
        "disk_pct": snap.disk_used_pct,
        "memory_pct": snap.memory_used_pct,
        "cron_count": snap.cron_jobs_count,
        "stuck_sessions": len(snap.stuck_sessions),
        "failed_platforms": snap.failed_platforms,
    }
    result, steps, errors = execute_action(action, snap, args.dry_run)

    if args.dry_run:
        print(f"[DRY] Action: {action} | Reason: {reason}")
        print(f"       Scores: {', '.join(f'{k}={v:.1f}' for k, v in scores.items())}")
        for step in steps:
            print(f"       -> {step.get('op','?')}: {step.get('result','?')}")
        print(f"       Result: {result}")
    else:
        outcome = "error" if errors else "ok"
        learnings = ""
        if errors:
            learnings = f"errors: {', '.join(errors[:3])}"
        _record_action_log(action, trigger, steps, outcome, errors, learnings)
        archived, kept = _rotate_action_log()
        if archived:
            print(f"Log rotation: archived {archived} entries, {kept} kept")
        print(f"Action: {action} | Result: {result}")
        if errors:
            print(f"Errors: {len(errors)}")
            for e in errors[:3]:
                print(f"  - {e}")
