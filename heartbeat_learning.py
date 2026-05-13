#!/usr/bin/env python3
"""heartbeat_learning.py — Pattern extraction from heartbeat action log.

Reads heartbeat_action_log.jsonl, finds patterns in time-series action data,
deduplicates via Jaccard similarity, writes heartbeat_patterns.json.

Pattern types:
  1. Recurring errors — same error across multiple cycles
  2. Trend shifts — REST frequency up/down over time
  3. Threshold misses — action fired but didn't improve the trigger metric
  4. Provider patterns — same provider degraded repeatedly
"""

from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


# ── config ────────────────────────────────────────────────────────
_LOG_PATH = Path.home() / ".hermes" / "heartbeat_action_log.jsonl"
_PATTERN_PATH = Path.home() / ".hermes" / "heartbeat_patterns.json"
_LOOKBACK_DAYS = 14
_MIN_EVENTS_FOR_PATTERN = 3

# ── keyword extraction (simple TF, stop-word filtered) ────────────
_STOP = {"the","a","an","is","of","to","in","and","or","not","no","ok","true",
         "false","none","error","action","trigger","ts","op","open",
         "succeeded","cleaned","removed","done","skipped"}


def _tokenize(text: str) -> set[str]:
    # split on non-word, lowercase, filter short + stop
    tokens = set()
    for t in re.split(r"[^a-zA-Z0-9_]", text.lower()):
        t = t.strip()
        if len(t) >= 3 and t not in _STOP:
            tokens.add(t)
    # also add 2-char tokens that look like codes (429, 503...)
    for t in re.findall(r"\b\d{3}\b", text):
        tokens.add(t)
    return tokens


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 0.0
    return len(a & b) / len(a | b)


# ── pattern detectors ─────────────────────────────────────────────

def _detect_recurring_errors(entries: list[dict]) -> list[dict]:
    """Find errors that appear across multiple non-consecutive cycles."""
    error_map: dict[str, list[int]] = defaultdict(list)
    for i, e in enumerate(entries):
        for err in e.get("errors", []):
            key = err if isinstance(err, str) else err.get("msg", str(err))
            # normalize: strip timestamps, IDs, specific sizes
            key = re.sub(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}.*?(?:Z|[+-]\d{2}:\d{2})", "<TS>", str(key))
            key = re.sub(r"\d+\.\d+GB", "<SIZE>", key)
            key = re.sub(r"[a-f0-9]{8,}", "<ID>", key)
            error_map[key].append(i)

    patterns = []
    for err_key, indices in error_map.items():
        if len(indices) >= _MIN_EVENTS_FOR_PATTERN:
            # check they're spread across at least 2 different days
            days = set()
            for idx in indices:
                ts = entries[idx].get("ts", "")
                if ts:
                    days.add(ts[:10])
            if len(days) >= 2:
                patterns.append({
                    "type": "recurring_error",
                    "error": err_key,
                    "occurrences": len(indices),
                    "days": len(days),
                    "first_seen": entries[indices[0]].get("ts", ""),
                    "last_seen": entries[indices[-1]].get("ts", ""),
                    "fingerprint_tokens": sorted(_tokenize(err_key)),
                })
    return patterns


def _detect_trend_shifts(entries: list[dict], lookback_days: int) -> list[dict]:
    """Detect if REST frequency is trending up or down."""
    if len(entries) < 7:
        return []

    # split into first half and second half
    mid = len(entries) // 2
    first_half = entries[:mid]
    second_half = entries[mid:]

    def rest_pct(batch: list[dict]) -> float:
        if not batch:
            return 0.0
        return sum(1 for e in batch if e.get("action") == "REST") / len(batch)

    r1, r2 = rest_pct(first_half), rest_pct(second_half)
    delta = r2 - r1

    if abs(delta) < 0.1:
        return []

    direction = "more stable" if delta > 0 else "more active"
    return [{
        "type": "trend_shift",
        "metric": "REST_frequency",
        "first_half_pct": round(r1, 3),
        "second_half_pct": round(r2, 3),
        "delta": round(delta, 3),
        "direction": direction,
        "interpretation": f"System is {direction} (REST {r1:.0%} → {r2:.0%})",
        "fingerprint_tokens": ["rest", "trend", "frequency", direction.replace(" ", "_")],
    }]


def _detect_action_ineffectiveness(entries: list[dict]) -> list[dict]:
    """Detect actions that ran but didn't improve the metric they target."""
    # Look for pairs: WORK(action) + next cycle's snapshot
    patterns = []
    for i in range(len(entries) - 1):
        cur = entries[i]
        nxt = entries[i + 1]
        if cur.get("action") != "WORK" or cur.get("outcome") != "ok":
            continue
        cur_trig = cur.get("trigger", {})
        nxt_trig = nxt.get("trigger", {})

        disk_before = cur_trig.get("disk_pct")
        disk_after = nxt_trig.get("disk_pct")
        if disk_before and disk_after and disk_after >= disk_before:
            # Disk didn't improve (stayed same or got worse) — cache clean ineffective
            patterns.append({
                "type": "ineffective_action",
                "action": "WORK",
                "metric": "disk_pct",
                "before": disk_before,
                "after": disk_after,
                "hypothesis": "Cache clean may not free enough space — check for large files outside ~/.cache/",
                "ts": cur.get("ts", ""),
                "fingerprint_tokens": ["work", "ineffective", "disk", "cache"],
            })
            if len(patterns) >= 3:
                break  # don't flood
    return patterns


def _detect_provider_patterns(entries: list[dict]) -> list[dict]:
    """Find providers that keep showing up in CONNECT action errors."""
    provider_days: dict[str, set[str]] = defaultdict(set)
    for e in entries:
        if e.get("action") != "CONNECT":
            continue
        for step in e.get("steps", []):
            provider = step.get("provider", "")
            if provider:
                ts = e.get("ts", "")
                if ts:
                    provider_days[provider].add(ts[:10])

    patterns = []
    for prov, days in provider_days.items():
        if len(days) >= _MIN_EVENTS_FOR_PATTERN:
            patterns.append({
                "type": "provider_pattern",
                "provider": prov,
                "degraded_days": len(days),
                "days_list": sorted(days),
                "suggestion": f"Consider switching {prov} to fallback or lowering cron frequency",
                "fingerprint_tokens": [prov.lower(), "provider", "degraded", "recurring"],
            })
    return patterns


# ── main ─────────────────────────────────────────────────────────

def _load_log() -> list[dict]:
    entries = []
    if _LOG_PATH.exists():
        for line in _LOG_PATH.read_text().strip().split("\n"):
            if line.strip():
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return entries


def _load_existing_patterns() -> dict[str, Any]:
    if _PATTERN_PATH.exists():
        try:
            return json.loads(_PATTERN_PATH.read_text())
        except (json.JSONDecodeError, FileNotFoundError):
            pass
    return {"patterns": [], "fingerprint_index": {}}


def _merge_patterns(new_patterns: list[dict], existing: dict) -> dict:
    """Jaccard-dedup: ≥0.5 update; <0.4 new; 0.4-0.5 keep if new type."""
    updated = dict(existing)
    idx = updated.get("fingerprint_index", {})

    for pat in new_patterns:
        fp = set(pat.get("fingerprint_tokens", []))
        fp_key = " ".join(sorted(fp))
        if not fp:
            continue

        best_match, best_score = None, 0.0
        for existing_key, existing_fp in idx.items():
            score = _jaccard(fp, set(existing_fp))
            if score > best_score:
                best_score = score
                best_match = existing_key

        if best_score >= 0.5 and best_match:
            # update existing pattern entry
            for existing_pat in updated["patterns"]:
                if existing_pat.get("fingerprint_tokens") == idx.get(best_match, []):
                    existing_pat["last_seen"] = datetime.now(timezone.utc).isoformat()
                    existing_pat["occurrences"] = existing_pat.get("occurrences", 1) + 1
                    existing_pat["updated"] = datetime.now(timezone.utc).isoformat()
                    break
        elif best_score < 0.4:
            pat["first_seen"] = pat.get("first_seen", datetime.now(timezone.utc).isoformat())
            pat["detected_at"] = datetime.now(timezone.utc).isoformat()
            updated["patterns"].append(pat)
            idx[fp_key] = list(fp)
        # 0.4-0.5: skip ambiguous match

    # prune old patterns (> 90 days), but preserve patterns without clear dates
    cutoff = datetime.now(timezone.utc) - timedelta(days=90)
    def _has_recent_date(p: dict) -> bool:
        date_str = p.get("detected_at") or p.get("first_seen") or ""
        if not date_str:
            return True  # no date → keep
        try:
            return datetime.fromisoformat(date_str) > cutoff
        except (ValueError, TypeError):
            return True  # unparseable → keep
    updated["patterns"] = [p for p in updated["patterns"] if _has_recent_date(p)]

    updated["fingerprint_index"] = idx
    updated["last_run"] = datetime.now(timezone.utc).isoformat()
    return updated


def main() -> None:
    entries = _load_log()
    if len(entries) < _MIN_EVENTS_FOR_PATTERN:
        print(f"Not enough data ({len(entries)} entries, need {_MIN_EVENTS_FOR_PATTERN}).")
        return

    # filter to lookback window
    cutoff = datetime.now(timezone.utc) - timedelta(days=_LOOKBACK_DAYS)
    recent = []
    for e in entries:
        ts = e.get("ts", "")
        if ts:
            try:
                dt = datetime.fromisoformat(ts)
                if dt >= cutoff:
                    recent.append(e)
            except ValueError:
                recent.append(e)
        else:
            recent.append(e)

    # run all detectors
    new_patterns = []
    new_patterns.extend(_detect_recurring_errors(recent))
    new_patterns.extend(_detect_trend_shifts(recent, _LOOKBACK_DAYS))
    new_patterns.extend(_detect_action_ineffectiveness(recent))
    new_patterns.extend(_detect_provider_patterns(recent))

    existing = _load_existing_patterns()
    merged = _merge_patterns(new_patterns, existing)

    _PATTERN_PATH.write_text(json.dumps(merged, indent=2, ensure_ascii=False))
    print(f"Extracted {len(new_patterns)} new candidate patterns, "
          f"merged into {len(merged['patterns'])} total patterns.")


if __name__ == "__main__":
    main()
