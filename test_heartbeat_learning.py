"""Tests for heartbeat_learning.py — pattern extraction from action log."""

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

import heartbeat_learning as hl


# ── helpers ────────────────────────────────────────────────────────

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _entry(action: str, ts: str = None, outcome: str = "ok",
           errors: list = None, trigger: dict = None,
           steps: list = None) -> dict:
    e = {"action": action, "ts": ts or _now(), "outcome": outcome}
    if errors:
        e["errors"] = errors
    if trigger:
        e["trigger"] = trigger
    if steps:
        e["steps"] = steps
    return e


# ── recurring error detector ───────────────────────────────────────

def test_recurring_errors_detected():
    entries = [
        _entry("EVOLVE", ts="2026-05-01T10:00:00+00:00", errors=["pytest crashed: SIGKILL"]),
        _entry("EVOLVE", ts="2026-05-05T10:00:00+00:00", errors=["pytest crashed: SIGKILL"]),
        _entry("EVOLVE", ts="2026-05-07T10:00:00+00:00", errors=["pytest crashed: SIGKILL"]),
    ]
    patterns = hl._detect_recurring_errors(entries)
    assert len(patterns) == 1
    assert patterns[0]["type"] == "recurring_error"
    assert patterns[0]["occurrences"] == 3
    assert patterns[0]["days"] == 3


def test_recurring_errors_insufficient():
    entries = [
        _entry("EVOLVE", errors=["err"]),
        _entry("EVOLVE", errors=["err"]),
    ]
    patterns = hl._detect_recurring_errors(entries)
    assert len(patterns) == 0


def test_recurring_errors_same_day_no_pattern():
    entries = [
        _entry("EVOLVE", ts="2026-05-01T10:00:00+00:00", errors=["pytest crashed"]),
        _entry("EVOLVE", ts="2026-05-01T11:00:00+00:00", errors=["pytest crashed"]),
        _entry("EVOLVE", ts="2026-05-01T12:00:00+00:00", errors=["pytest crashed"]),
    ]
    patterns = hl._detect_recurring_errors(entries)
    # same day is < 2 days so filtered out
    assert len(patterns) == 0


def test_recurring_errors_normalize_timestamps():
    entries = [
        _entry("EVOLVE", ts="2026-05-01T10:00:00+00:00",
               errors=["Error 2026-05-01T10:00:00Z connection timeout 429"]),
        _entry("EVOLVE", ts="2026-05-05T10:00:00+00:00",
               errors=["Error 2026-05-05T10:00:00Z connection timeout 429"]),
        _entry("EVOLVE", ts="2026-05-07T10:00:00+00:00",
               errors=["Error 2026-05-07T10:00:00Z connection timeout 429"]),
    ]
    patterns = hl._detect_recurring_errors(entries)
    assert len(patterns) == 1
    # normalized key should have <TS> not actual dates
    assert "<TS>" in patterns[0]["error"]


# ── trend shift detector ───────────────────────────────────────────

def test_trend_shift_more_stable():
    entries = [
        _entry("WORK"), _entry("REST"), _entry("WORK"), _entry("WORK"),
        _entry("REST"), _entry("REST"), _entry("REST"), _entry("EVOLVE"),
    ]
    patterns = hl._detect_trend_shifts(entries, 14)
    assert len(patterns) == 1
    assert patterns[0]["type"] == "trend_shift"
    # REST in first half: 1/4=25%, second half: 3/4=75% → delta +0.5
    assert patterns[0]["delta"] == pytest.approx(0.5)
    assert "more stable" in patterns[0]["direction"]


def test_trend_shift_more_active():
    entries = [
        _entry("REST"), _entry("REST"), _entry("REST"), _entry("REST"),
        _entry("WORK"), _entry("WORK"), _entry("EVOLVE"), _entry("WORK"),
    ]
    patterns = hl._detect_trend_shifts(entries, 14)
    assert len(patterns) == 1
    assert "more active" in patterns[0]["direction"]


def test_trend_shift_no_significant_change():
    entries = [
        _entry("REST"), _entry("WORK"), _entry("WORK"), _entry("CONNECT"),
        _entry("REST"), _entry("WORK"), _entry("WORK"), _entry("EVOLVE"),
    ]
    patterns = hl._detect_trend_shifts(entries, 14)
    # delta = 0 (both halves: 1/4=25% REST)
    assert len(patterns) == 0


def test_trend_shift_too_few_entries():
    entries = [_entry("REST"), _entry("WORK"), _entry("REST")]
    patterns = hl._detect_trend_shifts(entries, 14)
    assert len(patterns) == 0


# ── action ineffectiveness detector ────────────────────────────────

def test_ineffective_work_disk():
    entries = [
        _entry("WORK", outcome="ok", trigger={"disk_pct": 45.5},
               ts="2026-05-01T10:00:00+00:00"),
        _entry("REST", trigger={"disk_pct": 45.5},
               ts="2026-05-01T10:30:00+00:00"),
    ]
    patterns = hl._detect_action_ineffectiveness(entries)
    assert len(patterns) == 1
    assert patterns[0]["type"] == "ineffective_action"
    assert patterns[0]["metric"] == "disk_pct"


def test_effective_work_disk_no_pattern():
    entries = [
        _entry("WORK", outcome="ok", trigger={"disk_pct": 50.0},
               ts="2026-05-01T10:00:00+00:00"),
        _entry("REST", trigger={"disk_pct": 30.0},  # disk dropped
               ts="2026-05-01T10:30:00+00:00"),
    ]
    patterns = hl._detect_action_ineffectiveness(entries)
    assert len(patterns) == 0


def test_ineffective_action_caps_at_3():
    # generate 10 ineffective pairs
    entries = []
    for i in range(20):
        entries.append(_entry("WORK", outcome="ok",
                              trigger={"disk_pct": 50.0 + i * 0.1},
                              ts=f"2026-05-{1+i:02d}T10:00:00+00:00"))
        entries.append(_entry("REST", trigger={"disk_pct": 50.1 + i * 0.1},
                              ts=f"2026-05-{1+i:02d}T10:30:00+00:00"))
    patterns = hl._detect_action_ineffectiveness(entries)
    assert len(patterns) == 3  # capped


# ── provider pattern detector ──────────────────────────────────────

def test_provider_pattern_detected():
    entries = [
        _entry("CONNECT", ts="2026-05-01T10:00:00+00:00", outcome="ok",
               steps=[{"provider": "opencode"},
                      {"action": "pause", "provider": "opencode", "result": "paused"}]),
        _entry("CONNECT", ts="2026-05-03T10:00:00+00:00", outcome="ok",
               steps=[{"provider": "opencode"},
                      {"action": "pause", "provider": "opencode", "result": "paused"}]),
        _entry("CONNECT", ts="2026-05-05T10:00:00+00:00", outcome="ok",
               steps=[{"provider": "opencode"},
                      {"action": "pause", "provider": "opencode", "result": "paused"}]),
    ]
    patterns = hl._detect_provider_patterns(entries)
    assert len(patterns) == 1
    assert patterns[0]["provider"] == "opencode"
    assert patterns[0]["degraded_days"] == 3


def test_provider_pattern_insufficient_days():
    entries = [
        _entry("CONNECT", ts="2026-05-01T10:00:00+00:00",
               steps=[{"provider": "opencode"}]),
        _entry("CONNECT", ts="2026-05-01T11:00:00+00:00",
               steps=[{"provider": "opencode"}]),
    ]
    patterns = hl._detect_provider_patterns(entries)
    assert len(patterns) == 0


# ── tokenizer ──────────────────────────────────────────────────────

def test_tokenize_basic():
    tokens = hl._tokenize("pytest crashed with SIGKILL")
    assert "pytest" in tokens
    assert "crashed" in tokens
    assert "sigkill" in tokens


def test_tokenize_error_codes():
    tokens = hl._tokenize("HTTP 429 Too Many Requests")
    assert "429" in tokens
    assert "http" in tokens
    assert "many" in tokens
    assert "requests" in tokens


def test_tokenize_stop_words():
    tokens = hl._tokenize("the result is ok and not failed")
    assert "the" not in tokens  # in STOP
    assert "and" not in tokens  # in STOP
    assert "ok" not in tokens  # in STOP
    assert "result" in tokens
    assert "failed" in tokens


# ── jaccard ────────────────────────────────────────────────────────

def test_jaccard_identical():
    assert hl._jaccard({"a", "b", "c"}, {"a", "b", "c"}) == 1.0


def test_jaccard_disjoint():
    assert hl._jaccard({"a", "b"}, {"c", "d"}) == 0.0


def test_jaccard_partial():
    s = hl._jaccard({"a", "b", "c"}, {"a", "b", "d"})
    assert s == pytest.approx(2/4)


def test_jaccard_empty():
    assert hl._jaccard(set(), set()) == 0.0


# ── merge_patterns ─────────────────────────────────────────────────

def test_merge_new_pattern():
    existing = {"patterns": [], "fingerprint_index": {}}
    new = [{"type": "recurring_error", "error": "pytest crash",
            "fingerprint_tokens": ["pytest", "crash"]}]
    result = hl._merge_patterns(new, existing)
    assert len(result["patterns"]) == 1


def test_merge_dedup_high_similarity():
    existing = {
        "patterns": [{"type": "recurring_error", "error": "pytest crash",
                      "occurrences": 1,
                      "fingerprint_tokens": ["pytest", "crash", "sigkill"]}],
        "fingerprint_index": {"pytest crash sigkill": ["pytest", "crash", "sigkill"]}
    }
    new = [{"type": "recurring_error", "error": "pytest crash again",
            "fingerprint_tokens": ["pytest", "crash", "sigkill"]}]
    result = hl._merge_patterns(new, existing)
    # should update existing, not add new
    assert len(result["patterns"]) == 1
    assert result["patterns"][0]["occurrences"] == 2


def test_merge_dedup_low_similarity():
    existing = {
        "patterns": [{"type": "recurring_error", "error": "pytest crash",
                      "occurrences": 1,
                      "fingerprint_tokens": ["pytest", "crash"]}],
        "fingerprint_index": {"pytest crash": ["pytest", "crash"]}
    }
    new = [{"type": "provider_pattern", "provider": "opencode",
            "fingerprint_tokens": ["opencode", "provider", "degraded"]}]
    result = hl._merge_patterns(new, existing)
    assert len(result["patterns"]) == 2  # both kept


# ── integration: full pipeline with synthetic data ─────────────────

def test_full_pipeline_with_synthetic_data():
    """Create 30 entries with known patterns, run full pipeline."""
    entries = []
    base = datetime(2026, 5, 1, tzinfo=timezone.utc)

    for i in range(30):
        ts = (base + timedelta(days=i//2, hours=i % 24)).isoformat()
        if i % 7 == 0:
            entries.append(_entry("WORK", ts=ts, outcome="ok",
                                   trigger={"disk_pct": 60 - i * 0.5},
                                   errors=["disk full warning"] if i > 3 else []))
        elif i % 7 in (1, 2):
            entries.append(_entry("CONNECT", ts=ts, outcome="ok",
                                   steps=[{"provider": "opencode"},
                                          {"action": "pause", "provider": "opencode"}]))
        elif i % 7 in (3, 4, 5):
            entries.append(_entry("REST", ts=ts))
        else:
            entries.append(_entry("EVOLVE", ts=ts,
                                   errors=["pytest failed: import error"] if i < 14 else []))

    with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as f:
        for e in entries:
            f.write(json.dumps(e) + "\n")
        tmp_path = f.name

    try:
        with patch.object(hl, "_LOG_PATH", Path(tmp_path)):
            with patch.object(hl, "_PATTERN_PATH", Path(tmp_path.replace(".jsonl", "_patterns.json"))):
                hl.main()
                patterns = json.loads(Path(tmp_path.replace(".jsonl", "_patterns.json")).read_text())
                assert len(patterns["patterns"]) > 0
                types = {p["type"] for p in patterns["patterns"]}
                assert "provider_pattern" in types
    finally:
        Path(tmp_path).unlink(missing_ok=True)
        Path(tmp_path.replace(".jsonl", "_patterns.json")).unlink(missing_ok=True)
