"""Heartbeat utility functions — safe I/O, shell, probes, coverage."""

from __future__ import annotations

import json, os, re, subprocess, time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from heartbeat.config import (
    _HERMES_HOME, _CRON_JOBS_PATH, _COVERAGE_PATH, _COVERAGE_RE,
    _PROVIDER_PROBE_URLS, _CACHE_CLEAN_MTIME_DAYS,
)


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
    import subprocess
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


def _cache_clean_threshold(disk_pct: float | None) -> int:
    """Return cache mtime threshold in days, based on disk pressure."""
    if disk_pct is None:
        return _CACHE_CLEAN_MTIME_DAYS
    if disk_pct > 90:
        return 1
    if disk_pct > 80:
        return 3
    if disk_pct > 70:
        return 5
    return _CACHE_CLEAN_MTIME_DAYS


def _probe_provider(provider: str) -> tuple[bool, str]:
    """Lightweight HTTP probe for a provider. Returns (alive, detail)."""
    if provider not in _PROVIDER_PROBE_URLS:
        return False, f"no probe URL for {provider}"
    url = _PROVIDER_PROBE_URLS[provider]
    ok, out = _safe_shell(
        ["curl", "-s", "--max-time", "10", "-o", "/dev/null", "-w", "%{http_code}", url],
        timeout=15,
    )
    if ok and out.strip():
        code = out.strip()
        return True, f"HTTP {code}"
    return False, out[:80] if out else "no output"


def _parse_coverage_pct(cov_output: str) -> int | None:
    """Extract TOTAL coverage percentage from pytest --cov output."""
    m = _COVERAGE_RE.search(cov_output)
    if m:
        return int(m.group(1))
    return None


def _track_coverage(pct: int) -> dict:
    """Compare current coverage with last, save new baseline."""
    prev: dict = {}
    if _COVERAGE_PATH.exists():
        try:
            prev = json.loads(_COVERAGE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    prev_pct = prev.get("coverage_pct")
    delta = pct - prev_pct if prev_pct is not None else None
    baseline = {
        "coverage_pct": pct,
        "ts": datetime.now(timezone.utc).isoformat(),
        "prev_pct": prev_pct,
        "delta": delta,
    }
    _safe_json_write(_COVERAGE_PATH, baseline)
    return baseline


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
