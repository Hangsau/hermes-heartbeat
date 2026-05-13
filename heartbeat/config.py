"""Heartbeat configuration constants."""

import re
from pathlib import Path

_HERMES_HOME = Path.home() / ".hermes"

# Paths
_STATE_PATH = _HERMES_HOME / "heartbeat_state.json"
_DECISIONS_PATH = _HERMES_HOME / "heartbeat_decisions.jsonl"
_CRON_JOBS_PATH = _HERMES_HOME / "cron" / "jobs.json"
_GATEWAY_PID_PATH = _HERMES_HOME / "gateway.pid"
_HEALTH_LOG_DIR = _HERMES_HOME / "health_logs"

# Timing
_AUTONOMIC_INTERVAL_SEC = 30
_COGNITIVE_INTERVAL_SEC = 300
_STUCK_THRESHOLD_MIN = 30
_STUCK_RECOVERY_MIN = 45
_ACTION_TIMEOUT_SEC = 300
_DECISION_COOLDOWN_SEC = 600
_WARMTH_IDLE_HOURS = 24

# Thresholds
_CACHE_BLOAT_THRESHOLD = 128
_DISK_WARN_PCT = 85
_DISK_CRIT_PCT = 95
_QUEUED_EVENTS_WARN = 10

# Scoring weights
_WEIGHT_PENDING_WORK = 2.0
_WEIGHT_CACHE_BLOAT = 1.5
_WEIGHT_FAILED_PLATFORMS = 2.0
_WEIGHT_IDLE_TIME = 0.5
_WEIGHT_EXPLORE_IDLE = 4.0  # boost EXPLORE when truly idle
_WEIGHT_REPETITION_PENALTY = -1.5

# Action log
_ACTION_LOG_PATH = _HERMES_HOME / "heartbeat_action_log.jsonl"
_ACTION_LOG_ARCHIVE_PATH = _HERMES_HOME / "heartbeat_action_log_archive.jsonl"
_PATTERNS_PATH = _HERMES_HOME / "heartbeat_patterns.json"
_LEARNING_SCRIPT = _HERMES_HOME / "scripts" / "heartbeat_learning.py"
_ACTION_LOG_ROTATE_DAYS = 30

# Session archiving
_SESSION_ARCHIVE_DIR = _HERMES_HOME / "sessions" / "archive"
_SESSION_ARCHIVE_IDLE_HOURS = 168  # 7 days
_SESSION_ARCHIVE_MIN_IDLE_MINUTES = 60
_CACHE_CLEAN_MTIME_DAYS = 7

# Coverage
_COVERAGE_PATH = _HERMES_HOME / "heartbeat_coverage.json"
_COVERAGE_RE = re.compile(r"TOTAL\s+\d+\s+\d+\s+(\d+)%")

# Provider probes
_PROVIDER_PROBE_URLS: dict[str, str] = {
    "openrouter": "https://openrouter.ai/api/v1/models",
    "anthropic": "https://api.anthropic.com/v1/messages",
    "openai": "https://api.openai.com/v1/models",
    "gemini": "https://generativelanguage.googleapis.com/v1beta/models",
    "ollama": "http://localhost:11434/api/tags",
}

# Git repos to push
_GIT_REPOS = [Path.home() / "managed-agents-research"]

# Daemon whitelist
_STUCK_DAEMON_WHITELIST = ["hermes gateway run", "hermes-admin/app.py"]
