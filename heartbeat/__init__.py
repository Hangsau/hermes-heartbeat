"""Heartbeat v2 — modular package."""

from heartbeat.config import (
    _HERMES_HOME, _STATE_PATH, _DECISIONS_PATH, _CRON_JOBS_PATH,
    _GATEWAY_PID_PATH, _HEALTH_LOG_DIR,
    _AUTONOMIC_INTERVAL_SEC, _COGNITIVE_INTERVAL_SEC,
    _STUCK_THRESHOLD_MIN, _STUCK_RECOVERY_MIN, _ACTION_TIMEOUT_SEC,
    _DECISION_COOLDOWN_SEC, _WARMTH_IDLE_HOURS,
    _CACHE_BLOAT_THRESHOLD, _DISK_WARN_PCT, _DISK_CRIT_PCT,
    _QUEUED_EVENTS_WARN,
    _WEIGHT_PENDING_WORK, _WEIGHT_CACHE_BLOAT, _WEIGHT_FAILED_PLATFORMS,
    _WEIGHT_IDLE_TIME, _WEIGHT_REPETITION_PENALTY,
    _ACTION_LOG_PATH, _ACTION_LOG_ARCHIVE_PATH, _PATTERNS_PATH,
    _LEARNING_SCRIPT, _ACTION_LOG_ROTATE_DAYS,
    _SESSION_ARCHIVE_DIR, _SESSION_ARCHIVE_IDLE_HOURS,
    _SESSION_ARCHIVE_MIN_IDLE_MINUTES, _CACHE_CLEAN_MTIME_DAYS,
    _COVERAGE_PATH, _COVERAGE_RE, _PROVIDER_PROBE_URLS,
    _GIT_REPOS, _STUCK_DAEMON_WHITELIST,
)

from heartbeat.utils import (
    _safe_json_read, _safe_json_write, _safe_shell, _safe_file_mtime,
    _probe_provider, _parse_coverage_pct, _track_coverage,
    _scan_cron_errors, _cache_clean_threshold,
)

from heartbeat.snapshot import (
    HeartbeatSnapshot, build_heartbeat_snapshot,
    _disk_usage, _memory_usage, _cron_jobs_count, _list_hermes_processes,
    _system_uptime, _count_active_sessions, _is_daemon_process,
    _detect_stuck_sessions, _scan_cold_sessions, _kanban_ready_tasks,
    _cache_size_mb, _provider_health_from_logs,
)

from heartbeat.scoring import (
    _read_decision_history, _is_on_cooldown,
    score_actions, select_action, record_decision,
)

from heartbeat.actions import (
    action_work, action_connect, action_evolve, action_rest, action_report,
    action_explore, execute_action,
    _record_action_log, _rotate_action_log, _summarize_today,
)
