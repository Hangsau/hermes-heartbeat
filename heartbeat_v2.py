#!/usr/bin/env python3
"""Heartbeat v2 — thin wrapper importing from heartbeat/ package.

Run standalone or invoked by the internal-heartbeat cron.
  --dry-run    Build snapshot and decisions, do not execute actions.
  --action=X   Execute only action X (WORK|REST|EVOLVE|CONNECT|REPORT|EXPLORE).
"""

from heartbeat.main import main

if __name__ == "__main__":
    main()
