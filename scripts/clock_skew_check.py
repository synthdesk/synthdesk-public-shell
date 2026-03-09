#!/usr/bin/env python3
"""
Clock skew daily presence check.

Verifies that `clock_skew_daily_YYYY-MM-DD.json` exists for a given date.
If missing, writes a warning artifact to the chosen output directory.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path


def _date_str(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def main() -> int:
    parser = argparse.ArgumentParser(description="Clock skew daily presence check.")
    parser.add_argument(
        "--base-dir",
        default="/root/synthdesk-listener/runs/0.2.0",
        help="Runs base dir containing clock_skew_daily_*.json files",
    )
    parser.add_argument(
        "--output-dir",
        default="/root/synthdesk/clock_skew",
        help="Directory for warning artifacts",
    )
    parser.add_argument(
        "--date",
        help="Date to check (YYYY-MM-DD). Default: yesterday UTC.",
    )
    args = parser.parse_args()

    if args.date:
        day = args.date
    else:
        day = _date_str(datetime.now(timezone.utc) - timedelta(days=1))

    base_dir = Path(args.base_dir)
    expected = base_dir / f"clock_skew_daily_{day}.json"
    if expected.exists():
        return 0

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    warn_path = output_dir / f"clock_skew_warning_{day}.json"
    if warn_path.exists():
        return 0

    payload = {
        "date": day,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "missing": str(expected),
        "reason": "clock_skew_daily file not found",
    }
    warn_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
