#!/usr/bin/env python3
"""
Slice Freeze - Create a frozen, hash-verified slice of synthetic listener data.

Creates deterministic filenames:
    frozen/<date>/ticks_<start>_<end>.jsonl
    frozen/<date>/spine_<start>_<end>.jsonl
    frozen/<date>/manifest.json

Usage:
    python3 scripts/slice_freeze.py \
        --runs-dir ./fixtures/slice_freeze/runs \
        --date 2026-01-10 \
        [--hours 24]
"""

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional


def sha256_file(path: Path) -> str:
    """Compute SHA256 hash of file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8192), b""):
            digest.update(chunk)
    return digest.hexdigest()


def parse_timestamp(ts_str: str) -> Optional[datetime]:
    """Parse ISO timestamp to datetime."""
    if not ts_str:
        return None
    try:
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        return datetime.fromisoformat(ts_str)
    except ValueError:
        return None


def filter_ticks_by_time(
    tick_dir: Path,
    start_time: datetime,
    end_time: datetime,
    output_path: Path,
) -> int:
    """Filter tick rows to the requested time window."""
    count = 0
    tick_file = tick_dir / "tick_observation.jsonl"

    if not tick_file.exists():
        return 0

    with tick_file.open("r", encoding="utf-8") as infile, output_path.open("w", encoding="utf-8") as outfile:
        for line in infile:
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            ts_str = obj.get("timestamp") or obj.get("ts_utc") or obj.get("ts")
            ts = parse_timestamp(ts_str)
            if ts is None:
                continue

            if start_time <= ts <= end_time:
                outfile.write(line)
                count += 1

    return count


def filter_spine_by_time(
    spine_path: Path,
    start_time: datetime,
    end_time: datetime,
    output_path: Path,
) -> int:
    """Filter spine rows to the requested time window."""
    count = 0

    with spine_path.open("r", encoding="utf-8") as infile, output_path.open("w", encoding="utf-8") as outfile:
        for line in infile:
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            ts_str = obj.get("timestamp")
            ts = parse_timestamp(ts_str)
            if ts is None:
                continue

            if start_time <= ts <= end_time:
                outfile.write(line)
                count += 1

    return count


def main() -> int:
    parser = argparse.ArgumentParser(description="Freeze a synthetic slice for testing")
    parser.add_argument("--runs-dir", required=True, help="Path to synthetic runs directory")
    parser.add_argument("--date", required=True, help="Date to freeze (YYYY-MM-DD)")
    parser.add_argument("--hours", type=int, default=24, help="Hours to include")
    args = parser.parse_args()

    runs_dir = Path(args.runs_dir)
    target_date = args.date

    try:
        dt = datetime.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        print(f"Error: invalid date format: {target_date}", file=sys.stderr)
        return 1

    start_time = dt
    end_time = dt + timedelta(hours=args.hours)

    tick_dir = runs_dir / target_date
    spine_path = runs_dir / "event_spine.jsonl"
    frozen_dir = runs_dir / "frozen" / target_date

    if not tick_dir.exists():
        print(f"Error: tick dir not found: {tick_dir}", file=sys.stderr)
        return 1

    if not spine_path.exists():
        print(f"Error: spine not found: {spine_path}", file=sys.stderr)
        return 1

    frozen_dir.mkdir(parents=True, exist_ok=True)

    time_suffix = f"{start_time.strftime('%Y%m%dT%H%M%S')}_{end_time.strftime('%Y%m%dT%H%M%S')}"
    ticks_out = frozen_dir / f"ticks_{time_suffix}.jsonl"
    spine_out = frozen_dir / f"spine_{time_suffix}.jsonl"
    manifest_path = frozen_dir / "manifest.json"

    print("Slice Freeze")
    print("=" * 50)
    print(f"Date: {target_date}")
    print(f"Window: {start_time.isoformat()} to {end_time.isoformat()}")
    print(f"Output: {frozen_dir}")
    print()

    print("Filtering ticks...")
    tick_count = filter_ticks_by_time(tick_dir, start_time, end_time, ticks_out)
    print(f"  Ticks: {tick_count}")

    print("Filtering spine...")
    spine_count = filter_spine_by_time(spine_path, start_time, end_time, spine_out)
    print(f"  Spine events: {spine_count}")

    print("Computing hashes...")
    ticks_sha = sha256_file(ticks_out) if ticks_out.exists() else None
    spine_sha = sha256_file(spine_out) if spine_out.exists() else None

    manifest = {
        "frozen_at": datetime.now(timezone.utc).isoformat(),
        "date": target_date,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "ticks": {
            "path": str(ticks_out.name),
            "count": tick_count,
            "sha256": ticks_sha,
        },
        "spine": {
            "path": str(spine_out.name),
            "count": spine_count,
            "sha256": spine_sha,
        },
    }

    with manifest_path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, indent=2)

    print()
    print("Manifest:")
    print(json.dumps(manifest, indent=2))
    print()
    print(f"Written to: {manifest_path}")

    if tick_count == 0 and spine_count == 0:
        print("\nWarning: Empty slice (no data in window)")
        return 1

    print("\nSlice frozen successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
