#!/usr/bin/env python3
"""
Data Inventory - report data availability for a time window.
"""

import argparse
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

DEFAULT_DATA_ROOT = "/root/synthdesk-listener/runs/0.2.0"
LAYER_FILES = {
    "ticks": "tick_observation.jsonl",
    "orderbook": "orderbook_observation.jsonl",
}


def parse_iso_utc(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def atomic_write_json(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    with tmp_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    tmp_path.replace(path)


def iter_jsonl(path: Path) -> Iterable[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            try:
                yield json.loads(text)
            except json.JSONDecodeError:
                continue


def epoch_to_utc(value: float) -> datetime:
    if value > 1e12:
        value = value / 1000.0
    return datetime.fromtimestamp(value, tz=timezone.utc)


def extract_timestamp(record: Dict[str, Any]) -> Optional[datetime]:
    for key in ("ts_utc", "timestamp", "ts", "receipt_ts"):
        if key in record and record[key] is not None:
            value = record[key]
            if isinstance(value, (int, float)):
                return epoch_to_utc(float(value))
            if isinstance(value, str):
                try:
                    return parse_iso_utc(value)
                except ValueError:
                    return None
    for key in ("ts_ingest_ms", "ts_exchange_ms", "ts_ms"):
        if key in record and record[key] is not None:
            return epoch_to_utc(float(record[key]))
    return None


def iter_day_paths(data_root: Path, start_utc: datetime, end_utc: datetime, filename: str) -> List[Path]:
    paths = []
    current = start_utc.date()
    end_date = end_utc.date()
    while current <= end_date:
        day_dir = data_root / current.strftime("%Y-%m-%d")
        path = day_dir / filename
        if path.exists():
            paths.append(path)
        current += timedelta(days=1)
    return paths


def scan_timestamps(paths: Iterable[Path]) -> Tuple[Optional[datetime], Optional[datetime], int]:
    first_ts = None
    last_ts = None
    count = 0
    for path in paths:
        for record in iter_jsonl(path):
            ts = extract_timestamp(record)
            if ts is None:
                continue
            count += 1
            if first_ts is None or ts < first_ts:
                first_ts = ts
            if last_ts is None or ts > last_ts:
                last_ts = ts
    return first_ts, last_ts, count


def build_layer_report(paths: List[Path]) -> Dict[str, Any]:
    if not paths:
        return {
            "available": False,
            "coverage_pct": 0.0,
            "first": None,
            "last": None,
            "missing_segments": [],
        }

    first_ts, last_ts, count = scan_timestamps(paths)
    available = count > 0
    coverage_pct = 100.0 if available else 0.0

    return {
        "available": available,
        "coverage_pct": coverage_pct,
        "first": first_ts.isoformat() if first_ts else None,
        "last": last_ts.isoformat() if last_ts else None,
        "missing_segments": [],
    }


def run_inventory(start_utc: datetime, end_utc: datetime, data_root: Path, layers: List[str]) -> Dict[str, Any]:
    layer_reports: Dict[str, Any] = {}

    for layer in layers:
        if layer == "perception":
            spine_path = data_root / "event_spine.jsonl"
            paths = [spine_path] if spine_path.exists() else []
            layer_reports[layer] = build_layer_report(paths)
        elif layer in LAYER_FILES:
            paths = iter_day_paths(data_root, start_utc, end_utc, LAYER_FILES[layer])
            layer_reports[layer] = build_layer_report(paths)
        else:
            layer_reports[layer] = {
                "available": False,
                "coverage_pct": 0.0,
                "first": None,
                "last": None,
                "missing_segments": [],
                "note": "unknown layer",
            }

    return {
        "window": {
            "start_utc": start_utc.isoformat(),
            "end_utc": end_utc.isoformat(),
        },
        "layers": layer_reports,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Report data availability for a UTC window.")
    parser.add_argument("--start", required=True, help="Window start (UTC ISO-8601)")
    parser.add_argument("--end", required=True, help="Window end (UTC ISO-8601)")
    parser.add_argument("--layers", default="ticks,perception,orderbook", help="Comma-separated layers")
    parser.add_argument("--data-root", default=DEFAULT_DATA_ROOT, help="Data root path")
    parser.add_argument("--output", help="Write JSON output to file")
    args = parser.parse_args()

    try:
        start_utc = parse_iso_utc(args.start)
        end_utc = parse_iso_utc(args.end)
    except ValueError as exc:
        print(f"Error parsing timestamps: {exc}", file=sys.stderr)
        return 1

    if end_utc <= start_utc:
        print("Error: end time must be after start time", file=sys.stderr)
        return 1

    layers = [layer.strip() for layer in args.layers.split(",") if layer.strip()]
    data_root = Path(args.data_root)

    inventory = run_inventory(start_utc, end_utc, data_root, layers)
    missing_layers = [
        layer for layer, report in inventory["layers"].items()
        if not report.get("available")
    ]
    if missing_layers and len(missing_layers) == len(layers):
        print(f"No data available for layers: {', '.join(missing_layers)}", file=sys.stderr)
        exit_code = 2
    else:
        exit_code = 0
        for layer in missing_layers:
            print(f"Warning: no data for layer {layer}", file=sys.stderr)

    if args.output:
        atomic_write_json(Path(args.output), inventory)
    else:
        print(json.dumps(inventory, indent=2))

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
