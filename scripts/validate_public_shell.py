#!/usr/bin/env python3
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
EXPECTED_REPO_FILES = {
    ".github/workflows/ci.yml",
    "docs/public_shell_export_v1.md",
    "export_audit_report.json",
    "export_audit_report.md",
    "schemas/courts_index_v1.schema.json",
    "schemas/eval_manifest_v1.schema.json",
    "schemas/narrative_v0.schema.json",
    "scripts/validate_public_shell.py",
}
EXPORTED_PUBLIC_FILES = {
    "docs/public_shell_export_v1.md",
    "schemas/courts_index_v1.schema.json",
    "schemas/eval_manifest_v1.schema.json",
    "schemas/narrative_v0.schema.json",
}
SCHEMA_EXPECTATIONS = {
    "schemas/courts_index_v1.schema.json": {"title": "courts_index_v1"},
    "schemas/eval_manifest_v1.schema.json": {"$id": "eval_manifest_v1"},
    "schemas/narrative_v0.schema.json": {"$id": "narrative_v0"},
}
AUDIT_COUNT_KEYS = [
    "scanned_files",
    "exported_files",
    "blocked_files",
    "review_required_files",
    "skipped_files",
]
AUDIT_REQUIRED_KEYS = AUDIT_COUNT_KEYS + ["files", "classification_legend", "summary_line"]
DOC_REQUIRED_PHRASES = [
    "Public Shell Export v1",
    "Classification Meanings",
    "Exit Codes",
    "Manual Mirror-Ready Flow",
]


def _repo_files(root: Path) -> set[str]:
    files = set()
    for path in root.rglob("*"):
        if not path.is_file():
            continue
        rel = path.relative_to(root)
        if rel.parts and rel.parts[0] == ".git":
            continue
        files.add(rel.as_posix())
    return files


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


def main() -> int:
    repo_files = _repo_files(ROOT)
    _require(repo_files == EXPECTED_REPO_FILES, f"unexpected repo file set: {sorted(repo_files)}")

    docs_path = ROOT / "docs/public_shell_export_v1.md"
    docs_text = docs_path.read_text(encoding="utf-8")
    _require(docs_text.strip() != "", f"{docs_path} is empty")
    for phrase in DOC_REQUIRED_PHRASES:
        _require(phrase in docs_text, f"{docs_path} is missing required phrase: {phrase}")

    schema_payloads: dict[str, dict] = {}
    for rel_path, expectations in SCHEMA_EXPECTATIONS.items():
        path = ROOT / rel_path
        payload = json.loads(path.read_text(encoding="utf-8"))
        _require(isinstance(payload, dict), f"{rel_path} did not parse as a JSON object")
        _require(payload.get("type") == "object", f"{rel_path} must declare top-level type=object")
        _require(payload.get("export") == "public", f"{rel_path} must carry export=public")
        _require("$schema" in payload, f"{rel_path} is missing $schema")
        for key, expected in expectations.items():
            _require(payload.get(key) == expected, f"{rel_path} expected {key}={expected!r}")
        schema_payloads[rel_path] = payload

    audit_json_path = ROOT / "export_audit_report.json"
    audit = json.loads(audit_json_path.read_text(encoding="utf-8"))
    _require(isinstance(audit, dict), "export_audit_report.json must parse as a JSON object")
    for key in AUDIT_REQUIRED_KEYS:
        _require(key in audit, f"export_audit_report.json is missing key: {key}")
    for key in AUDIT_COUNT_KEYS:
        _require(isinstance(audit[key], int), f"export_audit_report.json key {key} must be an integer")

    files = audit["files"]
    _require(isinstance(files, list), "export_audit_report.json key files must be a list")
    _require(audit["scanned_files"] == len(files), "audit scanned_files does not match files list length")

    counts = Counter(item.get("classification") for item in files)
    blocked_total = counts["blocked_path"] + counts["blocked_metadata"] + counts["blocked_content"]
    _require(audit["exported_files"] == counts["exported"], "audit exported_files count mismatch")
    _require(audit["blocked_files"] == blocked_total, "audit blocked_files count mismatch")
    _require(audit["review_required_files"] == counts["review_required"], "audit review_required_files count mismatch")
    _require(audit["skipped_files"] == counts["skipped_unsupported"], "audit skipped_files count mismatch")

    audit_exported = {
        item["relative_path"]
        for item in files
        if item.get("classification") == "exported"
    }
    _require(audit_exported == EXPORTED_PUBLIC_FILES, f"audit exported file set mismatch: {sorted(audit_exported)}")

    audit_md_path = ROOT / "export_audit_report.md"
    audit_md = audit_md_path.read_text(encoding="utf-8")
    _require(audit_md.strip() != "", f"{audit_md_path} is empty")
    _require("# Public Shell Export Audit" in audit_md, f"{audit_md_path} is missing expected title")

    print(
        "validated_public_shell="
        f"repo_files:{len(repo_files)} "
        f"schemas:{len(schema_payloads)} "
        f"audit_exported:{len(audit_exported)} "
        f"scanned_files:{audit['scanned_files']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
