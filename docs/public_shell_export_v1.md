---
export: public
---

# Public Shell Export v1

## Why This Exists

The public shell is generated from the private source-of-truth repo. Work stays in `synthdesk/synthdesk`; the mirror is a derived artifact produced by a local exporter that fails closed by default.

## Model

The exporter applies four policy layers in order:

1. Path policy decides whether a file is allowlisted, denied, or in a review bucket.
2. Metadata policy handles explicit per-file tags for review paths.
3. Transform policy chooses how approved text is projected into the mirror.
4. Content policy scans the projected text and blocks matches against a conservative deny list.

The default posture is block unless a file is clearly approved.

## Supported Tags

Use one of these explicit tags near the top of a text file:

- `export: public`
- `export: review`
- `export: private`

Supported forms in v1:

- Markdown frontmatter
- YAML top-level metadata
- TOML top-level metadata
- JSON top-level metadata
- Hash-comment headers for Python and shell files

Review semantics are explicit in v1:

- Review path with `export: public`: eligible for export if path, transform, and content checks pass.
- Review path with `export: review`: reported as `review_required` and not exported.
- Review path with no required tag: reported as `blocked_metadata`.
- Any path with `export: private`: reported as `blocked_metadata`.

## Commands

Dry run:

```bash
python3 scripts/export_public_shell.py --dry-run
```

Materialize a local mirror projection:

```bash
python3 scripts/export_public_shell.py --output-dir /tmp/synthdesk-public-shell
```

Materialize after clearing an existing target directory:

```bash
python3 scripts/export_public_shell.py --output-dir /tmp/synthdesk-public-shell --clean-output-dir
```

The policy file defaults to `public_export_policy.yaml`. Add `--verbose` for one-line per-file classification output.
When `--clean-output-dir` is used, the exporter only clears the chosen target directory and preserves the target clone’s `.git` metadata.

## Classification Meanings

- `exported`: all policy layers passed and the file was exported, or would be exported in dry-run mode.
- `blocked_path`: the path is denied or did not match any allow or review rule.
- `blocked_metadata`: metadata policy blocked the file, including review-path files missing `export: public`.
- `blocked_content`: content scan matched a deny pattern after transforms.
- `review_required`: the file carries explicit `export: review` and is reported without export.
- `skipped_unsupported`: the file could not be exported safely in v1, such as binary input, symlink, or unsupported transform.

## Audit Output

Each run writes:

- `export_audit_report.json`
- `export_audit_report.md`

The JSON report is the full machine-readable audit surface. For each scanned file it records:

- relative path
- classification
- matched policy buckets
- metadata tag
- first deny match, if any
- transform applied, if any
- note explaining the decision

Top-level counts summarize scanned, exported, blocked, review-required, and skipped files.
The Markdown audit also includes the classification legend, cleanup flags, and the success summary line.

## Exit Codes

- `0`: successful run, including successful dry-run, even when many files are blocked or skipped.
- `2`: configuration, policy, CLI, or cleanup-safety error.
- `3`: internal exporter failure such as an unexpected filesystem error during execution.

## v1 Limits

- The deny list is intentionally conservative and may block harmless text.
- Only UTF-8 text is scanned and exported.
- Binary files and symlinks are blocked or skipped.
- `copy_as_is` is implemented fully.
- `redact_hostnames` is implemented for text files.
- `require_synthetic_only` is recognized but intentionally blocked until there is a stronger contract for proving synthetic inputs.
- `--clean-output-dir` is intentionally conservative and refuses dangerous targets such as repo-root, home, root-level paths, and paths inside the private repo.
- `--clean-output-dir` preserves `.git` in the target directory so a local public-shell clone stays a git repo after materialization.
- No remote sync or public push automation is included in v1.

## Manual Mirror-Ready Flow

Recommended first local materialization flow for a fresh public-shell clone:

```bash
git clone https://github.com/synthdesk/synthdesk-public-shell.git /tmp/synthdesk-public-shell
python3 scripts/export_public_shell.py --output-dir /tmp/synthdesk-public-shell --clean-output-dir
git -C /tmp/synthdesk-public-shell status --short
```

Recommended initial shape of that clone after materialization:

- exported files at repo root, preserving their relative paths
- `export_audit_report.json`
- `export_audit_report.md`

No `.gitignore` is required for v1 if the audit artifacts are meant to stay committed alongside the exported surface. No push step is included here.

## Safe Next Steps

- Add a small set of intentionally public docs or fixtures under allowlisted paths.
- Add a private-repo workflow that refreshes a fresh staging directory and audits it before any mirror update.
- Add richer redactions only after each rule has a clear proof surface in the audit report.
