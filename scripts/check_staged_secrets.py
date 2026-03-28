#!/usr/bin/env python3
"""Scan only staged git index content for secrets using gitleaks.

This script materializes staged blobs from the git index into a temporary
snapshot directory under .git/local-security-reports, scans that snapshot with
gitleaks in redacted mode, and fails with a non-zero exit code on findings.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import sys
import tempfile
import os
import re
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Iterable


REPO_ROOT = Path(__file__).resolve().parents[1]
GIT_DIR = REPO_ROOT / ".git"
REPORTS_ROOT = GIT_DIR / "local-security-reports"

FALLBACK_RULES: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("private_key_block", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |DSA )?PRIVATE KEY-----")),
    ("aws_access_key_id", re.compile(r"AKIA[0-9A-Z]{16}")),
    ("github_pat", re.compile(r"github_pat_[A-Za-z0-9_]{20,}")),
    ("github_token", re.compile(r"ghp_[A-Za-z0-9]{36}")),
    ("slack_token", re.compile(r"xox[baprs]-[A-Za-z0-9-]{10,}")),
    ("supabase_db_secret", re.compile(r"sb_secret_[A-Za-z0-9_\-]{16,}")),
    ("openai_key", re.compile(r"sk-[A-Za-z0-9]{20,}")),
)


def _is_forbidden_env_file(git_path: str) -> bool:
    name = PurePosixPath(git_path).name
    if name == ".env" or name == ".envrc":
        return True
    if name.startswith(".env.") and name != ".env.example":
        return True
    return False


def _detect_forbidden_staged_files(staged_files: Iterable[str]) -> list[str]:
    return [git_path for git_path in staged_files if _is_forbidden_env_file(git_path)]


def _resolve_gitleaks_executable() -> str | None:
    direct = shutil.which("gitleaks")
    if direct:
        return direct

    local_app_data = os.getenv("LOCALAPPDATA")
    if not local_app_data:
        return None

    package_root = Path(local_app_data) / "Microsoft" / "WinGet" / "Packages"
    candidates = list(package_root.glob("Gitleaks.Gitleaks_*/*gitleaks.exe"))
    if candidates:
        # Pick the most recently modified candidate.
        candidates.sort(key=lambda p: p.stat().st_mtime, reverse=True)
        return str(candidates[0])
    return None


def _run(cmd: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        check=check,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )


def _run_bytes(cmd: list[str], cwd: Path, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(
        cmd,
        cwd=str(cwd),
        check=check,
        capture_output=True,
    )


def _staged_files() -> list[str]:
    result = _run(
        ["git", "diff", "--cached", "--name-only", "--diff-filter=ACMR"],
        cwd=REPO_ROOT,
    )
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _safe_relative_path(git_path: str) -> Path:
    rel = PurePosixPath(git_path)
    if rel.is_absolute() or any(part in ("..", "") for part in rel.parts):
        raise ValueError(f"Unsafe staged path: {git_path}")
    return Path(*rel.parts)


def _materialize_index_snapshot(staged_files: Iterable[str], snapshot_dir: Path) -> int:
    written = 0
    for git_path in staged_files:
        rel_path = _safe_relative_path(git_path)
        try:
            blob = _run_bytes(["git", "show", f":{git_path}"], cwd=REPO_ROOT).stdout
        except subprocess.CalledProcessError:
            # If a path cannot be read from index (rare edge case), skip it.
            continue

        destination = snapshot_dir / rel_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(blob)
        written += 1
    return written


def _load_findings_count(report_path: Path) -> int:
    if not report_path.exists() or report_path.stat().st_size == 0:
        return 0
    try:
        data = json.loads(report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return 0
    if isinstance(data, list):
        return len(data)
    return 0


def _fallback_scan(snapshot_dir: Path) -> list[dict[str, str]]:
    findings: list[dict[str, str]] = []
    for file_path in snapshot_dir.rglob("*"):
        if not file_path.is_file():
            continue
        try:
            content = file_path.read_text(encoding="utf-8", errors="ignore")
        except OSError:
            continue

        rel = file_path.relative_to(snapshot_dir).as_posix()
        for rule_name, rule in FALLBACK_RULES:
            if rule.search(content):
                findings.append({"file": rel, "rule": rule_name})
    return findings


def main() -> int:
    if not GIT_DIR.exists():
        print("ERROR: .git directory not found. Run this from inside the repository.")
        return 2

    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    report_dir = REPORTS_ROOT / f"staged-scan-{timestamp}"
    report_dir.mkdir(parents=True, exist_ok=True)

    staged_files = _staged_files()
    if not staged_files:
        print("No staged files to scan.")
        return 0

    snapshot_dir_path = Path(
        tempfile.mkdtemp(prefix="staged-snapshot-", dir=str(REPORTS_ROOT))
    )
    report_path = report_dir / "gitleaks-staged.json"
    stdout_path = report_dir / "gitleaks.stdout.log"
    stderr_path = report_dir / "gitleaks.stderr.log"
    summary_path = report_dir / "summary.json"
    fallback_report_path = report_dir / "fallback-findings.json"
    forbidden_files_report_path = report_dir / "forbidden-staged-files.json"

    findings = 0
    try:
        files_written = _materialize_index_snapshot(staged_files, snapshot_dir_path)
        forbidden_files = _detect_forbidden_staged_files(staged_files)

        gitleaks_exe = _resolve_gitleaks_executable()
        if not gitleaks_exe:
            print("ERROR: gitleaks is not installed or not discoverable in this environment.")
            print("Install it first, then rerun the scan.")
            return 2

        command = [
            gitleaks_exe,
            "dir",
            str(snapshot_dir_path),
            "--report-format",
            "json",
            "--report-path",
            str(report_path),
            "--redact",
            "--no-banner",
            "--exit-code",
            "1",
        ]

        config_path = REPO_ROOT / ".gitleaks.toml"
        if config_path.exists():
            command.extend(["--config", str(config_path)])

        proc = _run(command, cwd=REPO_ROOT, check=False)

        stdout_path.write_text(proc.stdout or "", encoding="utf-8")
        stderr_path.write_text(proc.stderr or "", encoding="utf-8")

        gitleaks_findings = _load_findings_count(report_path)
        fallback_findings = _fallback_scan(snapshot_dir_path)
        fallback_report_path.write_text(
            json.dumps(fallback_findings, indent=2),
            encoding="utf-8",
        )
        forbidden_files_report_path.write_text(
            json.dumps(forbidden_files, indent=2),
            encoding="utf-8",
        )

        findings = gitleaks_findings + len(fallback_findings) + len(forbidden_files)
        summary_path.write_text(
            json.dumps(
                {
                    "scan": "staged",
                    "timestamp_utc": timestamp,
                    "staged_files_count": len(staged_files),
                    "materialized_files_count": files_written,
                    "findings_count": findings,
                    "gitleaks_findings_count": gitleaks_findings,
                    "fallback_findings_count": len(fallback_findings),
                    "forbidden_staged_files_count": len(forbidden_files),
                    "report_path": str(report_path),
                    "fallback_report_path": str(fallback_report_path),
                    "forbidden_files_report_path": str(forbidden_files_report_path),
                    "used_config": str(config_path) if config_path.exists() else "default",
                    "exit_code": proc.returncode,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

        if findings > 0:
            print("Secret scan failed: staged content contains potential secrets.")
            print(f"Findings: {findings}")
            print(f"Redacted report: {report_path}")
            print(f"Fallback report: {fallback_report_path}")
            if forbidden_files:
                print("Forbidden staged env files:")
                for git_path in forbidden_files:
                    print(f"  - {git_path}")
                print(f"Forbidden file report: {forbidden_files_report_path}")
            return 1

        if proc.returncode not in (0, 1):
            print("ERROR: gitleaks execution failed.")
            print(f"Check logs: {stderr_path}")
            return 2

        print("Staged secret scan passed.")
        print(f"Scanned staged files: {len(staged_files)}")
        print(f"Report: {report_path}")
        return 0
    finally:
        shutil.rmtree(snapshot_dir_path, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
