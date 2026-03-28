#!/usr/bin/env python3
"""Run a full git history secret scan with gitleaks in redacted mode."""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
GIT_DIR = REPO_ROOT / ".git"
REPORTS_ROOT = GIT_DIR / "local-security-reports"


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


def main() -> int:
    if not GIT_DIR.exists():
        print("ERROR: .git directory not found. Run this from inside the repository.")
        return 2

    REPORTS_ROOT.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    report_dir = REPORTS_ROOT / f"history-scan-{timestamp}"
    report_dir.mkdir(parents=True, exist_ok=True)

    report_path = report_dir / "gitleaks-history.json"
    stdout_path = report_dir / "gitleaks.stdout.log"
    stderr_path = report_dir / "gitleaks.stderr.log"
    summary_path = report_dir / "summary.json"

    gitleaks_exe = _resolve_gitleaks_executable()
    if not gitleaks_exe:
        print("ERROR: gitleaks is not installed or not discoverable in this environment.")
        print("Install it first, then rerun the scan.")
        return 2

    command = [
        gitleaks_exe,
        "git",
        str(REPO_ROOT),
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

    findings = _load_findings_count(report_path)
    summary_path.write_text(
        json.dumps(
            {
                "scan": "history",
                "timestamp_utc": timestamp,
                "findings_count": findings,
                "report_path": str(report_path),
                "used_config": str(config_path) if config_path.exists() else "default",
                "exit_code": proc.returncode,
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    if proc.returncode == 1 and findings > 0:
        print("History secret scan failed: repository history contains potential secrets.")
        print(f"Findings: {findings}")
        print(f"Redacted report: {report_path}")
        return 1

    if proc.returncode not in (0, 1):
        print("ERROR: gitleaks execution failed.")
        print(f"Check logs: {stderr_path}")
        return 2

    print("History secret scan passed.")
    print(f"Report: {report_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
