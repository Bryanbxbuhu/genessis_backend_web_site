from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class FeedSyncReporter:
    """Collect and persist feed/source health records for a sync run."""

    VALID_STATUSES = {"ok", "warning", "error", "skipped"}
    BASE_FIELDS = {
        "source_key": None,
        "name": None,
        "city_key": None,
        "primary_url": None,
        "fallback_url": None,
        "final_url_used": None,
        "http_status_code": None,
        "content_type": None,
        "total_entries_parsed": 0,
        "items_returned_after_filtering": 0,
        "used_fallback": False,
        "status": "skipped",
        "error_message": None,
    }

    def __init__(self, output_dir: str | Path = "reports/feed_health") -> None:
        self.output_dir = Path(output_dir)
        self.started_at = datetime.now(timezone.utc)
        self.records: list[dict[str, Any]] = []

    def add_record(self, record: dict[str, Any]) -> None:
        normalized = dict(self.BASE_FIELDS)
        normalized.update(record or {})

        status = str(normalized.get("status") or "error").strip().lower()
        if status not in self.VALID_STATUSES:
            normalized["status"] = "error"
            if not normalized.get("error_message"):
                normalized["error_message"] = f"invalid_status:{status}"
        else:
            normalized["status"] = status

        normalized["used_fallback"] = bool(normalized.get("used_fallback", False))

        for key in ("total_entries_parsed", "items_returned_after_filtering"):
            value = normalized.get(key, 0)
            try:
                normalized[key] = int(value or 0)
            except Exception:
                normalized[key] = 0

        http_code = normalized.get("http_status_code")
        if http_code in ("", None):
            normalized["http_status_code"] = None
        else:
            try:
                normalized["http_status_code"] = int(http_code)
            except Exception:
                normalized["http_status_code"] = None

        if normalized.get("error_message") is not None:
            normalized["error_message"] = str(normalized["error_message"])

        self.records.append(normalized)

    def status_counts(self) -> dict[str, int]:
        counts = Counter(record.get("status", "error") for record in self.records)
        return {status: int(counts.get(status, 0)) for status in ("ok", "warning", "error", "skipped")}

    def write_reports(self) -> tuple[Path, Path]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        timestamp = self.started_at.strftime("%Y%m%d_%H%M%S")
        json_path = self.output_dir / f"feed_health_{timestamp}.json"
        md_path = self.output_dir / f"feed_health_{timestamp}.md"

        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "started_at": self.started_at.isoformat(),
            "total_sources": len(self.records),
            "counts": self.status_counts(),
            "records": self.records,
        }

        json_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        md_path.write_text(self._render_markdown(payload), encoding="utf-8")
        return json_path, md_path

    def print_console_summary(self, *, max_failures: int = 10) -> None:
        counts = self.status_counts()
        print(
            "\nFeed health summary: "
            f"ok={counts['ok']} warning={counts['warning']} error={counts['error']} skipped={counts['skipped']}"
        )
        failures = [r for r in self.records if r.get("status") in {"warning", "error"}]
        if not failures:
            return

        print(f"First {min(max_failures, len(failures))} failing sources:")
        for record in failures[:max_failures]:
            city_key = record.get("city_key") or "global"
            source_key = record.get("source_key") or "unknown_source"
            status = record.get("status") or "error"
            message = record.get("error_message") or "no details"
            print(f" - [{status}] {city_key}:{source_key} -> {message}")

    def _render_markdown(self, payload: dict[str, Any]) -> str:
        counts = payload.get("counts", {})
        lines = [
            "# Feed Sync Health Report",
            "",
            f"- Generated at: {payload.get('generated_at')}",
            f"- Started at: {payload.get('started_at')}",
            f"- Total sources: {payload.get('total_sources', 0)}",
            (
                "- Status counts: "
                f"ok={counts.get('ok', 0)}, "
                f"warning={counts.get('warning', 0)}, "
                f"error={counts.get('error', 0)}, "
                f"skipped={counts.get('skipped', 0)}"
            ),
            "",
        ]

        for record in self.records:
            source_key = record.get("source_key") or "unknown_source"
            name = record.get("name") or "Unnamed source"
            status = record.get("status") or "error"
            city_key = record.get("city_key") or "global"

            lines.append(f"## {source_key} ({status})")
            lines.append(f"- Name: {name}")
            lines.append(f"- City: {city_key}")
            lines.append(f"- Primary URL: {record.get('primary_url') or 'n/a'}")
            lines.append(f"- Fallback URL: {record.get('fallback_url') or 'n/a'}")
            lines.append(f"- Final URL used: {record.get('final_url_used') or 'n/a'}")
            lines.append(f"- HTTP status: {record.get('http_status_code') if record.get('http_status_code') is not None else 'n/a'}")
            lines.append(f"- Content type: {record.get('content_type') or 'n/a'}")
            lines.append(f"- Total entries parsed: {record.get('total_entries_parsed', 0)}")
            lines.append(
                "- Items returned after filtering: "
                f"{record.get('items_returned_after_filtering', 0)}"
            )
            lines.append(f"- Used fallback: {bool(record.get('used_fallback', False))}")
            lines.append(f"- Error: {record.get('error_message') or 'n/a'}")
            lines.append("")

        return "\n".join(lines).strip() + "\n"
