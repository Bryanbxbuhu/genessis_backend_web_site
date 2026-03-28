#!/usr/bin/env python3
import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _coerce_object(value: Any, *, field_name: str, city_key: str) -> Dict[str, Any]:
    if isinstance(value, dict):
        return value
    if value is None:
        return {}
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"{city_key}: {field_name} is a non-JSON string and cannot be exported as an object"
            ) from exc
        if not isinstance(parsed, dict):
            raise ValueError(
                f"{city_key}: {field_name} must decode to a JSON object, got {type(parsed).__name__}"
            )
        return parsed
    raise ValueError(f"{city_key}: {field_name} must be a JSON object, got {type(value).__name__}")


def normalize_city_report_row(row: Dict[str, Any]) -> Dict[str, Any]:
    city_key = str(row.get("city_key") or "").strip() or "<unknown-city>"
    normalized = dict(row)
    normalized["report_data"] = _coerce_object(
        row.get("report_data"),
        field_name="report_data",
        city_key=city_key,
    )
    normalized["trend_data"] = _coerce_object(
        row.get("trend_data"),
        field_name="trend_data",
        city_key=city_key,
    )
    return normalized


def normalize_city_report_rows(rows: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, dict):
            raise ValueError(f"Expected dict row, got {type(row).__name__}")
        normalized.append(normalize_city_report_row(row))
    return normalized


def validate_export_payload(payload: Any) -> None:
    if not isinstance(payload, list):
        raise ValueError("Export payload must be a list")
    for row in payload:
        if not isinstance(row, dict):
            raise ValueError("Each export row must be an object")
        city_key = str(row.get("city_key") or "").strip() or "<unknown-city>"
        for field_name in ("report_data", "trend_data"):
            field_value = row.get(field_name)
            if not isinstance(field_value, dict):
                raise ValueError(f"{city_key}: {field_name} must be a JSON object after export")


def round_trip_validate_file(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    validate_export_payload(payload)


def fetch_city_reports() -> List[Dict[str, Any]]:
    from storage.supabase_store import SupabaseStore

    store = SupabaseStore()
    response = store.client.table("city_reports").select("*").order("city_key").execute()
    rows = response.data or []
    if not isinstance(rows, list):
        raise RuntimeError("Unexpected response from city_reports query")
    return [row for row in rows if isinstance(row, dict)]


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Export city_reports rows while preserving report_data and trend_data as JSON objects."
        )
    )
    parser.add_argument(
        "--output",
        default="reports/city_reports_export.json",
        help="destination JSON file path",
    )
    parser.add_argument(
        "--skip-round-trip",
        action="store_true",
        help="skip post-export round-trip JSON-object validation",
    )
    args = parser.parse_args()

    output_path = Path(args.output).expanduser().resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    rows = fetch_city_reports()
    normalized_rows = normalize_city_report_rows(rows)
    validate_export_payload(normalized_rows)

    serialized = json.dumps(normalized_rows, ensure_ascii=False, indent=2)
    if not serialized.endswith("\n"):
        serialized += "\n"
    output_path.write_text(serialized, encoding="utf-8")
    print(f"Exported {len(normalized_rows)} city_reports rows to {output_path}")

    if not args.skip_round_trip:
        round_trip_validate_file(output_path)
        print("Round-trip validation passed: report_data and trend_data are JSON objects.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
