"""
Lightweight coverage tracker for report generation.

Provides minimal structures required by report_loader and travel_agent:
- CoverageStatus enum
- SectionCoverage objects with status and reason
- ReportCoverage with section attributes and summary_message
"""
from __future__ import annotations
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict


class CoverageStatus(Enum):
    OK = "ok"
    PARTIAL = "partial"
    MISSING = "missing"


@dataclass
class SectionCoverage:
    status: CoverageStatus
    count: int
    min_required: int
    reason: str = ""


@dataclass
class ReportCoverage:
    sections: Dict[str, SectionCoverage] = field(default_factory=dict)

    def _get(self, name: str) -> SectionCoverage:
        return self.sections.get(name) or SectionCoverage(
            status=CoverageStatus.MISSING, count=0, min_required=1, reason="not evaluated"
        )

    @property
    def medical_facilities(self) -> SectionCoverage:
        return self._get("medical_facilities")

    @property
    def transit(self) -> SectionCoverage:
        return self._get("transit")

    @property
    def weather(self) -> SectionCoverage:
        return self._get("weather")

    @property
    def incidents(self) -> SectionCoverage:
        return self._get("incidents")

    @property
    def supplies(self) -> SectionCoverage:
        return self._get("supplies")

    @property
    def summary_message(self) -> str:
        if not self.sections:
            return "Coverage not evaluated"
        missing = [name for name, info in self.sections.items() if info.status != CoverageStatus.OK]
        if not missing:
            return "✅ Coverage complete"
        missing_list = ", ".join(sorted(missing))
        return f"⚠️ Partial coverage: missing {missing_list}"


def validate_section_coverage(
    coverage: ReportCoverage,
    section_name: str,
    items,
    *,
    min_required: int = 1,
) -> None:
    count = len(items) if items is not None else 0
    if count >= min_required:
        status = CoverageStatus.OK
        reason = ""
    elif count == 0:
        status = CoverageStatus.MISSING
        reason = "no data"
    else:
        status = CoverageStatus.PARTIAL
        reason = f"{count}/{min_required} items"

    section_cov = SectionCoverage(
        status=status,
        count=count,
        min_required=min_required,
        reason=reason,
    )
    coverage.sections[section_name] = section_cov