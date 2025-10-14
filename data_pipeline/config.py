from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class PipelineConfig:
    """Configuration options for the data pipeline."""

    raw_data_path: Path = field(default_factory=lambda: Path("data/raw/sample_data.csv"))
    processed_data_path: Path = field(default_factory=lambda: Path("data/processed/processed_data.csv"))
    report_path: Path = field(default_factory=lambda: Path("reports/summary_report.json"))

    def ensure_directories(self) -> None:
        """Ensure that output directories exist."""

        self.processed_data_path.parent.mkdir(parents=True, exist_ok=True)
        self.report_path.parent.mkdir(parents=True, exist_ok=True)


DEFAULT_CONFIG = PipelineConfig()
