from __future__ import annotations

import csv
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from data_pipeline import PipelineConfig
from scripts.run_pipeline import run_pipeline


def test_run_pipeline_creates_outputs(tmp_path: Path) -> None:
    raw = tmp_path / "raw.csv"
    processed = tmp_path / "processed.csv"
    report = tmp_path / "report.json"

    raw.write_text(
        """customer_id,signup_date,last_login,total_purchases,total_spent
1,2023-01-05,2023-04-01,5,250.50
2,2023-02-10,2023-04-03,2,120.00
3,2023-02-22,2023-03-27,7,560.30
"""
    )

    config = PipelineConfig(raw_data_path=raw, processed_data_path=processed, report_path=report)

    run_pipeline(config)

    with processed.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        processed_rows = list(reader)

    assert processed_rows and "estimated_clv" in processed_rows[0]
    assert len(processed_rows) == 3

    report_data = json.loads(report.read_text())
    assert report_data["purchases"]["total_customers"] == 3.0
    assert set(report_data["correlations"]) == {"total_purchases", "total_spent", "estimated_clv"}
