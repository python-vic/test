from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Dict, Iterable


def save_rows(rows: Iterable[Dict[str, object]], path: Path) -> None:
    """Persist tabular data to CSV."""

    rows = list(rows)
    if not rows:
        raise ValueError("No rows provided to save")

    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def save_report(report: dict, path: Path) -> None:
    """Persist the analysis report as JSON."""

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2))
