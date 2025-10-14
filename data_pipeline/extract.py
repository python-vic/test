from __future__ import annotations

import csv
from pathlib import Path
from typing import Dict, List


def extract_csv(path: Path) -> List[Dict[str, str]]:
    """Load a CSV file into a list of dictionaries."""

    with path.open(newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        return [dict(row) for row in reader]
