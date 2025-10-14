from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List


def normalize_column_names(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """Return rows with lower snake_case column names."""

    normalized: List[Dict[str, object]] = []
    for row in rows:
        normalized.append({key.strip().lower().replace(" ", "_"): value for key, value in row.items()})
    return normalized


def coerce_dates(rows: List[Dict[str, object]], columns: Iterable[str]) -> List[Dict[str, object]]:
    """Ensure specified columns are parsed as datetimes."""

    coerced: List[Dict[str, object]] = []
    for row in rows:
        updated = dict(row)
        for column in columns:
            value = updated.get(column)
            if value in ("", None):
                updated[column] = None
            else:
                try:
                    updated[column] = datetime.fromisoformat(str(value))
                except ValueError:
                    updated[column] = None
        coerced.append(updated)
    return coerced


def add_customer_lifetime_value(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """Compute an estimated customer lifetime value (CLV)."""

    enriched: List[Dict[str, object]] = []
    for row in rows:
        updated = dict(row)
        total_spent = float(updated.get("total_spent", 0) or 0)
        total_purchases = int(updated.get("total_purchases", 0) or 0)
        avg_purchase_value = 0.0
        if total_purchases > 0:
            avg_purchase_value = total_spent / total_purchases
        updated["avg_purchase_value"] = avg_purchase_value
        updated["estimated_clv"] = avg_purchase_value * total_purchases * 1.5
        enriched.append(updated)
    return enriched


def convert_types(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """Convert numerical columns to the appropriate Python types."""

    converted: List[Dict[str, object]] = []
    for row in rows:
        updated = dict(row)
        updated["customer_id"] = int(updated.get("customer_id", 0) or 0)
        updated["total_purchases"] = int(updated.get("total_purchases", 0) or 0)
        updated["total_spent"] = float(updated.get("total_spent", 0) or 0)
        converted.append(updated)
    return converted


def deduplicate(rows: List[Dict[str, object]], key: str) -> List[Dict[str, object]]:
    """Remove duplicate entries based on the provided key."""

    seen = set()
    unique_rows: List[Dict[str, object]] = []
    for row in rows:
        identifier = row.get(key)
        if identifier in seen:
            continue
        seen.add(identifier)
        unique_rows.append(row)
    return unique_rows


def clean_data(rows: List[Dict[str, object]]) -> List[Dict[str, object]]:
    """Apply a standard set of cleaning and enrichment steps."""

    rows = normalize_column_names(rows)
    rows = convert_types(rows)
    rows = deduplicate(rows, "customer_id")
    rows = coerce_dates(rows, ["signup_date", "last_login"])
    rows = add_customer_lifetime_value(rows)
    return rows
