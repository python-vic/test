from __future__ import annotations

from statistics import mean
from typing import Dict, Iterable, List


def summarize_purchases(rows: List[Dict[str, object]]) -> Dict[str, float]:
    """Produce high-level metrics about customer purchases."""

    total_customers = float(len({row["customer_id"] for row in rows}))
    purchases = [float(row["total_purchases"]) for row in rows]
    spent = [float(row["total_spent"]) for row in rows]
    conversion_rate = float(sum(1 for value in purchases if value > 0) / len(purchases)) if purchases else 0.0
    return {
        "total_customers": total_customers,
        "average_purchases": mean(purchases) if purchases else 0.0,
        "average_spent": mean(spent) if spent else 0.0,
        "conversion_rate": conversion_rate,
    }


def summarize_recent_activity(rows: List[Dict[str, object]]) -> Dict[str, str]:
    """Identify the most recent signup and login dates."""

    signup_dates = [row["signup_date"] for row in rows if row.get("signup_date")]
    login_dates = [row["last_login"] for row in rows if row.get("last_login")]

    latest_signup = max(signup_dates).isoformat() if signup_dates else ""
    latest_login = max(login_dates).isoformat() if login_dates else ""
    return {"latest_signup": latest_signup, "latest_login": latest_login}


def _column_values(rows: List[Dict[str, object]], column: str) -> List[float]:
    return [float(row[column]) for row in rows]


def _pearson_correlation(xs: Iterable[float], ys: Iterable[float]) -> float:
    xs_list = list(xs)
    ys_list = list(ys)
    n = len(xs_list)
    if n == 0 or n != len(ys_list):
        return 0.0

    mean_x = sum(xs_list) / n
    mean_y = sum(ys_list) / n
    numerator = sum((x - mean_x) * (y - mean_y) for x, y in zip(xs_list, ys_list))
    denominator_x = sum((x - mean_x) ** 2 for x in xs_list)
    denominator_y = sum((y - mean_y) ** 2 for y in ys_list)
    if denominator_x == 0 or denominator_y == 0:
        return 0.0
    return numerator / (denominator_x ** 0.5 * denominator_y ** 0.5)


def correlation_matrix(rows: List[Dict[str, object]], columns: List[str]) -> Dict[str, Dict[str, float]]:
    """Return a correlation matrix for the specified columns."""

    matrix: Dict[str, Dict[str, float]] = {}
    for column_x in columns:
        matrix[column_x] = {}
        values_x = _column_values(rows, column_x)
        for column_y in columns:
            values_y = _column_values(rows, column_y)
            matrix[column_x][column_y] = _pearson_correlation(values_x, values_y)
    return matrix


def build_report(rows: List[Dict[str, object]]) -> Dict[str, object]:
    """Generate a comprehensive analysis report."""

    return {
        "purchases": summarize_purchases(rows),
        "recent_activity": summarize_recent_activity(rows),
        "correlations": correlation_matrix(rows, ["total_purchases", "total_spent", "estimated_clv"]),
    }
