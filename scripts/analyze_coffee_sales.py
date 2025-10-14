from __future__ import annotations

import csv
from dataclasses import dataclass
from math import sqrt
from pathlib import Path
from statistics import mean, median
from typing import Iterable, List


DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "coffee_sales.csv"
OUTPUT_PATH = Path(__file__).resolve().parents[1] / "outputs" / "coffee_sales_summary.md"


@dataclass
class MonthlyRecord:
    month: str
    marketing_spend: float
    online_sales: float
    in_store_sales: float

    @property
    def total_sales(self) -> float:
        return self.online_sales + self.in_store_sales

    @property
    def online_share(self) -> float:
        total = self.total_sales
        return self.online_sales / total if total else float("nan")


def _sample_standard_deviation(values: Iterable[float]) -> float:
    items: List[float] = list(values)
    if len(items) < 2:
        return float("nan")
    avg = mean(items)
    variance = sum((value - avg) ** 2 for value in items) / (len(items) - 1)
    return sqrt(variance)


def _pearson_correlation(xs: Iterable[float], ys: Iterable[float]) -> float:
    x_values = list(xs)
    y_values = list(ys)
    if len(x_values) != len(y_values) or len(x_values) < 2:
        return float("nan")
    x_avg = mean(x_values)
    y_avg = mean(y_values)
    numerator = sum((x - x_avg) * (y - y_avg) for x, y in zip(x_values, y_values))
    denominator = sqrt(
        sum((x - x_avg) ** 2 for x in x_values)
        * sum((y - y_avg) ** 2 for y in y_values)
    )
    return numerator / denominator if denominator else float("nan")


def _simple_linear_regression(xs: Iterable[float], ys: Iterable[float]) -> tuple[float, float]:
    x_values = list(xs)
    y_values = list(ys)
    if len(x_values) != len(y_values) or not x_values:
        raise ValueError("Input sequences must be of equal non-zero length")
    x_avg = mean(x_values)
    y_avg = mean(y_values)
    denominator = sum((x - x_avg) ** 2 for x in x_values)
    if not denominator:
        raise ValueError("Cannot compute regression with zero variance in X")
    slope = sum((x - x_avg) * (y - y_avg) for x, y in zip(x_values, y_values)) / denominator
    intercept = y_avg - slope * x_avg
    return slope, intercept


def load_records(path: Path = DATA_PATH) -> list[MonthlyRecord]:
    with path.open("r", newline="") as handle:
        reader = csv.DictReader(handle)
        records: list[MonthlyRecord] = []
        for row in reader:
            records.append(
                MonthlyRecord(
                    month=row["month"],
                    marketing_spend=float(row["marketing_spend"]),
                    online_sales=float(row["online_sales"]),
                    in_store_sales=float(row["in_store_sales"]),
                )
            )
    return records


def build_summary(records: list[MonthlyRecord]) -> str:
    marketing_spend = [record.marketing_spend for record in records]
    online_sales = [record.online_sales for record in records]
    in_store_sales = [record.in_store_sales for record in records]
    total_sales = [record.total_sales for record in records]

    slope, intercept = _simple_linear_regression(marketing_spend, total_sales)
    correlation = _pearson_correlation(marketing_spend, total_sales)

    summary_lines = [
        "# Coffee Chain Sales Analysis",
        "",
        "## Key Metrics",
        f"- Average monthly marketing spend: ${mean(marketing_spend):,.2f}",
        f"- Average monthly online sales: ${mean(online_sales):,.2f}",
        f"- Average monthly in-store sales: ${mean(in_store_sales):,.2f}",
        f"- Average monthly total sales: ${mean(total_sales):,.2f}",
        f"- Median monthly total sales: ${median(total_sales):,.2f}",
        f"- Standard deviation of total sales: ${_sample_standard_deviation(total_sales):,.2f}",
        "",
        "## Relationship Between Marketing and Sales",
        (
            f"- Pearson correlation between marketing spend and total sales: {correlation:.3f}. "
            "Values close to 1 indicate a strong positive relationship."
        ),
        (
            "- Simple linear regression of total sales on marketing spend yields "
            f"a slope of {slope:.2f} and intercept of ${intercept:,.2f}. "
            "Every additional dollar of marketing spend is associated with the slope amount in sales."
        ),
        "",
    ]

    growth_lines = ["## Month-over-Month Growth"]
    best_growth = None
    for previous, current in zip(records, records[1:]):
        growth = (current.total_sales - previous.total_sales) / previous.total_sales
        growth_lines.append(
            f"- {previous.month} to {current.month}: {growth * 100:.1f}% change in total sales"
        )
        if best_growth is None or growth > best_growth[1]:
            best_growth = (current.month, growth)
    if best_growth is not None:
        growth_lines.append(
            "",
        )
        growth_lines.append(
            f"- Highest growth occurred in {best_growth[0]} with a {best_growth[1] * 100:.1f}% increase over the prior month."
        )
    summary_lines.extend(growth_lines)

    online_share_lines = ["", "## Online Sales Share"]
    avg_online_share = mean(record.online_share for record in records)
    online_share_lines.append(
        f"- Average online sales share of total revenue: {avg_online_share * 100:.1f}%"
    )
    top_online = max(records, key=lambda record: record.online_share)
    online_share_lines.append(
        f"- Highest online share observed in {top_online.month} at {top_online.online_share * 100:.1f}%"
    )
    summary_lines.extend(online_share_lines)

    return "\n".join(summary_lines) + "\n"


def main() -> None:
    records = load_records()
    summary = build_summary(records)
    OUTPUT_PATH.write_text(summary, encoding="utf-8")
    print(summary)


if __name__ == "__main__":
    main()
