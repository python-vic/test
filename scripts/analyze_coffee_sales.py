from __future__ import annotations

import csv
import shutil
from dataclasses import dataclass
from math import sqrt
from pathlib import Path
from statistics import mean, median
from typing import Iterable, List

import matplotlib.pyplot as plt


DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "coffee_sales.csv"
OUTPUT_DIR = Path(__file__).resolve().parents[1] / "outputs"
OUTPUT_PATH = OUTPUT_DIR / "coffee_sales_summary.md"
SITE_DIR = Path(__file__).resolve().parents[1] / "docs"
SITE_ASSETS_DIR = SITE_DIR / "assets"
SITE_INDEX_PATH = SITE_DIR / "index.md"



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


def build_summary(
    records: list[MonthlyRecord], plot_paths: dict[str, Path | str] | None = None
) -> str:
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

    if plot_paths:
        visualization_lines = ["", "## Visualizations"]
        for label, path in plot_paths.items():
            relative = Path(path)
            visualization_lines.append(f"![{label}]({relative.as_posix()})")
        summary_lines.extend(visualization_lines)

    return "\n".join(summary_lines) + "\n"


def generate_plots(records: list[MonthlyRecord], output_dir: Path = OUTPUT_DIR) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    months = [record.month for record in records]
    marketing_spend = [record.marketing_spend for record in records]
    online_sales = [record.online_sales for record in records]
    in_store_sales = [record.in_store_sales for record in records]
    total_sales = [record.total_sales for record in records]

    def save_fig(fig: plt.Figure, filename: str) -> Path:
        fig.tight_layout()
        path = output_dir / filename
        fig.savefig(path, dpi=150)
        plt.close(fig)
        return path

    plots: dict[str, Path] = {}

    fig1, ax1 = plt.subplots(figsize=(10, 6))
    ax1.plot(months, total_sales, marker="o", linewidth=2, label="Total sales")
    ax1.set_title("Monthly Total Sales")
    ax1.set_xlabel("Month")
    ax1.set_ylabel("Sales (USD)")
    ax1.grid(True, axis="y", linestyle="--", alpha=0.4)
    ax1.tick_params(axis="x", rotation=45)
    fig1.tight_layout()
    plots["Monthly total sales trend"] = save_fig(fig1, "coffee_total_sales_trend.png")

    fig2, ax2 = plt.subplots(figsize=(10, 6))
    ax2.bar(months, in_store_sales, label="In-store sales")
    ax2.bar(months, online_sales, bottom=in_store_sales, label="Online sales")
    ax2.set_title("Sales Breakdown by Channel")
    ax2.set_xlabel("Month")
    ax2.set_ylabel("Sales (USD)")
    ax2.tick_params(axis="x", rotation=45)
    ax2.legend()
    plots["Channel sales breakdown"] = save_fig(fig2, "coffee_channel_sales_breakdown.png")

    slope, intercept = _simple_linear_regression(marketing_spend, total_sales)
    fit_line = [slope * spend + intercept for spend in marketing_spend]

    fig3, ax3 = plt.subplots(figsize=(8, 6))
    ax3.scatter(marketing_spend, total_sales, color="tab:blue", label="Monthly totals")
    ax3.plot(marketing_spend, fit_line, color="tab:orange", label="Regression line")
    ax3.set_title("Marketing Spend vs. Total Sales")
    ax3.set_xlabel("Marketing spend (USD)")
    ax3.set_ylabel("Total sales (USD)")
    ax3.grid(True, linestyle="--", alpha=0.4)
    ax3.legend()
    plots["Marketing vs sales scatter"] = save_fig(fig3, "coffee_marketing_vs_sales.png")

    return plots


def publish_site(records: list[MonthlyRecord], plot_paths: dict[str, Path], site_dir: Path = SITE_DIR) -> None:
    site_dir.mkdir(parents=True, exist_ok=True)
    assets_dir = site_dir / "assets"
    assets_dir.mkdir(parents=True, exist_ok=True)

    relative_paths: dict[str, Path] = {}
    for label, original_path in plot_paths.items():
        destination = assets_dir / original_path.name
        shutil.copy2(original_path, destination)
        relative_paths[label] = Path("assets") / original_path.name

    site_summary = build_summary(records, plot_paths=relative_paths)
    site_index_path = site_dir / "index.md"
    site_index_path.write_text(site_summary, encoding="utf-8")


def main() -> None:
    records = load_records()
    plot_paths = generate_plots(records)
    relative_output_paths = {label: Path(path.name) for label, path in plot_paths.items()}
    summary = build_summary(records, plot_paths=relative_output_paths)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(summary, encoding="utf-8")
    publish_site(records, plot_paths)
    print(summary)


if __name__ == "__main__":
    main()
