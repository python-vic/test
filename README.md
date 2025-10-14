# Coffee Sales Data Analysis

This repository contains a lightweight data analysis example for a fictional coffee chain. The project demonstrates how to
summarize sales performance when only the Python standard library is available.

## Data

`data/coffee_sales.csv` provides monthly records for 2023 including marketing spend, online sales, and in-store sales. The
dataset is intentionally small so the analysis can focus on communicating insights.

## Analysis Script

`scripts/analyze_coffee_sales.py` loads the dataset, computes descriptive statistics, explores the relationship between
marketing and total sales, evaluates month-over-month performance, and summarizes the online sales share of revenue. The
results are written to `outputs/coffee_sales_summary.md` and echoed to the console.

To reproduce the analysis run:

```bash
python scripts/analyze_coffee_sales.py
```

No external dependencies are required.

## Output

The generated report can be found in `outputs/coffee_sales_summary.md`. It highlights the strong positive correlation between
marketing investments and total sales, the months with the fastest growth, and how online sales contribute to revenue.
