# Customer Data Analysis Pipeline

This repository contains a simple yet extensible data analysis pipeline for customer transaction data. The
pipeline extracts data from CSV files, applies a series of cleaning and enrichment steps, and generates a
summary analytics report.

## Project structure

```
├── data_pipeline/          # Core pipeline modules (config, extract, transform, analysis, load)
├── data/                   # Example raw dataset and output directories
├── reports/                # Generated reports are stored here
├── scripts/run_pipeline.py # Command line entry point for executing the pipeline
└── tests/                  # Automated tests that validate the pipeline
```

## Getting started

1. Create a virtual environment and install dependencies (only needed for running the tests):

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

2. Run the pipeline on the provided sample dataset:

   ```bash
   python scripts/run_pipeline.py
   ```

   This command will read `data/raw/sample_data.csv`, create a cleaned version in
   `data/processed/processed_data.csv`, and produce a JSON analysis in `reports/summary_report.json`.

3. Execute the automated tests:

   ```bash
   pytest
   ```

## Extending the pipeline

- **New data sources:** Add extractors in `data_pipeline/extract.py` to load data from APIs, databases, or
  other formats.
- **Custom transformations:** Implement additional transformation functions in `data_pipeline/transform.py`
  to tailor the cleaning rules for your domain.
- **Additional analytics:** Extend `data_pipeline/analysis.py` with new metrics or visualizations, and export
  them via `data_pipeline/load.py`.

The modular design makes it straightforward to plug in extra steps without modifying the rest of the system.
