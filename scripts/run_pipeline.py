from __future__ import annotations

import argparse
from pathlib import Path

from data_pipeline import DEFAULT_CONFIG, PipelineConfig, build_report, clean_data, extract_csv, save_report, save_rows


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the customer analytics pipeline")
    parser.add_argument("--raw", type=Path, default=DEFAULT_CONFIG.raw_data_path, help="Path to the raw CSV data")
    parser.add_argument(
        "--processed", type=Path, default=DEFAULT_CONFIG.processed_data_path, help="Where to store the processed CSV"
    )
    parser.add_argument("--report", type=Path, default=DEFAULT_CONFIG.report_path, help="Where to store the JSON report")
    return parser.parse_args()


def run_pipeline(config: PipelineConfig) -> None:
    config.ensure_directories()

    rows = extract_csv(config.raw_data_path)
    cleaned = clean_data(rows)
    report = build_report(cleaned)

    save_rows(cleaned, config.processed_data_path)
    save_report(report, config.report_path)


def main() -> None:
    args = parse_arguments()
    config = PipelineConfig(raw_data_path=args.raw, processed_data_path=args.processed, report_path=args.report)
    run_pipeline(config)


if __name__ == "__main__":
    main()
