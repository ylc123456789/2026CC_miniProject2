#!/usr/bin/env python3
"""Independent validation script for project outputs.

This script does not use the MapReduce mapper/reducer or Ray remote tasks. It uses a
direct Python calculation to validate that output files match the dataset.
"""
import argparse
import csv
from collections import Counter, defaultdict
from pathlib import Path
from typing import Dict, Tuple

SLOW_THRESHOLD_MS = 800


def read_dataset(input_path: Path):
    with input_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def normalize_lines(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]


def validate_mapreduce(input_path: Path, output_dir: Path) -> None:
    request_counts = Counter()
    error_counts = Counter()
    slow_endpoint_counts = Counter()

    for row in read_dataset(input_path):
        service_name = (row.get("service_name") or "").strip()
        endpoint = (row.get("endpoint") or "").strip()
        if not service_name:
            continue
        try:
            status_code = int((row.get("status_code") or "").strip())
            response_time_ms = int((row.get("response_time_ms") or "").strip())
        except ValueError:
            continue

        request_counts[service_name] += 1
        if status_code >= 500:
            error_counts[service_name] += 1
        if response_time_ms > SLOW_THRESHOLD_MS:
            slow_endpoint_counts[f"{service_name},{endpoint}"] += 1

    expected_request_lines = [f"{k}\t{v}" for k, v in sorted(request_counts.items())]
    expected_error_lines = [f"{k}\t{v}" for k, v in sorted(error_counts.items())]
    expected_slow_lines = [
        f"{k}\t{v}"
        for k, v in sorted(slow_endpoint_counts.items(), key=lambda item: (-item[1], item[0]))[:10]
    ]

    checks = [
        ("request_count_by_service.txt", expected_request_lines),
        ("server_error_count_by_service.txt", expected_error_lines),
        ("top10_slow_endpoints.txt", expected_slow_lines),
    ]
    for filename, expected_lines in checks:
        actual_path = output_dir / filename
        actual_lines = normalize_lines(actual_path.read_text(encoding="utf-8"))
        if actual_lines != expected_lines:
            raise AssertionError(f"Validation failed for {filename}\nExpected: {expected_lines}\nActual: {actual_lines}")
        print(f"[OK] {filename} matches independent calculation.")


def validate_ray_metrics(input_path: Path, output_dir: Path) -> None:
    metrics: Dict[str, Dict[str, int]] = defaultdict(lambda: {"total": 0, "slow": 0, "server_error": 0, "timeout": 0})

    for row in read_dataset(input_path):
        service_name = (row.get("service_name") or "").strip()
        if not service_name:
            continue
        try:
            status_code = int((row.get("status_code") or "").strip())
            response_time_ms = int((row.get("response_time_ms") or "").strip())
        except ValueError:
            continue
        error_type = (row.get("error_type") or "").strip()

        metrics[service_name]["total"] += 1
        if response_time_ms > SLOW_THRESHOLD_MS:
            metrics[service_name]["slow"] += 1
        if status_code >= 500:
            metrics[service_name]["server_error"] += 1
        if error_type == "Timeout":
            metrics[service_name]["timeout"] += 1

    metrics_path = output_dir / "ray_service_metrics.csv"
    with metrics_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        actual = {row["service_name"]: row for row in reader}

    for service_name, expected in metrics.items():
        row = actual.get(service_name)
        if row is None:
            raise AssertionError(f"Missing service in ray_service_metrics.csv: {service_name}")
        if int(row["total_requests"]) != expected["total"]:
            raise AssertionError(f"Total mismatch for {service_name}")
        if int(row["slow_requests"]) != expected["slow"]:
            raise AssertionError(f"Slow mismatch for {service_name}")
        if int(row["server_errors"]) != expected["server_error"]:
            raise AssertionError(f"Server error mismatch for {service_name}")
        if int(row["timeout_errors"]) != expected["timeout"]:
            raise AssertionError(f"Timeout mismatch for {service_name}")
    print("[OK] ray_service_metrics.csv matches independent calculation.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate outputs using direct Python calculation.")
    parser.add_argument("--input", required=True, help="Input CSV path.")
    parser.add_argument("--output-dir", default="outputs", help="Output directory.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    validate_mapreduce(input_path, output_dir)
    validate_ray_metrics(input_path, output_dir)
    print("[OK] All validation checks passed.")


if __name__ == "__main__":
    main()
