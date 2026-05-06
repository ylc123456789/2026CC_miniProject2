#!/usr/bin/env python3
"""Ray degraded service detection for cloud service logs.

A service is degraded if at least one condition is met:
- slow request rate > 20%, where a slow request has response_time_ms > 800
- server error rate > 10%, where a server error has status_code >= 500
- Timeout error count >= 5

The dataset is split into partitions. Each Ray remote task processes one partition
and returns partial service metrics. The driver combines partial metrics and writes
final degraded-service results.
"""
import argparse
import csv
import math
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

import ray

SLOW_THRESHOLD_MS = 800
SLOW_RATE_THRESHOLD = 0.20
SERVER_ERROR_RATE_THRESHOLD = 0.10
TIMEOUT_THRESHOLD = 5

MetricDict = Dict[str, Dict[str, int]]


def empty_metrics() -> Dict[str, int]:
    return {"total": 0, "slow": 0, "server_error": 0, "timeout": 0}


@ray.remote
def process_partition(rows: List[dict]) -> MetricDict:
    """Process one data partition and return partial metrics by service."""
    metrics: MetricDict = defaultdict(empty_metrics)

    for row in rows:
        service_name = (row.get("service_name") or "").strip()
        if not service_name or service_name == "service_name":
            continue

        try:
            status_code = int((row.get("status_code") or "").strip())
            response_time_ms = int((row.get("response_time_ms") or "").strip())
        except ValueError:
            continue

        error_type = (row.get("error_type") or "").strip()
        service_metrics = metrics[service_name]
        service_metrics["total"] += 1
        if response_time_ms > SLOW_THRESHOLD_MS:
            service_metrics["slow"] += 1
        if status_code >= 500:
            service_metrics["server_error"] += 1
        if error_type == "Timeout":
            service_metrics["timeout"] += 1

    return dict(metrics)


def read_rows(input_path: Path) -> List[dict]:
    with input_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]


def split_list(items: List[dict], num_partitions: int) -> List[List[dict]]:
    if num_partitions <= 0:
        raise ValueError("num_partitions must be positive")
    if not items:
        return []
    chunk_size = math.ceil(len(items) / num_partitions)
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


def combine_metrics(partials: Iterable[MetricDict]) -> MetricDict:
    combined: MetricDict = defaultdict(empty_metrics)
    for partial in partials:
        for service_name, values in partial.items():
            target = combined[service_name]
            target["total"] += values.get("total", 0)
            target["slow"] += values.get("slow", 0)
            target["server_error"] += values.get("server_error", 0)
            target["timeout"] += values.get("timeout", 0)
    return dict(combined)


def detect_degraded_services(metrics: MetricDict) -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    for service_name in sorted(metrics):
        values = metrics[service_name]
        total = values["total"]
        if total == 0:
            continue

        slow_rate = values["slow"] / total
        server_error_rate = values["server_error"] / total
        reasons = []

        if slow_rate > SLOW_RATE_THRESHOLD:
            reasons.append(f"high slow request rate ({values['slow']}/{total}={slow_rate:.2%})")
        if server_error_rate > SERVER_ERROR_RATE_THRESHOLD:
            reasons.append(
                f"high server error rate ({values['server_error']}/{total}={server_error_rate:.2%})"
            )
        if values["timeout"] >= TIMEOUT_THRESHOLD:
            reasons.append(f"repeated timeout errors ({values['timeout']})")

        if reasons:
            results.append((service_name, "; ".join(reasons)))

    return results


def write_degraded_output(results: List[Tuple[str, str]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["service_name", "reason"])
        for service_name, reason in results:
            writer.writerow([service_name, reason])


def write_metrics_output(metrics: MetricDict, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "service_name",
                "total_requests",
                "slow_requests",
                "server_errors",
                "timeout_errors",
                "slow_rate",
                "server_error_rate",
            ]
        )
        for service_name in sorted(metrics):
            values = metrics[service_name]
            total = values["total"]
            slow_rate = values["slow"] / total if total else 0.0
            server_error_rate = values["server_error"] / total if total else 0.0
            writer.writerow(
                [
                    service_name,
                    total,
                    values["slow"],
                    values["server_error"],
                    values["timeout"],
                    f"{slow_rate:.6f}",
                    f"{server_error_rate:.6f}",
                ]
            )


def main() -> None:
    parser = argparse.ArgumentParser(description="Detect degraded services with Ray.")
    parser.add_argument("--input", required=True, help="Input CSV path.")
    parser.add_argument("--output", default="outputs/degraded_services_ray.csv", help="Output CSV path.")
    parser.add_argument(
        "--metrics-output",
        default="outputs/ray_service_metrics.csv",
        help="Detailed service metrics CSV path.",
    )
    parser.add_argument("--num-partitions", type=int, default=4, help="Number of Ray partitions.")
    parser.add_argument(
        "--runtime-output",
        default="outputs/runtime_results.txt",
        help="Runtime result file path.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    metrics_output_path = Path(args.metrics_output)
    runtime_output_path = Path(args.runtime_output)

    start = time.perf_counter()
    rows = read_rows(input_path)

    ray.init(ignore_reinit_error=True)
    partitions = split_list(rows, args.num_partitions)
    object_refs = [process_partition.remote(partition) for partition in partitions]
    partials = ray.get(object_refs)
    combined_metrics = combine_metrics(partials)
    degraded_results = detect_degraded_services(combined_metrics)

    write_degraded_output(degraded_results, output_path)
    write_metrics_output(combined_metrics, metrics_output_path)

    elapsed = time.perf_counter() - start
    runtime_output_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not runtime_output_path.exists()
    with runtime_output_path.open("a", encoding="utf-8") as f:
        if write_header:
            f.write("tool,job,runtime_seconds,environment\n")
        f.write(f"ray,degraded_service_detection,{elapsed:.6f},ray_local_mode_partitions_{args.num_partitions}\n")

    print(f"[OK] Wrote degraded services to {output_path}")
    print(f"[OK] Wrote service metrics to {metrics_output_path}")
    print(f"[OK] Runtime: {elapsed:.6f}s")
    ray.shutdown()


if __name__ == "__main__":
    main()
