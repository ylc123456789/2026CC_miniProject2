#!/usr/bin/env python3
"""Local runner that simulates Hadoop Streaming for the three MapReduce jobs.

The runner executes mapper -> sort -> reducer. This is useful for local correctness
checking before running the same mapper/reducer scripts on Hadoop Streaming.
"""
import argparse
import subprocess
import sys
import time
from pathlib import Path
from typing import Tuple

ROOT = Path(__file__).resolve().parents[1]

JOBS = [
    (
        "request_count_by_service",
        ROOT / "mapreduce" / "request_count_mapper.py",
        ROOT / "mapreduce" / "request_count_reducer.py",
        "request_count_by_service.txt",
    ),
    (
        "server_error_count_by_service",
        ROOT / "mapreduce" / "error_count_mapper.py",
        ROOT / "mapreduce" / "error_count_reducer.py",
        "server_error_count_by_service.txt",
    ),
    (
        "top10_slow_endpoints",
        ROOT / "mapreduce" / "slow_endpoint_mapper.py",
        ROOT / "mapreduce" / "slow_endpoint_reducer.py",
        "top10_slow_endpoints.txt",
    ),
]


def run_command(command: list[str], input_text: str | None = None) -> Tuple[str, float]:
    start = time.perf_counter()
    result = subprocess.run(
        command,
        input=input_text,
        text=True,
        capture_output=True,
        check=True,
    )
    elapsed = time.perf_counter() - start
    return result.stdout, elapsed


def run_job(input_path: Path, mapper: Path, reducer: Path) -> Tuple[str, float]:
    with input_path.open("r", encoding="utf-8", newline="") as f:
        input_text = f.read()

    mapped_output, mapper_time = run_command([sys.executable, str(mapper)], input_text)
    sorted_lines = "\n".join(sorted(line for line in mapped_output.splitlines() if line.strip())) + "\n"
    reduced_output, reducer_time = run_command([sys.executable, str(reducer)], sorted_lines)
    return reduced_output, mapper_time + reducer_time


def main() -> None:
    parser = argparse.ArgumentParser(description="Run local MapReduce simulation.")
    parser.add_argument("--input", required=True, help="Input CSV path.")
    parser.add_argument("--output-dir", default="outputs", help="Output directory.")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    runtime_lines = []
    for job_name, mapper, reducer, output_name in JOBS:
        output_text, elapsed = run_job(input_path, mapper, reducer)
        output_path = output_dir / output_name
        output_path.write_text(output_text, encoding="utf-8")
        runtime_lines.append(f"local_mapreduce,{job_name},{elapsed:.6f},local_python_simulation")
        print(f"[OK] {job_name}: {output_path} ({elapsed:.6f}s)")

    runtime_path = output_dir / "runtime_results.txt"
    existing = runtime_path.read_text(encoding="utf-8") if runtime_path.exists() else "tool,job,runtime_seconds,environment\n"
    with runtime_path.open("w", encoding="utf-8") as f:
        if not existing.startswith("tool,job,runtime_seconds,environment"):
            f.write("tool,job,runtime_seconds,environment\n")
        else:
            f.write(existing.rstrip() + "\n")
        f.write("\n".join(runtime_lines) + "\n")


if __name__ == "__main__":
    main()
