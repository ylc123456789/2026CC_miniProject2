#!/usr/bin/env python3
"""Run all local analytics tasks: MapReduce simulation + Ray detection."""
import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run(command: list[str]) -> None:
    print("$ " + " ".join(command))
    subprocess.run(command, check=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run all local tasks.")
    parser.add_argument("--input", default=str(ROOT / "data" / "cloud_service_logs.csv"), help="Input CSV path.")
    parser.add_argument("--output-dir", default=str(ROOT / "outputs"), help="Output directory.")
    parser.add_argument("--num-partitions", type=int, default=4, help="Ray partitions.")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    run([
        sys.executable,
        str(ROOT / "scripts" / "run_local_mapreduce.py"),
        "--input",
        args.input,
        "--output-dir",
        str(output_dir),
    ])

    run([
        sys.executable,
        str(ROOT / "ray" / "degraded_service_detection.py"),
        "--input",
        args.input,
        "--output",
        str(output_dir / "degraded_services_ray.csv"),
        "--metrics-output",
        str(output_dir / "ray_service_metrics.csv"),
        "--runtime-output",
        str(output_dir / "runtime_results.txt"),
        "--num-partitions",
        str(args.num_partitions),
    ])

    print("[OK] All local tasks completed.")


if __name__ == "__main__":
    main()
