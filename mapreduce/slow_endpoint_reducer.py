#!/usr/bin/env python3
"""Reducer for top 10 slow endpoints.

Input: sorted service_name,endpoint\t1 pairs from stdin.
Output: top 10 service_name,endpoint\tslow_request_count, sorted by count descending.

This reducer is intended to run with one reducer task, because top-10 requires a global ranking.
"""
import sys
from typing import List, Tuple

TOP_K = 10


def main() -> None:
    current_key = None
    current_count = 0
    aggregated: List[Tuple[str, int]] = []

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            key, value = line.split("\t", 1)
            value_int = int(value)
        except ValueError:
            continue

        if current_key is None:
            current_key = key
            current_count = value_int
        elif key == current_key:
            current_count += value_int
        else:
            aggregated.append((current_key, current_count))
            current_key = key
            current_count = value_int

    if current_key is not None:
        aggregated.append((current_key, current_count))

    top_results = sorted(aggregated, key=lambda item: (-item[1], item[0]))[:TOP_K]
    for key, count in top_results:
        print(f"{key}\t{count}")


if __name__ == "__main__":
    main()
