#!/usr/bin/env python3
"""Mapper for top slow endpoints.

A request is slow if response_time_ms > 800.
Input: CSV log records from stdin.
Output: service_name,endpoint\t1 for slow requests.
"""
import csv
import sys

SERVICE_NAME_INDEX = 3
ENDPOINT_INDEX = 4
RESPONSE_TIME_INDEX = 7
SLOW_THRESHOLD_MS = 800


def main() -> None:
    reader = csv.reader(sys.stdin)
    for row in reader:
        if not row or len(row) <= RESPONSE_TIME_INDEX:
            continue
        if row[0].strip() == "timestamp":
            continue

        service_name = row[SERVICE_NAME_INDEX].strip()
        endpoint = row[ENDPOINT_INDEX].strip()
        try:
            response_time_ms = int(row[RESPONSE_TIME_INDEX].strip())
        except ValueError:
            continue

        if service_name and endpoint and response_time_ms > SLOW_THRESHOLD_MS:
            print(f"{service_name},{endpoint}\t1")


if __name__ == "__main__":
    main()
