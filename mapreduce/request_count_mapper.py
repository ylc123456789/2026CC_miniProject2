#!/usr/bin/env python3
"""Mapper for request count by service.

Input: CSV log records from stdin.
Output: service_name\t1
"""
import csv
import sys

SERVICE_NAME_INDEX = 3


def main() -> None:
    reader = csv.reader(sys.stdin)
    for row in reader:
        if not row or len(row) <= SERVICE_NAME_INDEX:
            continue
        if row[0].strip() == "timestamp":
            continue
        service_name = row[SERVICE_NAME_INDEX].strip()
        if service_name:
            print(f"{service_name}\t1")


if __name__ == "__main__":
    main()
