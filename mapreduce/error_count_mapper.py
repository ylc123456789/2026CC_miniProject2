#!/usr/bin/env python3
"""Mapper for server error count by service.

A server error is defined as status_code >= 500.
Input: CSV log records from stdin.
Output: service_name\t1 for records with status_code >= 500.
"""
import csv
import sys

SERVICE_NAME_INDEX = 3
STATUS_CODE_INDEX = 6


def main() -> None:
    reader = csv.reader(sys.stdin)
    for row in reader:
        if not row or len(row) <= STATUS_CODE_INDEX:
            continue
        if row[0].strip() == "timestamp":
            continue

        service_name = row[SERVICE_NAME_INDEX].strip()
        try:
            status_code = int(row[STATUS_CODE_INDEX].strip())
        except ValueError:
            continue

        if service_name and status_code >= 500:
            print(f"{service_name}\t1")


if __name__ == "__main__":
    main()
