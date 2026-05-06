#!/usr/bin/env python3
"""Reducer for request count by service.

Input: sorted service_name\t1 pairs from stdin.
Output: service_name\ttotal_count
"""
import sys


def emit(key: str, count: int) -> None:
    if key:
        print(f"{key}\t{count}")


def main() -> None:
    current_key = None
    current_count = 0

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
            emit(current_key, current_count)
            current_key = key
            current_count = value_int

    if current_key is not None:
        emit(current_key, current_count)


if __name__ == "__main__":
    main()
