"""MVP CLI trigger for the builder server.

Usage:
    python trigger.py <dataset_name> <dataset_version> <start> <end>

Example:
    python trigger.py mock-ohlc 0.1.0 2024-01-01 2024-01-31
"""

import sys

import requests


def main():
    if len(sys.argv) != 5:
        print(f"Usage: {sys.argv[0]} <dataset_name> <dataset_version> <start> <end>")  # noqa: T201 -- cli output
        sys.exit(1)

    dataset_name, dataset_version, start, end = sys.argv[1:]
    url = f"http://localhost:3000/build/{dataset_name}/{dataset_version}"

    print(f"Triggering build: {dataset_name}/{dataset_version} [{start}, {end}]")  # noqa: T201 -- cli output
    resp = requests.post(url, params={"start": start, "end": end})

    if resp.ok:
        print(f"Success: {resp.json()}")  # noqa: T201 -- cli output
    else:
        print(f"Error ({resp.status_code}): {resp.text}")  # noqa: T201 -- cli output
        sys.exit(1)


if __name__ == "__main__":
    main()
