#!/usr/bin/env python3
"""
Combine all benchmark JSON results in a directory into one HTML report.

Usage:
    python combine_results.py ./results/run_all_20260224_120000
    python combine_results.py ./results/run_all_20260224_120000 --open

The script reads every benchmark_*.json file in the given directory,
generates 'combined_report.html' in the same directory, and prints the path.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from output.combined_html_reporter import write


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="combine_results.py",
        description="Combine benchmark JSON results into a single HTML report.",
    )
    parser.add_argument(
        "directory",
        help="Directory containing benchmark_*.json result files.",
    )
    parser.add_argument(
        "--open",
        action="store_true",
        help="Open the generated HTML in the default browser after creation.",
    )
    args = parser.parse_args()

    output_dir = Path(args.directory).resolve()
    if not output_dir.is_dir():
        print(f"Error: '{output_dir}' is not a directory.", file=sys.stderr)
        return 1

    path = write(output_dir)
    if not path:
        return 1

    print(f"Combined report written to:\n  {path}")

    if args.open:
        try:
            import webbrowser
            webbrowser.open(path.as_uri())
        except Exception as e:
            print(f"  [warn] Could not open browser: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
