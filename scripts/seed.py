"""CLI entry point: generate all synthetic CVM data and report row counts."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import polars as pl

from cvm.simulator.generator import generate_all


def main() -> None:
    """Run generate_all and print a row-count summary for each parquet file."""
    output_dir = "data/simulated"
    generate_all(output_dir=output_dir)

    entities = ["plans", "customers", "usage", "offers", "campaigns", "assignments"]
    print(f"\n{'Entity':<14} {'Rows':>8}")
    print("-" * 24)
    for name in entities:
        path = f"{output_dir}/{name}.parquet"
        rows = pl.read_parquet(path).height
        print(f"{name:<14} {rows:>8,}")
    print()


if __name__ == "__main__":
    main()
