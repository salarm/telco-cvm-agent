"""CLI: generate a CVM weekly analyst report for one of four simulated weeks.

Usage:
    uv run python scripts/run_analyst.py --week 3   # richest data (default)
    uv run python scripts/run_analyst.py --week 1

Week-to-date mapping is derived dynamically from the usage parquet so the
script stays correct regardless of when seed.py was run.
"""

import argparse
import sys
from datetime import timedelta
from pathlib import Path

import polars as pl

# Allow running the script without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from cvm.agents.analyst import run_analyst


def _week_ranges() -> dict[int, tuple]:
    """Compute four equal week ranges from the actual usage date span."""
    usage = pl.read_parquet(
        Path(__file__).resolve().parent.parent / "data" / "simulated" / "usage.parquet"
    )
    period_start = usage["date"].min()
    period_end = usage["date"].max()

    return {
        1: (period_start, period_start + timedelta(days=6)),
        2: (period_start + timedelta(days=7), period_start + timedelta(days=13)),
        3: (period_start + timedelta(days=14), period_start + timedelta(days=20)),
        4: (period_start + timedelta(days=21), period_end),
    }


def main() -> None:
    """Parse --week argument, run the analyst agent, and save the report."""
    parser = argparse.ArgumentParser(
        description="Generate a CVM weekly analyst report."
    )
    parser.add_argument(
        "--week",
        type=int,
        choices=[1, 2, 3, 4],
        default=3,
        help="Week number within the 30-day simulated period (default: 3).",
    )
    args = parser.parse_args()

    ranges = _week_ranges()
    week_start, week_end = ranges[args.week]

    print(
        f"Running CVM Analyst for week {args.week}: {week_start} → {week_end} …",
        flush=True,
    )

    report = run_analyst(week_start, week_end)

    reports_dir = Path(__file__).resolve().parent.parent / "reports"
    reports_dir.mkdir(exist_ok=True)
    out_path = reports_dir / f"week_{args.week}.md"
    out_path.write_text(report, encoding="utf-8")

    print(report)
    print(f"\n— Saved to {out_path}", flush=True)


if __name__ == "__main__":
    main()
