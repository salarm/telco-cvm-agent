"""Pure-Python data access layer for the CVM simulator parquet files.

All functions read from ``data/simulated/`` relative to the project root,
return JSON-serialisable dicts or lists, and accept ``week_start``/``week_end``
``date`` parameters so the analyst agent can slice any 7-day window.
"""

from datetime import date, datetime, time
from pathlib import Path
from typing import Any

import polars as pl

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent.parent
_DATA_DIR = _PROJECT_ROOT / "data" / "simulated"


def _load(name: str) -> pl.DataFrame:
    """Read a single parquet file from the data directory."""
    return pl.read_parquet(_DATA_DIR / f"{name}.parquet")


def _filter_assignments(week_start: date, week_end: date) -> pl.DataFrame:
    """Return the assignments DataFrame narrowed to the given date range.

    ``assigned_at`` is a Datetime column; comparison is done against
    midnight of ``week_start`` and the last microsecond of ``week_end``.
    """
    df = _load("assignments")
    lo = datetime.combine(week_start, time.min)
    hi = datetime.combine(week_end, time.max)
    return df.filter((pl.col("assigned_at") >= lo) & (pl.col("assigned_at") <= hi))


def get_weekly_summary(week_start: date, week_end: date) -> dict[str, Any]:
    """Return a high-level summary of offer-assignment outcomes for the week.

    Loads assignments within ``week_start``..``week_end``, joins with the
    customers table to compute churn-risk distributions, and loads usage data
    to count active customers.  Returns a dict with keys: ``total_assignments``,
    ``viewed_rate`` (fraction with a non-null ``shown_at``), ``acceptance_rate``
    (fraction where ``decision == 'viewed_accepted'``), ``total_revenue``,
    ``active_customers`` (distinct customers with usage in the window),
    ``churn_risk_responders`` (mean/median churn_risk for viewed assignments),
    and ``churn_risk_non_responders`` (mean/median for non-viewed assignments).
    """
    assignments = _filter_assignments(week_start, week_end)
    customers = _load("customers")
    usage = _load("usage")

    total = assignments.height
    if total == 0:
        return {
            "total_assignments": 0,
            "viewed_rate": 0.0,
            "acceptance_rate": 0.0,
            "total_revenue": 0.0,
            "active_customers": int(
                usage.filter(
                    (pl.col("date") >= week_start) & (pl.col("date") <= week_end)
                )["customer_id"].n_unique()
            ),
            "churn_risk_responders": {"mean": None, "median": None},
            "churn_risk_non_responders": {"mean": None, "median": None},
        }

    viewed = int(assignments["shown_at"].is_not_null().sum())
    accepted = int((assignments["decision"] == "viewed_accepted").sum())
    total_revenue = round(float(assignments["revenue"].sum()), 2)

    active_customers = int(
        usage.filter((pl.col("date") >= week_start) & (pl.col("date") <= week_end))[
            "customer_id"
        ].n_unique()
    )

    enriched = assignments.join(
        customers.select(["customer_id", "churn_risk"]), on="customer_id", how="left"
    )
    responders = enriched.filter(pl.col("shown_at").is_not_null())
    non_responders = enriched.filter(pl.col("shown_at").is_null())

    def _risk_stats(df: pl.DataFrame) -> dict[str, float | None]:
        cr = df["churn_risk"].drop_nulls()
        if cr.is_empty():
            return {"mean": None, "median": None}
        return {
            "mean": round(float(cr.mean()), 4),  # type: ignore[arg-type]
            "median": round(float(cr.median()), 4),  # type: ignore[arg-type]
        }

    return {
        "total_assignments": total,
        "viewed_rate": round(viewed / total, 4),
        "acceptance_rate": round(accepted / total, 4),
        "total_revenue": total_revenue,
        "active_customers": active_customers,
        "churn_risk_responders": _risk_stats(responders),
        "churn_risk_non_responders": _risk_stats(non_responders),
    }


def get_segment_breakdown(
    week_start: date,
    week_end: date,
    dimension: str,
) -> list[dict[str, Any]]:
    """Break down assignment acceptance and revenue by a single customer dimension.

    ``dimension`` must be one of ``"value_segment"``, ``"plan_tier"``,
    ``"age_bracket"``, or ``"channel"``.  For ``"plan_tier"`` the function
    joins assignments → customers → plans to resolve the plan tier; for
    ``"channel"`` it uses the ``channel`` column directly on assignments; for
    all others it joins assignments → customers.  Returns a list of dicts,
    each with keys ``dimension_value``, ``assignments``, ``acceptance_rate``,
    and ``revenue``, sorted descending by ``revenue``.
    """
    valid = {"value_segment", "plan_tier", "age_bracket", "channel"}
    if dimension not in valid:
        raise ValueError(f"dimension must be one of {valid}, got {dimension!r}")

    assignments = _filter_assignments(week_start, week_end)
    if assignments.is_empty():
        return []

    if dimension == "channel":
        base = assignments
        dim_col = "channel"
    else:
        customers = _load("customers")
        base = assignments.join(
            customers.select(
                ["customer_id", "value_segment", "age_bracket", "plan_id"]
            ),
            on="customer_id",
            how="left",
        )
        if dimension == "plan_tier":
            plans = _load("plans")
            base = base.join(
                plans.select([pl.col("plan_id"), pl.col("tier").alias("plan_tier")]),
                on="plan_id",
                how="left",
            )
            dim_col = "plan_tier"
        else:
            dim_col = dimension

    agg = (
        base.group_by(dim_col)
        .agg(
            [
                pl.len().alias("assignments"),
                (pl.col("decision") == "viewed_accepted").sum().alias("accepted"),
                pl.col("revenue").sum(),
            ]
        )
        .with_columns(
            (pl.col("accepted").cast(pl.Float64) / pl.col("assignments")).alias(
                "acceptance_rate"
            )
        )
        .sort("revenue", descending=True)
    )

    return [
        {
            "dimension_value": str(row[dim_col]),
            "assignments": int(row["assignments"]),
            "acceptance_rate": round(float(row["acceptance_rate"]), 4),
            "revenue": round(float(row["revenue"]), 2),
        }
        for row in agg.to_dicts()
    ]


def get_offer_performance(week_start: date, week_end: date) -> list[dict[str, Any]]:
    """Return per-offer metrics for the week, sorted descending by acceptance rate.

    Loads assignments and groups by ``offer_id``.  For each offer the function
    reports ``shown`` (non-null ``shown_at`` count), ``accepted`` (decisions
    equal to ``'viewed_accepted'``), ``revenue``, and ``acceptance_rate``
    (accepted / shown, guarded against zero-division).  Offers with zero
    impressions are included with a zero acceptance rate.
    """
    assignments = _filter_assignments(week_start, week_end)
    if assignments.is_empty():
        return []

    stats = (
        assignments.group_by("offer_id")
        .agg(
            [
                pl.col("shown_at").is_not_null().sum().alias("shown"),
                (pl.col("decision") == "viewed_accepted").sum().alias("accepted"),
                pl.col("revenue").sum(),
            ]
        )
        .with_columns(
            (
                pl.col("accepted").cast(pl.Float64)
                / pl.col("shown").clip(lower_bound=1)
            ).alias("acceptance_rate")
        )
        .sort("acceptance_rate", descending=True)
    )

    return [
        {
            "offer_id": row["offer_id"],
            "shown": int(row["shown"]),
            "accepted": int(row["accepted"]),
            "revenue": round(float(row["revenue"]), 2),
            "acceptance_rate": round(float(row["acceptance_rate"]), 4),
        }
        for row in stats.to_dicts()
    ]


def get_uptake_by_channel(week_start: date, week_end: date) -> list[dict[str, Any]]:
    """Return assignment volumes and acceptance rates broken down by delivery channel.

    Channels are ``app_banner``, ``sms``, and ``in_app_push``.  Returns a list
    of dicts with keys ``channel``, ``assignments``, ``viewed``, ``accepted``,
    ``viewed_rate``, and ``acceptance_rate``.  Rows are sorted descending by
    ``acceptance_rate`` so the highest-performing channel appears first.
    """
    assignments = _filter_assignments(week_start, week_end)
    if assignments.is_empty():
        return []

    stats = (
        assignments.group_by("channel")
        .agg(
            [
                pl.len().alias("assignments"),
                pl.col("shown_at").is_not_null().sum().alias("viewed"),
                (pl.col("decision") == "viewed_accepted").sum().alias("accepted"),
                pl.col("revenue").sum(),
            ]
        )
        .with_columns(
            [
                (pl.col("viewed").cast(pl.Float64) / pl.col("assignments")).alias(
                    "viewed_rate"
                ),
                (pl.col("accepted").cast(pl.Float64) / pl.col("assignments")).alias(
                    "acceptance_rate"
                ),
            ]
        )
        .sort("acceptance_rate", descending=True)
    )

    return [
        {
            "channel": row["channel"],
            "assignments": int(row["assignments"]),
            "viewed": int(row["viewed"]),
            "accepted": int(row["accepted"]),
            "viewed_rate": round(float(row["viewed_rate"]), 4),
            "acceptance_rate": round(float(row["acceptance_rate"]), 4),
        }
        for row in stats.to_dicts()
    ]


def get_burn_patterns(week_start: date, week_end: date) -> dict[str, Any]:
    """Identify customers projected to exhaust their plan allowance before cycle end.

    Loads daily usage within ``week_start``..``week_end``, computes each
    customer's average daily data and voice consumption, then extrapolates to a
    30-day monthly total.  Customers whose projected data usage exceeds 85 % of
    their plan's data allowance, or whose projected voice usage exceeds 85 % of
    their voice allowance, are flagged as at-risk.  The result dict contains
    ``total_customers_analysed``, ``at_risk_count``, ``at_risk_pct``,
    ``by_value_segment`` (list of ``{value_segment, at_risk_count}``), and
    ``by_plan_tier`` (list of ``{tier, at_risk_count}``).
    """
    usage = _load("usage")
    customers = _load("customers")
    plans = _load("plans")

    week_usage = usage.filter(
        (pl.col("date") >= week_start) & (pl.col("date") <= week_end)
    )
    if week_usage.is_empty():
        return {
            "total_customers_analysed": 0,
            "at_risk_count": 0,
            "at_risk_pct": 0.0,
            "by_value_segment": [],
            "by_plan_tier": [],
        }

    cust_usage = week_usage.group_by("customer_id").agg(
        [
            pl.col("data_mb").sum(),
            pl.col("voice_min").sum(),
            pl.len().alias("days_seen"),
        ]
    )

    enriched = (
        cust_usage.join(
            customers.select(["customer_id", "plan_id", "value_segment"]),
            on="customer_id",
            how="left",
        )
        .join(
            plans.select(
                [
                    pl.col("plan_id"),
                    (pl.col("data_gb") * 1024).alias("plan_data_mb"),
                    pl.col("voice_min").alias("plan_voice_min"),
                    pl.col("tier"),
                ]
            ),
            on="plan_id",
            how="left",
        )
        .with_columns(
            [
                (pl.col("data_mb") / pl.col("days_seen") * 30).alias("proj_data_mb"),
                (pl.col("voice_min") / pl.col("days_seen") * 30).alias(
                    "proj_voice_min"
                ),
            ]
        )
        .with_columns(
            (
                (pl.col("proj_data_mb") > pl.col("plan_data_mb") * 0.85)
                | (pl.col("proj_voice_min") > pl.col("plan_voice_min") * 0.85)
            ).alias("at_risk")
        )
    )

    total = enriched.height
    at_risk_df = enriched.filter(pl.col("at_risk"))
    at_risk_count = at_risk_df.height

    by_segment = (
        at_risk_df.group_by("value_segment")
        .agg(pl.len().alias("at_risk_count"))
        .sort("value_segment")
        .to_dicts()
    )
    by_tier = (
        at_risk_df.group_by("tier")
        .agg(pl.len().alias("at_risk_count"))
        .sort("tier")
        .to_dicts()
    )

    return {
        "total_customers_analysed": total,
        "at_risk_count": at_risk_count,
        "at_risk_pct": round(at_risk_count / total * 100, 1) if total else 0.0,
        "by_value_segment": [
            {
                "value_segment": r["value_segment"],
                "at_risk_count": int(r["at_risk_count"]),
            }
            for r in by_segment
        ],
        "by_plan_tier": [
            {"tier": r["tier"], "at_risk_count": int(r["at_risk_count"])}
            for r in by_tier
        ],
    }


def compute_revenue_impact(week_start: date, week_end: date) -> dict[str, Any]:
    """Compute total incremental revenue generated by accepted offers in the week.

    Filters assignments to ``'viewed_accepted'`` decisions within the date
    window and joins with the customers table to break revenue by
    ``value_segment``.  Returns a dict with ``total_revenue``,
    ``customers_generating_revenue`` (distinct count), ``top_5_offers`` (list
    of ``{offer_id, times_accepted, revenue}`` sorted by revenue desc), and
    ``revenue_by_segment`` (list of ``{value_segment, revenue}``).
    """
    assignments = _filter_assignments(week_start, week_end)
    customers = _load("customers")

    accepted = assignments.filter(pl.col("decision") == "viewed_accepted")
    if accepted.is_empty():
        return {
            "total_revenue": 0.0,
            "customers_generating_revenue": 0,
            "top_5_offers": [],
            "revenue_by_segment": [],
        }

    total_revenue = round(float(accepted["revenue"].sum()), 2)
    customers_with_rev = int(accepted["customer_id"].n_unique())

    top_5 = (
        accepted.group_by("offer_id")
        .agg(
            [
                pl.len().alias("times_accepted"),
                pl.col("revenue").sum(),
            ]
        )
        .sort("revenue", descending=True)
        .head(5)
    )

    by_seg = (
        accepted.join(
            customers.select(["customer_id", "value_segment"]),
            on="customer_id",
            how="left",
        )
        .group_by("value_segment")
        .agg(pl.col("revenue").sum())
        .sort("revenue", descending=True)
    )

    return {
        "total_revenue": total_revenue,
        "customers_generating_revenue": customers_with_rev,
        "top_5_offers": [
            {
                "offer_id": r["offer_id"],
                "times_accepted": int(r["times_accepted"]),
                "revenue": round(float(r["revenue"]), 2),
            }
            for r in top_5.to_dicts()
        ],
        "revenue_by_segment": [
            {
                "value_segment": r["value_segment"],
                "revenue": round(float(r["revenue"]), 2),
            }
            for r in by_seg.to_dicts()
        ],
    }
