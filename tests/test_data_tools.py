"""Tests for cvm.tools.data_tools — correctness, expected keys, and reconciliation."""

from datetime import timedelta
from pathlib import Path

import polars as pl
import pytest

from cvm.tools.data_tools import (
    compute_revenue_impact,
    get_burn_patterns,
    get_offer_performance,
    get_segment_breakdown,
    get_uptake_by_channel,
    get_weekly_summary,
)

# ---------------------------------------------------------------------------
# Helpers — derive date ranges from the actual parquet files
# ---------------------------------------------------------------------------

_DATA_DIR = Path(__file__).resolve().parent.parent / "data" / "simulated"


@pytest.fixture(scope="session", autouse=True)
def ensure_parquet_data() -> None:
    """Generate simulation data if parquet files are missing."""
    if not (_DATA_DIR / "assignments.parquet").exists():
        from cvm.simulator.generator import generate_all

        generate_all()


@pytest.fixture(scope="session")
def period():
    """Return (period_start, period_end) dates from the usage parquet."""
    usage = pl.read_parquet(_DATA_DIR / "usage.parquet")
    return usage["date"].min(), usage["date"].max()


@pytest.fixture(scope="session")
def full_range(period):
    """Full 30-day date range — guarantees all tools return data."""
    return period[0], period[1]


@pytest.fixture(scope="session")
def week3_range(period):
    """Week 3 date range — all assignments fall in this window."""
    start = period[0]
    return start + timedelta(days=14), start + timedelta(days=20)


@pytest.fixture(scope="session")
def empty_range(period):
    """A future date range guaranteed to have no assignments or usage."""
    end = period[1]
    return end + timedelta(days=100), end + timedelta(days=106)


# ---------------------------------------------------------------------------
# get_weekly_summary
# ---------------------------------------------------------------------------


class TestGetWeeklySummary:
    _REQUIRED_KEYS = {
        "total_assignments",
        "viewed_rate",
        "acceptance_rate",
        "total_revenue",
        "active_customers",
        "churn_risk_responders",
        "churn_risk_non_responders",
    }

    def test_returns_expected_keys(self, full_range) -> None:
        """Result must contain all required top-level keys."""
        result = get_weekly_summary(*full_range)
        assert self._REQUIRED_KEYS.issubset(result.keys())

    def test_full_range_has_assignments(self, full_range) -> None:
        """There must be assignments in the full 30-day period."""
        result = get_weekly_summary(*full_range)
        assert result["total_assignments"] > 0

    def test_rates_in_unit_interval(self, full_range) -> None:
        """Viewed rate and acceptance rate must be between 0 and 1."""
        result = get_weekly_summary(*full_range)
        assert 0.0 <= result["viewed_rate"] <= 1.0
        assert 0.0 <= result["acceptance_rate"] <= 1.0

    def test_active_customers_positive(self, full_range) -> None:
        """There must be active customers across the full period."""
        result = get_weekly_summary(*full_range)
        assert result["active_customers"] > 0

    def test_revenue_non_negative(self, full_range) -> None:
        """Total revenue must be zero or positive."""
        result = get_weekly_summary(*full_range)
        assert result["total_revenue"] >= 0.0

    def test_empty_range_returns_zeros(self, empty_range) -> None:
        """An out-of-range window must return zeros without raising."""
        result = get_weekly_summary(*empty_range)
        assert result["total_assignments"] == 0
        assert result["total_revenue"] == 0.0


# ---------------------------------------------------------------------------
# get_segment_breakdown
# ---------------------------------------------------------------------------


class TestGetSegmentBreakdown:
    _REQUIRED_ROW_KEYS = {
        "dimension_value",
        "assignments",
        "acceptance_rate",
        "revenue",
    }

    @pytest.mark.parametrize(
        "dimension",
        ["value_segment", "plan_tier", "age_bracket", "channel"],
    )
    def test_returns_list_of_dicts(self, full_range, dimension) -> None:
        """Each row must have the four required keys."""
        rows = get_segment_breakdown(*full_range, dimension=dimension)
        assert isinstance(rows, list)
        assert len(rows) > 0
        for row in rows:
            assert self._REQUIRED_ROW_KEYS.issubset(row.keys())

    def test_acceptance_rates_in_unit_interval(self, full_range) -> None:
        """All acceptance rates must be in [0, 1]."""
        rows = get_segment_breakdown(*full_range, dimension="value_segment")
        for row in rows:
            assert 0.0 <= row["acceptance_rate"] <= 1.0

    def test_revenue_sum_reconciles_with_summary(self, week3_range) -> None:
        """Sum of segment revenues must approximately equal total summary revenue."""
        summary = get_weekly_summary(*week3_range)
        rows = get_segment_breakdown(*week3_range, dimension="value_segment")
        segment_total = sum(r["revenue"] for r in rows)
        assert abs(segment_total - summary["total_revenue"]) < 0.02

    def test_invalid_dimension_raises(self, full_range) -> None:
        """An unrecognised dimension must raise ValueError."""
        with pytest.raises(ValueError, match="dimension must be one of"):
            get_segment_breakdown(*full_range, dimension="city")

    def test_empty_range_returns_empty_list(self, empty_range) -> None:
        """No data in range must return an empty list, not an error."""
        rows = get_segment_breakdown(*empty_range, dimension="channel")
        assert rows == []


# ---------------------------------------------------------------------------
# get_offer_performance
# ---------------------------------------------------------------------------


class TestGetOfferPerformance:
    _REQUIRED_KEYS = {"offer_id", "shown", "accepted", "revenue", "acceptance_rate"}

    def test_returns_list(self, full_range) -> None:
        """Must return a non-empty list of dicts."""
        rows = get_offer_performance(*full_range)
        assert isinstance(rows, list)
        assert len(rows) > 0

    def test_required_keys_present(self, full_range) -> None:
        """Every row must have the five required keys."""
        rows = get_offer_performance(*full_range)
        for row in rows:
            assert self._REQUIRED_KEYS.issubset(row.keys())

    def test_sorted_by_acceptance_rate_desc(self, full_range) -> None:
        """Rows must be ordered descending by acceptance_rate."""
        rows = get_offer_performance(*full_range)
        rates = [r["acceptance_rate"] for r in rows]
        assert rates == sorted(rates, reverse=True)

    def test_accepted_le_shown(self, full_range) -> None:
        """Accepted count must never exceed shown count."""
        rows = get_offer_performance(*full_range)
        for row in rows:
            assert row["accepted"] <= row["shown"]

    def test_empty_range_returns_empty_list(self, empty_range) -> None:
        """No data in range must return empty list."""
        assert get_offer_performance(*empty_range) == []


# ---------------------------------------------------------------------------
# get_uptake_by_channel
# ---------------------------------------------------------------------------


class TestGetUptakeByChannel:
    _CHANNELS = {"app_banner", "sms", "in_app_push"}
    _REQUIRED_KEYS = {
        "channel",
        "assignments",
        "viewed",
        "accepted",
        "viewed_rate",
        "acceptance_rate",
    }

    def test_returns_all_channels(self, full_range) -> None:
        """All three channels must appear in the output."""
        rows = get_uptake_by_channel(*full_range)
        channels = {r["channel"] for r in rows}
        assert channels == self._CHANNELS

    def test_required_keys_present(self, full_range) -> None:
        """Every row must carry the six required keys."""
        rows = get_uptake_by_channel(*full_range)
        for row in rows:
            assert self._REQUIRED_KEYS.issubset(row.keys())

    def test_assignments_sum_equals_total(self, week3_range) -> None:
        """Sum of per-channel assignment counts must equal summary total."""
        summary = get_weekly_summary(*week3_range)
        rows = get_uptake_by_channel(*week3_range)
        channel_total = sum(r["assignments"] for r in rows)
        assert channel_total == summary["total_assignments"]

    def test_empty_range_returns_empty(self, empty_range) -> None:
        """No data in range must return empty list."""
        assert get_uptake_by_channel(*empty_range) == []


# ---------------------------------------------------------------------------
# get_burn_patterns
# ---------------------------------------------------------------------------


class TestGetBurnPatterns:
    _REQUIRED_KEYS = {
        "total_customers_analysed",
        "at_risk_count",
        "at_risk_pct",
        "by_value_segment",
        "by_plan_tier",
    }

    def test_returns_expected_keys(self, full_range) -> None:
        """Result must have all required top-level keys."""
        result = get_burn_patterns(*full_range)
        assert self._REQUIRED_KEYS.issubset(result.keys())

    def test_at_risk_count_le_total(self, full_range) -> None:
        """At-risk count must not exceed the total analysed."""
        result = get_burn_patterns(*full_range)
        assert result["at_risk_count"] <= result["total_customers_analysed"]

    def test_at_risk_pct_matches_count(self, full_range) -> None:
        """at_risk_pct must be consistent with at_risk_count / total."""
        result = get_burn_patterns(*full_range)
        total = result["total_customers_analysed"]
        if total > 0:
            expected_pct = round(result["at_risk_count"] / total * 100, 1)
            assert abs(result["at_risk_pct"] - expected_pct) < 0.01

    def test_by_segment_sum_le_at_risk(self, full_range) -> None:
        """Sum of segment at-risk counts must equal the overall at_risk_count."""
        result = get_burn_patterns(*full_range)
        seg_sum = sum(r["at_risk_count"] for r in result["by_value_segment"])
        assert seg_sum == result["at_risk_count"]

    def test_heavy_users_present(self, full_range) -> None:
        """Simulation generates heavy users so at_risk_count must be > 0."""
        result = get_burn_patterns(*full_range)
        assert result["at_risk_count"] > 0

    def test_empty_range_returns_zeros(self, empty_range) -> None:
        """No usage in range must return zero counts without error."""
        result = get_burn_patterns(*empty_range)
        assert result["at_risk_count"] == 0
        assert result["by_value_segment"] == []


# ---------------------------------------------------------------------------
# compute_revenue_impact
# ---------------------------------------------------------------------------


class TestComputeRevenueImpact:
    _REQUIRED_KEYS = {
        "total_revenue",
        "customers_generating_revenue",
        "top_5_offers",
        "revenue_by_segment",
    }

    def test_returns_expected_keys(self, full_range) -> None:
        """Result must have all required top-level keys."""
        result = compute_revenue_impact(*full_range)
        assert self._REQUIRED_KEYS.issubset(result.keys())

    def test_total_revenue_matches_summary(self, week3_range) -> None:
        """Revenue from this function must match the summary's total_revenue."""
        summary = get_weekly_summary(*week3_range)
        impact = compute_revenue_impact(*week3_range)
        assert abs(impact["total_revenue"] - summary["total_revenue"]) < 0.02

    def test_top_5_offers_at_most_five(self, full_range) -> None:
        """Top offers list must have at most 5 entries."""
        result = compute_revenue_impact(*full_range)
        assert len(result["top_5_offers"]) <= 5

    def test_top_5_sorted_by_revenue(self, full_range) -> None:
        """Top offers must be sorted descending by revenue."""
        result = compute_revenue_impact(*full_range)
        revenues = [o["revenue"] for o in result["top_5_offers"]]
        assert revenues == sorted(revenues, reverse=True)

    def test_segment_revenue_sum_reconciles(self, week3_range) -> None:
        """Sum of per-segment revenue must equal total_revenue."""
        result = compute_revenue_impact(*week3_range)
        seg_sum = sum(r["revenue"] for r in result["revenue_by_segment"])
        assert abs(seg_sum - result["total_revenue"]) < 0.02

    def test_customers_generating_revenue_positive(self, week3_range) -> None:
        """There must be at least one revenue-generating customer in week 3."""
        result = compute_revenue_impact(*week3_range)
        assert result["customers_generating_revenue"] > 0

    def test_empty_range_returns_zeros(self, empty_range) -> None:
        """No accepted assignments in range must return zeros without error."""
        result = compute_revenue_impact(*empty_range)
        assert result["total_revenue"] == 0.0
        assert result["top_5_offers"] == []
