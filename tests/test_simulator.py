"""Tests for the CVM simulator: validation, reproducibility, and referential integrity."""

from datetime import date

import pytest
from pydantic import ValidationError

from cvm.simulator.entities import (
    Campaign,
    Customer,
    Offer,
    OfferAssignment,
    Plan,
    UsageDay,
)
from cvm.simulator.generator import (
    generate_assignments,
    generate_campaigns,
    generate_customers,
    generate_offers,
    generate_plans,
    generate_usage,
)


# ---------------------------------------------------------------------------
# Model validation
# ---------------------------------------------------------------------------


class TestPlanValidation:
    def test_valid_plan(self) -> None:
        """A fully specified plan with a valid tier must parse without error."""
        plan = Plan(
            plan_id="P001",
            name="Starter",
            monthly_price=10.0,
            data_gb=2.0,
            voice_min=100,
            sms_count=200,
            tier="starter",
        )
        assert plan.plan_id == "P001"
        assert plan.tier == "starter"

    def test_invalid_tier_raises(self) -> None:
        """An unrecognised tier literal must raise a ValidationError."""
        with pytest.raises(ValidationError):
            Plan(
                plan_id="P001",
                name="Starter",
                monthly_price=10.0,
                data_gb=2.0,
                voice_min=100,
                sms_count=200,
                tier="ultra",  # type: ignore[arg-type]
            )


class TestCustomerValidation:
    def test_valid_customer(self) -> None:
        """A customer with churn_risk on the boundary [0, 1] must be accepted."""
        customer = Customer(
            customer_id="C000001",
            msisdn_hash="abc123def456",
            plan_id="P002",
            activation_date=date(2023, 6, 1),
            tenure_months=12,
            age_bracket="25-34",
            city="Karachi",
            value_segment="mid",
            churn_risk=0.0,
        )
        assert customer.churn_risk == 0.0

    def test_churn_risk_above_one_raises(self) -> None:
        """churn_risk > 1.0 must raise a ValidationError."""
        with pytest.raises(ValidationError):
            Customer(
                customer_id="C000001",
                msisdn_hash="abc123def456",
                plan_id="P002",
                activation_date=date(2023, 6, 1),
                tenure_months=12,
                age_bracket="25-34",
                city="Karachi",
                value_segment="mid",
                churn_risk=1.5,
            )

    def test_churn_risk_below_zero_raises(self) -> None:
        """churn_risk < 0.0 must raise a ValidationError."""
        with pytest.raises(ValidationError):
            Customer(
                customer_id="C000001",
                msisdn_hash="abc123def456",
                plan_id="P002",
                activation_date=date(2023, 6, 1),
                tenure_months=12,
                age_bracket="25-34",
                city="Karachi",
                value_segment="mid",
                churn_risk=-0.1,
            )

    def test_invalid_value_segment_raises(self) -> None:
        """An unrecognised value_segment must raise a ValidationError."""
        with pytest.raises(ValidationError):
            Customer(
                customer_id="C000001",
                msisdn_hash="abc123def456",
                plan_id="P002",
                activation_date=date(2023, 6, 1),
                tenure_months=12,
                age_bracket="25-34",
                city="Karachi",
                value_segment="elite",  # type: ignore[arg-type]
                churn_risk=0.3,
            )


class TestOfferValidation:
    def test_valid_discount_offer(self) -> None:
        """A discount offer with no payload fields must be valid."""
        offer = Offer(
            offer_id="OFR_DC_01",
            offer_type="discount",
            original_price=20.0,
            offered_price=17.0,
            valid_from=date(2025, 1, 1),
            valid_to=date(2025, 1, 31),
            target_plan_tiers=["saver"],
        )
        assert offer.payload_data_mb is None

    def test_invalid_offer_type_raises(self) -> None:
        """An unrecognised offer_type must raise a ValidationError."""
        with pytest.raises(ValidationError):
            Offer(
                offer_id="OFR_XX_01",
                offer_type="free_phone",  # type: ignore[arg-type]
                original_price=20.0,
                offered_price=17.0,
                valid_from=date(2025, 1, 1),
                valid_to=date(2025, 1, 31),
                target_plan_tiers=["saver"],
            )


# ---------------------------------------------------------------------------
# Reproducibility
# ---------------------------------------------------------------------------


class TestReproducibility:
    def test_same_seed_yields_identical_customers(self) -> None:
        """Two calls with the same seed must produce byte-for-byte identical output."""
        run_a = generate_customers(n=100, seed=42)
        run_b = generate_customers(n=100, seed=42)
        assert [c.customer_id for c in run_a] == [c.customer_id for c in run_b]
        assert [c.churn_risk for c in run_a] == [c.churn_risk for c in run_b]
        assert [c.plan_id for c in run_a] == [c.plan_id for c in run_b]

    def test_different_seed_yields_different_customers(self) -> None:
        """Different seeds must produce different churn_risk distributions."""
        run_a = generate_customers(n=100, seed=42)
        run_b = generate_customers(n=100, seed=99)
        assert [c.churn_risk for c in run_a] != [c.churn_risk for c in run_b]


# ---------------------------------------------------------------------------
# UsageDay coverage
# ---------------------------------------------------------------------------


class TestUsage:
    def test_one_usage_day_per_customer_per_day(self) -> None:
        """Each (customer_id, date) pair must appear exactly once in usage output."""
        plans = generate_plans()
        customers = generate_customers(n=50, seed=42)
        usage = generate_usage(customers, plans, days=7, seed=42)

        seen: set[tuple[str, date]] = set()
        for u in usage:
            key = (u.customer_id, u.date)
            assert key not in seen, f"Duplicate usage record for {key}"
            seen.add(key)

    def test_usage_record_count(self) -> None:
        """Total usage records must equal n_customers × days."""
        plans = generate_plans()
        customers = generate_customers(n=20, seed=7)
        usage = generate_usage(customers, plans, days=5, seed=7)
        assert len(usage) == 20 * 5

    def test_usage_non_negative(self) -> None:
        """All usage values must be non-negative."""
        plans = generate_plans()
        customers = generate_customers(n=30, seed=1)
        usage = generate_usage(customers, plans, days=7, seed=1)
        for u in usage:
            assert u.data_mb >= 0.0
            assert u.voice_min >= 0.0
            assert u.sms_count >= 0


# ---------------------------------------------------------------------------
# Referential integrity
# ---------------------------------------------------------------------------


class TestReferentialIntegrity:
    @pytest.fixture(scope="class")
    def generated(self) -> dict:
        """Shared fixture: generate a small but complete dataset once per class."""
        plans = generate_plans()
        customers = generate_customers(n=200, seed=42)
        usage = generate_usage(customers, plans, days=30, seed=42)
        offers = generate_offers(plans)
        campaigns = generate_campaigns(date.today(), offers)
        assignments = generate_assignments(customers, offers, campaigns, usage, seed=42)
        return {
            "customers": customers,
            "offers": offers,
            "campaigns": campaigns,
            "assignments": assignments,
        }

    def test_no_orphan_customer_in_assignments(self, generated: dict) -> None:
        """Every assignment.customer_id must reference a known customer."""
        known_ids = {c.customer_id for c in generated["customers"]}
        for a in generated["assignments"]:
            assert a.customer_id in known_ids, f"Orphan customer_id: {a.customer_id}"

    def test_no_orphan_offer_in_assignments(self, generated: dict) -> None:
        """Every assignment.offer_id must reference a known offer."""
        known_ids = {o.offer_id for o in generated["offers"]}
        for a in generated["assignments"]:
            assert a.offer_id in known_ids, f"Orphan offer_id: {a.offer_id}"

    def test_offer_count(self, generated: dict) -> None:
        """generate_offers must return exactly 20 offers."""
        assert len(generated["offers"]) == 20

    def test_campaign_count(self, generated: dict) -> None:
        """generate_campaigns must return exactly 3 campaigns."""
        assert len(generated["campaigns"]) == 3

    def test_assignment_decisions_are_valid(self, generated: dict) -> None:
        """All assignment decisions must be one of the five permitted literals."""
        valid_decisions = {
            "viewed_accepted",
            "viewed_rejected",
            "viewed_ignored",
            "not_viewed",
            "expired",
        }
        for a in generated["assignments"]:
            assert a.decision in valid_decisions

    def test_revenue_only_on_accepted(self, generated: dict) -> None:
        """Revenue must be positive only for viewed_accepted decisions."""
        for a in generated["assignments"]:
            if a.decision == "viewed_accepted":
                assert a.revenue > 0.0
            else:
                assert a.revenue == 0.0


# ---------------------------------------------------------------------------
# Entity model smoke tests (round-trip via model_dump)
# ---------------------------------------------------------------------------


class TestModelRoundTrip:
    def test_plan_roundtrip(self) -> None:
        """Plans must survive a model_dump / model_validate round-trip."""
        for plan in generate_plans():
            assert Plan.model_validate(plan.model_dump()) == plan

    def test_offer_roundtrip(self) -> None:
        """Offers must survive a model_dump / model_validate round-trip."""
        plans = generate_plans()
        for offer in generate_offers(plans):
            assert Offer.model_validate(offer.model_dump()) == offer


# ---------------------------------------------------------------------------
# Unused model import check (ensures entities module exports are importable)
# ---------------------------------------------------------------------------


def test_all_entities_importable() -> None:
    """All six entity classes must be importable from cvm.simulator.entities."""
    assert all(
        cls is not None
        for cls in (Plan, Customer, UsageDay, Offer, OfferAssignment, Campaign)
    )
