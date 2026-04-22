"""Synthetic data generation for the telco CVM simulator."""

import hashlib
import os
import uuid
from datetime import date, datetime, timedelta

import numpy as np
import polars as pl
from faker import Faker

from cvm.simulator.entities import (
    Campaign,
    Customer,
    Offer,
    OfferAssignment,
    Plan,
    UsageDay,
)

_CITIES: list[str] = [
    "Karachi",
    "Lahore",
    "Islamabad",
    "Rawalpindi",
    "Faisalabad",
    "Multan",
    "Peshawar",
    "Quetta",
    "Hyderabad",
    "Gujranwala",
]

# Monthly data allowances in MB per plan — mirrors Plan.data_gb without requiring plans list.
_PLAN_DATA_MB: dict[str, float] = {
    "P001": 2.0 * 1024,
    "P002": 5.0 * 1024,
    "P003": 15.0 * 1024,
    "P004": 40.0 * 1024,
}


def generate_plans() -> list[Plan]:
    """Return four canonical subscription plans covering entry through premium tier."""
    return [
        Plan(
            plan_id="P001",
            name="Starter",
            monthly_price=10.0,
            data_gb=2.0,
            voice_min=100,
            sms_count=200,
            tier="starter",
        ),
        Plan(
            plan_id="P002",
            name="Saver",
            monthly_price=20.0,
            data_gb=5.0,
            voice_min=300,
            sms_count=500,
            tier="saver",
        ),
        Plan(
            plan_id="P003",
            name="Prime",
            monthly_price=35.0,
            data_gb=15.0,
            voice_min=1000,
            sms_count=1000,
            tier="prime",
        ),
        Plan(
            plan_id="P004",
            name="Max",
            monthly_price=50.0,
            data_gb=40.0,
            voice_min=2000,
            sms_count=5000,
            tier="max",
        ),
    ]


def generate_customers(n: int = 1000, seed: int = 42) -> list[Customer]:
    """Generate n synthetic customers with realistic plan and demographic distributions.

    Plan distribution skews mid-tier; value_segment is correlated with plan tier;
    churn_risk is inversely correlated with tenure and value_segment.
    """
    rng = np.random.default_rng(seed)
    fake = Faker()
    fake.seed_instance(seed)

    plan_ids = ["P001", "P002", "P003", "P004"]
    plan_weights = [0.25, 0.35, 0.30, 0.10]

    age_brackets: list[str] = ["18-24", "25-34", "35-44", "45-54", "55+"]
    age_weights = [0.20, 0.35, 0.25, 0.12, 0.08]

    value_by_plan: dict[str, tuple[list[str], list[float]]] = {
        "P001": (["low", "mid", "high", "premium"], [0.60, 0.30, 0.08, 0.02]),
        "P002": (["low", "mid", "high", "premium"], [0.25, 0.50, 0.20, 0.05]),
        "P003": (["low", "mid", "high", "premium"], [0.10, 0.35, 0.40, 0.15]),
        "P004": (["low", "mid", "high", "premium"], [0.05, 0.15, 0.45, 0.35]),
    }

    churn_base: dict[str, float] = {
        "low": 0.65,
        "mid": 0.40,
        "high": 0.20,
        "premium": 0.10,
    }
    tenure_base: dict[str, int] = {
        "low": 6,
        "mid": 18,
        "high": 30,
        "premium": 48,
    }

    today = date.today()
    customers: list[Customer] = []

    for i in range(n):
        plan_idx = int(rng.choice(len(plan_ids), p=plan_weights))
        plan_id = plan_ids[plan_idx]

        age_idx = int(rng.choice(len(age_brackets), p=age_weights))
        age_bracket = age_brackets[age_idx]

        segs, seg_weights = value_by_plan[plan_id]
        seg_idx = int(rng.choice(len(segs), p=seg_weights))
        value_segment = segs[seg_idx]

        t_base = tenure_base[value_segment]
        tenure_months = int(max(1, rng.normal(t_base, t_base * 0.4)))
        activation_date = today - timedelta(days=tenure_months * 30)

        churn = float(np.clip(rng.normal(churn_base[value_segment], 0.08), 0.0, 1.0))

        customer_id = f"C{i + 1:06d}"
        msisdn = fake.numerify("03#########")
        msisdn_hash = hashlib.sha256(msisdn.encode()).hexdigest()[:16]
        city = _CITIES[int(rng.integers(0, len(_CITIES)))]

        customers.append(
            Customer(
                customer_id=customer_id,
                msisdn_hash=msisdn_hash,
                plan_id=plan_id,
                activation_date=activation_date,
                tenure_months=tenure_months,
                age_bracket=age_bracket,  # type: ignore[arg-type]
                city=city,
                value_segment=value_segment,  # type: ignore[arg-type]
                churn_risk=churn,
            )
        )

    return customers


def generate_usage(
    customers: list[Customer],
    plans: list[Plan],
    days: int = 30,
    seed: int = 42,
    start_date: date | None = None,
) -> list[UsageDay]:
    """Generate daily usage records for each customer over the given number of days.

    Each customer receives a latent burn profile (heavy/medium/light). Heavy users
    consume ~140 % of their monthly allowance and will exceed limits by mid-month.
    Day-of-week variation boosts data on weekends and voice on weekdays.
    """
    rng = np.random.default_rng(seed)
    plan_by_id = {p.plan_id: p for p in plans}

    burn_profiles = ["heavy", "medium", "light"]
    burn_weights = [0.25, 0.50, 0.25]

    # Mean daily fraction of (monthly allowance / days); heavy > 1 means over-limit.
    burn_rates: dict[str, tuple[float, float]] = {
        "heavy": (1.4, 0.30),
        "medium": (0.85, 0.15),
        "light": (0.35, 0.10),
    }

    if start_date is None:
        start_date = date.today() - timedelta(days=days)

    records: list[UsageDay] = []

    for customer in customers:
        plan = plan_by_id[customer.plan_id]
        profile_idx = int(rng.choice(len(burn_profiles), p=burn_weights))
        profile = burn_profiles[profile_idx]
        mean_rate, std_rate = burn_rates[profile]

        monthly_data_mb = plan.data_gb * 1024
        monthly_voice = float(plan.voice_min)
        monthly_sms = float(plan.sms_count)

        for day_offset in range(days):
            d = start_date + timedelta(days=day_offset)
            dow = d.weekday()  # 0 = Mon … 6 = Sun

            dow_data = 1.2 if dow >= 5 else 1.0
            dow_voice = 0.7 if dow >= 5 else 1.1

            daily_rate = float(np.clip(rng.normal(mean_rate, std_rate), 0.0, 2.0))

            raw_data = (monthly_data_mb / days) * daily_rate * dow_data
            data_mb = float(max(0.0, rng.normal(raw_data, raw_data * 0.2 + 1e-6)))

            raw_voice = (monthly_voice / days) * daily_rate * dow_voice
            voice_min = float(max(0.0, rng.normal(raw_voice, raw_voice * 0.2 + 1e-6)))

            raw_sms = (monthly_sms / days) * daily_rate
            sms_count = int(max(0, round(rng.normal(raw_sms, raw_sms * 0.3 + 1e-6))))

            records.append(
                UsageDay(
                    date=d,
                    customer_id=customer.customer_id,
                    data_mb=round(data_mb, 2),
                    voice_min=round(voice_min, 2),
                    sms_count=sms_count,
                )
            )

    return records


def generate_offers(plans: list[Plan]) -> list[Offer]:
    """Generate exactly 20 offers across four types, priced relative to plan prices.

    Returns 8 data topups (4 per starter/saver), 3 voice topups, 4 combos, and
    5 discounts aimed at churn-risk customers.
    """
    plan_by_tier = {p.tier: p for p in plans}
    today = date.today()
    valid_from = today - timedelta(days=5)
    valid_to = today + timedelta(days=35)

    offers: list[Offer] = []

    # 8 data topups: 4 amounts × 2 tiers (starter, saver)
    data_amounts_mb = [512, 1024, 2048, 5120]
    for tier in ("starter", "saver"):
        plan = plan_by_tier[tier]
        for idx, mb in enumerate(data_amounts_mb, start=1):
            ratio = mb / (plan.data_gb * 1024)
            base = plan.monthly_price * ratio
            offers.append(
                Offer(
                    offer_id=f"OFR_DT_{tier[:2].upper()}_{idx:02d}",
                    offer_type="data_topup",
                    payload_data_mb=mb,
                    original_price=round(base * 1.25, 2),
                    offered_price=round(base, 2),
                    valid_from=valid_from,
                    valid_to=valid_to,
                    target_plan_tiers=[tier],
                )
            )

    # 3 voice topups targeting entry tiers
    for idx, mins in enumerate((60, 150, 300), start=1):
        plan = plan_by_tier["starter"]
        ratio = mins / plan.voice_min
        base = plan.monthly_price * ratio
        offers.append(
            Offer(
                offer_id=f"OFR_VT_{idx:02d}",
                offer_type="voice_topup",
                payload_voice_min=mins,
                original_price=round(base * 1.25, 2),
                offered_price=round(base, 2),
                valid_from=valid_from,
                valid_to=valid_to,
                target_plan_tiers=["starter", "saver"],
            )
        )

    # 4 combos spanning tier boundaries
    combos: list[tuple[int, int, list[str]]] = [
        (512, 50, ["starter"]),
        (1024, 100, ["starter", "saver"]),
        (2048, 200, ["saver", "prime"]),
        (5120, 500, ["prime", "max"]),
    ]
    for idx, (data_mb, voice_min, tiers) in enumerate(combos, start=1):
        plan = plan_by_tier[tiers[0]]
        base = plan.monthly_price * 0.25
        offers.append(
            Offer(
                offer_id=f"OFR_CB_{idx:02d}",
                offer_type="combo",
                payload_data_mb=data_mb,
                payload_voice_min=voice_min,
                original_price=round(base * 1.30, 2),
                offered_price=round(base, 2),
                valid_from=valid_from,
                valid_to=valid_to,
                target_plan_tiers=tiers,
            )
        )

    # 5 discounts for churn prevention
    discounts: list[tuple[float, list[str]]] = [
        (0.10, ["starter"]),
        (0.15, ["saver"]),
        (0.10, ["prime"]),
        (0.20, ["starter", "saver"]),
        (0.15, ["prime", "max"]),
    ]
    for idx, (pct, tiers) in enumerate(discounts, start=1):
        plan = plan_by_tier[tiers[0]]
        original = plan.monthly_price
        offers.append(
            Offer(
                offer_id=f"OFR_DC_{idx:02d}",
                offer_type="discount",
                original_price=original,
                offered_price=round(original * (1 - pct), 2),
                valid_from=valid_from,
                valid_to=valid_to,
                target_plan_tiers=tiers,
            )
        )

    return offers  # 8 + 3 + 4 + 5 = 20


def generate_campaigns(start_date: date, offers: list[Offer]) -> list[Campaign]:
    """Return 2 SMS blast campaigns and 1 continuous in-app banner refresh campaign.

    SMS blast 1 (day 12) targets high-usage customers with topup offers.
    SMS blast 2 (day 28) targets churn-risk customers with discount/combo offers.
    The banner runs the full month surfacing combos and discounts to all segments.
    """
    topup_ids = [
        o.offer_id for o in offers if o.offer_type in ("data_topup", "voice_topup")
    ]
    combo_ids = [o.offer_id for o in offers if o.offer_type == "combo"]
    discount_ids = [o.offer_id for o in offers if o.offer_type == "discount"]

    day12 = start_date + timedelta(days=11)
    day28 = start_date + timedelta(days=27)
    month_end = start_date + timedelta(days=29)

    return [
        Campaign(
            campaign_id="CAMP_SMS_01",
            campaign_type="sms_blast",
            start_date=day12,
            end_date=day12,
            target_segment="high_usage",
            offer_ids=topup_ids[:5],
        ),
        Campaign(
            campaign_id="CAMP_SMS_02",
            campaign_type="sms_blast",
            start_date=day28,
            end_date=day28,
            target_segment="churn_risk",
            offer_ids=(discount_ids + combo_ids)[:5],
        ),
        Campaign(
            campaign_id="CAMP_BANNER_01",
            campaign_type="in_app_banner_refresh",
            start_date=start_date,
            end_date=month_end,
            target_segment="all",
            offer_ids=(combo_ids + discount_ids)[:6],
        ),
    ]


def generate_assignments(
    customers: list[Customer],
    offers: list[Offer],
    campaigns: list[Campaign],  # noqa: ARG001
    usage: list[UsageDay],
    seed: int = 42,
) -> list[OfferAssignment]:
    """Generate rule-based offer assignments driven by burn rate, value, and churn risk.

    Customers projected to exhaust data before month-end receive topup offers.
    High churn-risk customers receive discount offers. Acceptance probability rises
    with value_segment and churn_risk: high-value + high-churn are most likely to accept.
    """
    rng = np.random.default_rng(seed)

    plan_by_cid = {c.customer_id: c.plan_id for c in customers}
    value_by_cid = {c.customer_id: c.value_segment for c in customers}
    churn_by_cid = {c.customer_id: c.churn_risk for c in customers}

    all_dates = sorted({u.date for u in usage})
    if not all_dates:
        return []
    midpoint = all_dates[len(all_dates) // 2]

    half_usage: dict[str, float] = {}
    for u in usage:
        if u.date <= midpoint:
            half_usage[u.customer_id] = half_usage.get(u.customer_id, 0.0) + u.data_mb

    offers_by_type: dict[str, list[Offer]] = {}
    for o in offers:
        offers_by_type.setdefault(o.offer_type, []).append(o)

    # Base acceptance probability by value segment (before churn modulation).
    accept_base: dict[str, float] = {
        "low": 0.12,
        "mid": 0.28,
        "high": 0.45,
        "premium": 0.40,
    }

    channels: list[str] = ["app_banner", "sms", "in_app_push"]
    base_dt = datetime.combine(midpoint, datetime.min.time())

    assignments: list[OfferAssignment] = []

    for customer in customers:
        cid = customer.customer_id
        plan_limit = _PLAN_DATA_MB[plan_by_cid[cid]]
        projected = half_usage.get(cid, 0.0) * 2.0
        churn = churn_by_cid[cid]
        value_seg = value_by_cid[cid]

        relevant_types: list[str] = []
        if projected > plan_limit * 0.90:
            relevant_types.append("data_topup")
        if churn > 0.40:
            relevant_types.append("discount")
        if rng.random() < 0.25:
            relevant_types.append("combo")
        if not relevant_types:
            continue

        n_offers = int(rng.choice([1, 2], p=[0.70, 0.30]))
        p_accept = float(np.clip(accept_base[value_seg] * (1.0 + churn), 0.0, 0.90))
        p_shown = 0.75

        for _ in range(n_offers):
            offer_type = relevant_types[int(rng.integers(0, len(relevant_types)))]
            type_offers = offers_by_type.get(offer_type, [])
            if not type_offers:
                continue

            offer = type_offers[int(rng.integers(0, len(type_offers)))]
            channel = channels[int(rng.integers(0, len(channels)))]
            assigned_at = base_dt + timedelta(hours=int(rng.integers(0, 48)))

            shown_at: datetime | None = None
            decision_at: datetime | None = None

            if rng.random() < p_shown:
                shown_at = assigned_at + timedelta(hours=int(rng.integers(1, 12)))
                r = float(rng.random())
                if r < p_accept:
                    decision = "viewed_accepted"
                    revenue = offer.offered_price
                elif r < p_accept + 0.25:
                    decision = "viewed_rejected"
                    revenue = 0.0
                elif r < p_accept + 0.50:
                    decision = "viewed_ignored"
                    revenue = 0.0
                else:
                    decision = "expired"
                    revenue = 0.0
                decision_at = shown_at + timedelta(hours=int(rng.integers(1, 24)))
            else:
                decision = "not_viewed"
                revenue = 0.0

            assignments.append(
                OfferAssignment(
                    assignment_id=str(uuid.uuid4()),
                    customer_id=cid,
                    offer_id=offer.offer_id,
                    channel=channel,  # type: ignore[arg-type]
                    assigned_at=assigned_at,
                    shown_at=shown_at,
                    decision=decision,  # type: ignore[arg-type]
                    decision_at=decision_at,
                    revenue=revenue,
                )
            )

    return assignments


def _to_df(records: list) -> pl.DataFrame:
    """Serialize a list of pydantic models to a polars DataFrame."""
    return pl.from_dicts([r.model_dump() for r in records])


def generate_all(output_dir: str = "data/simulated") -> None:
    """Orchestrate full data generation and write one parquet file per entity.

    Writes plans, customers, usage, offers, campaigns, and assignments to output_dir.
    All foreign keys are referentially consistent across files.
    """
    os.makedirs(output_dir, exist_ok=True)

    start_date = date.today() - timedelta(days=30)

    plans = generate_plans()
    customers = generate_customers()
    usage = generate_usage(customers, plans, start_date=start_date)
    offers = generate_offers(plans)
    campaigns = generate_campaigns(start_date, offers)
    assignments = generate_assignments(customers, offers, campaigns, usage)

    _to_df(plans).write_parquet(f"{output_dir}/plans.parquet")
    _to_df(customers).write_parquet(f"{output_dir}/customers.parquet")
    _to_df(usage).write_parquet(f"{output_dir}/usage.parquet")
    _to_df(offers).write_parquet(f"{output_dir}/offers.parquet")
    _to_df(campaigns).write_parquet(f"{output_dir}/campaigns.parquet")
    _to_df(assignments).write_parquet(f"{output_dir}/assignments.parquet")
