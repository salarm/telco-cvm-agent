"""Pydantic v2 domain models for the telco CVM simulator."""

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field


class Plan(BaseModel):
    """A mobile subscription plan with pricing and allowances."""

    plan_id: str
    name: str
    monthly_price: float
    data_gb: float
    voice_min: int
    sms_count: int
    tier: Literal["starter", "saver", "prime", "max"]


class Customer(BaseModel):
    """A telco subscriber with demographic, plan, and risk attributes."""

    customer_id: str
    msisdn_hash: str
    plan_id: str
    activation_date: date
    tenure_months: int
    age_bracket: Literal["18-24", "25-34", "35-44", "45-54", "55+"]
    city: str
    value_segment: Literal["low", "mid", "high", "premium"]
    churn_risk: float = Field(ge=0.0, le=1.0)


class UsageDay(BaseModel):
    """One day of network usage for a single customer."""

    date: date
    customer_id: str
    data_mb: float
    voice_min: float
    sms_count: int


class Offer(BaseModel):
    """A marketing offer targeted at one or more plan-tier segments."""

    offer_id: str
    offer_type: Literal["data_topup", "voice_topup", "combo", "discount"]
    payload_data_mb: int | None = None
    payload_voice_min: int | None = None
    payload_sms_count: int | None = None
    original_price: float
    offered_price: float
    valid_from: date
    valid_to: date
    target_plan_tiers: list[str]


class OfferAssignment(BaseModel):
    """Lifecycle record of a single offer presented to a customer."""

    assignment_id: str
    customer_id: str
    offer_id: str
    channel: Literal["app_banner", "sms", "in_app_push"]
    assigned_at: datetime
    shown_at: datetime | None = None
    decision: Literal[
        "viewed_accepted",
        "viewed_rejected",
        "viewed_ignored",
        "not_viewed",
        "expired",
    ]
    decision_at: datetime | None = None
    revenue: float


class Campaign(BaseModel):
    """A marketing campaign bundling offers for a target customer segment."""

    campaign_id: str
    campaign_type: Literal["sms_blast", "app_push", "in_app_banner_refresh"]
    start_date: date
    end_date: date
    target_segment: str
    offer_ids: list[str]
