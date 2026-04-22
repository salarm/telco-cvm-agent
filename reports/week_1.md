I now have all the data needed. Here is the report:

---

# CVM Weekly Report — 2026-03-23 to 2026-03-29

## Executive Summary
Zero offers were assigned to any customer this week, resulting in £0 revenue and 0% acceptance and viewed rates across all channels and segments — a complete campaign execution failure against a base of 1,000 active customers. Performance is critically off-track: no CVM activity ran for the entire seven-day period. The immediate priority for next week is to diagnose the assignment pipeline failure and execute emergency retention offers to the 616 customers (61.6% of the base) currently projected to exhaust their plan allowance before cycle end.

## What Worked
1. **Customer base is intact** — 1,000 active customers were registered in the platform, confirming the underlying data feed is live and the addressable audience is available for next week's campaigns.
2. **Burn-pattern detection is operational** — The burn-pattern model successfully profiled all 1,000 customers and identified 616 at-risk individuals, demonstrating that the analytics layer is functioning even though the campaign layer was not.
3. **At-risk segmentation is granular** — Burn risk is bucketed across all four value segments (premium: 70, high: 142, mid: 241, low: 163) and all four plan tiers (max: 64, prime: 186, saver: 214, starter: 152), giving next week's team a ready-made targeting list with no additional data prep required.

## What Didn't
1. **No offers assigned — zero campaign execution** — Total assignments for the week: 0. Viewed rate: 0.0%. Acceptance rate: 0.0%. Revenue: £0. Every CVM output metric is at absolute zero, indicating the assignment engine did not trigger at all.
2. **61.6% of the base is burning toward plan limits with no intervention** — 616 of 1,000 customers are on track to exceed 85% of their plan allowance before cycle end. With no offers deployed this week, these customers received no upsell or retention touchpoint, elevating churn and overage-churn risk simultaneously.
3. **Churn-risk comparison is unavailable** — The responder vs. non-responder churn-risk split (mean and median) both returned null, as expected when acceptance is zero. This means one full week of behavioural signal for model retraining has been lost.

## Anomalies & Questions
- **Assignment engine silent for a full week** — With 1,000 active customers and an operational analytics layer, zero assignments is not a marginal shortfall; it implies a pipeline, scheduling, or configuration break. Root cause must be confirmed before next week's run: was it a scheduler failure, a decisioning-engine outage, or a data dependency block?
- **Segment and channel breakdowns returned empty arrays** — All four calls (`value_segment`, `channel`, `get_offer_performance`, `get_uptake_by_channel`) returned no rows, which is consistent with zero assignments but should be verified to ensure these endpoints themselves are not masking a data ingestion issue.

## Recommended Actions for Next Week
1. **Resolve the assignment pipeline failure before Monday** — Conduct an immediate post-mortem on the decisioning/scheduling engine; confirm the fix is validated in staging before the next campaign window opens. Target: full assignment volume restored to ≥1,000 customers by 2026-03-30.
2. **Launch emergency retention offers to the 616 at-risk customers** — Prioritise the 241 mid-segment / 214 saver-tier customers (largest at-risk cohorts) with data top-up or plan-upgrade offers via the highest-performing channel from prior weeks; measure acceptance within 48 hours to recover lost weekly revenue.
3. **Double-up offer frequency next week to compensate for the missed week** — Where contact-policy rules permit, increase cadence for premium (70 at-risk) and high (142 at-risk) value segments specifically, as these represent the greatest revenue-per-customer recovery opportunity and cannot absorb a second consecutive week without engagement.