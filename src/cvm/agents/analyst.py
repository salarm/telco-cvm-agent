"""CVM Analyst agent — drives the weekly narrative report via the Claude Agent SDK."""

import asyncio
import json
from datetime import date
from pathlib import Path
from typing import Any

import claude_agent_sdk as sdk

from cvm.tools.data_tools import (
    compute_revenue_impact,
    get_burn_patterns,
    get_offer_performance,
    get_segment_breakdown,
    get_uptake_by_channel,
    get_weekly_summary,
)

_PROMPTS_DIR = Path(__file__).resolve().parent.parent / "prompts"


def _ok(data: Any) -> dict[str, Any]:
    """Wrap a JSON-serialisable value in the MCP tool-result envelope."""
    return {
        "content": [{"type": "text", "text": json.dumps(data, indent=2, default=str)}]
    }


# ---------------------------------------------------------------------------
# In-process MCP tool definitions
# ---------------------------------------------------------------------------

_DATE_SCHEMA = {
    "week_start": sdk.Annotated[str, "ISO-8601 date (YYYY-MM-DD)"],
    "week_end": sdk.Annotated[str, "ISO-8601 date (YYYY-MM-DD)"],
}


@sdk.tool(
    "get_weekly_summary",
    "Top-line KPIs for the week: total assignments, viewed/acceptance rates, "
    "revenue, active customers, churn-risk distribution of responders vs non-responders.",
    _DATE_SCHEMA,
)
async def _weekly_summary_tool(args: dict[str, Any]) -> dict[str, Any]:
    """MCP wrapper for get_weekly_summary."""
    return _ok(
        get_weekly_summary(
            date.fromisoformat(args["week_start"]),
            date.fromisoformat(args["week_end"]),
        )
    )


@sdk.tool(
    "get_segment_breakdown",
    "Break acceptance rate and revenue by a single customer dimension. "
    "dimension must be one of: value_segment, plan_tier, age_bracket, channel.",
    {
        "week_start": sdk.Annotated[str, "ISO-8601 date (YYYY-MM-DD)"],
        "week_end": sdk.Annotated[str, "ISO-8601 date (YYYY-MM-DD)"],
        "dimension": sdk.Annotated[
            str,
            "One of: value_segment, plan_tier, age_bracket, channel",
        ],
    },
)
async def _segment_breakdown_tool(args: dict[str, Any]) -> dict[str, Any]:
    """MCP wrapper for get_segment_breakdown."""
    return _ok(
        get_segment_breakdown(
            date.fromisoformat(args["week_start"]),
            date.fromisoformat(args["week_end"]),
            args["dimension"],
        )
    )


@sdk.tool(
    "get_offer_performance",
    "Per-offer metrics (shown, accepted, revenue, acceptance_rate) sorted by "
    "acceptance_rate descending.",
    _DATE_SCHEMA,
)
async def _offer_performance_tool(args: dict[str, Any]) -> dict[str, Any]:
    """MCP wrapper for get_offer_performance."""
    return _ok(
        get_offer_performance(
            date.fromisoformat(args["week_start"]),
            date.fromisoformat(args["week_end"]),
        )
    )


@sdk.tool(
    "get_uptake_by_channel",
    "Assignment volumes, viewed rates, and acceptance rates split by delivery "
    "channel: app_banner, sms, in_app_push.",
    _DATE_SCHEMA,
)
async def _uptake_by_channel_tool(args: dict[str, Any]) -> dict[str, Any]:
    """MCP wrapper for get_uptake_by_channel."""
    return _ok(
        get_uptake_by_channel(
            date.fromisoformat(args["week_start"]),
            date.fromisoformat(args["week_end"]),
        )
    )


@sdk.tool(
    "get_burn_patterns",
    "Customers projected to exceed 85 % of their plan allowance before cycle end, "
    "bucketed by value_segment and plan_tier. Useful for sizing next-week retention opportunity.",
    _DATE_SCHEMA,
)
async def _burn_patterns_tool(args: dict[str, Any]) -> dict[str, Any]:
    """MCP wrapper for get_burn_patterns."""
    return _ok(
        get_burn_patterns(
            date.fromisoformat(args["week_start"]),
            date.fromisoformat(args["week_end"]),
        )
    )


@sdk.tool(
    "compute_revenue_impact",
    "Total accepted-offer revenue, top-5 revenue-generating offers, distinct "
    "customers generating revenue, and revenue broken down by value_segment.",
    _DATE_SCHEMA,
)
async def _revenue_impact_tool(args: dict[str, Any]) -> dict[str, Any]:
    """MCP wrapper for compute_revenue_impact."""
    return _ok(
        compute_revenue_impact(
            date.fromisoformat(args["week_start"]),
            date.fromisoformat(args["week_end"]),
        )
    )


# ---------------------------------------------------------------------------
# Agent runner
# ---------------------------------------------------------------------------

_ALL_TOOLS = [
    _weekly_summary_tool,
    _segment_breakdown_tool,
    _offer_performance_tool,
    _uptake_by_channel_tool,
    _burn_patterns_tool,
    _revenue_impact_tool,
]

_TOOL_NAMES = [
    "get_weekly_summary",
    "get_segment_breakdown",
    "get_offer_performance",
    "get_uptake_by_channel",
    "get_burn_patterns",
    "compute_revenue_impact",
]


async def _run_async(week_start: date, week_end: date) -> str:
    """Execute the analyst agent and return the final markdown report string.

    Creates an in-process MCP server with the six data tools, configures
    ``query()`` with the analyst system prompt, and iterates the message stream
    to extract the final ``ResultMessage.result`` text.
    """
    system_prompt = (_PROMPTS_DIR / "analyst_system.md").read_text()

    cvm_server = sdk.create_sdk_mcp_server("cvm_data", tools=_ALL_TOOLS)

    options = sdk.ClaudeAgentOptions(
        system_prompt=system_prompt,
        mcp_servers={"cvm": cvm_server},
        allowed_tools=_TOOL_NAMES,
        permission_mode="bypassPermissions",
        max_turns=30,
    )

    prompt = f"Produce a CVM weekly report for the week {week_start} to {week_end}."

    report_parts: list[str] = []

    async for msg in sdk.query(prompt=prompt, options=options):
        if isinstance(msg, sdk.ResultMessage):
            if msg.result:
                return msg.result
            break
        if isinstance(msg, sdk.AssistantMessage):
            for block in msg.content:
                if isinstance(block, sdk.TextBlock):
                    report_parts.append(block.text)

    return "\n".join(report_parts)


def run_analyst(week_start: date, week_end: date) -> str:
    """Run the CVM Analyst agent and return the weekly markdown report.

    Synchronous entry point for CLI callers.  Spins up a fresh asyncio event
    loop per call, which is safe for single-threaded scripts.
    """
    return asyncio.run(_run_async(week_start, week_end))
