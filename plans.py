"""
Plan definitions.

Two paid tiers:
  solo  — 1 seat (Solopreneur), full tool access, unlimited calls
  team  — 2-50 seats (Team), full tool access per seat, unlimited calls

Admin accounts bypass all subscription checks.
"""
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Plan:
    id: str
    name: str
    display_name: str
    # None = unlimited
    monthly_tool_calls: Optional[int]
    # None = unlimited concurrent sessions
    max_sessions: Optional[int]
    # Tool access level: "read" | "all"
    tool_access: str
    # Trial length in days (0 = no trial)
    trial_days: int
    # Max seats (1 for solo, None for team — controlled by Team.max_seats)
    max_seats: int
    # Whether team management features are available
    has_team_dashboard: bool


# ---------------------------------------------------------------------------
# Defined plans
# ---------------------------------------------------------------------------

PLANS: dict[str, Plan] = {
    "free_trial": Plan(
        id="free_trial",
        name="free_trial",
        display_name="Free Trial",
        monthly_tool_calls=250,
        max_sessions=1,
        tool_access="read",
        trial_days=14,
        max_seats=1,
        has_team_dashboard=False,
    ),
    # Solopreneur — 1 user, 1 MCP credential, full access
    "solo": Plan(
        id="solo",
        name="solo",
        display_name="Solopreneur",
        monthly_tool_calls=None,
        max_sessions=1,
        tool_access="all",
        trial_days=0,
        max_seats=1,
        has_team_dashboard=False,
    ),
    # Team — N seats (controlled by Team.max_seats), full access + team dashboard
    "team": Plan(
        id="team",
        name="team",
        display_name="Team",
        monthly_tool_calls=None,
        max_sessions=1,       # per seat — each credential allows 1 active connection
        tool_access="all",
        trial_days=0,
        max_seats=50,         # hard cap; actual seat count set per subscription
        has_team_dashboard=True,
    ),
    # Legacy plan name kept for backwards compat
    "pro": Plan(
        id="pro",
        name="pro",
        display_name="Pro",
        monthly_tool_calls=None,
        max_sessions=None,
        tool_access="all",
        trial_days=0,
        max_seats=1,
        has_team_dashboard=False,
    ),
}

# Airwallex product/price IDs — override via env vars AIRWALLEX_SOLO_PRICE_ID,
# AIRWALLEX_TEAM_PRICE_ID if you have separate price IDs.
SOLO_PLAN_ID = "solo"
TEAM_PLAN_ID = "team"
# Legacy mapping (the existing Airwallex product maps to "solo")
PAID_PLAN_ID = "solo"
DEFAULT_TRIAL_PLAN_ID = "free_trial"


def get_plan(plan_id: str) -> Plan:
    return PLANS.get(plan_id, PLANS[DEFAULT_TRIAL_PLAN_ID])


def is_write_allowed(plan_id: str) -> bool:
    return get_plan(plan_id).tool_access == "all"


def is_within_call_limit(plan_id: str, calls_this_month: int) -> bool:
    plan = get_plan(plan_id)
    if plan.monthly_tool_calls is None:
        return True
    return calls_this_month < plan.monthly_tool_calls


def is_team_plan(plan_id: str) -> bool:
    return get_plan(plan_id).has_team_dashboard
