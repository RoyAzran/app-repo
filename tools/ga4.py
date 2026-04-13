"""
Google Analytics 4 tools — adapted for remote server.
All 22 tools from mcp-ga4/server.py.
Credentials come from current_user_ctx (per-user Google refresh token) instead of env vars.
All GA4 tools are READ-only — no require_editor() calls needed.
"""
import json
import os
from datetime import date, timedelta

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from mcp_instance import mcp
from auth import current_user_ctx

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GA4_SCOPES = [
    "https://www.googleapis.com/auth/analytics.readonly",
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
]


# ---------------------------------------------------------------------------
# Credential helpers — NO global cache; always build per-request for multi-user
# ---------------------------------------------------------------------------

def _creds() -> Credentials:
    """Return fresh OAuth2 credentials for the currently authenticated user."""
    user = current_user_ctx.get(None)
    if user is None:
        raise RuntimeError("Not authenticated.")
    rt = user.get_google_token()
    if not rt:
        raise RuntimeError("Google account not connected. Visit /onboard to connect your Google account.")
    creds = Credentials(
        token=None,
        refresh_token=rt,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        scopes=GA4_SCOPES,
    )
    creds.refresh(Request())
    return creds


def _data_svc():
    return build("analyticsdata", "v1beta", credentials=_creds())


def _admin_svc():
    return build("analyticsadmin", "v1beta", credentials=_creds())


def _resolve_property(property_id: str) -> str:
    if property_id:
        pid = property_id.strip()
        return f"properties/{pid}" if not pid.startswith("properties/") else pid
    env_id = os.environ.get("GA4_PROPERTY_ID", "")
    if env_id:
        return f"properties/{env_id}" if not env_id.startswith("properties/") else env_id
    admin = _admin_svc()
    accounts = admin.accounts().list().execute().get("accounts", [])
    for acct in accounts:
        props = admin.properties().list(filter=f"parent:{acct['name']}").execute().get("properties", [])
        if props:
            return props[0]["name"]
    raise ValueError("No GA4 property found. Set GA4_PROPERTY_ID in .env or pass property_id.")


def _run_report(property_id: str, body: dict) -> dict:
    prop = _resolve_property(property_id)
    return _data_svc().properties().runReport(property=prop, body=body).execute()


def _parse_report(response: dict) -> list:
    dim_headers = [d["name"] for d in response.get("dimensionHeaders", [])]
    met_headers = [m["name"] for m in response.get("metricHeaders", [])]
    rows = []
    for row in response.get("rows", []):
        r = {dim_headers[i]: row["dimensionValues"][i]["value"] for i in range(len(dim_headers))}
        r.update({met_headers[j]: row["metricValues"][j]["value"] for j in range(len(met_headers))})
        rows.append(r)
    return rows


# ---------------------------------------------------------------------------
# Tools
# ---------------------------------------------------------------------------

@mcp.tool()
def list_ga_properties() -> str:
    """List all Google Analytics 4 properties the user has access to."""
    try:
        admin = _admin_svc()
        accounts = admin.accounts().list().execute().get("accounts", [])
        properties = []
        for acct in accounts:
            props = admin.properties().list(filter=f"parent:{acct['name']}").execute().get("properties", [])
            for p in props:
                properties.append({
                    "property_id": p["name"].split("/")[-1],
                    "display_name": p.get("displayName", ""),
                    "account": acct.get("displayName", acct["name"]),
                })
        return json.dumps({"properties": properties, "total": len(properties)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def list_properties() -> str:
    """List all Google Analytics 4 properties the user has access to."""
    return list_ga_properties()


@mcp.tool()
def run_ga_report(
    dimensions: str,
    metrics: str,
    property_id: str = "",
    start_date: str = "",
    end_date: str = "",
    row_limit: int = 20,
    order_by_metric: str = "",
) -> str:
    """Run a custom Google Analytics 4 report with any dimensions and metrics.

    Args:
        dimensions: Comma-separated GA4 dimension names, e.g. 'date,sessionDefaultChannelGroup'.
        metrics: Comma-separated GA4 metric names, e.g. 'sessions,activeUsers,conversions'.
        property_id: GA4 property ID (numeric). Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        row_limit: Max rows. Default 20.
        order_by_metric: Metric to order results by (descending). Leave blank for default.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        dims = [{"name": d.strip()} for d in dimensions.split(",") if d.strip()]
        mets = [{"name": m.strip()} for m in metrics.split(",") if m.strip()]
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": dims, "metrics": mets, "limit": row_limit,
        }
        if order_by_metric:
            body["orderBys"] = [{"metric": {"metricName": order_by_metric}, "desc": True}]
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "rows": _parse_report(response), "row_count": response.get("rowCount", 0)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_overview(property_id: str = "", start_date: str = "", end_date: str = "") -> str:
    """Get a high-level traffic overview from GA4: sessions, users, pageviews, bounce rate, average session duration.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "metrics": [
                {"name": "sessions"}, {"name": "activeUsers"}, {"name": "screenPageViews"},
                {"name": "bounceRate"}, {"name": "averageSessionDuration"}, {"name": "newUsers"},
            ],
        }
        response = _run_report(property_id, body)
        metrics = {}
        if response.get("rows"):
            for j, m in enumerate(response.get("metricHeaders", [])):
                metrics[m["name"]] = response["rows"][0]["metricValues"][j]["value"]
        return json.dumps({"date_range": f"{sd} to {ed}", "metrics": metrics})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_traffic_sources(property_id: str = "", start_date: str = "", end_date: str = "", row_limit: int = 15) -> str:
    """Get GA4 traffic broken down by default channel group.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        row_limit: Max channels. Default 15.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": "sessionDefaultChannelGroup"}],
            "metrics": [{"name": "sessions"}, {"name": "activeUsers"}, {"name": "conversions"}, {"name": "screenPageViews"}],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": row_limit,
        }
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "channels": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_top_pages(property_id: str = "", start_date: str = "", end_date: str = "", row_limit: int = 20) -> str:
    """Get the most visited pages in GA4 by page views.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        row_limit: Max rows. Default 20.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": "pagePath"}, {"name": "pageTitle"}],
            "metrics": [{"name": "screenPageViews"}, {"name": "activeUsers"}, {"name": "averageSessionDuration"}, {"name": "bounceRate"}],
            "orderBys": [{"metric": {"metricName": "screenPageViews"}, "desc": True}],
            "limit": row_limit,
        }
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "pages": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_device_breakdown(property_id: str = "", start_date: str = "", end_date: str = "") -> str:
    """Get GA4 sessions and conversions broken down by device category: desktop, mobile, tablet.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": "deviceCategory"}],
            "metrics": [{"name": "sessions"}, {"name": "activeUsers"}, {"name": "conversions"}, {"name": "bounceRate"}],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
        }
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "devices": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_conversions(property_id: str = "", start_date: str = "", end_date: str = "", row_limit: int = 20) -> str:
    """Get GA4 conversion events with their counts and conversion rates.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        row_limit: Max conversion events. Default 20.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": "eventName"}],
            "metrics": [{"name": "conversions"}, {"name": "totalRevenue"}, {"name": "sessions"}],
            "orderBys": [{"metric": {"metricName": "conversions"}, "desc": True}],
            "limit": row_limit,
        }
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "conversions": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_ecommerce(property_id: str = "", start_date: str = "", end_date: str = "") -> str:
    """Get GA4 ecommerce overview: revenue, transactions, average order value, purchase rate.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "metrics": [
                {"name": "totalRevenue"}, {"name": "transactions"},
                {"name": "averagePurchaseRevenue"}, {"name": "purchaseToViewRate"},
                {"name": "itemRevenue"}, {"name": "itemsPurchased"},
            ],
        }
        response = _run_report(property_id, body)
        metrics: dict = {}
        if response.get("rows"):
            for j, m in enumerate(response.get("metricHeaders", [])):
                metrics[m["name"]] = response["rows"][0]["metricValues"][j]["value"]
        return json.dumps({"date_range": f"{sd} to {ed}", "ecommerce": metrics})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_realtime(property_id: str = "") -> str:
    """Get real-time active users currently on the site (last 30 minutes), broken down by page and country.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
    """
    try:
        prop = _resolve_property(property_id)
        svc = _data_svc()
        users_resp = svc.properties().runRealtimeReport(property=prop, body={"metrics": [{"name": "activeUsers"}]}).execute()
        active_users = users_resp.get("rows", [{}])[0].get("metricValues", [{}])[0].get("value", "0") if users_resp.get("rows") else "0"
        pages_resp = svc.properties().runRealtimeReport(property=prop, body={"dimensions": [{"name": "pagePath"}], "metrics": [{"name": "activeUsers"}], "limit": 10}).execute()
        geo_resp = svc.properties().runRealtimeReport(property=prop, body={"dimensions": [{"name": "country"}], "metrics": [{"name": "activeUsers"}], "limit": 10}).execute()

        def _parse_rt(resp):
            dh = [d["name"] for d in resp.get("dimensionHeaders", [])]
            mh = [m["name"] for m in resp.get("metricHeaders", [])]
            rows = []
            for row in resp.get("rows", []):
                r = {dh[i]: row["dimensionValues"][i]["value"] for i in range(len(dh))}
                r.update({mh[j]: row["metricValues"][j]["value"] for j in range(len(mh))})
                rows.append(r)
            return rows

        return json.dumps({"active_users_last_30min": active_users, "top_pages": _parse_rt(pages_resp), "by_country": _parse_rt(geo_resp)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_landing_pages(property_id: str = "", start_date: str = "", end_date: str = "", row_limit: int = 20) -> str:
    """Get GA4 landing page performance: sessions, bounce rate, conversions, engagement rate.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        row_limit: Max rows. Default 20.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": "landingPage"}],
            "metrics": [{"name": "sessions"}, {"name": "activeUsers"}, {"name": "bounceRate"}, {"name": "conversions"}, {"name": "engagementRate"}],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": row_limit,
        }
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "landing_pages": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_user_retention(property_id: str = "", start_date: str = "", end_date: str = "") -> str:
    """Get GA4 user retention metrics: new vs returning users, engagement rate, sessions per user.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 90 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=90))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": "newVsReturning"}],
            "metrics": [{"name": "activeUsers"}, {"name": "sessions"}, {"name": "engagementRate"}, {"name": "sessionsPerUser"}],
            "orderBys": [{"metric": {"metricName": "activeUsers"}, "desc": True}],
        }
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "retention": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_geo(property_id: str = "", start_date: str = "", end_date: str = "", row_limit: int = 20, breakdown: str = "country") -> str:
    """Get GA4 sessions and users broken down by geographic location.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        row_limit: Max rows. Default 20.
        breakdown: 'country', 'region', or 'city'. Default country.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": breakdown}],
            "metrics": [{"name": "sessions"}, {"name": "activeUsers"}, {"name": "conversions"}, {"name": "bounceRate"}],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": row_limit,
        }
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "breakdown_by": breakdown, "rows": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_user_journey(property_id: str = "", start_date: str = "", end_date: str = "", row_limit: int = 20) -> str:
    """Get GA4 user journey data — how users navigate through the site using page path combined with channel.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        row_limit: Max rows. Default 20.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": "pagePath"}, {"name": "sessionDefaultChannelGroup"}],
            "metrics": [{"name": "sessions"}, {"name": "activeUsers"}, {"name": "engagedSessions"}, {"name": "bounceRate"}],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": row_limit,
        }
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "user_journey": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_events(property_id: str = "", start_date: str = "", end_date: str = "", row_limit: int = 25) -> str:
    """Get all GA4 events with their counts and unique user counts.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        row_limit: Max events. Default 25.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": "eventName"}],
            "metrics": [{"name": "eventCount"}, {"name": "eventCountPerUser"}, {"name": "totalUsers"}],
            "orderBys": [{"metric": {"metricName": "eventCount"}, "desc": True}],
            "limit": row_limit,
        }
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "events": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_funnel(
    steps: list[str],
    property_id: str = "",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Simulate a conversion funnel by showing user drop-off across a series of pages.

    Args:
        steps: List of page path strings, e.g. ['/pricing', '/checkout', '/thank-you'] (required).
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        results = []
        for step in steps:
            body = {
                "dateRanges": [{"startDate": sd, "endDate": ed}],
                "dimensions": [{"name": "pagePath"}],
                "metrics": [{"name": "activeUsers"}, {"name": "sessions"}],
                "dimensionFilter": {"filter": {
                    "fieldName": "pagePath",
                    "stringFilter": {"matchType": "EXACT", "value": step},
                }},
            }
            resp = _run_report(property_id, body)
            rows = resp.get("rows", [])
            users = rows[0]["metricValues"][0]["value"] if rows else "0"
            sessions = rows[0]["metricValues"][1]["value"] if rows else "0"
            results.append({"step": step, "activeUsers": users, "sessions": sessions})
        for i in range(1, len(results)):
            prev = int(results[i - 1]["activeUsers"])
            curr = int(results[i]["activeUsers"])
            drop = round((prev - curr) / max(prev, 1) * 100, 1)
            results[i]["dropoff_pct"] = f"{drop}%"
        return json.dumps({"date_range": f"{sd} to {ed}", "funnel": results})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_audiences(property_id: str = "") -> str:
    """List GA4 audiences defined in the property.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
    """
    try:
        prop = _resolve_property(property_id)
        admin = _admin_svc()
        audiences = admin.properties().audiences().list(parent=prop).execute().get("audiences", [])
        result = [{"name": a.get("name", ""), "display_name": a.get("displayName", ""), "description": a.get("description", ""), "membership_duration_days": a.get("membershipDurationDays", 0)} for a in audiences]
        return json.dumps({"property": prop, "audiences": result, "total": len(result)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_segments(property_id: str = "", start_date: str = "", end_date: str = "", segment_filter: str = "") -> str:
    """Get GA4 performance segmented by session default channel.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        segment_filter: Channel to filter on, e.g. 'Organic Search'. Leave blank for all.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body: dict = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": "sessionDefaultChannelGroup"}, {"name": "deviceCategory"}],
            "metrics": [{"name": "sessions"}, {"name": "activeUsers"}, {"name": "conversions"}],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": 50,
        }
        if segment_filter:
            body["dimensionFilter"] = {"filter": {
                "fieldName": "sessionDefaultChannelGroup",
                "stringFilter": {"matchType": "EXACT", "value": segment_filter},
            }}
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "segments": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_acquisition(property_id: str = "", start_date: str = "", end_date: str = "", row_limit: int = 20) -> str:
    """Get GA4 user acquisition broken down by source / medium / campaign.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        row_limit: Max rows. Default 20.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": "sessionSource"}, {"name": "sessionMedium"}, {"name": "sessionCampaignName"}],
            "metrics": [{"name": "sessions"}, {"name": "activeUsers"}, {"name": "newUsers"}, {"name": "conversions"}],
            "orderBys": [{"metric": {"metricName": "sessions"}, "desc": True}],
            "limit": row_limit,
        }
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "acquisition": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_items(property_id: str = "", start_date: str = "", end_date: str = "", row_limit: int = 20) -> str:
    """Get GA4 ecommerce item performance: items viewed, added to cart, purchased, and revenue per item.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        row_limit: Max items. Default 20.
    """
    try:
        ed = end_date or str(date.today())
        sd = start_date or str(date.today() - timedelta(days=28))
        body = {
            "dateRanges": [{"startDate": sd, "endDate": ed}],
            "dimensions": [{"name": "itemId"}, {"name": "itemName"}],
            "metrics": [{"name": "itemsViewed"}, {"name": "itemsAddedToCart"}, {"name": "itemsPurchased"}, {"name": "itemRevenue"}],
            "orderBys": [{"metric": {"metricName": "itemRevenue"}, "desc": True}],
            "limit": row_limit,
        }
        response = _run_report(property_id, body)
        return json.dumps({"date_range": f"{sd} to {ed}", "items": _parse_report(response)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@mcp.tool()
def get_ga_metadata(property_id: str = "") -> str:
    """Get the list of all available dimensions and metrics for a GA4 property.

    Args:
        property_id: GA4 property ID. Leave blank to use the default property.
    """
    try:
        prop = _resolve_property(property_id)
        svc = _data_svc()
        metadata = svc.properties().getMetadata(name=f"{prop}/metadata").execute()
        dimensions = [{"api_name": d["apiName"], "ui_name": d.get("uiName", ""), "description": d.get("description", "")} for d in metadata.get("dimensions", [])]
        metrics = [{"api_name": m["apiName"], "ui_name": m.get("uiName", ""), "description": m.get("description", "")} for m in metadata.get("metrics", [])]
        return json.dumps({"property": prop, "dimensions": dimensions, "metrics": metrics})
    except Exception as e:
        return json.dumps({"error": str(e)})
