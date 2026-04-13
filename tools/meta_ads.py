#!/usr/bin/env python3
"""
Meta Ads MCP Server — 58 tools

Analytics (read): get_meta_overview, get_meta_campaigns, get_meta_adsets, get_meta_daily_trend,
  get_meta_ads, get_meta_demographics, get_meta_placements, get_meta_geo,
  get_meta_video_performance, get_meta_conversion_funnel, get_meta_frequency,
  get_meta_hourly_trend, meta_list_ad_accounts, meta_get_audiences, meta_get_pages,
  meta_get_pixels, meta_get_catalog, meta_get_creative_previews, meta_get_page_insights,
  meta_get_reach_estimate, meta_get_targeting_search, meta_get_pixel_stats,
  meta_get_custom_conversions, meta_get_ad_creatives

Listing (no dates): meta_list_campaigns, meta_list_adsets

Ads Management (write): meta_update_status, meta_update_budget, meta_create_campaign,
  meta_create_adset, meta_create_ad_creative, meta_upload_ad_video, meta_create_video_ad_creative,
  meta_create_ad,
  meta_duplicate_campaign, meta_duplicate_adset, meta_update_adset_targeting, meta_create_audience,
  meta_create_lookalike, meta_get_leadgen_forms

Google Drive: google_drive_list_videos, google_drive_list_files,
  google_drive_get_video_info, google_drive_download_file,
  google_drive_watch_folder, google_drive_upload_video_to_meta

Pages — Feed & Posts: meta_get_page_feed, meta_get_page_post, meta_create_page_post,
  meta_update_page_post, meta_delete_page_post, meta_get_page_scheduled_posts

Pages — Comments: meta_get_post_comments, meta_reply_to_comment, meta_hide_comment,
  meta_delete_comment, meta_like_object, meta_get_page_post_reactions

Pages — Messaging: meta_get_page_conversations, meta_get_conversation_messages,
  meta_reply_to_page_message

Pages — Media: meta_get_page_photos, meta_get_page_videos, meta_get_page_albums

Pages — Events: meta_get_page_events, meta_create_page_event

Pages — Management: meta_get_page_info, meta_update_page_info, meta_get_page_reviews,
  meta_get_page_mentions, meta_get_page_roles, meta_add_page_role, meta_remove_page_role,
  meta_get_page_blocked_users, meta_block_page_user, meta_unblock_page_user,
  meta_get_instagram_account, meta_get_instagram_media, meta_create_instagram_post

Setup: pip install -r requirements.txt
       cp .env.example .env  # then fill in credentials
       python server.py
"""

import base64
import csv
import hashlib
import io
import json
import os
import time
from datetime import date, timedelta
from typing import Optional

import requests

from mcp_instance import mcp
from auth import current_user_ctx
from permissions import require_editor

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
GRAPH_BASE = "https://graph.facebook.com/v22.0"



def _token() -> str:
    user = current_user_ctx.get(None)
    if user is None:
        raise RuntimeError("Not authenticated.")
    t = user.get_meta_token()
    if not t:
        raise RuntimeError("Meta account not connected. Visit /onboard.")
    return t


def _account_id() -> str:
    aid = os.environ.get("META_AD_ACCOUNT_ID", "")
    if not aid:
        return ""
    return aid if aid.startswith("act_") else f"act_{aid}"



_MISSING_ACCOUNT_ERROR = {"error": "account_id is required. Call meta_list_ad_accounts first to find your account IDs, then pass account_id to this tool."}


def _get(path: str, params: Optional[dict] = None) -> dict:
    if path.startswith("/"):
        return _MISSING_ACCOUNT_ERROR
    p = {"access_token": _token()}
    if params:
        p.update(params)
    resp = requests.get(f"{GRAPH_BASE}/{path}", params=p, timeout=30)
    if not resp.ok:
        return {"error": resp.json().get("error", resp.text[:300])}
    return resp.json()


def _post(path: str, data: Optional[dict] = None) -> dict:
    if path.startswith("/"):
        return _MISSING_ACCOUNT_ERROR
    d = {"access_token": _token()}
    if data:
        d.update(data)
    resp = requests.post(f"{GRAPH_BASE}/{path}", data=d, timeout=30)
    if not resp.ok:
        return {"error": resp.json().get("error", resp.text[:300])}
    return resp.json()


def _delete(path: str) -> dict:
    """DELETE using user token from environment."""
    if path.startswith("/"):
        return _MISSING_ACCOUNT_ERROR
    resp = requests.delete(f"{GRAPH_BASE}/{path}", params={"access_token": _token()}, timeout=30)
    if not resp.ok:
        return {"error": resp.json().get("error", resp.text[:300])}
    return resp.json()


def _sha256(value: str) -> str:
    """Return lowercase SHA-256 hex digest of a string (strips whitespace first)."""
    return hashlib.sha256(value.strip().lower().encode()).hexdigest()


INSIGHTS_FIELDS = "campaign_name,adset_name,ad_name,impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type,conversions,cost_per_conversion"

# ---------------------------------------------------------------------------
# Analytics Tools (read-only)
# ---------------------------------------------------------------------------

@mcp.tool()
def get_meta_overview(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
) -> str:
    """Get a high-level Meta Ads performance overview: spend, impressions, clicks, CTR, CPC, ROAS.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/insights", {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "fields": "impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions,conversions",
    }))


@mcp.tool()
def get_meta_campaigns(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
    status_filter: str = "ACTIVE",
) -> str:
    """Get Meta Ads campaign performance including spend, impressions, clicks, CTR for each campaign.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use .env default.
        status_filter: Filter by campaign status: ACTIVE, PAUSED, ARCHIVED, ALL. Default ACTIVE.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    params = {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "level": "campaign",
        "fields": "campaign_id,campaign_name,impressions,clicks,spend,ctr,cpc,cpm,reach,actions",
        "limit": 50,
    }
    if status_filter != "ALL":
        params["effective_status"] = json.dumps([status_filter])
    return json.dumps(_get(f"{acct}/insights", params))


@mcp.tool()
def get_meta_adsets(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
    campaign_id: str = "",
) -> str:
    """Get Meta Ads adset-level performance: spend, impressions, CPC, frequency per ad set.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use .env default.
        campaign_id: Filter by a specific campaign ID. Leave blank for all.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    params = {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "level": "adset",
        "fields": "adset_id,adset_name,campaign_name,impressions,clicks,spend,ctr,cpc,cpm,frequency",
        "limit": 50,
    }
    if campaign_id:
        params["filtering"] = json.dumps([{"field": "campaign.id", "operator": "EQUAL", "value": campaign_id}])
    return json.dumps(_get(f"{acct}/insights", params))


@mcp.tool()
def get_meta_daily_trend(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
) -> str:
    """Get Meta Ads daily performance trend: daily spend, impressions, clicks, CTR over time.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/insights", {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "time_increment": 1,
        "fields": "date_start,impressions,clicks,spend,ctr,cpc,reach",
        "limit": 90,
    }))


@mcp.tool()
def get_meta_ads(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
    adset_id: str = "",
) -> str:
    """Get Meta Ads individual ad performance: clicks, impressions, spend, CTR, CPC per creative.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use .env default.
        adset_id: Filter by adset ID. Leave blank for all ads.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    params = {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "level": "ad",
        "fields": "ad_id,ad_name,adset_name,campaign_name,impressions,clicks,spend,ctr,cpc,cpm",
        "limit": 50,
    }
    if adset_id:
        params["filtering"] = json.dumps([{"field": "adset.id", "operator": "EQUAL", "value": adset_id}])
    return json.dumps(_get(f"{acct}/insights", params))


@mcp.tool()
def get_meta_demographics(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
) -> str:
    """Get Meta Ads performance broken down by age and gender demographics.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/insights", {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "breakdowns": "age,gender",
        "fields": "age,gender,impressions,clicks,spend,ctr,cpc,reach",
        "limit": 100,
    }))


@mcp.tool()
def get_meta_placements(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
) -> str:
    """Get Meta Ads performance broken down by publisher platform and ad placement (Feed, Stories, Reels, etc.).

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/insights", {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "breakdowns": "publisher_platform,platform_position",
        "fields": "publisher_platform,platform_position,impressions,clicks,spend,ctr,cpm",
        "limit": 50,
    }))


@mcp.tool()
def get_meta_geo(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
    row_limit: int = 25,
) -> str:
    """Get Meta Ads performance broken down by country or region.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use .env default.
        row_limit: Max countries. Default 25.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/insights", {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "breakdowns": "country",
        "fields": "country,impressions,clicks,spend,ctr,cpc,reach",
        "limit": row_limit,
    }))


@mcp.tool()
def get_meta_video_performance(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
) -> str:
    """Get Meta Ads video performance: video plays, 25/50/75/100% completion rates, ThruPlays.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/insights", {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "level": "ad",
        "fields": "ad_name,video_play_actions,video_p25_watched_actions,video_p50_watched_actions,video_p75_watched_actions,video_p100_watched_actions,video_thruplay_watched_actions,impressions,spend",
        "limit": 50,
    }))


@mcp.tool()
def get_meta_conversion_funnel(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
) -> str:
    """Get Meta Ads conversion funnel: impressions → link clicks → landing page views → adds to cart → purchases.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/insights", {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "fields": "impressions,clicks,spend,actions,cost_per_action_type",
        "action_breakdowns": "action_type",
        "limit": 1,
    }))


@mcp.tool()
def get_meta_frequency(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
) -> str:
    """Get Meta Ads frequency and reach data per campaign to detect ad fatigue.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/insights", {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "level": "campaign",
        "fields": "campaign_name,reach,frequency,impressions,spend",
        "limit": 50,
    }))


@mcp.tool()
def get_meta_hourly_trend(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
) -> str:
    """Get Meta Ads performance by hour of day to find peak delivery times.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 7 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=7))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/insights", {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "breakdowns": "hourly_stats_aggregated_by_advertiser_time_zone",
        "fields": "hourly_stats_aggregated_by_advertiser_time_zone,impressions,clicks,spend,ctr",
        "limit": 24,
    }))


@mcp.tool()
def meta_list_ad_accounts(user_id: str = "me", name_filter: str = "") -> str:
    """List all Meta ad accounts the user has access to, with optional name filtering.

    Paginates through ALL accounts (not just the first 50). If name_filter is provided,
    returns only accounts whose name contains that string (case-insensitive).

    Args:
        user_id: Facebook user ID. Leave blank to use 'me'.
        name_filter: Optional partial name to filter results, e.g. 'רוקט' or 'rocket'.
    """
    params = {"fields": "id,name,account_status,currency,timezone_name", "limit": 200}
    all_accounts: list = []
    endpoint = f"{user_id}/adaccounts"
    while True:
        result = _get(endpoint, params)
        if isinstance(result, dict) and "error" in result:
            return json.dumps(result)
        data = result.get("data", [])
        all_accounts.extend(data)
        after = result.get("paging", {}).get("cursors", {}).get("after")
        if not after or not data:
            break
        params["after"] = after

    if name_filter:
        q = name_filter.strip().lower()
        all_accounts = [a for a in all_accounts if q in a.get("name", "").lower()]

    return json.dumps({"data": all_accounts, "total": len(all_accounts)})


@mcp.tool()
def meta_get_audiences(account_id: str = "") -> str:
    """List all custom audiences in the Meta ad account.

    Args:
        account_id: Ad account ID. Leave blank to use .env default.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/customaudiences", {"fields": "id,name,approximate_count_lower_bound,approximate_count_upper_bound,subtype,description,customer_file_source,data_source", "limit": 50}))


@mcp.tool()
def meta_get_pages() -> str:
    """List Facebook Pages managed by the authenticated user."""
    return json.dumps(_get("me/accounts", {"fields": "id,name,category,fan_count,access_token", "limit": 50}))


@mcp.tool()
def meta_get_pixels(account_id: str = "") -> str:
    """List all Meta Pixels (datasets) associated with the ad account.

    Args:
        account_id: Ad account ID. Leave blank to use .env default.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/adspixels", {"fields": "id,name,code,last_fired_time,is_unavailable", "limit": 50}))


@mcp.tool()
def meta_get_catalog(account_id: str = "") -> str:
    """List Meta product catalogs linked to the ad account.

    Args:
        account_id: Ad account ID. Leave blank to use .env default.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/product_catalogs", {"fields": "id,name,product_count", "limit": 25}))


@mcp.tool()
def meta_get_creative_previews(ad_id: str) -> str:
    """Get creative preview URLs for a specific Meta ad.

    Args:
        ad_id: The Meta ad ID to get previews for (required).
    """
    return json.dumps(_get(f"{ad_id}/previews", {"ad_format": "DESKTOP_FEED_STANDARD"}))


@mcp.tool()
def meta_get_page_insights(
    page_id: str,
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Get Meta Page insights: reach, impressions, and engagement metrics for a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
    """
    import datetime as _dt
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    since_epoch = int(_dt.datetime.fromisoformat(sd).timestamp())
    until_epoch = int(_dt.datetime.fromisoformat(ed).timestamp())

    return json.dumps(_get(f"{page_id}/insights", {
        "metric": "page_impressions,page_reach,page_engaged_users,page_fan_adds",
        "period": "day",
        "since": since_epoch,
        "until": until_epoch,
    }))


@mcp.tool()
def meta_get_reach_estimate(
    targeting_spec: str,
    account_id: str = "",
) -> str:
    """Estimate the reach for a targeting spec in Meta Ads.

    Args:
        targeting_spec: JSON string with targeting spec, e.g. '{"age_min":25,"age_max":45,"genders":[1],"geo_locations":{"countries":["US"]}}' (required).
        account_id: Ad account ID. Leave blank to use .env default.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/reachestimate", {
        "targeting_spec": targeting_spec,
        "optimize_for": "REACH",
    }))


@mcp.tool()
def meta_get_targeting_search(
    q: str,
    targeting_type: str = "interests",
) -> str:
    """Search for Meta Ads targeting options: interests, behaviors, demographics.

    Args:
        q: Search query, e.g. 'fitness', 'technology', 'travel' (required).
        targeting_type: Type to search: 'interests', 'behaviors', 'demographics'. Default interests.
    """
    if targeting_type == "behaviors":
        params = {
            "type": "adTargetingCategory",
            "class": "behaviors",
            "limit": 30,
        }
    else:
        # interests or demographics — use suggestion endpoint
        params = {
            "type": "adinterestsuggestion",
            "interest_list": json.dumps([q]),
            "locale": "he_IL",
            "limit": 30,
        }
    return json.dumps(_get("search", params))


@mcp.tool()
def meta_get_pixel_stats(
    pixel_id: str,
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Get event statistics for a Meta Pixel — how many times each event fired.

    Args:
        pixel_id: The Meta Pixel ID (required).
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    return json.dumps(_get(f"{pixel_id}/stats", {
        "start_time": sd,
        "end_time": ed,
        "aggregation": "event",
    }))


@mcp.tool()
def meta_get_custom_conversions(account_id: str = "") -> str:
    """List all custom conversions defined in the Meta ad account.

    Args:
        account_id: Ad account ID. Leave blank to use .env default.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/customconversions", {"fields": "id,name,description,pixel,event_source_type,rule", "limit": 50}))


@mcp.tool()
def meta_get_ad_creatives(account_id: str = "") -> str:
    """List all ad creatives in the Meta ad account with their names and object types.

    Args:
        account_id: Ad account ID. Leave blank to use .env default.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/adcreatives", {"fields": "id,name,object_type,body,title,image_url,status", "limit": 50}))


# ---------------------------------------------------------------------------
# Listing Tools (no date/insights required)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_list_campaigns(
    account_id: str = "",
    status_filter: str = "ALL",
) -> str:
    """List Meta Ads campaigns with their IDs, names, status, objective, and budgets — no date range needed.

    Use this to find campaign IDs for duplication, status updates, or budget changes.

    Args:
        account_id: Ad account ID. Leave blank to use .env default.
        status_filter: Filter by status: ACTIVE, PAUSED, ARCHIVED, ALL. Default ALL.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    params = {
        "fields": "id,name,status,objective,daily_budget,lifetime_budget,start_time,stop_time",
        "limit": 50,
    }
    if status_filter != "ALL":
        params["effective_status"] = json.dumps([status_filter])
    return json.dumps(_get(f"{acct}/campaigns", params))


@mcp.tool()
def meta_list_adsets(
    account_id: str = "",
    campaign_id: str = "",
    status_filter: str = "ALL",
) -> str:
    """List Meta Ads ad sets with their IDs, names, status, and budgets — no date range needed.

    Use this to find ad set IDs for duplication, status updates, or targeting changes.

    Args:
        account_id: Ad account ID. Leave blank to use .env default.
        campaign_id: Filter by a specific campaign ID. Leave blank for all ad sets.
        status_filter: Filter by status: ACTIVE, PAUSED, ARCHIVED, ALL. Default ALL.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    params = {
        "fields": "id,name,status,campaign_id,campaign_name,daily_budget,lifetime_budget,targeting,optimization_goal,billing_event",
        "limit": 50,
    }
    if status_filter != "ALL":
        params["effective_status"] = json.dumps([status_filter])
    if campaign_id:
        params["filtering"] = json.dumps([{"field": "campaign.id", "operator": "EQUAL", "value": campaign_id}])
    return json.dumps(_get(f"{acct}/adsets", params))


# ---------------------------------------------------------------------------
# Management Tools (write)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_update_status(
    object_id: str,
    object_type: str,
    status: str,
) -> str:
    """Update the status (active/paused/archived) of a Meta Ads campaign, ad set, or ad.

    Args:
        object_id: The ID of the campaign, adset, or ad to update (required).
        object_type: One of 'campaign', 'adset', or 'ad' (required).
        status: New status: ACTIVE, PAUSED, or ARCHIVED (required).
    """
    if (err := require_editor("meta_update_status")): return err
    field_map = {"campaign": "campaign", "adset": "adset", "ad": "ad"}
    if object_type not in field_map:
        return json.dumps({"error": "object_type must be campaign, adset, or ad"})
    return json.dumps(_post(object_id, {"status": status}))


@mcp.tool()
def meta_update_budget(
    campaign_id: str,
    daily_budget: str = "",
    lifetime_budget: str = "",
) -> str:
    """Update the daily or lifetime budget for a Meta Ads campaign (CBO — campaign-level budget).

    Use meta_update_adset_budget to update budget at the ad set level.

    Args:
        campaign_id: Campaign ID to update (required).
        daily_budget: New daily budget in the account currency's smallest unit, e.g. '5000' for ₪50. Leave blank to keep unchanged.
        lifetime_budget: New lifetime budget in smallest currency unit. Leave blank to keep unchanged.
    """
    if (err := require_editor("meta_update_budget")): return err
    data = {}
    if daily_budget:
        data["daily_budget"] = daily_budget
    if lifetime_budget:
        data["lifetime_budget"] = lifetime_budget
    if not data:
        return json.dumps({"error": "Provide daily_budget or lifetime_budget."})
    return json.dumps(_post(campaign_id, data))


@mcp.tool()
def meta_update_adset_budget(
    adset_id: str,
    daily_budget: str = "",
    lifetime_budget: str = "",
    bid_amount: str = "",
    end_time: str = "",
) -> str:
    """Update the budget (and optionally the bid) for a Meta Ads ad set.

    Works for ad sets in non-CBO campaigns where budget is set per ad set.
    Use meta_update_budget to change a CBO campaign-level budget instead.

    Args:
        adset_id: Ad set ID to update (required).
        daily_budget: New daily budget in smallest currency unit, e.g. '5000' for ₪50. Leave blank to keep unchanged.
        lifetime_budget: New lifetime budget in smallest currency unit. Leave blank to keep unchanged.
        bid_amount: New manual bid in smallest currency unit. Only for COST_CAP / LOWEST_COST_WITH_BID_CAP strategies.
        end_time: New end time in ISO 8601, e.g. '2025-12-31T23:59:00+0200'. Required when switching to lifetime_budget.
    """
    if (err := require_editor("meta_update_adset_budget")): return err
    data: dict = {}
    if daily_budget:
        data["daily_budget"] = daily_budget
    if lifetime_budget:
        data["lifetime_budget"] = lifetime_budget
    if bid_amount:
        data["bid_amount"] = bid_amount
    if end_time:
        data["end_time"] = end_time
    if not data:
        return json.dumps({"error": "Provide at least one of: daily_budget, lifetime_budget, bid_amount."})
    return json.dumps(_post(adset_id, data))


@mcp.tool()
def meta_switch_budget_mode(
    campaign_id: str,
    mode: str,
    account_id: str = "",
    campaign_daily_budget: str = "",
    campaign_lifetime_budget: str = "",
) -> str:
    """Switch a Meta Ads campaign between CBO (Campaign Budget Optimization) and ad-set-level budgets.

    CBO mode: budget is set at the campaign level and Meta distributes it automatically across ad sets.
    Ad-set mode: each ad set controls its own budget independently.

    When switching FROM CBO to ad-set mode: Meta clears ad set budgets during the switch.
    Call meta_update_adset_budget for each ad set afterward to restore their budgets.
    The response includes the list of ad sets so you know which ones to update.

    When switching FROM ad-set to CBO mode: pass campaign_daily_budget or campaign_lifetime_budget
    to set the new campaign-level budget in the same call.

    Args:
        campaign_id: Campaign ID to switch (required).
        mode: Target mode — 'cbo' to enable campaign-level budget, 'adset' to use per-ad-set budgets (required).
        account_id: Ad account ID. Leave blank to use .env default.
        campaign_daily_budget: Daily budget in smallest currency unit to set when enabling CBO, e.g. '10000' for ₪100.
        campaign_lifetime_budget: Lifetime budget in smallest currency unit to set when enabling CBO.
    """
    if (err := require_editor("meta_switch_budget_mode")): return err
    if mode not in ("cbo", "adset"):
        return json.dumps({"error": "mode must be 'cbo' or 'adset'."})

    enable_cbo = mode == "cbo"
    data: dict = {"is_adset_budget_sharing_enabled": "false" if enable_cbo else "true"}
    if enable_cbo and campaign_daily_budget:
        data["daily_budget"] = campaign_daily_budget
    if enable_cbo and campaign_lifetime_budget:
        data["lifetime_budget"] = campaign_lifetime_budget

    result = _post(campaign_id, data)
    if "error" in result:
        return json.dumps(result)

    if enable_cbo:
        return json.dumps({
            "success": True,
            "mode": "CBO (campaign-level budget)",
            "campaign_id": campaign_id,
            "meta_response": result,
            "note": "Campaign now uses CBO. Budget distributed automatically across ad sets.",
        })

    # Switched to ad-set mode — list ad sets so user can restore budgets
    acct = _account_id() if not account_id else (account_id if account_id.startswith("act_") else f"act_{account_id}")
    adsets = _get(f"{acct}/adsets", {
        "filtering": json.dumps([{"field": "campaign.id", "operator": "EQUAL", "value": campaign_id}]),
        "fields": "id,name,daily_budget,lifetime_budget",
        "limit": 100,
    })
    return json.dumps({
        "success": True,
        "mode": "Ad-set-level budgets",
        "campaign_id": campaign_id,
        "meta_response": result,
        "adsets": adsets.get("data", []),
        "note": "Campaign switched to ad-set budgets. Use meta_update_adset_budget to set individual budgets per ad set.",
    })


@mcp.tool()
def meta_create_campaign(
    name: str,
    objective: str,
    status: str = "PAUSED",
    account_id: str = "",
    special_ad_categories: str = "NONE",
    is_cbo: bool = False,
    daily_budget: str = "",
    lifetime_budget: str = "",
) -> str:
    """Create a new Meta Ads campaign.

    Args:
        name: Campaign name (required).
        objective: Campaign objective: OUTCOME_TRAFFIC, OUTCOME_AWARENESS, OUTCOME_LEADS, OUTCOME_SALES, OUTCOME_ENGAGEMENT, OUTCOME_APP_PROMOTION (required).
        status: Initial status: ACTIVE or PAUSED. Default PAUSED.
        account_id: Ad account ID. Leave blank to use .env default.
        special_ad_categories: One of NONE, EMPLOYMENT, HOUSING, CREDIT, ISSUES_ELECTIONS_POLITICS. Default NONE.
        is_cbo: Enable Campaign Budget Optimization (budget at campaign level). Default False (budget set per ad set).
        daily_budget: Daily budget in smallest currency unit (e.g. '5000' = ₪50). Only used when is_cbo=True.
        lifetime_budget: Lifetime budget in smallest currency unit. Only used when is_cbo=True.
    """
    if (err := require_editor("meta_create_campaign")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    data: dict = {
        "name": name,
        "objective": objective,
        "status": status,
        "special_ad_categories": json.dumps([special_ad_categories] if special_ad_categories != "NONE" else []),
        "is_adset_budget_sharing_enabled": "false" if not is_cbo else "true",
    }
    if is_cbo and daily_budget:
        data["daily_budget"] = daily_budget
    if is_cbo and lifetime_budget:
        data["lifetime_budget"] = lifetime_budget
    return json.dumps(_post(f"{acct}/campaigns", data))


@mcp.tool()
def meta_create_adset(
    name: str,
    campaign_id: str,
    targeting: str,
    billing_event: str = "IMPRESSIONS",
    optimization_goal: str = "LINK_CLICKS",
    status: str = "PAUSED",
    account_id: str = "",
    daily_budget: str = "",
    lifetime_budget: str = "",
    bid_amount: str = "",
    bid_strategy: str = "",
    promoted_object: str = "",
    start_time: str = "",
    end_time: str = "",
    frequency_control_specs: str = "",
    attribution_spec: str = "",
    pacing_type: str = "",
    destination_type: str = "",
    adset_schedule: str = "",
    daily_min_spend_target: str = "",
    daily_spend_cap: str = "",
    advantage_audience: bool = False,
) -> str:
    """Create a new Meta Ads ad set within a campaign.

    Args:
        name: Ad set name (required).
        campaign_id: Parent campaign ID (required).
        targeting: JSON string with targeting spec, e.g. '{"age_min":25,"age_max":50,"geo_locations":{"countries":["IL"]}}' (required).
        billing_event: IMPRESSIONS, LINK_CLICKS, POST_ENGAGEMENT. Default IMPRESSIONS.
        optimization_goal: LINK_CLICKS, OFFSITE_CONVERSIONS, LEAD_GENERATION, CONVERSATIONS, REACH, etc. Default LINK_CLICKS.
        status: ACTIVE or PAUSED. Default PAUSED.
        account_id: Ad account ID. Leave blank to use .env default.
        daily_budget: Daily budget in smallest currency unit, e.g. '5000' for ₪50.
        lifetime_budget: Lifetime budget in smallest currency unit (required with dayparting).
        bid_amount: Manual bid cap in smallest currency unit.
        bid_strategy: LOWEST_COST_WITHOUT_CAP, LOWEST_COST_WITH_BID_CAP, COST_CAP.
        promoted_object: JSON e.g. '{"pixel_id":"123","custom_event_type":"PURCHASE"}'.
        start_time: ISO 8601, e.g. '2026-04-01T00:00:00+0300'.
        end_time: ISO 8601. Required with lifetime_budget.
        frequency_control_specs: JSON to cap impressions per user, e.g.
          '[{"event":"IMPRESSIONS","interval_days":7,"max_frequency":3}]'.
        attribution_spec: JSON attribution windows, e.g.
          '[{"event_type":"CLICK_THROUGH","window_days":7},{"event_type":"VIEW_THROUGH","window_days":1}]'.
        pacing_type: 'standard' or 'no_pacing' (accelerated). Default standard.
        destination_type: Where clicks go: WEBSITE, MESSENGER, INSTAGRAM_DIRECT,
          WHATSAPP, ON_AD, APP. Default WEBSITE.
        adset_schedule: JSON daypart windows (requires lifetime_budget), e.g.
          '[{"start_minute":540,"end_minute":1080,"days":[1,2,3,4,5]}]'.
        daily_min_spend_target: Soft spend floor in CBO campaigns (smallest unit).
        daily_spend_cap: Hard spend ceiling in CBO campaigns (smallest unit).
        advantage_audience: Set True to enable Advantage+ Audience automation (Meta AI expands beyond targeting).
    """
    if (err := require_editor("meta_create_adset")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()

    # Build targeting — inject advantage_audience if requested
    targeting_dict: dict = {}
    if targeting:
        try:
            targeting_dict = json.loads(targeting)
        except Exception:
            targeting_dict = {}
    if advantage_audience:
        targeting_dict["targeting_automation"] = {"advantage_audience": 1}
    final_targeting = json.dumps(targeting_dict) if targeting_dict else targeting

    data: dict = {
        "name": name,
        "campaign_id": campaign_id,
        "targeting": final_targeting,
        "billing_event": billing_event,
        "optimization_goal": optimization_goal,
        "status": status,
    }
    if daily_budget:         data["daily_budget"] = daily_budget
    if lifetime_budget:      data["lifetime_budget"] = lifetime_budget
    if bid_amount:           data["bid_amount"] = bid_amount
    if bid_strategy:         data["bid_strategy"] = bid_strategy
    if promoted_object:      data["promoted_object"] = promoted_object
    if start_time:           data["start_time"] = start_time
    if end_time:             data["end_time"] = end_time
    if frequency_control_specs:  data["frequency_control_specs"] = frequency_control_specs
    if attribution_spec:         data["attribution_spec"] = attribution_spec
    if pacing_type:              data["pacing_type"] = json.dumps([pacing_type])
    if destination_type:         data["destination_type"] = destination_type
    if daily_min_spend_target:   data["daily_min_spend_target"] = daily_min_spend_target
    if daily_spend_cap:          data["daily_spend_cap"] = daily_spend_cap
    if adset_schedule:
        data["adset_schedule"] = adset_schedule
        data["pacing_type"] = json.dumps(["day_parting"])

    return json.dumps(_post(f"{acct}/adsets", data))


@mcp.tool()
def meta_create_ad_creative(
    name: str,
    page_id: str,
    message: str,
    link: str,
    headline: str = "",
    description: str = "",
    image_url: str = "",
    account_id: str = "",
) -> str:
    """Create a new Meta Ads creative (link ad format).

    Args:
        name: Creative name (required).
        page_id: Facebook Page ID to post from (required).
        message: The ad text/body copy (required).
        link: The destination URL (required).
        headline: Ad headline. Optional.
        description: Link description. Optional.
        image_url: Image URL for the creative. Optional for link ads.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    if (err := require_editor("meta_create_ad_creative")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    object_story_spec = {
        "page_id": page_id,
        "link_data": {"message": message, "link": link},
    }
    if headline:
        object_story_spec["link_data"]["name"] = headline
    if description:
        object_story_spec["link_data"]["description"] = description
    if image_url:
        object_story_spec["link_data"]["picture"] = image_url
    return json.dumps(_post(f"{acct}/adcreatives", {
        "name": name,
        "object_story_spec": json.dumps(object_story_spec),
    }))


@mcp.tool()
def meta_upload_ad_video(
    file_url: str,
    title: str = "",
    account_id: str = "",
) -> str:
    """Upload a video to Meta's ad video library from a public URL.

    Supports Google Drive share links (https://drive.google.com/file/d/ID/view).
    Google Drive videos are downloaded locally first (bypassing the virus-scan
    HTML page that blocks Meta's servers) and then uploaded via multipart upload.

    Args:
        file_url: Public URL of the video file. Google Drive share links are supported.
        title: Optional title for the video in the library.
        account_id: Ad account ID. Leave blank to use .env default.

    Returns:
        JSON with the new video ID, e.g. {"id": "12345678"}.
    """
    if (err := require_editor("meta_upload_ad_video")): return err
    import re
    import tempfile
    import os

    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()

    gd_match = re.search(r"drive\.google\.com/file/d/([^/?#]+)", file_url)
    if gd_match:
        file_id = gd_match.group(1)
        # Use drive.usercontent.google.com to bypass virus-scan warning page
        download_url = f"https://drive.usercontent.google.com/download?id={file_id}&export=download&confirm=t"

        session = requests.Session()
        resp = session.get(download_url, stream=True, timeout=300)
        resp.raise_for_status()

        suffix = ".mp4"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp_path = tmp.name
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    tmp.write(chunk)

        try:
            video_title = title or os.path.basename(tmp_path)
            token = _token()
            upload_url = f"https://graph-video.facebook.com/v21.0/{acct}/advideos"
            with open(tmp_path, "rb") as f:
                result = requests.post(
                    upload_url,
                    data={"title": video_title, "access_token": token},
                    files={"source": (video_title, f, "video/mp4")},
                    timeout=600,
                )
            return result.text
        finally:
            os.unlink(tmp_path)

    # Non-Drive URL: pass directly to Meta
    payload: dict = {"file_url": file_url}
    if title:
        payload["title"] = title
    return json.dumps(_post(f"{acct}/advideos", payload))


@mcp.tool()
def meta_create_video_ad_creative(
    name: str,
    page_id: str,
    video_id: str,
    message: str,
    title: str = "",
    description: str = "",
    call_to_action_type: str = "LEARN_MORE",
    call_to_action_link: str = "",
    account_id: str = "",
) -> str:
    """Create a Meta Ads creative for a video ad.

    Use meta_upload_ad_video first to get a video_id, then call this tool.

    Args:
        name: Creative name (required).
        page_id: Facebook Page ID to post from (required).
        video_id: Meta video ID returned by meta_upload_ad_video (required).
        message: The ad text/body copy shown above the video (required).
        title: Headline shown below the video. Optional.
        description: Description below the headline. Optional.
        call_to_action_type: CTA button type, e.g. LEARN_MORE, SHOP_NOW, SIGN_UP, DOWNLOAD. Default LEARN_MORE.
        call_to_action_link: Destination URL for the CTA button. Required when call_to_action_type is set.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    if (err := require_editor("meta_create_video_ad_creative")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    video_data: dict = {"video_id": video_id, "message": message}
    if title:
        video_data["title"] = title
    if description:
        video_data["description"] = description
    if call_to_action_type and call_to_action_link:
        video_data["call_to_action"] = {
            "type": call_to_action_type,
            "value": {"link": call_to_action_link},
        }
    object_story_spec = {
        "page_id": page_id,
        "video_data": video_data,
    }
    return json.dumps(_post(f"{acct}/adcreatives", {
        "name": name,
        "object_story_spec": json.dumps(object_story_spec),
    }))


@mcp.tool()
def meta_create_ad(
    name: str,
    adset_id: str,
    creative_id: str,
    status: str = "PAUSED",
    account_id: str = "",
) -> str:
    """Create a new Meta Ad by combining an ad set and a creative.

    Args:
        name: Ad name (required).
        adset_id: The ad set ID to place this ad in (required).
        creative_id: The creative ID to use (required).
        status: ACTIVE or PAUSED. Default PAUSED.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    if (err := require_editor("meta_create_ad")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_post(f"{acct}/ads", {
        "name": name,
        "adset_id": adset_id,
        "creative": json.dumps({"creative_id": creative_id}),
        "status": status,
    }))


@mcp.tool()
def meta_duplicate_campaign(
    campaign_id: str,
    new_name: str = "",
    account_id: str = "",
    sequential: bool = False,
) -> str:
    """Duplicate an existing Meta Ads campaign (copies the campaign, ad sets, and ads).

    For campaigns with more than 3 ad sets, Meta blocks a single synchronous copy.
    Set sequential=True to duplicate each ad set one-by-one — slower but works for any size.

    Args:
        campaign_id: Campaign ID to duplicate (required).
        new_name: Name for the duplicated campaign. Defaults to original name + ' (Copy)'.
        account_id: Ad account ID. Leave blank to use .env default.
        sequential: If True, creates a new campaign shell then duplicates each ad set individually. Use for campaigns with 4+ ad sets. Default False.
    """
    if (err := require_editor("meta_duplicate_campaign")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()

    if not sequential:
        data: dict = {"deep_copy": "true"}
        if new_name:
            data["rename_options"] = json.dumps({"rename_prefix": new_name, "rename_suffix": ""})
        return json.dumps(_post(f"{campaign_id}/copies", data))

    # Sequential mode: copy campaign shell first, then duplicate each adset individually
    # Step 1: get campaign info
    campaign_info = _get(campaign_id, {"fields": "name,objective,status,is_adset_budget_sharing_enabled,special_ad_categories"})
    if "error" in campaign_info:
        return json.dumps(campaign_info)

    dest_name = new_name or f"{campaign_info.get('name', campaign_id)} (Copy)"
    new_campaign = _post(f"{acct}/campaigns", {
        "name": dest_name,
        "objective": campaign_info.get("objective", "OUTCOME_TRAFFIC"),
        "status": "PAUSED",
        "special_ad_categories": json.dumps(campaign_info.get("special_ad_categories", [])),
        "is_adset_budget_sharing_enabled": "true" if campaign_info.get("is_adset_budget_sharing_enabled") else "false",
    })
    if "error" in new_campaign:
        return json.dumps(new_campaign)
    new_campaign_id = new_campaign["id"]

    # Step 2: list all adsets in original campaign
    adsets = _get(f"{acct}/adsets", {
        "filtering": json.dumps([{"field": "campaign.id", "operator": "EQUAL", "value": campaign_id}]),
        "fields": "id,name",
        "limit": 100,
    })
    if "error" in adsets:
        return json.dumps({"new_campaign_id": new_campaign_id, "error_listing_adsets": adsets["error"]})

    adset_list = adsets.get("data", [])
    results = {"new_campaign_id": new_campaign_id, "adsets_copied": [], "adsets_failed": []}

    # Step 3: duplicate each adset one by one
    for adset in adset_list:
        adset_id = adset["id"]
        copy_result = _post(f"{adset_id}/copies", {
            "campaign_id": new_campaign_id,
            "deep_copy": "true",
            "status_override": "PAUSED",
        })
        if "error" in copy_result:
            # Retry without deep_copy if Advantage+ creative error (3858504)
            error_code = copy_result.get("error", {}).get("error_subcode") or copy_result.get("error", {}).get("code")
            if str(error_code) == "3858504":
                copy_result = _post(f"{adset_id}/copies", {
                    "campaign_id": new_campaign_id,
                    "deep_copy": "false",
                    "status_override": "PAUSED",
                })
            if "error" in copy_result:
                results["adsets_failed"].append({"id": adset_id, "name": adset.get("name"), "error": copy_result["error"]})
                continue
        results["adsets_copied"].append({"original_id": adset_id, "new_id": copy_result.get("copied_adset_id", copy_result.get("id")), "name": adset.get("name")})

    return json.dumps(results)


@mcp.tool()
def meta_duplicate_adset(
    adset_id: str,
    new_name: str = "",
    campaign_id: str = "",
    deep_copy: bool = True,
    status_override: str = "PAUSED",
) -> str:
    """Duplicate an existing Meta Ads ad set (and optionally its ads).

    Args:
        adset_id: Ad set ID to duplicate (required).
        new_name: Name for the duplicated ad set. Defaults to original name + ' (Copy)'.
        campaign_id: Destination campaign ID. Leave blank to duplicate into the same campaign.
        deep_copy: If True, also copies the ads inside the ad set. Default True.
        status_override: Status for the new ad set: PAUSED, ACTIVE, or INHERITED_FROM_SOURCE. Default PAUSED.
    """
    if (err := require_editor("meta_duplicate_adset")): return err
    # If campaign_id not provided, fetch it from the adset so Meta doesn't reject the request
    resolved_campaign_id = campaign_id
    if not resolved_campaign_id:
        adset_info = _get(f"{adset_id}", {"fields": "campaign_id"})
        if "error" in adset_info:
            return json.dumps(adset_info)
        resolved_campaign_id = adset_info.get("campaign_id", "")

    data: dict = {
        "deep_copy": "true" if deep_copy else "false",
        "status_override": status_override,
    }
    if resolved_campaign_id:
        data["campaign_id"] = resolved_campaign_id
    if new_name:
        data["rename_options"] = json.dumps({"rename_prefix": new_name, "rename_suffix": ""})

    result = _post(f"{adset_id}/copies", data)

    # If Advantage+ creative blocks deep copy (error 3858504), retry without copying ads
    if "error" in result and deep_copy:
        error_code = result.get("error", {}).get("error_subcode") or result.get("error", {}).get("code")
        if str(error_code) == "3858504":
            data["deep_copy"] = "false"
            shallow_result = _post(f"{adset_id}/copies", data)
            if "error" not in shallow_result:
                shallow_result["_note"] = "Copied without ads — Advantage+ creative blocked deep copy (error 3858504)"
                return json.dumps(shallow_result)

    return json.dumps(result)


@mcp.tool()
def meta_update_adset_targeting(
    adset_id: str,
    targeting: str = "",
    frequency_control_specs: str = "",
    attribution_spec: str = "",
    pacing_type: str = "",
    destination_type: str = "",
    adset_schedule: str = "",
    daily_min_spend_target: str = "",
    daily_spend_cap: str = "",
    advantage_audience: bool = False,
    status: str = "",
    name: str = "",
) -> str:
    """Update targeting, scheduling, and delivery settings for an existing ad set.

    All parameters are optional — only provided fields are updated.

    Args:
        adset_id: The ad set ID to update (required).
        targeting: JSON targeting spec. Example: '{"age_min":30,"age_max":55,"genders":[2],"geo_locations":{"countries":["IL"]}}'
        frequency_control_specs: JSON array to cap frequency. Example: '[{"event":"IMPRESSIONS","interval_days":7,"max_frequency":3}]'
        attribution_spec: JSON array for attribution windows. Example: '[{"event_type":"CLICK_THROUGH","window_days":7}]'
        pacing_type: standard or no_pacing (for dayparting use day_parting).
        destination_type: WEBSITE, MESSENGER, WHATSAPP, INSTAGRAM_DIRECT, ON_AD.
        adset_schedule: JSON dayparting schedule (requires lifetime_budget on the adset).
                        Example: '[{"start_minute":480,"end_minute":1200,"days":[1,2,3,4,5],"timezone_type":"USER"}]'
        daily_min_spend_target: Soft spend floor in cents for CBO campaigns.
        daily_spend_cap: Hard spend ceiling in cents for CBO campaigns.
        advantage_audience: Set True to enable Advantage+ AI audience automation.
        status: ACTIVE, PAUSED, DELETED, or ARCHIVED.
        name: New name for the ad set.
    """
    if (err := require_editor("meta_update_adset_targeting")): return err
    payload: dict = {}
    if targeting:
        try:
            tgt = json.loads(targeting) if isinstance(targeting, str) else targeting
        except Exception:
            tgt = targeting
        if advantage_audience:
            if isinstance(tgt, dict):
                tgt["targeting_automation"] = {"advantage_audience": 1}
        payload["targeting"] = tgt
    elif advantage_audience:
        payload["targeting"] = {"targeting_automation": {"advantage_audience": 1}}

    if frequency_control_specs:
        payload["frequency_control_specs"] = frequency_control_specs
    if attribution_spec:
        payload["attribution_spec"] = attribution_spec
    if pacing_type:
        payload["pacing_type"] = [pacing_type]
    if destination_type:
        payload["destination_type"] = destination_type
    if adset_schedule:
        payload["adset_schedule"] = adset_schedule
        if "pacing_type" not in payload:
            payload["pacing_type"] = ["day_parting"]
    if daily_min_spend_target:
        payload["daily_min_spend_target"] = daily_min_spend_target
    if daily_spend_cap:
        payload["daily_spend_cap"] = daily_spend_cap
    if status:
        payload["status"] = status
    if name:
        payload["name"] = name

    if not payload:
        return json.dumps({"error": "No fields provided to update."})
    return json.dumps(_post(adset_id, payload))


@mcp.tool()
def meta_create_audience(
    name: str,
    description: str = "",
    customer_file_source: str = "USER_PROVIDED_ONLY",
    account_id: str = "",
) -> str:
    """Create a new custom audience (customer list) in Meta Ads.

    Args:
        name: Audience name (required).
        description: Optional description.
        customer_file_source: Source: USER_PROVIDED_ONLY, PARTNER_PROVIDED_ONLY, or BOTH_USER_AND_PARTNER_PROVIDED. Default USER_PROVIDED_ONLY.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    if (err := require_editor("meta_create_audience")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_post(f"{acct}/customaudiences", {
        "name": name,
        "description": description,
        "customer_file_source": customer_file_source,
        "subtype": "CUSTOM",
    }))


@mcp.tool()
def meta_create_lookalike(
    source_audience_id: str,
    countries: str,
    ratio: float = 0.01,
    account_id: str = "",
) -> str:
    """Create a Lookalike Audience based on an existing custom audience.

    Args:
        source_audience_id: Custom audience ID to base lookalike on (required).
        countries: Comma-separated country codes, e.g. 'US,CA,GB' (required).
        ratio: Lookalike ratio 0.01–0.20 (1% to 20% of population). Default 0.01.
        account_id: Ad account ID. Leave blank to use .env default.
    """
    if (err := require_editor("meta_create_lookalike")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    country_list = [c.strip() for c in countries.split(",") if c.strip()]
    return json.dumps(_post(f"{acct}/customaudiences", {
        "name": f"Lookalike ({', '.join(country_list)}, {int(ratio * 100)}%)",
        "subtype": "LOOKALIKE",
        "origin_audience_id": source_audience_id,
        "lookalike_spec": json.dumps({
            "type": "similarity",
            "starting_ratio": 0,
            "ratio": ratio,
            "country": country_list[0] if len(country_list) == 1 else None,
            "country_list": country_list if len(country_list) > 1 else None,
        }),
    }))


@mcp.tool()
def meta_get_leadgen_forms(page_id: str) -> str:
    """Get all lead generation forms for a Facebook Page.

    Args:
        page_id: Facebook Page ID to get leadgen forms from (required).
    """
    return json.dumps(_get(f"{page_id}/leadgen_forms", {"fields": "id,name,status,leads_count,created_time", "limit": 50}))


# ---------------------------------------------------------------------------
# Pages API — Helper functions
# ---------------------------------------------------------------------------

def _get_with_token(path: str, token: str, params: Optional[dict] = None) -> dict:
    p = {"access_token": token}
    if params:
        p.update(params)
    resp = requests.get(f"{GRAPH_BASE}/{path}", params=p, timeout=30)
    if not resp.ok:
        return {"error": resp.json().get("error", resp.text[:300])}
    return resp.json()


def _post_with_token(path: str, token: str, data: Optional[dict] = None) -> dict:
    d = {"access_token": token}
    if data:
        d.update(data)
    resp = requests.post(f"{GRAPH_BASE}/{path}", data=d, timeout=30)
    if not resp.ok:
        return {"error": resp.json().get("error", resp.text[:300])}
    return resp.json()


def _delete_with_token(path: str, token: str) -> dict:
    resp = requests.delete(f"{GRAPH_BASE}/{path}", params={"access_token": token}, timeout=30)
    if not resp.ok:
        return {"error": resp.json().get("error", resp.text[:300])}
    return resp.json()


def _page_tok(page_access_token: str) -> str:
    """Return page token if provided, else raise a clear error."""
    if not page_access_token:
        raise ValueError(
            "page_access_token is required for Page operations. "
            "Get it from the 'access_token' field in meta_get_pages response."
        )
    return page_access_token


# ---------------------------------------------------------------------------
# Pages API — Page Feed / Posts
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_page_feed(
    page_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get the published posts feed of a Facebook Page.

    Args:
        page_id: Facebook Page ID (required). Get it from meta_get_pages.
        page_access_token: Page access token. Get it from the access_token field in meta_get_pages. Falls back to user token if blank.
        limit: Number of posts to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/feed", tok, {
        "fields": "id,message,story,created_time,permalink_url,likes.summary(true),comments.summary(true),shares",
        "limit": limit,
    }))


@mcp.tool()
def meta_get_page_post(
    post_id: str,
    page_access_token: str = "",
) -> str:
    """Get details of a specific Facebook Page post.

    Args:
        post_id: Post ID (required), e.g. '123456789_987654321'.
        page_access_token: Page access token. Falls back to user token if blank.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(post_id, tok, {
        "fields": "id,message,story,created_time,updated_time,permalink_url,likes.summary(true),comments.summary(true),shares,attachments,is_published",
    }))


@mcp.tool()
def meta_create_page_post(
    page_id: str,
    message: str = "",
    link: str = "",
    image_url: str = "",
    scheduled_publish_time: str = "",
    published: bool = True,
    page_access_token: str = "",
) -> str:
    """Create a new post on a Facebook Page (text, link, or photo).

    Args:
        page_id: Facebook Page ID (required).
        message: Post text/body (required unless image_url is provided).
        link: URL to share as a link post. Optional.
        image_url: Publicly accessible image URL to post as a photo. Optional.
        scheduled_publish_time: Unix timestamp for scheduling, e.g. '1735689600'. Only used when published=False.
        published: True to publish immediately, False to schedule. Default True.
        page_access_token: Page access token (required for posting). Get from meta_get_pages.
    """
    if (err := require_editor("meta_create_page_post")): return err
    tok = _page_tok(page_access_token)
    data: dict = {"published": "true" if published else "false"}
    if message:
        data["message"] = message
    if link:
        data["link"] = link
    if scheduled_publish_time and not published:
        data["scheduled_publish_time"] = scheduled_publish_time
    if image_url:
        data["url"] = image_url
        return json.dumps(_post_with_token(f"{page_id}/photos", tok, data))
    return json.dumps(_post_with_token(f"{page_id}/feed", tok, data))


@mcp.tool()
def meta_update_page_post(
    post_id: str,
    message: str,
    page_access_token: str = "",
) -> str:
    """Update the message/text of an existing Facebook Page post.

    Args:
        post_id: Post ID to update (required).
        message: New post text (required).
        page_access_token: Page access token. Falls back to user token if blank.
    """
    if (err := require_editor("meta_update_page_post")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_post_with_token(post_id, tok, {"message": message}))


@mcp.tool()
def meta_delete_page_post(
    post_id: str,
    page_access_token: str = "",
) -> str:
    """Delete a Facebook Page post.

    Args:
        post_id: Post ID to delete (required).
        page_access_token: Page access token. Falls back to user token if blank.
    """
    if (err := require_editor("meta_delete_page_post")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_delete_with_token(post_id, tok))


@mcp.tool()
def meta_get_page_scheduled_posts(
    page_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get scheduled (unpublished) posts for a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of posts to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/scheduled_posts", tok, {
        "fields": "id,message,scheduled_publish_time,created_time,permalink_url",
        "limit": limit,
    }))


# ---------------------------------------------------------------------------
# Pages API — Comments
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_post_comments(
    post_id: str,
    page_access_token: str = "",
    limit: int = 50,
    summary: bool = True,
) -> str:
    """Get comments on a Facebook Page post.

    Args:
        post_id: Post ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of comments to return. Default 50.
        summary: Include total comment count summary. Default True.
    """
    tok = _page_tok(page_access_token)
    params: dict = {
        "fields": "id,message,from,created_time,like_count,comment_count,can_hide,is_hidden",
        "limit": limit,
        "filter": "stream",
    }
    if summary:
        params["summary"] = "true"
    return json.dumps(_get_with_token(f"{post_id}/comments", tok, params))


@mcp.tool()
def meta_reply_to_comment(
    comment_id: str,
    message: str,
    page_access_token: str = "",
) -> str:
    """Reply to a comment on a Facebook Page post.

    Args:
        comment_id: Comment ID to reply to (required).
        message: Reply text (required).
        page_access_token: Page access token (required for replying as the page).
    """
    if (err := require_editor("meta_reply_to_comment")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_post_with_token(f"{comment_id}/comments", tok, {"message": message}))


@mcp.tool()
def meta_hide_comment(
    comment_id: str,
    is_hidden: bool = True,
    page_access_token: str = "",
) -> str:
    """Hide or unhide a comment on a Facebook Page post.

    Args:
        comment_id: Comment ID (required).
        is_hidden: True to hide, False to unhide. Default True.
        page_access_token: Page access token (required).
    """
    if (err := require_editor("meta_hide_comment")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_post_with_token(comment_id, tok, {"is_hidden": "true" if is_hidden else "false"}))


@mcp.tool()
def meta_delete_comment(
    comment_id: str,
    page_access_token: str = "",
) -> str:
    """Delete a comment on a Facebook Page post.

    Args:
        comment_id: Comment ID to delete (required).
        page_access_token: Page access token (required).
    """
    if (err := require_editor("meta_delete_comment")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_delete_with_token(comment_id, tok))


@mcp.tool()
def meta_like_object(
    object_id: str,
    page_access_token: str = "",
    unlike: bool = False,
) -> str:
    """Like or unlike a Facebook post or comment as the Page.

    Args:
        object_id: Post ID or comment ID to like/unlike (required).
        page_access_token: Page access token (required to act as the page).
        unlike: True to remove the like, False to add it. Default False.
    """
    if (err := require_editor("meta_like_object")): return err
    tok = _page_tok(page_access_token)
    if unlike:
        return json.dumps(_delete_with_token(f"{object_id}/likes", tok))
    return json.dumps(_post_with_token(f"{object_id}/likes", tok))


# ---------------------------------------------------------------------------
# Pages API — Messaging (Inbox / Conversations)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_page_conversations(
    page_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get the inbox conversations (messages) for a Facebook Page.

    Requires pages_messaging permission on the page token.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token with pages_messaging permission (required).
        limit: Number of conversations to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/conversations", tok, {
        "fields": "id,snippet,updated_time,message_count,unread_count,participants",
        "limit": limit,
    }))


@mcp.tool()
def meta_get_conversation_messages(
    conversation_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get messages in a Facebook Page conversation/thread.

    Args:
        conversation_id: Conversation ID from meta_get_page_conversations (required).
        page_access_token: Page access token (required).
        limit: Number of messages to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{conversation_id}/messages", tok, {
        "fields": "id,message,from,created_time,attachments",
        "limit": limit,
    }))


@mcp.tool()
def meta_reply_to_page_message(
    recipient_id: str,
    message: str,
    page_id: str,
    page_access_token: str = "",
    messaging_type: str = "RESPONSE",
) -> str:
    """Send a reply message from a Facebook Page to a user.

    Requires pages_messaging permission on the page token.

    Args:
        recipient_id: The PSID (Page-Scoped User ID) of the recipient (required).
        message: Message text to send (required).
        page_id: Facebook Page ID sending the message (required).
        page_access_token: Page access token with pages_messaging permission (required).
        messaging_type: RESPONSE (reply to user message), UPDATE, or MESSAGE_TAG. Default RESPONSE.
    """
    if (err := require_editor("meta_reply_to_page_message")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_post_with_token(f"{page_id}/messages", tok, {
        "recipient": json.dumps({"id": recipient_id}),
        "message": json.dumps({"text": message}),
        "messaging_type": messaging_type,
    }))


# ---------------------------------------------------------------------------
# Pages API — Media (Photos, Videos, Albums)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_page_photos(
    page_id: str,
    page_access_token: str = "",
    limit: int = 25,
    photo_type: str = "uploaded",
) -> str:
    """Get photos published on a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of photos to return. Default 25.
        photo_type: 'uploaded' (posted by page), 'tagged' (page is tagged), or 'profile'. Default uploaded.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/photos", tok, {
        "type": photo_type,
        "fields": "id,name,created_time,likes.summary(true),images,permalink_url",
        "limit": limit,
    }))


@mcp.tool()
def meta_get_page_videos(
    page_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get videos published on a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of videos to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/videos", tok, {
        "fields": "id,title,description,created_time,length,views,likes.summary(true),comments.summary(true),permalink_url",
        "limit": limit,
    }))


@mcp.tool()
def meta_get_page_albums(
    page_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get photo albums on a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of albums to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/albums", tok, {
        "fields": "id,name,description,created_time,count,type,link",
        "limit": limit,
    }))


# ---------------------------------------------------------------------------
# Pages API — Events
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_page_events(
    page_id: str,
    page_access_token: str = "",
    limit: int = 25,
    time_filter: str = "upcoming",
) -> str:
    """Get events for a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of events to return. Default 25.
        time_filter: 'upcoming', 'past', or 'canceled'. Default upcoming.
    """
    tok = _page_tok(page_access_token)
    params: dict = {
        "fields": "id,name,description,start_time,end_time,place,attending_count,interested_count,ticket_uri,cover",
        "limit": limit,
    }
    if time_filter == "upcoming":
        params["time_filter"] = "upcoming"
    elif time_filter == "past":
        params["time_filter"] = "past"
    return json.dumps(_get_with_token(f"{page_id}/events", tok, params))


@mcp.tool()
def meta_create_page_event(
    page_id: str,
    name: str,
    start_time: str,
    end_time: str = "",
    description: str = "",
    location: str = "",
    ticket_uri: str = "",
    is_online: bool = False,
    page_access_token: str = "",
) -> str:
    """Create a new event on a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        name: Event name (required).
        start_time: Event start time in ISO 8601 format, e.g. '2025-06-15T18:00:00+0300' (required).
        end_time: Event end time in ISO 8601 format. Optional.
        description: Event description. Optional.
        location: Physical location/venue name. Optional.
        ticket_uri: URL to purchase tickets. Optional.
        is_online: True if the event is online only. Default False.
        page_access_token: Page access token (required).
    """
    tok = _page_tok(page_access_token)
    data: dict = {
        "name": name,
        "start_time": start_time,
    }
    if end_time:
        data["end_time"] = end_time
    if description:
        data["description"] = description
    if location:
        data["location"] = location
    if ticket_uri:
        data["ticket_uri"] = ticket_uri
    if is_online:
        data["online_event_format"] = "LIVE"
    return json.dumps(_post_with_token(f"{page_id}/events", tok, data))


# ---------------------------------------------------------------------------
# Pages API — Reviews & Page Info
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_page_reviews(
    page_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get ratings and reviews for a Facebook Page.

    Requires pages_read_user_content permission.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of reviews to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/ratings", tok, {
        "fields": "reviewer,rating,review_text,created_time,has_rating,has_review",
        "limit": limit,
    }))


@mcp.tool()
def meta_get_page_info(
    page_id: str,
    page_access_token: str = "",
) -> str:
    """Get detailed information about a Facebook Page (name, about, category, contact info, etc.).

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(page_id, tok, {
        "fields": "id,name,about,description,category,fan_count,followers_count,website,phone,emails,location,hours,rating_count,overall_star_rating,link,username,verification_status,cover,picture",
    }))


@mcp.tool()
def meta_update_page_info(
    page_id: str,
    page_access_token: str,
    about: str = "",
    description: str = "",
    website: str = "",
    phone: str = "",
    hours: str = "",
    emails: str = "",
) -> str:
    """Update basic information on a Facebook Page (about, description, website, phone, hours).

    Requires pages_manage_metadata permission.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token with pages_manage_metadata permission (required).
        about: Short description / about text. Optional.
        description: Long description. Optional.
        website: Page website URL. Optional.
        phone: Contact phone number. Optional.
        hours: JSON string of business hours, e.g. '{"mon_1_open":"09:00","mon_1_close":"18:00"}'. Optional.
        emails: Comma-separated list of contact emails. Optional.
    """
    tok = _page_tok(page_access_token)
    data: dict = {}
    if about:
        data["about"] = about
    if description:
        data["description"] = description
    if website:
        data["website"] = website
    if phone:
        data["phone"] = phone
    if hours:
        data["hours"] = hours
    if emails:
        data["emails"] = json.dumps([e.strip() for e in emails.split(",") if e.strip()])
    if not data:
        return json.dumps({"error": "Provide at least one field to update."})
    return json.dumps(_post_with_token(page_id, tok, data))


@mcp.tool()
def meta_get_page_mentions(
    page_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get posts and comments where the Facebook Page is mentioned/tagged.

    Requires pages_read_user_content permission.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of mentions to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/tagged", tok, {
        "fields": "id,message,story,created_time,from,permalink_url",
        "limit": limit,
    }))


@mcp.tool()
def meta_get_page_post_reactions(
    post_id: str,
    reaction_type: str = "TOTAL",
    page_access_token: str = "",
) -> str:
    """Get reactions (likes, loves, hahas, wows, sads, angrys) on a Facebook Page post.

    Args:
        post_id: Post ID (required).
        reaction_type: TOTAL, LIKE, LOVE, HAHA, WOW, SAD, ANGRY, or CARE. Default TOTAL (all reactions).
        page_access_token: Page access token. Falls back to user token if blank.
    """
    tok = _page_tok(page_access_token)
    params: dict = {
        "fields": "id,name,pic,type",
        "limit": 100,
    }
    if reaction_type != "TOTAL":
        params["type"] = reaction_type
    return json.dumps(_get_with_token(f"{post_id}/reactions", tok, params))


# ---------------------------------------------------------------------------
# Pages API — Roles & Access Management
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_page_roles(
    page_id: str,
    page_access_token: str = "",
) -> str:
    """Get all people and their roles on a Facebook Page (admins, editors, moderators, advertisers, analysts).

    Requires Page admin access.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token with admin rights (required).
    """
    tok = _page_tok(page_access_token)
    results = {}
    for role in ["admins", "editors", "moderators", "advertisers", "analysts"]:
        results[role] = _get_with_token(f"{page_id}/roles", tok, {"fields": "id,name,role", "limit": 50})
    # Use the proper /roles endpoint which returns all roles at once
    all_roles = _get_with_token(f"{page_id}/roles", tok, {"fields": "id,name,role", "limit": 100})
    return json.dumps(all_roles)


@mcp.tool()
def meta_add_page_role(
    page_id: str,
    user: str,
    role: str,
    page_access_token: str,
) -> str:
    """Add a person to a Facebook Page with a specific role.

    Requires Page admin access. The user must have liked the page or be connectable.

    Args:
        page_id: Facebook Page ID (required).
        user: Facebook user ID or username of the person to add (required).
        role: Role to assign: ADMINISTRATOR, EDITOR, MODERATOR, ADVERTISER, or ANALYST (required).
        page_access_token: Page access token with admin rights (required).
    """
    if (err := require_editor("meta_add_page_role")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_post_with_token(f"{page_id}/roles", tok, {
        "user": user,
        "role": role,
    }))


@mcp.tool()
def meta_remove_page_role(
    page_id: str,
    user: str,
    page_access_token: str,
) -> str:
    """Remove a person from a Facebook Page role.

    Requires Page admin access.

    Args:
        page_id: Facebook Page ID (required).
        user: Facebook user ID of the person to remove (required).
        page_access_token: Page access token with admin rights (required).
    """
    if (err := require_editor("meta_remove_page_role")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_post_with_token(f"{page_id}/roles", tok, {
        "user": user,
        "role": "REMOVE",
    }))


@mcp.tool()
def meta_get_page_blocked_users(
    page_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get users who are blocked from the Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token (required).
        limit: Number of blocked users to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/blocked", tok, {
        "fields": "id,name,pic",
        "limit": limit,
    }))


@mcp.tool()
def meta_block_page_user(
    page_id: str,
    user_id: str,
    page_access_token: str,
) -> str:
    """Block a user from interacting with a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        user_id: Facebook user ID to block (required).
        page_access_token: Page access token with moderation rights (required).
    """
    if (err := require_editor("meta_block_page_user")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_post_with_token(f"{page_id}/blocked", tok, {"user": user_id}))


@mcp.tool()
def meta_unblock_page_user(
    page_id: str,
    user_id: str,
    page_access_token: str,
) -> str:
    """Unblock a user so they can interact with a Facebook Page again.

    Args:
        page_id: Facebook Page ID (required).
        user_id: Facebook user ID to unblock (required).
        page_access_token: Page access token with moderation rights (required).
    """
    if (err := require_editor("meta_unblock_page_user")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_delete_with_token(f"{page_id}/blocked?user={user_id}", tok))


# ---------------------------------------------------------------------------
# Pages API — Instagram Integration (via Facebook Page)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_instagram_account(
    page_id: str,
    page_access_token: str = "",
) -> str:
    """Get the Instagram Business/Creator account linked to a Facebook Page.

    Returns the Instagram account ID needed for posting and insights.

    Args:
        page_id: Facebook Page ID linked to the Instagram account (required).
        page_access_token: Page access token. Falls back to user token if blank.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(page_id, tok, {
        "fields": "instagram_business_account{id,name,username,biography,followers_count,follows_count,media_count,profile_picture_url,website}",
    }))


@mcp.tool()
def meta_get_instagram_media(
    instagram_account_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get media (posts) from an Instagram Business account linked to a Facebook Page.

    Args:
        instagram_account_id: Instagram Business Account ID (get it from meta_get_instagram_account) (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of media items to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{instagram_account_id}/media", tok, {
        "fields": "id,caption,media_type,media_url,thumbnail_url,timestamp,like_count,comments_count,permalink",
        "limit": limit,
    }))


@mcp.tool()
def meta_create_instagram_post(
    instagram_account_id: str,
    image_url: str,
    caption: str = "",
    page_access_token: str = "",
    is_carousel: bool = False,
    children: str = "",
) -> str:
    """Create and publish a post on Instagram via the Facebook Page (Instagram Graph API).

    This is a 2-step process: first creates a container, then publishes it.
    For carousel posts, first create individual item containers, then pass their IDs in 'children'.

    Args:
        instagram_account_id: Instagram Business Account ID (required).
        image_url: Public URL of the image (JPEG/PNG) or video to post. For carousels, leave blank.
        caption: Post caption text. Optional.
        page_access_token: Page access token (required for posting).
        is_carousel: True to create a carousel post. Pass children IDs in 'children'. Default False.
        children: Comma-separated container IDs for carousel items. Only used when is_carousel=True.
    """
    if (err := require_editor("meta_create_instagram_post")): return err
    tok = _page_tok(page_access_token)

    if is_carousel:
        children_list = [c.strip() for c in children.split(",") if c.strip()]
        container = _post_with_token(f"{instagram_account_id}/media", tok, {
            "media_type": "CAROUSEL",
            "caption": caption,
            "children": json.dumps(children_list),
        })
    else:
        container_data: dict = {"image_url": image_url}
        if caption:
            container_data["caption"] = caption
        container = _post_with_token(f"{instagram_account_id}/media", tok, container_data)

    if "error" in container:
        return json.dumps(container)

    container_id = container.get("id")
    if not container_id:
        return json.dumps({"error": "Failed to create media container", "response": container})

    # Step 2: publish
    publish = _post_with_token(f"{instagram_account_id}/media_publish", tok, {
        "creation_id": container_id,
    })
    return json.dumps({"container_id": container_id, "publish_result": publish})


@mcp.tool()
def meta_get_instagram_insights(
    instagram_account_id: str,
    metric: str = "impressions,reach,follower_count,profile_views",
    period: str = "day",
    start_date: str = "",
    end_date: str = "",
    page_access_token: str = "",
) -> str:
    """Get insights/analytics for an Instagram Business account linked to a Facebook Page.

    Args:
        instagram_account_id: Instagram Business Account ID (required).
        metric: Comma-separated metrics: impressions, reach, follower_count, profile_views, email_contacts, phone_call_clicks, website_clicks. Default impressions,reach,follower_count,profile_views.
        period: 'day', 'week', 'month', or 'lifetime'. Default day.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        page_access_token: Page access token. Falls back to user token if blank.
    """
    import datetime as _dt
    tok = _page_tok(page_access_token)
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    since_epoch = int(_dt.datetime.fromisoformat(sd).timestamp())
    until_epoch = int(_dt.datetime.fromisoformat(ed).timestamp())
    return json.dumps(_get_with_token(f"{instagram_account_id}/insights", tok, {
        "metric": metric,
        "period": period,
        "since": since_epoch,
        "until": until_epoch,
    }))


@mcp.tool()
def meta_publish_page_post_to_instagram(
    page_id: str,
    post_id: str,
    page_access_token: str,
) -> str:
    """Cross-post an existing Facebook Page post to the linked Instagram account.

    Note: The post must contain an image or video. Text-only posts cannot be cross-posted.

    Args:
        page_id: Facebook Page ID (required).
        post_id: Facebook Page post ID to cross-post (required).
        page_access_token: Page access token (required).
    """
    if (err := require_editor("meta_publish_page_post_to_instagram")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_post_with_token(f"{page_id}/crosspost_whitelisted_pages", tok, {
        "crosspost_id": post_id,
    }))


# ---------------------------------------------------------------------------
# Pages API — Publish / Unpublish / Restrict
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_set_page_publish_status(
    page_id: str,
    is_published: bool,
    page_access_token: str,
) -> str:
    """Publish or unpublish a Facebook Page (make it visible or hidden to the public).

    Requires Page admin access.

    Args:
        page_id: Facebook Page ID (required).
        is_published: True to publish/make visible, False to unpublish/hide. (required).
        page_access_token: Page access token with admin rights (required).
    """
    if (err := require_editor("meta_set_page_publish_status")): return err
    tok = _page_tok(page_access_token)
    return json.dumps(_post_with_token(page_id, tok, {
        "is_published": "true" if is_published else "false",
    }))


@mcp.tool()
def meta_get_page_insights_extended(
    page_id: str,
    metrics: str = "page_impressions,page_reach,page_engaged_users,page_fan_adds,page_fan_removes,page_views_total,page_post_engagements,page_actions_post_reactions_total",
    period: str = "day",
    start_date: str = "",
    end_date: str = "",
    page_access_token: str = "",
) -> str:
    """Get comprehensive Facebook Page insights including fans, views, reactions, and engagement.

    Args:
        page_id: Facebook Page ID (required).
        metrics: Comma-separated metric names. Default includes impressions, reach, engagement, fans, views.
        period: 'day', 'week', 'month', or 'lifetime'. Default day.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        page_access_token: Page access token. Falls back to user token if blank.
    """
    import datetime as _dt
    tok = _page_tok(page_access_token)
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    since_epoch = int(_dt.datetime.fromisoformat(sd).timestamp())
    until_epoch = int(_dt.datetime.fromisoformat(ed).timestamp())
    return json.dumps(_get_with_token(f"{page_id}/insights", tok, {
        "metric": metrics,
        "period": period,
        "since": since_epoch,
        "until": until_epoch,
    }))


# ---------------------------------------------------------------------------
# Google Drive helpers
# ---------------------------------------------------------------------------

def _gdrive_service():
    """Return an authenticated Google Drive API v3 service object using per-user OAuth."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build

    user = current_user_ctx.get(None)
    if user is None:
        raise RuntimeError("Not authenticated.")
    rt = user.get_google_token()
    if not rt:
        raise RuntimeError("Google account not connected. Visit /onboard.")
    creds = Credentials(
        token=None,
        refresh_token=rt,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=os.environ["GOOGLE_CLIENT_ID"],
        client_secret=os.environ["GOOGLE_CLIENT_SECRET"],
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    creds.refresh(Request())
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _extract_folder_id(folder_url_or_id: str) -> str:
    """Accept a full Drive folder URL or a bare folder ID."""
    import re
    m = re.search(r"folders/([a-zA-Z0-9_-]+)", folder_url_or_id)
    if m:
        return m.group(1)
    m = re.search(r"id=([a-zA-Z0-9_-]+)", folder_url_or_id)
    if m:
        return m.group(1)
    # Assume it's already a bare ID
    return folder_url_or_id.strip()


def _extract_file_id(file_url_or_id: str) -> str:
    """Accept a full Drive file URL or a bare file ID."""
    import re
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", file_url_or_id)
    if m:
        return m.group(1)
    m = re.search(r"id=([a-zA-Z0-9_-]+)", file_url_or_id)
    if m:
        return m.group(1)
    return file_url_or_id.strip()


# Video MIME types recognised by Google Drive
_VIDEO_MIME_TYPES = [
    "video/mp4", "video/quicktime", "video/x-msvideo", "video/x-ms-wmv",
    "video/mpeg", "video/webm", "video/3gpp", "video/x-flv", "video/ogg",
    "video/x-matroska",
]


@mcp.tool()
def google_drive_list_videos(
    folder_url: str = "",
) -> str:
    """List all video files inside a Google Drive folder.

    Args:
        folder_url: Full Google Drive folder URL
                    (https://drive.google.com/drive/folders/FOLDER_ID)
                    or a bare folder ID.
                    Leave blank to search across the entire Drive.

    Returns:
        JSON array of video files, each with:
        id, name, mimeType, size (bytes), modifiedTime, webViewLink.
        Pass the 'id' or 'webViewLink' to google_drive_get_video_info or
        meta_upload_ad_video.
    """
    service = _gdrive_service()

    mime_filter = " or ".join(f"mimeType='{m}'" for m in _VIDEO_MIME_TYPES)
    query = f"({mime_filter}) and trashed=false"

    if folder_url:
        folder_id = _extract_folder_id(folder_url)
        query += f" and '{folder_id}' in parents"

    fields = "files(id,name,mimeType,size,modifiedTime,webViewLink)"
    result = service.files().list(
        q=query,
        fields=fields,
        pageSize=50,
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()

    files = result.get("files", [])
    if not files:
        return json.dumps({"message": "No video files found.", "files": []})
    return json.dumps({"count": len(files), "files": files}, ensure_ascii=False)


@mcp.tool()
def google_drive_get_video_info(
    file_url: str,
) -> str:
    """Get detailed metadata for a Google Drive video file.

    Use this to inspect a video before uploading it to Meta — you can see its
    name, size, MIME type, description, and thumbnail.

    Args:
        file_url: Full Google Drive file URL
                  (https://drive.google.com/file/d/FILE_ID/view)
                  or a bare file ID.

    Returns:
        JSON with: id, name, mimeType, size, description,
        createdTime, modifiedTime, thumbnailLink, webViewLink.
    """
    service = _gdrive_service()
    file_id = _extract_file_id(file_url)

    meta = service.files().get(
        fileId=file_id,
        fields="id,name,mimeType,size,description,createdTime,modifiedTime,thumbnailLink,webViewLink",
        supportsAllDrives=True,
    ).execute()

    return json.dumps(meta, ensure_ascii=False)


@mcp.tool()
def google_drive_upload_video_to_meta(
    file_url: str,
    title: str = "",
    account_id: str = "",
) -> str:
    """Full one-shot flow: download a video from Google Drive and upload it to
    the Meta Ads video library.

    Combines google_drive_get_video_info + meta_upload_ad_video into a single
    call. Handles large files and the Drive virus-scan warning page automatically.

    Args:
        file_url: Full Google Drive file URL or bare file ID.
        title: Optional title for the video in Meta. Defaults to the Drive filename.
        account_id: Ad account ID. Leave blank to use .env default.

    Returns:
        JSON with the Meta video ID, e.g. {"id": "12345678"}.
    """
    import tempfile

    service = _gdrive_service()
    file_id = _extract_file_id(file_url)

    # 1. Fetch metadata to get the real filename and MIME type
    meta = service.files().get(
        fileId=file_id,
        fields="id,name,mimeType,size",
        supportsAllDrives=True,
    ).execute()

    video_title = title or meta.get("name", "video")
    mime_type = meta.get("mimeType", "video/mp4")

    # 2. Download via Drive API (authenticated) — avoids the public virus-scan wall
    from googleapiclient.http import MediaIoBaseDownload
    import io

    request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
    suffix = "." + mime_type.split("/")[-1].split(";")[0]  # e.g. ".mp4"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp_path = tmp.name

    try:
        with open(tmp_path, "wb") as fh:
            downloader = MediaIoBaseDownload(fh, request, chunksize=4 * 1024 * 1024)
            done = False
            while not done:
                _, done = downloader.next_chunk()

        # 3. Upload to Meta via multipart
        token = _token()
        acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
        upload_url = f"https://graph-video.facebook.com/v21.0/{acct}/advideos"

        with open(tmp_path, "rb") as f:
            result = requests.post(
                upload_url,
                data={"title": video_title, "access_token": token},
                files={"source": (video_title, f, mime_type)},
                timeout=600,
            )
        return result.text
    finally:
        os.unlink(tmp_path)


@mcp.tool()
def google_drive_list_files(
    folder_url: str = "",
    mime_type_filter: str = "",
    page_size: int = 50,
    include_trashed: bool = False,
) -> str:
    """List ANY files in a Google Drive folder — not just videos.

    Args:
        folder_url: Full Drive folder URL
                    (https://drive.google.com/drive/folders/FOLDER_ID)
                    or a bare folder ID. Leave blank to search all of Drive.
        mime_type_filter: Optional MIME type to filter by, e.g. "video/mp4",
                          "image/jpeg", "application/pdf", "application/vnd.google-apps.document".
                          Leave blank to list all file types.
        page_size: Max number of files to return (1–1000, default 50).
        include_trashed: Include files in the trash. Default False.

    Returns:
        JSON with count and an array of files, each with:
        id, name, mimeType, size (bytes), modifiedTime, createdTime,
        webViewLink, webContentLink.
    """
    service = _gdrive_service()

    conditions = []
    if not include_trashed:
        conditions.append("trashed=false")
    if mime_type_filter:
        conditions.append(f"mimeType='{mime_type_filter}'")
    if folder_url:
        folder_id = _extract_folder_id(folder_url)
        conditions.append(f"'{folder_id}' in parents")

    query = " and ".join(conditions) if conditions else None

    fields = "nextPageToken,files(id,name,mimeType,size,modifiedTime,createdTime,webViewLink,webContentLink)"
    kwargs: dict = {
        "fields": fields,
        "pageSize": min(max(1, page_size), 1000),
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
        "orderBy": "modifiedTime desc",
    }
    if query:
        kwargs["q"] = query

    result = service.files().list(**kwargs).execute()
    files = result.get("files", [])
    next_page = result.get("nextPageToken")

    return json.dumps(
        {"count": len(files), "files": files, "nextPageToken": next_page},
        ensure_ascii=False,
    )


@mcp.tool()
def google_drive_download_file(
    file_url: str,
    destination_dir: str = "",
) -> str:
    """Download ANY file from Google Drive to local disk.

    Works for all Drive-hosted file types: MP4 videos, PDFs, images, ZIPs,
    Office docs, etc.  Google Workspace formats (Docs, Sheets, Slides) are
    automatically exported to their most portable equivalent
    (DOCX, XLSX, PPTX).

    Args:
        file_url: Full Google Drive file URL
                  (https://drive.google.com/file/d/FILE_ID/view)
                  or a bare file ID.
        destination_dir: Local directory to save the file. Defaults to the
                         system temp directory.

    Returns:
        JSON with:
        - local_path: Absolute path to the downloaded file.
        - name: Original Drive filename.
        - mimeType: MIME type of the downloaded file.
        - size_bytes: File size on disk.
    """
    import tempfile
    import io as _io
    from googleapiclient.http import MediaIoBaseDownload

    service = _gdrive_service()
    file_id = _extract_file_id(file_url)

    # Fetch metadata first
    meta = service.files().get(
        fileId=file_id,
        fields="id,name,mimeType,size",
        supportsAllDrives=True,
    ).execute()

    name = meta.get("name", "downloaded_file")
    mime = meta.get("mimeType", "application/octet-stream")

    # Google Workspace formats need export rather than direct download
    _EXPORT_MAP = {
        "application/vnd.google-apps.document":
            ("application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx"),
        "application/vnd.google-apps.spreadsheet":
            ("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"),
        "application/vnd.google-apps.presentation":
            ("application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx"),
        "application/vnd.google-apps.drawing":
            ("image/png", ".png"),
        "application/vnd.google-apps.script":
            ("application/vnd.google-apps.script+json", ".json"),
    }

    dest_dir = destination_dir or tempfile.gettempdir()
    os.makedirs(dest_dir, exist_ok=True)

    if mime in _EXPORT_MAP:
        export_mime, ext = _EXPORT_MAP[mime]
        safe_name = name + ext
        local_path = os.path.join(dest_dir, safe_name)
        request = service.files().export_media(fileId=file_id, mimeType=export_mime)
        actual_mime = export_mime
    else:
        # Derive extension from the original name or MIME type
        ext = ("." + name.rsplit(".", 1)[-1]) if "." in name else ""
        local_path = os.path.join(dest_dir, name)
        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        actual_mime = mime

    buf = _io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request, chunksize=8 * 1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()

    with open(local_path, "wb") as fh:
        fh.write(buf.getvalue())

    size = os.path.getsize(local_path)
    return json.dumps(
        {"local_path": local_path, "name": name, "mimeType": actual_mime, "size_bytes": size},
        ensure_ascii=False,
    )


@mcp.tool()
def google_drive_watch_folder(
    folder_url: str,
    since_minutes: int = 60,
    mime_type_filter: str = "",
    page_size: int = 100,
) -> str:
    """Poll a Google Drive folder for files created or modified recently.

    Because Drive push-notification webhooks require a public HTTPS server,
    this tool uses polling instead: it lists all files in the folder whose
    modifiedTime is newer than (now − since_minutes) and returns them.

    Call this tool on a schedule (e.g. every minute) to detect new uploads.

    Args:
        folder_url: Full Drive folder URL or bare folder ID.
        since_minutes: How far back to look for changes, in minutes (default 60).
                       Use 1 for near-real-time polling.
        mime_type_filter: Optional MIME type to filter results, e.g. "video/mp4".
                          Leave blank for all file types.
        page_size: Max files to return (1–1000, default 100).

    Returns:
        JSON with:
        - checked_at: ISO timestamp when this check ran (UTC).
        - since: ISO timestamp lower-bound used for the query.
        - new_or_modified_count: How many files matched.
        - files: List of matching files with id, name, mimeType, size,
                 createdTime, modifiedTime, webViewLink.
    """
    from datetime import timezone

    service = _gdrive_service()
    folder_id = _extract_folder_id(folder_url)

    now_utc = date.today()  # use datetime instead below
    import datetime as _dt
    now_utc = _dt.datetime.now(_dt.timezone.utc)
    since_utc = now_utc - _dt.timedelta(minutes=since_minutes)
    since_str = since_utc.strftime("%Y-%m-%dT%H:%M:%S")  # RFC 3339 without tz suffix

    conditions = [
        f"'{folder_id}' in parents",
        "trashed=false",
        f"modifiedTime > '{since_str}'",
    ]
    if mime_type_filter:
        conditions.append(f"mimeType='{mime_type_filter}'")

    query = " and ".join(conditions)
    fields = "files(id,name,mimeType,size,createdTime,modifiedTime,webViewLink)"

    result = service.files().list(
        q=query,
        fields=fields,
        pageSize=min(max(1, page_size), 1000),
        orderBy="modifiedTime desc",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()

    files = result.get("files", [])
    return json.dumps(
        {
            "checked_at": now_utc.isoformat(),
            "since": since_utc.isoformat(),
            "new_or_modified_count": len(files),
            "files": files,
        },
        ensure_ascii=False,
    )


# ---------------------------------------------------------------------------
# BLOCK 1: AD ACCOUNT MANAGEMENT (META-01 to META-08)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_ad_account(
    account_id: str = "",
) -> str:
    """Get full details for a specific Meta ad account: name, currency, timezone, status, balance, spend.

    Args:
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}", {
        "fields": "id,name,currency,timezone_name,account_status,business,balance,amount_spent,spend_cap,daily_spend_limit,funding_source_details",
    }))


@mcp.tool()
def meta_get_account_spending_limit(
    account_id: str = "",
) -> str:
    """Get the spend cap, daily limit, amount spent, and remaining budget for a Meta ad account.

    Args:
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    result = _get(f"{acct}", {
        "fields": "spend_cap,daily_spend_limit,amount_spent,balance",
    })
    if "error" in result:
        return json.dumps(result)
    spent = int(result.get("amount_spent", 0))
    cap = int(result.get("spend_cap", 0))
    daily = int(result.get("daily_spend_limit", 0))
    remaining = (cap - spent) if cap > 0 else None
    return json.dumps({
        "spend_cap": cap,
        "daily_limit": daily,
        "amount_spent": spent,
        "remaining": remaining,
        "balance": result.get("balance"),
        "raw": result,
    })


@mcp.tool()
def meta_list_ads(
    account_id: str = "",
    campaign_id: str = "",
    adset_id: str = "",
    status_filter: str = "ALL",
) -> str:
    """List all ads with their IDs, creative IDs, adset IDs, campaign IDs, and status.

    Args:
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        campaign_id: Filter by campaign ID. Leave blank for all.
        adset_id: Filter by adset ID. Leave blank for all.
        status_filter: ACTIVE, PAUSED, ARCHIVED, or ALL. Default ALL.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    params: dict = {
        "fields": "id,name,status,creative{id,name},adset_id,campaign_id",
        "limit": 100,
    }
    filters = []
    if campaign_id:
        filters.append({"field": "campaign.id", "operator": "EQUAL", "value": campaign_id})
    if adset_id:
        filters.append({"field": "adset.id", "operator": "EQUAL", "value": adset_id})
    if filters:
        params["filtering"] = json.dumps(filters)
    if status_filter != "ALL":
        params["effective_status"] = json.dumps([status_filter])
    return json.dumps(_get(f"{acct}/ads", params))


@mcp.tool()
def meta_get_ad(
    ad_id: str,
) -> str:
    """Get full details for a specific Meta ad including creative, adset, campaign, tracking specs.

    Args:
        ad_id: Ad ID (required).
    """
    return json.dumps(_get(f"{ad_id}", {
        "fields": "id,name,status,creative{id,name,body,title,image_url,thumbnail_url},adset_id,campaign_id,tracking_specs,conversion_specs,created_time,updated_time",
    }))


@mcp.tool()
def meta_update_ad(
    ad_id: str,
    name: str = "",
    status: str = "",
) -> str:
    """Update the name or status of a Meta ad.

    Args:
        ad_id: Ad ID to update (required).
        name: New ad name. Leave blank to keep unchanged.
        status: New status: ACTIVE, PAUSED, or ARCHIVED. Leave blank to keep unchanged.
    """
    if (err := require_editor("meta_update_ad")): return err
    data: dict = {}
    if name:
        data["name"] = name
    if status:
        data["status"] = status
    if not data:
        return json.dumps({"error": "Provide name or status to update."})
    result = _post(ad_id, data)
    return json.dumps({"success": "error" not in result, **result})


@mcp.tool()
def meta_delete_ad(
    ad_id: str,
) -> str:
    """Delete (archive) a Meta ad.

    Args:
        ad_id: Ad ID to delete (required).
    """
    if (err := require_editor("meta_delete_ad")): return err
    result = _delete(ad_id)
    return json.dumps({"success": result.get("success", False), "ad_id": ad_id, **result})


@mcp.tool()
def meta_delete_adset(
    adset_id: str,
) -> str:
    """Delete (archive) a Meta ad set.

    Args:
        adset_id: Ad set ID to delete (required).
    """
    if (err := require_editor("meta_delete_adset")): return err
    result = _delete(adset_id)
    return json.dumps({"success": result.get("success", False), "adset_id": adset_id, **result})


@mcp.tool()
def meta_delete_campaign(
    campaign_id: str,
) -> str:
    """Delete (archive) a Meta campaign.

    Args:
        campaign_id: Campaign ID to delete (required).
    """
    if (err := require_editor("meta_delete_campaign")): return err
    result = _delete(campaign_id)
    return json.dumps({"success": result.get("success", False), "campaign_id": campaign_id, **result})


# ---------------------------------------------------------------------------
# BLOCK 2: CREATIVE MANAGEMENT (META-09 to META-14)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_ad_creative(
    creative_id: str,
) -> str:
    """Get full details of a specific Meta ad creative including image hash, video ID, copy, and call-to-action.

    Args:
        creative_id: Creative ID (required).
    """
    return json.dumps(_get(f"{creative_id}", {
        "fields": "id,name,body,title,image_url,image_hash,video_id,object_type,object_url,call_to_action,thumbnail_url,status,effective_object_story_id,object_story_spec",
    }))


@mcp.tool()
def meta_update_ad_creative(
    creative_id: str,
    account_id: str = "",
    body: str = "",
    title: str = "",
    call_to_action_type: str = "",
    call_to_action_link: str = "",
) -> str:
    """Update ad creative copy by creating a new creative (Meta does not allow in-place edits).

    Returns the new_creative_id — use meta_update_ad to attach it to your ad.

    Args:
        creative_id: Existing creative ID to base the copy on (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        body: New ad body copy. Leave blank to keep from original.
        title: New ad title/headline. Leave blank to keep from original.
        call_to_action_type: CTA type e.g. LEARN_MORE, SHOP_NOW, SIGN_UP. Leave blank to keep original.
        call_to_action_link: CTA destination URL. Leave blank to keep original.
    """
    if (err := require_editor("meta_update_ad_creative")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    # Fetch existing creative
    existing = _get(f"{creative_id}", {
        "fields": "name,body,title,image_hash,video_id,object_type,object_url,call_to_action,object_story_spec",
    })
    if "error" in existing:
        return json.dumps(existing)
    data: dict = {
        "name": f"{existing.get('name', creative_id)}_updated",
    }
    if existing.get("object_story_spec"):
        spec = existing["object_story_spec"]
        link_data = spec.get("link_data") or spec.get("video_data") or {}
        if body:
            link_data["message"] = body
        if title:
            link_data["name"] = title
        if call_to_action_type or call_to_action_link:
            cta = link_data.get("call_to_action", {})
            if call_to_action_type:
                cta["type"] = call_to_action_type
            if call_to_action_link:
                cta.setdefault("value", {})["link"] = call_to_action_link
            link_data["call_to_action"] = cta
        if "link_data" in spec:
            spec["link_data"] = link_data
        elif "video_data" in spec:
            spec["video_data"] = link_data
        data["object_story_spec"] = json.dumps(spec)
    else:
        if body:
            data["body"] = body
        if title:
            data["title"] = title
        if existing.get("image_hash"):
            data["image_hash"] = existing["image_hash"]
        if existing.get("video_id"):
            data["video_id"] = existing["video_id"]
        if existing.get("object_url"):
            data["object_url"] = existing["object_url"]
    result = _post(f"{acct}/adcreatives", data)
    if "error" in result:
        return json.dumps(result)
    return json.dumps({"new_creative_id": result.get("id"), "result": result})


@mcp.tool()
def meta_upload_image_to_meta(
    image_url: str,
    account_id: str = "",
) -> str:
    """Upload a JPG/PNG image from a public URL to Meta's ad image library.

    The returned image_hash is needed to create image-based ad creatives.

    Args:
        image_url: Public URL of the image (JPEG or PNG) to upload (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
    """
    if (err := require_editor("meta_upload_image_to_meta")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    result = _post(f"{acct}/adimages", {"url": image_url})
    if "error" in result:
        return json.dumps(result)
    images = result.get("images", {})
    if images:
        first = next(iter(images.values()))
        return json.dumps({"image_hash": first.get("hash"), "url": first.get("url"), "raw": result})
    return json.dumps(result)


@mcp.tool()
def meta_create_carousel_ad_creative(
    name: str,
    page_id: str,
    message: str,
    link: str,
    cards: str,
    account_id: str = "",
) -> str:
    """Create a carousel (multi-card) ad creative with 2-10 cards.

    Args:
        name: Creative name (required).
        page_id: Facebook Page ID (required).
        message: Ad body copy shown above the carousel (required).
        link: Default destination URL for cards without a specific link (required).
        cards: JSON array of card objects. Each card: {"image_hash": str, "video_id": str, "title": str, "description": str, "link": str, "call_to_action_type": str}. image_hash or video_id required per card (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
    """
    if (err := require_editor("meta_create_carousel_ad_creative")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    try:
        card_list = json.loads(cards)
    except Exception:
        return json.dumps({"error": "cards must be a valid JSON array."})
    if not isinstance(card_list, list) or len(card_list) < 2:
        return json.dumps({"error": "cards must be a JSON array with at least 2 items."})
    attachments = []
    for card in card_list:
        att: dict = {"link": card.get("link", link)}
        if card.get("image_hash"):
            att["image_hash"] = card["image_hash"]
        if card.get("video_id"):
            att["video_id"] = card["video_id"]
        if card.get("title"):
            att["name"] = card["title"]
        if card.get("description"):
            att["description"] = card["description"]
        if card.get("call_to_action_type"):
            att["call_to_action"] = {"type": card["call_to_action_type"], "value": {"link": card.get("link", link)}}
        attachments.append(att)
    spec = {
        "page_id": page_id,
        "link_data": {
            "link": link,
            "message": message,
            "child_attachments": attachments,
            "multi_share_optimized": True,
        },
    }
    result = _post(f"{acct}/adcreatives", {
        "name": name,
        "object_story_spec": json.dumps(spec),
    })
    if "error" in result:
        return json.dumps(result)
    return json.dumps({"creative_id": result.get("id"), "result": result})


@mcp.tool()
def meta_create_story_ad_creative(
    name: str,
    page_id: str,
    body: str,
    link: str,
    account_id: str = "",
    instagram_account_id: str = "",
    image_hash: str = "",
    video_id: str = "",
    call_to_action_type: str = "LEARN_MORE",
) -> str:
    """Create a Story format (9:16 vertical) ad creative for Facebook/Instagram Stories.

    Args:
        name: Creative name (required).
        page_id: Facebook Page ID (required).
        body: Ad body copy (required).
        link: Destination URL (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        instagram_account_id: Instagram account ID if targeting Instagram Stories.
        image_hash: Image hash from meta_upload_image_to_meta. Use this OR video_id.
        video_id: Video ID from meta_upload_ad_video. Use this OR image_hash.
        call_to_action_type: CTA type e.g. LEARN_MORE, SHOP_NOW. Default LEARN_MORE.
    """
    if (err := require_editor("meta_create_story_ad_creative")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    if not image_hash and not video_id:
        return json.dumps({"error": "Provide either image_hash or video_id."})
    cta = {"type": call_to_action_type, "value": {"link": link}}
    if video_id:
        media_data: dict = {
            "video_id": video_id,
            "call_to_action": cta,
            "message": body,
            "link_description": link,
        }
        spec: dict = {"page_id": page_id, "video_data": media_data}
    else:
        media_data = {
            "image_hash": image_hash,
            "link": link,
            "message": body,
            "call_to_action": cta,
        }
        spec = {"page_id": page_id, "link_data": media_data}
    if instagram_account_id:
        spec["instagram_actor_id"] = instagram_account_id
    result = _post(f"{acct}/adcreatives", {
        "name": name,
        "object_story_spec": json.dumps(spec),
    })
    if "error" in result:
        return json.dumps(result)
    return json.dumps({"creative_id": result.get("id"), "result": result})


@mcp.tool()
def meta_create_collection_ad_creative(
    name: str,
    page_id: str,
    body: str,
    headline: str,
    product_set_id: str,
    link: str,
    account_id: str = "",
    hero_image_hash: str = "",
    hero_video_id: str = "",
) -> str:
    """Create a Collection ad creative (hero image/video + product catalog below).

    Args:
        name: Creative name (required).
        page_id: Facebook Page ID (required).
        body: Ad body copy (required).
        headline: Ad headline (required).
        product_set_id: Product set ID from your Meta catalog (required).
        link: Destination URL (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        hero_image_hash: Image hash for the hero. Use this OR hero_video_id.
        hero_video_id: Video ID for the hero. Use this OR hero_image_hash.
    """
    if (err := require_editor("meta_create_collection_ad_creative")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    if not hero_image_hash and not hero_video_id:
        return json.dumps({"error": "Provide either hero_image_hash or hero_video_id."})
    template_spec = {
        "url": link,
        "product_set_id": product_set_id,
    }
    if hero_video_id:
        spec: dict = {
            "page_id": page_id,
            "video_data": {
                "video_id": hero_video_id,
                "message": body,
                "title": headline,
                "call_to_action": {"type": "SHOP_NOW", "value": {"link": link}},
            },
        }
    else:
        spec = {
            "page_id": page_id,
            "link_data": {
                "image_hash": hero_image_hash,
                "link": link,
                "message": body,
                "name": headline,
                "call_to_action": {"type": "SHOP_NOW", "value": {"link": link}},
            },
        }
    result = _post(f"{acct}/adcreatives", {
        "name": name,
        "object_story_spec": json.dumps(spec),
        "template_url_spec": json.dumps(template_spec),
        "format": "COLLECTION",
    })
    if "error" in result:
        return json.dumps(result)
    return json.dumps({"creative_id": result.get("id"), "result": result})


# ---------------------------------------------------------------------------
# BLOCK 3: AUDIENCE MANAGEMENT (META-15 to META-20)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_upload_customer_list_to_audience(
    audience_id: str,
    users: str,
    operation: str = "ADD",
    account_id: str = "",
) -> str:
    """Upload hashed customer data (email/phone) to an existing Meta Custom Audience.

    Emails and phone numbers are automatically SHA-256 hashed before sending.

    Args:
        audience_id: Custom audience ID to upload to (required).
        users: JSON array of user objects. Each object may include: email, phone, first_name, last_name. Example: '[{"email":"user@example.com","phone":"+972501234567"}]' (required).
        operation: ADD to add users, REMOVE to remove them. Default ADD.
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
    """
    if (err := require_editor("meta_upload_customer_list_to_audience")): return err
    try:
        user_list = json.loads(users)
    except Exception:
        return json.dumps({"error": "users must be a valid JSON array."})

    schema: list = []
    data_rows: list = []

    for u in user_list:
        row: list = []
        if not schema:
            if u.get("email"):
                schema.append("EMAIL")
            if u.get("phone"):
                schema.append("PHONE")
            if u.get("first_name"):
                schema.append("FN")
            if u.get("last_name"):
                schema.append("LN")
        for field in schema:
            if field == "EMAIL":
                row.append(_sha256(u.get("email", "")))
            elif field == "PHONE":
                row.append(_sha256(u.get("phone", "")))
            elif field == "FN":
                row.append(_sha256(u.get("first_name", "")))
            elif field == "LN":
                row.append(_sha256(u.get("last_name", "")))
        if row:
            data_rows.append(row)

    if not schema or not data_rows:
        return json.dumps({"error": "No valid user data found. Provide email, phone, first_name, or last_name."})

    payload = {
        "schema": schema,
        "data": data_rows,
        "is_raw": False,
    }
    result = _post(f"{audience_id}/users", {
        "payload": json.dumps(payload),
        "operation_type": operation,
    })
    return json.dumps({
        "success": "error" not in result,
        "num_received": result.get("num_received"),
        "num_invalid_entries": result.get("num_invalid_entries"),
        "result": result,
    })


@mcp.tool()
def meta_delete_audience(
    audience_id: str,
) -> str:
    """Delete a Meta Custom Audience.

    Args:
        audience_id: Custom audience ID to delete (required).
    """
    if (err := require_editor("meta_delete_audience")): return err
    result = _delete(audience_id)
    return json.dumps({"success": result.get("success", False), "audience_id": audience_id, **result})


@mcp.tool()
def meta_get_audience(
    audience_id: str,
) -> str:
    """Get details and estimated size for a specific Meta Custom Audience.

    Args:
        audience_id: Custom audience ID (required).
    """
    return json.dumps(_get(f"{audience_id}", {
        "fields": "id,name,approximate_count_lower_bound,approximate_count_upper_bound,customer_file_source,subtype,description,data_source,operation_status,time_created,time_updated",
    }))


@mcp.tool()
def meta_update_audience(
    audience_id: str,
    name: str = "",
    description: str = "",
) -> str:
    """Update the name or description of a Meta Custom Audience.

    Args:
        audience_id: Custom audience ID to update (required).
        name: New audience name. Leave blank to keep unchanged.
        description: New audience description. Leave blank to keep unchanged.
    """
    if (err := require_editor("meta_update_audience")): return err
    data: dict = {}
    if name:
        data["name"] = name
    if description:
        data["description"] = description
    if not data:
        return json.dumps({"error": "Provide name or description to update."})
    result = _post(audience_id, data)
    return json.dumps({"success": "error" not in result, **result})


@mcp.tool()
def meta_create_website_audience(
    name: str,
    pixel_id: str,
    event: str,
    retention_days: int,
    account_id: str = "",
    url_contains: str = "",
    url_equals: str = "",
    description: str = "",
) -> str:
    """Create a website Custom Audience based on Meta Pixel events.

    Args:
        name: Audience name (required).
        pixel_id: Meta Pixel ID (required).
        event: Pixel event to target: PageView, ViewContent, AddToCart, Purchase, Lead, CompleteRegistration, etc. (required).
        retention_days: How many days to look back (1-180) (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        url_contains: Optional URL condition — include users who visited URLs containing this string.
        url_equals: Optional URL condition — include users who visited this exact URL.
        description: Audience description.
    """
    if (err := require_editor("meta_create_website_audience")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    rule_conditions: list = [{"event_sources": [{"id": pixel_id, "type": "pixel"}], "retention_seconds": retention_days * 86400, "filter": {"operator": "and", "filters": [{"field": "event", "operator": "=", "value": event}]}}]
    if url_contains:
        rule_conditions[0]["filter"]["filters"].append({"field": "url", "operator": "i_contains", "value": url_contains})
    if url_equals:
        rule_conditions[0]["filter"]["filters"].append({"field": "url", "operator": "=", "value": url_equals})
    data: dict = {
        "name": name,
        "subtype": "WEBSITE",
        "pixel_id": pixel_id,
        "rule": json.dumps({"inclusions": {"operator": "or", "rules": rule_conditions}}),
    }
    if description:
        data["description"] = description
    result = _post(f"{acct}/customaudiences", data)
    if "error" in result:
        return json.dumps(result)
    return json.dumps({"audience_id": result.get("id"), "result": result})


@mcp.tool()
def meta_create_engagement_audience(
    name: str,
    source_type: str,
    source_id: str,
    engagement_type: str,
    retention_days: int,
    account_id: str = "",
    description: str = "",
) -> str:
    """Create an audience of people who engaged with your Facebook Page, Instagram, or video content.

    Args:
        name: Audience name (required).
        source_type: PAGE, INSTAGRAM_ACCOUNT, VIDEO, LEAD_FORM, or EVENT (required).
        source_id: ID of the page, IG account, video, lead form, or event to target (required).
        engagement_type: PAGE_LIKED, PAGE_VISITED, PAGE_ENGAGED, IG_ACCOUNT_FOLLOWED, IG_MEDIA_INTERACTED, VIDEO_WATCHED_25, VIDEO_WATCHED_50, VIDEO_WATCHED_75, VIDEO_WATCHED_95 (required).
        retention_days: Lookback window in days (1-365) (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        description: Audience description.
    """
    if (err := require_editor("meta_create_engagement_audience")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    engagement_spec = {
        "object_id": source_id,
        "engagement_type": engagement_type,
        "retention_seconds": retention_days * 86400,
    }
    data: dict = {
        "name": name,
        "subtype": "ENGAGEMENT",
        "engagement_specs": json.dumps([engagement_spec]),
    }
    if description:
        data["description"] = description
    result = _post(f"{acct}/customaudiences", data)
    if "error" in result:
        return json.dumps(result)
    return json.dumps({"audience_id": result.get("id"), "result": result})


# ---------------------------------------------------------------------------
# BLOCK 4: PERFORMANCE REPORTING (META-21 to META-26)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_campaign_performance(
    campaign_id: str,
    start_date: str = "",
    end_date: str = "",
    breakdown: str = "",
) -> str:
    """Get detailed performance metrics for a specific Meta campaign.

    Args:
        campaign_id: Campaign ID (required).
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        breakdown: Optional breakdown: age, gender, placement, device, country, or publisher_platform.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    params: dict = {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "fields": "campaign_id,campaign_name,spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type,conversions,cost_per_conversion,action_values",
        "level": "campaign",
    }
    if breakdown:
        params["breakdowns"] = breakdown
    return json.dumps(_get(f"{campaign_id}/insights", params))


@mcp.tool()
def meta_get_adset_performance(
    adset_id: str,
    start_date: str = "",
    end_date: str = "",
    breakdown: str = "",
) -> str:
    """Get detailed performance metrics for a specific Meta ad set.

    Args:
        adset_id: Ad set ID (required).
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        breakdown: Optional breakdown: age, gender, placement, device, country, or publisher_platform.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    params: dict = {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "fields": "adset_id,adset_name,campaign_name,spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type,conversions,cost_per_conversion,action_values,targeting",
        "level": "adset",
    }
    if breakdown:
        params["breakdowns"] = breakdown
    return json.dumps(_get(f"{adset_id}/insights", params))


@mcp.tool()
def meta_get_ad_performance(
    ad_id: str,
    start_date: str = "",
    end_date: str = "",
    breakdown: str = "",
) -> str:
    """Get detailed performance metrics for a specific Meta ad.

    Args:
        ad_id: Ad ID (required).
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        breakdown: Optional breakdown: age, gender, placement, device, country, or publisher_platform.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    params: dict = {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "fields": "ad_id,ad_name,adset_name,campaign_name,spend,impressions,clicks,ctr,cpc,cpm,reach,frequency,actions,cost_per_action_type,conversions,cost_per_conversion,action_values",
        "level": "ad",
    }
    if breakdown:
        params["breakdowns"] = breakdown
    return json.dumps(_get(f"{ad_id}/insights", params))


@mcp.tool()
def meta_get_geo_performance(
    account_id: str = "",
    campaign_id: str = "",
    start_date: str = "",
    end_date: str = "",
    geo_level: str = "country",
) -> str:
    """Get Meta Ads performance broken down by geographic location (country, region, or DMA).

    Args:
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        campaign_id: Filter by campaign ID. Leave blank for account-level.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        geo_level: country, region, or dma. Default country.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    path = f"{campaign_id}/insights" if campaign_id else f"{acct}/insights"
    params: dict = {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "breakdowns": geo_level,
        "fields": f"{geo_level},impressions,clicks,spend,ctr,cpc,reach,actions,conversions",
        "limit": 200,
    }
    return json.dumps(_get(path, params))


@mcp.tool()
def meta_get_device_performance(
    account_id: str = "",
    campaign_id: str = "",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Get Meta Ads performance broken down by device platform (mobile, desktop, tablet).

    Args:
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        campaign_id: Filter by campaign ID. Leave blank for account-level.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    path = f"{campaign_id}/insights" if campaign_id else f"{acct}/insights"
    return json.dumps(_get(path, {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "breakdowns": "device_platform",
        "fields": "device_platform,impressions,clicks,spend,ctr,cpc,reach,actions,conversions",
        "limit": 50,
    }))


@mcp.tool()
def meta_get_attribution_report(
    account_id: str = "",
    campaign_id: str = "",
    start_date: str = "",
    end_date: str = "",
    attribution_windows: str = "1d_click,7d_click,1d_view",
) -> str:
    """Get Meta Ads conversion attribution broken down by click and view attribution windows.

    Args:
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        campaign_id: Filter by campaign ID. Leave blank for account-level.
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        attribution_windows: Comma-separated list of windows: 1d_click, 7d_click, 28d_click, 1d_view, 7d_view. Default: 1d_click,7d_click,1d_view.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    path = f"{campaign_id}/insights" if campaign_id else f"{acct}/insights"
    windows = [w.strip() for w in attribution_windows.split(",") if w.strip()]
    return json.dumps(_get(path, {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "action_attribution_windows": json.dumps(windows),
        "fields": "campaign_name,adset_name,spend,impressions,actions,cost_per_action_type,action_values",
        "level": "campaign" if campaign_id else "account",
        "limit": 100,
    }))


# ---------------------------------------------------------------------------
# BLOCK 5: PIXEL & CONVERSIONS (META-27 to META-30)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_create_pixel(
    name: str,
    account_id: str = "",
) -> str:
    """Create a new Meta Pixel for an ad account.

    Args:
        name: Pixel name (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
    """
    if (err := require_editor("meta_create_pixel")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    result = _post(f"{acct}/adspixels", {"name": name})
    if "error" in result:
        return json.dumps(result)
    return json.dumps({"pixel_id": result.get("id"), "result": result})


@mcp.tool()
def meta_get_pixel_events(
    pixel_id: str,
    start_date: str = "",
    end_date: str = "",
) -> str:
    """List all events fired on a Meta Pixel with their counts and last fired time.

    Args:
        pixel_id: Meta Pixel ID (required).
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    return json.dumps(_get(f"{pixel_id}/stats", {
        "start_time": sd,
        "end_time": ed,
        "aggregation": "event",
    }))


@mcp.tool()
def meta_send_server_event(
    pixel_id: str,
    event_name: str,
    event_time: str,
    user_data: str,
    pixel_access_token: str,
    event_source_url: str = "",
    custom_data: str = "",
    test_event_code: str = "",
    event_id: str = "",
    action_source: str = "website",
) -> str:
    """Send a server-side conversion event to Meta via the Conversions API (CAPI).

    Emails, phone numbers, first name, and last name in user_data are automatically SHA-256 hashed.
    Use event_id to deduplicate with browser pixel events (same event_id on both sides).

    Args:
        pixel_id: Meta Pixel ID (required).
        event_name: Event name: Purchase, Lead, ViewContent, AddToCart, CompleteRegistration, etc. (required).
        event_time: Unix timestamp of the event as a string, e.g. '1735689600' (required).
        user_data: JSON object with user info. Fields: email, phone, first_name, last_name, country, city, zip, client_ip_address, client_user_agent, fbc, fbp (required).
        pixel_access_token: Pixel-specific access token (different from ad account token) (required).
        event_source_url: The URL where the event occurred (for website events).
        custom_data: Optional JSON object with: value, currency, order_id, content_name, content_type, contents (array of {id, quantity}).
        test_event_code: Optional test event code from Meta Events Manager to verify the event.
        event_id: Unique ID for deduplication with browser pixel. Must match the event_id sent by the browser pixel for the same event.
        action_source: Where the event happened. One of: website, app, email, phone_call, chat, physical_store, system_generated, other. Default: website.
    """
    if (err := require_editor("meta_send_server_event")): return err
    try:
        ud = json.loads(user_data)
    except Exception:
        return json.dumps({"error": "user_data must be a valid JSON object."})

    # Hash PII fields
    hashed_ud: dict = {}
    if ud.get("email"):
        hashed_ud["em"] = [_sha256(ud["email"])]
    if ud.get("phone"):
        hashed_ud["ph"] = [_sha256(ud["phone"])]
    if ud.get("first_name"):
        hashed_ud["fn"] = [_sha256(ud["first_name"])]
    if ud.get("last_name"):
        hashed_ud["ln"] = [_sha256(ud["last_name"])]
    if ud.get("country"):
        hashed_ud["country"] = [_sha256(ud["country"])]
    if ud.get("city"):
        hashed_ud["ct"] = [_sha256(ud["city"])]
    if ud.get("zip"):
        hashed_ud["zp"] = [_sha256(ud["zip"])]
    for passthrough in ("client_ip_address", "client_user_agent", "fbc", "fbp"):
        if ud.get(passthrough):
            hashed_ud[passthrough] = ud[passthrough]

    event: dict = {
        "event_name": event_name,
        "event_time": int(event_time),
        "action_source": action_source or "website",
        "user_data": hashed_ud,
    }
    if event_source_url:
        event["event_source_url"] = event_source_url
    if event_id:
        event["event_id"] = event_id

    if custom_data:
        try:
            event["custom_data"] = json.loads(custom_data)
        except Exception:
            pass

    payload: dict = {"data": json.dumps([event])}
    if test_event_code:
        payload["test_event_code"] = test_event_code

    # CAPI uses the pixel access token, not the user token
    resp = requests.post(
        f"{GRAPH_BASE}/{pixel_id}/events",
        params={"access_token": pixel_access_token},
        data=payload,
        timeout=30,
    )
    if not resp.ok:
        return json.dumps({"error": resp.json().get("error", resp.text[:300])})
    result = resp.json()
    return json.dumps({
        "events_received": result.get("events_received"),
        "messages": result.get("messages", []),
        "fbtrace_id": result.get("fbtrace_id"),
        "raw": result,
    })


@mcp.tool()
def meta_create_custom_conversion(
    name: str,
    pixel_id: str,
    rule: str,
    custom_event_type: str,
    account_id: str = "",
    description: str = "",
) -> str:
    """Create a custom conversion from a pixel event with URL conditions.

    Args:
        name: Custom conversion name (required).
        pixel_id: Meta Pixel ID (required).
        rule: JSON rule for the conversion condition. Example: '{"and":[{"url":{"i_contains":"thank-you"}}]}' (required).
        custom_event_type: PURCHASE, LEAD, COMPLETE_REGISTRATION, ADD_TO_CART, VIEW_CONTENT, SEARCH, ADD_TO_WISHLIST, ADD_PAYMENT_INFO, INITIATED_CHECKOUT, CONTACT, FIND_LOCATION, SCHEDULE, START_TRIAL, SUBMIT_APPLICATION, SUBSCRIBE, OTHER (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        description: Optional description.
    """
    if (err := require_editor("meta_create_custom_conversion")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    data: dict = {
        "name": name,
        "pixel_id": pixel_id,
        "rule": rule,
        "custom_event_type": custom_event_type,
        "event_source_type": "PIXEL",
    }
    if description:
        data["description"] = description
    result = _post(f"{acct}/customconversions", data)
    if "error" in result:
        return json.dumps(result)
    return json.dumps({"custom_conversion_id": result.get("id"), "result": result})


# ---------------------------------------------------------------------------
# BLOCK 6: INSTAGRAM COMPLETE (META-31 to META-40)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_instagram_stories(
    instagram_account_id: str,
    page_access_token: str = "",
) -> str:
    """Get active Instagram stories for a business account.

    Args:
        instagram_account_id: Instagram Business Account ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{instagram_account_id}/stories", tok, {
        "fields": "id,media_type,media_url,thumbnail_url,timestamp,permalink",
    }))


@mcp.tool()
def meta_create_instagram_reel(
    instagram_account_id: str,
    video_url: str,
    page_access_token: str,
    caption: str = "",
    share_to_feed: bool = True,
) -> str:
    """Create and publish an Instagram Reel.

    This is a 2-step process: creates a media container, then publishes it.

    Args:
        instagram_account_id: Instagram Business Account ID (required).
        video_url: Public URL of the video file (required).
        page_access_token: Page access token with instagram_content_publish permission (required).
        caption: Reel caption text. Optional.
        share_to_feed: Also show the Reel in the feed. Default True.
    """
    if (err := require_editor("meta_create_instagram_reel")): return err
    tok = _page_tok(page_access_token)
    container_data: dict = {
        "media_type": "REELS",
        "video_url": video_url,
        "share_to_feed": "true" if share_to_feed else "false",
    }
    if caption:
        container_data["caption"] = caption
    container = _post_with_token(f"{instagram_account_id}/media", tok, container_data)
    if "error" in container:
        return json.dumps(container)
    container_id = container.get("id")
    if not container_id:
        return json.dumps({"error": "Failed to create Reel container", "response": container})
    publish = _post_with_token(f"{instagram_account_id}/media_publish", tok, {"creation_id": container_id})
    if "error" in publish:
        return json.dumps({"error": "Container created but publish failed", "container_id": container_id, "publish_error": publish})
    reel_id = publish.get("id")
    permalink_result = _get_with_token(f"{reel_id}", tok, {"fields": "permalink"}) if reel_id else {}
    return json.dumps({
        "reel_id": reel_id,
        "container_id": container_id,
        "permalink": permalink_result.get("permalink"),
        "publish_result": publish,
    })


@mcp.tool()
def meta_get_instagram_post_insights(
    media_id: str,
    page_access_token: str = "",
) -> str:
    """Get insights for a specific Instagram post or reel.

    Args:
        media_id: Instagram media ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{media_id}/insights", tok, {
        "metric": "impressions,reach,engagement,saved,video_views,plays,likes,comments,shares,total_interactions",
    }))


@mcp.tool()
def meta_schedule_instagram_post(
    instagram_account_id: str,
    image_url: str,
    publish_at: str,
    page_access_token: str,
    caption: str = "",
) -> str:
    """Schedule an Instagram post for future publishing.

    Args:
        instagram_account_id: Instagram Business Account ID (required).
        image_url: Public URL of the image (JPEG/PNG) to post (required).
        publish_at: Scheduled publish time in ISO 8601, e.g. '2025-06-15T10:00:00+0300' (required).
        page_access_token: Page access token with instagram_content_publish permission (required).
        caption: Post caption text. Optional.
    """
    if (err := require_editor("meta_schedule_instagram_post")): return err
    import datetime as _dt
    tok = _page_tok(page_access_token)
    try:
        dt = _dt.datetime.fromisoformat(publish_at.replace("Z", "+00:00"))
        scheduled_ts = int(dt.timestamp())
    except Exception:
        return json.dumps({"error": f"Invalid publish_at format: {publish_at}. Use ISO 8601."})
    container_data: dict = {
        "image_url": image_url,
        "published": "false",
        "scheduled_publish_time": scheduled_ts,
    }
    if caption:
        container_data["caption"] = caption
    result = _post_with_token(f"{instagram_account_id}/media", tok, container_data)
    if "error" in result:
        return json.dumps(result)
    return json.dumps({
        "container_id": result.get("id"),
        "scheduled_time": publish_at,
        "scheduled_timestamp": scheduled_ts,
        "result": result,
    })


@mcp.tool()
def meta_delete_instagram_post(
    media_id: str,
    page_access_token: str,
) -> str:
    """Delete an Instagram post.

    Args:
        media_id: Instagram media ID to delete (required).
        page_access_token: Page access token (required).
    """
    if (err := require_editor("meta_delete_instagram_post")): return err
    tok = _page_tok(page_access_token)
    result = _delete_with_token(media_id, tok)
    return json.dumps({"success": result.get("success", False), "media_id": media_id, **result})


@mcp.tool()
def meta_get_instagram_mentions(
    instagram_account_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get posts and comments where your Instagram account is mentioned or tagged.

    Args:
        instagram_account_id: Instagram Business Account ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of mentions to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{instagram_account_id}/tags", tok, {
        "fields": "id,caption,media_type,permalink,timestamp,username",
        "limit": limit,
    }))


@mcp.tool()
def meta_reply_to_instagram_comment(
    comment_id: str,
    message: str,
    page_access_token: str,
) -> str:
    """Reply to a comment on an Instagram post.

    Args:
        comment_id: Instagram comment ID to reply to (required).
        message: Reply text (required).
        page_access_token: Page access token (required).
    """
    if (err := require_editor("meta_reply_to_instagram_comment")): return err
    tok = _page_tok(page_access_token)
    result = _post_with_token(f"{comment_id}/replies", tok, {"message": message})
    if "error" in result:
        return json.dumps(result)
    return json.dumps({"reply_id": result.get("id"), "result": result})


@mcp.tool()
def meta_delete_instagram_comment(
    comment_id: str,
    page_access_token: str,
) -> str:
    """Delete a comment on an Instagram post.

    Args:
        comment_id: Instagram comment ID to delete (required).
        page_access_token: Page access token (required).
    """
    if (err := require_editor("meta_delete_instagram_comment")): return err
    tok = _page_tok(page_access_token)
    result = _delete_with_token(comment_id, tok)
    return json.dumps({"success": result.get("success", False), "comment_id": comment_id, **result})


@mcp.tool()
def meta_get_instagram_hashtag_posts(
    hashtag: str,
    instagram_account_id: str,
    page_access_token: str = "",
    limit: int = 10,
) -> str:
    """Search for recent public posts using a specific Instagram hashtag (Business Discovery API).

    Args:
        hashtag: Hashtag to search without the # symbol, e.g. 'digitalmarketing' (required).
        instagram_account_id: Your Instagram Business Account ID (required for auth) (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of posts to return. Default 10, max 50.
    """
    tok = _page_tok(page_access_token)
    # Step 1: get hashtag ID
    hashtag_result = _get_with_token("ig_hashtag_search", tok, {
        "user_id": instagram_account_id,
        "q": hashtag,
    })
    if "error" in hashtag_result:
        return json.dumps(hashtag_result)
    hashtag_data = hashtag_result.get("data", [])
    if not hashtag_data:
        return json.dumps({"error": f"Hashtag '{hashtag}' not found."})
    hashtag_id = hashtag_data[0].get("id")
    # Step 2: get recent media
    media = _get_with_token(f"{hashtag_id}/recent_media", tok, {
        "user_id": instagram_account_id,
        "fields": "id,media_type,media_url,permalink,timestamp,like_count,comments_count",
        "limit": min(limit, 50),
    })
    return json.dumps({"hashtag": hashtag, "hashtag_id": hashtag_id, "posts": media})


@mcp.tool()
def meta_get_instagram_competitor_analysis(
    target_username: str,
    instagram_account_id: str,
    page_access_token: str = "",
) -> str:
    """Get public profile info from a competitor's Instagram account using the Business Discovery API.

    Args:
        target_username: Competitor's Instagram username without the @ symbol (required).
        instagram_account_id: Your Instagram Business Account ID (required for auth) (required).
        page_access_token: Page access token. Falls back to user token if blank.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{instagram_account_id}", tok, {
        "fields": f"business_discovery.fields(username,name,biography,followers_count,follows_count,media_count,website,profile_picture_url,is_verified)&username={target_username}",
    }))


# ---------------------------------------------------------------------------
# BLOCK 7: FACEBOOK PAGE — MISSING TOOLS (META-41 to META-46)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_page_photo_album(
    album_id: str,
    page_access_token: str = "",
    limit: int = 50,
) -> str:
    """Get all photos in a specific Facebook Page photo album.

    Args:
        album_id: Album ID from meta_get_page_albums (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of photos to return. Default 50.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{album_id}/photos", tok, {
        "fields": "id,picture,source,name,created_time,likes.summary(true),comments.summary(true)",
        "limit": limit,
    }))


@mcp.tool()
def meta_create_page_photo_album(
    page_id: str,
    name: str,
    page_access_token: str,
    description: str = "",
    location: str = "",
) -> str:
    """Create a new photo album on a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        name: Album name (required).
        page_access_token: Page access token with pages_manage_posts permission (required).
        description: Album description. Optional.
        location: Album location. Optional.
    """
    if (err := require_editor("meta_create_page_photo_album")): return err
    tok = _page_tok(page_access_token)
    data: dict = {"name": name}
    if description:
        data["message"] = description
    if location:
        data["location"] = location
    result = _post_with_token(f"{page_id}/albums", tok, data)
    if "error" in result:
        return json.dumps(result)
    return json.dumps({"album_id": result.get("id"), "result": result})


@mcp.tool()
def meta_get_page_stories(
    page_id: str,
    page_access_token: str,
) -> str:
    """Get current stories posted by a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token (required).
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/stories", tok, {
        "fields": "id,message,story,created_time,permalink_url",
        "limit": 25,
    }))


@mcp.tool()
def meta_pin_post(
    page_id: str,
    post_id: str,
    page_access_token: str,
) -> str:
    """Pin a post to the top of a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        post_id: Post ID to pin (required).
        page_access_token: Page access token with admin rights (required).
    """
    if (err := require_editor("meta_pin_post")): return err
    tok = _page_tok(page_access_token)
    result = _post_with_token(page_id, tok, {"pinned_post": post_id})
    return json.dumps({"success": "error" not in result, "post_id": post_id, "page_id": page_id, **result})


@mcp.tool()
def meta_get_page_tags(
    page_id: str,
    page_access_token: str = "",
) -> str:
    """Get tags/labels applied to a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/labels", tok, {
        "fields": "id,name",
        "limit": 50,
    }))


@mcp.tool()
def meta_get_page_liked_pages(
    page_id: str,
    page_access_token: str = "",
    limit: int = 25,
) -> str:
    """Get pages liked by or associated with a Facebook Page.

    Args:
        page_id: Facebook Page ID (required).
        page_access_token: Page access token. Falls back to user token if blank.
        limit: Number of pages to return. Default 25.
    """
    tok = _page_tok(page_access_token)
    return json.dumps(_get_with_token(f"{page_id}/likes", tok, {
        "fields": "id,name,category,fan_count,website",
        "limit": limit,
    }))


# ---------------------------------------------------------------------------
# BLOCK 8: CATALOG & COMMERCE (META-47 to META-50)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_list_catalog_products(
    catalog_id: str,
    limit: int = 50,
    filter_status: str = "",
) -> str:
    """List all products in a Meta product catalog.

    Args:
        catalog_id: Meta catalog ID (required).
        limit: Number of products to return. Default 50, max 200.
        filter_status: Filter by availability: 'in stock', 'out of stock', 'preorder', 'available for order'. Leave blank for all.
    """
    params: dict = {
        "fields": "id,name,price,currency,availability,image_url,product_url,brand,description,retailer_id,condition,category",
        "limit": min(limit, 200),
    }
    if filter_status:
        params["filter"] = json.dumps({"availability": {"is_any": [filter_status]}})
    return json.dumps(_get(f"{catalog_id}/products", params))


@mcp.tool()
def meta_get_catalog_diagnostics(
    catalog_id: str,
) -> str:
    """Get product feed errors and warnings for a Meta catalog.

    Args:
        catalog_id: Meta catalog ID (required).
    """
    return json.dumps(_get(f"{catalog_id}/diagnostics", {
        "fields": "affected_items_count,affected_features,severity,type,description,samples",
        "limit": 50,
    }))


@mcp.tool()
def meta_create_product_set(
    catalog_id: str,
    name: str,
    filter: str,
) -> str:
    """Create a product set (subset of catalog for targeting in Collection/DPA ads).

    Args:
        catalog_id: Meta catalog ID (required).
        name: Product set name (required).
        filter: JSON filter expression. Examples: '{"retailer_id":{"is_any":["SKU1","SKU2"]}}' or '{"price":{"gt":10000}}' (prices in cents) or '{"category":{"is_any":["Shoes"]}}' (required).
    """
    if (err := require_editor("meta_create_product_set")): return err
    result = _post(f"{catalog_id}/product_sets", {
        "name": name,
        "filter": filter,
    })
    if "error" in result:
        return json.dumps(result)
    return json.dumps({"product_set_id": result.get("id"), "result": result})


@mcp.tool()
def meta_get_product_sets(
    catalog_id: str,
) -> str:
    """List all product sets in a Meta catalog.

    Args:
        catalog_id: Meta catalog ID (required).
    """
    return json.dumps(_get(f"{catalog_id}/product_sets", {
        "fields": "id,name,filter,product_count",
        "limit": 100,
    }))


# ---------------------------------------------------------------------------
# BLOCK 9: UTILITY TOOLS (META-51 to META-57)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_ad_account_users(
    account_id: str = "",
) -> str:
    """List all users with access to a Meta ad account, including their roles and permission levels.

    Args:
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/userpermissions", {
        "fields": "user,role,permission_level",
        "limit": 100,
    }))


@mcp.tool()
def meta_add_ad_account_user(
    user_id: str,
    role: str,
    account_id: str = "",
) -> str:
    """Add a user to a Meta ad account with a specific role.

    Args:
        user_id: Facebook User ID to add (required).
        role: User role: ADMIN (1001), GENERAL_USER (1002), or REPORTS_ONLY (1003) (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
    """
    if (err := require_editor("meta_add_ad_account_user")): return err
    role_map = {"ADMIN": 1001, "GENERAL_USER": 1002, "REPORTS_ONLY": 1003}
    if role not in role_map:
        return json.dumps({"error": f"role must be one of: {', '.join(role_map.keys())}"})
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    result = _post(f"{acct}/userpermissions", {
        "user": user_id,
        "role": role_map[role],
    })
    return json.dumps({"success": "error" not in result, **result})


@mcp.tool()
def meta_remove_ad_account_user(
    user_id: str,
    account_id: str = "",
) -> str:
    """Remove a user's access from a Meta ad account.

    Args:
        user_id: Facebook User ID to remove (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
    """
    if (err := require_editor("meta_remove_ad_account_user")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    resp = requests.delete(
        f"{GRAPH_BASE}/{acct}/userpermissions",
        params={"access_token": _token(), "user": user_id},
        timeout=30,
    )
    if not resp.ok:
        return json.dumps({"error": resp.json().get("error", resp.text[:300])})
    result = resp.json()
    return json.dumps({"success": result.get("success", False), "user_id": user_id, **result})


@mcp.tool()
def meta_get_business_pages(
    business_id: str,
) -> str:
    """Get all Facebook Pages connected to a Meta Business account.

    Args:
        business_id: Meta Business Manager account ID (required).
    """
    return json.dumps(_get(f"{business_id}/owned_pages", {
        "fields": "id,name,category,fan_count,access_token,link,verification_status",
        "limit": 100,
    }))


@mcp.tool()
def meta_get_targeting_suggestions(
    interests: str,
    limit: int = 25,
) -> str:
    """Get interest targeting suggestions based on seed interests using Meta's audience targeting search.

    Args:
        interests: Comma-separated list of interest names to use as seeds, e.g. 'digital marketing,SEO,content marketing' (required).
        limit: Number of suggestions to return. Default 25.
    """
    interest_list = [i.strip() for i in interests.split(",") if i.strip()]
    return json.dumps(_get("search", {
        "type": "adinterestsuggestion",
        "interest_list": json.dumps(interest_list),
        "limit": limit,
    }))


@mcp.tool()
def meta_copy_ad_to_adset(
    ad_id: str,
    target_adset_id: str,
    new_name: str = "",
    status: str = "PAUSED",
) -> str:
    """Copy an existing Meta ad to a different ad set, reusing the same creative.

    Args:
        ad_id: Source ad ID to copy (required).
        target_adset_id: Destination ad set ID (required).
        new_name: Name for the copied ad. Leave blank to auto-generate.
        status: Status for the new ad: ACTIVE or PAUSED. Default PAUSED.
    """
    if (err := require_editor("meta_copy_ad_to_adset")): return err
    params: dict = {"adset_id": target_adset_id, "status_option": "PAUSED" if status == "PAUSED" else "ACTIVE"}
    if new_name:
        params["rename_options"] = json.dumps({"rename_strategy": "EXACT_COPY", "overwrite_with_name": new_name})
    result = _post(f"{ad_id}/copies", params)
    if "error" in result:
        return json.dumps(result)
    copied_ids = result.get("copied_ad_id") or (result.get("copies", [{}])[0].get("ad_id") if result.get("copies") else None)
    return json.dumps({"new_ad_id": copied_ids, "target_adset_id": target_adset_id, "result": result})


@mcp.tool()
def meta_preview_ad_creative(
    creative_id: str,
    ad_format: str = "MOBILE_FEED_STANDARD",
) -> str:
    """Get a preview of an ad creative in a specific ad format.

    Args:
        creative_id: Creative ID to preview (required).
        ad_format: Ad format for the preview. Options: DESKTOP_FEED_STANDARD, MOBILE_FEED_STANDARD, INSTAGRAM_STANDARD, INSTAGRAM_STORY, INSTAGRAM_REELS, AUDIENCE_NETWORK_INSTREAM_VIDEO, FACEBOOK_STORY, RIGHT_COLUMN_STANDARD. Default MOBILE_FEED_STANDARD.
    """
    return json.dumps(_get(f"{creative_id}/previews", {
        "ad_format": ad_format,
    }))


# ---------------------------------------------------------------------------
# WORKFLOW TOOLS
# ---------------------------------------------------------------------------


@mcp.tool()
def meta_bulk_updater(
    object_ids: str,
    object_type: str,
    action: str,
    params: str = "",
    delay_ms: int = 300,
    dry_run: bool = False,
) -> str:
    """Bulk-apply an action to multiple Meta campaigns, ad sets, or ads with rate-limit protection.

    Supports status changes, budget updates, targeting updates, and duplication.
    Set dry_run=True first to preview operations before executing — use this for
    confirmation: call with dry_run=True, show the plan to the user, then call again
    with dry_run=False only after confirmation.

    Args:
        object_ids: Comma-separated list of IDs, e.g. "123,456,789" (required).
        object_type: campaign, adset, or ad (required).
        action: One of: pause, activate, update_budget, update_targeting, duplicate (required).
        params: JSON string with action-specific overrides.
                  pause/activate    → not needed
                  update_budget     → {"daily_budget": "5000"} or {"lifetime_budget": "50000"}
                  update_targeting  → any field accepted by meta_update_adset_targeting,
                                      e.g. '{"targeting":"{...}","advantage_audience":true}'
                  duplicate         → {"status_override":"PAUSED","deep_copy":true}
        delay_ms: Milliseconds between API calls. Default 300.
        dry_run: If True, return the list of planned operations WITHOUT executing them.
                 Always call with dry_run=True first, confirm with user, then run for real.
    """
    if (err := require_editor("meta_bulk_updater")): return err
    valid_types = {"campaign", "adset", "ad"}
    valid_actions = {"pause", "activate", "update_budget", "update_targeting", "duplicate"}
    if object_type not in valid_types:
        return json.dumps({"error": f"object_type must be one of: {valid_types}"})
    if action not in valid_actions:
        return json.dumps({"error": f"action must be one of: {valid_actions}"})

    ids = [i.strip() for i in object_ids.split(",") if i.strip()]
    if not ids:
        return json.dumps({"error": "No valid IDs provided in object_ids."})

    extra: dict = {}
    if params:
        try:
            extra = json.loads(params)
        except Exception:
            return json.dumps({"error": f"params is not valid JSON: {params}"})

    # ── DRY RUN — preview only ──────────────────────────────────────────────
    if dry_run:
        plan = []
        for obj_id in ids:
            if action == "pause":
                plan.append({"id": obj_id, "operation": f"POST /{obj_id} status=PAUSED"})
            elif action == "activate":
                plan.append({"id": obj_id, "operation": f"POST /{obj_id} status=ACTIVE"})
            elif action == "update_budget":
                plan.append({"id": obj_id, "operation": f"POST /{obj_id} {extra}"})
            elif action == "update_targeting":
                plan.append({"id": obj_id, "operation": f"POST /{obj_id} targeting update"})
            elif action == "duplicate":
                plan.append({"id": obj_id, "operation": f"POST /{obj_id}/copies deep_copy={extra.get('deep_copy', True)}"})
        return json.dumps({
            "dry_run": True,
            "total": len(ids),
            "action": action,
            "object_type": object_type,
            "planned_operations": plan,
            "message": "DRY RUN — no changes made. Confirm with user then call again with dry_run=False.",
        })

    # ── Helper: POST with exponential backoff on 429 ─────────────────────────
    def _post_backoff(path: str, data: dict, max_retries: int = 4) -> dict:
        backoff = 1.0
        for attempt in range(max_retries):
            result = _post(path, data)
            err = result.get("error", {})
            code = err.get("code") if isinstance(err, dict) else None
            if code == 17 or code == 32 or code == 4:  # rate limit codes
                time.sleep(backoff)
                backoff *= 2
                continue
            return result
        return result  # return last result after retries

    delay_s = delay_ms / 1000.0
    succeeded = []
    failed = []

    for obj_id in ids:
        try:
            if action == "pause":
                result = _post_backoff(obj_id, {"status": "PAUSED"})
            elif action == "activate":
                result = _post_backoff(obj_id, {"status": "ACTIVE"})
            elif action == "update_budget":
                if not extra:
                    failed.append({"id": obj_id, "reason": "params required for update_budget"})
                    continue
                result = _post_backoff(obj_id, extra)
            elif action == "update_targeting":
                if not extra:
                    failed.append({"id": obj_id, "reason": "params required for update_targeting"})
                    continue
                result = _post_backoff(obj_id, extra)
            elif action == "duplicate":
                dup_data: dict = {
                    "deep_copy": str(extra.get("deep_copy", True)).lower(),
                    "status_override": extra.get("status_override", "PAUSED"),
                }
                result = _post_backoff(f"{obj_id}/copies", dup_data)
            else:
                failed.append({"id": obj_id, "reason": f"Unknown action: {action}"})
                continue

            if "error" in result:
                err = result["error"]
                reason = err.get("message", str(err)) if isinstance(err, dict) else str(err)
                failed.append({"id": obj_id, "reason": reason})
            else:
                succeeded.append({"id": obj_id, "result": result})
        except Exception as exc:
            failed.append({"id": obj_id, "reason": str(exc)})

        if delay_s > 0:
            time.sleep(delay_s)

    return json.dumps({
        "total": len(ids),
        "succeeded": len(succeeded),
        "failed": len(failed),
        "action": action,
        "object_type": object_type,
        "succeeded_details": succeeded,
        "failed_details": failed,
        "summary": f"{len(succeeded)}/{len(ids)} succeeded",
    })


@mcp.tool()
def meta_image_pipeline(
    image_url: str,
    account_id: str = "",
) -> str:
    """Upload an image from a public URL to the Meta image library and return image_hash.

    Downloads the image bytes and uploads as multipart/form-data (works even without
    Advanced API Access). Use this as step 2 of the image pipeline:
      Step 1: Generate image externally (Gemini / Nano Banana / any tool) → get public URL
      Step 2: Call this tool with the URL → get image_hash ready for ad creatives

    Args:
        image_url: Public URL to the JPG/PNG/GIF image to upload (required).
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
    """
    if (err := require_editor("meta_image_pipeline")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    if not acct:
        return json.dumps({"success": False, "error": "account_id is required."})

    # Step 1: download image bytes
    try:
        dl = requests.get(image_url, timeout=30, headers={"User-Agent": "Mozilla/5.0"})
        dl.raise_for_status()
        img_bytes = dl.content
        content_type = dl.headers.get("Content-Type", "image/jpeg").split(";")[0].strip()
    except Exception as e:
        return json.dumps({"success": False, "error": f"Failed to download image: {e}"})

    ext_map = {"image/jpeg": "jpg", "image/jpg": "jpg", "image/png": "png",
               "image/gif": "gif", "image/webp": "webp"}
    ext = ext_map.get(content_type, "jpg")
    filename = f"upload.{ext}"

    # Step 2: upload as multipart to Meta adimages
    try:
        resp = requests.post(
            f"{GRAPH_BASE}/{acct}/adimages",
            data={"access_token": _token()},
            files={"filename": (filename, img_bytes, content_type)},
            timeout=60,
        )
        result = resp.json()
    except Exception as e:
        return json.dumps({"success": False, "error": f"Upload failed: {e}"})

    if "error" in result:
        # Fallback: try base64 bytes param
        try:
            b64 = base64.b64encode(img_bytes).decode()
            result2 = _post(f"{acct}/adimages", {"bytes": b64})
            if "images" in result2:
                result = result2
            else:
                return json.dumps({"success": False, "error": result["error"], "fallback_error": result2.get("error")})
        except Exception as e2:
            return json.dumps({"success": False, "error": result["error"], "fallback_error": str(e2)})

    images = result.get("images", {})
    if not images:
        return json.dumps({"success": False, "error": "No image data returned.", "raw": result})

    first_image = next(iter(images.values()))
    image_hash = first_image.get("hash", "")
    image_meta_url = first_image.get("url", "")

    return json.dumps({
        "success": True,
        "image_hash": image_hash,
        "meta_url": image_meta_url,
        "source_url": image_url,
        "account_id": acct,
        "usage": f"Use image_hash='{image_hash}' in meta_create_ad_creative or meta_create_carousel_ad_creative",
    })


@mcp.tool()
def meta_adset_matrix_builder(
    campaign_id: str,
    creative_id: str,
    adsets: str,
    daily_budget_per_adset: int = 5000,
    optimization_goal: str = "OFFSITE_CONVERSIONS",
    billing_event: str = "IMPRESSIONS",
    ad_name_prefix: str = "",
    account_id: str = "",
    dry_run: bool = False,
) -> str:
    """Create one ad set + one ad per entry in a structured adsets list (matrix / multi-audience structure).

    Every adset and ad is created as PAUSED. All ads share the same creative.
    Set dry_run=True first to preview the full plan before creating anything.

    Args:
        campaign_id: Campaign ID to create ad sets under (required).
        creative_id: Ad creative ID to use for all ads (required).
        adsets: JSON array of ad set definitions (required). Each entry:
                  name           – ad set name (required)
                  targeting      – full targeting JSON string (required)
                  daily_budget   – optional per-adset budget override (agoras/cents)
                  frequency_cap  – optional int: max impressions per user per 7 days
                Example:
                '[{"name":"Ages 25-34 IL","targeting":"{\"age_min\":25,\"age_max\":34,\"geo_locations\":{\"countries\":[\"IL\"]}}","daily_budget":8000},
                  {"name":"Retargeting Website","targeting":"{\"custom_audiences\":[{\"id\":\"123\"}]}","frequency_cap":3}]'
        daily_budget_per_adset: Default daily budget in smallest currency unit (e.g. 5000 = ₪50). Per-adset daily_budget overrides this.
        optimization_goal: Optimization goal. Default OFFSITE_CONVERSIONS.
        billing_event: Billing event. Default IMPRESSIONS.
        ad_name_prefix: Prefix for ad names. Default 'Ad'.
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        dry_run: If True, return the full creation plan WITHOUT executing. Use for confirmation.
    """
    if (err := require_editor("meta_adset_matrix_builder")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    if not acct:
        return json.dumps({"error": "account_id is required."})

    try:
        adset_list: list = json.loads(adsets)
    except Exception as exc:
        return json.dumps({"error": f"adsets is not valid JSON: {exc}"})

    if not isinstance(adset_list, list) or not adset_list:
        return json.dumps({"error": "adsets must be a non-empty JSON array."})

    prefix_ad = ad_name_prefix or "Ad"

    # ── DRY RUN ──────────────────────────────────────────────────────────────
    if dry_run:
        plan = []
        for i, entry in enumerate(adset_list, 1):
            budget = entry.get("daily_budget", daily_budget_per_adset)
            freq = entry.get("frequency_cap")
            plan.append({
                "index": i,
                "adset_name": entry.get("name", f"AdSet {i}"),
                "ad_name": f"{prefix_ad} — {entry.get('name', i)} ({i}/{len(adset_list)})",
                "daily_budget": budget,
                "optimization_goal": optimization_goal,
                "frequency_cap_7d": freq,
                "targeting_preview": (entry.get("targeting", "")[:120] + "…") if len(entry.get("targeting", "")) > 120 else entry.get("targeting", ""),
            })
        return json.dumps({
            "dry_run": True,
            "campaign_id": campaign_id,
            "creative_id": creative_id,
            "total_adsets": len(adset_list),
            "plan": plan,
            "message": "DRY RUN — no changes made. Confirm with user, then call again with dry_run=False.",
        })

    # ── CREATE ───────────────────────────────────────────────────────────────
    created: list[dict] = []
    failed: list[dict] = []

    for i, entry in enumerate(adset_list, 1):
        adset_name = entry.get("name") or f"AdSet {i}"
        targeting_raw = entry.get("targeting", "")
        budget = entry.get("daily_budget", daily_budget_per_adset)
        freq_cap = entry.get("frequency_cap")

        # Validate targeting
        if not targeting_raw:
            failed.append({"index": i, "name": adset_name, "step": "adset", "reason": "targeting is required"})
            continue
        try:
            _ = json.loads(targeting_raw) if isinstance(targeting_raw, str) else targeting_raw
        except Exception:
            failed.append({"index": i, "name": adset_name, "step": "adset", "reason": "targeting is not valid JSON"})
            continue

        adset_data: dict = {
            "name": adset_name,
            "campaign_id": campaign_id,
            "daily_budget": str(budget),
            "optimization_goal": optimization_goal,
            "billing_event": billing_event,
            "targeting": targeting_raw if isinstance(targeting_raw, str) else json.dumps(targeting_raw),
            "status": "PAUSED",
        }
        if freq_cap:
            adset_data["frequency_control_specs"] = json.dumps([{
                "event": "IMPRESSIONS",
                "interval_days": 7,
                "max_frequency": int(freq_cap),
            }])

        # Retry on 429
        adset_result: dict = {}
        for _attempt in range(4):
            adset_result = _post(f"{acct}/adsets", adset_data)
            err_code = adset_result.get("error", {}).get("code") if isinstance(adset_result.get("error"), dict) else None
            if err_code in (4, 17, 32):
                time.sleep(2 ** _attempt)
                continue
            break

        if "error" in adset_result:
            err = adset_result["error"]
            reason = err.get("message", str(err)) if isinstance(err, dict) else str(err)
            failed.append({"index": i, "name": adset_name, "step": "adset", "reason": reason})
            time.sleep(0.3)
            continue

        adset_id = adset_result.get("id", "")
        ad_name = f"{prefix_ad} — {adset_name} ({i}/{len(adset_list)})"

        # Create the ad
        ad_result: dict = {}
        for _attempt in range(4):
            ad_result = _post(f"{acct}/ads", {
                "name": ad_name,
                "adset_id": adset_id,
                "creative": json.dumps({"creative_id": creative_id}),
                "status": "PAUSED",
            })
            err_code = ad_result.get("error", {}).get("code") if isinstance(ad_result.get("error"), dict) else None
            if err_code in (4, 17, 32):
                time.sleep(2 ** _attempt)
                continue
            break

        if "error" in ad_result:
            err = ad_result["error"]
            reason = err.get("message", str(err)) if isinstance(err, dict) else str(err)
            failed.append({"index": i, "name": adset_name, "adset_id": adset_id, "step": "ad", "reason": reason})
        else:
            created.append({
                "index": i,
                "adset_id": adset_id,
                "ad_id": ad_result.get("id", ""),
                "name": adset_name,
                "budget": budget,
            })

        time.sleep(0.3)

    return json.dumps({
        "campaign_id": campaign_id,
        "total_requested": len(adset_list),
        "adsets_created": len(created),
        "failed": len(failed),
        "summary": f"{len(created)}/{len(adset_list)} ad sets created successfully",
        "created": created,
        "failed_details": failed,
    })


@mcp.tool()
def meta_lead_export(
    page_id: str,
    form_ids: str = "",
    since: str = "",
    until: str = "",
    page_access_token: str = "",
    save_dir: str = "",
) -> str:
    """Export all leads from Facebook Lead Gen forms to CSV files (one per form).

    Fetches all forms for the page (or specific form IDs), reads all leads with
    pagination, and saves one CSV file per form as leads_{form_id}_{date}.csv.
    The CSV content is also returned inline for immediate use.

    Args:
        page_id: Facebook Page ID (required).
        form_ids: Comma-separated specific form IDs to export. Leave blank to fetch all forms on the page.
        since: Filter leads created after this value — Unix timestamp or YYYY-MM-DD. Optional.
        until: Filter leads created before this value — Unix timestamp or YYYY-MM-DD. Optional.
        page_access_token: Page access token. Leave blank to use META_ACCESS_TOKEN from .env.
        save_dir: Directory path to save CSV files. Defaults to current working directory.
    """
    import os as _os
    token = page_access_token or _token()

    # ── Helper: GET with 429 backoff ─────────────────────────────────────────
    def _get_leads(path: str, params: dict, max_retries: int = 5) -> dict:
        backoff = 1.0
        for _ in range(max_retries):
            resp = _get_with_token(path, token, params)
            err = resp.get("error", {})
            code = err.get("code") if isinstance(err, dict) else None
            if code in (4, 17, 32):
                time.sleep(backoff)
                backoff = min(backoff * 2, 32)
                continue
            return resp
        return resp

    # 1. Get form IDs
    if form_ids.strip():
        fids = [f.strip() for f in form_ids.split(",") if f.strip()]
        fid_names = {fid: fid for fid in fids}
    else:
        forms_resp = _get_leads(f"{page_id}/leadgen_forms", {"fields": "id,name", "limit": 50})
        if "error" in forms_resp:
            return json.dumps({"success": False, "error": forms_resp["error"]})
        fids = [f["id"] for f in forms_resp.get("data", [])]
        fid_names = {f["id"]: f.get("name", f["id"]) for f in forms_resp.get("data", [])}

    if not fids:
        return json.dumps({"success": False, "error": "No lead gen forms found for this page."})

    today_str = str(date.today())
    out_dir = save_dir.strip() if save_dir.strip() else _os.getcwd()
    saved_files: list[str] = []
    form_summary: list[dict] = []
    all_csvs: dict[str, str] = {}

    # 2. Fetch + export per form
    for fid in fids:
        base_params: dict = {"fields": "id,created_time,field_data", "limit": 100}
        filters = []
        if since:
            filters.append({"field": "time_created", "operator": "GREATER_THAN", "value": since})
        if until:
            filters.append({"field": "time_created", "operator": "LESS_THAN", "value": until})
        if filters:
            base_params["filtering"] = json.dumps(filters)

        leads_resp = _get_leads(f"{fid}/leads", base_params)
        leads: list[dict] = leads_resp.get("data", [])

        # Paginate through all pages
        while leads_resp.get("paging", {}).get("next"):
            after = leads_resp.get("paging", {}).get("cursors", {}).get("after", "")
            if not after:
                break
            p2 = dict(base_params)
            p2["after"] = after
            leads_resp = _get_leads(f"{fid}/leads", p2)
            leads.extend(leads_resp.get("data", []))

        form_summary.append({"form_id": fid, "form_name": fid_names.get(fid, fid), "leads_fetched": len(leads)})

        if not leads:
            continue

        # Build rows
        rows: list[dict] = []
        for lead in leads:
            row: dict = {
                "lead_id": lead.get("id", ""),
                "created_time": lead.get("created_time", ""),
                "form_id": fid,
                "form_name": fid_names.get(fid, fid),
            }
            for field in lead.get("field_data", []):
                key = field.get("name", "")
                val = ", ".join(field.get("values", []))
                row[key] = val
            rows.append(row)

        # Build CSV
        all_keys: list[str] = []
        seen: set[str] = set()
        for row in rows:
            for k in row:
                if k not in seen:
                    all_keys.append(k)
                    seen.add(k)

        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        csv_str = buf.getvalue()
        all_csvs[fid] = csv_str

        # Save file
        safe_name = fid_names.get(fid, fid).replace("/", "_").replace("\\", "_")[:40]
        filename = f"leads_{safe_name}_{today_str}.csv"
        filepath = _os.path.join(out_dir, filename)
        try:
            with open(filepath, "w", encoding="utf-8-sig", newline="") as f:
                f.write(csv_str)
            saved_files.append(filepath)
        except Exception as e:
            saved_files.append(f"SAVE_FAILED:{filepath} — {e}")

    total_leads = sum(s["leads_fetched"] for s in form_summary)

    return json.dumps({
        "success": True,
        "page_id": page_id,
        "total_forms": len(fids),
        "total_leads": total_leads,
        "form_summary": form_summary,
        "saved_files": saved_files,
        "csv_by_form": all_csvs,
    })


@mcp.tool()
def meta_cross_account_report(
    account_ids: str,
    start_date: str = "",
    end_date: str = "",
    sort_by: str = "roas",
) -> str:
    """Run a performance overview across multiple ad accounts and return a unified table.

    Calls each account individually and merges results, then sorts by the chosen metric.
    Ideal for agency-level reporting across all clients in one call.

    Args:
        account_ids: Comma-separated list of ad account IDs (with or without 'act_' prefix), e.g. "act_123,456,789" (required).
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        sort_by: Metric to sort results by. Options: roas, spend, impressions, clicks, ctr, cpm. Default roas.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))

    ids = [a.strip() for a in account_ids.split(",") if a.strip()]
    if not ids:
        return json.dumps({"success": False, "error": "No account IDs provided."})

    rows = []
    errors = []

    for raw_id in ids:
        acct = raw_id if raw_id.startswith("act_") else f"act_{raw_id}"
        result = _get(f"{acct}/insights", {
            "time_range": json.dumps({"since": sd, "until": ed}),
            "fields": "account_name,impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions,conversions",
        })

        if "error" in result:
            err = result["error"]
            errors.append({"account_id": acct, "error": err.get("message", str(err)) if isinstance(err, dict) else str(err)})
            continue

        data = result.get("data", [])
        if not data:
            rows.append({
                "account_id": acct,
                "account_name": "—",
                "spend": 0,
                "impressions": 0,
                "clicks": 0,
                "ctr": 0,
                "cpc": 0,
                "cpm": 0,
                "reach": 0,
                "frequency": 0,
                "conversions": 0,
                "roas": 0,
            })
            continue

        d = data[0]

        # Parse conversions
        conversions = 0
        conv_value = 0.0
        for action in d.get("actions", []):
            if action.get("action_type") in ("offsite_conversion.fb_pixel_purchase", "purchase"):
                conversions += int(action.get("value", 0))
        for action in d.get("conversions", []):
            if action.get("action_type") in ("offsite_conversion.fb_pixel_purchase", "purchase"):
                conv_value += float(action.get("value", 0))

        spend = float(d.get("spend", 0) or 0)
        roas = round(conv_value / spend, 2) if spend > 0 else 0

        rows.append({
            "account_id": acct,
            "account_name": d.get("account_name", acct),
            "spend": round(spend, 2),
            "impressions": int(d.get("impressions", 0) or 0),
            "clicks": int(d.get("clicks", 0) or 0),
            "ctr": round(float(d.get("ctr", 0) or 0), 2),
            "cpc": round(float(d.get("cpc", 0) or 0), 2),
            "cpm": round(float(d.get("cpm", 0) or 0), 2),
            "reach": int(d.get("reach", 0) or 0),
            "frequency": round(float(d.get("frequency", 0) or 0), 2),
            "conversions": conversions,
            "conv_value": round(conv_value, 2),
            "roas": roas,
        })

        time.sleep(0.3)

    # Sort
    valid_sort = {"roas", "spend", "impressions", "clicks", "ctr", "cpm", "reach", "conversions"}
    sort_key = sort_by if sort_by in valid_sort else "roas"
    rows.sort(key=lambda r: r.get(sort_key, 0), reverse=True)

    # Build summary row
    total_spend = sum(r["spend"] for r in rows)
    total_conv_value = sum(r.get("conv_value", 0) for r in rows)
    summary = {
        "accounts_queried": len(ids),
        "accounts_with_data": len(rows),
        "accounts_with_errors": len(errors),
        "date_range": f"{sd} → {ed}",
        "total_spend": round(total_spend, 2),
        "total_conversions": sum(r["conversions"] for r in rows),
        "total_conv_value": round(total_conv_value, 2),
        "blended_roas": round(total_conv_value / total_spend, 2) if total_spend > 0 else 0,
        "sorted_by": sort_key,
    }

    return json.dumps({
        "summary": summary,
        "accounts": rows,
        "errors": errors,
    })


@mcp.tool()
def meta_frequency_watcher(
    frequency_threshold: float = 3.0,
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
    include_paused: bool = False,
) -> str:
    """Monitor ad frequency across all campaigns and flag campaigns exceeding the threshold.

    Returns a list of at-risk campaigns with tailored recommendations:
    refresh_creative, reduce_budget, or pause.

    Args:
        frequency_threshold: Flag campaigns with frequency above this value. Default 3.0.
        start_date: Start date YYYY-MM-DD. Defaults to 14 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        include_paused: Include paused campaigns in the check. Default False (active only).
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=14))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()

    params: dict = {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "level": "campaign",
        "fields": "campaign_id,campaign_name,reach,frequency,impressions,spend,actions",
        "limit": 100,
    }
    if not include_paused:
        params["effective_status"] = json.dumps(["ACTIVE"])

    result = _get(f"{acct}/insights", params)
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})

    campaigns = result.get("data", [])

    ok = []
    flagged = []

    for c in campaigns:
        freq = float(c.get("frequency", 0) or 0)
        spend = float(c.get("spend", 0) or 0)
        reach = int(c.get("reach", 0) or 0)
        impressions = int(c.get("impressions", 0) or 0)

        # Derive recommendation
        if freq >= frequency_threshold * 1.5:
            recommendation = "pause"
            severity = "critical"
            reason = f"Frequency {freq:.1f} is {freq / frequency_threshold:.1f}x above threshold — severe ad fatigue."
        elif freq >= frequency_threshold * 1.2:
            recommendation = "refresh_creative"
            severity = "high"
            reason = f"Frequency {freq:.1f} — audience is oversaturated. Swap creatives immediately."
        elif freq >= frequency_threshold:
            recommendation = "reduce_budget"
            severity = "medium"
            reason = f"Frequency {freq:.1f} just crossed threshold. Reduce budget or expand audience."
        else:
            ok.append({
                "campaign_id": c.get("campaign_id", ""),
                "campaign_name": c.get("campaign_name", ""),
                "frequency": round(freq, 2),
                "status": "ok",
            })
            continue

        flagged.append({
            "campaign_id": c.get("campaign_id", ""),
            "campaign_name": c.get("campaign_name", ""),
            "frequency": round(freq, 2),
            "threshold": frequency_threshold,
            "reach": reach,
            "impressions": impressions,
            "spend": round(spend, 2),
            "severity": severity,
            "recommendation": recommendation,
            "reason": reason,
            "next_steps": {
                "pause": f"Call meta_update_status(object_id='{c.get('campaign_id','')}', object_type='campaign', status='PAUSED')",
                "refresh_creative": "Create new creative variant with meta_create_ad_creative, then update ad",
                "reduce_budget": f"Call meta_update_budget(campaign_id='{c.get('campaign_id','')}', daily_budget='<lower_amount>')",
            }.get(recommendation, ""),
        })

    # Sort flagged by frequency descending
    flagged.sort(key=lambda x: x["frequency"], reverse=True)

    return json.dumps({
        "date_range": f"{sd} → {ed}",
        "threshold": frequency_threshold,
        "total_campaigns_checked": len(campaigns),
        "flagged": len(flagged),
        "healthy": len(ok),
        "flagged_campaigns": flagged,
        "healthy_campaigns": ok,
        "summary": (
            f"{len(flagged)} campaign(s) above frequency {frequency_threshold}. "
            f"Critical: {sum(1 for f in flagged if f['severity']=='critical')}, "
            f"High: {sum(1 for f in flagged if f['severity']=='high')}, "
            f"Medium: {sum(1 for f in flagged if f['severity']=='medium')}."
        ) if flagged else f"All {len(ok)} campaigns are healthy (frequency < {frequency_threshold}).",
    })


# ---------------------------------------------------------------------------
# ADVANCED AUTOMATION TOOLS
# ---------------------------------------------------------------------------


@mcp.tool()
def meta_create_automated_rule(
    name: str,
    rule_type: str,
    evaluation_spec: str,
    execution_spec: str,
    account_id: str = "",
    schedule_spec: str = "",
    trigger_spec: str = "",
) -> str:
    """Create a Meta automated rule that monitors and acts on ad performance automatically.

    Two types:
    - SCHEDULE_BASED: runs on interval (DAILY/HOURLY/SEMI_HOURLY). Use schedule_spec.
    - TRIGGER_BASED: real-time reaction to performance changes. Use trigger_spec.
      NOTE: Trigger-based rules are API-only — not available in Ads Manager UI.

    Args:
        name: Rule name (required).
        rule_type: 'SCHEDULE_BASED' or 'TRIGGER_BASED' (required).
        evaluation_spec: JSON string — what to evaluate. Example:
          '{"evaluation_type":"SCHEDULE","filters":[{"field":"entity_type","value":"ADSET"},
            {"field":"campaign.delivery_status","value":"Active"}]}'
        execution_spec: JSON string — what action to take. Example:
          '{"execution_type":"PAUSE"}' or
          '{"execution_type":"CHANGE_BUDGET","execution_options":[{"field":"budget","value":10,"action_type":"PERCENTAGE","operator":"MULTIPLY_BY"}]}'
        account_id: Ad account ID. Leave blank for .env default.
        schedule_spec: JSON string for schedule-based rules. Example:
          '{"schedule_type":"DAILY","start_minute":480}' (480=8AM)
        trigger_spec: JSON string for trigger-based rules. Example:
          '{"type":"STATS_CHANGE","trigger_filters":[{"field":"cost_per_result","operator":">","value":50}]}'
    """
    if (err := require_editor("meta_create_automated_rule")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    data: dict = {
        "name": name,
        "rule_type": rule_type,
        "evaluation_spec": evaluation_spec,
        "execution_spec": execution_spec,
    }
    if schedule_spec:
        data["schedule_spec"] = schedule_spec
    if trigger_spec:
        data["trigger_spec"] = trigger_spec
    result = _post(f"{acct}/adrules_library", data)
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})
    return json.dumps({"success": True, "rule_id": result.get("id"), "name": name, "rule_type": rule_type})


@mcp.tool()
def meta_list_automated_rules(
    account_id: str = "",
    status: str = "ENABLED",
) -> str:
    """List all automated rules in an ad account.

    Args:
        account_id: Ad account ID. Leave blank for .env default.
        status: Filter by status: ENABLED, DISABLED, ALL. Default ENABLED.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    params: dict = {
        "fields": "id,name,status,rule_type,evaluation_spec,execution_spec,schedule_spec,created_time",
        "limit": 50,
    }
    if status != "ALL":
        params["status"] = status
    return json.dumps(_get(f"{acct}/adrules_library", params))


@mcp.tool()
def meta_update_automated_rule(
    rule_id: str,
    status: str = "",
    name: str = "",
) -> str:
    """Enable, disable, or rename an automated rule.

    Args:
        rule_id: Automated rule ID (required).
        status: 'ENABLED' or 'DISABLED'.
        name: New name for the rule.
    """
    if (err := require_editor("meta_update_automated_rule")): return err
    data: dict = {}
    if status:
        data["status"] = status
    if name:
        data["name"] = name
    if not data:
        return json.dumps({"success": False, "error": "Provide status or name to update."})
    result = _post(rule_id, data)
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})
    return json.dumps({"success": True, "rule_id": rule_id, "updated": data})


@mcp.tool()
def meta_delete_automated_rule(rule_id: str) -> str:
    """Delete an automated rule permanently.

    Args:
        rule_id: Automated rule ID to delete (required).
    """
    if (err := require_editor("meta_delete_automated_rule")): return err
    result = _delete(rule_id)
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})
    return json.dumps({"success": True, "rule_id": rule_id})


@mcp.tool()
def meta_create_ab_test(
    name: str,
    business_id: str,
    start_time: str,
    end_time: str,
    cells: str,
    confidence_level: float = 0.95,
    account_id: str = "",
) -> str:
    """Create an A/B split test (ad study) with mutually exclusive audience cells.

    Each cell receives a non-overlapping audience slice. Ideal for testing:
    - Different audiences (same creative)
    - Different placements
    - Different objectives

    Args:
        name: Study name (required).
        business_id: Meta Business Manager ID (required).
        start_time: ISO 8601 start datetime, e.g. '2026-04-01T00:00:00+0000' (required).
        end_time: ISO 8601 end datetime (required).
        cells: JSON array of cell configs. Each cell needs 'name', 'treatment_percentage',
               and optionally 'adsets' list. Example:
               '[{"name":"Control","treatment_percentage":50,"adsets":[{"id":"123"}]},
                 {"name":"Test","treatment_percentage":50,"adsets":[{"id":"456"}]}]'
        confidence_level: Statistical confidence level 0-1. Default 0.95.
        account_id: Ad account ID for context. Leave blank for .env default.
    """
    if (err := require_editor("meta_create_ab_test")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    try:
        cells_list = json.loads(cells) if isinstance(cells, str) else cells
    except json.JSONDecodeError as e:
        return json.dumps({"success": False, "error": f"Invalid cells JSON: {e}"})

    data: dict = {
        "name": name,
        "start_time": start_time,
        "end_time": end_time,
        "cells": json.dumps(cells_list),
        "confidence_level": str(confidence_level),
        "type": "SPLIT_TEST",
    }
    if acct:
        data["ad_account_id"] = acct.replace("act_", "")

    result = _post(f"{business_id}/ad_studies", data)
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})
    return json.dumps({"success": True, "study_id": result.get("id"), "name": name})


@mcp.tool()
def meta_get_ab_test_results(study_id: str) -> str:
    """Get results and status of an A/B split test.

    Args:
        study_id: Ad study ID (required).
    """
    result = _get(study_id, {
        "fields": "id,name,status,start_time,end_time,cells,confidence_level,"
                  "type,winner_cell_id,created_time"
    })
    return json.dumps(result)


@mcp.tool()
def meta_create_adset_with_dayparting(
    name: str,
    campaign_id: str,
    lifetime_budget: int,
    start_time: str,
    end_time: str,
    schedule: str,
    optimization_goal: str = "OFFSITE_CONVERSIONS",
    billing_event: str = "IMPRESSIONS",
    targeting: str = "",
    bid_amount: int = 0,
    account_id: str = "",
) -> str:
    """Create an ad set with dayparting (scheduled delivery by hour/day).

    NOTE: Dayparting REQUIRES lifetime_budget — daily budget does not support scheduling.

    Args:
        name: Ad set name (required).
        campaign_id: Campaign ID (required).
        lifetime_budget: Total budget in smallest currency unit, e.g. 100000 = ₪1000 (required).
        start_time: Campaign start ISO datetime, e.g. '2026-04-01T00:00:00+0300' (required).
        end_time: Campaign end ISO datetime (required).
        schedule: JSON array of day-part windows. Each entry:
          {"start_minute": 540, "end_minute": 1080, "days": [1,2,3,4,5]}
          start/end_minute = minutes after midnight (must be multiples of 30).
          days: 0=Sun, 1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri, 6=Sat. (required)
          Example (Mon-Fri 9AM-6PM): '[{"start_minute":540,"end_minute":1080,"days":[1,2,3,4,5]}]'
        optimization_goal: Default OFFSITE_CONVERSIONS.
        billing_event: Default IMPRESSIONS.
        targeting: JSON targeting spec string. Leave blank for broad targeting.
        bid_amount: Bid cap in smallest currency unit. 0 = auto bid.
        account_id: Ad account ID. Leave blank for .env default.
    """
    if (err := require_editor("meta_create_adset_with_dayparting")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    try:
        schedule_list = json.loads(schedule) if isinstance(schedule, str) else schedule
    except json.JSONDecodeError as e:
        return json.dumps({"success": False, "error": f"Invalid schedule JSON: {e}"})

    data: dict = {
        "name": name,
        "campaign_id": campaign_id,
        "lifetime_budget": str(lifetime_budget),
        "start_time": start_time,
        "end_time": end_time,
        "adset_schedule": json.dumps(schedule_list),
        "pacing_type": json.dumps(["day_parting"]),
        "optimization_goal": optimization_goal,
        "billing_event": billing_event,
        "status": "PAUSED",
    }
    if targeting:
        data["targeting"] = targeting
    else:
        data["targeting"] = json.dumps({"geo_locations": {"countries": ["IL"]}})
    if bid_amount:
        data["bid_amount"] = str(bid_amount)

    result = _post(f"{acct}/adsets", data)
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})
    return json.dumps({
        "success": True,
        "adset_id": result.get("id"),
        "name": name,
        "schedule": schedule_list,
        "note": "Ad set created PAUSED. Activate when ready.",
    })


@mcp.tool()
def meta_create_advantage_plus_campaign(
    name: str,
    objective: str,
    budget: int,
    budget_type: str = "DAILY",
    bid_strategy: str = "LOWEST_COST_WITHOUT_CAP",
    account_id: str = "",
) -> str:
    """Create a Meta Advantage+ campaign (maximum AI automation: audience + placements + budget).

    Creates a campaign with all three Advantage+ levers enabled simultaneously:
    - Advantage+ Budget (campaign-level)
    - Advantage+ Audience (full automation)
    - Advantage+ Placements (automatic)

    Supported objectives: OUTCOME_SALES, OUTCOME_LEADS, OUTCOME_APP_PROMOTION

    Args:
        name: Campaign name (required).
        objective: 'OUTCOME_SALES', 'OUTCOME_LEADS', or 'OUTCOME_APP_PROMOTION' (required).
        budget: Daily or lifetime budget in smallest currency unit, e.g. 20000 = ₪200 (required).
        budget_type: 'DAILY' or 'LIFETIME'. Default DAILY.
        bid_strategy: 'LOWEST_COST_WITHOUT_CAP', 'COST_CAP', 'LOWEST_COST_WITH_BID_CAP',
                      'LOWEST_COST_WITH_MIN_ROAS'. Default LOWEST_COST_WITHOUT_CAP.
        account_id: Ad account ID. Leave blank for .env default.
    """
    if (err := require_editor("meta_create_advantage_plus_campaign")): return err
    valid_objectives = {"OUTCOME_SALES", "OUTCOME_LEADS", "OUTCOME_APP_PROMOTION"}
    if objective not in valid_objectives:
        return json.dumps({"success": False,
                           "error": f"objective must be one of: {valid_objectives}"})

    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()

    campaign_data: dict = {
        "name": name,
        "objective": objective,
        "bid_strategy": bid_strategy,
        "special_ad_categories": json.dumps([]),
        "status": "PAUSED",
    }
    if budget_type == "DAILY":
        campaign_data["daily_budget"] = str(budget)
    else:
        campaign_data["lifetime_budget"] = str(budget)

    campaign_result = _post(f"{acct}/campaigns", campaign_data)
    if "error" in campaign_result:
        return json.dumps({"success": False, "step": "campaign", "error": campaign_result["error"]})

    campaign_id = campaign_result.get("id")

    # Create Advantage+ adset: only geo targeting, advantage_audience=1, no placement restrictions
    adset_data: dict = {
        "name": f"{name} — Advantage+ AdSet",
        "campaign_id": campaign_id,
        "optimization_goal": "OFFSITE_CONVERSIONS" if objective == "OUTCOME_SALES" else "LEAD_GENERATION",
        "billing_event": "IMPRESSIONS",
        "targeting": json.dumps({
            "geo_locations": {"countries": ["IL"]},
            "targeting_automation": {"advantage_audience": 1},
        }),
        "status": "PAUSED",
    }

    adset_result = _post(f"{acct}/adsets", adset_data)
    if "error" in adset_result:
        return json.dumps({
            "success": True,
            "campaign_id": campaign_id,
            "campaign_note": "Campaign created. AdSet creation failed (may need creative/pixel first).",
            "adset_error": adset_result["error"],
        })

    return json.dumps({
        "success": True,
        "campaign_id": campaign_id,
        "adset_id": adset_result.get("id"),
        "objective": objective,
        "advantage_plus": True,
        "automation": {
            "audience": "advantage+ (full automation)",
            "placements": "advantage+ (automatic)",
            "budget": f"campaign-level {budget_type.lower()}",
        },
        "note": "Created PAUSED. Add creative and activate when ready.",
    })


@mcp.tool()
def meta_send_whatsapp_message(
    phone_number_id: str,
    to: str,
    message_type: str = "text",
    text: str = "",
    template_name: str = "",
    template_language: str = "he",
    template_components: str = "",
    access_token: str = "",
) -> str:
    """Send a WhatsApp message via Meta WhatsApp Business Cloud API.

    Requires a WhatsApp Business Account (WABA) with messaging permissions.
    For outbound messages to new contacts, use template_name (pre-approved templates only).
    Free-form text only works within 24h customer service window.

    Args:
        phone_number_id: WhatsApp Business phone number ID from WABA (required).
        to: Recipient phone number with country code, e.g. '972501234567' (required).
        message_type: 'text' or 'template'. Default 'text'.
        text: Message text for text type messages.
        template_name: Approved template name for template messages.
        template_language: Template language code. Default 'he' (Hebrew).
        template_components: JSON array of template components for dynamic values.
        access_token: WhatsApp System User access token. Required — different from Meta Ads token.
    """
    if (err := require_editor("meta_send_whatsapp_message")): return err
    if not access_token:
        return json.dumps({"success": False,
                           "error": "WhatsApp requires a separate System User token with whatsapp_business_messaging permission."})

    body: dict = {
        "messaging_product": "whatsapp",
        "to": to.replace("+", "").replace(" ", "").replace("-", ""),
    }

    if message_type == "text":
        if not text:
            return json.dumps({"success": False, "error": "text is required for message_type='text'."})
        body["type"] = "text"
        body["text"] = {"body": text}
    elif message_type == "template":
        if not template_name:
            return json.dumps({"success": False, "error": "template_name is required for message_type='template'."})
        body["type"] = "template"
        tmpl: dict = {
            "name": template_name,
            "language": {"code": template_language},
        }
        if template_components:
            try:
                tmpl["components"] = json.loads(template_components)
            except json.JSONDecodeError as e:
                return json.dumps({"success": False, "error": f"Invalid template_components JSON: {e}"})
        body["template"] = tmpl
    else:
        return json.dumps({"success": False, "error": "message_type must be 'text' or 'template'."})

    resp = requests.post(
        f"{GRAPH_BASE}/{phone_number_id}/messages",
        json={**body, "access_token": access_token},
        timeout=20,
    )
    result = resp.json()
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})
    return json.dumps({"success": True, "message_id": result.get("messages", [{}])[0].get("id"), "to": to})


@mcp.tool()
def meta_async_batch(
    account_id: str,
    operations: str,
    notification_uri: str = "",
) -> str:
    """Submit up to 1000 ad operations in a single async batch request.

    Much faster than calling tools in a loop for bulk creation. Meta processes
    operations in parallel and returns a request_set_id to poll for results.

    Args:
        account_id: Ad account ID (required).
        operations: JSON array of operations. Each operation needs 'method' (POST/DELETE),
                    'relative_url', and optionally 'body'. Example:
                    '[{"method":"POST","relative_url":"act_123/campaigns",
                       "body":"name=Test&objective=OUTCOME_SALES&special_ad_categories=[]&status=PAUSED"}]'
        notification_uri: Optional webhook URL to call when batch completes.
    """
    if (err := require_editor("meta_async_batch")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    try:
        ops = json.loads(operations) if isinstance(operations, str) else operations
    except json.JSONDecodeError as e:
        return json.dumps({"success": False, "error": f"Invalid operations JSON: {e}"})

    if len(ops) > 1000:
        return json.dumps({"success": False, "error": "Max 1000 operations per async batch."})

    data: dict = {
        "adbatch": json.dumps(ops),
        "name": f"batch_{int(time.time())}",
    }
    if notification_uri:
        data["notification_uri"] = notification_uri

    result = _post(f"{acct}/async_batch_requests", data)
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})

    request_set_id = result.get("id")
    return json.dumps({
        "success": True,
        "request_set_id": request_set_id,
        "operations_submitted": len(ops),
        "how_to_check": f"Call meta_get_async_batch_status(request_set_id='{request_set_id}')",
    })


@mcp.tool()
def meta_get_async_batch_status(request_set_id: str) -> str:
    """Check the status of an async batch request.

    Args:
        request_set_id: Async batch request set ID from meta_async_batch (required).
    """
    result = _get(request_set_id, {
        "fields": "id,name,total_count,success_count,error_count,is_completed,requests"
    })
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})

    is_done = result.get("is_completed", False)
    total = result.get("total_count", 0)
    success = result.get("success_count", 0)
    errors = result.get("error_count", 0)
    pct = round(success / total * 100, 1) if total > 0 else 0

    return json.dumps({
        "request_set_id": request_set_id,
        "is_completed": is_done,
        "total": total,
        "success": success,
        "errors": errors,
        "success_rate": f"{pct}%",
        "status": "DONE" if is_done else "IN_PROGRESS",
        "requests_sample": result.get("requests", {}).get("data", [])[:5],
    })


@mcp.tool()
def meta_get_delivery_insights(
    object_id: str,
    object_type: str = "campaign",
) -> str:
    """Get delivery diagnostics for a campaign, ad set, or ad.

    Returns: delivery status, learning phase, estimated daily results, issues.

    Args:
        object_id: Campaign, adset, or ad ID (required).
        object_type: 'campaign', 'adset', or 'ad'. Used for context only. Default 'campaign'.
    """
    result = _get(object_id, {
        "fields": "id,name,status,effective_status,delivery_info,"
                  "adset_schedule,issues_info,configured_status,"
                  "learning_stage_info,account_id"
    })
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})

    delivery = result.get("delivery_info", {})
    issues = result.get("issues_info", [])
    learning = result.get("learning_stage_info", {})

    return json.dumps({
        "object_id": object_id,
        "object_type": object_type,
        "name": result.get("name"),
        "status": result.get("status"),
        "effective_status": result.get("effective_status"),
        "delivery_status": delivery.get("status") if delivery else None,
        "estimated_daily_results": delivery if delivery else None,
        "learning_phase": {
            "status": learning.get("status") if learning else None,
            "attribution_windows": learning.get("attribution_windows") if learning else None,
            "exits": learning.get("exits") if learning else None,
        } if learning else None,
        "issues": [{"level": i.get("level"), "message": i.get("error_message")} for i in issues],
        "has_issues": len(issues) > 0,
        "raw": result,
    })


@mcp.tool()
def meta_set_adset_budget_guardrails(
    adset_id: str,
    daily_min_spend_target: int = 0,
    daily_spend_cap: int = 0,
    lifetime_min_spend_target: int = 0,
    lifetime_spend_cap: int = 0,
) -> str:
    """Set budget guardrails on an ad set within a Campaign Budget Optimization campaign.

    Use to control how Meta distributes the campaign budget among ad sets:
    - min_spend_target: soft floor (Meta tries to hit this but not guaranteed)
    - spend_cap: hard ceiling (Meta will never exceed this per day/lifetime)

    Values in smallest currency unit (e.g. 5000 = ₪50).

    Args:
        adset_id: Ad set ID (required).
        daily_min_spend_target: Minimum daily spend floor (soft). 0 = no floor.
        daily_spend_cap: Maximum daily spend cap (hard). 0 = no cap.
        lifetime_min_spend_target: Minimum lifetime spend floor (soft). 0 = no floor.
        lifetime_spend_cap: Maximum lifetime spend cap (hard). 0 = no cap.
    """
    if (err := require_editor("meta_set_adset_budget_guardrails")): return err
    data: dict = {}
    if daily_min_spend_target:
        data["daily_min_spend_target"] = str(daily_min_spend_target)
    if daily_spend_cap:
        data["daily_spend_cap"] = str(daily_spend_cap)
    if lifetime_min_spend_target:
        data["lifetime_min_spend_target"] = str(lifetime_min_spend_target)
    if lifetime_spend_cap:
        data["lifetime_spend_cap"] = str(lifetime_spend_cap)
    if not data:
        return json.dumps({"success": False, "error": "Provide at least one guardrail value."})

    result = _post(adset_id, data)
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})
    return json.dumps({"success": True, "adset_id": adset_id, "guardrails_set": data})


@mcp.tool()
def meta_update_adset_budget(
    adset_id: str,
    daily_budget: int = 0,
    lifetime_budget: int = 0,
    bid_amount: int = 0,
) -> str:
    """Update the budget or bid on a specific ad set.

    Args:
        adset_id: Ad set ID to update (required).
        daily_budget: New daily budget in smallest currency unit. 0 = no change.
        lifetime_budget: New lifetime budget in smallest currency unit. 0 = no change.
        bid_amount: New bid cap in smallest currency unit. 0 = no change.
    """
    if (err := require_editor("meta_update_adset_budget")): return err
    data: dict = {}
    if daily_budget:
        data["daily_budget"] = str(daily_budget)
    if lifetime_budget:
        data["lifetime_budget"] = str(lifetime_budget)
    if bid_amount:
        data["bid_amount"] = str(bid_amount)
    if not data:
        return json.dumps({"success": False, "error": "Provide daily_budget, lifetime_budget, or bid_amount."})
    result = _post(adset_id, data)
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})
    return json.dumps({"success": True, "adset_id": adset_id, "updated": data})


@mcp.tool()
def meta_get_ad_suggestions(
    account_id: str = "",
    objective: str = "OUTCOME_SALES",
) -> str:
    """Get Meta's automated ad suggestions and recommendations for an account.

    Returns recommendations for budget increases, creative refresh, audience expansion, etc.

    Args:
        account_id: Ad account ID. Leave blank for .env default.
        objective: Campaign objective to filter recommendations. Default OUTCOME_SALES.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    result = _get(f"{acct}/recommendations", {
        "fields": "id,code,confidence,importance,message,title,campaign_id,adset_id",
        "limit": 50,
    })
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})

    recs = result.get("data", [])
    # Group by importance
    by_importance: dict = {"HIGH": [], "MEDIUM": [], "LOW": []}
    for r in recs:
        imp = r.get("importance", "LOW").upper()
        by_importance.setdefault(imp, []).append({
            "id": r.get("id"),
            "title": r.get("title"),
            "message": r.get("message"),
            "code": r.get("code"),
            "campaign_id": r.get("campaign_id"),
            "adset_id": r.get("adset_id"),
        })

    return json.dumps({
        "total_recommendations": len(recs),
        "high_priority": len(by_importance["HIGH"]),
        "medium_priority": len(by_importance["MEDIUM"]),
        "low_priority": len(by_importance["LOW"]),
        "recommendations": by_importance,
    })


@mcp.tool()
def meta_apply_recommendation(recommendation_id: str) -> str:
    """Apply a Meta recommendation (e.g. budget increase, audience expansion) automatically.

    Args:
        recommendation_id: Recommendation ID from meta_get_ad_suggestions (required).
    """
    if (err := require_editor("meta_apply_recommendation")): return err
    result = _post(f"{recommendation_id}/apply", {})
    if "error" in result:
        return json.dumps({"success": False, "error": result["error"]})
    return json.dumps({"success": True, "recommendation_id": recommendation_id, "applied": True})


@mcp.tool()
def meta_get_account_diagnostics(account_id: str = "") -> str:
    """Get a full diagnostic report for an ad account: spending limits, status, active campaigns,
    pixel health, and recent issues — all in one call.

    Args:
        account_id: Ad account ID. Leave blank for .env default.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    from datetime import date, timedelta

    diag: dict = {}

    # Account info
    acct_info = _get(acct, {
        "fields": "id,name,account_status,currency,timezone_name,spend_cap,amount_spent,balance,funding_source_details"
    })
    diag["account"] = acct_info

    # Pixel health
    pixels = _get(f"{acct}/adspixels", {"fields": "id,name,last_fired_time", "limit": 5})
    diag["pixels"] = pixels.get("data", [])

    # Active campaigns count
    camps = _get(f"{acct}/campaigns", {
        "effective_status": json.dumps(["ACTIVE"]),
        "fields": "id,name",
        "limit": 100,
    })
    diag["active_campaigns"] = len(camps.get("data", []))

    # Recent spend (last 7 days)
    ed = str(date.today())
    sd = str(date.today() - timedelta(days=7))
    spend = _get(f"{acct}/insights", {
        "time_range": json.dumps({"since": sd, "until": ed}),
        "fields": "spend,impressions,clicks,actions",
    })
    diag["last_7d"] = spend.get("data", [{}])[0] if spend.get("data") else {}

    status_map = {1: "ACTIVE", 2: "DISABLED", 3: "UNSETTLED", 7: "PENDING_RISK_REVIEW",
                  9: "IN_GRACE_PERIOD", 100: "PENDING_CLOSURE", 101: "CLOSED",
                  201: "ANY_ACTIVE", 202: "ANY_CLOSED"}
    if "account_status" in diag.get("account", {}):
        diag["account"]["account_status_label"] = status_map.get(
            diag["account"]["account_status"], "UNKNOWN")

    return json.dumps(diag)


@mcp.tool()
def meta_sync_batch(operations: str) -> str:
    """Run up to 50 Graph API calls in a single synchronous batch request.

    Faster than N individual calls for dependent chains. Supports JSONPath references
    between steps: use {result=step_name:$.id} to pass the ID from step N to step N+1.

    Args:
        operations: JSON array of operations. Each needs 'method', 'relative_url',
                    and optionally 'body' and 'name'. Example:
                    '[{"method":"GET","relative_url":"me","name":"get_me"},
                      {"method":"GET","relative_url":"me/accounts"}]'
    """
    try:
        ops = json.loads(operations) if isinstance(operations, str) else operations
    except json.JSONDecodeError as e:
        return json.dumps({"success": False, "error": f"Invalid operations JSON: {e}"})
    if len(ops) > 50:
        return json.dumps({"success": False, "error": "Sync batch max is 50 operations. Use meta_async_batch for larger batches."})

    resp = requests.post(
        "https://graph.facebook.com/",
        data={"access_token": _token(), "batch": json.dumps(ops)},
        timeout=60,
    )
    results = resp.json()
    if isinstance(results, dict) and "error" in results:
        return json.dumps({"success": False, "error": results["error"]})

    parsed = []
    for i, r in enumerate(results or []):
        try:
            body = json.loads(r.get("body", "{}"))
        except Exception:
            body = r.get("body", "")
        parsed.append({
            "step": i,
            "name": ops[i].get("name", f"step_{i}") if i < len(ops) else f"step_{i}",
            "status_code": r.get("code"),
            "success": r.get("code") in (200, 201),
            "body": body,
        })

    return json.dumps({
        "success": True,
        "total": len(parsed),
        "succeeded": sum(1 for p in parsed if p["success"]),
        "failed": sum(1 for p in parsed if not p["success"]),
        "results": parsed,
    })


# ---------------------------------------------------------------------------
# ADVANTAGE+ CREATIVE
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_create_advantage_plus_creative(
    account_id: str,
    page_id: str,
    asset_feed_spec: str,
    name: str = "Advantage+ Creative",
    degrees_of_freedom: str = "",
) -> str:
    """Create an Advantage+ (dynamic) ad creative using asset_feed_spec.

    asset_feed_spec lets you provide multiple headlines, bodies, images, videos, and CTAs
    so Meta's AI can mix-and-match for best performance per user.

    Args:
        account_id: Ad account ID. Leave blank for .env default.
        page_id: Facebook Page ID that the ad will come from.
        asset_feed_spec: JSON object. Keys: bodies (array of {text}), titles (array of {text}),
                         link_urls (array of {website_url, display_url}), images (array of {hash}),
                         videos (array of {video_id, thumbnail_hash}),
                         call_to_action_types (array of strings like LEARN_MORE, SHOP_NOW).
                         Example: '{"bodies":[{"text":"Try now"}],"titles":[{"text":"Best deal"}],
                         "call_to_action_types":["LEARN_MORE"]}'
        name: Creative name.
        degrees_of_freedom: Optional JSON to enable/disable specific transformations.
                            Example: '{"creative_features_spec":{"standard_enhancements":{"enroll_status":"OPT_IN"}}}'
    """
    if (err := require_editor("meta_create_advantage_plus_creative")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    try:
        afs = json.loads(asset_feed_spec)
    except Exception as e:
        return json.dumps({"error": f"asset_feed_spec must be valid JSON: {e}"})

    payload: dict = {
        "name": name,
        "object_story_spec": {
            "page_id": page_id,
        },
        "asset_feed_spec": afs,
    }
    if degrees_of_freedom:
        try:
            payload["degrees_of_freedom_spec"] = json.loads(degrees_of_freedom)
        except Exception:
            pass

    return json.dumps(_post(f"{acct}/adcreatives", payload))


# ---------------------------------------------------------------------------
# BOOST / POST-BASED CREATIVE
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_boost_existing_post(
    account_id: str,
    page_id: str,
    post_id: str,
    campaign_id: str,
    adset_id: str,
    ad_name: str = "Boosted Post",
) -> str:
    """Boost an existing organic Facebook Page post as an ad using object_story_id.

    This creates an ad creative referencing the post, then creates an ad in the given adset.
    The post keeps its existing likes/comments/shares (social proof).

    Args:
        account_id: Ad account ID. Leave blank for .env default.
        page_id: Facebook Page ID that owns the post.
        post_id: The Facebook post ID (the numeric part after the page ID, or full post ID like 123_456).
        campaign_id: Campaign to place the boosted post ad in.
        adset_id: Ad set to place the ad in.
        ad_name: Name for the ad (default: 'Boosted Post').
    """
    if (err := require_editor("meta_boost_existing_post")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()

    # Build the story_id — Meta format is {page_id}_{post_id}
    if "_" in str(post_id) and not str(post_id).startswith(str(page_id)):
        story_id = post_id  # already full ID
    else:
        story_id = f"{page_id}_{post_id}"

    creative_payload = {
        "name": f"{ad_name} Creative",
        "object_story_id": story_id,
    }
    creative_result = _post(f"{acct}/adcreatives", creative_payload)
    if "error" in creative_result:
        return json.dumps({"error": "Creative creation failed", "details": creative_result})

    creative_id = creative_result.get("id")
    ad_payload = {
        "name": ad_name,
        "adset_id": adset_id,
        "creative": json.dumps({"creative_id": creative_id}),
        "status": "PAUSED",
    }
    ad_result = _post(f"{acct}/ads", ad_payload)
    return json.dumps({
        "creative_id": creative_id,
        "ad": ad_result,
        "story_id_used": story_id,
    })


# ---------------------------------------------------------------------------
# LEAD GEN FORMS
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_create_leadgen_form(
    page_id: str,
    name: str,
    questions: str,
    privacy_policy_url: str,
    page_access_token: str,
    thank_you_message: str = "",
    locale: str = "he_IL",
    follow_up_action_url: str = "",
    context_card_title: str = "",
    context_card_content: str = "",
) -> str:
    """Create a Facebook Lead Gen form on a Page.

    Args:
        page_id: Facebook Page ID.
        name: Internal name for the form.
        questions: JSON array of question objects. Each has 'type' (built-in: FULL_NAME, EMAIL,
                   PHONE, CITY, COMPANY_NAME, JOB_TITLE, etc.) or for custom questions:
                   {"type":"CUSTOM","label":"Your question","key":"custom_key"}.
                   Example: '[{"type":"FULL_NAME"},{"type":"EMAIL"},{"type":"PHONE"}]'
        privacy_policy_url: URL to your privacy policy (required by Meta).
        page_access_token: Page-level access token.
        thank_you_message: Message shown after submission. Defaults to generic.
        locale: Form locale, e.g. he_IL, en_US.
        follow_up_action_url: URL to redirect after form submit.
        context_card_title: Optional intro card title shown before the form.
        context_card_content: Optional intro card body text.
    """
    if (err := require_editor("meta_create_leadgen_form")): return err
    try:
        qs = json.loads(questions)
    except Exception as e:
        return json.dumps({"error": f"questions must be valid JSON array: {e}"})

    payload: dict = {
        "name": name,
        "questions": qs,
        "privacy_policy": {"url": privacy_policy_url},
        "locale": locale,
        "access_token": page_access_token,
    }
    if thank_you_message:
        payload["thank_you_page"] = {"title": "תודה!", "body": thank_you_message}
        if follow_up_action_url:
            payload["thank_you_page"]["website_url"] = follow_up_action_url

    if context_card_title:
        payload["context_card"] = {
            "style": "LIST_STYLE",
            "title": context_card_title,
            "content": [context_card_content] if context_card_content else [],
        }

    resp = requests.post(
        f"{GRAPH_BASE}/{page_id}/leadgen_forms",
        json=payload,
        timeout=30,
    )
    if not resp.ok:
        return json.dumps({"error": resp.json().get("error", resp.text[:300])})
    return json.dumps(resp.json())


@mcp.tool()
def meta_list_leadgen_forms(page_id: str, page_access_token: str) -> str:
    """List all lead gen forms for a Facebook Page.

    Args:
        page_id: Facebook Page ID.
        page_access_token: Page-level access token.
    """
    resp = requests.get(
        f"{GRAPH_BASE}/{page_id}/leadgen_forms",
        params={
            "fields": "id,name,status,created_time,leads_count,expired_leads_count",
            "access_token": page_access_token,
        },
        timeout=30,
    )
    if not resp.ok:
        return json.dumps({"error": resp.json().get("error", resp.text[:300])})
    return json.dumps(resp.json())


# ---------------------------------------------------------------------------
# INSTAGRAM STORY CREATION (Container API)
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_create_instagram_story(
    ig_user_id: str,
    media_type: str,
    media_url: str,
    page_access_token: str,
    link_url: str = "",
    sticker_data: str = "",
) -> str:
    """Create and publish an Instagram Story (image or video) using the container API.

    Two-step process: create container → publish. Supports link stickers.

    Args:
        ig_user_id: Instagram Business Account ID (from CLIENT_PAGES.md).
        media_type: IMAGE or VIDEO.
        media_url: Public URL of the image or video.
        page_access_token: Page-level access token for the linked Facebook Page.
        link_url: Optional URL for the link sticker (swipe-up equivalent).
        sticker_data: Optional JSON for custom sticker overlays.
    """
    if (err := require_editor("meta_create_instagram_story")): return err
    # Step 1 — create container
    container_params: dict = {
        "media_type": "STORIES",
        "access_token": page_access_token,
    }
    if media_type.upper() == "VIDEO":
        container_params["video_url"] = media_url
    else:
        container_params["image_url"] = media_url

    if link_url:
        container_params["story_sticker_ids"] = "link"
        container_params["url"] = link_url

    if sticker_data:
        try:
            container_params.update(json.loads(sticker_data))
        except Exception:
            pass

    create_resp = requests.post(
        f"{GRAPH_BASE}/{ig_user_id}/media",
        params=container_params,
        timeout=30,
    )
    if not create_resp.ok:
        return json.dumps({"error": "Container creation failed", "details": create_resp.json()})

    container_id = create_resp.json().get("id")
    if not container_id:
        return json.dumps({"error": "No container ID returned", "response": create_resp.json()})

    # Step 2 — publish
    publish_resp = requests.post(
        f"{GRAPH_BASE}/{ig_user_id}/media_publish",
        params={"creation_id": container_id, "access_token": page_access_token},
        timeout=30,
    )
    if not publish_resp.ok:
        return json.dumps({"error": "Publish failed", "container_id": container_id, "details": publish_resp.json()})

    return json.dumps({
        "success": True,
        "container_id": container_id,
        "media_id": publish_resp.json().get("id"),
    })


# ---------------------------------------------------------------------------
# REACH & FREQUENCY PREDICTION
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_reach_frequency_prediction(
    account_id: str,
    start_time: str,
    end_time: str,
    budget: str,
    target_spec: str,
    prediction_mode: str = "0",
    objective: str = "REACH",
    frequency_cap: str = "",
    interval_frequency_cap_reset_period: str = "",
) -> str:
    """Get Reach & Frequency prediction for a campaign before buying.

    Returns estimated reach, impressions, frequency, and CPM for a given
    audience + budget + date range. Use this before launching a branding campaign.

    Args:
        account_id: Ad account ID. Leave blank for .env default.
        start_time: Start time as Unix timestamp string (e.g. '1735689600').
        end_time: End time as Unix timestamp string.
        budget: Total budget in account currency smallest unit (cents/agorot).
        target_spec: JSON targeting spec (same as adset targeting). Example:
                     '{"geo_locations":{"countries":["IL"]},"age_min":25,"age_max":45}'
        prediction_mode: 0=BASIC (budget→reach), 1=EXPERT (reach→budget). Default: 0.
        objective: REACH or BRAND_AWARENESS. Default: REACH.
        frequency_cap: Max impressions per user (e.g. '3').
        interval_frequency_cap_reset_period: Hours for frequency cap reset (e.g. '168' for weekly).
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()

    payload: dict = {
        "start_time": int(start_time),
        "end_time": int(end_time),
        "budget": int(budget),
        "target_spec": json.loads(target_spec) if isinstance(target_spec, str) else target_spec,
        "prediction_mode": int(prediction_mode),
        "objective": objective,
    }
    if frequency_cap:
        payload["frequency_cap"] = int(frequency_cap)
    if interval_frequency_cap_reset_period:
        payload["interval_frequency_cap_reset_period"] = int(interval_frequency_cap_reset_period)

    return json.dumps(_post(f"{acct}/reachfrequencypredictions", payload))


# ---------------------------------------------------------------------------
# PUBLISHER BLOCK LISTS / BRAND SAFETY
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_publisher_block_lists(account_id: str = "") -> str:
    """List all publisher block lists for an ad account (Brand Safety).

    Args:
        account_id: Ad account ID. Leave blank for .env default.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(f"{acct}/publisher_block_lists", {"fields": "id,name,last_update_time"}))


@mcp.tool()
def meta_create_publisher_block_list(
    name: str,
    publisher_urls: str,
    account_id: str = "",
) -> str:
    """Create a publisher block list to exclude specific websites/apps from your ads.

    Args:
        account_id: Ad account ID. Leave blank for .env default.
        name: Name for the block list.
        publisher_urls: JSON array of domain strings to block.
                        Example: '["example.com","badsite.net"]'
    """
    if (err := require_editor("meta_create_publisher_block_list")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    try:
        urls = json.loads(publisher_urls)
    except Exception as e:
        return json.dumps({"error": f"publisher_urls must be valid JSON array: {e}"})
    return json.dumps(_post(f"{acct}/publisher_block_lists", {"name": name, "publisher_urls": urls}))


@mcp.tool()
def meta_get_brand_safety_controls(account_id: str = "") -> str:
    """Get current brand safety / content suitability controls for an ad account.

    Returns: content_delivery_report, inventory_filter settings.

    Args:
        account_id: Ad account ID. Leave blank for .env default.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_get(acct, {
        "fields": "brand_safety_content_filter_levels,content_delivery_report,is_notifications_enabled"
    }))


# ---------------------------------------------------------------------------
# LIVE VIDEO
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_create_live_video(
    page_id: str,
    page_access_token: str,
    title: str = "",
    description: str = "",
    status: str = "UNPUBLISHED",
) -> str:
    """Create a live video broadcast on a Facebook Page. Returns stream_url and stream_key.

    After creating, use the returned stream_url to start streaming (e.g. OBS, ffmpeg).
    Then call meta_publish_live_video to make it public.

    Args:
        page_id: Facebook Page ID.
        page_access_token: Page-level access token.
        title: Live video title.
        description: Live video description.
        status: UNPUBLISHED (default, stream privately first) or LIVE_NOW.
    """
    if (err := require_editor("meta_create_live_video")): return err
    payload: dict = {
        "status": status,
        "access_token": page_access_token,
    }
    if title:
        payload["title"] = title
    if description:
        payload["description"] = description

    resp = requests.post(
        f"{GRAPH_BASE}/{page_id}/live_videos",
        json=payload,
        timeout=30,
    )
    if not resp.ok:
        return json.dumps({"error": resp.json().get("error", resp.text[:300])})
    result = resp.json()
    return json.dumps({
        "id": result.get("id"),
        "stream_url": result.get("stream_url"),
        "secure_stream_url": result.get("secure_stream_url"),
        "status": result.get("status"),
    })


@mcp.tool()
def meta_publish_live_video(
    live_video_id: str,
    page_access_token: str,
) -> str:
    """Publish (go live) or end a live video broadcast.

    Args:
        live_video_id: The live video ID returned by meta_create_live_video.
        page_access_token: Page-level access token.
    """
    if (err := require_editor("meta_publish_live_video")): return err
    resp = requests.post(
        f"{GRAPH_BASE}/{live_video_id}",
        json={"status": "LIVE", "access_token": page_access_token},
        timeout=30,
    )
    if not resp.ok:
        return json.dumps({"error": resp.json().get("error", resp.text[:300])})
    return json.dumps(resp.json())


@mcp.tool()
def meta_end_live_video(
    live_video_id: str,
    page_access_token: str,
) -> str:
    """End a live video broadcast.

    Args:
        live_video_id: The live video ID.
        page_access_token: Page-level access token.
    """
    if (err := require_editor("meta_end_live_video")): return err
    resp = requests.post(
        f"{GRAPH_BASE}/{live_video_id}",
        json={"end_live_video": True, "access_token": page_access_token},
        timeout=30,
    )
    if not resp.ok:
        return json.dumps({"error": resp.json().get("error", resp.text[:300])})
    return json.dumps(resp.json())


# ---------------------------------------------------------------------------
# CATALOG PRODUCT FEED MANAGEMENT
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_get_product_feeds(catalog_id: str) -> str:
    """List all product feeds in a catalog.

    Args:
        catalog_id: Meta product catalog ID.
    """
    return json.dumps(_get(f"{catalog_id}/product_feeds", {
        "fields": "id,name,schedule,ingestion_stats,latest_upload"
    }))


@mcp.tool()
def meta_create_product_feed(
    catalog_id: str,
    name: str,
    feed_url: str,
    schedule: str = "",
    update_schedule: str = "",
) -> str:
    """Create a product feed (data source) for a catalog.

    Args:
        catalog_id: Meta product catalog ID.
        name: Feed name.
        feed_url: Public URL of the CSV/TSV/XML product feed.
        schedule: Optional JSON for recurring ingestion schedule.
                  Example: '{"interval":"DAILY","url":"https://...","hour":"6"}'
        update_schedule: Optional JSON for incremental update schedule.
    """
    if (err := require_editor("meta_create_product_feed")): return err
    payload: dict = {"name": name}
    if schedule:
        try:
            payload["schedule"] = json.loads(schedule)
        except Exception:
            payload["schedule"] = {"interval": "DAILY", "url": feed_url, "hour": "6"}
    else:
        # Default: daily fetch
        payload["schedule"] = {"interval": "DAILY", "url": feed_url, "hour": "6"}

    if update_schedule:
        try:
            payload["update_schedule"] = json.loads(update_schedule)
        except Exception:
            pass

    return json.dumps(_post(f"{catalog_id}/product_feeds", payload))


@mcp.tool()
def meta_upload_product_feed(
    feed_id: str,
    feed_url: str,
) -> str:
    """Trigger an immediate upload/ingestion of a product feed.

    Args:
        feed_id: Product feed ID (from meta_get_product_feeds).
        feed_url: Public URL to fetch the feed from.
    """
    if (err := require_editor("meta_upload_product_feed")): return err
    return json.dumps(_post(f"{feed_id}/uploads", {"url": feed_url}))


@mcp.tool()
def meta_update_catalog_product(
    catalog_id: str,
    retailer_id: str,
    updates: str,
) -> str:
    """Update a single product in a catalog by retailer ID.

    Args:
        catalog_id: Meta product catalog ID.
        retailer_id: The product's retailer_id (your internal SKU / product ID).
        updates: JSON object with fields to update. Common fields:
                 name, description, price (in cents, e.g. 9900 for $99),
                 sale_price, availability (in stock/out of stock),
                 url, image_url, condition (new/refurbished/used).
                 Example: '{"price":9900,"availability":"in stock"}'
    """
    if (err := require_editor("meta_update_catalog_product")): return err
    try:
        upd = json.loads(updates)
    except Exception as e:
        return json.dumps({"error": f"updates must be valid JSON: {e}"})
    return json.dumps(_post(f"{catalog_id}/items_batch", {
        "requests": [{"method": "UPDATE", "retailer_id": retailer_id, "data": upd}]
    }))


# ---------------------------------------------------------------------------
# PAGE CTA BUTTON
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_set_page_cta(
    page_id: str,
    page_access_token: str,
    cta_type: str,
    cta_value: str = "",
) -> str:
    """Set or update the call-to-action button on a Facebook Page.

    Args:
        page_id: Facebook Page ID.
        page_access_token: Page-level access token.
        cta_type: CTA type. Options: CALL_NOW, CONTACT_US, SEND_MESSAGE, BOOK_NOW,
                  SHOP_NOW, SIGN_UP, WATCH_VIDEO, SEND_EMAIL, LEARN_MORE, GET_DIRECTIONS,
                  REQUEST_APPOINTMENT, GET_QUOTE, PLAY_GAME.
        cta_value: Associated value — phone number for CALL_NOW, URL for SHOP_NOW/SIGN_UP/etc.
                   For SEND_MESSAGE leave empty (opens Messenger).
    """
    if (err := require_editor("meta_set_page_cta")): return err
    payload: dict = {
        "type": cta_type,
        "access_token": page_access_token,
    }
    if cta_value:
        # Meta wraps value in a nested object based on CTA type
        if cta_type == "CALL_NOW":
            payload["value"] = {"phone_number": cta_value}
        elif cta_type == "SEND_EMAIL":
            payload["value"] = {"email": cta_value}
        else:
            payload["value"] = {"web_url": cta_value}

    resp = requests.post(
        f"{GRAPH_BASE}/{page_id}/call_to_actions",
        json=payload,
        timeout=30,
    )
    if not resp.ok:
        return json.dumps({"error": resp.json().get("error", resp.text[:300])})
    return json.dumps(resp.json())


@mcp.tool()
def meta_get_page_cta(page_id: str, page_access_token: str) -> str:
    """Get the current CTA button set on a Facebook Page.

    Args:
        page_id: Facebook Page ID.
        page_access_token: Page-level access token.
    """
    resp = requests.get(
        f"{GRAPH_BASE}/{page_id}/call_to_actions",
        params={"fields": "id,type,value", "access_token": page_access_token},
        timeout=30,
    )
    if not resp.ok:
        return json.dumps({"error": resp.json().get("error", resp.text[:300])})
    return json.dumps(resp.json())


# ---------------------------------------------------------------------------
# BUSINESS MANAGER SYSTEM USERS
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_list_system_users(business_id: str) -> str:
    """List all system users in a Business Manager.

    System users are non-human accounts used for API integrations and automation.

    Args:
        business_id: Business Manager ID.
    """
    return json.dumps(_get(f"{business_id}/system_users", {
        "fields": "id,name,role,created_time"
    }))


@mcp.tool()
def meta_create_system_user(
    business_id: str,
    name: str,
    role: str = "EMPLOYEE",
) -> str:
    """Create a system user in a Business Manager.

    Args:
        business_id: Business Manager ID.
        name: Display name for the system user.
        role: ADMIN or EMPLOYEE. Default: EMPLOYEE.
    """
    if (err := require_editor("meta_create_system_user")): return err
    return json.dumps(_post(f"{business_id}/system_users", {
        "name": name,
        "role": role,
    }))


@mcp.tool()
def meta_get_system_user_token(
    system_user_id: str,
    app_id: str,
    app_secret: str,
    scope: str = "ads_management,pages_manage_posts,pages_read_engagement",
) -> str:
    """Generate an access token for a system user.

    The token never expires (unless permissions are revoked). Ideal for automation.

    Args:
        system_user_id: System user ID.
        app_id: Facebook App ID.
        app_secret: Facebook App Secret.
        scope: Comma-separated permissions to request.
    """
    if (err := require_editor("meta_get_system_user_token")): return err
    return json.dumps(_post(f"{system_user_id}/access_tokens", {
        "business_app": app_id,
        "appsecret_proof": _sha256(f"{app_secret}"),  # simplified; production needs HMAC
        "scope": scope,
    }))


@mcp.tool()
def meta_assign_system_user_to_account(
    account_id: str,
    system_user_id: str,
    role: str = "ANALYST",
) -> str:
    """Assign a system user to an ad account with a specific role.

    Args:
        account_id: Ad account ID.
        system_user_id: System user ID.
        role: ADMIN, ADVERTISER, or ANALYST. Default: ANALYST.
    """
    if (err := require_editor("meta_assign_system_user_to_account")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    return json.dumps(_post(f"{acct}/users", {
        "user": system_user_id,
        "role": role,
    }))


# ---------------------------------------------------------------------------
# Saved Audiences & Enhanced Conversions
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_create_saved_audience(
    name: str,
    targeting: str,
    account_id: str = "",
) -> str:
    """Save a reusable targeting template (Saved Audience) in Meta Ads.

    Saved audiences can be reused across ad sets without re-entering targeting
    each time. Accessible in Ads Manager under Audiences > Saved Audiences.

    Args:
        name: Display name for the saved audience (required).
        targeting: Full targeting spec as JSON string. Example:
            '{"age_min":25,"age_max":45,"genders":[1],
              "geo_locations":{"countries":["IL"]},
              "interests":[{"id":"6003139266461","name":"Real estate"}]}'
        account_id: Ad account ID (e.g. act_123456). Leave blank for default.
    """
    if (err := require_editor("meta_create_saved_audience")): return err
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    try:
        tgt = json.loads(targeting) if isinstance(targeting, str) else targeting
    except Exception:
        return json.dumps({"error": "targeting must be valid JSON"})
    return json.dumps(_post(f"{acct}/saved_audiences", {
        "name": name,
        "targeting": json.dumps(tgt),
    }))


@mcp.tool()
def meta_upload_enhanced_conversions(
    pixel_id: str,
    events: str,
    upload_tag: str = "",
    account_id: str = "",
) -> str:
    """Upload offline or enhanced conversion data with hashed PII for Meta matching.

    Used for offline purchase matching, CRM uploads, and enhanced conversions.
    PII fields (email, phone, fn, ln) are auto-hashed with SHA-256 before sending.

    Args:
        pixel_id: Meta Pixel (or dataset) ID (required).
        events: JSON array of conversion events. Each event supports:
            - event_name: PURCHASE, LEAD, COMPLETE_REGISTRATION, etc. (required)
            - event_time: Unix timestamp (required)
            - value: Order value as float
            - currency: ISO currency code, e.g. ILS, USD
            - order_id: Unique order/transaction ID (for deduplication)
            - email: Customer email (will be SHA-256 hashed)
            - phone: Customer phone with country code (will be hashed)
            - fn: First name (will be hashed)
            - ln: Last name (will be hashed)
            - external_id: Your internal customer ID (will be hashed)
            - country: Two-letter country code (will be hashed)
            - city: City (will be hashed)
            - zip: Postal code (will be hashed)
          Example:
            '[{"event_name":"PURCHASE","event_time":1700000000,
               "value":250.0,"currency":"ILS","order_id":"ORD-001",
               "email":"customer@example.com","phone":"+972501234567"}]'
        upload_tag: Optional label to identify this upload batch in reporting.
        account_id: Ad account ID. Leave blank for default.
    """
    if (err := require_editor("meta_upload_enhanced_conversions")): return err
    import hashlib

    def _sha256(val: str) -> str:
        return hashlib.sha256(val.strip().lower().encode()).hexdigest()

    PII_FIELDS = {"email", "phone", "fn", "ln", "external_id", "country", "city", "zip"}

    try:
        raw_events = json.loads(events) if isinstance(events, str) else events
    except Exception:
        return json.dumps({"error": "events must be valid JSON array"})

    upload_data = []
    for ev in raw_events:
        event_name = ev.get("event_name", "")
        event_time = ev.get("event_time", "")
        if not event_name or not event_time:
            continue

        user_data: dict = {}
        for field in PII_FIELDS:
            if ev.get(field):
                user_data[field] = _sha256(str(ev[field]))

        custom_data: dict = {}
        for field in ("value", "currency", "order_id"):
            if ev.get(field) is not None:
                custom_data[field] = ev[field]

        entry: dict = {
            "event_name": event_name,
            "event_time": int(event_time),
            "user_data": user_data,
        }
        if custom_data:
            entry["custom_data"] = custom_data

        upload_data.append(entry)

    if not upload_data:
        return json.dumps({"error": "No valid events found after parsing."})

    payload: dict = {"data": json.dumps(upload_data)}
    if upload_tag:
        payload["upload_tag"] = upload_tag

    return json.dumps(_post(f"{pixel_id}/events", payload))


# ---------------------------------------------------------------------------
# Change History & Payment Diagnostics
# ---------------------------------------------------------------------------

# Mapping of Meta activity event_type codes to human-readable Hebrew descriptions
_ACTIVITY_EVENT_LABELS: dict = {
    # --- Campaign ---
    "create_campaign":                    "יצירת קמפיין",
    "delete_campaign":                    "מחיקת קמפיין",
    "update_campaign_budget":             "שינוי תקציב קמפיין",
    "update_campaign_name":               "שינוי שם קמפיין",
    "update_campaign_run_status":         "שינוי סטטוס קמפיין",
    "update_campaign_objective":          "שינוי מטרת קמפיין",
    "update_campaign_spend_cap":          "שינוי תקרת הוצאה",
    "update_campaign_start_time":         "שינוי תאריך התחלה (קמפיין)",
    "update_campaign_stop_time":          "שינוי תאריך סיום (קמפיין)",
    "update_campaign_bid_strategy":       "שינוי אסטרטגיית הצעות מחיר (קמפיין)",
    "update_campaign_daily_budget":       "שינוי תקציב יומי קמפיין",
    "update_campaign_lifetime_budget":    "שינוי תקציב לייפטיים קמפיין",
    # --- AdSet ---
    "create_adset":                       "יצירת אד-סט",
    "delete_adset":                       "מחיקת אד-סט",
    "update_adset_budget":                "שינוי תקציב אד-סט",
    "update_adset_run_status":            "שינוי סטטוס אד-סט",
    "update_adset_bidding":               "שינוי הצעת מחיר",
    "update_adset_targeting":             "שינוי קהל יעד",
    "update_adset_schedule":              "שינוי לוח זמנים",
    "update_adset_name":                  "שינוי שם אד-סט",
    "update_adset_optimization_goal":     "שינוי מטרת אופטימיזציה",
    "update_adset_billing_event":         "שינוי אירוע חיוב",
    "update_adset_promoted_object":       "שינוי אובייקט מקודם",
    "update_adset_pacing_type":           "שינוי סוג תזמון תקציב",
    "update_adset_start_time":            "שינוי תאריך התחלה (אד-סט)",
    "update_adset_end_time":              "שינוי תאריך סיום (אד-סט)",
    "update_adset_daily_budget":          "שינוי תקציב יומי אד-סט",
    "update_adset_lifetime_budget":       "שינוי תקציב לייפטיים אד-סט",
    "update_adset_bid_amount":            "שינוי סכום הצעת מחיר",
    "update_adset_bid_strategy":          "שינוי אסטרטגיית הצעות (אד-סט)",
    "update_adset_attribution_spec":      "שינוי הגדרות attribution",
    "update_adset_frequency_control":     "שינוי הגבלת תדירות",
    "update_adset_destination_type":      "שינוי יעד מודעה",
    # --- Ad ---
    "create_ad":                          "יצירת מודעה",
    "delete_ad":                          "מחיקת מודעה",
    "update_ad_creative":                 "שינוי קריאייטיב",
    "update_ad_run_status":               "שינוי סטטוס מודעה",
    "update_ad_name":                     "שינוי שם מודעה",
    "update_ad_bid_amount":               "שינוי הצעת מחיר (מודעה)",
    "update_ad_tracking_specs":           "שינוי פיקסל / מעקב",
    "update_ad_conversion_specs":         "שינוי הגדרות המרה",
    # --- Creative / Asset ---
    "update_creative":                    "עדכון קריאייטיב",
    "create_creative":                    "יצירת קריאייטיב",
    "delete_creative":                    "מחיקת קריאייטיב",
    # --- Account / Billing ---
    "update_account_spend_limit":         "שינוי תקרת הוצאה (חשבון)",
    "update_billing_limit":               "שינוי מגבלת חיוב",
    "update_payment_method":              "עדכון אמצעי תשלום",
    "remove_payment_method":              "הסרת אמצעי תשלום",
}

_ACCOUNT_STATUS_LABELS: dict = {
    1:   ("ACTIVE", "תקין", "ok"),
    2:   ("DISABLED", "מושבת", "critical"),
    3:   ("UNSETTLED", "חוב פתוח — שגיאת תשלום", "critical"),
    7:   ("PENDING_RISK_REVIEW", "בבדיקת סיכון", "warning"),
    9:   ("IN_GRACE_PERIOD", "תקופת חסד — בקרוב יחסם", "warning"),
    100: ("PENDING_CLOSURE", "ממתין לסגירה", "warning"),
    101: ("CLOSED", "חשבון סגור / חסום בגלל חוב", "critical"),
    201: ("ANY_ACTIVE", "פעיל", "ok"),
    202: ("ANY_CLOSED", "סגור", "critical"),
}

_PAYMENT_STATUS_LABELS: dict = {
    "ACTIVE":   ("תקין", "ok"),
    "DECLINED": ("נדחה — שגיאת תשלום!", "critical"),
    "EXPIRED":  ("פג תוקף", "warning"),
    "INVALID":  ("לא תקין", "critical"),
    "PENDING":  ("ממתין לאישור", "warning"),
}


def _fetch_all_activities(account_id: str, object_id: str, object_type: str, since: str, until: str) -> list:
    """Paginate through all activities via the account-level /activities endpoint.

    Meta removed per-object /activities in newer API versions.
    Correct endpoint: GET /act_{account_id}/activities
    Filter by oid (object ID) and object_type (CAMPAIGN, ADSET, AD).
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    fields = (
        "actor_id,actor_name,event_time,event_type,object_id,object_name,"
        "translated_event_types,extra_data,date_time_in_timezone"
    )
    params: dict = {
        "fields": fields,
        "since": since,
        "until": until,
        "limit": 100,
    }
    if object_id:
        params["oid"] = object_id
    if object_type:
        params["object_type"] = object_type

    all_items: list = []
    endpoint = f"{acct}/activities"
    while True:
        result = _get(endpoint, params)
        if isinstance(result, dict) and "error" in result:
            return [result]
        data = result.get("data", [])
        all_items.extend(data)
        after = result.get("paging", {}).get("cursors", {}).get("after")
        if not after or not data:
            break
        params["after"] = after
    return all_items


def _format_activity(ev: dict) -> dict:
    """Convert a raw activity event into a readable dict."""
    raw_type = ev.get("event_type", "")
    # Use Hebrew label if exists; only fall back to translated_event_types when unknown
    hebrew_label = _ACTIVITY_EVENT_LABELS.get(raw_type)
    if hebrew_label:
        action_label = hebrew_label
    else:
        translated = ev.get("translated_event_types", [])
        if translated:
            action_label = translated[0] if isinstance(translated, list) else str(translated)
        else:
            action_label = raw_type  # last resort: raw event_type code

    extra = ev.get("extra_data", "")
    from_val, to_val = "", ""
    if extra:
        try:
            extra_dict = json.loads(extra) if isinstance(extra, str) else extra
            if isinstance(extra_dict, dict):
                # Try multiple known key patterns Meta uses
                from_val = str(
                    extra_dict.get("OLD_VALUE",
                    extra_dict.get("old_value",
                    extra_dict.get("PREVIOUS_VALUE",
                    extra_dict.get("previous_value", ""))))
                )
                to_val = str(
                    extra_dict.get("NEW_VALUE",
                    extra_dict.get("new_value",
                    extra_dict.get("CURRENT_VALUE",
                    extra_dict.get("current_value", ""))))
                )
                # If still empty, serialize the whole dict so nothing is lost
                if not from_val and not to_val:
                    to_val = json.dumps(extra_dict, ensure_ascii=False)
            else:
                to_val = str(extra_dict)
        except Exception:
            to_val = str(extra)

    return {
        "date": ev.get("date_time_in_timezone", ev.get("event_time", "")),
        "user": ev.get("actor_name", ev.get("actor_id", "לא ידוע")),
        "action": action_label,
        "event_type": raw_type,
        "from": from_val,
        "to": to_val,
        "object_name": ev.get("object_name", ""),
        "object_id": ev.get("object_id", ""),
    }


@mcp.tool()
def meta_get_campaign_change_history(
    campaign_id: str,
    account_id: str = "",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Retrieve the full change history of a Meta Ads campaign.

    Shows who changed what and when — budget, status, name, bids, and more.
    Uses the account-level /activities endpoint (correct for API v22+) filtered
    by campaign ID and object_type=CAMPAIGN.

    Args:
        campaign_id: The campaign ID to inspect (required).
        account_id: Ad account ID (e.g. act_123456 or just 123456).
                    Leave blank to use the default account from env.
        start_date: Start date YYYY-MM-DD. Default: 30 days ago.
        end_date: End date YYYY-MM-DD. Default: today.

    Returns JSON with campaign_name and changes list sorted newest-first.
    Each change: date, user, action, from, to, object_name.
    """
    import datetime as _dt
    today = _dt.date.today()
    since = start_date or str(today - _dt.timedelta(days=30))
    until = end_date or str(today)

    meta_info = _get(campaign_id, {"fields": "name"})
    campaign_name = meta_info.get("name", campaign_id) if isinstance(meta_info, dict) else campaign_id

    activities = _fetch_all_activities(account_id, campaign_id, "CAMPAIGN", since, until)

    if activities and isinstance(activities[0], dict) and "error" in activities[0]:
        return json.dumps({"error": activities[0], "campaign_id": campaign_id})

    changes = [_format_activity(ev) for ev in activities]
    changes.sort(key=lambda x: x.get("date", ""), reverse=True)

    return json.dumps({
        "campaign_id": campaign_id,
        "campaign_name": campaign_name,
        "period": {"from": since, "to": until},
        "total_changes": len(changes),
        "changes": changes,
    }, ensure_ascii=False)


@mcp.tool()
def meta_get_adset_change_history(
    adset_id: str,
    account_id: str = "",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Retrieve the full change history of a Meta Ads ad set.

    Shows who changed what and when — budget, targeting, status, bidding, and more.
    Uses the account-level /activities endpoint (correct for API v22+) filtered
    by ad set ID and object_type=ADSET.

    Args:
        adset_id: The ad set ID to inspect (required).
        account_id: Ad account ID (e.g. act_123456 or just 123456).
                    Leave blank to use the default account from env.
        start_date: Start date YYYY-MM-DD. Default: 30 days ago.
        end_date: End date YYYY-MM-DD. Default: today.

    Returns JSON with adset_name and changes list sorted newest-first.
    Each change: date, user, action, from, to, object_name.
    """
    import datetime as _dt
    today = _dt.date.today()
    since = start_date or str(today - _dt.timedelta(days=30))
    until = end_date or str(today)

    meta_info = _get(adset_id, {"fields": "name"})
    adset_name = meta_info.get("name", adset_id) if isinstance(meta_info, dict) else adset_id

    activities = _fetch_all_activities(account_id, adset_id, "ADSET", since, until)

    if activities and isinstance(activities[0], dict) and "error" in activities[0]:
        return json.dumps({"error": activities[0], "adset_id": adset_id})

    changes = [_format_activity(ev) for ev in activities]
    changes.sort(key=lambda x: x.get("date", ""), reverse=True)

    return json.dumps({
        "adset_id": adset_id,
        "adset_name": adset_name,
        "period": {"from": since, "to": until},
        "total_changes": len(changes),
        "changes": changes,
    }, ensure_ascii=False)


@mcp.tool()
def meta_get_ad_change_history(
    ad_id: str,
    account_id: str = "",
    start_date: str = "",
    end_date: str = "",
) -> str:
    """Retrieve the full change history of a specific Meta Ads ad (creative level).

    Shows who changed what and when — creative, status, tracking, bids, and more.
    Uses the account-level /activities endpoint filtered by ad ID and object_type=AD.

    Args:
        ad_id: The ad ID to inspect (required).
        account_id: Ad account ID (e.g. act_123456 or just 123456).
                    Leave blank to use the default account from env.
        start_date: Start date YYYY-MM-DD. Default: 30 days ago.
        end_date: End date YYYY-MM-DD. Default: today.

    Returns JSON with ad_name and changes list sorted newest-first.
    Each change: date, user, action, event_type, from, to, object_name, object_id.
    """
    import datetime as _dt
    today = _dt.date.today()
    since = start_date or str(today - _dt.timedelta(days=30))
    until = end_date or str(today)

    meta_info = _get(ad_id, {"fields": "name"})
    ad_name = meta_info.get("name", ad_id) if isinstance(meta_info, dict) else ad_id

    activities = _fetch_all_activities(account_id, ad_id, "AD", since, until)

    if activities and isinstance(activities[0], dict) and "error" in activities[0]:
        return json.dumps({"error": activities[0], "ad_id": ad_id})

    changes = [_format_activity(ev) for ev in activities]
    changes.sort(key=lambda x: x.get("date", ""), reverse=True)

    return json.dumps({
        "ad_id": ad_id,
        "ad_name": ad_name,
        "period": {"from": since, "to": until},
        "total_changes": len(changes),
        "changes": changes,
    }, ensure_ascii=False)


@mcp.tool()
def meta_get_account_change_history(
    account_id: str = "",
    start_date: str = "",
    end_date: str = "",
    object_type: str = "",
    limit: int = 500,
) -> str:
    """Retrieve the full change history across the ENTIRE Meta Ads account in one call.

    The fastest way to audit what changed, who changed it, and when —
    across all campaigns, ad sets, and ads simultaneously.
    No need to know specific IDs in advance.

    Args:
        account_id: Ad account ID (e.g. act_123456 or just 123456).
                    Leave blank to use the default account from env.
        start_date: Start date YYYY-MM-DD. Default: 7 days ago.
        end_date: End date YYYY-MM-DD. Default: today.
        object_type: Optional filter — CAMPAIGN, ADSET, or AD.
                     Leave blank to return ALL object types.
        limit: Max number of changes to return (default 500, max 2000).

    Returns JSON with:
      - period: date range queried
      - total_changes: total count
      - by_user: summary of changes per user (name → count)
      - by_type: summary of changes per event type (Hebrew label → count)
      - changes: full list sorted newest-first, each entry has:
          date, user, action, event_type, from, to, object_name, object_id
    """
    import datetime as _dt
    today = _dt.date.today()
    since = start_date or str(today - _dt.timedelta(days=7))
    until = end_date or str(today)
    cap = min(max(1, limit), 2000)

    activities = _fetch_all_activities(account_id, "", object_type.upper() if object_type else "", since, until)

    if activities and isinstance(activities[0], dict) and "error" in activities[0]:
        return json.dumps({"error": activities[0]})

    changes = [_format_activity(ev) for ev in activities]
    changes.sort(key=lambda x: x.get("date", ""), reverse=True)
    changes = changes[:cap]

    # Build summary aggregates
    by_user: dict = {}
    by_type: dict = {}
    for ch in changes:
        u = ch.get("user", "לא ידוע")
        a = ch.get("action", "")
        by_user[u] = by_user.get(u, 0) + 1
        by_type[a] = by_type.get(a, 0) + 1

    # Sort summaries by count descending
    by_user = dict(sorted(by_user.items(), key=lambda x: x[1], reverse=True))
    by_type = dict(sorted(by_type.items(), key=lambda x: x[1], reverse=True))

    return json.dumps({
        "period": {"from": since, "to": until},
        "object_type_filter": object_type.upper() if object_type else "ALL",
        "total_changes": len(changes),
        "by_user": by_user,
        "by_type": by_type,
        "changes": changes,
    }, ensure_ascii=False)


@mcp.tool()
def meta_check_payment_errors(
    account_id: str = "",
) -> str:
    """Check if a Meta Ads account has any active payment errors or billing issues.

    Inspects account status codes and payment method statuses to detect:
    - Unpaid debt (UNSETTLED / status 3)
    - Declined payment methods
    - Expired cards
    - Grace period warnings
    - Blocked accounts due to non-payment (status 101)

    Args:
        account_id: Ad account ID. Leave blank to use the default account.

    Returns a structured JSON with has_payment_error, severity (ok/warning/critical),
    account_status details, payment methods list, and a recommendation in Hebrew.
    """
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()

    # Step A: account info
    acct_data = _get(acct, {
        "fields": "account_status,disable_reason,funding_source_details,balance,currency,spend_cap,name"
    })
    if isinstance(acct_data, dict) and "error" in acct_data:
        return json.dumps({"error": acct_data, "account_id": acct})

    status_code = acct_data.get("account_status", 1)
    status_en, status_he, severity = _ACCOUNT_STATUS_LABELS.get(
        status_code, (str(status_code), "לא ידוע", "warning")
    )

    # Step B: payment methods
    pm_data = _get(f"{acct}/payment_methods", {"fields": "type,status,display_string,is_primary"})
    raw_methods = pm_data.get("data", []) if isinstance(pm_data, dict) else []

    payment_methods = []
    worst_pm_severity = "ok"
    for pm in raw_methods:
        pm_status = pm.get("status", "")
        pm_label, pm_sev = _PAYMENT_STATUS_LABELS.get(pm_status, (pm_status, "warning"))
        if pm_sev == "critical":
            worst_pm_severity = "critical"
        elif pm_sev == "warning" and worst_pm_severity == "ok":
            worst_pm_severity = "warning"
        payment_methods.append({
            "type": pm.get("type", ""),
            "display": pm.get("display_string", ""),
            "is_primary": pm.get("is_primary", False),
            "status": pm_status,
            "status_he": pm_label,
            "severity": pm_sev,
        })

    # Determine overall severity
    overall_severity = severity
    if worst_pm_severity == "critical" and overall_severity != "critical":
        overall_severity = "critical"
    elif worst_pm_severity == "warning" and overall_severity == "ok":
        overall_severity = "warning"

    has_error = overall_severity in ("critical", "warning")

    # Build recommendation
    if overall_severity == "critical":
        recommendation = "פעולה דחופה נדרשת: יש לעדכן אמצעי תשלום בדחיפות ולסלק חוב פתוח."
    elif overall_severity == "warning":
        recommendation = "יש לבדוק את אמצעי התשלום — החשבון עלול להיחסם בקרוב."
    else:
        recommendation = "החשבון תקין — אין בעיות תשלום."

    return json.dumps({
        "account_id": acct,
        "account_name": acct_data.get("name", ""),
        "has_payment_error": has_error,
        "severity": overall_severity,
        "account_status_code": status_code,
        "account_status_en": status_en,
        "account_status_he": status_he,
        "disable_reason": acct_data.get("disable_reason", ""),
        "balance": acct_data.get("balance", ""),
        "currency": acct_data.get("currency", ""),
        "payment_methods": payment_methods,
        "recommendation": recommendation,
    }, ensure_ascii=False)


@mcp.tool()
def meta_bulk_payment_check(
    account_ids: str,
) -> str:
    """Check payment errors across multiple Meta Ads accounts at once.

    Runs meta_check_payment_errors on each account with a 0.5s delay between
    calls to respect rate limits. Returns a summary of all issues found.

    Args:
        account_ids: Comma-separated list of ad account IDs.
                     Example: "act_111,act_222,act_333" or "111,222,333"

    Returns a JSON summary with total checked, number of issues found,
    and a list of accounts with problems (severity + recommendation).
    """
    ids = [a.strip() for a in account_ids.split(",") if a.strip()]
    if not ids:
        return json.dumps({"error": "No account IDs provided."})

    results: list = []
    errors_found = 0

    for acct in ids:
        try:
            raw = meta_check_payment_errors(account_id=acct)
            data = json.loads(raw)
        except Exception as exc:
            data = {"account_id": acct, "has_payment_error": True, "severity": "critical",
                    "recommendation": f"שגיאה בבדיקה: {exc}"}

        if data.get("has_payment_error"):
            errors_found += 1
            results.append({
                "account_id": data.get("account_id", acct),
                "account_name": data.get("account_name", ""),
                "severity": data.get("severity", ""),
                "account_status_he": data.get("account_status_he", ""),
                "issue": data.get("recommendation", ""),
                "payment_methods": [
                    pm for pm in data.get("payment_methods", [])
                    if pm.get("severity") != "ok"
                ],
            })
        time.sleep(0.5)

    return json.dumps({
        "checked": len(ids),
        "errors_found": errors_found,
        "all_clear": errors_found == 0,
        "accounts_with_issues": results,
        "summary": f"נבדקו {len(ids)} חשבונות — נמצאו {errors_found} עם בעיות תשלום.",
    }, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Account Search
# ---------------------------------------------------------------------------

@mcp.tool()
def meta_find_account(
    query: str,
    user_id: str = "me",
    business_id: str = "",
) -> str:
    """Find a Meta Ads account by partial name, ID fragment, or currency.

    Searches across ALL accounts (130+) using fuzzy partial matching.
    Checks user accounts and optionally business accounts.
    Case-insensitive. Hebrew, English, and mixed names all supported.

    Examples of valid queries:
      - "רוקט"        → matches "Rocket Digital | רוקט"
      - "ynet"        → matches "Ynet – News"
      - "269"         → matches account IDs containing "269"
      - "ILS"         → all ILS-currency accounts
      - "רמי לוי"     → matches partial client name

    Args:
        query: Any partial string to match against account name or ID (required).
        user_id: Facebook user ID. Default: "me".
        business_id: Optional Business Manager ID to also search business accounts.

    Returns a list of matching accounts with id, name, status, and currency,
    plus the total count of matches.
    """
    q = query.strip().lower()
    if not q:
        return json.dumps({"error": "query is required"})

    # --- collect from user adaccounts (paginated) ---
    all_accounts: list = []
    params: dict = {"fields": "id,name,account_status,currency,timezone_name", "limit": 200}
    endpoint = f"{user_id}/adaccounts"
    while True:
        result = _get(endpoint, params)
        if isinstance(result, dict) and "error" in result:
            break
        data = result.get("data", [])
        all_accounts.extend(data)
        after = result.get("paging", {}).get("cursors", {}).get("after")
        if not after or not data:
            break
        params["after"] = after

    # --- optionally collect from business accounts (paginated) ---
    if business_id:
        biz_params: dict = {"fields": "id,name,account_status,currency,timezone_name", "limit": 200}
        biz_endpoint = f"{business_id}/owned_ad_accounts"
        while True:
            biz_result = _get(biz_endpoint, biz_params)
            if isinstance(biz_result, dict) and "error" in biz_result:
                break
            biz_data = biz_result.get("data", [])
            # deduplicate by id
            existing_ids = {a["id"] for a in all_accounts}
            for acct in biz_data:
                if acct.get("id") not in existing_ids:
                    all_accounts.append(acct)
                    existing_ids.add(acct["id"])
            after = biz_result.get("paging", {}).get("cursors", {}).get("after")
            if not after or not biz_data:
                break
            biz_params["after"] = after

    # --- score and filter ---
    STATUS_LABELS = {1: "פעיל", 2: "מושבת", 3: "חוב פתוח", 7: "בדיקת סיכון",
                     9: "תקופת חסד", 101: "חסום"}

    scored: list = []
    for acct in all_accounts:
        name = acct.get("name", "")
        acct_id = acct.get("id", "")
        currency = acct.get("currency", "")
        search_blob = f"{name} {acct_id} {currency}".lower()

        if q not in search_blob:
            continue

        # score: exact name match > starts with > contains
        name_lower = name.lower()
        if name_lower == q:
            score = 100
        elif name_lower.startswith(q):
            score = 80
        elif q in name_lower:
            score = 60
        elif q in acct_id.lower():
            score = 40
        else:
            score = 20

        status_code = acct.get("account_status", 1)
        scored.append({
            "id": acct_id,
            "name": name,
            "status_code": status_code,
            "status": STATUS_LABELS.get(status_code, str(status_code)),
            "currency": currency,
            "timezone": acct.get("timezone_name", ""),
            "_score": score,
        })

    scored.sort(key=lambda x: x["_score"], reverse=True)
    # remove internal score field
    for r in scored:
        del r["_score"]

    if not scored:
        return json.dumps({
            "matches": [],
            "total": 0,
            "message": f"לא נמצאו חשבונות עבור החיפוש: '{query}'. בדוק את האיות או נסה מילה קצרה יותר.",
        }, ensure_ascii=False)

    return json.dumps({
        "matches": scored,
        "total": len(scored),
        "searched_total_accounts": len(all_accounts),
    }, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Dashboard Generator
# ---------------------------------------------------------------------------

@mcp.tool()
def generate_meta_dashboard(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
    dashboard_title: str = "",
) -> str:
    """Generate a complete, beautiful Hebrew RTL Meta Ads dashboard as an HTML artifact.

    Fetches overview, campaigns, daily trend, demographics, and placements data,
    then returns a fully self-contained HTML page (zero external dependencies —
    all charts drawn with pure Canvas API so they work inside Claude's sandbox).
    Claude should display the returned HTML as an artifact.

    Args:
        start_date: Start date YYYY-MM-DD. Defaults to 28 days ago.
        end_date: End date YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        dashboard_title: Custom dashboard title in Hebrew. Optional.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    time_range = json.dumps({"since": sd, "until": ed})

    # ── Fetch all data ────────────────────────────────────────────────────

    def safe(result):
        if isinstance(result, dict) and "error" in result:
            return []
        return result.get("data", [])

    overview_raw   = _get(f"{acct}/insights", {"time_range": time_range, "fields": "impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions"})
    campaigns_raw  = _get(f"{acct}/insights", {"time_range": time_range, "level": "campaign", "fields": "campaign_name,impressions,clicks,spend,ctr,cpc,actions", "limit": 10, "sort": json.dumps(["spend_descending"]), "effective_status": json.dumps(["ACTIVE", "PAUSED"])})
    daily_raw      = _get(f"{acct}/insights", {"time_range": time_range, "time_increment": 1, "fields": "date_start,impressions,clicks,spend,ctr", "limit": 90})
    demo_raw       = _get(f"{acct}/insights", {"time_range": time_range, "breakdowns": "age,gender", "fields": "age,gender,impressions,clicks,spend,reach", "limit": 60})
    placement_raw  = _get(f"{acct}/insights", {"time_range": time_range, "breakdowns": "publisher_platform,platform_position", "fields": "publisher_platform,platform_position,impressions,clicks,spend", "limit": 30})

    # ── Process overview ──────────────────────────────────────────────────

    ov_list = safe(overview_raw)
    ov = ov_list[0] if ov_list else {}

    total_spend       = float(ov.get("spend", 0))
    total_impressions = int(ov.get("impressions", 0))
    total_clicks      = int(ov.get("clicks", 0))
    total_reach       = int(ov.get("reach", 0))
    ctr_val           = float(ov.get("ctr", 0))
    cpc_val           = float(ov.get("cpc", 0))
    cpm_val           = float(ov.get("cpm", 0))
    freq_val          = float(ov.get("frequency", 0))

    actions_list = ov.get("actions", [])
    purchases = sum(float(a.get("value", 0)) for a in actions_list if a.get("action_type") in ("purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase"))
    roas_val = round(purchases / total_spend, 2) if total_spend > 0 else 0.0

    def fmt(n, dec=0, prefix=""):
        try:
            v = float(n)
            if dec:
                return f"{prefix}{v:,.{dec}f}"
            return f"{prefix}{int(v):,}"
        except Exception:
            return "0"

    def fmt_date_he(d_str):
        try:
            from datetime import datetime as _dt
            return _dt.strptime(d_str, "%Y-%m-%d").strftime("%d/%m")
        except Exception:
            return d_str

    # ── Process campaigns ─────────────────────────────────────────────────

    campaigns = safe(campaigns_raw)[:10]
    camp_names   = json.dumps([c.get("campaign_name", "")[:35] for c in campaigns], ensure_ascii=False)
    camp_spend   = json.dumps([float(c.get("spend", 0)) for c in campaigns])
    camp_clicks  = json.dumps([int(c.get("clicks", 0)) for c in campaigns])

    # ── Process daily trend ───────────────────────────────────────────────

    daily = safe(daily_raw)
    daily_dates  = json.dumps([fmt_date_he(d.get("date_start", "")) for d in daily], ensure_ascii=False)
    daily_spend  = json.dumps([float(d.get("spend", 0)) for d in daily])
    daily_impr   = json.dumps([int(d.get("impressions", 0)) for d in daily])
    daily_clicks = json.dumps([int(d.get("clicks", 0)) for d in daily])

    # ── Process demographics ──────────────────────────────────────────────

    demo = safe(demo_raw)
    gender_map = {"male": "גברים", "female": "נשים", "unknown": "לא ידוע"}
    age_groups = sorted(set(d.get("age", "") for d in demo if d.get("age")))
    genders    = sorted(set(d.get("gender", "") for d in demo if d.get("gender")))

    demo_lookup = {f"{d.get('age')}_{d.get('gender')}": float(d.get("spend", 0)) for d in demo}
    demo_age_labels = json.dumps(age_groups, ensure_ascii=False)
    demo_datasets_list = []
    demo_colors = ["rgba(99,102,241,0.85)", "rgba(236,72,153,0.85)", "rgba(156,163,175,0.7)"]
    for i, g in enumerate(genders):
        pts = [demo_lookup.get(f"{age}_{g}", 0) for age in age_groups]
        demo_datasets_list.append({"label": gender_map.get(g, g), "data": pts, "backgroundColor": demo_colors[i % len(demo_colors)], "borderRadius": 6})
    demo_datasets = json.dumps(demo_datasets_list, ensure_ascii=False)

    # ── Process placements ────────────────────────────────────────────────

    plat_label_map = {"facebook": "פייסבוק", "instagram": "אינסטגרם", "audience_network": "Audience Network", "messenger": "מסנג׳ר"}
    plat_totals: dict = {}
    for p in safe(placement_raw):
        lbl = plat_label_map.get(p.get("publisher_platform", ""), p.get("publisher_platform", ""))
        plat_totals[lbl] = plat_totals.get(lbl, 0) + float(p.get("spend", 0))
    plat_labels = json.dumps(list(plat_totals.keys()), ensure_ascii=False)
    plat_values = json.dumps([round(v, 2) for v in plat_totals.values()])

    # ── Meta ──────────────────────────────────────────────────────────────

    title = dashboard_title or "דשבורד Meta Ads"
    subtitle = f"{sd} — {ed}"

    # ── Self-contained Canvas chart library (no CDN) ──────────────────────
    # Written as a plain string so we never need to escape JS braces inside
    # an f-string — only the outer html f-string uses {{ }} for CSS rules.
    js_charts = r"""
const DPR = window.devicePixelRatio || 1;
const COLORS = ['#7c3aed','#6366f1','#ec4899','#10b981','#f59e0b','#3b82f6','#06b6d4'];
const GRID_C = 'rgba(48,54,61,0.8)';
const MUTED_C = '#8b949e';
const TEXT_C = '#e6edf3';

function numFmt(n) {
  n = Math.abs(+n);
  if (n >= 1e6)  return (n/1e6).toFixed(1) + 'M';
  if (n >= 1000) return (n/1000).toFixed(0) + 'K';
  return n.toFixed(0);
}

function initCanvas(id, H) {
  const el = document.getElementById(id);
  if (!el) return null;
  const W = el.parentElement.clientWidth - 2;
  el.style.width = W + 'px';
  el.style.height = H + 'px';
  el.width  = Math.round(W * DPR);
  el.height = Math.round(H * DPR);
  const ctx = el.getContext('2d');
  ctx.scale(DPR, DPR);
  return { ctx, W, H };
}

/* ── Line chart with dual Y axes ── */
function drawLine(id, labels, datasets, H) {
  H = H || 300;
  const r = initCanvas(id, H);
  if (!r) return;
  const { ctx, W } = r;
  const pT=28, pB=44, pL=64, pR=54;
  const cW = W - pL - pR, cH = H - pT - pB;

  const maxSpend  = Math.max(...(datasets[0]||{data:[]}).data, 1);
  const maxClicks = datasets[1] ? Math.max(...datasets[1].data, 1) : 1;
  const step = cW / Math.max(labels.length - 1, 1);

  /* grid + Y labels */
  for (let i = 0; i <= 5; i++) {
    const y = pT + (cH / 5) * i;
    ctx.beginPath(); ctx.strokeStyle = GRID_C; ctx.lineWidth = 1;
    ctx.moveTo(pL, y); ctx.lineTo(pL + cW, y); ctx.stroke();
    ctx.fillStyle = MUTED_C; ctx.font = '10px system-ui';
    ctx.textAlign = 'right';
    ctx.fillText('₪' + numFmt(maxSpend - (maxSpend/5)*i), pL - 4, y + 4);
    if (datasets[1]) {
      ctx.textAlign = 'left';
      ctx.fillText(numFmt(maxClicks - (maxClicks/5)*i), pL + cW + 4, y + 4);
    }
  }

  /* X labels */
  const evry = Math.ceil(labels.length / 14);
  ctx.fillStyle = MUTED_C; ctx.font = '10px system-ui'; ctx.textAlign = 'center';
  labels.forEach((l, i) => { if (i % evry === 0) ctx.fillText(l, pL + i*step, H - 6); });

  /* series */
  datasets.forEach((ds, di) => {
    const maxV = di === 0 ? maxSpend : maxClicks;
    const color = ds.color || COLORS[di];
    const pts = ds.data.map((v, i) => ({ x: pL + i*step, y: pT + cH - (v/maxV)*cH }));
    if (ds.fill && pts.length > 1) {
      ctx.beginPath();
      ctx.moveTo(pts[0].x, pts[0].y);
      pts.slice(1).forEach(p => ctx.lineTo(p.x, p.y));
      ctx.lineTo(pts[pts.length-1].x, pT+cH); ctx.lineTo(pts[0].x, pT+cH); ctx.closePath();
      ctx.globalAlpha = 0.13; ctx.fillStyle = color; ctx.fill(); ctx.globalAlpha = 1;
    }
    if (pts.length > 1) {
      ctx.beginPath(); ctx.strokeStyle = color; ctx.lineWidth = 2.5;
      ctx.lineJoin = 'round'; ctx.lineCap = 'round';
      ctx.moveTo(pts[0].x, pts[0].y);
      pts.slice(1).forEach(p => ctx.lineTo(p.x, p.y));
      ctx.stroke();
    }
  });

  /* legend */
  let lx = pL;
  datasets.forEach((ds, di) => {
    const color = ds.color || COLORS[di];
    ctx.fillStyle = color; ctx.fillRect(lx, H-14, 18, 4);
    ctx.fillStyle = MUTED_C; ctx.font = '11px system-ui'; ctx.textAlign = 'left';
    ctx.fillText(ds.label, lx + 22, H - 6);
    lx += ctx.measureText(ds.label).width + 50;
  });
}

/* ── Donut chart ── */
function drawDonut(id, labels, values, H) {
  H = H || 270;
  const r = initCanvas(id, H);
  if (!r) return;
  const { ctx, W } = r;
  const legH = Math.ceil(labels.length / 2) * 24 + 12;
  const chartH = H - legH;
  const cx = W/2, cy = chartH/2;
  const radius = Math.min(cx - 12, cy - 12);
  const inner  = radius * 0.58;
  const total  = values.reduce((a, b) => a + b, 0) || 1;

  let angle = -Math.PI / 2;
  values.forEach((v, i) => {
    const slice = (v / total) * Math.PI * 2;
    ctx.beginPath(); ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, radius, angle, angle + slice); ctx.closePath();
    ctx.fillStyle = COLORS[i % COLORS.length]; ctx.fill();
    ctx.strokeStyle = '#161b22'; ctx.lineWidth = 2; ctx.stroke();
    angle += slice;
  });

  ctx.beginPath(); ctx.arc(cx, cy, inner, 0, Math.PI*2);
  ctx.fillStyle = '#161b22'; ctx.fill();
  ctx.fillStyle = TEXT_C; ctx.font = 'bold 13px system-ui'; ctx.textAlign = 'center';
  ctx.fillText('₪' + numFmt(total), cx, cy + 5);
  ctx.fillStyle = MUTED_C; ctx.font = '10px system-ui';
  ctx.fillText('הוצאה כוללת', cx, cy + 20);

  labels.forEach((lbl, i) => {
    const col = i % 2, row = Math.floor(i / 2);
    const lx = col === 0 ? 10 : W/2 + 10;
    const ly = H - legH + row*24 + 20;
    ctx.fillStyle = COLORS[i % COLORS.length];
    ctx.beginPath(); ctx.arc(lx + 5, ly - 4, 5, 0, Math.PI*2); ctx.fill();
    ctx.fillStyle = MUTED_C; ctx.font = '11px system-ui'; ctx.textAlign = 'left';
    ctx.fillText(lbl + '  ' + ((values[i]/total)*100).toFixed(0) + '%', lx + 14, ly);
  });
}

/* ── Grouped vertical bar chart ── */
function drawGroupedBar(id, labels, datasets, H) {
  H = H || 280;
  const r = initCanvas(id, H);
  if (!r) return;
  const { ctx, W } = r;
  const pT=20, pB=52, pL=58, pR=16;
  const cW = W-pL-pR, cH = H-pT-pB;
  const allVals = datasets.flatMap(ds => ds.data);
  const maxV = Math.max(...allVals, 1) * 1.1;
  const groupW = cW / (labels.length || 1);
  const dsN = datasets.length;
  const barW = (groupW * 0.78) / dsN;
  const gPad = groupW * 0.11;

  for (let i = 0; i <= 5; i++) {
    const y = pT + (cH/5)*i;
    ctx.beginPath(); ctx.strokeStyle = GRID_C; ctx.lineWidth = 1;
    ctx.moveTo(pL, y); ctx.lineTo(pL+cW, y); ctx.stroke();
    ctx.fillStyle = MUTED_C; ctx.font = '10px system-ui'; ctx.textAlign = 'right';
    ctx.fillText('₪'+numFmt(maxV-(maxV/5)*i), pL-4, y+4);
  }

  labels.forEach((lbl, gi) => {
    const gX = pL + gi*groupW + gPad;
    datasets.forEach((ds, di) => {
      const v = ds.data[gi] || 0;
      const bH = (v/maxV)*cH;
      const bX = gX + di*barW, bY = pT+cH-bH;
      ctx.fillStyle = ds.backgroundColor || ds.color || COLORS[di % COLORS.length];
      ctx.beginPath();
      if (ctx.roundRect) { ctx.roundRect(bX, bY, barW-2, bH, [4,4,0,0]); }
      else { ctx.rect(bX, bY, barW-2, bH); }
      ctx.fill();
    });
    ctx.fillStyle = MUTED_C; ctx.font = '11px system-ui'; ctx.textAlign = 'center';
    ctx.fillText(lbl, pL + gi*groupW + groupW/2, H-pB+16);
  });

  let lx = pL;
  const ly = H - 14;
  datasets.forEach((ds, di) => {
    const color = ds.backgroundColor || ds.color || COLORS[di % COLORS.length];
    ctx.fillStyle = color; ctx.fillRect(lx, ly-10, 12, 10);
    ctx.fillStyle = MUTED_C; ctx.font = '11px system-ui'; ctx.textAlign = 'left';
    ctx.fillText(ds.label, lx+16, ly);
    lx += ctx.measureText(ds.label).width + 38;
  });
}

/* ── Campaign table (DOM) ── */
(function() {
  const maxSpend = Math.max(...CAMP_SPEND, 1);
  const table = document.getElementById('campTable');
  if (!table) return;
  if (!CAMP_NAMES.length) {
    table.innerHTML = '<tbody><tr><td colspan="3" style="color:#8b949e;text-align:center;padding:24px">אין נתוני קמפיינים</td></tr></tbody>';
    return;
  }
  table.innerHTML = '<thead><tr><th>שם קמפיין</th><th>הוצאה</th><th>קליקים</th></tr></thead><tbody></tbody>';
  const tbody = table.querySelector('tbody');
  CAMP_NAMES.forEach((name, i) => {
    const pct = Math.max(4, Math.round((CAMP_SPEND[i] / maxSpend) * 100));
    const tr = document.createElement('tr');
    tr.innerHTML =
      `<td style="max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${name}</td>` +
      `<td><div style="display:flex;align-items:center;gap:8px">` +
        `<div style="height:6px;width:${pct}px;min-width:4px;border-radius:3px;flex-shrink:0;background:linear-gradient(90deg,#7c3aed,#ec4899)"></div>` +
        `<span>₪${(+CAMP_SPEND[i]).toLocaleString()}</span></div></td>` +
      `<td>${(+CAMP_CLICKS[i]).toLocaleString()}</td>`;
    tbody.appendChild(tr);
  });
})();

/* ── Init ── */
document.addEventListener('DOMContentLoaded', function() {
  drawLine('dailyChart', DAILY_DATES, [
    { label: 'הוצאה (₪)', data: DAILY_SPEND,  color: '#7c3aed', fill: true },
    { label: 'קליקים',    data: DAILY_CLICKS, color: '#06b6d4' },
  ], 310);
  drawDonut('platChart',  PLAT_LABELS, PLAT_VALUES, 280);
  drawGroupedBar('demoChart', DEMO_AGE, DEMO_DS, 280);
});
"""

    # ── HTML ──────────────────────────────────────────────────────────────

    html = f"""<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{title}</title>
<style>
  :root {{
    --bg:       #0d1117;
    --surface:  #161b22;
    --surface2: #1c2330;
    --border:   #30363d;
    --text:     #e6edf3;
    --muted:    #8b949e;
    --indigo:   #7c3aed;
    --indigo2:  #6366f1;
    --pink:     #ec4899;
    --green:    #10b981;
    --amber:    #f59e0b;
    --blue:     #3b82f6;
    --cyan:     #06b6d4;
    --red:      #ef4444;
  }}
  *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{
    font-family: -apple-system, "Segoe UI", Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    direction: rtl;
    text-align: right;
    min-height: 100vh;
    padding: 0 0 48px;
  }}

  /* ── Header ── */
  .header {{
    background: linear-gradient(135deg, #1a1040 0%, #0d1117 60%);
    border-bottom: 1px solid var(--border);
    padding: 28px 36px 24px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    flex-wrap: wrap;
    gap: 12px;
  }}
  .header-left {{ display: flex; flex-direction: column; gap: 4px; }}
  .logo-row {{ display: flex; align-items: center; gap: 10px; }}
  .logo-dot {{
    width: 38px; height: 38px; border-radius: 10px;
    background: linear-gradient(135deg, var(--indigo), var(--pink));
    display: flex; align-items: center; justify-content: center;
    font-size: 20px;
  }}
  h1 {{ font-size: 1.65rem; font-weight: 700; letter-spacing: -0.5px; }}
  .subtitle {{ color: var(--muted); font-size: 0.88rem; margin-top: 2px; }}
  .date-badge {{
    background: var(--surface2); border: 1px solid var(--border);
    border-radius: 8px; padding: 6px 14px;
    font-size: 0.82rem; color: var(--muted);
  }}

  /* ── Layout ── */
  .container {{ max-width: 1400px; margin: 0 auto; padding: 28px 36px 0; }}
  .section-label {{
    font-size: 0.72rem; font-weight: 700; text-transform: uppercase;
    letter-spacing: 1px; color: var(--muted); margin-bottom: 14px;
  }}

  /* ── KPI cards ── */
  .kpi-grid {{
    display: grid;
    grid-template-columns: repeat(auto-fill, minmax(160px, 1fr));
    gap: 14px;
    margin-bottom: 28px;
  }}
  .kpi {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 20px 18px 16px;
    position: relative;
    overflow: hidden;
    transition: transform .15s, box-shadow .15s;
  }}
  .kpi:hover {{ transform: translateY(-2px); box-shadow: 0 8px 24px rgba(0,0,0,.35); }}
  .kpi::before {{
    content: '';
    position: absolute;
    top: 0; right: 0; left: 0;
    height: 3px;
    border-radius: 14px 14px 0 0;
  }}
  .kpi.indigo::before  {{ background: linear-gradient(90deg, var(--indigo), var(--indigo2)); }}
  .kpi.pink::before    {{ background: linear-gradient(90deg, var(--pink), #f472b6); }}
  .kpi.green::before   {{ background: linear-gradient(90deg, var(--green), #34d399); }}
  .kpi.amber::before   {{ background: linear-gradient(90deg, var(--amber), #fbbf24); }}
  .kpi.blue::before    {{ background: linear-gradient(90deg, var(--blue), #60a5fa); }}
  .kpi.cyan::before    {{ background: linear-gradient(90deg, var(--cyan), #22d3ee); }}
  .kpi.red::before     {{ background: linear-gradient(90deg, var(--red), #f87171); }}
  .kpi.violet::before  {{ background: linear-gradient(90deg, #8b5cf6, #a78bfa); }}
  .kpi-label {{ font-size: 0.78rem; color: var(--muted); margin-bottom: 8px; font-weight: 500; }}
  .kpi-value {{ font-size: 1.6rem; font-weight: 800; line-height: 1; letter-spacing: -1px; }}
  .kpi-sub {{ font-size: 0.75rem; color: var(--muted); margin-top: 6px; }}

  /* ── Chart cards ── */
  .charts-row {{
    display: grid;
    gap: 14px;
    margin-bottom: 14px;
  }}
  .charts-row.cols-1 {{ grid-template-columns: 1fr; }}
  .charts-row.cols-2 {{ grid-template-columns: 1fr 1fr; }}
  .charts-row.cols-3-1 {{ grid-template-columns: 2fr 1fr; }}
  @media (max-width: 900px) {{
    .charts-row.cols-2, .charts-row.cols-3-1 {{ grid-template-columns: 1fr; }}
    .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
    .container {{ padding: 16px 16px 0; }}
    .header {{ padding: 20px 16px 16px; }}
  }}
  .card {{
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    padding: 20px 22px;
  }}
  .card-title {{
    font-size: 0.92rem;
    font-weight: 700;
    margin-bottom: 18px;
    display: flex;
    align-items: center;
    gap: 8px;
  }}
  .card-title .icon {{
    width: 28px; height: 28px; border-radius: 7px;
    display: flex; align-items: center; justify-content: center;
    font-size: 14px; flex-shrink: 0;
  }}
  .chart-wrap {{ position: relative; }}
  .chart-wrap canvas {{ max-height: 280px; }}
  .chart-wrap-tall canvas {{ max-height: 320px; }}

  /* ── Table ── */
  .table-wrap {{ overflow-x: auto; margin-top: 4px; }}
  table {{ width: 100%; border-collapse: collapse; font-size: 0.84rem; }}
  th {{
    text-align: right; padding: 9px 12px;
    color: var(--muted); font-weight: 600; font-size: 0.75rem;
    border-bottom: 1px solid var(--border);
    white-space: nowrap;
  }}
  td {{
    padding: 9px 12px; border-bottom: 1px solid rgba(48,54,61,.5);
    white-space: nowrap;
  }}
  tr:last-child td {{ border-bottom: none; }}
  tr:hover td {{ background: rgba(255,255,255,.03); }}
  .spend-bar {{
    display: inline-block; height: 6px; border-radius: 3px;
    background: linear-gradient(90deg, var(--indigo), var(--pink));
    vertical-align: middle; margin-left: 8px;
    transition: width .3s;
  }}

  /* ── Footer ── */
  .footer {{
    text-align: center; margin-top: 40px;
    font-size: 0.75rem; color: var(--muted);
    padding-bottom: 24px;
  }}
</style>
</head>
<body>

<div class="header">
  <div class="header-left">
    <div class="logo-row">
      <div class="logo-dot">📊</div>
      <h1>{title}</h1>
    </div>
    <div class="subtitle">ביצועי קמפיינים · {subtitle}</div>
  </div>
  <div class="date-badge">🗓 {sd} &nbsp;—&nbsp; {ed}</div>
</div>

<div class="container">

  <!-- KPI Cards -->
  <div class="section-label">מדדים מרכזיים</div>
  <div class="kpi-grid">
    <div class="kpi indigo">
      <div class="kpi-label">הוצאה כוללת</div>
      <div class="kpi-value">{fmt(total_spend, 0, '₪')}</div>
      <div class="kpi-sub">{sd} — {ed}</div>
    </div>
    <div class="kpi blue">
      <div class="kpi-label">חשיפות</div>
      <div class="kpi-value">{fmt(total_impressions)}</div>
      <div class="kpi-sub">CPM ₪{fmt(cpm_val, 2)}</div>
    </div>
    <div class="kpi cyan">
      <div class="kpi-label">קליקים</div>
      <div class="kpi-value">{fmt(total_clicks)}</div>
      <div class="kpi-sub">CTR {fmt(ctr_val, 2)}%</div>
    </div>
    <div class="kpi violet">
      <div class="kpi-label">טווח הגעה</div>
      <div class="kpi-value">{fmt(total_reach)}</div>
      <div class="kpi-sub">תדירות {fmt(freq_val, 1)}</div>
    </div>
    <div class="kpi pink">
      <div class="kpi-label">CPC</div>
      <div class="kpi-value">₪{fmt(cpc_val, 2)}</div>
      <div class="kpi-sub">עלות לקליק</div>
    </div>
    <div class="kpi green">
      <div class="kpi-label">ROAS</div>
      <div class="kpi-value">{roas_val:.2f}x</div>
      <div class="kpi-sub">החזר על הוצאות פרסום</div>
    </div>
    <div class="kpi amber">
      <div class="kpi-label">רכישות</div>
      <div class="kpi-value">{fmt(purchases)}</div>
      <div class="kpi-sub">המרות כוללות</div>
    </div>
    <div class="kpi red">
      <div class="kpi-label">CPM</div>
      <div class="kpi-value">₪{fmt(cpm_val, 2)}</div>
      <div class="kpi-sub">עלות לאלף חשיפות</div>
    </div>
  </div>

  <!-- Daily Trend -->
  <div class="section-label">מגמה יומית</div>
  <div class="charts-row cols-1" style="margin-bottom:14px">
    <div class="card">
      <div class="card-title">
        <div class="icon" style="background:rgba(99,102,241,.15);">📈</div>
        ביצועים יומיים — הוצאה, חשיפות וקליקים לאורך זמן
      </div>
      <div class="chart-wrap chart-wrap-tall">
        <canvas id="dailyChart"></canvas>
      </div>
    </div>
  </div>

  <!-- Campaigns + Placements -->
  <div class="section-label">קמפיינים ופלטפורמות</div>
  <div class="charts-row cols-3-1" style="margin-bottom:14px">
    <div class="card">
      <div class="card-title">
        <div class="icon" style="background:rgba(59,130,246,.15);">🏆</div>
        ביצועי קמפיינים — הוצאה וקליקים
      </div>
      <div class="table-wrap">
        <table id="campTable"></table>
      </div>
    </div>
    <div class="card">
      <div class="card-title">
        <div class="icon" style="background:rgba(236,72,153,.15);">🎯</div>
        הוצאה לפי פלטפורמה
      </div>
      <div class="chart-wrap">
        <canvas id="platChart"></canvas>
      </div>
    </div>
  </div>

  <!-- Demographics -->
  <div class="section-label">דמוגרפיה</div>
  <div class="charts-row cols-1" style="margin-bottom:0">
    <div class="card">
      <div class="card-title">
        <div class="icon" style="background:rgba(16,185,129,.15);">👥</div>
        הוצאה לפי גיל ומגדר
      </div>
      <div class="chart-wrap">
        <canvas id="demoChart"></canvas>
      </div>
    </div>
  </div>

</div>

<div class="footer">נוצר אוטומטית · Meta Ads Dashboard · {ed}</div>

<script>
/* ── Data (injected by server) ── */
const CAMP_NAMES   = {camp_names};
const CAMP_SPEND   = {camp_spend};
const CAMP_CLICKS  = {camp_clicks};
const DAILY_DATES  = {daily_dates};
const DAILY_SPEND  = {daily_spend};
const DAILY_IMPR   = {daily_impr};
const DAILY_CLICKS = {daily_clicks};
const PLAT_LABELS  = {plat_labels};
const PLAT_VALUES  = {plat_values};
const DEMO_AGE     = {demo_age_labels};
const DEMO_DS      = {demo_datasets};

/* ── Chart library (pure Canvas — zero external dependencies) ── */
{js_charts}
</script>
</body>
</html>"""

    return html


# ---------------------------------------------------------------------------
# PDF Report & Figma-level Dashboard
# ---------------------------------------------------------------------------

@mcp.tool()
def generate_meta_pdf(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
    output_path: str = "",
) -> dict:
    """Generate a complete Meta Ads PDF report saved to disk.

    Creates an A3 landscape dark-themed PDF with KPI summary, daily trend chart,
    platform distribution donut, demographics bar chart, and campaign performance table.
    Returns the path to the saved PDF file.

    Args:
        start_date: YYYY-MM-DD. Defaults to 28 days ago.
        end_date: YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        output_path: Full path for the PDF. Defaults to ~/Desktop/meta_report_YYYYMMDD.pdf
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.gridspec as gridspec
        from matplotlib.patches import FancyBboxPatch
        import numpy as np
    except ImportError:
        return {"error": "matplotlib not installed. Run: pip install matplotlib numpy"}

    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    time_range = json.dumps({"since": sd, "until": ed})

    def safe(result):
        if isinstance(result, dict) and "error" in result:
            return []
        return result.get("data", [])

    overview_raw  = _get(f"{acct}/insights", {"time_range": time_range, "fields": "impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions"})
    campaigns_raw = _get(f"{acct}/insights", {"time_range": time_range, "level": "campaign", "fields": "campaign_name,impressions,clicks,spend,ctr,actions", "limit": 10, "sort": json.dumps(["spend_descending"]), "effective_status": json.dumps(["ACTIVE", "PAUSED"])})
    daily_raw     = _get(f"{acct}/insights", {"time_range": time_range, "time_increment": 1, "fields": "date_start,impressions,clicks,spend,ctr", "limit": 90})
    demo_raw      = _get(f"{acct}/insights", {"time_range": time_range, "breakdowns": "age,gender", "fields": "age,gender,impressions,clicks,spend,reach", "limit": 60})
    placement_raw = _get(f"{acct}/insights", {"time_range": time_range, "breakdowns": "publisher_platform,platform_position", "fields": "publisher_platform,platform_position,impressions,clicks,spend", "limit": 30})

    ov_list = safe(overview_raw)
    ov = ov_list[0] if ov_list else {}
    total_spend       = float(ov.get("spend", 0))
    total_impressions = int(ov.get("impressions", 0))
    total_clicks      = int(ov.get("clicks", 0))
    total_reach       = int(ov.get("reach", 0))
    ctr_val           = float(ov.get("ctr", 0))
    cpc_val           = float(ov.get("cpc", 0))
    cpm_val           = float(ov.get("cpm", 0))
    freq_val          = float(ov.get("frequency", 0))
    actions_list      = ov.get("actions", [])
    purchases         = sum(float(a.get("value", 0)) for a in actions_list if a.get("action_type") in ("purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase"))
    roas_val          = round(purchases / total_spend, 2) if total_spend > 0 else 0.0

    def numfmt(n, dec=0, prefix=""):
        try:
            v = float(n)
            if abs(v) >= 1_000_000:
                return f"{prefix}{v/1_000_000:.1f}M"
            if abs(v) >= 1_000:
                return f"{prefix}{v/1_000:.1f}K"
            return f"{prefix}{v:.{dec}f}" if dec else f"{prefix}{int(v):,}"
        except Exception:
            return "0"

    campaigns = safe(campaigns_raw)[:10]
    camp_names  = [c.get("campaign_name", "")[:42] for c in campaigns]
    camp_spend  = [float(c.get("spend", 0)) for c in campaigns]
    camp_clicks = [int(c.get("clicks", 0)) for c in campaigns]
    camp_impr   = [int(c.get("impressions", 0)) for c in campaigns]
    camp_ctr    = [float(c.get("ctr", 0)) for c in campaigns]

    daily = safe(daily_raw)
    daily_dates  = [d.get("date_start", "")[-5:] for d in daily]
    daily_spend  = [float(d.get("spend", 0)) for d in daily]
    daily_clicks = [int(d.get("clicks", 0)) for d in daily]

    plat_map = {"facebook": "Facebook", "instagram": "Instagram", "audience_network": "Audience Net.", "messenger": "Messenger"}
    plat_totals: dict = {}
    for p in safe(placement_raw):
        lbl = plat_map.get(p.get("publisher_platform", ""), p.get("publisher_platform", ""))
        plat_totals[lbl] = plat_totals.get(lbl, 0) + float(p.get("spend", 0))
    plat_labels = list(plat_totals.keys())
    plat_values = [round(v, 2) for v in plat_totals.values()]

    demo = safe(demo_raw)
    age_groups  = sorted(set(d.get("age", "") for d in demo if d.get("age")))
    genders     = sorted(set(d.get("gender", "") for d in demo if d.get("gender")))
    gender_lbl  = {"male": "Male", "female": "Female", "unknown": "Unknown"}
    demo_lookup = {f"{d.get('age')}_{d.get('gender')}": float(d.get("spend", 0)) for d in demo}

    # ── Colours ──────────────────────────────────────────────────────────────
    BG     = "#0d1117"
    SURF   = "#161b22"
    SURF2  = "#1c2330"
    BORDER = "#30363d"
    TEXT   = "#e6edf3"
    MUTED  = "#8b949e"
    COLORS = ["#7c3aed", "#ec4899", "#10b981", "#f59e0b", "#3b82f6", "#06b6d4", "#8b5cf6", "#f87171"]

    plt.rcParams.update({
        "font.family": "DejaVu Sans",
        "text.color": TEXT,
        "axes.facecolor": SURF,
        "axes.edgecolor": BORDER,
        "axes.labelcolor": MUTED,
        "xtick.color": MUTED,
        "ytick.color": MUTED,
        "figure.facecolor": BG,
        "grid.color": BORDER,
        "grid.alpha": 0.45,
        "font.size": 9,
    })

    fig = plt.figure(figsize=(16.54, 11.69))
    fig.patch.set_facecolor(BG)

    outer = gridspec.GridSpec(
        4, 1, figure=fig,
        height_ratios=[0.055, 0.115, 0.43, 0.40],
        hspace=0.05, left=0.03, right=0.97, top=0.97, bottom=0.03,
    )

    # ── Header ───────────────────────────────────────────────────────────────
    ax_hdr = fig.add_subplot(outer[0])
    ax_hdr.set_facecolor("#13103a")
    ax_hdr.set_xlim(0, 1); ax_hdr.set_ylim(0, 1); ax_hdr.axis("off")
    ax_hdr.text(0.015, 0.55, "Meta Ads Report", fontsize=17, fontweight="bold", color=TEXT, va="center")
    ax_hdr.text(0.985, 0.55, f"Period: {sd}  \u2192  {ed}", fontsize=10, color=MUTED, va="center", ha="right")
    bar_colors = ["#7c3aed", "#6366f1", "#ec4899", "#f59e0b", "#10b981", "#06b6d4"]
    for i, c in enumerate(bar_colors):
        ax_hdr.axhline(0.04, xmin=i/6, xmax=(i+1)/6, color=c, linewidth=2.5)

    # ── KPI tiles ────────────────────────────────────────────────────────────
    kpi_gs = gridspec.GridSpecFromSubplotSpec(1, 8, subplot_spec=outer[1], wspace=0.07)
    kpis = [
        ("Spend",       f"\u20aa{numfmt(total_spend, 0)}",  "#7c3aed"),
        ("Impressions", numfmt(total_impressions),           "#ec4899"),
        ("Clicks",      numfmt(total_clicks),                "#10b981"),
        ("Reach",       numfmt(total_reach),                 "#3b82f6"),
        ("CTR",         f"{ctr_val:.2f}%",                  "#f59e0b"),
        ("CPC",         f"\u20aa{cpc_val:.2f}",             "#06b6d4"),
        ("CPM",         f"\u20aa{cpm_val:.2f}",             "#8b5cf6"),
        ("ROAS",        f"{roas_val:.2f}x",                 "#10b981"),
    ]
    for i, (label, value, color) in enumerate(kpis):
        ax = fig.add_subplot(kpi_gs[i])
        ax.set_facecolor(SURF); ax.set_xlim(0, 1); ax.set_ylim(0, 1); ax.axis("off")
        ax.add_patch(FancyBboxPatch((0, 0.92), 1, 0.08, boxstyle="square,pad=0",
                                    facecolor=color, alpha=0.9, transform=ax.transAxes, zorder=5))
        ax.text(0.5, 0.58, value, fontsize=12, fontweight="bold", color=TEXT, ha="center", va="center")
        ax.text(0.5, 0.24, label, fontsize=7.5, color=MUTED, ha="center", va="center")
        for spine in ax.spines.values():
            spine.set_edgecolor(BORDER); spine.set_linewidth(0.7); spine.set_visible(True)

    # ── Middle: Daily trend | Placement donut ────────────────────────────────
    mid = gridspec.GridSpecFromSubplotSpec(1, 2, subplot_spec=outer[2], wspace=0.07, width_ratios=[2.2, 1])

    ax_line = fig.add_subplot(mid[0])
    if daily_dates and daily_spend:
        x = range(len(daily_dates))
        ax_line.fill_between(x, daily_spend, alpha=0.15, color=COLORS[0])
        ax_line.plot(x, daily_spend, color=COLORS[0], linewidth=2.2, label="Spend (\u20aa)")
        ax2 = ax_line.twinx()
        ax2.plot(x, daily_clicks, color=COLORS[5], linewidth=1.6, linestyle="--", alpha=0.85, label="Clicks")
        ax2.tick_params(colors=MUTED); ax2.spines[:].set_color(BORDER)
        step = max(1, len(daily_dates) // 12)
        ax_line.set_xticks(list(range(0, len(daily_dates), step)))
        ax_line.set_xticklabels([daily_dates[i] for i in range(0, len(daily_dates), step)], rotation=40, ha="right", fontsize=7.5)
        lines1, lbs1 = ax_line.get_legend_handles_labels()
        lines2, lbs2 = ax2.get_legend_handles_labels()
        ax_line.legend(lines1 + lines2, lbs1 + lbs2, loc="upper left", fontsize=8,
                       facecolor=SURF2, edgecolor=BORDER, labelcolor=TEXT)
    ax_line.set_title("Daily Spend & Clicks", fontsize=10, fontweight="bold", color=TEXT, pad=8)
    ax_line.grid(True, axis="y"); ax_line.spines[:].set_color(BORDER)
    ax_line.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"\u20aa{numfmt(v)}"))

    ax_donut = fig.add_subplot(mid[1])
    ax_donut.set_facecolor(SURF)
    if plat_labels and plat_values:
        wedges, _, autotexts = ax_donut.pie(
            plat_values, labels=None, colors=COLORS[:len(plat_labels)],
            autopct="%1.0f%%", pctdistance=0.72, startangle=90,
            wedgeprops={"width": 0.52, "edgecolor": BG, "linewidth": 2},
        )
        for at in autotexts: at.set_fontsize(8); at.set_color(TEXT)
        ax_donut.legend(wedges, plat_labels, loc="lower center",
                        bbox_to_anchor=(0.5, -0.14), ncol=2, fontsize=8,
                        facecolor=SURF2, edgecolor=BORDER, labelcolor=TEXT)
    ax_donut.set_title("Spend by Platform", fontsize=10, fontweight="bold", color=TEXT, pad=8)

    # ── Bottom: Campaign table | Demographics bar ─────────────────────────────
    bot = gridspec.GridSpecFromSubplotSpec(1, 2, subplot_spec=outer[3], wspace=0.07, width_ratios=[3, 2])

    ax_tbl = fig.add_subplot(bot[0])
    ax_tbl.set_facecolor(SURF); ax_tbl.axis("off")
    ax_tbl.set_title("Top Campaigns by Spend", fontsize=10, fontweight="bold", color=TEXT, pad=8)
    if camp_names:
        rows = [
            [camp_names[i], f"\u20aa{camp_spend[i]:,.0f}", f"{camp_clicks[i]:,}",
             numfmt(camp_impr[i] if i < len(camp_impr) else 0),
             f"{camp_ctr[i]:.2f}%" if i < len(camp_ctr) else "\u2014"]
            for i in range(len(camp_names))
        ]
        tbl = ax_tbl.table(
            cellText=rows, colLabels=["Campaign", "Spend", "Clicks", "Impressions", "CTR"],
            cellLoc="left", loc="upper center", bbox=[0, -0.05, 1, 1.0],
        )
        tbl.auto_set_font_size(False); tbl.set_fontsize(7.8)
        for (r, c), cell in tbl.get_celld().items():
            cell.set_facecolor(SURF2 if r == 0 else (SURF if r % 2 == 0 else "#191f2a"))
            cell.set_edgecolor(BORDER); cell.set_linewidth(0.4)
            cell.set_text_props(color=MUTED if r == 0 else TEXT,
                                fontweight="bold" if r == 0 else "normal", fontsize=7 if r == 0 else 7.8)
            if c == 0: cell.set_width(0.38)
    else:
        ax_tbl.text(0.5, 0.5, "No campaign data", ha="center", va="center", color=MUTED, fontsize=11)

    ax_demo = fig.add_subplot(bot[1])
    ax_demo.set_facecolor(SURF)
    ax_demo.set_title("Spend by Age & Gender", fontsize=10, fontweight="bold", color=TEXT, pad=8)
    if age_groups and genders:
        x_pos = np.arange(len(age_groups))
        bar_w = 0.75 / max(len(genders), 1)
        gen_colors = [COLORS[0], COLORS[1], MUTED]
        for gi, g in enumerate(genders):
            vals = [demo_lookup.get(f"{age}_{g}", 0) for age in age_groups]
            offset = (gi - len(genders) / 2 + 0.5) * bar_w
            ax_demo.bar(x_pos + offset, vals, bar_w * 0.88, label=gender_lbl.get(g, g),
                        color=gen_colors[gi % 3], alpha=0.85)
        ax_demo.set_xticks(x_pos)
        ax_demo.set_xticklabels(age_groups, fontsize=8)
        ax_demo.legend(fontsize=8, facecolor=SURF2, edgecolor=BORDER, labelcolor=TEXT)
        ax_demo.yaxis.set_major_formatter(plt.FuncFormatter(lambda v, _: f"\u20aa{numfmt(v)}"))
    else:
        ax_demo.text(0.5, 0.5, "No demographics data", ha="center", va="center",
                     color=MUTED, fontsize=11, transform=ax_demo.transAxes)
    ax_demo.grid(True, axis="y"); ax_demo.spines[:].set_color(BORDER)

    # ── Save ─────────────────────────────────────────────────────────────────
    if not output_path:
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        os.makedirs(desktop, exist_ok=True)
        output_path = os.path.join(desktop, f"meta_report_{ed.replace('-', '')}.pdf")

    plt.savefig(output_path, format="pdf", dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    return {"success": True, "path": output_path, "period": f"{sd} \u2192 {ed}"}


@mcp.tool()
def generate_meta_dashboard_figma(
    start_date: str = "",
    end_date: str = "",
    account_id: str = "",
    dashboard_title: str = "",
) -> str:
    """Generate a premium Figma-level Meta Ads dashboard as an HTML artifact.

    Same data as generate_meta_dashboard but with a completely redesigned UI:
    glassmorphism cards, Inter font, gradient glow KPIs, premium typography,
    animated counters, and a dark-navy design system.

    Args:
        start_date: YYYY-MM-DD. Defaults to 28 days ago.
        end_date: YYYY-MM-DD. Defaults to today.
        account_id: Ad account ID. Leave blank to use META_AD_ACCOUNT_ID from .env.
        dashboard_title: Custom title. Optional.
    """
    ed = end_date or str(date.today())
    sd = start_date or str(date.today() - timedelta(days=28))
    acct = (account_id if account_id.startswith("act_") else f"act_{account_id}") if account_id else _account_id()
    time_range = json.dumps({"since": sd, "until": ed})

    def safe(result):
        if isinstance(result, dict) and "error" in result:
            return []
        return result.get("data", [])

    overview_raw   = _get(f"{acct}/insights", {"time_range": time_range, "fields": "impressions,clicks,spend,ctr,cpc,cpm,reach,frequency,actions"})
    campaigns_raw  = _get(f"{acct}/insights", {"time_range": time_range, "level": "campaign", "fields": "campaign_name,impressions,clicks,spend,ctr,cpc,actions", "limit": 10, "sort": json.dumps(["spend_descending"]), "effective_status": json.dumps(["ACTIVE", "PAUSED"])})
    daily_raw      = _get(f"{acct}/insights", {"time_range": time_range, "time_increment": 1, "fields": "date_start,impressions,clicks,spend,ctr", "limit": 90})
    demo_raw       = _get(f"{acct}/insights", {"time_range": time_range, "breakdowns": "age,gender", "fields": "age,gender,impressions,clicks,spend,reach", "limit": 60})
    placement_raw  = _get(f"{acct}/insights", {"time_range": time_range, "breakdowns": "publisher_platform,platform_position", "fields": "publisher_platform,platform_position,impressions,clicks,spend", "limit": 30})

    ov_list = safe(overview_raw)
    ov = ov_list[0] if ov_list else {}
    total_spend       = float(ov.get("spend", 0))
    total_impressions = int(ov.get("impressions", 0))
    total_clicks      = int(ov.get("clicks", 0))
    total_reach       = int(ov.get("reach", 0))
    ctr_val           = float(ov.get("ctr", 0))
    cpc_val           = float(ov.get("cpc", 0))
    cpm_val           = float(ov.get("cpm", 0))
    freq_val          = float(ov.get("frequency", 0))
    actions_list      = ov.get("actions", [])
    purchases         = sum(float(a.get("value", 0)) for a in actions_list if a.get("action_type") in ("purchase", "omni_purchase", "offsite_conversion.fb_pixel_purchase"))
    roas_val          = round(purchases / total_spend, 2) if total_spend > 0 else 0.0

    def fmt(n, dec=0, prefix=""):
        try:
            v = float(n)
            if abs(v) >= 1_000_000: return f"{prefix}{v/1_000_000:.1f}M"
            if abs(v) >= 1_000:     return f"{prefix}{v/1_000:.1f}K"
            return f"{prefix}{v:.{dec}f}" if dec else f"{prefix}{int(v):,}"
        except Exception: return "0"

    def fmt_date(d):
        try:
            from datetime import datetime as _dt
            return _dt.strptime(d, "%Y-%m-%d").strftime("%d/%m")
        except Exception: return d

    campaigns   = safe(campaigns_raw)[:10]
    camp_names  = json.dumps([c.get("campaign_name", "")[:36] for c in campaigns], ensure_ascii=False)
    camp_spend  = json.dumps([float(c.get("spend", 0)) for c in campaigns])
    camp_clicks = json.dumps([int(c.get("clicks", 0)) for c in campaigns])
    camp_impr   = json.dumps([int(c.get("impressions", 0)) for c in campaigns])
    camp_ctr    = json.dumps([float(c.get("ctr", 0)) for c in campaigns])

    daily       = safe(daily_raw)
    daily_dates = json.dumps([fmt_date(d.get("date_start", "")) for d in daily], ensure_ascii=False)
    daily_spend = json.dumps([float(d.get("spend", 0)) for d in daily])
    daily_clicks = json.dumps([int(d.get("clicks", 0)) for d in daily])
    daily_impr  = json.dumps([int(d.get("impressions", 0)) for d in daily])

    plat_map = {"facebook": "פייסבוק", "instagram": "אינסטגרם", "audience_network": "Audience Net.", "messenger": "מסנג׳ר"}
    plat_totals: dict = {}
    for p in safe(placement_raw):
        lbl = plat_map.get(p.get("publisher_platform", ""), p.get("publisher_platform", ""))
        plat_totals[lbl] = plat_totals.get(lbl, 0) + float(p.get("spend", 0))
    plat_labels = json.dumps(list(plat_totals.keys()), ensure_ascii=False)
    plat_values = json.dumps([round(v, 2) for v in plat_totals.values()])

    demo = safe(demo_raw)
    gmap = {"male": "גברים", "female": "נשים", "unknown": "לא ידוע"}
    age_groups = sorted(set(d.get("age", "") for d in demo if d.get("age")))
    genders    = sorted(set(d.get("gender", "") for d in demo if d.get("gender")))
    demo_lookup = {f"{d.get('age')}_{d.get('gender')}": float(d.get("spend", 0)) for d in demo}
    demo_age_labels  = json.dumps(age_groups, ensure_ascii=False)
    demo_colors_list = ["rgba(124,58,237,0.88)", "rgba(236,72,153,0.88)", "rgba(139,149,158,0.75)"]
    demo_datasets_list = []
    for i, g in enumerate(genders):
        pts = [demo_lookup.get(f"{age}_{g}", 0) for age in age_groups]
        demo_datasets_list.append({"label": gmap.get(g, g), "data": pts, "backgroundColor": demo_colors_list[i % 3], "borderRadius": 7, "borderSkipped": False})
    demo_datasets = json.dumps(demo_datasets_list, ensure_ascii=False)

    title    = dashboard_title or "Meta Ads Dashboard"
    subtitle = f"{sd} — {ed}"

    js_charts = r"""
const DPR = window.devicePixelRatio || 1;
const C = {
  violet:'#7c3aed', violet2:'#a78bfa', indigo:'#6366f1',
  pink:'#ec4899',   cyan:'#06b6d4',    green:'#10b981',
  amber:'#f59e0b',  blue:'#3b82f6',    red:'#ef4444',
  muted:'#7c8db5',  text:'#c9d4f0',    grid:'rgba(255,255,255,0.07)',
};
const ALL = [C.violet,C.pink,C.green,C.amber,C.blue,C.cyan,'#8b5cf6','#f87171'];

function numFmt(n) {
  n = Math.abs(+n);
  if (n >= 1e6)  return (n/1e6).toFixed(1)+'M';
  if (n >= 1000) return (n/1000).toFixed(0)+'K';
  return n.toFixed(0);
}

function initCanvas(id, H) {
  const el = document.getElementById(id);
  if (!el) return null;
  const W = el.parentElement.clientWidth - 2;
  el.style.width = W+'px'; el.style.height = H+'px';
  el.width = Math.round(W*DPR); el.height = Math.round(H*DPR);
  const ctx = el.getContext('2d');
  ctx.scale(DPR, DPR);
  return {ctx, W, H};
}

function roundRect(ctx, x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x+r, y); ctx.lineTo(x+w-r, y);
  ctx.quadraticCurveTo(x+w, y, x+w, y+r);
  ctx.lineTo(x+w, y+h-r); ctx.quadraticCurveTo(x+w, y+h, x+w-r, y+h);
  ctx.lineTo(x+r, y+h); ctx.quadraticCurveTo(x, y+h, x, y+h-r);
  ctx.lineTo(x, y+r); ctx.quadraticCurveTo(x, y, x+r, y);
  ctx.closePath();
}

function drawLine(id, labels, datasets, H) {
  H = H || 290;
  const r = initCanvas(id, H); if (!r) return;
  const {ctx, W} = r;
  const pT=24, pB=42, pL=62, pR=52;
  const cW = W-pL-pR, cH = H-pT-pB;
  const maxA = Math.max(...(datasets[0]||{data:[]}).data, 1);
  const maxB = datasets[1] ? Math.max(...datasets[1].data, 1) : 1;
  const step = cW / Math.max(labels.length-1, 1);

  for (let i=0; i<=5; i++) {
    const y = pT+(cH/5)*i;
    ctx.beginPath(); ctx.strokeStyle=C.grid; ctx.lineWidth=1;
    ctx.moveTo(pL, y); ctx.lineTo(pL+cW, y); ctx.stroke();
    ctx.fillStyle=C.muted; ctx.font='9.5px Inter,system-ui';
    ctx.textAlign='right';
    ctx.fillText('₪'+numFmt(maxA-(maxA/5)*i), pL-6, y+4);
    if (datasets[1]) {
      ctx.textAlign='left';
      ctx.fillText(numFmt(maxB-(maxB/5)*i), pL+cW+6, y+4);
    }
  }

  const evry = Math.ceil(labels.length/14);
  ctx.fillStyle=C.muted; ctx.font='9px Inter,system-ui'; ctx.textAlign='center';
  labels.forEach((l,i)=>{ if(i%evry===0) ctx.fillText(l, pL+i*step, H-6); });

  datasets.forEach((ds, di) => {
    const maxV = di===0 ? maxA : maxB;
    const color = ds.color || ALL[di];
    const pts = ds.data.map((v,i)=>({x:pL+i*step, y:pT+cH-(v/maxV)*cH}));
    if (ds.fill && pts.length>1) {
      const grad = ctx.createLinearGradient(0, pT, 0, pT+cH);
      grad.addColorStop(0, color.replace(')', ',0.22)').replace('rgb', 'rgba'));
      grad.addColorStop(1, 'rgba(0,0,0,0)');
      ctx.beginPath();
      ctx.moveTo(pts[0].x, pts[0].y);
      pts.slice(1).forEach(p => ctx.lineTo(p.x, p.y));
      ctx.lineTo(pts[pts.length-1].x, pT+cH);
      ctx.lineTo(pts[0].x, pT+cH); ctx.closePath();
      ctx.fillStyle=grad; ctx.fill();
    }
    if (pts.length>1) {
      ctx.beginPath(); ctx.strokeStyle=color; ctx.lineWidth=2.2;
      ctx.lineJoin='round'; ctx.lineCap='round';
      ctx.moveTo(pts[0].x, pts[0].y);
      pts.slice(1).forEach(p => ctx.lineTo(p.x, p.y));
      ctx.stroke();
    }
    pts.forEach(p => {
      ctx.beginPath(); ctx.arc(p.x, p.y, 2.5, 0, Math.PI*2);
      ctx.fillStyle=color; ctx.fill();
    });
  });

  const lx_start = pL+4;
  let lx = lx_start;
  datasets.forEach(ds => {
    const color = ds.color || ALL[0];
    ctx.beginPath(); ctx.strokeStyle=color; ctx.lineWidth=2;
    ctx.moveTo(lx, 14); ctx.lineTo(lx+18, 14); ctx.stroke();
    ctx.fillStyle=C.text; ctx.font='9px Inter,system-ui'; ctx.textAlign='left';
    ctx.fillText(ds.label, lx+22, 18);
    lx += ctx.measureText(ds.label).width + 46;
  });
}

function drawDonut(id, labels, values, H) {
  H = H || 260;
  const r = initCanvas(id, H); if (!r) return;
  const {ctx, W} = r;
  const total = values.reduce((a,b)=>a+b, 0)||1;
  const cx=W/2, cy=H/2-14, radius=Math.min(W,H)*0.32;
  let angle = -Math.PI/2;
  values.forEach((v, i) => {
    const slice = (v/total)*Math.PI*2;
    ctx.beginPath(); ctx.moveTo(cx, cy);
    ctx.arc(cx, cy, radius, angle, angle+slice);
    ctx.closePath();
    const grad = ctx.createRadialGradient(cx, cy, radius*0.55, cx, cy, radius);
    grad.addColorStop(0, ALL[i % ALL.length]+'cc');
    grad.addColorStop(1, ALL[i % ALL.length]);
    ctx.fillStyle=grad; ctx.fill();
    ctx.beginPath(); ctx.arc(cx, cy, radius*0.54, 0, Math.PI*2);
    ctx.fillStyle='#0a0f1e'; ctx.fill();
    angle += slice;
  });
  ctx.fillStyle=C.text; ctx.font='bold 14px Inter,system-ui'; ctx.textAlign='center';
  ctx.fillText('₪'+numFmt(total), cx, cy+5);
  ctx.fillStyle=C.muted; ctx.font='10px Inter,system-ui';
  ctx.fillText('total', cx, cy+20);
  let lx = 8, ly = H-22;
  labels.forEach((lbl, i) => {
    roundRect(ctx, lx, ly-8, 10, 10, 2);
    ctx.fillStyle=ALL[i%ALL.length]; ctx.fill();
    ctx.fillStyle=C.muted; ctx.font='9px Inter,system-ui'; ctx.textAlign='left';
    ctx.fillText(lbl, lx+14, ly+1);
    lx += ctx.measureText(lbl).width + 30;
  });
}

function drawGroupedBar(id, labels, datasets, H) {
  H = H || 260;
  const r = initCanvas(id, H); if (!r) return;
  const {ctx, W} = r;
  const pT=24, pB=36, pL=58, pR=16;
  const cW=W-pL-pR, cH=H-pT-pB;
  const n=labels.length, gW=cW/n;
  const bW=Math.min(gW*0.7/Math.max(datasets.length,1), 26);
  const allVals=datasets.flatMap(d=>d.data);
  const maxV=Math.max(...allVals, 1);
  for (let i=0; i<=5; i++) {
    const y=pT+(cH/5)*i;
    ctx.beginPath(); ctx.strokeStyle=C.grid; ctx.lineWidth=1;
    ctx.moveTo(pL,y); ctx.lineTo(pL+cW,y); ctx.stroke();
    ctx.fillStyle=C.muted; ctx.font='9px Inter,system-ui'; ctx.textAlign='right';
    ctx.fillText('₪'+numFmt(maxV-(maxV/5)*i), pL-5, y+4);
  }
  ctx.fillStyle=C.muted; ctx.font='9.5px Inter,system-ui'; ctx.textAlign='center';
  labels.forEach((l,i)=>ctx.fillText(l, pL+(i+0.5)*gW, H-8));
  datasets.forEach((ds,di) => {
    const offset=(di-datasets.length/2+0.5)*bW;
    ds.data.forEach((v,i) => {
      const bH=(v/maxV)*cH;
      const x=pL+(i+0.5)*gW+offset-bW/2;
      const y=pT+cH-bH;
      const grad=ctx.createLinearGradient(0,y,0,y+bH);
      grad.addColorStop(0, ds.backgroundColor||ALL[di]);
      grad.addColorStop(1, (ds.backgroundColor||ALL[di]).replace('0.88','0.35'));
      roundRect(ctx, x, y, bW, bH, 4);
      ctx.fillStyle=grad; ctx.fill();
    });
  });
  let lx=pL;
  datasets.forEach((ds,di) => {
    roundRect(ctx, lx, 8, 10, 10, 2);
    ctx.fillStyle=ds.backgroundColor||ALL[di]; ctx.fill();
    ctx.fillStyle=C.text; ctx.font='9px Inter,system-ui'; ctx.textAlign='left';
    ctx.fillText(ds.label, lx+14, 18);
    lx+=ctx.measureText(ds.label).width+34;
  });
}

(function buildTable() {
  const table=document.getElementById('campTable'); if (!table) return;
  if (!CAMP_NAMES.length) {
    table.innerHTML='<tbody><tr><td colspan="5" style="text-align:center;color:#7c8db5;padding:28px">אין נתוני קמפיינים</td></tr></tbody>';
    return;
  }
  const maxS=Math.max(...CAMP_SPEND,1);
  table.innerHTML='<thead><tr><th>קמפיין</th><th>הוצאה</th><th>חשיפות</th><th>קליקים</th><th>CTR</th></tr></thead><tbody></tbody>';
  const tb=table.querySelector('tbody');
  CAMP_NAMES.forEach((name,i)=>{
    const pct=Math.max(3, Math.round((CAMP_SPEND[i]/maxS)*90));
    const tr=document.createElement('tr');
    tr.innerHTML=
      `<td class="camp-name">${name}</td>`+
      `<td><div class="bar-cell"><div class="spend-bar" style="width:${pct}px"></div>`+
        `<span>₪${(+CAMP_SPEND[i]).toLocaleString()}</span></div></td>`+
      `<td>${(+CAMP_IMPR[i]).toLocaleString()}</td>`+
      `<td>${(+CAMP_CLICKS[i]).toLocaleString()}</td>`+
      `<td><span class="ctr-badge">${(+CAMP_CTR[i]).toFixed(2)}%</span></td>`;
    tb.appendChild(tr);
  });
})();

function animateCounters() {
  document.querySelectorAll('[data-val]').forEach(el => {
    const target = +el.dataset.val;
    const isFloat = el.dataset.float === '1';
    const prefix = el.dataset.prefix || '';
    const suffix = el.dataset.suffix || '';
    const big = target >= 1000;
    const dur = 900, fps = 60, steps = Math.round(dur/(1000/fps));
    let cur = 0;
    const inc = target / steps;
    const id = setInterval(() => {
      cur = Math.min(cur + inc, target);
      let display;
      if (big && cur >= 1000000)      display = prefix + (cur/1000000).toFixed(1) + 'M' + suffix;
      else if (big && cur >= 1000)    display = prefix + (cur/1000).toFixed(0) + 'K' + suffix;
      else if (isFloat)               display = prefix + cur.toFixed(2) + suffix;
      else                            display = prefix + Math.round(cur).toLocaleString() + suffix;
      el.textContent = display;
      if (cur >= target) clearInterval(id);
    }, 1000/fps);
  });
}

document.addEventListener('DOMContentLoaded', function() {
  animateCounters();
  drawLine('dailyChart', DAILY_DATES, [
    {label:'הוצאה (₪)', data:DAILY_SPEND,  color:'#7c3aed', fill:true},
    {label:'קליקים',    data:DAILY_CLICKS, color:'#06b6d4'},
  ], 300);
  drawDonut('platChart',  PLAT_LABELS, PLAT_VALUES, 280);
  drawGroupedBar('demoChart', DEMO_AGE, DEMO_DS, 280);
});
"""

    kpi_rows = [
        ("הוצאה כוללת",  fmt(total_spend, 0), f'data-val="{total_spend:.0f}" data-prefix="₪"',  "violet",  "💸"),
        ("חשיפות",       fmt(total_impressions), f'data-val="{total_impressions}" data-prefix=""', "pink",   "👁"),
        ("קליקים",       fmt(total_clicks),  f'data-val="{total_clicks}" data-prefix=""',         "green",   "🖱"),
        ("טווח הגעה",    fmt(total_reach),   f'data-val="{total_reach}" data-prefix=""',          "blue",    "📡"),
        ("CTR",          f"{ctr_val:.2f}%",  f'data-val="{ctr_val:.2f}" data-float="1" data-suffix="%"', "amber", "📊"),
        ("CPC",          f"₪{cpc_val:.2f}",  f'data-val="{cpc_val:.2f}" data-float="1" data-prefix="₪"', "cyan",  "💰"),
        ("CPM",          f"₪{cpm_val:.2f}",  f'data-val="{cpm_val:.2f}" data-float="1" data-prefix="₪"', "purple","📈"),
        ("ROAS",         f"{roas_val:.2f}x", f'data-val="{roas_val:.2f}" data-float="1" data-suffix="x"', "teal", "🎯"),
    ]

    kpi_html = ""
    for label, val_static, data_attrs, color_key, icon in kpi_rows:
        kpi_html += f"""
    <div class="kpi kpi-{color_key}">
      <div class="kpi-icon">{icon}</div>
      <div class="kpi-label">{label}</div>
      <div class="kpi-value" {data_attrs}>{val_static}</div>
    </div>"""

    html = f"""<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{title}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
<style>
:root {{
  --bg:       #050c1f;
  --bg2:      #070d22;
  --glass:    rgba(255,255,255,0.04);
  --glass-b:  rgba(255,255,255,0.08);
  --glass-h:  rgba(255,255,255,0.07);
  --text:     #c9d4f0;
  --text-hi:  #edf1ff;
  --muted:    #7c8db5;
  --subtle:   #3d4f72;
  --violet:   #7c3aed; --violet-g: rgba(124,58,237,.35);
  --pink:     #ec4899; --pink-g:   rgba(236,72,153,.35);
  --green:    #10b981; --green-g:  rgba(16,185,129,.35);
  --amber:    #f59e0b; --amber-g:  rgba(245,158,11,.35);
  --blue:     #3b82f6; --blue-g:   rgba(59,130,246,.35);
  --cyan:     #06b6d4; --cyan-g:   rgba(6,182,212,.35);
  --purple:   #8b5cf6; --purple-g: rgba(139,92,246,.35);
  --teal:     #14b8a6; --teal-g:   rgba(20,184,166,.35);
  --red:      #ef4444;
  --radius:   16px;
  --radius-sm:10px;
  --radius-lg:24px;
}}
*, *::before, *::after {{ box-sizing:border-box; margin:0; padding:0; }}
body {{
  font-family: 'Inter', system-ui, -apple-system, sans-serif;
  background: var(--bg);
  color: var(--text);
  direction: rtl;
  text-align: right;
  min-height: 100vh;
  padding-bottom: 60px;
  background-image:
    radial-gradient(ellipse 80% 50% at 20% -10%, rgba(124,58,237,.18) 0%, transparent 60%),
    radial-gradient(ellipse 60% 40% at 80% 110%, rgba(59,130,246,.12) 0%, transparent 55%);
}}

/* ── Header ── */
.hdr {{
  position: relative;
  padding: 36px 44px 30px;
  display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 14px;
  border-bottom: 1px solid var(--glass-b);
  backdrop-filter: blur(20px);
}}
.hdr::before {{
  content:'';
  position:absolute; inset:0;
  background: linear-gradient(135deg, rgba(124,58,237,.12) 0%, rgba(6,182,212,.06) 50%, transparent 100%);
  pointer-events:none;
}}
.hdr-left {{ display:flex; flex-direction:column; gap:5px; position:relative; }}
.hdr-logo {{
  display:flex; align-items:center; gap:13px;
}}
.logo-mark {{
  width:44px; height:44px; border-radius:13px;
  background: linear-gradient(135deg, var(--violet), var(--pink));
  display:flex; align-items:center; justify-content:center;
  font-size:22px;
  box-shadow: 0 0 22px var(--violet-g), 0 4px 12px rgba(0,0,0,.4);
}}
.hdr-title {{
  font-size:1.8rem; font-weight:800; letter-spacing:-0.8px;
  background: linear-gradient(135deg, #fff 30%, var(--violet2, #a78bfa) 70%, var(--pink));
  -webkit-background-clip: text; -webkit-text-fill-color: transparent;
  background-clip: text;
}}
.hdr-sub {{ color: var(--muted); font-size:.85rem; font-weight:400; margin-right:57px; }}
.hdr-right {{ display:flex; gap:10px; align-items:center; position:relative; }}
.badge {{
  background: var(--glass); border:1px solid var(--glass-b);
  border-radius:10px; padding:7px 16px;
  font-size:.82rem; color:var(--muted); font-weight:500;
  backdrop-filter: blur(8px);
}}
.live-dot {{
  width:8px; height:8px; border-radius:50%; background:var(--green);
  box-shadow: 0 0 8px var(--green);
  animation: pulse 2s infinite;
}}
@keyframes pulse {{ 0%,100%{{opacity:1}} 50%{{opacity:.4}} }}

/* ── Layout ── */
.wrap {{ max-width:1480px; margin:0 auto; padding:32px 44px 0; }}
.section-hdr {{
  font-size:.72rem; font-weight:700; letter-spacing:1.5px;
  text-transform:uppercase; color:var(--muted);
  margin-bottom:16px; display:flex; align-items:center; gap:8px;
}}
.section-hdr::after {{
  content:''; flex:1; height:1px; background:var(--glass-b);
}}

/* ── KPI grid ── */
.kpi-grid {{
  display:grid;
  grid-template-columns: repeat(4, 1fr);
  gap:14px;
  margin-bottom:30px;
}}
@media(max-width:1100px) {{ .kpi-grid{{ grid-template-columns:repeat(4,1fr) }} }}
@media(max-width:750px)  {{ .kpi-grid{{ grid-template-columns:repeat(2,1fr) }} }}

.kpi {{
  background: var(--glass);
  border: 1px solid var(--glass-b);
  border-radius: var(--radius);
  padding: 22px 20px 18px;
  position: relative;
  overflow: hidden;
  cursor: default;
  transition: transform .2s, box-shadow .2s, background .2s;
  backdrop-filter: blur(12px);
}}
.kpi:hover {{
  transform: translateY(-3px);
  background: var(--glass-h);
}}
.kpi-icon {{
  position:absolute; top:16px; left:18px;
  font-size:20px; opacity:.7;
}}
.kpi-label {{
  font-size:.77rem; font-weight:500; color:var(--muted);
  margin-bottom:10px; text-transform:uppercase; letter-spacing:.6px;
}}
.kpi-value {{
  font-size:1.75rem; font-weight:800; color:var(--text-hi);
  letter-spacing:-1px; font-variant-numeric:tabular-nums;
  line-height:1;
}}
.kpi::after {{
  content:'';
  position:absolute; bottom:0; right:0; left:0; height:2px;
  border-radius:0 0 var(--radius) var(--radius);
}}
.kpi-violet {{ box-shadow:0 0 0 1px var(--violet-g), 0 4px 20px rgba(0,0,0,.3); }}
.kpi-violet:hover {{ box-shadow: 0 0 20px var(--violet-g), 0 8px 30px rgba(0,0,0,.4); }}
.kpi-violet::after {{ background: linear-gradient(90deg, var(--violet), #a78bfa); }}
.kpi-pink {{ box-shadow:0 0 0 1px var(--pink-g), 0 4px 20px rgba(0,0,0,.3); }}
.kpi-pink:hover {{ box-shadow: 0 0 20px var(--pink-g), 0 8px 30px rgba(0,0,0,.4); }}
.kpi-pink::after {{ background: linear-gradient(90deg, var(--pink), #f472b6); }}
.kpi-green {{ box-shadow:0 0 0 1px var(--green-g), 0 4px 20px rgba(0,0,0,.3); }}
.kpi-green:hover {{ box-shadow: 0 0 20px var(--green-g), 0 8px 30px rgba(0,0,0,.4); }}
.kpi-green::after {{ background: linear-gradient(90deg, var(--green), #34d399); }}
.kpi-blue {{ box-shadow:0 0 0 1px var(--blue-g), 0 4px 20px rgba(0,0,0,.3); }}
.kpi-blue:hover {{ box-shadow: 0 0 20px var(--blue-g), 0 8px 30px rgba(0,0,0,.4); }}
.kpi-blue::after {{ background: linear-gradient(90deg, var(--blue), #60a5fa); }}
.kpi-amber {{ box-shadow:0 0 0 1px var(--amber-g), 0 4px 20px rgba(0,0,0,.3); }}
.kpi-amber:hover {{ box-shadow: 0 0 20px var(--amber-g), 0 8px 30px rgba(0,0,0,.4); }}
.kpi-amber::after {{ background: linear-gradient(90deg, var(--amber), #fbbf24); }}
.kpi-cyan {{ box-shadow:0 0 0 1px var(--cyan-g), 0 4px 20px rgba(0,0,0,.3); }}
.kpi-cyan:hover {{ box-shadow: 0 0 20px var(--cyan-g), 0 8px 30px rgba(0,0,0,.4); }}
.kpi-cyan::after {{ background: linear-gradient(90deg, var(--cyan), #22d3ee); }}
.kpi-purple {{ box-shadow:0 0 0 1px var(--purple-g), 0 4px 20px rgba(0,0,0,.3); }}
.kpi-purple:hover {{ box-shadow: 0 0 20px var(--purple-g), 0 8px 30px rgba(0,0,0,.4); }}
.kpi-purple::after {{ background: linear-gradient(90deg, var(--purple), #c4b5fd); }}
.kpi-teal {{ box-shadow:0 0 0 1px var(--teal-g), 0 4px 20px rgba(0,0,0,.3); }}
.kpi-teal:hover {{ box-shadow: 0 0 20px var(--teal-g), 0 8px 30px rgba(0,0,0,.4); }}
.kpi-teal::after {{ background: linear-gradient(90deg, var(--teal), #2dd4bf); }}

/* ── Chart cards ── */
.charts-row {{ display:grid; gap:14px; margin-bottom:14px; }}
.cols-1 {{ grid-template-columns:1fr; }}
.cols-2 {{ grid-template-columns:1fr 1fr; }}
.cols-3-1 {{ grid-template-columns:2.2fr 1fr; }}
@media(max-width:960px) {{
  .cols-2, .cols-3-1 {{ grid-template-columns:1fr; }}
  .kpi-grid {{ grid-template-columns:repeat(2,1fr) !important; }}
  .wrap {{ padding:20px 18px 0; }}
  .hdr {{ padding:24px 20px 20px; }}
}}

.card {{
  background: var(--glass);
  border: 1px solid var(--glass-b);
  border-radius: var(--radius);
  padding: 22px 24px;
  backdrop-filter: blur(12px);
  transition: box-shadow .2s;
}}
.card:hover {{ box-shadow: 0 8px 32px rgba(0,0,0,.35); }}

.card-hdr {{
  display:flex; align-items:center; gap:10px;
  margin-bottom:18px;
}}
.card-icon {{
  width:32px; height:32px; border-radius:9px;
  display:flex; align-items:center; justify-content:center;
  font-size:15px; flex-shrink:0;
}}
.card-title {{
  font-size:.93rem; font-weight:700; color:var(--text-hi); letter-spacing:-.2px;
}}
.card-sub {{
  font-size:.78rem; color:var(--muted); margin-top:1px;
}}

/* ── Table ── */
.tbl-wrap {{ overflow-x:auto; margin-top:4px; }}
table {{ width:100%; border-collapse:collapse; font-size:.83rem; }}
th {{
  text-align:right; padding:10px 13px;
  color:var(--muted); font-weight:600; font-size:.73rem;
  border-bottom:1px solid var(--glass-b);
  white-space:nowrap; letter-spacing:.5px; text-transform:uppercase;
}}
td {{ padding:10px 13px; border-bottom:1px solid rgba(255,255,255,.04); white-space:nowrap; }}
tr:last-child td {{ border-bottom:none; }}
tr:hover td {{ background:rgba(255,255,255,.03); }}
.camp-name {{ max-width:240px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; font-weight:500; color:var(--text-hi); }}
.bar-cell {{ display:flex; align-items:center; gap:9px; }}
.spend-bar {{
  height:5px; min-width:4px; border-radius:3px; flex-shrink:0;
  background:linear-gradient(90deg, var(--violet), var(--pink));
  box-shadow: 0 0 6px var(--violet-g);
}}
.ctr-badge {{
  background:rgba(16,185,129,.12); color:var(--green);
  border:1px solid rgba(16,185,129,.2);
  border-radius:6px; padding:2px 8px;
  font-size:.78rem; font-weight:600;
}}

/* ── Footer ── */
.footer {{
  text-align:center; padding:32px 0 0;
  color:var(--subtle); font-size:.78rem;
  border-top:1px solid var(--glass-b);
  margin-top:28px;
}}
</style>
</head>
<body>

<header class="hdr">
  <div class="hdr-left">
    <div class="hdr-logo">
      <div class="logo-mark">📊</div>
      <span class="hdr-title">{title}</span>
    </div>
    <span class="hdr-sub">{subtitle}</span>
  </div>
  <div class="hdr-right">
    <div class="live-dot"></div>
    <div class="badge">תקופה: {sd} — {ed}</div>
  </div>
</header>

<div class="wrap">

  <div class="section-hdr" style="margin-top:28px;">מדדים עיקריים</div>
  <div class="kpi-grid">
    {kpi_html}
  </div>

  <div class="section-hdr">טרנד יומי</div>
  <div class="charts-row cols-3-1" style="margin-bottom:14px">
    <div class="card">
      <div class="card-hdr">
        <div class="card-icon" style="background:rgba(124,58,237,.18);">📈</div>
        <div>
          <div class="card-title">הוצאה וקליקים יומיים</div>
          <div class="card-sub">ציר ימני = קליקים · ציר שמאלי = הוצאה</div>
        </div>
      </div>
      <canvas id="dailyChart"></canvas>
    </div>
    <div class="card">
      <div class="card-hdr">
        <div class="card-icon" style="background:rgba(6,182,212,.18);">🍩</div>
        <div>
          <div class="card-title">פלטפורמות</div>
          <div class="card-sub">חלוקת הוצאה</div>
        </div>
      </div>
      <canvas id="platChart"></canvas>
    </div>
  </div>

  <div class="section-hdr">ביצועי קמפיינים</div>
  <div class="charts-row cols-1" style="margin-bottom:14px">
    <div class="card">
      <div class="card-hdr">
        <div class="card-icon" style="background:rgba(245,158,11,.18);">🏆</div>
        <div>
          <div class="card-title">Top קמפיינים לפי הוצאה</div>
          <div class="card-sub">10 מובילים בתקופה</div>
        </div>
      </div>
      <div class="tbl-wrap">
        <table id="campTable"></table>
      </div>
    </div>
  </div>

  <div class="section-hdr">דמוגרפיה</div>
  <div class="charts-row cols-1" style="margin-bottom:0">
    <div class="card">
      <div class="card-hdr">
        <div class="card-icon" style="background:rgba(16,185,129,.18);">👥</div>
        <div>
          <div class="card-title">הוצאה לפי גיל ומגדר</div>
          <div class="card-sub">breakdown לכל קבוצת גיל</div>
        </div>
      </div>
      <canvas id="demoChart"></canvas>
    </div>
  </div>

</div>

<div class="footer">נוצר אוטומטית · Meta Ads Premium Dashboard · {ed}</div>

<script>
const CAMP_NAMES   = {camp_names};
const CAMP_SPEND   = {camp_spend};
const CAMP_CLICKS  = {camp_clicks};
const CAMP_IMPR    = {camp_impr};
const CAMP_CTR     = {camp_ctr};
const DAILY_DATES  = {daily_dates};
const DAILY_SPEND  = {daily_spend};
const DAILY_CLICKS = {daily_clicks};
const DAILY_IMPR   = {daily_impr};
const PLAT_LABELS  = {plat_labels};
const PLAT_VALUES  = {plat_values};
const DEMO_AGE     = {demo_age_labels};
const DEMO_DS      = {demo_datasets};
{js_charts}
</script>
</body>
</html>"""

    return html


# ---------------------------------------------------------------------------
# Generic Report Renderer — works with ANY data source
# ---------------------------------------------------------------------------

@mcp.tool()
def render_report_pdf(
    title: str,
    metrics: str = "[]",
    table_headers: str = "[]",
    table_rows: str = "[]",
    time_series: str = "{}",
    subtitle: str = "",
    output_path: str = "",
) -> dict:
    """Generate a universal PDF report from any structured data.

    Pass data from ANY source (Google Ads, Meta, Analytics, GA4, etc.) and get
    a beautiful dark-themed PDF saved to disk.

    Args:
        title: Report title (e.g. "Google Ads Weekly Report").
        metrics: JSON list of KPI objects:
                 [{"label":"Spend","value":"$1,234","color":"#7c3aed"}, ...]
                 color is optional (defaults cycle through palette).
        table_headers: JSON list of column names: ["Campaign","Spend","Clicks"]
        table_rows: JSON list of rows (list of lists):
                    [["Campaign A","$500","1,200"],["Campaign B","$300","800"]]
        time_series: JSON object with optional line chart data:
                     {"labels":["01/01","02/01",...],
                      "series":[{"name":"Spend","values":[100,200,...],"color":"#7c3aed"},
                                {"name":"Clicks","values":[50,80,...],"color":"#06b6d4","secondary":true}]}
        subtitle: Optional date range or description shown under the title.
        output_path: Where to save the PDF. Defaults to ~/Desktop/<title>_YYYYMMDD.pdf
    """
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import matplotlib.gridspec as gridspec
        from matplotlib.patches import FancyBboxPatch
        import numpy as np
    except ImportError:
        return {"error": "matplotlib not installed. Run: pip install matplotlib numpy"}

    try:
        metrics_data    = json.loads(metrics)
        headers         = json.loads(table_headers)
        rows            = json.loads(table_rows)
        ts              = json.loads(time_series)
    except Exception as e:
        return {"error": f"JSON parse error: {e}"}

    has_chart = bool(ts.get("labels") and ts.get("series"))
    has_table = bool(headers and rows)

    BG     = "#0d1117"; SURF = "#161b22"; SURF2 = "#1c2330"
    BORDER = "#30363d"; TEXT = "#e6edf3"; MUTED = "#8b949e"
    PALETTE = ["#7c3aed","#ec4899","#10b981","#f59e0b","#3b82f6","#06b6d4","#8b5cf6","#f87171","#34d399","#fbbf24"]

    plt.rcParams.update({
        "font.family":"DejaVu Sans","text.color":TEXT,
        "axes.facecolor":SURF,"axes.edgecolor":BORDER,
        "axes.labelcolor":MUTED,"xtick.color":MUTED,"ytick.color":MUTED,
        "figure.facecolor":BG,"grid.color":BORDER,"grid.alpha":0.45,"font.size":9,
    })

    # Determine layout
    n_sections = (1 if has_chart else 0) + (1 if has_table else 0)
    height_ratios = [0.055, 0.13]
    if has_chart: height_ratios.append(0.42)
    if has_table: height_ratios.append(0.395 if has_chart else 0.815)
    total = sum(height_ratios)
    height_ratios = [h/total for h in height_ratios]

    fig = plt.figure(figsize=(16.54, 11.69))
    fig.patch.set_facecolor(BG)

    outer = gridspec.GridSpec(
        len(height_ratios), 1, figure=fig,
        height_ratios=height_ratios,
        hspace=0.05, left=0.03, right=0.97, top=0.97, bottom=0.03,
    )

    # ── Header ───────────────────────────────────────────────────────────────
    ax_hdr = fig.add_subplot(outer[0])
    ax_hdr.set_facecolor("#13103a"); ax_hdr.set_xlim(0,1); ax_hdr.set_ylim(0,1); ax_hdr.axis("off")
    ax_hdr.text(0.015, 0.55, title, fontsize=17, fontweight="bold", color=TEXT, va="center")
    if subtitle:
        ax_hdr.text(0.985, 0.55, subtitle, fontsize=10, color=MUTED, va="center", ha="right")
    for i, c in enumerate(["#7c3aed","#6366f1","#ec4899","#f59e0b","#10b981","#06b6d4"]):
        ax_hdr.axhline(0.04, xmin=i/6, xmax=(i+1)/6, color=c, linewidth=2.5)

    # ── KPI tiles ────────────────────────────────────────────────────────────
    n_metrics = max(len(metrics_data), 1)
    cols = min(n_metrics, 8)
    kpi_gs = gridspec.GridSpecFromSubplotSpec(1, cols, subplot_spec=outer[1], wspace=0.07)
    for i, m in enumerate(metrics_data[:cols]):
        color = m.get("color", PALETTE[i % len(PALETTE)])
        ax = fig.add_subplot(kpi_gs[i])
        ax.set_facecolor(SURF); ax.set_xlim(0,1); ax.set_ylim(0,1); ax.axis("off")
        ax.add_patch(FancyBboxPatch((0, 0.92), 1, 0.08, boxstyle="square,pad=0",
                                    facecolor=color, alpha=0.9, transform=ax.transAxes, zorder=5))
        val_str = str(m.get("value", ""))
        fontsize = 11 if len(val_str) > 8 else 13
        ax.text(0.5, 0.58, val_str, fontsize=fontsize, fontweight="bold", color=TEXT, ha="center", va="center")
        ax.text(0.5, 0.24, str(m.get("label","")), fontsize=7.5, color=MUTED, ha="center", va="center")
        for spine in ax.spines.values():
            spine.set_edgecolor(BORDER); spine.set_linewidth(0.7); spine.set_visible(True)

    section_idx = 2

    # ── Time series chart ─────────────────────────────────────────────────────
    if has_chart:
        ax_line = fig.add_subplot(outer[section_idx])
        labels_ts = ts["labels"]
        series    = ts["series"]
        primary   = [s for s in series if not s.get("secondary")]
        secondary = [s for s in series if s.get("secondary")]

        if primary:
            x = range(len(labels_ts))
            s0 = primary[0]
            color0 = s0.get("color", PALETTE[0])
            ax_line.fill_between(x, s0["values"], alpha=0.14, color=color0)
            ax_line.plot(x, s0["values"], color=color0, linewidth=2.2, label=s0["name"])
            for extra in primary[1:]:
                ec = extra.get("color", PALETTE[1])
                ax_line.plot(x, extra["values"], color=ec, linewidth=1.8, label=extra["name"])

        if secondary:
            ax2 = ax_line.twinx()
            for s in secondary:
                sc = s.get("color", PALETTE[5])
                ax2.plot(x, s["values"], color=sc, linewidth=1.6, linestyle="--", alpha=0.85, label=s["name"])
            ax2.tick_params(colors=MUTED); ax2.spines[:].set_color(BORDER)
            lines2, lbs2 = ax2.get_legend_handles_labels()
        else:
            lines2, lbs2 = [], []

        step = max(1, len(labels_ts) // 14)
        ax_line.set_xticks(list(range(0, len(labels_ts), step)))
        ax_line.set_xticklabels([labels_ts[i] for i in range(0, len(labels_ts), step)], rotation=40, ha="right", fontsize=7.5)
        lines1, lbs1 = ax_line.get_legend_handles_labels()
        ax_line.legend(lines1+lines2, lbs1+lbs2, loc="upper left", fontsize=8,
                       facecolor=SURF2, edgecolor=BORDER, labelcolor=TEXT)
        ax_line.set_title("Trend", fontsize=10, fontweight="bold", color=TEXT, pad=8)
        ax_line.grid(True, axis="y"); ax_line.spines[:].set_color(BORDER)
        section_idx += 1

    # ── Table ────────────────────────────────────────────────────────────────
    if has_table:
        ax_tbl = fig.add_subplot(outer[section_idx])
        ax_tbl.set_facecolor(SURF); ax_tbl.axis("off")
        tbl = ax_tbl.table(
            cellText=rows, colLabels=headers,
            cellLoc="left", loc="upper center", bbox=[0, 0.0, 1, 0.96],
        )
        tbl.auto_set_font_size(False); tbl.set_fontsize(8)
        for (r, c), cell in tbl.get_celld().items():
            cell.set_facecolor(SURF2 if r == 0 else (SURF if r % 2 == 0 else "#191f2a"))
            cell.set_edgecolor(BORDER); cell.set_linewidth(0.4)
            cell.set_text_props(color=MUTED if r == 0 else TEXT,
                                fontweight="bold" if r == 0 else "normal",
                                fontsize=7 if r == 0 else 8)
        tbl.auto_set_column_width(list(range(len(headers))))

    # ── Save ─────────────────────────────────────────────────────────────────
    if not output_path:
        desktop = os.path.join(os.path.expanduser("~"), "Desktop")
        os.makedirs(desktop, exist_ok=True)
        safe_title = "".join(c if c.isalnum() or c in " _-" else "_" for c in title)[:40].strip()
        fname = f"{safe_title}_{str(date.today()).replace('-','')}.pdf"
        output_path = os.path.join(desktop, fname)

    plt.savefig(output_path, format="pdf", dpi=150, bbox_inches="tight", facecolor=BG)
    plt.close(fig)
    return {"success": True, "path": output_path}


@mcp.tool()
def render_report_html(
    title: str,
    metrics: str = "[]",
    table_headers: str = "[]",
    table_rows: str = "[]",
    time_series: str = "{}",
    subtitle: str = "",
    color_accent: str = "#7c3aed",
) -> str:
    """Generate a premium Figma-level HTML dashboard from any structured data.

    Works with ANY data source. Claude should display the returned HTML as an artifact.

    Args:
        title: Dashboard title.
        metrics: JSON list: [{"label":"Spend","value":"$1,234","color":"#7c3aed"}, ...]
        table_headers: JSON list of column names.
        table_rows: JSON list of rows (list of lists).
        time_series: JSON: {"labels":[...],"series":[{"name":"X","values":[...],"color":"#hex","secondary":true/false}]}
        subtitle: Date range or description shown under the title.
        color_accent: Primary accent color for the header gradient (hex).
    """
    try:
        metrics_data = json.loads(metrics)
        headers      = json.loads(table_headers)
        rows         = json.loads(table_rows)
        ts           = json.loads(time_series)
    except Exception as e:
        return f"<p>JSON parse error: {e}</p>"

    has_chart = bool(ts.get("labels") and ts.get("series"))
    has_table = bool(headers and rows)

    PALETTE = ["#7c3aed","#ec4899","#10b981","#f59e0b","#3b82f6","#06b6d4","#8b5cf6","#f87171","#34d399","#fbbf24"]
    KPI_COLORS = ["violet","pink","green","amber","blue","cyan","purple","teal","red","indigo"]

    # ── KPI HTML ─────────────────────────────────────────────────────────────
    kpi_html = ""
    for i, m in enumerate(metrics_data):
        color  = m.get("color", PALETTE[i % len(PALETTE)])
        klabel = KPI_COLORS[i % len(KPI_COLORS)]
        label  = m.get("label", "")
        value  = m.get("value", "")
        icon   = m.get("icon", "📊")
        kpi_html += f"""
    <div class="kpi kpi-{klabel}" style="--kpi-color:{color};">
      <div class="kpi-icon">{icon}</div>
      <div class="kpi-label">{label}</div>
      <div class="kpi-value">{value}</div>
    </div>"""

    # ── Table HTML ────────────────────────────────────────────────────────────
    table_html = ""
    if has_table:
        th_html = "".join(f"<th>{h}</th>" for h in headers)
        td_rows = ""
        for row in rows:
            tds = "".join(f"<td>{cell}</td>" for cell in row)
            td_rows += f"<tr>{tds}</tr>"
        table_html = f"""
  <div class="section-hdr">נתונים</div>
  <div class="card" style="margin-bottom:14px">
    <div class="card-hdr">
      <div class="card-icon" style="background:rgba(245,158,11,.18);">📋</div>
      <div class="card-title">{title}</div>
    </div>
    <div class="tbl-wrap">
      <table><thead><tr>{th_html}</tr></thead><tbody>{td_rows}</tbody></table>
    </div>
  </div>"""

    # ── Chart data JS ─────────────────────────────────────────────────────────
    chart_js_data = ""
    chart_html    = ""
    if has_chart:
        ts_labels  = json.dumps(ts["labels"], ensure_ascii=False)
        ts_series  = json.dumps(ts["series"], ensure_ascii=False)
        chart_js_data = f"const TS_LABELS={ts_labels};const TS_SERIES={ts_series};"
        chart_html = """
  <div class="section-hdr">טרנד</div>
  <div class="card" style="margin-bottom:14px">
    <div class="card-hdr">
      <div class="card-icon" style="background:rgba(124,58,237,.18);">📈</div>
      <div class="card-title">Trend</div>
    </div>
    <canvas id="tsChart"></canvas>
  </div>"""

    chart_draw_js = ""
    if has_chart:
        chart_draw_js = r"""
const DPR=window.devicePixelRatio||1;
const PALETTE=['#7c3aed','#ec4899','#10b981','#f59e0b','#3b82f6','#06b6d4','#8b5cf6','#f87171'];
function numFmt(n){n=Math.abs(+n);if(n>=1e6)return(n/1e6).toFixed(1)+'M';if(n>=1000)return(n/1000).toFixed(0)+'K';return n.toFixed(0);}
function initCanvas(id,H){
  const el=document.getElementById(id);if(!el)return null;
  const W=el.parentElement.clientWidth-2;
  el.style.width=W+'px';el.style.height=H+'px';
  el.width=Math.round(W*DPR);el.height=Math.round(H*DPR);
  const ctx=el.getContext('2d');ctx.scale(DPR,DPR);return{ctx,W,H};
}
function drawLine(id,labels,series,H){
  H=H||300;const r=initCanvas(id,H);if(!r)return;
  const{ctx,W}=r;const pT=24,pB=42,pL=64,pR=54;
  const cW=W-pL-pR,cH=H-pT-pB;
  const primary=series.filter(s=>!s.secondary);
  const secondary=series.filter(s=>s.secondary);
  const maxA=Math.max(...primary.flatMap(s=>s.values),1);
  const maxB=secondary.length?Math.max(...secondary.flatMap(s=>s.values),1):1;
  const step=cW/Math.max(labels.length-1,1);
  const GRID='rgba(255,255,255,0.07)',MUTED='#7c8db5',TEXT='#c9d4f0';
  for(let i=0;i<=5;i++){
    const y=pT+(cH/5)*i;
    ctx.beginPath();ctx.strokeStyle=GRID;ctx.lineWidth=1;ctx.moveTo(pL,y);ctx.lineTo(pL+cW,y);ctx.stroke();
    ctx.fillStyle=MUTED;ctx.font='9px Inter,system-ui';ctx.textAlign='right';
    ctx.fillText(numFmt(maxA-(maxA/5)*i),pL-6,y+4);
    if(secondary.length){ctx.textAlign='left';ctx.fillText(numFmt(maxB-(maxB/5)*i),pL+cW+6,y+4);}
  }
  const evry=Math.ceil(labels.length/14);
  ctx.fillStyle=MUTED;ctx.font='9px Inter,system-ui';ctx.textAlign='center';
  labels.forEach((l,i)=>{if(i%evry===0)ctx.fillText(l,pL+i*step,H-6);});
  [...primary,...secondary].forEach((ds,di)=>{
    const maxV=ds.secondary?maxB:maxA;
    const color=ds.color||PALETTE[di%PALETTE.length];
    const pts=ds.values.map((v,i)=>({x:pL+i*step,y:pT+cH-(v/maxV)*cH}));
    if(!ds.secondary&&pts.length>1){
      const grad=ctx.createLinearGradient(0,pT,0,pT+cH);
      grad.addColorStop(0,color+'33');grad.addColorStop(1,'rgba(0,0,0,0)');
      ctx.beginPath();ctx.moveTo(pts[0].x,pts[0].y);
      pts.slice(1).forEach(p=>ctx.lineTo(p.x,p.y));
      ctx.lineTo(pts[pts.length-1].x,pT+cH);ctx.lineTo(pts[0].x,pT+cH);ctx.closePath();
      ctx.fillStyle=grad;ctx.fill();
    }
    if(pts.length>1){
      ctx.beginPath();ctx.strokeStyle=color;ctx.lineWidth=ds.secondary?1.6:2.2;
      ctx.setLineDash(ds.secondary?[5,4]:[]);ctx.lineJoin='round';ctx.lineCap='round';
      ctx.moveTo(pts[0].x,pts[0].y);pts.slice(1).forEach(p=>ctx.lineTo(p.x,p.y));ctx.stroke();
      ctx.setLineDash([]);
    }
    pts.forEach(p=>{ctx.beginPath();ctx.arc(p.x,p.y,2.2,0,Math.PI*2);ctx.fillStyle=color;ctx.fill();});
  });
  let lx=pL+4;
  [...primary,...secondary].forEach((ds,di)=>{
    const color=ds.color||PALETTE[di%PALETTE.length];
    ctx.beginPath();ctx.strokeStyle=color;ctx.lineWidth=2;ctx.setLineDash(ds.secondary?[4,3]:[]);
    ctx.moveTo(lx,14);ctx.lineTo(lx+18,14);ctx.stroke();ctx.setLineDash([]);
    ctx.fillStyle=TEXT;ctx.font='9px Inter,system-ui';ctx.textAlign='left';
    ctx.fillText(ds.name,lx+22,18);lx+=ctx.measureText(ds.name).width+42;
  });
}
document.addEventListener('DOMContentLoaded',()=>drawLine('tsChart',TS_LABELS,TS_SERIES,300));
"""

    html = f"""<!DOCTYPE html>
<html dir="rtl" lang="he">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{title}</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap" rel="stylesheet">
<style>
:root{{
  --bg:#050c1f;--glass:rgba(255,255,255,0.04);--glass-b:rgba(255,255,255,0.08);--glass-h:rgba(255,255,255,0.07);
  --text:#c9d4f0;--text-hi:#edf1ff;--muted:#7c8db5;--subtle:#3d4f72;
  --violet:#7c3aed;--violet-g:rgba(124,58,237,.35);
  --pink:#ec4899;--pink-g:rgba(236,72,153,.35);
  --green:#10b981;--green-g:rgba(16,185,129,.35);
  --amber:#f59e0b;--amber-g:rgba(245,158,11,.35);
  --blue:#3b82f6;--blue-g:rgba(59,130,246,.35);
  --cyan:#06b6d4;--cyan-g:rgba(6,182,212,.35);
  --purple:#8b5cf6;--purple-g:rgba(139,92,246,.35);
  --teal:#14b8a6;--teal-g:rgba(20,184,166,.35);
  --red:#ef4444;--red-g:rgba(239,68,68,.35);
  --indigo:#6366f1;--indigo-g:rgba(99,102,241,.35);
  --radius:16px;
}}
*,*::before,*::after{{box-sizing:border-box;margin:0;padding:0;}}
body{{
  font-family:'Inter',system-ui,-apple-system,sans-serif;
  background:var(--bg);color:var(--text);direction:rtl;text-align:right;
  min-height:100vh;padding-bottom:60px;
  background-image:
    radial-gradient(ellipse 80% 50% at 20% -10%,{color_accent}22 0%,transparent 60%),
    radial-gradient(ellipse 60% 40% at 80% 110%,rgba(59,130,246,.1) 0%,transparent 55%);
}}
.hdr{{
  position:relative;padding:36px 44px 30px;
  display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:14px;
  border-bottom:1px solid var(--glass-b);backdrop-filter:blur(20px);
}}
.hdr::before{{
  content:'';position:absolute;inset:0;pointer-events:none;
  background:linear-gradient(135deg,{color_accent}18 0%,rgba(6,182,212,.06) 50%,transparent 100%);
}}
.hdr-left{{display:flex;flex-direction:column;gap:5px;position:relative;}}
.logo-mark{{
  width:44px;height:44px;border-radius:13px;
  background:linear-gradient(135deg,{color_accent},#ec4899);
  display:flex;align-items:center;justify-content:center;font-size:22px;
  box-shadow:0 0 22px {color_accent}55,0 4px 12px rgba(0,0,0,.4);
}}
.hdr-logo{{display:flex;align-items:center;gap:13px;}}
.hdr-title{{
  font-size:1.8rem;font-weight:800;letter-spacing:-.8px;
  background:linear-gradient(135deg,#fff 30%,{color_accent} 70%,#ec4899);
  -webkit-background-clip:text;-webkit-text-fill-color:transparent;background-clip:text;
}}
.hdr-sub{{color:var(--muted);font-size:.85rem;font-weight:400;margin-right:57px;}}
.badge{{
  background:var(--glass);border:1px solid var(--glass-b);
  border-radius:10px;padding:7px 16px;font-size:.82rem;color:var(--muted);font-weight:500;
}}
.live-dot{{width:8px;height:8px;border-radius:50%;background:var(--green);box-shadow:0 0 8px var(--green);animation:pulse 2s infinite;}}
@keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.4}}}}
.wrap{{max-width:1480px;margin:0 auto;padding:32px 44px 0;}}
.section-hdr{{
  font-size:.72rem;font-weight:700;letter-spacing:1.5px;text-transform:uppercase;
  color:var(--muted);margin-bottom:16px;display:flex;align-items:center;gap:8px;
}}
.section-hdr::after{{content:'';flex:1;height:1px;background:var(--glass-b);}}
.kpi-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:14px;margin-bottom:30px;}}
.kpi{{
  background:var(--glass);border:1px solid var(--glass-b);border-radius:var(--radius);
  padding:22px 20px 18px;position:relative;overflow:hidden;cursor:default;
  transition:transform .2s,box-shadow .2s,background .2s;backdrop-filter:blur(12px);
  box-shadow:0 0 0 1px var(--kpi-color,#7c3aed)44,0 4px 20px rgba(0,0,0,.3);
}}
.kpi:hover{{transform:translateY(-3px);background:var(--glass-h);box-shadow:0 0 20px var(--kpi-color,#7c3aed)55,0 8px 30px rgba(0,0,0,.4);}}
.kpi::after{{
  content:'';position:absolute;bottom:0;right:0;left:0;height:2px;
  border-radius:0 0 var(--radius) var(--radius);
  background:linear-gradient(90deg,var(--kpi-color,#7c3aed),var(--kpi-color,#7c3aed)88);
}}
.kpi-icon{{position:absolute;top:16px;left:18px;font-size:20px;opacity:.65;}}
.kpi-label{{font-size:.77rem;font-weight:500;color:var(--muted);margin-bottom:10px;text-transform:uppercase;letter-spacing:.6px;}}
.kpi-value{{font-size:1.75rem;font-weight:800;color:var(--text-hi);letter-spacing:-1px;font-variant-numeric:tabular-nums;line-height:1;}}
.card{{
  background:var(--glass);border:1px solid var(--glass-b);border-radius:var(--radius);
  padding:22px 24px;backdrop-filter:blur(12px);transition:box-shadow .2s;
}}
.card:hover{{box-shadow:0 8px 32px rgba(0,0,0,.35);}}
.card-hdr{{display:flex;align-items:center;gap:10px;margin-bottom:18px;}}
.card-icon{{width:32px;height:32px;border-radius:9px;display:flex;align-items:center;justify-content:center;font-size:15px;flex-shrink:0;}}
.card-title{{font-size:.93rem;font-weight:700;color:var(--text-hi);letter-spacing:-.2px;}}
.tbl-wrap{{overflow-x:auto;}}
table{{width:100%;border-collapse:collapse;font-size:.83rem;}}
th{{text-align:right;padding:10px 13px;color:var(--muted);font-weight:600;font-size:.73rem;border-bottom:1px solid var(--glass-b);white-space:nowrap;letter-spacing:.5px;text-transform:uppercase;}}
td{{padding:10px 13px;border-bottom:1px solid rgba(255,255,255,.04);white-space:nowrap;}}
tr:last-child td{{border-bottom:none;}}
tr:hover td{{background:rgba(255,255,255,.03);}}
canvas{{display:block;width:100%;}}
.footer{{text-align:center;padding:32px 0 0;color:var(--subtle);font-size:.78rem;border-top:1px solid var(--glass-b);margin-top:28px;}}
@media(max-width:750px){{.wrap{{padding:20px 16px 0;}}.hdr{{padding:24px 18px 20px;}}}}
</style>
</head>
<body>
<header class="hdr">
  <div class="hdr-left">
    <div class="hdr-logo">
      <div class="logo-mark">📊</div>
      <span class="hdr-title">{title}</span>
    </div>
    <span class="hdr-sub">{subtitle}</span>
  </div>
  <div class="hdr-right" style="display:flex;gap:10px;align-items:center;position:relative;">
    <div class="live-dot"></div>
    <div class="badge">{subtitle or title}</div>
  </div>
</header>
<div class="wrap">
  <div class="section-hdr" style="margin-top:28px;">מדדים עיקריים</div>
  <div class="kpi-grid">{kpi_html}</div>
  {chart_html}
  {table_html}
</div>
<div class="footer">Generated automatically · {title} · {str(date.today())}</div>
<script>
{chart_js_data}
{chart_draw_js}
</script>
</body>
</html>"""

    return html


