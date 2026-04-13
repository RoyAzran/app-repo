"""
RBAC permission definitions.

Roles:
  viewer  — read-only tools on all platforms
  editor  — read + write tools
  admin   — everything + admin REST API

Tool names that require at least editor role (write tools).
Everything NOT in WRITE_TOOLS is implicitly available to all roles.
"""
from enum import Enum


class Role(str, Enum):
    viewer = "viewer"
    editor = "editor"
    admin = "admin"


# ---------------------------------------------------------------------------
# Tools that mutate data — blocked for "viewer" role
# ---------------------------------------------------------------------------
WRITE_TOOLS: set[str] = {
    # ── Google Ads ──────────────────────────────────────────────────────────
    "gads_create_budget",
    "gads_create_campaign",
    "gads_create_adgroup",
    "gads_add_keywords",
    "gads_add_negative_keywords",
    "gads_update_campaign_status",
    "gads_update_adgroup_status",
    "gads_update_keyword_status",
    "gads_update_keyword_bid",
    "gads_update_campaign_budget",
    "gads_create_responsive_search_ad",
    "gads_pause_all_campaigns",
    "gads_enable_campaign",
    "gads_apply_recommendation",
    "gads_dismiss_recommendation",
    "gads_upload_image_asset",
    "gads_create_responsive_display_ad",
    "gads_create_asset_group",
    "gads_create_conversion_action",
    "gads_update_conversion_action",
    "gads_create_sitelink_asset",
    "gads_create_callout_asset",
    "gads_create_video_ad",
    "gads_bulk_create_keywords",
    "gads_create_label",
    "gads_apply_label",
    # ── Meta Ads ────────────────────────────────────────────────────────────
    "meta_update_status",
    "meta_update_budget",
    "meta_update_adset_budget",
    "meta_switch_budget_mode",
    "meta_create_campaign",
    "meta_create_adset",
    "meta_create_ad_creative",
    "meta_upload_ad_video",
    "meta_create_video_ad_creative",
    "meta_create_ad",
    "meta_duplicate_campaign",
    "meta_duplicate_adset",
    "meta_update_adset_targeting",
    "meta_create_audience",
    "meta_create_lookalike",
    "meta_delete_ad",
    "meta_delete_adset",
    "meta_delete_campaign",
    "meta_create_pixel",
    "meta_send_server_event",
    "meta_create_custom_conversion",
    "meta_upload_customer_list_to_audience",
    "meta_delete_audience",
    "meta_update_audience",
    "meta_create_website_audience",
    "meta_create_engagement_audience",
    "meta_get_ad_creative",
    "meta_update_ad_creative",
    "meta_upload_image_to_meta",
    "meta_create_carousel_ad_creative",
    "meta_create_story_ad_creative",
    "meta_create_collection_ad_creative",
    "meta_bulk_updater",
    "meta_image_pipeline",
    "meta_adset_matrix_builder",
    "meta_create_automated_rule",
    "meta_update_automated_rule",
    "meta_delete_automated_rule",
    "meta_create_ab_test",
    "meta_create_adset_with_dayparting",
    "meta_create_advantage_plus_campaign",
    "meta_copy_ad_to_adset",
    "meta_add_ad_account_user",
    "meta_remove_ad_account_user",
    "google_drive_upload_video_to_meta",
    # ── Google Search Console ────────────────────────────────────────────────
    "gsc_submit_sitemap",
    "gsc_delete_sitemap",
    # ── Google Sheets ────────────────────────────────────────────────────────
    "create_sheet",
    "write_sheet",
    "append_sheet",
    "update_sheet",
    "clear_sheet",
    "delete_sheet",
    "format_sheet",
    "create_chart",
    "share_sheet",
    # ── Meta Pages ───────────────────────────────────────────────────────────
    "create_page_post",
    "schedule_post",
    "update_post",
    "delete_post",
    "reply_to_comment",
    "delete_comment",
    "hide_comment",
    "send_message",
    "create_page_event",
    "upload_photo",
    "upload_video",
    "create_album",
    "like_post",
    "unlike_post",
    "block_user",
    "unblock_user",
    "respond_to_rating",
    "pin_post",
    "create_milestone",
}


def check_permission(tool_name: str, role: str) -> bool:
    """Return True if the role is allowed to call this tool."""
    if role in (Role.editor, Role.admin):
        return True
    # viewer: only read tools
    if role == Role.viewer:
        return tool_name not in WRITE_TOOLS
    return False


def require_editor(tool_name: str) -> str | None:
    """
    Check current user from context and return an error JSON string if denied.
    Returns None if the call is allowed.
    """
    from auth import current_user_ctx
    user = current_user_ctx.get(None)
    if user is None:
        import json
        return json.dumps({"error": "Not authenticated. Connect to the MCP server using a valid JWT."})
    if not check_permission(tool_name, user.role):
        import json
        return json.dumps({"error": f"Access denied: '{user.role}' role cannot call '{tool_name}'. Requires editor or admin role."})
    return None
