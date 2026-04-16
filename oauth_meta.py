"""
Meta OAuth2 flow — exchanges for a long-lived user token (60 days).

Routes:
  GET /auth/meta/start            – redirect to Facebook consent page
  GET /auth/meta/callback         – exchange code → long-lived token → store
  GET /auth/meta/refresh/{user_id} – manually refresh an expiring token
"""
import os
import secrets
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from database import SessionLocal, User, create_oauth_state, consume_oauth_state

router = APIRouter(prefix="/auth/meta", tags=["meta-oauth"])

GRAPH_BASE = "https://graph.facebook.com/v22.0"


def _cfg():
    return (
        os.environ.get("META_APP_ID", ""),
        os.environ.get("META_APP_SECRET", ""),
    )


def _base_url(request: Request) -> str:
    """Return BASE_URL env var if set, otherwise derive from request headers."""
    base = os.environ.get("BASE_URL", "").rstrip("/")
    if base:
        return base
    proto = request.headers.get("x-forwarded-proto", request.url.scheme).split(",")[0].strip()
    host = request.headers.get("x-forwarded-host", request.url.netloc).split(",")[0].strip()
    return f"{proto}://{host}"

SCOPES = ",".join([
    "ads_read",
    "ads_management",
    "business_management",
    "pages_show_list",
    "pages_read_engagement",
    "public_profile",
])


@router.get("/start")
async def meta_start(request: Request, user_id: str = "", return_to: str = ""):
    app_id, _ = _cfg()
    state = secrets.token_urlsafe(32)
    # Encode return_to into the state value so callback can redirect back
    state_value = f"{user_id}|return:{return_to}" if return_to else user_id
    create_oauth_state(state, state_value)

    params = {
        "client_id": app_id,
        "redirect_uri": f"{_base_url(request)}/auth/meta/callback",
        "scope": SCOPES,
        "response_type": "code",
        "state": state,
    }
    url = "https://www.facebook.com/dialog/oauth?" + urlencode(params)
    return RedirectResponse(url)


@router.get("/callback")
async def meta_callback(request: Request, code: str = "", state: str = "", error: str = ""):
    if error:
        return HTMLResponse(f"<p>Meta OAuth error: {error}. <a href='/onboard'>Try again</a>.</p>", status_code=400)

    raw_state_value = consume_oauth_state(state)
    if raw_state_value is None:
        return HTMLResponse("<p>Invalid or expired OAuth state. <a href='/onboard'>Try again</a>.</p>", status_code=400)

    # Detect return_to in state value
    return_to: str | None = None
    if "|return:" in (raw_state_value or ""):
        existing_user_id, return_to = raw_state_value.split("|return:", 1)
    else:
        existing_user_id = raw_state_value or ""

    app_id, app_secret = _cfg()
    redirect_uri = f"{_base_url(request)}/auth/meta/callback"
    # Step 1: Exchange code for short-lived token
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{GRAPH_BASE}/oauth/access_token",
            params={
                "client_id": app_id,
                "client_secret": app_secret,
                "redirect_uri": redirect_uri,
                "code": code,
            },
        )

    if resp.status_code != 200:
        return HTMLResponse(f"<p>Meta token exchange failed: {resp.text}</p>", status_code=400)

    short_token = resp.json().get("access_token")
    if not short_token:
        return HTMLResponse("<p>No access_token in Meta response.</p>", status_code=400)

    # Step 2: Exchange short-lived token for long-lived token (~60 days)
    async with httpx.AsyncClient() as client:
        ll_resp = await client.get(
            f"{GRAPH_BASE}/oauth/access_token",
            params={
                "grant_type": "fb_exchange_token",
                "client_id": app_id,
                "client_secret": app_secret,
                "fb_exchange_token": short_token,
            },
        )

    if ll_resp.status_code != 200:
        return HTMLResponse(f"<p>Long-lived token exchange failed: {ll_resp.text}</p>", status_code=400)

    ll_data = ll_resp.json()
    long_token = ll_data.get("access_token")
    expires_in = ll_data.get("expires_in", 5184000)  # default 60 days
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

    # Step 3: Fetch user email/name
    async with httpx.AsyncClient() as client:
        me_resp = await client.get(
            f"{GRAPH_BASE}/me",
            params={"fields": "id,name,email", "access_token": long_token},
        )

    me_data = me_resp.json()
    email = me_data.get("email", f"{me_data.get('id', 'unknown')}@meta.placeholder")
    name = me_data.get("name", email)

    db = SessionLocal()
    try:
        if existing_user_id:
            user = db.get(User, existing_user_id)
        else:
            user = db.query(User).filter(User.email == email).first()

        if user is None:
            user = User(email=email, name=name)
            db.add(user)

        user.set_meta_token(long_token)
        user.meta_token_expires_at = expires_at
        db.commit()
        db.refresh(user)
        user_id_final = user.id
    finally:
        db.close()

    if return_to:
        separator = "&" if "?" in return_to else "?"
        return RedirectResponse(f"{return_to}{separator}meta_ok=1&user_id={user_id_final}")
    return RedirectResponse(f"/onboard?meta_ok=1&user_id={user_id_final}")


@router.get("/refresh/{user_id}")
async def meta_refresh(user_id: str):
    """Manually refresh a user's Meta long-lived token before it expires."""
    db = SessionLocal()
    try:
        user = db.get(User, user_id)
        if not user:
            return {"error": "User not found"}
        current_token = user.get_meta_token()
        if not current_token:
            return {"error": "No Meta token stored for this user"}

        app_id, app_secret = _cfg()
        async with httpx.AsyncClient() as client:
            ll_resp = await client.get(
                f"{GRAPH_BASE}/oauth/access_token",
                params={
                    "grant_type": "fb_exchange_token",
                    "client_id": app_id,
                    "client_secret": app_secret,
                    "fb_exchange_token": current_token,
                },
            )

        if ll_resp.status_code != 200:
            return {"error": f"Refresh failed: {ll_resp.text}"}

        ll_data = ll_resp.json()
        new_token = ll_data.get("access_token")
        expires_in = ll_data.get("expires_in", 5184000)
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        user.set_meta_token(new_token)
        user.meta_token_expires_at = expires_at
        db.commit()

        return {"success": True, "expires_at": expires_at.isoformat()}
    finally:
        db.close()
