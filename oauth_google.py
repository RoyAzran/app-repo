"""
Google OAuth2 flow — two-step scope approach:

1. Sign-up flow  (/auth/google/start) — asks only for identity (openid, email, profile).
   The user sees a lightweight consent screen; no sensitive data is requested.
2. Service connect (/auth/google/connect) — requests the full API scopes needed by
   the MCP tools (Ads, GA4, GSC, Sheets, Drive).  Called after the user has an
   account and wants to actually use the tools.

Routes:
  GET /auth/google/start          – identity-only consent (sign-up)
  GET /auth/google/connect        – full-scope consent (service connection)
  GET /auth/google/callback       – exchange code, store refresh token
"""
import os
import secrets
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from database import SessionLocal, User, activate_oauth_code, create_oauth_state, consume_oauth_state, record_affiliate_referral, get_user_subscription, upsert_subscription
from auth import create_jwt

router = APIRouter(prefix="/auth/google", tags=["google-oauth"])


def _cfg():
    return (
        os.environ.get("GOOGLE_CLIENT_ID", ""),
        os.environ.get("GOOGLE_CLIENT_SECRET", ""),
    )


def _base_url(request: Request) -> str:
    """Return BASE_URL env var if set, otherwise derive from request headers."""
    base = os.environ.get("BASE_URL", "").rstrip("/")
    if base:
        return base
    proto = request.headers.get("x-forwarded-proto", request.url.scheme).split(",")[0].strip()
    host = request.headers.get("x-forwarded-host", request.url.netloc).split(",")[0].strip()
    return f"{proto}://{host}"

# Minimal scopes — only for identifying the user at sign-up
IDENTITY_SCOPES = " ".join([
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
])

# Full scopes — requested when the user actually connects their Google services
SERVICE_SCOPES = " ".join([
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/adwords",
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/webmasters",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
])


@router.get("/start")
async def google_start(request: Request, user_id: str = "", team_invite: str = ""):
    """Initiate identity-only Google OAuth (sign-up). Only asks for email/name."""
    client_id, _ = _cfg()
    state = secrets.token_urlsafe(32)
    # Embed invite token in state if present so the callback can auto-accept it
    state_value = f"invite:{team_invite}" if team_invite else user_id
    create_oauth_state(state, state_value)

    # Preserve UTM params in a cookie so the callback can read them after redirect
    utm_source = request.query_params.get("utm_source", "")
    utm_medium = request.query_params.get("utm_medium", "")
    utm_campaign = request.query_params.get("utm_campaign", "")

    utm_source = request.query_params.get("utm_source", "")
    utm_medium = request.query_params.get("utm_medium", "")
    utm_campaign = request.query_params.get("utm_campaign", "")

    params = {
        "client_id": client_id,
        "redirect_uri": f"{_base_url(request)}/auth/google/callback",
        "response_type": "code",
        "scope": IDENTITY_SCOPES,
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    response = RedirectResponse(url)
    # Store UTM in short-lived cookies for the callback
    if utm_source:
        response.set_cookie("utm_source", utm_source, max_age=600, httponly=True, samesite="lax")
    if utm_medium:
        response.set_cookie("utm_medium", utm_medium, max_age=600, httponly=True, samesite="lax")
    if utm_campaign:
        response.set_cookie("utm_campaign", utm_campaign, max_age=600, httponly=True, samesite="lax")
    return response


@router.get("/connect")
async def google_connect_services(request: Request, user_id: str = ""):
    """
    Initiate full-scope Google OAuth to connect marketing services.
    Called after the user already has an account and wants to use tools.
    """
    if not user_id:
        return HTMLResponse("<p>Missing user_id. <a href='/onboard'>Start over</a>.</p>", status_code=400)
    client_id, _ = _cfg()
    state = secrets.token_urlsafe(32)
    # Prefix with "services:" so the callback knows this is the full-scope flow
    create_oauth_state(state, f"services:{user_id}")

    params = {
        "client_id": client_id,
        "redirect_uri": f"{_base_url(request)}/auth/google/callback",
        "response_type": "code",
        "scope": SERVICE_SCOPES,
        "access_type": "offline",
        "prompt": "consent",
        "state": state,
    }
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params)
    return RedirectResponse(url)


@router.get("/callback")
async def google_callback(request: Request, code: str = "", state: str = "", error: str = ""):
    if error:
        return HTMLResponse(f"<p>Google OAuth error: {error}. <a href='/onboard'>Try again</a>.</p>", status_code=400)

    raw_state_value = consume_oauth_state(state)
    if raw_state_value is None:
        return HTMLResponse("<p>Invalid or expired OAuth state. <a href='/onboard'>Try again</a>.</p>", status_code=400)

    # Detect flow type from state prefix
    pending_oauth_code: str | None = None
    is_service_connect = False
    existing_user_id = ""
    pending_team_invite: str | None = None

    if isinstance(raw_state_value, str) and raw_state_value.startswith("oauth:"):
        # OAuth server flow (Claude connector) — full scopes
        remainder = raw_state_value[6:]
        if remainder.endswith(":meta"):
            pending_oauth_code = remainder[:-5]
            then_meta_flag = True
        else:
            pending_oauth_code = remainder
            then_meta_flag = False
    elif isinstance(raw_state_value, str) and raw_state_value.startswith("services:"):
        # Service connect flow — user already exists, connecting Google APIs
        is_service_connect = True
        existing_user_id = raw_state_value[9:]
    elif isinstance(raw_state_value, str) and raw_state_value.startswith("invite:"):
        # Team invite flow — accept invite after sign-in
        pending_team_invite = raw_state_value[7:]
    else:
        # Sign-up flow — identity-only scopes
        existing_user_id = raw_state_value

    client_id, client_secret = _cfg()
    redirect_uri = f"{_base_url(request)}/auth/google/callback"
    # Exchange authorization code for tokens
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://oauth2.googleapis.com/token",
            data={
                "code": code,
                "client_id": client_id,
                "client_secret": client_secret,
                "redirect_uri": redirect_uri,
                "grant_type": "authorization_code",
            },
        )

    if resp.status_code != 200:
        return HTMLResponse("<p>Token exchange failed. <a href='/onboard'>Try again</a>.</p>", status_code=400)

    token_data = resp.json()
    refresh_token = token_data.get("refresh_token")
    access_token = token_data.get("access_token")

    # Fetch user email/name
    async with httpx.AsyncClient() as client:
        info_resp = await client.get(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    user_info = info_resp.json()
    email = user_info.get("email", "")
    name = user_info.get("name", email)

    db = SessionLocal()
    is_new_user = False
    try:
        if existing_user_id:
            user = db.get(User, existing_user_id)
        else:
            user = db.query(User).filter(User.email == email).first()

        if user is None:
            is_new_user = True
            # Capture referral code from query param (?ref=CODE) if present
            ref_code = request.query_params.get("ref", "").strip().upper() or \
                       request.cookies.get("ref_code", "").strip().upper()
            user = User(
                email=email,
                name=name,
                referred_by=ref_code or None,
                utm_source=request.query_params.get("utm_source") or request.cookies.get("utm_source"),
                utm_medium=request.query_params.get("utm_medium") or request.cookies.get("utm_medium"),
                utm_campaign=request.query_params.get("utm_campaign") or request.cookies.get("utm_campaign"),
            )
            db.add(user)

        # Only store the refresh token if we got one (full-scope flows)
        if refresh_token:
            user.set_google_token(refresh_token)

        db.commit()
        db.refresh(user)
        user_id_final = user.id

        # Record affiliate referral if this is a new user with a ref code
        if is_new_user and user.referred_by:
            record_affiliate_referral(user.referred_by, user_id_final)
    finally:
        db.close()

    # Send welcome email for new sign-ups (async, best-effort)
    if is_new_user:
        try:
            from emails import send_welcome
            send_welcome(user_id_final, email, name)
        except Exception:
            pass  # never break signup for email failures

    # Team invite acceptance — auto-accept after Google sign-in
    new_cred_info: dict | None = None
    if pending_team_invite:
        try:
            from database import accept_team_invite, generate_mcp_credential, get_mcp_credential
            accepted = accept_team_invite(pending_team_invite, user_id_final)
            if accepted:
                # Generate a credential for this new team member
                if get_mcp_credential(user_id_final) is None:
                    client_id_cred, plain_secret = generate_mcp_credential(user_id_final)
                    new_cred_info = {"client_id": client_id_cred, "client_secret": plain_secret}
        except Exception:
            pass  # non-fatal

    # OAuth server flow: activate the pending code and redirect to connect-accounts page
    if pending_oauth_code:
        result = activate_oauth_code(pending_oauth_code, user_id_final)
        if result is None:
            return HTMLResponse("<p>OAuth flow expired. Please try connecting again.</p>", status_code=400)
        auto_meta = "&auto_meta=1" if then_meta_flag else ""
        return RedirectResponse(f"/oauth/connect-accounts?code={pending_oauth_code}&user_id={user_id_final}{auto_meta}")

    # Service connect flow: user already had account, just linked Google APIs
    if is_service_connect:
        if not refresh_token:
            return HTMLResponse(
                "<p>Google did not return a refresh token. "
                "<a href='/auth/google/connect?user_id=" + existing_user_id + "'>Try again</a>.</p>",
                status_code=400,
            )
        return RedirectResponse(f"/setup?user_id={user_id_final}")

    # Sign-up flow: issue a JWT and redirect back to the main page pricing section
    jwt_token = create_jwt(user_id_final)

    # If accepted a team invite, redirect to /manage with credentials in URL
    if pending_team_invite and new_cred_info:
        from urllib.parse import urlencode as _urlencode
        qs = _urlencode({
            "user_id": user_id_final,
            "token": jwt_token,
            "new_client_id": new_cred_info["client_id"],
            "new_client_secret": new_cred_info["client_secret"],
            "invited": "1",
        })
        return RedirectResponse(f"/manage?{qs}")

    # If accepted an invite but credential already existed, just go to dashboard
    if pending_team_invite:
        return RedirectResponse(f"/manage?token={jwt_token}&user_id={user_id_final}&invited=1")

    # Check if this user is an active team member (invited and already in a team)
    # If so, skip the payment step and go directly to the dashboard
    try:
        from database import get_team_for_member
        if not is_new_user and get_team_for_member(user_id_final):
            return RedirectResponse(f"/manage?token={jwt_token}&user_id={user_id_final}")
    except Exception:
        pass

    return RedirectResponse(f"/?google_ok=1&user_id={user_id_final}&token={jwt_token}")
