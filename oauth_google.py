"""
Google OAuth2 flow — handles all Google scopes in a single callback:
  Google Ads, GA4, GSC, Google Sheets, Drive.

Routes:
  GET /auth/google/start          – redirect to Google consent page
  GET /auth/google/callback       – exchange code, store refresh token
"""
import os
import secrets
from urllib.parse import urlencode

import httpx
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from database import SessionLocal, User, activate_oauth_code, create_oauth_state, consume_oauth_state

router = APIRouter(prefix="/auth/google", tags=["google-oauth"])


def _cfg():
    return (
        os.environ.get("GOOGLE_CLIENT_ID", ""),
        os.environ.get("GOOGLE_CLIENT_SECRET", ""),
    )


def _base_url(request: Request) -> str:
    """Derive base URL from request, respecting Vercel/proxy forwarded headers."""
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.url.netloc)
    return f"{proto}://{host}"

# All scopes needed across all Google platforms
SCOPES = " ".join([
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
async def google_start(request: Request, user_id: str = ""):
    """Initiate Google OAuth. Pass user_id if re-linking an existing account."""
    client_id, _ = _cfg()
    state = secrets.token_urlsafe(32)
    create_oauth_state(state, user_id)

    params = {
        "client_id": client_id,
        "redirect_uri": f"{_base_url(request)}/auth/google/callback",
        "response_type": "code",
        "scope": SCOPES,
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

    # Detect OAuth server flow (initiated by /oauth/authorize)
    pending_oauth_code: str | None = None
    if isinstance(raw_state_value, str) and raw_state_value.startswith("oauth:"):
        pending_oauth_code = raw_state_value[6:]
        existing_user_id = ""
    else:
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
        return HTMLResponse(f"<p>Token exchange failed: {resp.text}</p>", status_code=400)

    token_data = resp.json()
    refresh_token = token_data.get("refresh_token")
    access_token = token_data.get("access_token")

    if not refresh_token:
        return HTMLResponse(
            "<p>Google did not return a refresh_token. This usually means the account already "
            "authorized the app. <a href='/auth/google/start'>Re-authorize with prompt=consent</a>.</p>",
            status_code=400,
        )

    # Fetch user email
    async with httpx.AsyncClient() as client:
        info_resp = await client.get(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        )
    user_info = info_resp.json()
    email = user_info.get("email", "")
    name = user_info.get("name", email)

    db = SessionLocal()
    try:
        if existing_user_id:
            user = db.get(User, existing_user_id)
        else:
            user = db.query(User).filter(User.email == email).first()

        if user is None:
            user = User(email=email, name=name)
            db.add(user)

        user.set_google_token(refresh_token)
        db.commit()
        db.refresh(user)
        user_id_final = user.id
    finally:
        db.close()

    # OAuth server flow: activate the pending code and redirect back to Claude
    if pending_oauth_code:
        result = activate_oauth_code(pending_oauth_code, user_id_final)
        if result is None:
            return HTMLResponse("<p>OAuth flow expired. Please try connecting again.</p>", status_code=400)
        redirect_uri, original_state = result
        params = urlencode({"code": pending_oauth_code, "state": original_state})
        return RedirectResponse(f"{redirect_uri}?{params}")

    return RedirectResponse(f"/onboard?google_ok=1&user_id={user_id_final}")
