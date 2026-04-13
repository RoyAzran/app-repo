"""
OAuth 2.1 authorization server — enables Claude.ai's "Add custom connector" dialog.

Implements the authorization code flow with PKCE so Claude.ai can authenticate
users against this server without them needing to manage JWTs manually.

Routes:
  GET  /.well-known/oauth-authorization-server   – discovery metadata
  GET  /oauth/authorize                          – start auth (redirects to Google)
  POST /oauth/token                              – exchange code for JWT
"""
import base64
import hashlib
import os
import secrets
from urllib.parse import urlencode

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from auth import create_jwt
from database import activate_oauth_code, consume_oauth_code, create_oauth_state, create_pending_oauth_code

router = APIRouter(tags=["oauth-server"])

# All Google scopes (same set used by oauth_google.py)
_GOOGLE_SCOPES = " ".join([
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "https://www.googleapis.com/auth/adwords",
    "https://www.googleapis.com/auth/analytics.readonly",
    "https://www.googleapis.com/auth/webmasters",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
])


def _base_url(request: Request) -> str:
    proto = request.headers.get("x-forwarded-proto", request.url.scheme).split(",")[0].strip()
    host = request.headers.get("x-forwarded-host", request.url.netloc).split(",")[0].strip()
    return f"{proto}://{host}"


# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

@router.get("/.well-known/oauth-authorization-server")
async def oauth_metadata(request: Request):
    base = _base_url(request)
    return JSONResponse({
        "issuer": base,
        "authorization_endpoint": f"{base}/oauth/authorize",
        "token_endpoint": f"{base}/oauth/token",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["client_secret_post", "none"],
    })


# ---------------------------------------------------------------------------
# Authorization endpoint
# ---------------------------------------------------------------------------

@router.get("/oauth/authorize")
async def oauth_authorize(
    request: Request,
    response_type: str = "code",
    client_id: str = "",
    redirect_uri: str = "",
    state: str = "",
    code_challenge: str = "",
    code_challenge_method: str = "S256",
):
    if response_type != "code":
        return HTMLResponse("<p>Only response_type=code is supported.</p>", status_code=400)
    if not redirect_uri:
        return HTMLResponse("<p>redirect_uri is required.</p>", status_code=400)

    # Create a pending auth code row that will be activated after Google login
    pending_code = create_pending_oauth_code(
        redirect_uri=redirect_uri,
        code_challenge=code_challenge,
        code_challenge_method=code_challenge_method,
        client_id=client_id,
        original_state=state,
    )

    # Store internal CSRF state; use "oauth:<pending_code>" as the user_id slot
    # so google_callback can detect and handle the OAuth flow branch.
    internal_state = secrets.token_urlsafe(32)
    create_oauth_state(internal_state, user_id=f"oauth:{pending_code}")

    google_client_id = os.environ.get("GOOGLE_CLIENT_ID", "")
    params = {
        "client_id": google_client_id,
        "redirect_uri": f"{_base_url(request)}/auth/google/callback",
        "response_type": "code",
        "scope": _GOOGLE_SCOPES,
        "access_type": "offline",
        "prompt": "consent",
        "state": internal_state,
    }
    return RedirectResponse("https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params))


# ---------------------------------------------------------------------------
# Intermediate page: Google connected, optionally connect Meta
# ---------------------------------------------------------------------------

@router.get("/oauth/connect-accounts")
async def oauth_connect_accounts(
    request: Request,
    code: str = "",
    user_id: str = "",
    meta_ok: int = 0,
):
    """Show after Google auth — lets users also connect Meta Ads before going to Claude."""
    info = read_oauth_code(code)
    if info is None:
        return HTMLResponse("<p>OAuth session not found or expired. Please try connecting again.</p>", status_code=400)

    return_to_url = quote(f"/oauth/connect-accounts?code={code}&user_id={user_id}&meta_ok=1", safe="")
    meta_start_url = f"/auth/meta/start?user_id={user_id}&return_to={return_to_url}"
    finish_url = f"/oauth/finish?code={code}"

    google_block = """
      <div class=\"step connected\">
        <span class=\"icon\">&#10003;</span>
        <div>
          <strong>Google</strong> connected
          <div class=\"sub\">Ads &middot; Analytics &middot; Search Console &middot; Sheets</div>
        </div>
      </div>"""

    if meta_ok:
        meta_block = """
      <div class=\"step connected\">
        <span class=\"icon\">&#10003;</span>
        <div><strong>Meta Ads</strong> connected</div>
      </div>"""
    else:
        meta_block = f"""
      <div class=\"step\">
        <span class=\"icon\">&#9675;</span>
        <div>
          <strong>Meta Ads</strong> <span class=\"optional\">(optional)</span>
          <div class=\"sub\">Facebook &amp; Instagram ad campaigns</div>
          <a href=\"{meta_start_url}\" class=\"btn btn-meta\">Connect Meta Ads</a>
        </div>
      </div>"""

    html = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>Connect to Claude</title>
  <style>
    body {{ font-family: system-ui, -apple-system, sans-serif; max-width: 480px; margin: 60px auto; padding: 0 20px; color: #111; }}
    h1 {{ font-size: 1.4rem; margin-bottom: 4px; }}
    p.sub {{ color: #6b7280; margin-top: 0; }}
    .step {{ display: flex; gap: 14px; align-items: flex-start; padding: 16px; border: 1px solid #e5e7eb; border-radius: 10px; margin: 12px 0; }}
    .step.connected {{ border-color: #bbf7d0; background: #f0fdf4; }}
    .icon {{ font-size: 1.2rem; margin-top: 2px; min-width: 22px; text-align: center; color: #16a34a; }}
    .sub {{ font-size: 0.85rem; color: #6b7280; margin-top: 3px; }}
    .optional {{ font-weight: 400; color: #9ca3af; font-size: 0.85rem; }}
    .btn {{ display: inline-block; margin-top: 10px; padding: 8px 16px; border-radius: 6px; font-size: 0.9rem; font-weight: 600; text-decoration: none; }}
    .btn-meta {{ background: #1877F2; color: #fff; }}
    .btn-finish {{ background: #111827; color: #fff; padding: 12px 24px; font-size: 1rem; }}
    .divider {{ border: none; border-top: 1px solid #e5e7eb; margin: 20px 0; }}
  </style>
</head>
<body>
  <h1>Almost there!</h1>
  <p class=\"sub\">Connect your accounts, then continue to Claude.</p>
  {google_block}
  {meta_block}
  <hr class=\"divider\">
  <a href=\"{finish_url}\" class=\"btn btn-finish\">Continue to Claude &#8594;</a>
</body>
</html>"""
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# Finish: redirect browser back to Claude with the authorization code
# ---------------------------------------------------------------------------

@router.get("/oauth/finish")
async def oauth_finish(request: Request, code: str = ""):
    """Final step — redirect back to Claude with the authorization code."""
    info = read_oauth_code(code)
    if info is None:
        return HTMLResponse("<p>OAuth session not found or expired. Please try connecting again.</p>", status_code=400)
    _, redirect_uri, original_state = info
    params = urlencode({"code": code, "state": original_state})
    return RedirectResponse(f"{redirect_uri}?{params}")


# ---------------------------------------------------------------------------
# Token endpoint
# ---------------------------------------------------------------------------

@router.post("/oauth/token")
async def oauth_token(
    request: Request,
    grant_type: str = Form("authorization_code"),
    code: str = Form(""),
    redirect_uri: str = Form(""),
    code_verifier: str = Form(""),
    client_id: str = Form(""),
    client_secret: str = Form(""),
):
    # Only validate client_secret if one is explicitly configured (public clients have none)
    configured_secret = os.environ.get("OAUTH_CLIENT_SECRET", "")
    if configured_secret and client_secret != configured_secret:
        return JSONResponse({"error": "invalid_client"}, status_code=401)

    if grant_type != "authorization_code":
        return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)

    code_data = consume_oauth_code(code)
    if code_data is None:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "Code not found, expired, or already used."},
            status_code=400,
        )

    user_id, stored_challenge, stored_method, stored_redirect = code_data

    # PKCE verification (skip if no challenge stored)
    if stored_challenge:
        if not code_verifier:
            return JSONResponse(
                {"error": "invalid_grant", "error_description": "code_verifier required"},
                status_code=400,
            )
        if stored_method == "S256":
            digest = base64.urlsafe_b64encode(
                hashlib.sha256(code_verifier.encode()).digest()
            ).rstrip(b"=").decode()
            if digest != stored_challenge:
                return JSONResponse(
                    {"error": "invalid_grant", "error_description": "PKCE verification failed"},
                    status_code=400,
                )

    jwt_token = create_jwt(user_id)
    return JSONResponse({
        "access_token": jwt_token,
        "token_type": "bearer",
        "expires_in": 365 * 24 * 3600,  # 1 year
    })
