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
    proto = request.headers.get("x-forwarded-proto", request.url.scheme)
    host = request.headers.get("x-forwarded-host", request.url.netloc)
    return f"{proto}://{host}"


def _client_creds() -> tuple[str, str]:
    return (
        os.environ.get("OAUTH_CLIENT_ID", "claude"),
        os.environ.get("OAUTH_CLIENT_SECRET", ""),
    )


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
    expected_client_id, _ = _client_creds()
    if client_id != expected_client_id:
        return HTMLResponse(f"<p>Unknown client_id '{client_id}'.</p>", status_code=400)
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
    expected_client_id, expected_secret = _client_creds()

    if client_id != expected_client_id:
        return JSONResponse({"error": "invalid_client"}, status_code=401)
    # Only check client_secret if one is configured
    if expected_secret and client_secret != expected_secret:
        return JSONResponse({"error": "invalid_client"}, status_code=401)

    if grant_type != "authorization_code":
        return JSONResponse({"error": "unsupported_grant_type"}, status_code=400)

    code_data = consume_oauth_code(code)
    if code_data is None:
        return JSONResponse({"error": "invalid_grant", "error_description": "Code not found, expired, or already used."}, status_code=400)

    user_id, stored_challenge, stored_method, stored_redirect = code_data

    # PKCE verification
    if stored_challenge:
        if not code_verifier:
            return JSONResponse({"error": "invalid_grant", "error_description": "code_verifier required"}, status_code=400)
        if stored_method == "S256":
            digest = base64.urlsafe_b64encode(
                hashlib.sha256(code_verifier.encode()).digest()
            ).rstrip(b"=").decode()
            if digest != stored_challenge:
                return JSONResponse({"error": "invalid_grant", "error_description": "PKCE verification failed"}, status_code=400)

    # Validate redirect_uri matches
    if stored_redirect and redirect_uri and stored_redirect != redirect_uri:
        return JSONResponse({"error": "invalid_grant", "error_description": "redirect_uri mismatch"}, status_code=400)

    jwt_token = create_jwt(user_id)
    return JSONResponse({
        "access_token": jwt_token,
        "token_type": "bearer",
        "expires_in": 365 * 24 * 3600,  # 1 year
    })
