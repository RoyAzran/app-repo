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
from urllib.parse import quote, urlencode

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from auth import create_jwt
from database import (
    activate_mcp_session,
    activate_oauth_code,
    consume_oauth_code,
    create_oauth_state,
    create_pending_oauth_code,
    get_mcp_user_id_by_client_id,
    read_oauth_code,
    verify_mcp_credential,
)
router = APIRouter(tags=["oauth-server"])

# All Google scopes — OAuth server flow needs full scopes because
# Claude needs tool access immediately after connecting
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

    # Show a landing page so the user can choose what to connect
    # instead of being immediately redirected to Google.
    google_url = f"/oauth/do-google?pending_code={pending_code}"
    meta_url = f"/oauth/do-meta?pending_code={pending_code}"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Connect to Claude — MCP Ads</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: 'Inter', system-ui, sans-serif; background: #fafaf8; color: #0f0f0e; min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 24px; }}
    .card {{ background: #fff; border: 1px solid #e4e4df; border-radius: 18px; padding: 40px 36px; max-width: 440px; width: 100%; box-shadow: 0 4px 24px rgba(0,0,0,.06); }}
    .logo {{ font-weight: 800; font-size: 1.1rem; letter-spacing: -.4px; margin-bottom: 28px; display: flex; align-items: center; gap: 8px; }}
    .logo-mark {{ width: 28px; height: 28px; background: #d4622a; border-radius: 7px; display: flex; align-items: center; justify-content: center; font-size: 13px; font-weight: 900; color: #fff; }}
    h1 {{ font-size: 1.35rem; font-weight: 800; letter-spacing: -.5px; margin-bottom: 6px; }}
    .sub {{ font-size: .85rem; color: #5a5a55; margin-bottom: 28px; line-height: 1.5; }}
    .account-row {{ display: flex; align-items: center; gap: 14px; padding: 16px; border: 1px solid #e4e4df; border-radius: 12px; margin-bottom: 12px; }}
    .account-icon {{ width: 40px; height: 40px; border-radius: 10px; background: #f2f2ee; border: 1px solid #e4e4df; display: flex; align-items: center; justify-content: center; flex-shrink: 0; font-size: 1.1rem; }}
    .account-info {{ flex: 1; }}
    .account-name {{ font-size: .9rem; font-weight: 700; }}
    .account-scope {{ font-size: .75rem; color: #9a9a93; margin-top: 2px; }}
    .badge-req {{ font-size: .68rem; font-weight: 700; background: rgba(212,98,42,.1); color: #d4622a; border: 1px solid rgba(212,98,42,.2); border-radius: 99px; padding: 2px 8px; flex-shrink: 0; }}
    .badge-opt {{ font-size: .68rem; font-weight: 700; background: #f2f2ee; color: #9a9a93; border: 1px solid #e4e4df; border-radius: 99px; padding: 2px 8px; flex-shrink: 0; }}
    .divider {{ border: none; border-top: 1px solid #e4e4df; margin: 20px 0; }}
    .btn-google {{ display: flex; align-items: center; justify-content: center; gap: 10px; width: 100%; padding: 13px; border-radius: 10px; background: #4285f4; color: #fff; font-size: .95rem; font-weight: 700; text-decoration: none; border: none; cursor: pointer; font-family: inherit; transition: background .15s; margin-bottom: 10px; }}
    .btn-google:hover {{ background: #3367d6; }}
    .btn-meta-lg {{ display: flex; align-items: center; justify-content: center; gap: 10px; width: 100%; padding: 13px; border-radius: 10px; background: #1877f2; color: #fff; font-size: .95rem; font-weight: 700; text-decoration: none; border: none; cursor: pointer; font-family: inherit; transition: background .15s; }}
    .btn-meta-lg:hover {{ background: #1558b0; }}
  </style>
</head>
<body>
  <div class="card">
    <div class="logo"><div class="logo-mark">M</div> MCP Ads</div>
    <h1>Connect your accounts</h1>
    <p class="sub">Grant Claude access to your ad accounts and analytics. You can skip Meta and connect it later from your dashboard.</p>

    <div class="account-row">
      <div class="account-icon">
        <svg width="22" height="22" viewBox="0 0 24 24"><path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#4285F4"/><path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#34A853"/><path d="M5.84 14.1c-.22-.66-.35-1.36-.35-2.1s.13-1.44.35-2.1V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.83z" fill="#FBBC05"/><path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.83c.87-2.6 3.3-4.52 6.16-4.52z" fill="#EA4335"/></svg>
      </div>
      <div class="account-info">
        <div class="account-name">Google</div>
        <div class="account-scope">Ads · Analytics · Search Console · Sheets</div>
      </div>
    </div>

    <div class="account-row">
      <div class="account-icon">
        <svg width="22" height="22" viewBox="0 0 24 24" fill="#1877f2"><path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z"/></svg>
      </div>
      <div class="account-info">
        <div class="account-name">Meta Ads</div>
        <div class="account-scope">Facebook &amp; Instagram ad campaigns</div>
      </div>
    </div>

    <hr class="divider">

    <hr class="divider">
    <a href="{google_url}" class="btn-google">
      <svg width="18" height="18" viewBox="0 0 24 24"><path d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" fill="#fff"/><path d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" fill="#fff"/><path d="M5.84 14.1c-.22-.66-.35-1.36-.35-2.1s.13-1.44.35-2.1V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.83z" fill="#fff"/><path d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.83c.87-2.6 3.3-4.52 6.16-4.52z" fill="#fff"/></svg>
      Connect Google Ads, GSC, GA4 &amp; Sheets
    </a>
    <a href="{meta_url}" class="btn-meta-lg">
      <svg width="18" height="18" viewBox="0 0 24 24" fill="#fff"><path d="M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z"/></svg>
      Connect Meta Ads
    </a>
  </div>
</body>
</html>"""
    return HTMLResponse(html)


@router.get("/oauth/do-google")
async def oauth_do_google(request: Request, pending_code: str = "", then_meta: int = 0):
    """Actually redirect to Google — called when user clicks a connect button."""
    if not pending_code:
        return HTMLResponse("<p>Missing pending_code.</p>", status_code=400)

    internal_state = secrets.token_urlsafe(32)
    state_value = f"oauth:{pending_code}:meta" if then_meta else f"oauth:{pending_code}"
    create_oauth_state(internal_state, user_id=state_value)

    google_client_id = os.environ.get("GOOGLE_CLIENT_ID", "")
    base = os.environ.get("BASE_URL", "").rstrip("/") or _base_url(request)
    params = {
        "client_id": google_client_id,
        "redirect_uri": f"{base}/auth/google/callback",
        "response_type": "code",
        "scope": _GOOGLE_SCOPES,
        "access_type": "offline",
        "prompt": "consent",
        "state": internal_state,
    }
    return RedirectResponse("https://accounts.google.com/o/oauth2/v2/auth?" + urlencode(params))


@router.get("/oauth/do-meta")
async def oauth_do_meta(request: Request, pending_code: str = ""):
    """Redirect directly to Meta OAuth — called when user clicks 'Connect Meta Ads'."""
    if not pending_code:
        return HTMLResponse("<p>Missing pending_code.</p>", status_code=400)

    base = os.environ.get("BASE_URL", "").rstrip("/") or _base_url(request)
    # Encode pending_code into return_to so the Meta callback can activate it
    return_to = quote(f"{base}/oauth/meta-activated?pending_code={pending_code}", safe="")
    return RedirectResponse(f"/auth/meta/start?return_to={return_to}")


# ---------------------------------------------------------------------------
# Meta-only OAuth landing: activate pending code after Meta callback
# ---------------------------------------------------------------------------

@router.get("/oauth/meta-activated")
async def oauth_meta_activated(
    request: Request,
    pending_code: str = "",
    meta_ok: int = 0,
    user_id: str = "",
):
    """
    Called by /auth/meta/callback (via return_to) after Meta OAuth completes.
    Activates the pending OAuth code for the Meta-authenticated user, creates a
    trial subscription if needed, then shows the connect-accounts page.
    """
    if not pending_code:
        return HTMLResponse("<p>Missing pending_code.</p>", status_code=400)

    if not user_id:
        return HTMLResponse("<p>Meta authentication did not return a user ID. Please try again.</p>", status_code=400)

    # Activate the pending code for this user (Meta-only sign-up path)
    result = activate_oauth_code(pending_code, user_id)
    if result is None:
        return HTMLResponse("<p>OAuth session expired or already used. Please reconnect from Claude.</p>", status_code=400)

    # Forward to connect-accounts to show status and let user also add Google
    return RedirectResponse(
        f"/oauth/connect-accounts?code={pending_code}&user_id={user_id}&meta_ok=1"
    )


# ---------------------------------------------------------------------------
# Intermediate page: Google connected, optionally connect Meta
# ---------------------------------------------------------------------------

_OAUTH_EXPIRED_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Session Expired — MCP Ads</title>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap" rel="stylesheet">
  <style>
    *,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
    body{font-family:'Inter',system-ui,sans-serif;background:#fafaf8;color:#0f0f0e;min-height:100vh;display:flex;flex-direction:column;align-items:center;justify-content:center;padding:24px}
    .card{background:#fff;border:1px solid #e4e4df;border-radius:18px;padding:40px 36px;max-width:420px;width:100%;box-shadow:0 4px 24px rgba(0,0,0,.06);text-align:center}
    .logo{font-weight:800;font-size:1.1rem;letter-spacing:-.4px;margin-bottom:28px;display:flex;align-items:center;justify-content:center;gap:8px}
    .logo-mark{width:28px;height:28px;background:#d4622a;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:13px;font-weight:900;color:#fff}
    .icon{font-size:2.5rem;margin-bottom:14px}
    h1{font-size:1.25rem;font-weight:800;letter-spacing:-.4px;margin-bottom:10px}
    p{font-size:.88rem;color:#5a5a55;line-height:1.6;margin-bottom:20px}
    .hint{font-size:.78rem;color:#9a9a93;margin-top:4px}
    .btn{display:flex;align-items:center;justify-content:center;width:100%;padding:13px;border-radius:10px;font-size:.9rem;font-weight:700;border:none;cursor:pointer;font-family:inherit;text-decoration:none;transition:background .15s;margin-top:10px}
    .btn-dark{background:#0f0f0e;color:#fff}
    .btn-dark:hover{background:#2a2a28}
    .btn-outline{background:transparent;color:#0f0f0e;border:1px solid #d4d4ce}
    .btn-outline:hover{border-color:#5a5a55}
  </style>
</head>
<body>
  <div class="card">
    <div class="logo"><div class="logo-mark">M</div> MCP Ads</div>
    <div class="icon">&#9203;</div>
    <h1>Session expired</h1>
    <p>This authorization session has already been completed or has timed out.</p>
    <p>If you just connected your accounts, <strong>you&rsquo;re all set</strong> &mdash; go back to Claude and start using the tools.</p>
    <a href="https://claude.ai/new" class="btn btn-dark" target="_blank">Open Claude &rarr;</a>
    <a href="https://claude.ai/settings/integrations" class="btn btn-outline" target="_blank">Claude Settings &rarr;</a>
    <p class="hint">Need to reconnect? Go to Claude &rsaquo; Settings &rsaquo; Integrations and click your MCP server to re-authorize.</p>
  </div>
</body>
</html>"""

@router.get("/oauth/connect-accounts")
async def oauth_connect_accounts(
    request: Request,
    code: str = "",
    user_id: str = "",
    meta_ok: int = 0,
    auto_meta: int = 0,
):
    """Show after Google auth — lets users also connect Meta Ads before going to Claude."""
    info = read_oauth_code(code)
    if info is None:
        return HTMLResponse(_OAUTH_EXPIRED_HTML, status_code=400)

    return_to_url = quote(f"/oauth/connect-accounts?code={code}&user_id={user_id}&meta_ok=1", safe="")
    meta_start_url = f"/auth/meta/start?user_id={user_id}&return_to={return_to_url}"

    # If user clicked "Connect Meta" on page 1 and hasn't connected Meta yet, auto-redirect
    if auto_meta and not meta_ok:
        return RedirectResponse(meta_start_url)
    finish_url = f"/oauth/finish?code={code}"

    google_block = """
      <div class=\"account-row connected\">
        <div class=\"account-icon\">
          <svg width=\"22\" height=\"22\" viewBox=\"0 0 24 24\"><path d=\"M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z\" fill=\"#4285F4\"/><path d=\"M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z\" fill=\"#34A853\"/><path d=\"M5.84 14.1c-.22-.66-.35-1.36-.35-2.1s.13-1.44.35-2.1V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l3.66-2.83z\" fill=\"#FBBC05\"/><path d=\"M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.83c.87-2.6 3.3-4.52 6.16-4.52z\" fill=\"#EA4335\"/></svg>
        </div>
        <div class=\"account-info\">
          <div class=\"account-name\">Google</div>
          <div class=\"account-scope\">Ads &middot; Analytics &middot; Search Console &middot; Sheets</div>
        </div>
        <span class=\"check\">&#10003;</span>
      </div>"""

    if meta_ok:
        meta_block = """
      <div class=\"account-row connected\">
        <div class=\"account-icon\">
          <svg width=\"22\" height=\"22\" viewBox=\"0 0 24 24\" fill=\"#1877f2\"><path d=\"M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z\"/></svg>
        </div>
        <div class=\"account-info\">
          <div class=\"account-name\">Meta Ads</div>
          <div class=\"account-scope\">Facebook &amp; Instagram ad campaigns</div>
        </div>
        <span class=\"check\">&#10003;</span>
      </div>"""
    else:
        meta_block = f"""
      <div class=\"account-row\">
        <div class=\"account-icon\">
          <svg width=\"22\" height=\"22\" viewBox=\"0 0 24 24\" fill=\"#1877f2\"><path d=\"M24 12.073c0-6.627-5.373-12-12-12s-12 5.373-12 12c0 5.99 4.388 10.954 10.125 11.854v-8.385H7.078v-3.47h3.047V9.43c0-3.007 1.792-4.669 4.533-4.669 1.312 0 2.686.235 2.686.235v2.953H15.83c-1.491 0-1.956.925-1.956 1.874v2.25h3.328l-.532 3.47h-2.796v8.385C19.612 23.027 24 18.062 24 12.073z\"/></svg>
        </div>
        <div class=\"account-info\">
          <div class=\"account-name\">Meta Ads <span style=\"font-weight:400;color:#9a9a93;font-size:.8rem\">(optional)</span></div>
          <div class=\"account-scope\">Facebook &amp; Instagram ad campaigns</div>
          <a href=\"{meta_start_url}\" class=\"btn-meta\">Connect Meta Ads</a>
        </div>
      </div>"""

    html = f"""<!DOCTYPE html>
<html lang=\"en\">
<head>
  <meta charset=\"utf-8\">
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <title>Connect to Claude — MCP Ads</title>
  <link href=\"https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap\" rel=\"stylesheet\">
  <style>
    *, *::before, *::after {{ box-sizing: border-box; margin: 0; padding: 0; }}
    body {{ font-family: 'Inter', system-ui, sans-serif; background: #fafaf8; color: #0f0f0e; min-height: 100vh; display: flex; flex-direction: column; align-items: center; justify-content: center; padding: 24px; }}
    .card {{ background: #fff; border: 1px solid #e4e4df; border-radius: 18px; padding: 40px 36px; max-width: 440px; width: 100%; box-shadow: 0 4px 24px rgba(0,0,0,.06); }}
    .logo {{ font-weight: 800; font-size: 1.1rem; letter-spacing: -.4px; margin-bottom: 28px; display: flex; align-items: center; gap: 8px; }}
    .logo-mark {{ width: 28px; height: 28px; background: #d4622a; border-radius: 7px; display: flex; align-items: center; justify-content: center; font-size: 13px; font-weight: 900; color: #fff; }}
    h1 {{ font-size: 1.35rem; font-weight: 800; letter-spacing: -.5px; margin-bottom: 6px; }}
    .sub {{ font-size: .85rem; color: #5a5a55; margin-bottom: 28px; line-height: 1.5; }}
    .account-row {{ display: flex; align-items: center; gap: 14px; padding: 16px; border: 1px solid #e4e4df; border-radius: 12px; margin-bottom: 12px; }}
    .account-row.connected {{ border-color: #a7f3d0; background: #f0fdf4; }}
    .account-icon {{ width: 40px; height: 40px; border-radius: 10px; background: #f2f2ee; border: 1px solid #e4e4df; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }}
    .account-info {{ flex: 1; }}
    .account-name {{ font-size: .9rem; font-weight: 700; }}
    .account-scope {{ font-size: .75rem; color: #9a9a93; margin-top: 2px; }}
    .check {{ font-size: 1rem; color: #1a9e6e; font-weight: 700; flex-shrink: 0; }}
    .divider {{ border: none; border-top: 1px solid #e4e4df; margin: 20px 0; }}
    .btn-meta {{ display: inline-flex; align-items: center; gap: 8px; margin-top: 10px; padding: 9px 18px; border-radius: 8px; background: #1877f2; color: #fff; font-size: .85rem; font-weight: 700; text-decoration: none; border: none; cursor: pointer; font-family: inherit; }}
    .btn-finish {{ display: flex; align-items: center; justify-content: center; width: 100%; padding: 13px; border-radius: 10px; background: #0f0f0e; color: #fff; font-size: .95rem; font-weight: 700; text-decoration: none; border: none; cursor: pointer; font-family: inherit; }}
  </style>
</head>
<body>
  <div class=\"card\">
    <div class=\"logo\"><div class=\"logo-mark\">M</div> MCP Ads</div>
    <h1>Almost there!</h1>
    <p class=\"sub\">Connect your accounts, then continue to Claude.</p>
    {google_block}
    {meta_block}
    <hr class=\"divider\">
    <a href=\"{finish_url}\" class=\"btn-finish\">Continue to Claude &rarr;</a>
  </div>
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
        return HTMLResponse(_OAUTH_EXPIRED_HTML, status_code=400)
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
    # -----------------------------------------------------------------------
    # Client authentication
    # -----------------------------------------------------------------------
    # If client_id looks like a per-user MCP credential (prefix "mcp_"),
    # validate it against the credentials table.  Only one active JWT is
    # allowed per credential — issuing a new one revokes the previous one.
    #
    # If client_id is blank or not a known MCP credential, fall back to the
    # global OAUTH_CLIENT_SECRET env var (legacy / public-client support).
    # -----------------------------------------------------------------------
    resolved_user_id: str | None = None

    if client_id.startswith("mcp_"):
        if client_secret:
            # Full credential verification when secret is provided
            resolved_user_id = verify_mcp_credential(client_id, client_secret)
            if resolved_user_id is None:
                return JSONResponse({"error": "invalid_client"}, status_code=401)
        else:
            # Public-client / PKCE-only flow — Claude.ai does not send client_secret.
            # Security comes from PKCE code_verifier checked below.
            resolved_user_id = get_mcp_user_id_by_client_id(client_id)
            if resolved_user_id is None:
                return JSONResponse({"error": "invalid_client"}, status_code=401)
    else:
        # Legacy: optional global secret configured in env
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

    # If we authenticated via a per-user credential, the credential's user_id
    # MUST match the user who completed the Google OAuth flow.
    if resolved_user_id is not None and resolved_user_id != user_id:
        return JSONResponse(
            {"error": "invalid_grant", "error_description": "Credential does not match authenticated user."},
            status_code=400,
        )

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

    jwt_token, jti = create_jwt(user_id, return_jti=True)

    # Single-session enforcement for per-user credentials:
    # revoke any existing active JWT for this credential.
    if client_id.startswith("mcp_"):
        activate_mcp_session(client_id, jti)

    return JSONResponse({
        "access_token": jwt_token,
        "token_type": "bearer",
        "expires_in": 365 * 24 * 3600,  # 1 year
    })
