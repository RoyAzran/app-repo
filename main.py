"""
Main entry point for the Agency Remote MCP Server.

Architecture:
- FastAPI handles /onboard, /auth/*, /admin/* routes.
- mcp.streamable_http_app() is mounted at /mcp via an ASGI auth wrapper
  (NOT FastAPI middleware — FastAPI middleware is bypassed for mounted sub-apps).
- The ASGI wrapper validates the JWT, sets current_user_ctx, then forwards to MCP.
- All tool modules register on startup via `import mcp_server`.
"""
import os
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"), override=True)
import logging
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from contextvars import copy_context
from typing import Optional

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.orm import Session

from auth import create_jwt, current_user_ctx, get_jwt_jti, verify_jwt, verify_jwt_user, verify_jwt_user_any
from billing import cancel_subscription, create_checkout_url, create_or_get_customer
from database import (
    SessionLocal, Subscription, User, create_tables, get_db,
    get_usage_stats, upsert_subscription,
    revoke_all_user_sessions, count_active_sessions,
)
import mcp_server  # noqa: F401 — registers all tools onto mcp singleton
from mcp_instance import mcp
from oauth_google import router as google_router
from oauth_meta import router as meta_router
from oauth_server import router as oauth_server_router
from permissions import Role
from plans import DEFAULT_TRIAL_PLAN_ID, PAID_PLAN_ID, get_plan, is_within_call_limit
from usage import check_session_revoked, get_client_ip, track_session
from webhooks import router as webhooks_router
from affiliates import router as affiliates_router
from teams import router as teams_router

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Rate-limiting — in-memory sliding window (per-IP for auth, per-user for MCP)
# ---------------------------------------------------------------------------

_RATE_WINDOW = 60  # 1 minute window
_AUTH_RATE_LIMIT = int(os.environ.get("AUTH_RATE_LIMIT", "20"))   # auth req/min/IP
_MCP_RATE_LIMIT = int(os.environ.get("MCP_RATE_LIMIT", "120"))   # MCP req/min/user

_rate_counters: dict[str, list[float]] = defaultdict(list)

# In-process set of JTIs already seen this process lifetime.
# Used to skip the revoke_all_user_sessions DB write on every tool call —
# we only need to revoke once when a JTI is first observed.
_seen_jtis: set[str] = set()


def _check_rate_limit(key: str, limit: int) -> bool:
    """Return True if the request is WITHIN the rate limit."""
    now = time.time()
    window_start = now - _RATE_WINDOW
    # Prune old entries
    timestamps = _rate_counters[key]
    _rate_counters[key] = [t for t in timestamps if t > window_start]
    if len(_rate_counters[key]) >= limit:
        return False
    _rate_counters[key].append(now)
    return True


# ---------------------------------------------------------------------------
# Build the MCP ASGI app early — lifespan needs _session_manager
# ---------------------------------------------------------------------------
_mcp_starlette = mcp.streamable_http_app()


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_tables()
    # Start the MCP session manager's task group so handle_request works
    async with mcp._session_manager.run():
        yield


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Agency MCP Server",
    description="Unified remote MCP server for marketing tools",
    version="1.0.0",
    lifespan=lifespan,
)

# ---------------------------------------------------------------------------
# CORS — restrict to known origins in production
# ---------------------------------------------------------------------------
_ALLOWED_ORIGINS = [
    o.strip()
    for o in os.environ.get("ALLOWED_ORIGINS", "*").split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=_ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)

# Serve static HTML/CSS/JS from ./static
app.mount("/static", StaticFiles(directory="static"), name="static")


# ---------------------------------------------------------------------------
# Security headers middleware
# ---------------------------------------------------------------------------
@app.middleware("http")
async def security_headers(request: Request, call_next):
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    if os.environ.get("ENABLE_HSTS", "").lower() == "true":
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
    return response


# ---------------------------------------------------------------------------
# Rate limiting middleware for FastAPI routes (auth, billing, admin)
# ---------------------------------------------------------------------------
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    ip = request.client.host if request.client else "unknown"
    path = request.url.path

    # Only rate-limit auth/billing/admin endpoints (MCP is handled separately)
    if path.startswith(("/auth/", "/oauth/", "/billing/", "/admin/")):
        if not _check_rate_limit(f"ip:{ip}", _AUTH_RATE_LIMIT):
            return JSONResponse(
                {"detail": "Rate limit exceeded. Try again shortly."},
                status_code=429,
            )
    return await call_next(request)

app.include_router(google_router)
app.include_router(meta_router)
app.include_router(oauth_server_router)
app.include_router(webhooks_router)
app.include_router(affiliates_router)
app.include_router(teams_router)


# ---------------------------------------------------------------------------
# ASGI auth wrapper — wraps the MCP sub-app at ASGI level
# This is necessary because FastAPI middleware is bypassed for mounted apps.
# ---------------------------------------------------------------------------

class MCPAuthWrapper:
    """
    ASGI middleware that validates JWT, checks subscription status,
    enforces session tracking, single active connection, rate limits,
    and plan call limits. Injects user into current_user_ctx.
    """

    def __init__(self, app):
        self._app = app

    async def _deny(self, scope, send, status_code: int, detail: str):
        body = JSONResponse({"detail": detail}).body
        if scope["type"] == "http":
            await send({"type": "http.response.start", "status": status_code,
                "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())]})
            await send({"type": "http.response.body", "body": body})
        else:
            await send({"type": "websocket.close", "code": 4001})

    async def __call__(self, scope, receive, send):
        if scope["type"] not in ("http", "websocket"):
            await self._app(scope, receive, send)
            return

        # Extract Bearer token from Authorization header
        headers = dict(scope.get("headers", []))
        auth_header: bytes = headers.get(b"authorization", b"")
        token: Optional[str] = None
        if auth_header.startswith(b"Bearer "):
            token = auth_header[7:].decode("utf-8", errors="replace")

        auth_result = verify_jwt(token) if token else None

        if auth_result is None:
            await self._deny(scope, send, 401,
                "Not authenticated. Provide a valid Bearer JWT. Visit /onboard to get a token.")
            return

        user, sub = auth_result

        # Check JWT revocation via jti
        jti = get_jwt_jti(token) if token else None
        if check_session_revoked(jti):
            await self._deny(scope, send, 401,
                "This token has been revoked. Please re-authenticate at /onboard.")
            return

        # ---- Rate limit per user on MCP endpoint ----
        if not _check_rate_limit(f"mcp:{user.id}", _MCP_RATE_LIMIT):
            await self._deny(scope, send, 429, "Rate limit exceeded. Try again shortly.")
            return

        # ---- Determine the user's plan (reuse sub already fetched by verify_jwt) ----
        plan_id = PAID_PLAN_ID if (sub is None or sub.is_active) else DEFAULT_TRIAL_PLAN_ID
        plan = get_plan(plan_id)

        # ---- Enforce max concurrent sessions — only revoke on first-seen JTI ----
        # Using an in-process set avoids a DB write on every subsequent tool call.
        if plan.max_sessions is not None and jti and jti not in _seen_jtis:
            _seen_jtis.add(jti)
            if len(_seen_jtis) > 50_000:   # prevent unbounded growth
                _seen_jtis.clear()
            revoke_all_user_sessions(user.id, except_jti=jti)

        # ---- Enforce monthly tool-call limit ----
        usage = get_usage_stats(user.id, days=30)
        calls_this_month = usage.get("total_calls_period", 0) if isinstance(usage, dict) else 0
        if not is_within_call_limit(plan_id, calls_this_month):
            await self._deny(scope, send, 403,
                f"Monthly tool call limit reached ({plan.monthly_tool_calls}). "
                f"Upgrade to Pro for unlimited access.")
            return

        # Record session fingerprint & check for credential sharing
        if jti:
            try:
                allowed = track_session(user.id, jti, scope)
                if not allowed:
                    await self._deny(scope, send, 403,
                        "Access blocked: unusual login pattern detected. "
                        "Please contact support if this is unexpected.")
                    return
            except Exception:
                pass  # never block a request for telemetry failure

        # Set user in ContextVar for this request, then call the MCP app
        ctx = copy_context()

        async def run_in_ctx():
            current_user_ctx.set(user)
            await self._app(scope, receive, send)

        await ctx.run(run_in_ctx)


# ---------------------------------------------------------------------------
# Route /mcp at the ASGI level — before FastAPI routing.
# app.mount() strips the path prefix, causing mismatches with the Starlette
# sub-app's route at /mcp.  Instead, we intercept at the ASGI layer.
# ---------------------------------------------------------------------------

_mcp_wrapped = MCPAuthWrapper(_mcp_starlette)
_original_asgi = app.build_middleware_stack  # FastAPI builds this lazily

class _ASGIInterceptor:
    """Requests to /mcp (or /mcp/) go to the MCP sub-app;
    everything else goes through FastAPI."""

    def __init__(self):
        self._fastapi_app = None

    def _get_fastapi(self):
        if self._fastapi_app is None:
            self._fastapi_app = _original_asgi()
        return self._fastapi_app

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "")
        if scope["type"] in ("http", "websocket") and path.rstrip("/") == "/mcp":
            scope = dict(scope)
            scope["path"] = "/mcp"
            await _mcp_wrapped(scope, receive, send)
        else:
            await self._get_fastapi()(scope, receive, send)

app.build_middleware_stack = lambda: _ASGIInterceptor()


# ---------------------------------------------------------------------------
# Static pages
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
async def landing():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "index.html"))


@app.get("/robots.txt", include_in_schema=False)
async def robots():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "robots.txt"), media_type="text/plain")


@app.get("/sitemap.xml", include_in_schema=False)
async def sitemap():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "sitemap.xml"), media_type="application/xml")


@app.get("/onboard", include_in_schema=False)
async def onboard():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "onboard.html"))


@app.get("/connect", include_in_schema=False)
async def connect_guide():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "connect.html"))


@app.get("/pricing", include_in_schema=False)
async def pricing_redirect():
    from fastapi.responses import RedirectResponse as _Redirect
    return _Redirect(url="/#pricing", status_code=301)


@app.get("/setup", include_in_schema=False)
async def setup_guide():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "setup.html"))


@app.get("/manage", include_in_schema=False)
async def manage_page(token: str = ""):
    if not token:
        from fastapi.responses import RedirectResponse as _Redirect
        return _Redirect(url="/#pricing", status_code=302)
    user = verify_jwt_user(token)
    if user is None:
        from fastapi.responses import RedirectResponse as _Redirect
        return _Redirect(url="/#pricing", status_code=302)
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "manage.html"))


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Billing routes
# ---------------------------------------------------------------------------

def _get_current_user(request: Request) -> User:
    """Dependency: validate JWT and return the User (non-admin). Requires active subscription."""
    auth_header = request.headers.get("Authorization", "")
    token = auth_header.removeprefix("Bearer ").strip() if auth_header.startswith("Bearer ") else ""
    user = verify_jwt_user(token) if token else None
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated.")
    return user


def _get_current_user_any(request: Request) -> User:
    """Dependency: validate JWT, no subscription required. For pre-payment endpoints."""
    auth_header = request.headers.get("Authorization", "")
    token = auth_header.removeprefix("Bearer ").strip() if auth_header.startswith("Bearer ") else ""
    user = verify_jwt_user_any(token) if token else None
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated.")
    return user


@app.post("/billing/checkout")
async def billing_checkout(request: Request, user: User = Depends(_get_current_user_any)):
    """
    Create an Airwallex hosted payment page URL for the logged-in user.
    Accepts optional JSON body: {"seats": 2}  (default 1 = solo plan)
    """
    seats = 1
    try:
        body = await request.json()
        seats = max(1, int(body.get("seats", 1)))
    except Exception:
        pass

    db_sess = SessionLocal()
    try:
        sub = db_sess.query(Subscription).filter(Subscription.user_id == user.id).first()
        existing_customer_id = sub.airwallex_customer_id if sub else None
    finally:
        db_sess.close()

    try:
        checkout_url = await create_checkout_url(
            user_id=user.id,
            email=user.email,
            name=user.name,
            airwallex_customer_id=existing_customer_id,
            seats=seats,
        )
    except Exception as e:
        logger.error("Checkout creation failed for user %s: %s", user.id, e)
        raise HTTPException(status_code=500, detail="Could not create checkout session. Please try again.")

    return {"checkout_url": checkout_url}


@app.get("/billing/status")
def billing_status(request: Request, user: User = Depends(_get_current_user_any)):
    """Return current subscription status, usage stats, and connection info for the logged-in user."""
    from database import get_team_for_member, get_team_by_owner
    db_sess = SessionLocal()
    try:
        sub = db_sess.query(Subscription).filter(Subscription.user_id == user.id).first()
        # Refresh user to get latest token fields
        db_user = db_sess.get(User, user.id)
    finally:
        db_sess.close()

    usage = get_usage_stats(user.id, days=30)
    plan = get_plan(sub.status if sub else DEFAULT_TRIAL_PLAN_ID)

    google_connected = bool(db_user and db_user.google_refresh_token_enc)
    meta_connected = bool(db_user and db_user.meta_access_token_enc)

    # Team role detection
    owner_team = get_team_by_owner(user.id)
    member_team = get_team_for_member(user.id) if not owner_team else None
    is_team_owner = owner_team is not None
    is_team_member = member_team is not None and not is_team_owner
    team_name = (owner_team or member_team).name if (owner_team or member_team) else None

    return {
        "user_id": user.id,
        "email": user.email,
        "name": db_user.name if db_user else "",
        "google_connected": google_connected,
        "meta_connected": meta_connected,
        "is_team_owner": is_team_owner,
        "is_team_member": is_team_member,
        "team_name": team_name,
        "subscription": {
            "status": sub.status if sub else "none",
            "plan": PAID_PLAN_ID if sub and sub.is_active else DEFAULT_TRIAL_PLAN_ID,
            "current_period_end": sub.current_period_end.isoformat() if sub and sub.current_period_end else None,
            "trial_ends_at": sub.trial_ends_at.isoformat() if sub and sub.trial_ends_at else None,
            "airwallex_subscription_id": sub.airwallex_subscription_id if sub else None,
        },
        "usage": usage,
        "plan_details": {
            "name": plan.display_name,
            "monthly_tool_calls": plan.monthly_tool_calls or "unlimited",
        },
    }


@app.post("/billing/cancel")
async def billing_cancel(request: Request, user: User = Depends(_get_current_user)):
    """Cancel the logged-in user's Airwallex subscription."""
    db_sess = SessionLocal()
    try:
        sub = db_sess.query(Subscription).filter(Subscription.user_id == user.id).first()
    finally:
        db_sess.close()

    if not sub or not sub.airwallex_subscription_id:
        raise HTTPException(status_code=404, detail="No active subscription found.")

    try:
        ok = await cancel_subscription(sub.airwallex_subscription_id)
    except Exception as e:
        logger.error("Subscription cancellation failed for user %s: %s", user.id, e)
        raise HTTPException(status_code=500, detail="Cancellation failed. Please try again.")

    if ok:
        upsert_subscription(user.id, status="canceled")
        return {"success": True, "message": "Subscription cancelled."}
    raise HTTPException(status_code=500, detail="Airwallex cancellation failed.")


@app.get("/billing/success", include_in_schema=False)
async def billing_success(user_id: str = ""):
    """Airwallex redirects here after successful checkout — issue JWT and redirect to dashboard."""
    from fastapi.responses import RedirectResponse
    if not user_id:
        return RedirectResponse(url="/manage", status_code=302)
    # Issue a fresh JWT so the dashboard can load immediately (no localStorage dependency)
    try:
        token = create_jwt(user_id)
        return RedirectResponse(url=f"/manage?token={token}&user_id={user_id}&paid=1", status_code=302)
    except Exception:
        return RedirectResponse(url="/manage", status_code=302)


@app.get("/billing/start", include_in_schema=False)
async def billing_start(user_id: str = "", seats: int = 1):
    """
    Browser redirect: look up the user by ID, create an Airwallex checkout
    session, and redirect the browser straight to the Airwallex payment page.
    Called after Google OAuth — no JWT required.
    """
    from fastapi.responses import RedirectResponse

    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id")

    db_sess = SessionLocal()
    try:
        user = db_sess.get(User, user_id)
        if not user:
            raise HTTPException(status_code=404, detail="User not found")
        sub = db_sess.query(Subscription).filter(Subscription.user_id == user.id).first()
        existing_customer_id = sub.airwallex_customer_id if sub else None
        email = user.email
        name = user.name
    finally:
        db_sess.close()

    try:
        checkout_url = await create_checkout_url(
            user_id=user_id,
            email=email,
            name=name,
            airwallex_customer_id=existing_customer_id,
            seats=max(1, seats),
        )
    except Exception as e:
        logger.error("Checkout error for user %s: %s", user_id, e)
        raise HTTPException(status_code=500, detail="Could not create checkout session. Please try again.")

    return RedirectResponse(url=checkout_url, status_code=302)


@app.get("/billing/cancel", include_in_schema=False)
async def billing_cancel_redirect(user_id: str = ""):
    """Airwallex redirects here if the user cancels the payment flow."""
    return JSONResponse({"message": "Checkout cancelled. You can restart it from /billing/checkout.", "user_id": user_id})


# ---------------------------------------------------------------------------
# Session management
# ---------------------------------------------------------------------------

@app.get("/sessions")
def list_sessions(request: Request, user: User = Depends(_get_current_user), db: Session = Depends(get_db)):
    """Return the active sessions for the current user."""
    from database import UserSession
    sessions = db.query(UserSession).filter(
        UserSession.user_id == user.id,
        UserSession.revoked == False,
    ).order_by(UserSession.last_seen.desc()).all()
    return [
        {
            "id": s.id,
            "ip_address": s.ip_address,
            "last_seen": s.last_seen.isoformat() if s.last_seen else None,
            "created_at": s.created_at.isoformat() if s.created_at else None,
        }
        for s in sessions
    ]


@app.delete("/sessions/{session_id}")
def revoke_session_endpoint(
    session_id: str,
    request: Request,
    user: User = Depends(_get_current_user),
    db: Session = Depends(get_db),
):
    """Revoke a specific session (token) by its database ID."""
    from database import UserSession
    from database import revoke_session
    row = db.query(UserSession).filter(
        UserSession.id == session_id,
        UserSession.user_id == user.id,
    ).first()
    if not row:
        raise HTTPException(404, detail="Session not found.")
    revoke_session(row.jti)
    return {"revoked": True}


@app.post("/auth/refresh")
def refresh_token(request: Request, user: User = Depends(_get_current_user)):
    """
    Issue a fresh JWT for the current user.
    The old token remains valid until it expires — call /sessions to revoke it.
    """
    new_token = create_jwt(user.id)
    return {"jwt_token": new_token, "expires_in_days": 7}


# ---------------------------------------------------------------------------
# Admin helpers — require admin role
# ---------------------------------------------------------------------------

def _require_admin(request: Request, db: Session = Depends(get_db)) -> User:
    auth_header = request.headers.get("Authorization", "")
    token = auth_header.removeprefix("Bearer ").strip() if auth_header.startswith("Bearer ") else ""
    user = verify_jwt_user(token) if token else None
    if user is None or user.role != Role.admin.value:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Admin access required.")
    return user


class UserCreate(BaseModel):
    email: str
    name: str
    role: str = "viewer"


class UserUpdate(BaseModel):
    name: Optional[str] = None
    role: Optional[str] = None
    is_active: Optional[bool] = None


@app.get("/admin/users")
def admin_list_users(admin: User = Depends(_require_admin), db: Session = Depends(get_db)):
    users = db.query(User).all()
    return [{"id": u.id, "email": u.email, "name": u.name, "role": u.role, "is_active": u.is_active, "has_google": bool(u.google_refresh_token_enc), "has_meta": bool(u.meta_access_token_enc)} for u in users]


@app.post("/admin/users", status_code=201)
def admin_create_user(body: UserCreate, admin: User = Depends(_require_admin), db: Session = Depends(get_db)):
    if body.role not in (r.value for r in Role):
        raise HTTPException(400, detail=f"Invalid role. Choose from: {[r.value for r in Role]}")
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(409, detail="A user with this email already exists.")
    import uuid
    user = User(id=str(uuid.uuid4()), email=body.email, name=body.name, role=body.role)
    db.add(user)
    db.commit()
    db.refresh(user)
    jwt_token = create_jwt(user.id)
    return {"id": user.id, "email": user.email, "name": user.name, "role": user.role, "jwt_token": jwt_token}


@app.patch("/admin/users/{user_id}")
def admin_update_user(user_id: str, body: UserUpdate, admin: User = Depends(_require_admin), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, detail="User not found.")
    if body.role is not None:
        if body.role not in (r.value for r in Role):
            raise HTTPException(400, detail=f"Invalid role. Choose from: {[r.value for r in Role]}")
        user.role = body.role
    if body.name is not None:
        user.name = body.name
    if body.is_active is not None:
        user.is_active = body.is_active
    db.commit()
    return {"id": user.id, "email": user.email, "name": user.name, "role": user.role, "is_active": user.is_active}


@app.delete("/admin/users/{user_id}", status_code=204)
def admin_delete_user(user_id: str, admin: User = Depends(_require_admin), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, detail="User not found.")
    if user.id == admin.id:
        raise HTTPException(400, detail="Cannot delete your own admin account.")
    db.delete(user)
    db.commit()


@app.get("/admin/users/{user_id}/token")
def admin_get_user_token(user_id: str, admin: User = Depends(_require_admin), db: Session = Depends(get_db)):
    """(Re-)issue a JWT for the specified user. Useful for onboarding users who lost their token."""
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, detail="User not found.")
    return {"user_id": user.id, "email": user.email, "jwt_token": create_jwt(user.id)}


@app.get("/user/me")
def get_me(user: User = Depends(_get_current_user)):
    """Return basic profile info for the current user."""
    return {
        "id": user.id,
        "email": user.email,
        "name": user.name,
        "role": user.role,
        "has_google": bool(user.google_refresh_token_enc),
        "has_meta": bool(user.meta_access_token_enc),
        "referred_by": user.referred_by,
    }


# ---------------------------------------------------------------------------
# Internal cron endpoint — process drip emails
# Called by Cloud Scheduler or any HTTP cron trigger every hour.
# Protected by a shared secret in the Authorization header.
# ---------------------------------------------------------------------------

@app.post("/internal/process-emails")
async def process_emails_cron(request: Request):
    """
    Internal cron endpoint: sends pending drip/lifecycle emails.
    Must supply: Authorization: Bearer <CRON_SECRET>
    """
    cron_secret = os.environ.get("CRON_SECRET", "")
    if not cron_secret:
        raise HTTPException(status_code=503, detail="Cron not configured.")
    auth = request.headers.get("Authorization", "")
    token = auth.removeprefix("Bearer ").strip()
    if token != cron_secret:
        raise HTTPException(status_code=401, detail="Unauthorized.")

    from emails import process_drip_queue
    result = process_drip_queue()
    return {"ok": True, **result}


# ---------------------------------------------------------------------------
# Dev / local run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
