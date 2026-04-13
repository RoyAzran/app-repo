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
load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))
from contextlib import asynccontextmanager
from contextvars import copy_context
from typing import Optional

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request, status
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.orm import Session

from auth import create_jwt, current_user_ctx, verify_jwt
from database import SessionLocal, User, create_tables, get_db
import mcp_server  # noqa: F401 — registers all tools onto mcp singleton
from mcp_instance import mcp
from oauth_google import router as google_router
from oauth_meta import router as meta_router
from oauth_server import router as oauth_server_router
from permissions import Role


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

app.include_router(google_router)
app.include_router(meta_router)
app.include_router(oauth_server_router)


# ---------------------------------------------------------------------------
# Intercept /mcp at the ASGI level — before FastAPI routing.
# This avoids app.mount() trailing-slash redirects entirely.
# ---------------------------------------------------------------------------

_original_asgi = app.build_middleware_stack  # FastAPI builds this lazily

class ASGIInterceptor:
    """Wraps the entire FastAPI ASGI app.
    Requests to /mcp (or /mcp/) are forwarded directly to the MCP sub-app;
    everything else passes through to FastAPI as normal."""

    def __init__(self):
        self._fastapi_app = None

    def _get_fastapi(self):
        # Lazily resolve. We can't call build_middleware_stack at import time.
        if self._fastapi_app is None:
            self._fastapi_app = _original_asgi()
        return self._fastapi_app

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "")
        if scope["type"] in ("http", "websocket") and (path == "/mcp" or path == "/mcp/"):
            # Rewrite path to /mcp (what FastMCP's internal route expects)
            scope = dict(scope)
            scope["path"] = "/mcp"
            await _mcp_wrapped(scope, receive, send)
        else:
            await self._get_fastapi()(scope, receive, send)

_interceptor = ASGIInterceptor()

# Override build_middleware_stack so Uvicorn picks up our interceptor
app.build_middleware_stack = lambda: _interceptor


# ---------------------------------------------------------------------------
# ASGI auth wrapper — wraps the MCP sub-app at ASGI level
# This is necessary because FastAPI middleware is bypassed for mounted apps.
# ---------------------------------------------------------------------------

class MCPAuthWrapper:
    """ASGI middleware that validates JWT and injects user into current_user_ctx."""

    def __init__(self, app):
        self._app = app

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

        user: Optional[User] = verify_jwt(token) if token else None

        if user is None:
            # Return 401
            if scope["type"] == "http":
                body = b'{"detail":"Not authenticated. Provide a valid Bearer JWT."}'
                await send({
                    "type": "http.response.start",
                    "status": 401,
                    "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
                })
                await send({"type": "http.response.body", "body": body})
                return
            else:
                await send({"type": "websocket.close", "code": 4001})
                return

        # Set user in ContextVar for this request, then call the MCP app
        ctx = copy_context()

        async def run_in_ctx():
            current_user_ctx.set(user)
            try:
                await self._app(scope, receive, send)
            except Exception as exc:
                import traceback
                tb = traceback.format_exc()
                body = f'{{"detail":"MCP error: {type(exc).__name__}: {exc}","traceback":"{tb[:500]}"}}'.encode()
                await send({
                    "type": "http.response.start",
                    "status": 500,
                    "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
                })
                await send({"type": "http.response.body", "body": body})

        await ctx.run(run_in_ctx)


# Build the auth-wrapped MCP ASGI handler
_mcp_wrapped = MCPAuthWrapper(_mcp_starlette)


# ---------------------------------------------------------------------------
# Onboarding page
# ---------------------------------------------------------------------------

@app.get("/onboard", include_in_schema=False)
async def onboard():
    return FileResponse(os.path.join(os.path.dirname(__file__), "static", "onboard.html"))


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------

@app.get("/health")
async def health():
    return {"status": "ok"}


# ---------------------------------------------------------------------------
# Admin helpers — require admin role
# ---------------------------------------------------------------------------

def _require_admin(request: Request, db: Session = Depends(get_db)) -> User:
    auth_header = request.headers.get("Authorization", "")
    token = auth_header.removeprefix("Bearer ").strip() if auth_header.startswith("Bearer ") else ""
    user = verify_jwt(token) if token else None
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


# ---------------------------------------------------------------------------
# Dev / local run
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
