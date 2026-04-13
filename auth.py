"""
JWT creation/validation + per-request user ContextVar.
"""
import os
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import Optional

from jose import JWTError, jwt

from database import User, SessionLocal

JWT_SECRET = os.environ.get("JWT_SECRET_KEY", "change-me-in-production-please")
ALGORITHM = "HS256"
TOKEN_EXPIRE_DAYS = 90

# ---------------------------------------------------------------------------
# Per-request context — set by the ASGI auth wrapper before tool execution
# ---------------------------------------------------------------------------
current_user_ctx: ContextVar[Optional[User]] = ContextVar("current_user", default=None)


def create_jwt(user_id: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(days=TOKEN_EXPIRE_DAYS)
    payload = {"sub": user_id, "exp": expire}
    return jwt.encode(payload, JWT_SECRET, algorithm=ALGORITHM)


def verify_jwt(token: str) -> Optional[User]:
    """Decode the JWT and return the corresponding User, or None on any failure."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGORITHM])
        user_id: str = payload.get("sub")
        if not user_id:
            return None
    except JWTError:
        return None

    db = SessionLocal()
    try:
        user = db.get(User, user_id)
        if user and user.is_active:
            return user
        return None
    finally:
        db.close()
