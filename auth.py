"""
JWT creation/validation + per-request user ContextVar.

Security notes:
- JWT_SECRET_KEY is REQUIRED — the server refuses to start without it.
- Tokens have a 7-day expiry and carry a jti (JWT ID) for revocation support.
- verify_jwt also checks that the user has an active subscription.
"""
import os
import uuid
from contextvars import ContextVar
from datetime import datetime, timedelta, timezone
from typing import Optional

from jose import JWTError, jwt

from database import User, SessionLocal

_secret = os.environ.get("JWT_SECRET_KEY", "")
if not _secret:
    raise RuntimeError(
        "JWT_SECRET_KEY environment variable is not set. "
        "Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\""
    )

JWT_SECRET = _secret
ALGORITHM = "HS256"
TOKEN_EXPIRE_DAYS = 7

# ---------------------------------------------------------------------------
# Per-request context — set by the ASGI auth wrapper before tool execution
# ---------------------------------------------------------------------------
current_user_ctx: ContextVar[Optional[User]] = ContextVar("current_user", default=None)


def create_jwt(user_id: str, return_jti: bool = False) -> "str | tuple[str, str]":
    """Issue a signed JWT with a unique jti for possible revocation.
    If return_jti=True, returns (token, jti) instead of just token.
    """
    jti = str(uuid.uuid4())
    expire = datetime.now(timezone.utc) + timedelta(days=TOKEN_EXPIRE_DAYS)
    payload = {
        "sub": user_id,
        "exp": expire,
        "jti": jti,
        "iat": datetime.now(timezone.utc),
    }
    token = jwt.encode(payload, JWT_SECRET, algorithm=ALGORITHM)
    if return_jti:
        return token, jti
    return token


def verify_jwt_user(token: str) -> "Optional[User]":
    """Thin wrapper — returns just the User for callers that don't need the sub."""
    result = verify_jwt(token)
    if result is None:
        return None
    user, _ = result
    return user


def verify_jwt_user_any(token: str) -> "Optional[User]":
    """Like verify_jwt_user but does NOT require an active subscription.
    Use for pre-payment endpoints like /billing/checkout and /billing/status."""
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
        if not user or not user.is_active:
            return None
        return user
    finally:
        db.close()


def verify_jwt(token: str) -> "Optional[tuple[User, object]]":
    """
    Decode the JWT, verify the user exists, is active, and has an active subscription.
    Returns (User, Subscription) or None on any failure.
    Subscription may be None for admin users with no subscription row.
    """
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
        if not user or not user.is_active:
            return None

        # Check subscription — must be active (no free trial)
        from database import Subscription, TeamMember, Team
        sub = db.query(Subscription).filter(
            Subscription.user_id == user_id,
            Subscription.status == "active",
        ).first()
        if sub is None:
            if user.role == "admin":
                return user, None
            # Allow active team members whose team has an active subscription
            member = db.query(TeamMember).filter(
                TeamMember.user_id == user_id,
                TeamMember.status == "active",
            ).first()
            if member:
                team = db.get(Team, member.team_id)
                if team and team.status == "active":
                    return user, None
            return None

        return user, sub
    finally:
        db.close()


def get_jwt_jti(token: str) -> Optional[str]:
    """Extract jti from a token without full validation (for session tracking)."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[ALGORITHM])
        return payload.get("jti")
    except JWTError:
        return None
