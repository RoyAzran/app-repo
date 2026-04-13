"""
SQLAlchemy models + SQLite engine.
All sensitive tokens are stored encrypted with Fernet.
"""
import os
import uuid
from datetime import datetime, timedelta, timezone

from cryptography.fernet import Fernet
from sqlalchemy import Boolean, Column, DateTime, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./agency_mcp.db")

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        key = os.environ.get("FERNET_KEY", "")
        if not key:
            raise RuntimeError("FERNET_KEY env var not set. Generate with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\"")
        _fernet = Fernet(key.encode() if isinstance(key, str) else key)
    return _fernet


def encrypt_token(value: str) -> str:
    return _get_fernet().encrypt(value.encode()).decode()


def decrypt_token(value: str) -> str:
    return _get_fernet().decrypt(value.encode()).decode()


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False, default="")
    role = Column(String, nullable=False, default="viewer")      # viewer | editor | admin
    is_active = Column(Boolean, nullable=False, default=True)

    # Encrypted Google refresh token (covers Ads, GA4, GSC, Sheets)
    google_refresh_token_enc = Column(String, nullable=True)

    # Encrypted Meta user access token (covers Meta Ads + Pages)
    meta_access_token_enc = Column(String, nullable=True)
    meta_token_expires_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    # ---- helpers ----
    def set_google_token(self, refresh_token: str) -> None:
        self.google_refresh_token_enc = encrypt_token(refresh_token)

    def get_google_token(self) -> str | None:
        if not self.google_refresh_token_enc:
            return None
        return decrypt_token(self.google_refresh_token_enc)

    def set_meta_token(self, access_token: str) -> None:
        self.meta_access_token_enc = encrypt_token(access_token)

    def get_meta_token(self) -> str | None:
        if not self.meta_access_token_enc:
            return None
        return decrypt_token(self.meta_access_token_enc)


def create_tables() -> None:
    Base.metadata.create_all(bind=engine)


def get_db() -> Session:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------------
# OAuth CSRF state — stored in DB so serverless instances share state
# ---------------------------------------------------------------------------

class OAuthState(Base):
    __tablename__ = "oauth_states"

    state = Column(String, primary_key=True)
    user_id = Column(String, nullable=False, default="")
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


def create_oauth_state(state: str, user_id: str = "") -> None:
    db = SessionLocal()
    try:
        db.add(OAuthState(state=state, user_id=user_id))
        db.commit()
    finally:
        db.close()


def consume_oauth_state(state: str) -> str | None:
    """Return user_id if state is valid and not expired (10 min), then delete it."""
    db = SessionLocal()
    try:
        row = db.get(OAuthState, state)
        if row is None:
            return None
        # Expire after 10 minutes
        age = datetime.now(timezone.utc) - row.created_at.replace(tzinfo=timezone.utc)
        if age > timedelta(minutes=10):
            db.delete(row)
            db.commit()
            return None
        user_id = row.user_id
        db.delete(row)
        db.commit()
        return user_id
    finally:
        db.close()


# ---------------------------------------------------------------------------
# OAuth authorization codes — for the OAuth2 server (Claude.ai connector)
# ---------------------------------------------------------------------------

class OAuthCode(Base):
    __tablename__ = "oauth_codes"

    code = Column(String, primary_key=True)
    user_id = Column(String, nullable=True)           # set after user authenticates
    redirect_uri = Column(String, nullable=False)
    code_challenge = Column(String, nullable=True)
    code_challenge_method = Column(String, nullable=True)
    client_id = Column(String, nullable=False, default="")
    original_state = Column(String, nullable=False, default="")
    status = Column(String, nullable=False, default="pending")  # pending | ready | used
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


def create_pending_oauth_code(
    redirect_uri: str,
    code_challenge: str,
    code_challenge_method: str,
    client_id: str,
    original_state: str,
) -> str:
    """Create a pending authorization code and return the code string."""
    import secrets as _secrets
    code = _secrets.token_urlsafe(32)
    db = SessionLocal()
    try:
        db.add(OAuthCode(
            code=code,
            redirect_uri=redirect_uri,
            code_challenge=code_challenge,
            code_challenge_method=code_challenge_method,
            client_id=client_id,
            original_state=original_state,
        ))
        db.commit()
    finally:
        db.close()
    return code


def activate_oauth_code(code: str, user_id: str) -> tuple[str, str] | None:
    """
    Mark a pending code as ready with the authenticated user_id.
    Returns (redirect_uri, original_state) or None if not found / wrong status.
    """
    db = SessionLocal()
    try:
        row = db.get(OAuthCode, code)
        if row is None or row.status != "pending":
            return None
        row.user_id = user_id
        row.status = "ready"
        db.commit()
        return (row.redirect_uri, row.original_state)
    finally:
        db.close()


def consume_oauth_code(code: str) -> tuple[str, str, str, str] | None:
    """
    Exchange an authorization code for user data (deletes the row).
    Returns (user_id, code_challenge, code_challenge_method, redirect_uri) or None.
    Valid for 5 minutes after creation.
    """
    db = SessionLocal()
    try:
        row = db.get(OAuthCode, code)
        if row is None or row.status != "ready":
            return None
        age = datetime.now(timezone.utc) - row.created_at.replace(tzinfo=timezone.utc)
        if age > timedelta(minutes=5):
            db.delete(row)
            db.commit()
            return None
        result = (row.user_id, row.code_challenge or "", row.code_challenge_method or "S256", row.redirect_uri)
        db.delete(row)
        db.commit()
        return result
    finally:
        db.close()
