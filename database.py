"""
SQLAlchemy models — Supabase (PostgreSQL) or SQLite.
All sensitive tokens are stored encrypted with Fernet.
"""
import os
import uuid
from datetime import datetime, timedelta, timezone

from cryptography.fernet import Fernet
from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, Session, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./agency_mcp.db")

_is_sqlite = DATABASE_URL.startswith("sqlite")
_engine_kwargs: dict = {}
if _is_sqlite:
    _engine_kwargs["connect_args"] = {"check_same_thread": False}
else:
    # Supabase connection pooler (port 6543) requires these settings
    _engine_kwargs["connect_args"] = {"sslmode": "require"}
    _engine_kwargs["pool_pre_ping"] = True

engine = create_engine(DATABASE_URL, **_engine_kwargs)
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
    role = Column(String, nullable=False, default="editor")      # viewer | editor | admin
    is_active = Column(Boolean, nullable=False, default=True)

    # Encrypted Google refresh token (covers Ads, GA4, GSC, Sheets)
    google_refresh_token_enc = Column(String, nullable=True)

    # Encrypted Meta user access token (covers Meta Ads + Pages)
    meta_access_token_enc = Column(String, nullable=True)
    meta_token_expires_at = Column(DateTime, nullable=True)

    # Affiliate / referral tracking
    referred_by = Column(String, nullable=True, index=True)  # affiliate ref_code used at signup

    # UTM attribution (captured at first signup)
    utm_source = Column(String, nullable=True)
    utm_medium = Column(String, nullable=True)
    utm_campaign = Column(String, nullable=True)

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
        # Expire after 30 minutes
        age = datetime.now(timezone.utc) - row.created_at.replace(tzinfo=timezone.utc)
        if age > timedelta(minutes=30):
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
        if age > timedelta(minutes=30):  # 30 minutes — enough for two OAuth flows
            db.delete(row)
            db.commit()
            return None
        result = (row.user_id, row.code_challenge or "", row.code_challenge_method or "S256", row.redirect_uri)
        db.delete(row)
        db.commit()
        return result
    finally:
        db.close()


def read_oauth_code(code: str) -> tuple[str, str, str] | None:
    """Read a pending/ready code without consuming it.
    Returns (user_id, redirect_uri, original_state) or None.
    """
    db = SessionLocal()
    try:
        row = db.get(OAuthCode, code)
        if row is None:
            return None
        return (row.user_id or "", row.redirect_uri, row.original_state)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Subscription — tracks paid plan status per user
# ---------------------------------------------------------------------------

class Subscription(Base):
    __tablename__ = "subscriptions"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=False, index=True)
    # Status values: active | trialing | past_due | canceled | paused
    status = Column(String, nullable=False, default="trialing")

    # Airwallex IDs
    airwallex_customer_id = Column(String, nullable=True)
    airwallex_subscription_id = Column(String, nullable=True)

    # Billing period
    trial_ends_at = Column(DateTime, nullable=True)
    current_period_start = Column(DateTime, nullable=True)
    current_period_end = Column(DateTime, nullable=True)
    canceled_at = Column(DateTime, nullable=True)

    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    @property
    def is_active(self) -> bool:
        return self.status == "active"


def get_user_subscription(user_id: str) -> "Subscription | None":
    db = SessionLocal()
    try:
        return db.query(Subscription).filter(Subscription.user_id == user_id).first()
    finally:
        db.close()


def upsert_subscription(user_id: str, **kwargs) -> "Subscription":
    """Create or update the subscription for a user."""
    db = SessionLocal()
    try:
        sub = db.query(Subscription).filter(Subscription.user_id == user_id).first()
        if sub is None:
            sub = Subscription(user_id=user_id, **kwargs)
            db.add(sub)
        else:
            for k, v in kwargs.items():
                setattr(sub, k, v)
            sub.updated_at = datetime.now(timezone.utc)
        db.commit()
        db.refresh(sub)
        return sub
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Session — tracks active JWT sessions for anti-sharing enforcement
# ---------------------------------------------------------------------------

class UserSession(Base):
    __tablename__ = "user_sessions"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=False, index=True)
    jti = Column(String, unique=True, nullable=False, index=True)   # JWT ID
    ip_address = Column(String, nullable=True)
    user_agent_hash = Column(String, nullable=True)                 # SHA-256 of User-Agent
    last_seen = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    revoked = Column(Boolean, nullable=False, default=False)


def record_session(user_id: str, jti: str, ip: str, ua_hash: str) -> None:
    """Upsert a session record — updates last_seen if jti already exists."""
    db = SessionLocal()
    try:
        row = db.query(UserSession).filter(UserSession.jti == jti).first()
        if row is None:
            db.add(UserSession(user_id=user_id, jti=jti, ip_address=ip, user_agent_hash=ua_hash))
        else:
            row.last_seen = datetime.now(timezone.utc)
            row.ip_address = ip
        db.commit()
    finally:
        db.close()


def revoke_session(jti: str) -> None:
    db = SessionLocal()
    try:
        row = db.query(UserSession).filter(UserSession.jti == jti).first()
        if row:
            row.revoked = True
            db.commit()
    finally:
        db.close()


def revoke_all_user_sessions(user_id: str, except_jti: str = "") -> int:
    """Revoke all active sessions for a user, optionally keeping one. Returns count revoked."""
    db = SessionLocal()
    try:
        query = db.query(UserSession).filter(
            UserSession.user_id == user_id,
            UserSession.revoked == False,
        )
        if except_jti:
            query = query.filter(UserSession.jti != except_jti)
        rows = query.all()
        for row in rows:
            row.revoked = True
        db.commit()
        return len(rows)
    finally:
        db.close()


def count_active_sessions(user_id: str) -> int:
    """Count non-revoked sessions active in the last 2 hours."""
    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=2)
        return db.query(UserSession).filter(
            UserSession.user_id == user_id,
            UserSession.revoked == False,
            UserSession.last_seen >= cutoff,
        ).count()
    finally:
        db.close()


def is_session_revoked(jti: str) -> bool:
    db = SessionLocal()
    try:
        row = db.query(UserSession).filter(UserSession.jti == jti).first()
        if row is None:
            return False
        return row.revoked
    finally:
        db.close()


def count_unique_ips_last_24h(user_id: str) -> int:
    """Return the number of distinct IPs that used this account in the last 24 hours."""
    db = SessionLocal()
    try:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
        rows = (
            db.query(UserSession.ip_address)
            .filter(
                UserSession.user_id == user_id,
                UserSession.last_seen >= cutoff,
                UserSession.revoked == False,
            )
            .distinct()
            .all()
        )
        return len(rows)
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Audit log — immutable record of every MCP tool call
# ---------------------------------------------------------------------------

class AuditLog(Base):
    __tablename__ = "audit_logs"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=False, index=True)
    tool_name = Column(String, nullable=False, index=True)
    ip_address = Column(String, nullable=True)
    response_time_ms = Column(Integer, nullable=True)
    success = Column(Boolean, nullable=False, default=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)


def log_tool_call(
    user_id: str,
    tool_name: str,
    ip: str = "",
    response_time_ms: int = 0,
    success: bool = True,
    error_message: str = "",
) -> None:
    db = SessionLocal()
    try:
        db.add(AuditLog(
            user_id=user_id,
            tool_name=tool_name,
            ip_address=ip,
            response_time_ms=response_time_ms,
            success=success,
            error_message=error_message or None,
        ))
        db.commit()
    finally:
        db.close()


def get_usage_stats(user_id: str, days: int = 30) -> dict:
    """Return tool call counts for the user over the past N days."""
    db = SessionLocal()
    try:
        from sqlalchemy import func
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)
        total = db.query(func.count(AuditLog.id)).filter(
            AuditLog.user_id == user_id,
            AuditLog.created_at >= cutoff,
        ).scalar() or 0
        today_cutoff = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        today = db.query(func.count(AuditLog.id)).filter(
            AuditLog.user_id == user_id,
            AuditLog.created_at >= today_cutoff,
        ).scalar() or 0
        return {"total_calls_period": total, "calls_today": today, "period_days": days}
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Email log — deduplication + tracking for transactional & drip emails
# ---------------------------------------------------------------------------

class EmailLog(Base):
    __tablename__ = "email_log"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=False, index=True)
    # e.g. welcome | receipt | drip_day2 | drip_day5 | drip_day7 | onboarding | win_back
    email_type = Column(String, nullable=False, index=True)
    status = Column(String, nullable=False, default="sent")   # sent | failed
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)


def email_already_sent(user_id: str, email_type: str) -> bool:
    db = SessionLocal()
    try:
        return db.query(EmailLog).filter(
            EmailLog.user_id == user_id,
            EmailLog.email_type == email_type,
            EmailLog.status == "sent",
        ).first() is not None
    finally:
        db.close()


def log_email(user_id: str, email_type: str, status: str = "sent", error: str = "") -> None:
    db = SessionLocal()
    try:
        db.add(EmailLog(
            user_id=user_id,
            email_type=email_type,
            status=status,
            error_message=error or None,
        ))
        db.commit()
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Affiliate program
# ---------------------------------------------------------------------------

class Affiliate(Base):
    __tablename__ = "affiliates"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=False, unique=True, index=True)
    ref_code = Column(String, nullable=False, unique=True, index=True)
    commission_rate = Column(Integer, nullable=False, default=30)   # percent, e.g. 30 = 30%
    total_earned_cents = Column(Integer, nullable=False, default=0)
    pending_cents = Column(Integer, nullable=False, default=0)      # awaiting payout
    paid_out_cents = Column(Integer, nullable=False, default=0)
    # Payout destination — stored as plain text (filled by affiliate)
    payout_email = Column(String, nullable=True)
    status = Column(String, nullable=False, default="active")       # active | suspended
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class AffiliateReferral(Base):
    __tablename__ = "affiliate_referrals"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    affiliate_id = Column(String, nullable=False, index=True)       # Affiliate.id
    referred_user_id = Column(String, nullable=False, index=True)   # the new User.id
    # Filled when payment succeeds
    subscription_id = Column(String, nullable=True)
    commission_cents = Column(Integer, nullable=True)
    status = Column(String, nullable=False, default="pending")      # pending | earned | paid
    paid_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


def get_or_create_affiliate(user_id: str) -> "Affiliate":
    import secrets as _s
    db = SessionLocal()
    try:
        aff = db.query(Affiliate).filter(Affiliate.user_id == user_id).first()
        if aff:
            return aff
        code = _s.token_urlsafe(8).upper()
        # Ensure uniqueness
        while db.query(Affiliate).filter(Affiliate.ref_code == code).first():
            code = _s.token_urlsafe(8).upper()
        aff = Affiliate(user_id=user_id, ref_code=code)
        db.add(aff)
        db.commit()
        db.refresh(aff)
        return aff
    finally:
        db.close()


def record_affiliate_referral(ref_code: str, referred_user_id: str) -> None:
    """Call when a new user signs up with a referral code."""
    db = SessionLocal()
    try:
        aff = db.query(Affiliate).filter(Affiliate.ref_code == ref_code).first()
        if not aff or aff.status != "active":
            return
        # Avoid duplicate referrals for the same user
        existing = db.query(AffiliateReferral).filter(
            AffiliateReferral.referred_user_id == referred_user_id
        ).first()
        if existing:
            return
        db.add(AffiliateReferral(
            affiliate_id=aff.id,
            referred_user_id=referred_user_id,
        ))
        db.commit()
    finally:
        db.close()


def credit_affiliate_commission(referred_user_id: str, amount_cents: int) -> None:
    """Call when a referred user's payment succeeds. Credits commission to the affiliate."""
    db = SessionLocal()
    try:
        referral = db.query(AffiliateReferral).filter(
            AffiliateReferral.referred_user_id == referred_user_id,
            AffiliateReferral.status == "pending",
        ).first()
        if not referral:
            return
        aff = db.get(Affiliate, referral.affiliate_id)
        if not aff:
            return
        commission = int(amount_cents * aff.commission_rate / 100)
        referral.commission_cents = commission
        referral.status = "earned"
        aff.total_earned_cents += commission
        aff.pending_cents += commission
        db.commit()
    finally:
        db.close()


def get_affiliate_stats(user_id: str) -> dict | None:
    db = SessionLocal()
    try:
        aff = db.query(Affiliate).filter(Affiliate.user_id == user_id).first()
        if not aff:
            return None
        referrals = db.query(AffiliateReferral).filter(
            AffiliateReferral.affiliate_id == aff.id
        ).all()
        return {
            "ref_code": aff.ref_code,
            "commission_rate": aff.commission_rate,
            "total_referrals": len(referrals),
            "converted": sum(1 for r in referrals if r.status in ("earned", "paid")),
            "total_earned_cents": aff.total_earned_cents,
            "pending_cents": aff.pending_cents,
            "paid_out_cents": aff.paid_out_cents,
            "payout_email": aff.payout_email,
            "status": aff.status,
        }
    finally:
        db.close()


# ---------------------------------------------------------------------------
# MCP Credentials — per-user OAuth client_id / client_secret for Claude.ai
# Each paid seat gets ONE credential pair. When Claude uses it to get a new
# JWT, the old active JWT is revoked, enforcing single-session per credential.
# ---------------------------------------------------------------------------

import hashlib as _hashlib
import secrets as _secrets


class McpCredential(Base):
    __tablename__ = "mcp_credentials"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=False, unique=True, index=True)   # 1 cred per user
    # client_id shown to the user — e.g. "mcp_AbCd1234XyZ"
    client_id = Column(String, nullable=False, unique=True, index=True)
    # SHA-256 hex of the plain client_secret (never stored plain)
    client_secret_hash = Column(String, nullable=False)
    # JTI of the current active JWT — nulled out when user revokes manually
    active_jti = Column(String, nullable=True)
    last_connected_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


def _hash_secret(plain: str) -> str:
    return _hashlib.sha256(plain.encode()).hexdigest()


def generate_mcp_credential(user_id: str) -> tuple[str, str]:
    """
    Create (or replace) the MCP credential for a user.
    Returns (client_id, client_secret_plain) — plain secret shown ONCE.
    """
    client_id = "mcp_" + _secrets.token_urlsafe(12)
    plain_secret = _secrets.token_urlsafe(32)
    secret_hash = _hash_secret(plain_secret)

    db = SessionLocal()
    try:
        existing = db.query(McpCredential).filter(McpCredential.user_id == user_id).first()
        if existing:
            # Rotate: revoke active session first
            if existing.active_jti:
                _revoke_jti_inline(db, user_id, existing.active_jti)
            existing.client_id = client_id
            existing.client_secret_hash = secret_hash
            existing.active_jti = None
            existing.last_connected_at = None
        else:
            db.add(McpCredential(
                user_id=user_id,
                client_id=client_id,
                client_secret_hash=secret_hash,
            ))
        db.commit()
    finally:
        db.close()
    return client_id, plain_secret


def _revoke_jti_inline(db, user_id: str, jti: str) -> None:
    """Revoke a session JTI within an existing db session."""
    row = db.query(UserSession).filter(UserSession.jti == jti).first()
    if row:
        row.revoked = True


def verify_mcp_credential(client_id: str, client_secret: str) -> "str | None":
    """Verify client_id + client_secret. Returns user_id or None."""
    db = SessionLocal()
    try:
        cred = db.query(McpCredential).filter(McpCredential.client_id == client_id).first()
        if not cred:
            return None
        if cred.client_secret_hash != _hash_secret(client_secret):
            return None
        return cred.user_id
    finally:
        db.close()


def activate_mcp_session(client_id: str, new_jti: str) -> None:
    """
    Called when a new JWT is issued via /oauth/token.
    Revokes the previous active JTI for this credential (single-session enforcement).
    """
    db = SessionLocal()
    try:
        cred = db.query(McpCredential).filter(McpCredential.client_id == client_id).first()
        if not cred:
            return
        # Revoke the old active session
        if cred.active_jti and cred.active_jti != new_jti:
            _revoke_jti_inline(db, cred.user_id, cred.active_jti)
        cred.active_jti = new_jti
        cred.last_connected_at = datetime.now(timezone.utc)
        db.commit()
    finally:
        db.close()


def get_mcp_credential(user_id: str) -> "McpCredential | None":
    db = SessionLocal()
    try:
        return db.query(McpCredential).filter(McpCredential.user_id == user_id).first()
    finally:
        db.close()


def get_mcp_user_id_by_client_id(client_id: str) -> "str | None":
    """Look up user_id from client_id alone (no secret check). Used for public-client PKCE flows."""
    db = SessionLocal()
    try:
        cred = db.query(McpCredential).filter(McpCredential.client_id == client_id).first()
        return cred.user_id if cred else None
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Teams — group subscription; owner + N member seats
# ---------------------------------------------------------------------------

class Team(Base):
    __tablename__ = "teams"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    owner_user_id = Column(String, nullable=False, unique=True, index=True)
    # max seats purchased (1 = solo, N = team)
    max_seats = Column(Integer, nullable=False, default=1)
    # billing
    airwallex_subscription_id = Column(String, nullable=True)
    status = Column(String, nullable=False, default="active")   # active | canceled | past_due
    plan_id = Column(String, nullable=False, default="solo")    # solo | team
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )


class TeamMember(Base):
    __tablename__ = "team_members"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    team_id = Column(String, nullable=False, index=True)
    # user_id is null until invitation is accepted
    user_id = Column(String, nullable=True, index=True)
    email = Column(String, nullable=False, index=True)
    # invited | active | removed
    status = Column(String, nullable=False, default="invited")
    # Invite token for accepting without having to log in first
    invite_token = Column(String, nullable=True, unique=True)
    invited_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    joined_at = Column(DateTime, nullable=True)


def create_team(owner_user_id: str, name: str, max_seats: int = 1,
                plan_id: str = "solo") -> "Team":
    db = SessionLocal()
    try:
        team = Team(
            owner_user_id=owner_user_id,
            name=name,
            max_seats=max_seats,
            plan_id=plan_id,
        )
        db.add(team)
        db.flush()
        # Add owner as a member automatically
        db.add(TeamMember(
            team_id=team.id,
            user_id=owner_user_id,
            email="",  # filled after lookup
            status="active",
            joined_at=datetime.now(timezone.utc),
        ))
        db.commit()
        db.refresh(team)
        return team
    finally:
        db.close()


def get_team_by_owner(user_id: str) -> "Team | None":
    db = SessionLocal()
    try:
        return db.query(Team).filter(Team.owner_user_id == user_id).first()
    finally:
        db.close()


def get_team_for_member(user_id: str) -> "Team | None":
    """Return the team this user belongs to (as owner or member)."""
    db = SessionLocal()
    try:
        member = db.query(TeamMember).filter(
            TeamMember.user_id == user_id,
            TeamMember.status == "active",
        ).first()
        if not member:
            return db.query(Team).filter(Team.owner_user_id == user_id).first()
        return db.get(Team, member.team_id)
    finally:
        db.close()


def invite_team_member(team_id: str, email: str) -> "tuple[TeamMember, str]":
    """
    Create an invitation for an email address.
    Returns (TeamMember, invite_token).
    If the user already exists by email, links them immediately.
    """
    db = SessionLocal()
    try:
        # Check if already invited or active
        existing = db.query(TeamMember).filter(
            TeamMember.team_id == team_id,
            TeamMember.email == email.lower(),
            TeamMember.status.in_(["invited", "active"]),
        ).first()
        if existing:
            return existing, existing.invite_token or ""

        token = _secrets.token_urlsafe(32)
        # Check if a user with this email already has an account
        existing_user = db.query(User).filter(User.email == email.lower()).first()
        member = TeamMember(
            team_id=team_id,
            user_id=existing_user.id if existing_user else None,
            email=email.lower(),
            status="active" if existing_user else "invited",
            invite_token=None if existing_user else token,
            joined_at=datetime.now(timezone.utc) if existing_user else None,
        )
        db.add(member)
        db.commit()
        db.refresh(member)
        return member, token
    finally:
        db.close()


def accept_team_invite(token: str, user_id: str) -> "TeamMember | None":
    """Accept an invite by token. Returns the TeamMember or None if invalid."""
    db = SessionLocal()
    try:
        member = db.query(TeamMember).filter(
            TeamMember.invite_token == token,
            TeamMember.status == "invited",
        ).first()
        if not member:
            return None
        member.user_id = user_id
        member.status = "active"
        member.joined_at = datetime.now(timezone.utc)
        member.invite_token = None
        db.commit()
        db.refresh(member)
        return member
    finally:
        db.close()


def get_team_members_with_usage(team_id: str) -> list[dict]:
    """Return all active members with their last 30-day usage stats.
    The owner is always prepended as the first row with role='owner'."""
    db = SessionLocal()
    try:
        team = db.query(Team).filter(Team.id == team_id).first()
        members = db.query(TeamMember).filter(
            TeamMember.team_id == team_id,
            TeamMember.status.in_(["active", "invited"]),
        ).order_by(TeamMember.joined_at).all()

        result = []

        # Prepend owner as first row
        if team:
            owner = db.get(User, team.owner_user_id)
            owner_cred = db.query(McpCredential).filter(
                McpCredential.user_id == team.owner_user_id
            ).first() if owner else None
            owner_usage = get_usage_stats(team.owner_user_id, days=30) if owner else {}
            result.append({
                "member_id": team.owner_user_id,
                "user_id": team.owner_user_id,
                "email": owner.email if owner else "",
                "name": owner.name if owner else "",
                "status": "owner",
                "joined_at": team.created_at.isoformat() if team.created_at else None,
                "calls_today": owner_usage.get("calls_today", 0),
                "calls_month": owner_usage.get("total_calls_period", 0),
                "last_connected": owner_cred.last_connected_at.isoformat() if owner_cred and owner_cred.last_connected_at else None,
                "has_credential": owner_cred is not None,
                "client_id": owner_cred.client_id if owner_cred else None,
                "is_owner": True,
            })

        for m in members:
            usage = {}
            cred = None
            if m.user_id:
                usage = get_usage_stats(m.user_id, days=30)
                cred = db.query(McpCredential).filter(
                    McpCredential.user_id == m.user_id
                ).first()
            user = db.get(User, m.user_id) if m.user_id else None
            result.append({
                "member_id": m.id,
                "user_id": m.user_id,
                "email": m.email or (user.email if user else ""),
                "name": user.name if user else "",
                "status": m.status,
                "joined_at": m.joined_at.isoformat() if m.joined_at else None,
                "calls_today": usage.get("calls_today", 0),
                "calls_month": usage.get("total_calls_period", 0),
                "last_connected": cred.last_connected_at.isoformat() if cred and cred.last_connected_at else None,
                "has_credential": cred is not None,
                "client_id": cred.client_id if cred else None,
                "is_owner": False,
            })
        return result
    finally:
        db.close()


def count_active_team_seats(team_id: str) -> int:
    db = SessionLocal()
    try:
        return db.query(TeamMember).filter(
            TeamMember.team_id == team_id,
            TeamMember.status.in_(["active", "invited"]),
        ).count()
    finally:
        db.close()
