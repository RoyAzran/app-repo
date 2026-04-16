"""
Session tracking + request context helpers.

Responsibilities:
  1. Record every authenticated MCP request as a UserSession row.
  2. Log every tool call to AuditLog.
  3. Detect and block suspicious multi-IP usage (credential sharing).
  4. Expose helpers used by MCPAuthWrapper in main.py.
"""
import hashlib
import logging
import os
import time
from typing import Optional

from database import (
    count_unique_ips_last_24h,
    is_session_revoked,
    log_tool_call,
    record_session,
)

logger = logging.getLogger(__name__)

# Alert threshold — if one user's token is used from this many distinct IPs
# in a 24-hour window, flag it as likely credential sharing.
SUSPICIOUS_IP_THRESHOLD = int(os.environ.get("SUSPICIOUS_IP_THRESHOLD", "5"))
# Block when over threshold (set to "false" to only warn)
BLOCK_SUSPICIOUS_IPS = os.environ.get("BLOCK_SUSPICIOUS_IPS", "true").lower() == "true"


def hash_user_agent(ua: str) -> str:
    return hashlib.sha256(ua.encode()).hexdigest()[:16]


def get_client_ip(scope: dict) -> str:
    """Extract real client IP, respecting common proxy headers."""
    headers = dict(scope.get("headers", []))
    forwarded_for = headers.get(b"x-forwarded-for", b"").decode()
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()
    real_ip = headers.get(b"x-real-ip", b"").decode()
    if real_ip:
        return real_ip.strip()
    client = scope.get("client")
    if client:
        return client[0]
    return ""


def track_session(
    user_id: str,
    jti: str,
    scope: dict,
) -> bool:
    """
    Record this request's session fingerprint.
    Returns True if the request should proceed, False if it should be blocked
    (suspicious multi-IP credential sharing detected).
    """
    headers = dict(scope.get("headers", []))
    ua = headers.get(b"user-agent", b"").decode()
    ip = get_client_ip(scope)
    ua_hash = hash_user_agent(ua)

    record_session(user_id, jti, ip, ua_hash)

    unique_ips = count_unique_ips_last_24h(user_id)
    if unique_ips >= SUSPICIOUS_IP_THRESHOLD:
        logger.warning(
            "SUSPICIOUS: user %s accessed from %d distinct IPs in the last 24h "
            "(threshold=%d) — possible credential sharing",
            user_id,
            unique_ips,
            SUSPICIOUS_IP_THRESHOLD,
        )
        if BLOCK_SUSPICIOUS_IPS:
            return False
    return True


def check_session_revoked(jti: Optional[str]) -> bool:
    """Return True if this JWT has been explicitly revoked."""
    if not jti:
        return False
    return is_session_revoked(jti)


def record_tool_call(
    user_id: str,
    tool_name: str,
    scope: dict,
    start_time: float,
    success: bool = True,
    error: str = "",
) -> None:
    """Write an AuditLog entry for a completed tool call."""
    elapsed_ms = int((time.time() - start_time) * 1000)
    ip = get_client_ip(scope)
    log_tool_call(
        user_id=user_id,
        tool_name=tool_name,
        ip=ip,
        response_time_ms=elapsed_ms,
        success=success,
        error_message=error,
    )
