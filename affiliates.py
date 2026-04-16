"""
Affiliate program — FastAPI router.

Routes:
  POST /affiliate/join            — become an affiliate (any paid user)
  GET  /affiliate/dashboard       — view stats + ref link (authenticated)
  POST /affiliate/payout-email    — set payout email address
  GET  /affiliate/leaderboard     — top affiliates (public, anonymised)

Affiliate links look like:  https://mcp-ads.com/?ref=XXXXXXXXXX
When a visitor lands with ?ref=, the frontend stores it in sessionStorage.
On the signup callback (oauth_google.py) the ref_code is read from the query
param and saved to User.referred_by.

Commission is credited in webhooks.py on payment_attempt.SUCCEEDED.
"""
import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel

from auth import verify_jwt_user as verify_jwt
from database import (
    Affiliate, AffiliateReferral, SessionLocal, User,
    get_affiliate_stats, get_or_create_affiliate,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/affiliate", tags=["affiliate"])

_BASE = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")


# ── Auth dependency ────────────────────────────────────────────────────────────

def _get_user(request: Request) -> User:
    auth = request.headers.get("Authorization", "")
    token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
    user = verify_jwt(token) if token else None
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated.")
    return user


# ── Routes ─────────────────────────────────────────────────────────────────────

@router.post("/join")
def join_affiliate(user: User = Depends(_get_user)):
    """
    Any active user can join the affiliate program.
    Returns their unique referral code and shareable link.
    """
    aff = get_or_create_affiliate(user.id)
    ref_link = f"{_BASE}/?ref={aff.ref_code}"
    return {
        "ref_code": aff.ref_code,
        "ref_link": ref_link,
        "commission_rate": aff.commission_rate,
        "message": "Share your link. Earn 30% commission on every paying referral.",
    }


@router.get("/dashboard")
def affiliate_dashboard(user: User = Depends(_get_user)):
    """Return the authenticated user's affiliate stats and link."""
    stats = get_affiliate_stats(user.id)
    if stats is None:
        raise HTTPException(
            status_code=404,
            detail="You haven't joined the affiliate program yet. POST /affiliate/join to get started.",
        )
    stats["ref_link"] = f"{_BASE}/?ref={stats['ref_code']}"
    stats["total_earned_usd"] = round(stats["total_earned_cents"] / 100, 2)
    stats["pending_usd"] = round(stats["pending_cents"] / 100, 2)
    stats["paid_out_usd"] = round(stats["paid_out_cents"] / 100, 2)
    return stats


class PayoutEmailIn(BaseModel):
    payout_email: str


@router.post("/payout-email")
def set_payout_email(body: PayoutEmailIn, user: User = Depends(_get_user)):
    """Set or update the affiliate's payout email (used for Airwallex transfers)."""
    db = SessionLocal()
    try:
        aff = db.query(Affiliate).filter(Affiliate.user_id == user.id).first()
        if not aff:
            raise HTTPException(status_code=404, detail="Not an affiliate yet. POST /affiliate/join first.")
        # Basic email validation
        if "@" not in body.payout_email or "." not in body.payout_email.split("@")[-1]:
            raise HTTPException(status_code=422, detail="Invalid email address.")
        aff.payout_email = body.payout_email.strip().lower()
        db.commit()
    finally:
        db.close()
    return {"success": True, "payout_email": body.payout_email}


@router.get("/leaderboard")
def leaderboard():
    """Top 10 affiliates by total earnings. Names anonymised."""
    db = SessionLocal()
    try:
        top = (
            db.query(Affiliate)
            .filter(Affiliate.status == "active", Affiliate.total_earned_cents > 0)
            .order_by(Affiliate.total_earned_cents.desc())
            .limit(10)
            .all()
        )
        return [
            {
                "rank": i + 1,
                "ref_code": aff.ref_code,
                "total_earned_usd": round(aff.total_earned_cents / 100, 2),
                "commission_rate": aff.commission_rate,
            }
            for i, aff in enumerate(top)
        ]
    finally:
        db.close()


# ── Admin: trigger payout via Airwallex Transfers API ─────────────────────────

@router.post("/admin/pay/{affiliate_id}")
async def admin_pay_affiliate(affiliate_id: str, request: Request):
    """
    Admin-only: initiate an Airwallex transfer payout to a specific affiliate.
    Uses the Airwallex Transfers API to send their pending balance.
    Requires admin JWT.
    """
    auth = request.headers.get("Authorization", "")
    token = auth.removeprefix("Bearer ").strip() if auth.startswith("Bearer ") else ""
    admin = verify_jwt(token) if token else None
    if not admin or admin.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required.")

    db = SessionLocal()
    try:
        aff = db.get(Affiliate, affiliate_id)
        if not aff:
            raise HTTPException(status_code=404, detail="Affiliate not found.")
        if aff.pending_cents <= 0:
            raise HTTPException(status_code=400, detail="No pending balance to pay.")
        if not aff.payout_email:
            raise HTTPException(status_code=400, detail="Affiliate has no payout email set.")

        amount_cents = aff.pending_cents
        payout_email = aff.payout_email

        # Call Airwallex Transfers API
        result = await _airwallex_payout(
            amount_cents=amount_cents,
            recipient_email=payout_email,
            note=f"MarketingMCP affiliate payout — {aff.ref_code}",
        )

        if result.get("success"):
            aff.paid_out_cents += amount_cents
            aff.pending_cents = 0
            # Mark all earned referrals as paid
            db.query(AffiliateReferral).filter(
                AffiliateReferral.affiliate_id == aff.id,
                AffiliateReferral.status == "earned",
            ).update({"status": "paid"})
            db.commit()
            return {"success": True, "paid_usd": round(amount_cents / 100, 2)}
        else:
            raise HTTPException(status_code=500, detail=f"Airwallex transfer failed: {result.get('error')}")
    finally:
        db.close()


async def _airwallex_payout(amount_cents: int, recipient_email: str, note: str) -> dict:
    """
    Initiate an Airwallex transfer to a recipient email.
    Uses Airwallex Payouts / Transfers API.
    """
    import httpx
    import uuid

    client_id = os.environ.get("AIRWALLEX_CLIENT_ID", "")
    api_key = os.environ.get("AIRWALLEX_API_KEY", "")
    api_base = os.environ.get("AIRWALLEX_API_BASE", "https://api.airwallex.com").rstrip("/")

    if not client_id or not api_key:
        return {"success": False, "error": "Airwallex credentials not configured."}

    headers = {"x-client-id": client_id, "x-api-key": api_key, "Content-Type": "application/json"}

    payload = {
        "request_id": str(uuid.uuid4()),
        "amount": round(amount_cents / 100, 2),
        "currency": "USD",
        "beneficiary": {
            "entity_type": "PERSONAL",
            "email": recipient_email,
        },
        "payment_method": "LOCAL",
        "note": note,
    }

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(f"{api_base}/v1/transfers/create", json=payload, headers=headers)
        if resp.status_code in (200, 201):
            return {"success": True, "data": resp.json()}
        return {"success": False, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
    except Exception as exc:
        return {"success": False, "error": str(exc)}
