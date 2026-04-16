"""
Airwallex webhook handler — FastAPI router mounted at /webhooks.

Listens for Airwallex subscription lifecycle events and updates the
local Subscription record accordingly.

Key events handled:
  subscription.ACTIVE      → activate user's subscription
  subscription.CANCELLED   → cancel user's subscription
  subscription.PAST_DUE    → mark as past_due
  payment_attempt.SUCCEEDED → confirm payment / reactivate if past_due
  payment_attempt.FAILED    → increment failure count / notify
"""
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Request, Response

from billing import verify_webhook_signature
from database import upsert_subscription, SessionLocal, Subscription, User, credit_affiliate_commission
from plans import PAID_PLAN_ID

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhooks", tags=["webhooks"])


async def _raw_body(request: Request) -> bytes:
    return await request.body()


def _find_sub_by_airwallex_id(airwallex_sub_id: str) -> "Subscription | None":
    db = SessionLocal()
    try:
        return db.query(Subscription).filter(
            Subscription.airwallex_subscription_id == airwallex_sub_id
        ).first()
    finally:
        db.close()


@router.post("/airwallex", include_in_schema=False)
async def airwallex_webhook(request: Request):
    raw = await _raw_body(request)
    sig = request.headers.get("x-signature", "")

    if not verify_webhook_signature(raw, sig):
        logger.warning("Airwallex webhook: invalid signature")
        return Response(status_code=400, content="Invalid signature")

    try:
        event = request.app.state  # won't work — parse directly
        import json
        event = json.loads(raw)
    except Exception:
        return Response(status_code=400, content="Bad JSON")

    event_name: str = event.get("name", "")
    data: dict = event.get("data", {})
    sub_data: dict = data.get("object", data)

    airwallex_sub_id: str = sub_data.get("id", "")
    # Airwallex stores our user_id in metadata
    metadata: dict = sub_data.get("metadata", {})
    internal_user_id: str = metadata.get("internal_user_id", "")

    if not internal_user_id and airwallex_sub_id:
        # Try to look up by airwallex subscription ID
        existing = _find_sub_by_airwallex_id(airwallex_sub_id)
        if existing:
            internal_user_id = existing.user_id

    if not internal_user_id:
        logger.warning("Airwallex webhook: no user_id resolvable for event %s", event_name)
        return Response(status_code=200, content="ok")

    now = datetime.now(timezone.utc)

    if event_name in ("subscription.ACTIVE", "subscription.CREATED"):
        upsert_subscription(
            internal_user_id,
            status="active",
            airwallex_subscription_id=airwallex_sub_id,
            airwallex_customer_id=sub_data.get("customer_id"),
            current_period_start=_parse_dt(sub_data.get("current_period_start")),
            current_period_end=_parse_dt(sub_data.get("current_period_end")),
        )
        logger.info("Subscription activated for user %s", internal_user_id)
        # Generate MCP credential on first activation
        try:
            from database import get_mcp_credential, generate_mcp_credential
            if get_mcp_credential(internal_user_id) is None:
                generate_mcp_credential(internal_user_id)
                logger.info("MCP credential generated for user %s on subscription activation", internal_user_id)
        except Exception:
            pass
        # Send onboarding email (idempotent via email_log)
        _send_onboarding_email(internal_user_id)

    elif event_name == "subscription.CANCELLED":
        upsert_subscription(
            internal_user_id,
            status="canceled",
            airwallex_subscription_id=airwallex_sub_id,
            canceled_at=now,
        )
        logger.info("Subscription cancelled for user %s", internal_user_id)
        _send_win_back_email(internal_user_id)

    elif event_name == "subscription.PAST_DUE":
        upsert_subscription(
            internal_user_id,
            status="past_due",
            airwallex_subscription_id=airwallex_sub_id,
        )
        logger.warning("Subscription past_due for user %s", internal_user_id)

    elif event_name == "payment_attempt.SUCCEEDED":
        amount_cents = int((sub_data.get("amount") or 0) * 100)
        period_end = _parse_dt(sub_data.get("next_billing_date") or sub_data.get("current_period_end"))
        upsert_subscription(
            internal_user_id,
            status="active",
            airwallex_subscription_id=airwallex_sub_id,
            current_period_start=now,
            current_period_end=period_end,
        )
        logger.info("Payment succeeded for user %s", internal_user_id)
        # Credit affiliate commission
        try:
            credit_affiliate_commission(internal_user_id, amount_cents)
        except Exception:
            pass
        # Generate MCP credential if this is the user's first successful payment
        try:
            from database import get_mcp_credential, generate_mcp_credential
            if get_mcp_credential(internal_user_id) is None:
                generate_mcp_credential(internal_user_id)
                logger.info("MCP credential generated for user %s", internal_user_id)
        except Exception:
            pass
        # Send receipt email
        _send_receipt_email(internal_user_id, amount_cents, period_end)

    elif event_name == "payment_attempt.FAILED":
        logger.warning("Payment FAILED for user %s (airwallex sub %s)", internal_user_id, airwallex_sub_id)
        upsert_subscription(
            internal_user_id,
            status="past_due",
            airwallex_subscription_id=airwallex_sub_id,
        )

    else:
        logger.debug("Unhandled Airwallex event: %s", event_name)

    return Response(status_code=200, content="ok")


def _parse_dt(value) -> "datetime | None":
    if not value:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(value, tz=timezone.utc)
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _get_user(user_id: str):
    db = SessionLocal()
    try:
        return db.get(User, user_id)
    finally:
        db.close()


def _send_receipt_email(user_id: str, amount_cents: int, period_end) -> None:
    user = _get_user(user_id)
    if not user:
        return
    try:
        from database import email_already_sent
        from emails import send_receipt, send_receipt_renewal
        if not email_already_sent(user_id, "receipt"):
            send_receipt(user_id, user.email, user.name, amount_cents, period_end)
        else:
            send_receipt_renewal(user_id, user.email, user.name, amount_cents, period_end)
    except Exception as exc:
        logger.warning("Receipt email failed for %s: %s", user_id, exc)


def _send_onboarding_email(user_id: str) -> None:
    user = _get_user(user_id)
    if not user:
        return
    try:
        from emails import send_onboarding
        send_onboarding(user_id, user.email, user.name)
    except Exception as exc:
        logger.warning("Onboarding email failed for %s: %s", user_id, exc)


def _send_win_back_email(user_id: str) -> None:
    user = _get_user(user_id)
    if not user:
        return
    try:
        from emails import send_win_back
        send_win_back(user_id, user.email, user.name)
    except Exception as exc:
        logger.warning("Win-back email failed for %s: %s", user_id, exc)
