"""
Airwallex billing integration.

Handles:
- Creating a hosted payment page (HPP) link for new subscriptions
- Fetching an Airwallex API token (client_credentials flow)
- Cancelling an Airwallex subscription
- Verifying webhook signatures (HMAC-SHA256)

Environment variables required:
  AIRWALLEX_CLIENT_ID      — your Airwallex client ID
  AIRWALLEX_API_KEY        — your Airwallex API key
  AIRWALLEX_PRODUCT_ID     — the Airwallex product/plan ID for the Pro plan
  AIRWALLEX_WEBHOOK_SECRET — shared secret for webhook HMAC verification
  BASE_URL                 — your public domain (e.g. https://api.yourdomain.com)

Airwallex API base: https://api.airwallex.com  (use https://api-demo.airwallex.com for sandbox)
"""
import hashlib
import hmac
import os
import time
from typing import Optional

import httpx

# ---------------------------------------------------------------------------
# Config helpers
# ---------------------------------------------------------------------------

def _airwallex_base() -> str:
    return os.environ.get("AIRWALLEX_API_BASE", "https://api.airwallex.com")


def _client_id() -> str:
    v = os.environ.get("AIRWALLEX_CLIENT_ID", "")
    if not v:
        raise RuntimeError("AIRWALLEX_CLIENT_ID env var not set")
    return v


def _api_key() -> str:
    v = os.environ.get("AIRWALLEX_API_KEY", "")
    if not v:
        raise RuntimeError("AIRWALLEX_API_KEY env var not set")
    return v


def _product_id() -> str:
    v = os.environ.get("AIRWALLEX_PRODUCT_ID", "")
    if not v:
        raise RuntimeError("AIRWALLEX_PRODUCT_ID env var not set")
    return v


def _webhook_secret() -> str:
    return os.environ.get("AIRWALLEX_WEBHOOK_SECRET", "")


def _base_url() -> str:
    return os.environ.get("BASE_URL", "").rstrip("/")


# ---------------------------------------------------------------------------
# Auth — Airwallex uses client_credentials to get a short-lived Bearer token
# ---------------------------------------------------------------------------

_token_cache: dict = {"token": None, "expires_at": 0}


async def _get_bearer_token() -> str:
    """Return a cached Airwallex API Bearer token, refreshing if expired."""
    if _token_cache["token"] and time.time() < _token_cache["expires_at"] - 60:
        return _token_cache["token"]

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{_airwallex_base()}/api/v1/authentication/login",
            headers={
                "x-client-id": _client_id(),
                "x-api-key": _api_key(),
                "Content-Type": "application/json",
            },
        )

    if resp.status_code != 201:
        raise RuntimeError(f"Airwallex auth failed: {resp.status_code} {resp.text}")

    data = resp.json()
    _token_cache["token"] = data["token"]
    # Airwallex tokens expire in 30 minutes (1800s)
    _token_cache["expires_at"] = time.time() + data.get("expires_in", 1800)
    return _token_cache["token"]


# ---------------------------------------------------------------------------
# Customers — Billing Customer (new Billing API)
# ---------------------------------------------------------------------------

def _price_id() -> str:
    v = os.environ.get("AIRWALLEX_PRICE_ID", "")
    if not v:
        raise RuntimeError("AIRWALLEX_PRICE_ID env var not set")
    return v


async def create_or_get_customer(user_id: str, email: str, name: str) -> str:
    """
    Create an Airwallex Billing Customer and return their billing_customer_id.
    Uses the new /api/v1/billing_customers/create endpoint.
    """
    token = await _get_bearer_token()
    first = name.split()[0] if name else ""
    last = " ".join(name.split()[1:]) if name and len(name.split()) > 1 else ""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{_airwallex_base()}/api/v1/billing_customers/create",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "request_id": f"bcust-{user_id}",
                "merchant_customer_id": user_id,
                "email": email,
                "first_name": first,
                "last_name": last,
            },
        )
    if resp.status_code not in (200, 201):
        # If customer already exists, extract existing ID from error
        try:
            err = resp.json()
            if err.get("code") in ("duplicate_request_id", "already_exists"):
                existing_id = err.get("details", {}).get("id")
                if existing_id:
                    return existing_id
        except Exception:
            pass
        raise RuntimeError(f"Airwallex billing customer create failed: {resp.text}")
    return resp.json()["id"]


# ---------------------------------------------------------------------------
# Billing Checkout — hosted checkout page for subscriptions
# ---------------------------------------------------------------------------

async def create_checkout_url(
    user_id: str,
    email: str,
    name: str,
    airwallex_customer_id: Optional[str] = None,
    seats: int = 1,
) -> str:
    """
    Create an Airwallex Billing Checkout (hosted page) for a subscription.
    Uses the new /api/v1/billing_checkouts/create endpoint.
    Returns the URL to redirect the user to.
    """
    token = await _get_bearer_token()

    if not airwallex_customer_id:
        airwallex_customer_id = await create_or_get_customer(user_id, email, name)

    success_url = f"{_base_url()}/billing/success?user_id={user_id}"
    cancel_url = f"{_base_url()}/billing/cancel?user_id={user_id}"

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{_airwallex_base()}/api/v1/billing_checkouts/create",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={
                "request_id": f"checkout-{user_id}-{int(time.time())}",
                "mode": "SUBSCRIPTION",
                "billing_customer_id": airwallex_customer_id,
                "line_items": [
                    {"price_id": _price_id(), "quantity": max(1, seats)},
                ],
                "subscription_data": {
                    "collection_method": "AUTO_CHARGE",
                    "metadata": {"internal_user_id": user_id},
                },
                "success_url": success_url,
                "cancel_url": cancel_url,
            },
        )

    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Airwallex checkout create failed: {resp.text}")

    data = resp.json()
    checkout_url = data.get("url") or data.get("hosted_payment_page_url") or data.get("checkout_url")
    if not checkout_url:
        raise RuntimeError(f"No checkout URL in Airwallex response: {data}")
    return checkout_url


# ---------------------------------------------------------------------------
# Cancel subscription
# ---------------------------------------------------------------------------

async def cancel_subscription(airwallex_subscription_id: str) -> bool:
    """Cancel an Airwallex subscription. Returns True on success."""
    token = await _get_bearer_token()
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{_airwallex_base()}/api/v1/subscriptions/{airwallex_subscription_id}/cancel",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"request_id": f"cancel-{airwallex_subscription_id}-{int(time.time())}"},
        )
    return resp.status_code in (200, 201)


# ---------------------------------------------------------------------------
# Webhook signature verification
# ---------------------------------------------------------------------------

def verify_webhook_signature(payload_bytes: bytes, signature_header: str) -> bool:
    """
    Verify an Airwallex webhook payload using HMAC-SHA256.

    Airwallex sends the signature in the `x-signature` header as:
      t=<timestamp>,v1=<hex_signature>

    The signed string is: <timestamp>.<raw_body>
    """
    secret = _webhook_secret()
    if not secret:
        # SECURITY: In production, webhook secret MUST be set. Skip only in dev mode.
        import logging
        logging.getLogger(__name__).warning(
            "AIRWALLEX_WEBHOOK_SECRET not set — webhook signature verification SKIPPED. "
            "Set the secret for production use!"
        )
        return True

    try:
        parts = dict(item.split("=", 1) for item in signature_header.split(","))
        timestamp = parts.get("t", "")
        expected = parts.get("v1", "")
        if not timestamp or not expected:
            return False

        signed_payload = f"{timestamp}.".encode() + payload_bytes
        computed = hmac.new(
            secret.encode(),
            signed_payload,
            hashlib.sha256,
        ).hexdigest()
        return hmac.compare_digest(computed, expected)
    except Exception:
        return False
