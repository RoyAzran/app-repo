"""
Email service — Resend.

Sends transactional and marketing emails.
All sends are deduplicated via the email_log table so the same email type
is never sent twice to the same user (unless explicitly allowed via
allow_resend=True).

Email types:
  welcome            — fired once on account creation
  receipt            — fired once on first successful payment
  receipt_renewal    — fired on each subsequent payment (no dedup)
  drip_day2          — day 2 post-signup, no purchase yet
  drip_day5          — day 5 post-signup, no purchase yet
  drip_day7          — day 7 post-signup, no purchase yet (last chance)
  onboarding         — fired once after subscription goes active
  win_back           — fired once after subscription is cancelled
  trial_ending       — fired 3 days before trial expires
"""
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

import resend

from database import (
    EmailLog, SessionLocal, Subscription, User,
    email_already_sent, log_email,
)

logger = logging.getLogger(__name__)

resend.api_key = os.environ.get("RESEND_API_KEY", "").strip()
_FROM = os.environ.get("EMAIL_FROM", "MarketingMCP <hello@mcp-ads.com>")
_BASE = os.environ.get("BASE_URL", "http://localhost:8000").rstrip("/")

# ── Shared style ─────────────────────────────────────────────────────────────

_STYLE = """<style>
  body { margin:0; padding:0; background:#fafaf8; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif; }
  .wrap { max-width:560px; margin:40px auto; background:#fff; border-radius:12px; overflow:hidden; box-shadow:0 2px 16px rgba(0,0,0,.06); }
  .header { background:#d4622a; padding:32px 40px; }
  .header h1 { margin:0; color:#fff; font-size:24px; font-weight:700; }
  .body { padding:36px 40px; color:#333; line-height:1.65; font-size:15px; }
  .body h2 { font-size:20px; margin-top:0; color:#1a1a1a; }
  .btn { display:inline-block; margin-top:20px; padding:14px 28px; background:#d4622a; color:#fff !important; text-decoration:none; border-radius:8px; font-weight:600; font-size:15px; }
  .muted { color:#888; font-size:13px; margin-top:32px; border-top:1px solid #eee; padding-top:20px; }
  .stat { display:inline-block; background:#fff7f4; border:1px solid #f4c5ae; border-radius:8px; padding:12px 20px; margin:8px 8px 8px 0; }
  .stat .val { font-size:22px; font-weight:700; color:#d4622a; }
  .stat .lbl { font-size:12px; color:#888; margin-top:2px; }
</style>"""

_FOOTER = """<p class="muted">
  MarketingMCP · mcp-ads.com<br>
  <a href="{base}/manage" style="color:#d4622a;">Manage account</a> ·
  <a href="{base}/unsubscribe" style="color:#888;">Unsubscribe</a>
</p>""".format(base=_BASE)


def _html(body: str) -> str:
    return f"<!DOCTYPE html><html><head><meta charset='utf-8'>{_STYLE.strip()}</head><body><div class='wrap'>{body.strip()}{_FOOTER.strip()}</div></body></html>"


# ── Send helper ───────────────────────────────────────────────────────────────

def _send(to: str, subject: str, html: str, user_id: str, email_type: str,
          allow_resend: bool = False) -> bool:
    if not resend.api_key:
        logger.warning("RESEND_API_KEY not set — skipping %s to %s", email_type, to)
        return False
    if not allow_resend and email_already_sent(user_id, email_type):
        logger.debug("Email %s already sent to %s — skipping", email_type, user_id)
        return False
    try:
        resend.Emails.send({
            "from": _FROM,
            "to": [to],
            "subject": subject,
            "html": html,
        })
        log_email(user_id, email_type, "sent")
        logger.info("Email sent: %s → %s", email_type, to)
        return True
    except Exception as exc:
        logger.error("Email send failed (%s → %s): %s", email_type, to, exc)
        log_email(user_id, email_type, "failed", str(exc))
        return False


# ── Transactional emails ──────────────────────────────────────────────────────

def send_welcome(user_id: str, email: str, name: str) -> None:
    """Welcome email sent once on account creation."""
    first = (name or "").split()[0] or "there"
    html = _html(f"""
<div class="header"><h1>Welcome to MarketingMCP 👋</h1></div>
<div class="body">
  <h2>Hey {first}, you're in!</h2>
  <p>Your MarketingMCP account is live. You now have access to <strong>360+ AI-powered marketing tools</strong> for Google Ads, Meta Ads, GA4, Google Search Console, and Sheets — all in one place.</p>
  <p>Next step: connect your accounts and start your free trial.</p>
  <a class="btn" href="{_BASE}/manage">Connect accounts →</a>
  <p style="margin-top:28px;">If you have any questions, just reply to this email.</p>
</div>
    """)
    _send(email, "Welcome to MarketingMCP 🚀", html, user_id, "welcome")


def send_receipt(user_id: str, email: str, name: str, amount_cents: int,
                 period_end: Optional[datetime] = None) -> None:
    """Payment receipt — sent on first successful payment."""
    first = (name or "").split()[0] or "there"
    amount_str = f"${amount_cents / 100:.2f}"
    renewal = period_end.strftime("%B %d, %Y") if period_end else "next month"
    html = _html(f"""
<div class="header"><h1>Payment confirmed ✓</h1></div>
<div class="body">
  <h2>Thanks, {first}!</h2>
  <p>Your MarketingMCP Pro subscription is now <strong>active</strong>. Here's your receipt:</p>
  <div class="stat"><div class="val">{amount_str}</div><div class="lbl">Amount charged</div></div>
  <div class="stat"><div class="val">{renewal}</div><div class="lbl">Next renewal</div></div>
  <p style="margin-top:24px;">You now have unlimited access to all 360+ marketing tools. Time to put them to work.</p>
  <a class="btn" href="{_BASE}/manage">Open dashboard →</a>
</div>
    """)
    _send(email, f"Receipt: MarketingMCP Pro — {amount_str}", html, user_id, "receipt")


def send_receipt_renewal(user_id: str, email: str, name: str, amount_cents: int,
                         period_end: Optional[datetime] = None) -> None:
    """Renewal receipt — sent on subsequent payments. No dedup (allow_resend=True)."""
    first = (name or "").split()[0] or "there"
    amount_str = f"${amount_cents / 100:.2f}"
    renewal = period_end.strftime("%B %d, %Y") if period_end else "next month"
    html = _html(f"""
<div class="header"><h1>Subscription renewed ✓</h1></div>
<div class="body">
  <h2>Hey {first}</h2>
  <p>Your MarketingMCP Pro subscription has been renewed. Here are the details:</p>
  <div class="stat"><div class="val">{amount_str}</div><div class="lbl">Amount charged</div></div>
  <div class="stat"><div class="val">{renewal}</div><div class="lbl">Next renewal</div></div>
  <a class="btn" href="{_BASE}/manage">Manage subscription →</a>
</div>
    """)
    _send(email, f"MarketingMCP Pro renewed — {amount_str}", html, user_id, "receipt_renewal",
          allow_resend=True)


def send_onboarding(user_id: str, email: str, name: str) -> None:
    """Post-purchase onboarding — sent once after subscription goes active."""
    first = (name or "").split()[0] or "there"
    html = _html(f"""
<div class="header"><h1>You're all set — let's get started 🚀</h1></div>
<div class="body">
  <h2>Here's how to connect Claude to your ad accounts</h2>
  <p>Follow these 3 steps to get your AI marketing assistant running:</p>
  <ol style="padding-left:20px; line-height:2;">
    <li><a href="{_BASE}/manage" style="color:#d4622a;">Go to your dashboard</a> and connect Google & Meta</li>
    <li>Open <strong>Claude.ai</strong> → Settings → Integrations → Add custom MCP server</li>
    <li>Paste your MCP endpoint URL and token — and you're live!</li>
  </ol>
  <a class="btn" href="{_BASE}/manage">Get set up now →</a>
  <p style="margin-top:28px;"><strong>Available tools include:</strong><br>
    Google Ads campaign management · Meta Ads reporting · GA4 analytics ·
    Search Console · Google Sheets integration
  </p>
</div>
    """)
    _send(email, "Set up your MarketingMCP tools in 3 steps", html, user_id, "onboarding")


def send_win_back(user_id: str, email: str, name: str) -> None:
    """Win-back email after cancellation."""
    first = (name or "").split()[0] or "there"
    html = _html(f"""
<div class="header"><h1>We're sorry to see you go</h1></div>
<div class="body">
  <h2>Hey {first}, your subscription has been cancelled</h2>
  <p>Your MarketingMCP access will remain active until the end of your current billing period.</p>
  <p>If you cancelled by accident, or want to come back, you can reactivate anytime — your settings and connections will be saved.</p>
  <a class="btn" href="{_BASE}/billing/start?user_id={user_id}">Reactivate subscription →</a>
  <p style="margin-top:28px; color:#888;">Was there something we could have done better? Just reply — we read everything.</p>
</div>
    """)
    _send(email, "Your MarketingMCP subscription has been cancelled", html, user_id, "win_back")


def send_trial_ending(user_id: str, email: str, name: str, trial_ends: datetime) -> None:
    """Alert sent 3 days before trial expires."""
    first = (name or "").split()[0] or "there"
    days_left = max(0, (trial_ends - datetime.now(timezone.utc)).days)
    ends_str = trial_ends.strftime("%B %d")
    html = _html(f"""
<div class="header"><h1>Your free trial ends in {days_left} days</h1></div>
<div class="body">
  <h2>Hey {first},</h2>
  <p>Your MarketingMCP free trial expires on <strong>{ends_str}</strong>. After that, you'll lose access to all 360+ tools.</p>
  <p>Upgrade now to keep your Google Ads, Meta, and analytics AI tools running without interruption.</p>
  <a class="btn" href="{_BASE}/billing/start?user_id={user_id}">Upgrade to Pro →</a>
</div>
    """)
    _send(email, f"⏰ Your MarketingMCP trial ends {ends_str}", html, user_id, "trial_ending")


def send_team_invite(to_email: str, inviter_name: str, team_name: str, accept_url: str) -> None:
    """Send a team invitation email."""
    html = _html(f"""
<div class="header"><h1>You've been invited to {team_name}</h1></div>
<div class="body">
  <h2>Hey there,</h2>
  <p><strong>{inviter_name}</strong> has invited you to join the <strong>{team_name}</strong> team on MarketingMCP.</p>
  <p>MarketingMCP connects your Google Ads, Meta Ads, Analytics, and more directly to Claude — so your whole team can manage marketing through AI chat.</p>
  <a class="btn" href="{accept_url}">Accept invitation →</a>
  <p style="margin-top:24px; color:#888; font-size:0.85rem;">This invitation link expires in 7 days. If you weren't expecting this, you can safely ignore it.</p>
</div>
    """)
    _send(to_email, f"{inviter_name} invited you to {team_name} on MarketingMCP", html, "", "team_invite", allow_resend=True)


# ── Drip emails ───────────────────────────────────────────────────────────────

def send_drip_day2(user_id: str, email: str, name: str) -> None:
    first = (name or "").split()[0] or "there"
    html = _html(f"""
<div class="header"><h1>Still thinking it over?</h1></div>
<div class="body">
  <h2>Hey {first},</h2>
  <p>You signed up for MarketingMCP two days ago — great choice starting the free trial!</p>
  <p>Here's a quick look at what Pro unlocks:</p>
  <ul style="padding-left:20px; line-height:2;">
    <li>🎯 <strong>Google Ads</strong> — create, pause, adjust bids on campaigns via AI chat</li>
    <li>📊 <strong>Meta Ads</strong> — campaign insights and budget management</li>
    <li>📈 <strong>GA4 + GSC</strong> — traffic, conversions and search analytics</li>
    <li>📋 <strong>Sheets</strong> — pull your data directly into reports</li>
  </ul>
  <p>All through a single Claude chat. No dashboards, no tab-switching.</p>
  <a class="btn" href="{_BASE}/billing/start?user_id={user_id}">Start Pro — $49/month →</a>
</div>
    """)
    _send(email, "What you can do with MarketingMCP Pro", html, user_id, "drip_day2")


def send_drip_day5(user_id: str, email: str, name: str) -> None:
    first = (name or "").split()[0] or "there"
    html = _html(f"""
<div class="header"><h1>Your competitors are moving faster</h1></div>
<div class="body">
  <h2>Hey {first},</h2>
  <p>Every day you're managing Google Ads, Meta, and analytics manually, you're spending time that could be spent on strategy.</p>
  <p>MarketingMCP Pro users typically save <strong>3–5 hours per week</strong> on reporting and campaign management.</p>
  <p>One conversation with Claude can:</p>
  <ul style="padding-left:20px; line-height:2;">
    <li>Pause all underperforming ad groups across campaigns</li>
    <li>Pull a competitive keyword report from GSC</li>
    <li>Export last 30 days of Meta performance to a Sheet</li>
  </ul>
  <a class="btn" href="{_BASE}/billing/start?user_id={user_id}">Get Pro access now →</a>
</div>
    """)
    _send(email, "Save 3–5 hours/week on ad management", html, user_id, "drip_day5")


def send_drip_day7(user_id: str, email: str, name: str) -> None:
    first = (name or "").split()[0] or "there"
    html = _html(f"""
<div class="header"><h1>Last chance — trial ends soon</h1></div>
<div class="body">
  <h2>Hey {first},</h2>
  <p>Your MarketingMCP free trial is ending. After it expires, you'll need to upgrade to keep using the tools.</p>
  <p>Pro is <strong>$49/month</strong> — less than the cost of one hour of freelance work, and it handles what used to take you days.</p>
  <a class="btn" href="{_BASE}/billing/start?user_id={user_id}">Upgrade to Pro →</a>
  <p style="margin-top:24px; color:#888; font-size:14px;">Questions? Just reply to this email and we'll get back to you same day.</p>
</div>
    """)
    _send(email, "🔔 Your MarketingMCP trial is ending", html, user_id, "drip_day7")


# ── Drip scheduler — called from /internal/process-emails cron endpoint ──────

def process_drip_queue() -> dict:
    """
    Walk all users who haven't purchased yet and send the appropriate drip email
    based on how many days they've been signed up.
    Also sends trial_ending alerts.
    Called by the internal cron endpoint.
    """
    db = SessionLocal()
    sent = 0
    skipped = 0
    now = datetime.now(timezone.utc)
    try:
        users = db.query(User).filter(User.is_active == True).all()
        for user in users:
            age_days = (now - user.created_at.replace(tzinfo=timezone.utc)).days

            # Check subscription status
            sub = db.query(Subscription).filter(Subscription.user_id == user.id).first()
            has_paid = sub and sub.status in ("active",)
            is_canceled = sub and sub.status == "canceled"

            if is_canceled:
                if _send_if_eligible(user, "win_back"):
                    sent += 1
                else:
                    skipped += 1
                continue

            if has_paid:
                # Onboarding email — once after going active
                if not email_already_sent(user.id, "onboarding"):
                    send_onboarding(user.id, user.email, user.name)
                    sent += 1
                # Trial ending check
                if sub and sub.trial_ends_at:
                    days_to_trial_end = (sub.trial_ends_at.replace(tzinfo=timezone.utc) - now).days
                    if days_to_trial_end <= 3 and not email_already_sent(user.id, "trial_ending"):
                        send_trial_ending(user.id, user.email, user.name, sub.trial_ends_at)
                        sent += 1
                continue

            # Not paid — drip sequence
            if age_days >= 7 and not email_already_sent(user.id, "drip_day7"):
                send_drip_day7(user.id, user.email, user.name)
                sent += 1
            elif age_days >= 5 and not email_already_sent(user.id, "drip_day5"):
                send_drip_day5(user.id, user.email, user.name)
                sent += 1
            elif age_days >= 2 and not email_already_sent(user.id, "drip_day2"):
                send_drip_day2(user.id, user.email, user.name)
                sent += 1
            else:
                skipped += 1

    finally:
        db.close()

    return {"sent": sent, "skipped": skipped}


def _send_if_eligible(user: User, email_type: str) -> bool:
    """Helper: return True only if we haven't sent this type yet (used inline in loop)."""
    return not email_already_sent(user.id, email_type)
