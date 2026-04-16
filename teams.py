"""
Team management + MCP credential endpoints.

Routes (all require authentication):
  GET    /user/credentials            – get current user's MCP client_id
  POST   /user/credentials/rotate     – regenerate client_secret (shows new secret ONCE)
  GET    /team                        – get team info + member list (owner only)
  POST   /team                        – create a team (converts solo seat → team)
  POST   /team/invite                 – invite a member by email
  DELETE /team/members/{member_id}    – remove a member (owner only)
  GET    /team/usage                  – per-member usage stats (owner only)
  GET    /team/accept-invite          – accept an invite via token (link sent in email)
"""
import logging
import os

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, EmailStr

from auth import verify_jwt_user_any as verify_jwt
from database import (
    McpCredential,
    SessionLocal,
    Team,
    TeamMember,
    User,
    accept_team_invite,
    count_active_team_seats,
    generate_mcp_credential,
    get_mcp_credential,
    get_team_by_owner,
    get_team_for_member,
    get_team_members_with_usage,
    invite_team_member,
    create_team,
)
from plans import get_plan, TEAM_PLAN_ID

router = APIRouter(tags=["teams"])


# ---------------------------------------------------------------------------
# Auth dependency
# ---------------------------------------------------------------------------

def _get_current_user(request: Request) -> User:
    token = request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
    user = verify_jwt(token)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid or expired token")
    return user


# ---------------------------------------------------------------------------
# MCP Credentials
# ---------------------------------------------------------------------------

@router.get("/user/credentials")
async def get_credentials(user: User = Depends(_get_current_user)):
    """
    Return the user's MCP connection details.
    client_secret is NEVER returned here — it is only shown once at generation time.
    """
    cred = get_mcp_credential(user.id)
    server_url = os.environ.get("SERVER_BASE_URL", "").rstrip("/")
    if cred is None:
        return JSONResponse({
            "has_credential": False,
            "server_url": f"{server_url}/mcp",
            "message": "No credential yet. Subscribe to a plan to get your client credentials.",
        })
    return JSONResponse({
        "has_credential": True,
        "client_id": cred.client_id,
        "server_url": f"{server_url}/mcp",
        "last_connected": cred.last_connected_at.isoformat() if cred.last_connected_at else None,
    })


@router.post("/user/credentials/rotate")
async def rotate_credentials(user: User = Depends(_get_current_user)):
    """
    Regenerate the MCP client_secret.
    ⚠️ The new secret is shown ONCE — the user must update Claude.ai immediately.
    The existing active session is revoked.
    """
    client_id, plain_secret = generate_mcp_credential(user.id)
    server_url = os.environ.get("SERVER_BASE_URL", "").rstrip("/")
    return JSONResponse({
        "client_id": client_id,
        "client_secret": plain_secret,   # shown once only
        "server_url": f"{server_url}/mcp",
        "warning": "Save this client_secret now — it will NOT be shown again.",
    })


@router.post("/user/credentials/generate")
async def generate_credentials(user: User = Depends(_get_current_user)):
    """
    Generate an initial MCP credential for an existing paid user who doesn't have one yet.
    Returns existing client_id if already generated (without the secret).
    Use /user/credentials/rotate to reset both id and secret.
    """
    cred = get_mcp_credential(user.id)
    server_url = os.environ.get("SERVER_BASE_URL", "").rstrip("/")
    if cred is not None:
        return JSONResponse({
            "already_exists": True,
            "client_id": cred.client_id,
            "server_url": f"{server_url}/mcp",
            "message": "Use POST /user/credentials/rotate to get a new client_secret.",
        })
    client_id, plain_secret = generate_mcp_credential(user.id)
    return JSONResponse({
        "client_id": client_id,
        "client_secret": plain_secret,
        "server_url": f"{server_url}/mcp",
        "warning": "Save this client_secret now — it will NOT be shown again.",
    })


# ---------------------------------------------------------------------------
# Team management
# ---------------------------------------------------------------------------

class CreateTeamRequest(BaseModel):
    name: str
    seat_count: int = 2   # number of paid seats (≥2 for team plan)


class InviteMemberRequest(BaseModel):
    email: str


@router.post("/team")
async def create_team_endpoint(
    body: CreateTeamRequest,
    user: User = Depends(_get_current_user),
):
    """
    Create a team for the current user (they become owner, seat 1).
    seat_count controls max_seats on the Team record.
    Generates an MCP credential for the owner if they don't already have one.
    """
    if get_team_by_owner(user.id):
        raise HTTPException(status_code=400, detail="You already have a team. Use /team/invite to add members.")

    seat_count = max(2, body.seat_count)
    team = create_team(
        owner_user_id=user.id,
        name=body.name,
        max_seats=seat_count,
        plan_id=TEAM_PLAN_ID,
    )
    # Ensure owner has an MCP credential
    cred = get_mcp_credential(user.id)
    if cred is None:
        client_id, plain_secret = generate_mcp_credential(user.id)
        return JSONResponse({
            "team_id": team.id,
            "name": team.name,
            "max_seats": team.max_seats,
            "owner_credential": {
                "client_id": client_id,
                "client_secret": plain_secret,
                "warning": "Save this client_secret now — it will NOT be shown again.",
            },
        })
    return JSONResponse({
        "team_id": team.id,
        "name": team.name,
        "max_seats": team.max_seats,
    })


@router.get("/team")
async def get_team(user: User = Depends(_get_current_user)):
    """Return team info. Only the owner can use this endpoint."""
    team = get_team_by_owner(user.id)
    if team is None:
        # Check if user is a member of someone else's team
        member_team = get_team_for_member(user.id)
        if member_team:
            return JSONResponse({
                "role": "member",
                "team_id": member_team.id,
                "name": member_team.name,
            })
        raise HTTPException(status_code=404, detail="No team found. Create one with POST /team.")

    members = get_team_members_with_usage(team.id)
    member_seats_used = count_active_team_seats(team.id)
    member_seats_max = team.max_seats - 1  # owner occupies 1 seat
    return JSONResponse({
        "role": "owner",
        "team_id": team.id,
        "name": team.name,
        "plan": team.plan_id,
        "max_seats": team.max_seats,
        "member_seats_max": member_seats_max,
        "seats_used": member_seats_used,
        "seats_available": member_seats_max - member_seats_used,
        "status": team.status,
        "members": members,
    })


@router.post("/team/invite")
async def invite_member(
    body: InviteMemberRequest,
    user: User = Depends(_get_current_user),
):
    """
    Invite an email address to the team.
    - If the user already has an account, they're added immediately and get a credential.
    - If not, an invitation email is sent and they'll be activated on signup.
    """
    team = get_team_by_owner(user.id)
    if team is None:
        raise HTTPException(status_code=403, detail="Only team owners can invite members.")

    seats_used = count_active_team_seats(team.id)
    member_seats_max = team.max_seats - 1  # owner occupies 1 seat
    if seats_used >= member_seats_max:
        raise HTTPException(
            status_code=400,
            detail=f"Seat limit reached ({member_seats_max} member seats). Upgrade your plan to add more members.",
        )

    member, invite_token = invite_team_member(team.id, body.email.lower().strip())

    # If user already exists and was just linked, generate their credential
    new_credential: dict | None = None
    if member.user_id:
        cred = get_mcp_credential(member.user_id)
        if cred is None:
            client_id, plain_secret = generate_mcp_credential(member.user_id)
            new_credential = {
                "client_id": client_id,
                "client_secret": plain_secret,
                "warning": "Save this client_secret — it will NOT be shown again.",
            }

    # Send invitation email (best-effort)
    server_url = os.environ.get("SERVER_BASE_URL", "").rstrip("/")
    if invite_token:
        accept_url = f"{server_url}/team/accept-invite?token={invite_token}"
        try:
            from emails import send_team_invite
            send_team_invite(
                to_email=body.email,
                inviter_name=user.name or user.email,
                team_name=team.name,
                accept_url=accept_url,
            )
        except Exception as _email_exc:
            logging.getLogger(__name__).warning("team invite email failed: %s", _email_exc)

    response: dict = {
        "member_id": member.id,
        "email": member.email,
        "status": member.status,
    }
    if invite_token:
        response["invite_token"] = invite_token  # useful for testing / manual share
    if new_credential:
        response["credential"] = new_credential
    return JSONResponse(response)


@router.delete("/team/members/{member_id}")
async def remove_member(
    member_id: str,
    user: User = Depends(_get_current_user),
):
    """Remove a team member. Revokes their MCP credential and active session."""
    team = get_team_by_owner(user.id)
    if team is None:
        raise HTTPException(status_code=403, detail="Only team owners can remove members.")

    db = SessionLocal()
    try:
        member = db.query(TeamMember).filter(
            TeamMember.id == member_id,
            TeamMember.team_id == team.id,
        ).first()
        if member is None:
            raise HTTPException(status_code=404, detail="Member not found.")
        if member.user_id == user.id:
            raise HTTPException(status_code=400, detail="You cannot remove yourself (you are the owner).")

        member.status = "removed"

        # Revoke their MCP credential (and active session)
        if member.user_id:
            cred = db.query(McpCredential).filter(
                McpCredential.user_id == member.user_id
            ).first()
            if cred:
                # Revoke active JWT
                if cred.active_jti:
                    from database import _revoke_jti_inline
                    _revoke_jti_inline(db, member.user_id, cred.active_jti)
                db.delete(cred)

        db.commit()
    finally:
        db.close()

    return JSONResponse({"ok": True, "removed_member_id": member_id})


@router.get("/team/usage")
async def team_usage(user: User = Depends(_get_current_user)):
    """Return per-member usage stats for the past 30 days."""
    team = get_team_by_owner(user.id)
    if team is None:
        raise HTTPException(status_code=403, detail="Only team owners can view team usage.")
    members = get_team_members_with_usage(team.id)
    return JSONResponse({"team_id": team.id, "members": members})


# ---------------------------------------------------------------------------
# Accept invite (used from email link — no auth needed)
# ---------------------------------------------------------------------------

@router.get("/team/accept-invite")
async def accept_invite_page(token: str = Query(...)):
    """
    Browser-facing page that a team member visits from their invite email.
    If they're already logged in (via cookie/localStorage), handles it server-side.
    Otherwise shows a simple page directing them to sign up / log in.
    """
    server_url = os.environ.get("SERVER_BASE_URL", "").rstrip("/")
    # We store the token in a query param so the Google OAuth flow can pick it up
    google_auth_url = f"{server_url}/auth/google/start?team_invite={token}"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Join Team – MarketingMCP</title>
  <style>
    body {{ font-family: system-ui, -apple-system, sans-serif; max-width: 480px; margin: 80px auto; padding: 0 20px; color: #111; text-align: center; }}
    h1 {{ font-size: 1.6rem; }}
    p {{ color: #6b7280; }}
    .btn {{ display: inline-block; margin-top: 20px; padding: 14px 28px; background: #d4622a; color: #fff; border-radius: 8px; font-size: 1rem; font-weight: 700; text-decoration: none; }}
    .btn:hover {{ background: #b8541f; }}
  </style>
</head>
<body>
  <h1>You're invited!</h1>
  <p>Sign in with Google to join the team and get your MarketingMCP connection credentials.</p>
  <a href="{google_auth_url}" class="btn">Sign in with Google</a>
</body>
</html>"""
    return HTMLResponse(html)


@router.get("/team/credential-handoff")
async def credential_handoff(
    user_id: str = Query(""),
    token: str = Query(""),
    client_id: str = Query(""),
    client_secret: str = Query(""),
):
    """
    Shown to a new team member right after they accept an invite.
    Displays their MCP client_id and client_secret ONCE.
    """
    server_url = os.environ.get("SERVER_BASE_URL", "").rstrip("/")
    mcp_url = f"{server_url}/mcp"
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Your MarketingMCP Credentials</title>
  <style>
    *{{box-sizing:border-box}}
    body{{font-family:system-ui,-apple-system,sans-serif;background:#f9fafb;color:#111;margin:0;padding:40px 20px}}
    .card{{max-width:540px;margin:0 auto;background:#fff;border-radius:12px;padding:32px;box-shadow:0 2px 12px rgba(0,0,0,.08)}}
    h1{{font-size:1.5rem;margin:0 0 6px}}
    .sub{{color:#6b7280;margin:0 0 28px;font-size:.95rem}}
    .warn{{background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:14px 16px;margin-bottom:24px;font-size:.9rem;color:#92400e}}
    label{{display:block;font-size:.82rem;font-weight:600;color:#374151;margin-bottom:4px;text-transform:uppercase;letter-spacing:.5px}}
    .field{{display:flex;gap:8px;margin-bottom:18px}}
    input{{flex:1;padding:10px 14px;border:1px solid #d1d5db;border-radius:6px;font-size:.9rem;font-family:monospace;background:#f3f4f6}}
    .copy-btn{{padding:10px 14px;background:#d4622a;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:.85rem;font-weight:600;white-space:nowrap}}
    .copy-btn:hover{{background:#b8541f}}
    .divider{{border:none;border-top:1px solid #e5e7eb;margin:24px 0}}
    .steps{{font-size:.9rem;color:#374151;line-height:1.8}}
    .steps li{{margin-bottom:6px}}
    .steps code{{background:#f3f4f6;padding:2px 6px;border-radius:4px;font-size:.85rem}}
    .done-btn{{display:block;width:100%;padding:14px;background:#111827;color:#fff;border:none;border-radius:8px;font-size:1rem;font-weight:700;cursor:pointer;text-align:center;text-decoration:none;margin-top:24px}}
    .done-btn:hover{{background:#374151}}
  </style>
</head>
<body>
<div class="card">
  <h1>Welcome to the team! 🎉</h1>
  <p class="sub">Your MarketingMCP connection credentials are below. Save them now — the secret is shown only once.</p>
  <div class="warn">
    ⚠️ <strong>Copy your Client Secret now.</strong> It cannot be retrieved again. If you lose it, your team owner can rotate your credentials.
  </div>

  <label>MCP Server URL</label>
  <div class="field">
    <input id="f-url" value="{mcp_url}" readonly>
    <button class="copy-btn" onclick="copy('f-url')">Copy</button>
  </div>

  <label>OAuth Client ID</label>
  <div class="field">
    <input id="f-cid" value="{client_id}" readonly>
    <button class="copy-btn" onclick="copy('f-cid')">Copy</button>
  </div>

  <label>OAuth Client Secret</label>
  <div class="field">
    <input id="f-cs" value="{client_secret}" readonly>
    <button class="copy-btn" onclick="copy('f-cs')">Copy</button>
  </div>

  <hr class="divider">
  <p style="font-weight:600;margin-bottom:8px">How to connect in Claude.ai:</p>
  <ol class="steps">
    <li>Open <strong>Claude.ai</strong> → Settings → Integrations → <em>Add custom integration</em></li>
    <li>Enter the <strong>MCP Server URL</strong> above</li>
    <li>Toggle <em>"OAuth Client ID/Secret"</em> and paste the values above</li>
    <li>Click <strong>Connect</strong> and complete the Google sign-in</li>
  </ol>

  <a class="done-btn" href="/">Done — go to dashboard</a>
</div>
<script>
function copy(id) {{
  const el = document.getElementById(id);
  navigator.clipboard.writeText(el.value).then(() => {{
    const btn = el.nextElementSibling;
    btn.textContent = 'Copied!';
    setTimeout(() => btn.textContent = 'Copy', 1500);
  }});
}}
</script>
</body>
</html>"""
    return HTMLResponse(html)
