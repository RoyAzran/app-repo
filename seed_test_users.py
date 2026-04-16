"""
Seed two test users directly into the DB — no payment needed.

User 1: solo@test.com  — Solo plan  (active subscription)
User 2: team@test.com  — Team plan  (3 seats, 1 invited member)

Run:
    python seed_test_users.py

Prints JWT tokens you can paste into browser localStorage to log in.
"""
import os, sys
from datetime import datetime, timedelta, timezone
from dotenv import load_dotenv

load_dotenv(override=True)

# Must be loaded after env
from database import (
    SessionLocal, User, Subscription, Team, TeamMember,
    create_tables, generate_mcp_credential,
)
from auth import create_jwt

create_tables()
db = SessionLocal()

def upsert_user(email: str, name: str) -> User:
    u = db.query(User).filter(User.email == email).first()
    if not u:
        u = User(email=email, name=name, role="admin", is_active=True)
        db.add(u)
        db.flush()
    return u

def upsert_sub(user_id: str, plan: str) -> None:
    s = db.query(Subscription).filter(Subscription.user_id == user_id).first()
    now = datetime.now(timezone.utc)
    if not s:
        s = Subscription(
            user_id=user_id,
            status="active",
            airwallex_subscription_id=f"test_sub_{plan}_{user_id[:8]}",
            current_period_start=now,
            current_period_end=now + timedelta(days=30),
        )
        db.add(s)
    else:
        s.status = "active"
    db.flush()

# ---- User 1: Solo ----
solo = upsert_user("solo@test.com", "Solo Tester")
upsert_sub(solo.id, "solo")

solo_team = db.query(Team).filter(Team.owner_user_id == solo.id).first()
if not solo_team:
    solo_team = Team(owner_user_id=solo.id, name="Solo Workspace", max_seats=1, plan_id="solo", status="active")
    db.add(solo_team)
    db.flush()
    db.add(TeamMember(team_id=solo_team.id, user_id=solo.id, email=solo.email, status="active", joined_at=datetime.now(timezone.utc)))

# ---- User 2: Team Owner ----
team_owner = upsert_user("team@test.com", "Team Tester")
upsert_sub(team_owner.id, "team")

owner_team = db.query(Team).filter(Team.owner_user_id == team_owner.id).first()
if not owner_team:
    owner_team = Team(owner_user_id=team_owner.id, name="Acme Agency", max_seats=3, plan_id="team", status="active")
    db.add(owner_team)
    db.flush()
    db.add(TeamMember(team_id=owner_team.id, user_id=team_owner.id, email=team_owner.email, status="active", joined_at=datetime.now(timezone.utc)))
    # Add an invited member (not yet accepted)
    db.add(TeamMember(team_id=owner_team.id, user_id=None, email="member@test.com", status="invited"))

db.commit()

# Capture IDs before closing session
solo_id = solo.id
team_owner_id = team_owner.id

db.close()

# Generate MCP credentials (creates or rotates)
solo_cid, solo_secret = generate_mcp_credential(solo_id)
team_cid, team_secret = generate_mcp_credential(team_owner_id)

# Issue JWT tokens
solo_token = create_jwt(solo_id)
team_token = create_jwt(team_owner_id)

print("\n" + "="*60)
print("TEST USERS SEEDED")
print("="*60)

print("\n--- SOLO USER (solo@test.com) ---")
print(f"  User ID : {solo_id}")
print(f"  MCP Client ID : {solo_cid}")
print(f"  MCP Secret    : {solo_secret}  (one-time, save it)")
print(f"\n  JWT Token (paste in browser console):")
print(f"  localStorage.setItem('auth_token', '{solo_token}')")
print(f"  window.location.href = '/static/manage.html'")

print("\n--- TEAM OWNER (team@test.com) ---")
print(f"  User ID : {team_owner_id}")
print(f"  Team    : Acme Agency (3 seats, 1 invited: member@test.com)")
print(f"  MCP Client ID : {team_cid}")
print(f"  MCP Secret    : {team_secret}  (one-time, save it)")
print(f"\n  JWT Token (paste in browser console):")
print(f"  localStorage.setItem('auth_token', '{team_token}')")
print(f"  window.location.href = '/static/manage.html'")

print("\n" + "="*60)
print("Open http://localhost:8000 , open DevTools console, paste the")
print("localStorage lines above, then navigate to manage.html")
print("="*60 + "\n")
