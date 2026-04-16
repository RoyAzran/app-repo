# QA Checklist — mcp-ads.com

Full end-to-end review from both owner and member perspective.
Test on: **https://mcp-ads.com** (production)

---

## 1. ONBOARDING — New user flow

### 1.1 Landing & sign-up
- [ ] Visit `/` → marketing page loads, no console errors
- [ ] Visit `/pricing` → plans display correctly, no broken badges
- [ ] Click "Get Started" → redirects to `/onboard`
- [ ] `/onboard` → Google OAuth button visible
- [ ] Sign in with a **fresh Google account** → redirects to `/setup`
- [ ] Sign in with **Meta (Facebook)** → redirects to `/setup`

### 1.2 First-time setup
- [ ] `/setup` loads with loading spinner, then shows account info
- [ ] Trial badge shows correctly (e.g. "Trial — X days left")
- [ ] Onboarding welcome email received in inbox (check spam too)
- [ ] MCP credential (client ID + secret) generated and shown
- [ ] "Copy" buttons on credentials work

---

## 2. AUTHENTICATION

- [ ] Refresh page on `/manage` → stays logged in (JWT refresh works)
- [ ] Open two browser tabs → both show same account state
- [ ] Log out → redirected to `/` or `/onboard`
- [ ] Try accessing `/manage` while logged out → redirected to login
- [ ] Token expiry: wait or force-expire JWT → graceful re-auth, no white screen

---

## 3. SUBSCRIPTION & BILLING

### 3.1 Trial
- [ ] Fresh account shows "Trial" badge on dashboard
- [ ] Trial plan limits respected (check call count cap)
- [ ] `/billing/status` returns correct `plan`, `status`, `trial_ends_at`

### 3.2 Upgrade
- [ ] Click "Upgrade" → redirects to Airwallex checkout
- [ ] Complete payment with test card → returns to `/billing/success`
- [ ] Dashboard updates: trial badge gone, plan shows "Pro" or team plan name
- [ ] Receipt email received in inbox

### 3.3 Cancel
- [ ] "Cancel subscription" button only appears when subscribed (not on trial)
- [ ] Click cancel → confirmation prompt shown
- [ ] Confirm cancel → subscription status becomes "canceled" in UI
- [ ] Can still use service until period end (check `cancel_at_period_end`)
- [ ] Win-back email received after cancellation

### 3.4 Renewal
- [ ] Renewal webhook fires → receipt_renewal email received
- [ ] Dashboard seat count / plan unchanged after renewal

---

## 4. MCP CREDENTIALS

- [ ] Client ID and Secret visible on `/manage` (or `/setup`)
- [ ] Credential works in Claude Desktop — connect to `https://mcp-ads.com/mcp`
- [ ] "Rotate credential" → new secret issued, old one stops working
- [ ] "Generate new" → fresh client ID + secret pair
- [ ] `last_connected` timestamp updates after first MCP call

---

## 5. TEAM MANAGEMENT (Owner perspective)

### 5.1 Member list
- [ ] Owner appears first in the table with orange "owner" badge
- [ ] Owner row shows "You" instead of Remove button
- [ ] Seat badge shows e.g. "1/4 member seats" (owner excluded from count)
- [ ] "member seats" label not "total seats"

### 5.2 Invite
- [ ] Invite form accepts valid email and shows success toast
- [ ] Invite email delivered to recipient inbox (not spam)
- [ ] Email contains correct accept link (`/accept-invite?token=...`)
- [ ] Invited member appears in table with gray "invited" badge
- [ ] Inviting beyond seat limit shows a clear error message

### 5.3 Accept invite (Member perspective)
- [ ] Recipient clicks accept link → account created or linked
- [ ] Status changes from "invited" to "active" in owner dashboard
- [ ] Member gets MCP credential automatically
- [ ] Member can connect Claude to `/mcp` with their own credential

### 5.4 Remove member
- [ ] "Remove" button fires DELETE `/team/members/{member_id}`
- [ ] Member disappears from table immediately
- [ ] Removed member's credential is revoked (can no longer call MCP)
- [ ] Seat count decrements correctly

### 5.5 Permissions
- [ ] Non-owner member cannot access invite form
- [ ] Non-owner member cannot see Remove buttons
- [ ] Non-owner member cannot cancel team subscription

---

## 6. MCP TOOLS — Google Ads

In Claude Desktop, connected with valid credential:

- [ ] `google_ads_list_accounts` — lists accessible customer accounts
- [ ] `google_ads_campaign_performance` — returns campaign data with metrics
- [ ] `google_ads_ad_group_performance` — ad group breakdown
- [ ] `google_ads_keyword_performance` — keyword stats
- [ ] `google_ads_search_terms` — search term report
- [ ] `google_ads_keyword_ideas` — keyword planner suggestions
- [ ] Error case: call without Google linked → clear "not connected" message

---

## 7. MCP TOOLS — Google Analytics (GA4)

- [ ] `ga4_list_properties` — lists GA4 properties user has access to
- [ ] `ga4_realtime_report` — active users / current sessions
- [ ] `ga4_run_report` — sessions, users, events for date range
- [ ] `ga4_audience_report` — demographic breakdown
- [ ] `ga4_funnel_report` — multi-step funnel data
- [ ] Error case: no GA4 access → readable error, no 500

---

## 8. MCP TOOLS — Google Search Console (SEO)

- [ ] `gsc_search_analytics` — queries, clicks, impressions, CTR, position
- [ ] `gsc_index_coverage` — indexed / not indexed page breakdown
- [ ] `gsc_core_web_vitals` — CWV status per URL
- [ ] `gsc_mobile_usability` — mobile issues
- [ ] `gsc_links` — internal / external backlink data
- [ ] `gsc_sitemaps` — list and submit sitemaps
- [ ] Error case: site not verified in GSC → clear error

---

## 9. MCP TOOLS — Meta Ads

- [ ] `meta_ads_list_accounts` — ad accounts visible
- [ ] `meta_ads_campaign_performance` — campaign metrics
- [ ] `meta_ads_adset_performance` — ad set breakdown
- [ ] `meta_ads_ad_performance` — individual ad metrics
- [ ] `meta_ads_audience_insights` — audience data
- [ ] Error case: Meta not linked → prompt to connect at `/connect`

---

## 10. MCP TOOLS — Google Sheets

- [ ] `sheets_read_spreadsheet` — reads cells from a sheet
- [ ] `sheets_write_spreadsheet` — writes values (requires editor role)
- [ ] `sheets_create_spreadsheet` — creates new sheet in user's Drive
- [ ] `sheets_list_spreadsheets` — lists sheets accessible to user
- [ ] Error case: viewer role user tries to write → should now return clear error (role is editor by default)

---

## 11. OAUTH CONNECTIONS

### Google
- [ ] `/connect` → "Connect Google" button present
- [ ] OAuth flow completes, scopes granted (Ads + GA4 + GSC + Sheets)
- [ ] Re-connecting refreshes token without breaking existing sessions
- [ ] Revoking in Google account settings → next MCP call fails gracefully

### Meta
- [ ] "Connect Meta" button present on `/connect`
- [ ] OAuth flow completes
- [ ] Meta token stored and working for Meta Ads tools

---

## 12. DASHBOARD — UI / UX

- [ ] Dashboard loads instantly (no white screen flash)
- [ ] All em-dashes render as `—` (not `â€"` mojibake)
- [ ] Plan badge correct color (orange=trial, green=active, gray=canceled)
- [ ] Cancel subscription card only visible when there's an active subscription
- [ ] Usage counter shows correct call count
- [ ] "Sessions" section shows connected clients
- [ ] Mobile responsive at 375px width

---

## 13. EMAIL DELIVERY CHECKLIST

Check these email types all arrive and render correctly:

| Email type | Trigger | Check |
|---|---|---|
| `welcome` | New user signs up | [ ] Received, not spam |
| `onboarding` | Subscription activated | [ ] Received, correct plan name |
| `receipt` | First payment | [ ] Received, shows amount |
| `receipt_renewal` | Renewal webhook | [ ] Received |
| `team_invite` | Owner invites member | [ ] Received, accept link works |
| `trial_ending` | 3 days before trial ends | [ ] Scheduled via CRON |
| `drip_day2` | 2 days after signup, no purchase | [ ] Scheduled via CRON |
| `drip_day7` | 7 days after signup, no purchase | [ ] Scheduled via CRON |
| `win_back` | Subscription canceled | [ ] Received |

---

## 14. SEO / ANALYTICS

- [ ] GA4 tag `G-THYFH2QD4D` fires on all 8 pages (check Network tab → collect)
- [ ] Page titles are descriptive (not "Untitled")
- [ ] `<meta name="description">` present on `/`, `/pricing`, `/onboard`
- [ ] `/robots.txt` accessible or not blocking crawlers
- [ ] `/sitemap.xml` exists (if implemented)
- [ ] Core Web Vitals: LCP < 2.5s, CLS < 0.1, INP < 200ms (use PageSpeed Insights)
- [ ] No mixed-content (HTTP resources on HTTPS pages)

---

## 15. SECURITY

- [ ] Auth-required routes return 401 without valid JWT
- [ ] Team invite can only be sent by team owner, not members
- [ ] Admin routes (`/admin/*`) blocked for non-admin users
- [ ] CORS: only `mcp-ads.com` origins accepted in production
- [ ] No sensitive keys in HTML source or JS
- [ ] Rate limiting fires on rapid auth attempts (>20/min/IP)

---

## 16. CRON / SCHEDULED JOBS

- [ ] Cloud Scheduler job `process-emails-hourly` active in GCP console
- [ ] `POST /internal/process-emails` returns 200 with CRON_SECRET header
- [ ] Drip emails send on schedule (check email_log table day after test signup)
- [ ] Trial ending email fires ~3 days before expiry

---

## Known issues to retest after latest deploy

- [ ] Invite emails deliver (Resend whitespace fix — emails.py `_STYLE`/`_FOOTER`)
- [ ] Remove button works (was 404 — fixed URL + member_id key)
- [ ] Sheets tools accessible (was "Access denied" — role now defaults to editor)
- [ ] Owner appears in member list with "owner" badge
- [ ] Member seats badge shows correct count (owner excluded)
