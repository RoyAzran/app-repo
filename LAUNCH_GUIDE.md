# mcp-ads.com — Full Launch Guide

---

## 1. Google Cloud Run — Deploy the App

### 1.1 Prerequisites
- Install [Google Cloud CLI](https://cloud.google.com/sdk/docs/install)
- Create a project at console.cloud.google.com (e.g. `mcp-ads`)

### 1.2 Authenticate & set project
```bash
gcloud auth login
gcloud config set project mcp-ads
```

### 1.3 Enable required Cloud APIs
```bash
gcloud services enable \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com
```

### 1.4 Store secrets in Secret Manager (never use env vars directly in prod)
```bash
# Run once per secret
gcloud secrets create JWT_SECRET_KEY --data-file=- <<< "your-jwt-secret"
gcloud secrets create GOOGLE_CLIENT_ID --data-file=- <<< "your-client-id"
gcloud secrets create GOOGLE_CLIENT_SECRET --data-file=- <<< "your-client-secret"
gcloud secrets create AIRWALLEX_CLIENT_ID --data-file=- <<< "your-airwallex-id"
gcloud secrets create AIRWALLEX_CLIENT_SECRET --data-file=- <<< "your-airwallex-secret"
gcloud secrets create META_APP_ID --data-file=- <<< "your-meta-app-id"
gcloud secrets create META_APP_SECRET --data-file=- <<< "your-meta-app-secret"
```

### 1.5 Build & deploy
```bash
# From project root (where Dockerfile is)
gcloud run deploy mcp-ads \
  --source . \
  --region us-central1 \
  --allow-unauthenticated \
  --set-secrets="JWT_SECRET_KEY=JWT_SECRET_KEY:latest,GOOGLE_CLIENT_ID=GOOGLE_CLIENT_ID:latest,GOOGLE_CLIENT_SECRET=GOOGLE_CLIENT_SECRET:latest,AIRWALLEX_CLIENT_ID=AIRWALLEX_CLIENT_ID:latest,AIRWALLEX_CLIENT_SECRET=AIRWALLEX_CLIENT_SECRET:latest,META_APP_ID=META_APP_ID:latest,META_APP_SECRET=META_APP_SECRET:latest" \
  --memory 512Mi \
  --cpu 1 \
  --min-instances 1 \
  --max-instances 10
```

### 1.6 Database — SQLite → Cloud SQL (for production)
SQLite does not persist across Cloud Run deploys. Switch to PostgreSQL:
1. Create a Cloud SQL PostgreSQL instance in the console
2. Add `DATABASE_URL` secret: `postgresql://user:pass@/dbname?host=/cloudsql/PROJECT:REGION:INSTANCE`
3. Update `database.py` — replace SQLite engine with:
```python
DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///./agency_mcp.db")
engine = create_engine(DATABASE_URL)
```
4. Add `--add-cloudsql-instances PROJECT:REGION:INSTANCE` to the deploy command

---

## 2. Connect Domain mcp-ads.com

### 2.1 Buy the domain
- Recommended registrars: Cloudflare Registrar (cheapest), Namecheap, Google Domains (now Squarespace)

### 2.2 Map domain to Cloud Run
1. Google Cloud Console → Cloud Run → `mcp-ads` service → **Custom Domains** → **Add mapping**
2. Add `mcp-ads.com` and `www.mcp-ads.com`
3. Cloud Run gives you DNS records (A/CNAME) to add at your registrar
4. Add them at your registrar's DNS panel — propagation takes 5–30 min

### 2.3 SSL
Cloud Run provisions a free managed TLS certificate automatically — nothing to do.

### 2.4 Update OAuth redirect URIs
In **Google Cloud Console → APIs → Credentials → OAuth 2.0 Client**:
- Add authorized redirect URI: `https://mcp-ads.com/auth/google/callback`
- Add authorized JavaScript origin: `https://mcp-ads.com`

In **Meta Developer App** (see section 4):
- Add OAuth redirect URI: `https://mcp-ads.com/auth/meta/callback`

---

## 3. Google Search Console — Verify mcp-ads.com

1. Go to [search.google.com/search-console](https://search.google.com/search-console)
2. Add property → **Domain** → enter `mcp-ads.com`
3. Verify via DNS TXT record (add it at your registrar)
4. Once verified, traffic data starts flowing within 48 hours

> This is your own site's GSC. Customers connect *their* sites via OAuth when they use the app.

---

## 4. Meta / Facebook Ads — App Setup

### 4.1 Create Meta Developer App
1. Go to [developers.facebook.com](https://developers.facebook.com) → **My Apps → Create App**
2. Type: **Business** → App name: `MarketingMCP` or `mcp-ads`
3. Add product: **Marketing API**

### 4.2 Permissions to request
In App Review → Permissions:
```
ads_read
ads_management
business_management
pages_read_engagement   (optional, for page insights)
```

### 4.3 OAuth redirect
- App Settings → Basic → **Valid OAuth Redirect URIs**:
  `https://mcp-ads.com/auth/meta/callback`

### 4.4 Environment variables to add
```
META_APP_ID=your_app_id
META_APP_SECRET=your_app_secret
```

### 4.5 Go live
- App must be in **Live** mode (not Development) for real users to connect
- Submit each permission for App Review with use-case description

---

## 5. GA4 — Customers Connect Their Own

Customers connect their own GA4 via the Google OAuth flow already built.
The `analytics.readonly` scope gives access to all their GA4 properties.

For **your own** mcp-ads.com analytics:
1. Create a GA4 property at [analytics.google.com](https://analytics.google.com)
2. Add the measurement ID to index.html (for tracking signups/conversions):
```html
<!-- In <head> of index.html -->
<script async src="https://www.googletagmanager.com/gtag/js?id=G-XXXXXXXXXX"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){dataLayer.push(arguments);}
  gtag('js', new Date());
  gtag('config', 'G-XXXXXXXXXX');
</script>
```
3. Set up a **conversion event** for `sign_up` — fire it after Google OAuth connects

---

## 6. Run Ads — Get Customers

### 6.1 Google Ads
- Campaign type: **Search**
- Keywords: `mcp google ads tool`, `ai marketing automation`, `claude google ads`, `ai ppc tool`
- Landing page: `mcp-ads.com` → hero section
- Bid strategy: Maximize Conversions
- Budget: Start at $20–30/day

### 6.2 Meta Ads
- Campaign objective: **Lead Generation** or **Website Conversions**
- Audiences: Job titles → Digital Marketing Manager, Media Buyer, PPC Specialist, Paid Social Manager
- Ad copy angle: *"Your AI can now control your Google Ads campaigns"*
- Retarget: visitors of mcp-ads.com who didn't sign up

### 6.3 Conversion tracking setup
- Google Ads: create a conversion action for `mcp-ads.com/manage` page load (post-payment)
- Meta: install Meta Pixel on index.html, fire `Lead` event on Google OAuth connect, `Purchase` event on /manage load

---

## 7. Email Marketing

### 7.1 Recommended: Resend (simple, developer-friendly)
```bash
pip install resend
```
Add to `.env`:
```
RESEND_API_KEY=re_xxxxxxxxxxxx
```

### 7.2 Transactional emails to build (in main.py)

| Trigger | Email |
|---|---|
| After Google OAuth connect | "Welcome — complete your subscription" |
| After payment success (/manage load) | "You're live — here's your MCP token" |
| Day 3 after signup, not yet paid | "Still thinking it over?" nudge |
| Subscription cancelled | "Sorry to see you go — here's what you're losing" |
| Monthly | Usage summary — N tools called this month |

### 7.3 Example — welcome email trigger in oauth_google.py
```python
import resend
resend.api_key = os.environ.get("RESEND_API_KEY", "")

# After user created, send welcome email
resend.Emails.send({
    "from": "hello@mcp-ads.com",
    "to": email,
    "subject": "Your MarketingMCP account is ready",
    "html": f"""
        <p>Hi {name},</p>
        <p>Your Google account is connected. One step left — subscribe to activate your MCP token.</p>
        <p><a href='https://mcp-ads.com/#pricing'>Complete setup →</a></p>
    """
})
```

### 7.4 Newsletter / drip (optional)
- Use **Loops.so** or **Brevo** for drip sequences
- Capture email from the hero input (`pending_email` in localStorage) → POST to your backend → store it
- Sequence: Day 0 welcome → Day 2 "what can it do" → Day 5 customer story → Day 10 discount

---

## 8. Additional Revenue & Growth

### 8.1 Affiliate program
- Give each user a `?ref=USERID` link
- Track signups from that link in the DB
- Pay 20–30% recurring commission via Stripe/Airwallex payouts

### 8.2 Agency tier (future)
- $149/month plan: multiple client accounts, whitelabel, team members
- Add `plan` field to User model, gate tools accordingly (already partially done via `permissions.py`)

### 8.3 ChatGPT / OpenAI plugin
- Register mcp-ads.com as an MCP-compatible server
- List on [glama.ai](https://glama.ai) and [mcp.so](https://mcp.so) — free directories that drive signups

### 8.4 ProductHunt launch
- Schedule a launch on ProductHunt
- Prep: 10 upvote commitments from friends/colleagues before launch day
- Best day: Tuesday or Wednesday, post at 12:01am PST

---

## 9. Launch Checklist

- [ ] Cloud Run deployed, health check passes at `/health`
- [ ] `mcp-ads.com` DNS mapped, SSL green
- [ ] Google OAuth redirect URI updated to `https://mcp-ads.com/auth/google/callback`
- [ ] Meta OAuth redirect URI updated
- [ ] All secrets in Secret Manager (no plaintext in code)
- [ ] Cloud SQL connected (not SQLite)
- [ ] GA4 tracking on index.html
- [ ] Conversion events firing for signup + payment
- [ ] Resend transactional emails sending
- [ ] GSC property verified for mcp-ads.com
- [ ] Google OAuth app submitted for verification (or test users added)
- [ ] Meta App in Live mode, permissions approved
- [ ] Google Ads campaign live
- [ ] Meta Ads campaign live
- [ ] Listed on glama.ai and mcp.so

---

## 10. Key URLs Reference

| Service | URL |
|---|---|
| Cloud Console | console.cloud.google.com |
| Cloud Run service | console.cloud.google.com/run |
| Secret Manager | console.cloud.google.com/security/secret-manager |
| Google OAuth Credentials | console.cloud.google.com/apis/credentials |
| Meta Developer | developers.facebook.com/apps |
| Airwallex Dashboard | airwallex.com/app |
| Resend | resend.com |
| GSC | search.google.com/search-console |
| GA4 | analytics.google.com |
| ProductHunt | producthunt.com/posts/new |
| glama.ai listing | glama.ai/mcp/submit |
| mcp.so listing | mcp.so |
