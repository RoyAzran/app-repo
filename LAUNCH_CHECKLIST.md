# Launch Checklist — when mcp-ads.com is live

## Before you start: confirm the deployment is healthy
```
curl https://mcp-ads-123643736745.us-central1.run.app/health
# Expected: {"status":"ok"}
```

---

## 1. Map the domain to Cloud Run

1. Go to https://console.cloud.google.com/run
2. Click **mcp-ads** service → **Custom Domains** tab → **Add mapping**
3. Enter `mcp-ads.com` and also `www.mcp-ads.com`
4. Copy the DNS records that Google shows you (A / CNAME records)
5. Go to your domain registrar (Namecheap / GoDaddy / etc.) and add those DNS records
6. Wait for DNS to propagate (usually 5–30 min, up to 24h)
7. Google will auto-provision an SSL certificate — wait until the domain shows **Active** in the console

---

## 2. Update BASE_URL + SERVER_BASE_URL secrets

```powershell
$d = "https://mcp-ads.com"
[System.IO.File]::WriteAllText("$env:TEMP\u.txt", $d)
gcloud secrets versions add BASE_URL --project marketingmcp-493308 --data-file="$env:TEMP\u.txt"
gcloud secrets versions add SERVER_BASE_URL --project marketingmcp-493308 --data-file="$env:TEMP\u.txt"
Remove-Item "$env:TEMP\u.txt"
```

Then deploy to pick up the new values:
```powershell
gcloud run services update mcp-ads --region us-central1 --project marketingmcp-493308 --update-secrets "BASE_URL=BASE_URL:latest,SERVER_BASE_URL=SERVER_BASE_URL:latest"
```

---

## 3. Update Google OAuth redirect URIs

1. Go to https://console.cloud.google.com/apis/credentials
2. Find your **OAuth 2.0 Client ID**
3. Under **Authorized redirect URIs** add:
   - `https://mcp-ads.com/auth/google/callback`
   - `https://www.mcp-ads.com/auth/google/callback`
4. Remove `http://localhost:8000/auth/google/callback` (optional, keep for local dev)
5. Save

---

## 4. Update Airwallex webhook + redirect URLs

In your Airwallex dashboard:
- **Webhook URL**: set to `https://mcp-ads.com/webhooks/airwallex`
- **Success redirect URL**: set to `https://mcp-ads.com/billing/success?user_id={user_id}`
- **Cancel redirect URL**: set to `https://mcp-ads.com/billing/cancel?user_id={user_id}`

---

## 5. Set up Resend email

1. Sign up at https://resend.com
2. Add domain **mcp-ads.com** → copy the DNS records → add to your registrar
3. Wait for domain to verify in Resend
4. Create an API key in Resend → copy it
5. Push to Secret Manager:
```powershell
$k = "re_YOUR_REAL_KEY_HERE"
[System.IO.File]::WriteAllText("$env:TEMP\r.txt", $k)
gcloud secrets versions add RESEND_API_KEY --project marketingmcp-493308 --data-file="$env:TEMP\r.txt"
Remove-Item "$env:TEMP\r.txt"
```
6. Update the running service (no full rebuild needed):
```powershell
gcloud run services update mcp-ads --region us-central1 --project marketingmcp-493308 --update-secrets "RESEND_API_KEY=RESEND_API_KEY:latest"
```
7. Update `EMAIL_FROM` in emails.py if needed (currently set to `MarketingMCP <hello@mcp-ads.com>`)

---

## 6. Set up the drip email cron job (Cloud Scheduler)

1. Go to https://console.cloud.google.com/cloudscheduler
2. Click **Create Job**
3. Settings:
   - **Name**: `drip-emails`
   - **Region**: `us-central1`
   - **Schedule**: `0 * * * *` (every hour)
   - **Target**: HTTP
   - **URL**: `https://mcp-ads.com/internal/process-emails`
   - **HTTP Method**: POST
   - **Headers**: `Authorization: Bearer pFX2ogl6ABS8qT3MA08igyaAsm0m184`
     *(or check latest CRON_SECRET: `gcloud secrets versions access latest --secret=CRON_SECRET --project marketingmcp-493308`)*
4. Save + **Force run** once to test

---

## 7. Test the full signup flow end-to-end

1. Visit `https://mcp-ads.com`
2. Click **Get Started** → Google sign-in
3. After redirect, confirm welcome email arrives
4. Complete payment flow
5. Confirm receipt email arrives
6. Open Claude.ai → Settings → Integrations → Add MCP server → paste your token + endpoint
7. Test a tool call works

---

## 8. Update local .env for development

```env
BASE_URL=https://mcp-ads.com
SERVER_BASE_URL=https://mcp-ads.com
DATABASE_URL=postgresql://postgres:NEzgv8ZPBdRwisvi@db.dsluyejlzygnumssizvi.supabase.co:5432/postgres
```

---

## Optional / later

- [ ] Add `www` → apex redirect (can be done in Cloud Run domain mappings)
- [ ] Set up Google Cloud Monitoring alerts for container crashes
- [ ] Set up Airwallex test → live mode switch
- [ ] Add affiliate dashboard page to the frontend (currently API-only)
- [ ] Add `GET /user/me` display in the manage page showing Claude key status
