# SEO & Marketing Tasks

---

## WHAT I NEED FROM YOU (cannot be done without your input)

### 1. Production domain name
**Action needed:** Tell me your real domain (e.g. `marketingmcp.com`).
Right now every canonical URL, sitemap, robots.txt and OG tag says `https://marketingmcp.com`. If your domain is different, I need to do a global find-and-replace.

---

### 2. OG / social share image (1200 × 630 px)
**Action needed:** Create a screenshot of your product (a nice-looking chat demo or dashboard) sized 1200×630px and save it as:
```
static/og-image.png
```
This image shows up when someone shares your link on LinkedIn, Slack, Twitter, or Meta Ads link previews. Without it, link previews are blank.

Tool to make it fast: use Figma, Canva, or a screenshot of your landing page cropped to 1200×630.

---

### 3. Google Search Console verification
**Action needed:** Go to https://search.google.com/search-console → Add property → choose "URL prefix" → enter your domain → choose "HTML tag" method → copy the meta tag it gives you (looks like `<meta name="google-site-verification" content="XXXX">`).

Then tell me the content value and I'll add it to `index.html` `<head>` and submit the sitemap for you in the code.

---

### 4. Meta Pixel ID
**Action needed:** Go to Meta Business Suite → Events Manager → create a new Pixel → copy the Pixel ID (a 15-digit number like `1234567890123456`).

Tell me the ID and I'll add the full Meta Pixel + standard events (`Lead` on sign-up, `Purchase` on plan purchased) to all pages.

---

### 5. Google Ads conversion tracking ID + label
**Action needed:** Once you create your Google Ads account → Goals → Conversions → create a conversion called "Plan Purchased" → copy the "Conversion ID" and "Conversion label" (looks like `AW-123456789/AbCdEfGhIjK`).

Tell me these and I'll add the Google Ads gtag conversion snippet to the billing success callback.

---

### 6. Privacy & Terms pages — your actual domain in the links
**Action needed:** Open `static/privacy.html` and `static/terms.html` and update any `href` or email addresses that still say `localhost` or a placeholder domain to your real domain.

---

### 7. Pricing page is currently a 301 redirect
**Action needed:** When you're ready to have `/pricing` as a real indexable page (improving SEO), tell me and I'll create a dedicated `pricing.html` served at that URL instead of the current redirect to `/#pricing`.

---

## WHAT WAS DONE (no action needed from you)

### ✅ 1. Meta tags & Open Graph — all pages
**Files changed:** `static/index.html`, `static/pricing.html`, `static/privacy.html`, `static/terms.html`

Added to every public page:
- Optimised `<title>` tag with primary keyword
- `<meta name="description">` with benefit-focused copy
- `<meta name="robots" content="index, follow">`
- `<link rel="canonical">` — prevents duplicate content penalties
- Open Graph tags (`og:title`, `og:description`, `og:image`, `og:url`, `og:type`)
- Twitter Card tags

App pages (`onboard.html`) got `noindex, nofollow` — Google won't waste crawl budget on them.

**How to test:**
1. Open https://developers.facebook.com/tools/debug/ → paste your URL → hit Debug. You should see your title, description and OG image.
2. Open https://www.opengraph.xyz/ → paste your URL → check preview.
3. In Chrome DevTools → Elements → search for `og:title` in `<head>`.

---

### ✅ 2. JSON-LD structured data schema — index.html
**File changed:** `static/index.html`

Added three schema blocks inside a single `<script type="application/ld+json">`:
- **Organization** — tells Google the company name, URL, logo
- **SoftwareApplication** — tells Google this is a $49/mo web app in the "Business" category
- **FAQPage** — 5 Q&A pairs that can appear as rich results (expandable FAQ cards) directly in Google search results

**How to test:**
1. Go to https://search.google.com/test/rich-results
2. Enter your live URL (or paste the HTML)
3. You should see "SoftwareApplication" and "FAQPage" detected with no errors

---

### ✅ 3. GA4 custom event tracking
**Files changed:** `static/index.html`, `static/pricing.html`, `static/onboard.html`

GA4 Measurement ID already existed: `G-THYFH2QD4D`

Events added:

| Event | Where it fires | File |
|---|---|---|
| `checkout_initiated` | When user clicks "Connect Google" to start the flow | index.html, pricing.html |
| `google_connect` | When Google OAuth returns successfully (`?google_ok=1`) | index.html, onboard.html |
| `meta_connect` | When Meta OAuth returns successfully (`?meta_ok=1`) | pricing.html, onboard.html |
| `mcp_credential_issued` | When credentials are displayed (user is fully active) | pricing.html, onboard.html |

**How to test:**
1. Go to https://analytics.google.com → your GA4 property → Reports → Realtime
2. Open your site in another tab and click "Connect with Google" (don't need to complete OAuth)
3. You should see `checkout_initiated` appear in Realtime within ~10 seconds
4. After completing an OAuth flow on a staging/local server, you should see `google_connect` fire

**Next step (do once):** In GA4 → Admin → Events → find `checkout_initiated`, `google_connect`, `meta_connect`, `mcp_credential_issued` → toggle "Mark as conversion" for `google_connect` and `mcp_credential_issued`.

---

### ✅ 4. robots.txt
**File created:** `static/robots.txt`
**Route added:** `GET /robots.txt` in `main.py`

Allows Google to crawl: `/`, `/pricing`, `/static/privacy.html`, `/static/terms.html`, `/blog/*`
Blocks crawling of: `/onboard`, `/manage`, `/connect`, `/setup`, `/api/`, `/auth/`, `/billing/`, `/user/`, `/admin/`, `/mcp`

**How to test:**
- Visit `https://yourdomain.com/robots.txt` — should return plain text
- Use https://www.google.com/webmasters/tools/robots-testing-tool to validate

---

### ✅ 5. sitemap.xml
**File created:** `static/sitemap.xml`
**Route added:** `GET /sitemap.xml` in `main.py`

Lists: `/`, `/pricing`, `/static/privacy.html`, `/static/terms.html`
Includes commented template for adding blog posts.

**How to test:**
- Visit `https://yourdomain.com/sitemap.xml` — should return valid XML
- Submit in Google Search Console → Sitemaps → enter `sitemap.xml` → Submit

**When you add blog posts:** Add a new `<url>` block to `static/sitemap.xml` for each post.

---

## NEXT STEPS IN PRIORITY ORDER

1. Get your domain confirmed → I update all URLs in code (5 min)
2. Create `static/og-image.png` → social previews work
3. Verify GSC → submit sitemap → Google starts indexing
4. Give me Meta Pixel ID → I add full pixel + events (30 min of code)
5. Give me Google Ads conversion IDs → I add conversion snippets
6. Mark GA4 events as conversions in the GA4 dashboard
7. Build blog infrastructure (tell me if you want `/blog` subfolder in FastAPI or a separate CMS)
8. Publish first 2 blog posts from the content calendar
9. Set up Google Ads campaigns using the strategy doc
10. Create Meta Pixel audiences and launch first Meta Ads campaign
