# Meridian CFO Services — Launch SEO Audit

**Spec:** SPEC-MERIDIAN-SEO-001  
**Production URL:** https://mcfoservices.com  
**Audit date:** 2026-09-22  
**Auditor:** Automated production crawl + Lighthouse (mobile + desktop)  
**Scope:** Launch hardening audit — no redesign, no copy rewrite beyond targeted SEO clarity recommendations

---

## Executive Summary

| Criterion | Status |
|---|---|
| **Indexable** | ✅ Yes — production site is crawlable; no `noindex` on homepage |
| **Technically healthy** | ⚠️ Mostly — canonical/redirects/sitemap/robots are correct; several fixable defects remain |
| **Launch-ready for organic search** | ⚠️ **Conditional** — safe to index, but **not ready for SEO handoff** until top blockers/high items are resolved |

The production Framer site at `https://mcfoservices.com/` is a single-page application with correct apex canonicalization, valid robots.txt, and a clean sitemap. No placeholder Framer metadata (`My Framer Site`, `Made with Framer`) remains. Heading architecture is largely sound: section labels (`THE PROBLEM`, `NAVORA · IN DEVELOPMENT`) correctly use `<p>`, not promoted headings.

**Top 5 highest-impact issues:**

1. **GoDaddy legacy site still live and indexable** at `https://mcfoservices.godaddysites.com/` — separate sitemap, different H1/copy, duplicate-content risk despite a cross-domain canonical.
2. **Three `<h1>` elements in SSR HTML** (responsive breakpoint duplicates) — search engines may see multiple primary headings.
3. **Malformed Calendly URL** — one CTA link concatenates the booking URL twice (`?month=2026-09https://calendly.com/...`).
4. **No JSON-LD structured data** — missed opportunity for Organization/ProfessionalService rich results.
5. **Mobile LCP 9.2 s** — driven by large hero/product imagery (architecture JPG ~851 KB, Navora PNGs 309–650 KB); desktop LCP is 1.3 s.

**Must fix before SEO handoff:** Issues #1–#4 above, plus missing alt text on meaningful images and contact form placeholder values.

**Future opportunity:** Dedicated landing pages (`/financial-health-check`, `/fractional-cfo-services`, etc.), copy additions for underrepresented search-intent terms, OG image compression.

---

## Phase 1 — Crawl and Inventory

### URL Inventory

| URL | Status | Final URL | Canonical | Indexable | Title | Meta Description | H1 | Issues |
|---|---|---|---|---|---|---|---|---|
| `https://mcfoservices.com/` | 200 | (same) | `https://mcfoservices.com/` | ✅ Yes | Meridian CFO Services \| Financial Clarity for Business Owners | See metadata section | See Your Business Like a CFO. (×3 in HTML) | Multiple H1; missing JSON-LD; large images |
| `https://www.mcfoservices.com/` | 308 → 200 | `https://mcfoservices.com/` | (inherits) | ✅ Yes | (same) | (same) | (same) | Correct www → apex redirect |
| `http://mcfoservices.com/` | 308 → 200 | `https://mcfoservices.com/` | (inherits) | ✅ Yes | (same) | (same) | (same) | Correct HTTP → HTTPS |
| `http://www.mcfoservices.com/` | 308 → 308 → 200 | `https://mcfoservices.com/` | (inherits) | ✅ Yes | (same) | (same) | (same) | 2-hop chain (acceptable) |
| `https://mcfoservices.com/about` | 404 | — | — | N/A | — | — | — | Expected (SPA anchors only) |
| `https://mcfoservices.com/services` | 404 | — | — | N/A | — | — | — | Expected |
| `https://mcfoservices.com/contact` | 404 | — | — | N/A | — | — | — | Expected |
| `https://mcfoservices.com/navora` | 404 | — | — | N/A | — | — | — | Expected |
| `https://mcfoservices.com/financial-health-check` | 404 | — | — | N/A | — | — | — | Expected |
| `https://mcfoservices.com/privacy` | 404 | — | — | N/A | — | — | — | Missing — recommend future page |
| `https://mcfoservices.com/terms` | 404 | — | — | N/A | — | — | — | Missing — recommend future page |
| `https://mcfoservices.com/robots.txt` | 200 | (same) | — | N/A | — | — | — | ✅ Correct |
| `https://mcfoservices.com/sitemap.xml` | 200 | (same) | — | N/A | — | — | — | ✅ Single URL only |
| `https://mcfoservices.godaddysites.com/` | 200 | (same) | `https://mcfoservices.com/` | ⚠️ Yes (risk) | Meridian CFO Services | Explore top CFO services for expert financial guidance… | Your Satisfaction, Our Mission. (×2) | **Legacy site live** — own sitemap, different content |
| `https://mcfoservices.framer.website/` | 404 | — | — | N/A | — | — | — | ✅ Not accessible |
| `https://mcfoservices.framer.app/` | 404 | — | — | N/A | — | — | — | ✅ Not accessible |

### Redirect Behavior

| Check | Result |
|---|---|
| HTTP → HTTPS | ✅ `http://mcfoservices.com/` → 308 → `https://mcfoservices.com/` |
| www → apex | ✅ `https://www.mcfoservices.com/` → 308 → `https://mcfoservices.com/` |
| Redirect chains | ⚠️ `http://www.mcfoservices.com/` → `https://www.mcfoservices.com/` → `https://mcfoservices.com/` (2 hops — acceptable) |
| Redirect loops | ✅ None detected |
| Duplicate homepage URLs | ✅ Only `/` serves content; `/home`, `/index.html` return 404 |
| Framer staging/preview indexable | ✅ No active preview URLs found for this project |
| GoDaddy pages remain accessible | ❌ **`mcfoservices.godaddysites.com` returns 200** with its own sitemap at `/sitemap.website.xml` |
| Placeholder/generated Framer pages | ✅ None found on production domain |

### Internal / External Link Audit

| Link | Status | Notes |
|---|---|---|
| `./#services` | ✅ | Section ID `services` exists |
| `./#financial-health-check` | ✅ | Section ID `financial-health-check` exists |
| `./#navora` | ✅ | Section ID `navora` exists |
| `./#about` | ✅ | Section ID `about` exists |
| `./#contact` | ✅ | Section ID `contact` exists |
| `https://calendly.com/william-mcfoservices/20min` | ✅ 200 | 3 of 4 instances correct |
| `https://calendly.com/william-mcfoservices/20min?month=2026-09https://calendly.com/...` | ⚠️ 200 (Calendly tolerates) | **Malformed — duplicated URL** |
| `#` placeholder links | ✅ None found |
| Links to Framer preview URLs | ✅ None |
| Links to GoDaddy placeholder | ✅ None on production site |

---

## Phase 2 — Indexability / Technical SEO

### robots.txt

```
User-agent: *
Allow: /

Sitemap: https://mcfoservices.com/sitemap.xml
```

| Check | Result |
|---|---|
| Production crawlable | ✅ |
| Important pages disallowed | ✅ None disallowed |
| Framer blocking indexing | ✅ No — no `Disallow` or `noindex` |

### XML Sitemap

```xml
<?xml version="1.0" encoding="UTF-8"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://mcfoservices.com/</loc></url>
</urlset>
```

| Check | Result |
|---|---|
| Sitemap exists | ✅ |
| Homepage included | ✅ |
| Canonical domain URLs | ✅ |
| Staging/preview URLs | ✅ None |
| GoDaddy URLs | ✅ None |

### Canonical Tags

| Page | Canonical | Correct? |
|---|---|---|
| Homepage | `https://mcfoservices.com/` | ✅ |

GoDaddy legacy site sets `<link rel="canonical" href="https://mcfoservices.com/">` — helpful but does not remove the duplicate from Google's index by itself.

### Indexability Signals

| Signal | Homepage |
|---|---|
| `<meta name="robots">` | `max-image-preview:large` (allows indexing) |
| X-Robots-Tag header | None |
| noindex / nofollow | ✅ Not present |
| Duplicate pages | ⚠️ GoDaddy legacy site |
| Malformed canonical | ✅ None |

---

## Phase 3 — Metadata Audit

### Homepage Metadata (verified from rendered HTML)

| Field | Current Value | Assessment |
|---|---|---|
| **Title** | `Meridian CFO Services \| Financial Clarity for Business Owners` (61 chars) | ⚠️ Differs from spec target; length acceptable |
| **Meta description** | `Meridian CFO Services helps business owners turn QuickBooks Online data into clearer financial decisions through CFO insight, financial health reviews, and the upcoming Navora Financial Command Center.` (201 chars) | ⚠️ Slightly long (may truncate ~155–160); "upcoming" is accurate for In Development |
| **Canonical** | `https://mcfoservices.com/` | ✅ |
| **OG title** | Same as title | ✅ |
| **OG description** | Same as meta description | ✅ |
| **OG image** | `https://framerusercontent.com/assets/eVZBqk1O8PUAm5BPb2GyqUi0.png` | ⚠️ **1,661 KB** — oversized for social previews |
| **OG url** | `https://mcfoservices.com/` | ✅ |
| **OG type** | `website` | ✅ |
| **Twitter card** | `summary_large_image` | ✅ |
| **Twitter title/description/image** | Mirrors OG | ✅ |
| **Favicon (light)** | `https://framerusercontent.com/images/I3eOtHZuU6USiQNlFx29DF7t54.png` | ✅ |
| **Favicon (dark)** | `https://framerusercontent.com/images/Uk1thWhgmiYAVT3uvVDhjzNekC0.png` | ✅ |
| **Apple touch icon** | `https://framerusercontent.com/images/0hlXpf6RuDUImsJDNappjJhjmk.png` | ✅ |

### Placeholder Metadata Check

| Placeholder | Found? |
|---|---|
| My Framer Site | ✅ Not found |
| Made with Framer | ✅ Not found (generator meta shows `Framer 6ae18df` — normal, not user-facing) |
| Generic template descriptions | ✅ Not found |

### Title Recommendation

**Spec target:** `Meridian CFO Services | Financial Clarity for Growing Businesses` (64 chars)

**Current:** `…for Business Owners` (61 chars)

Both are clear and within acceptable length. "Growing Businesses" aligns slightly better with SMB/midsize positioning and the site's "scale" language. Not a blocker — operator choice.

### Meta Description Recommendation

**Current (201 chars):**
> Meridian CFO Services helps business owners turn QuickBooks Online data into clearer financial decisions through CFO insight, financial health reviews, and the upcoming Navora Financial Command Center.

**Suggested trim (192 chars) — only if truncation in SERPs is observed:**
> Meridian CFO Services helps business owners turn QuickBooks Online data into clearer financial decisions through CFO insight, financial health reviews, and the Navora Financial Command Center.

**Keep "upcoming"** — Navora is In Development; removing it would misrepresent availability.

---

## Phase 4 — Heading Architecture

Framer SSR renders responsive breakpoint variants server-side, producing duplicate heading nodes (only one visible per viewport).

### H1

| # | Current Tag | Text | Visible Breakpoint | Recommended |
|---|---|---|---|---|
| 1 | `<h1>` | See Your Business Like a CFO. | Desktop (hidden-ifsoa5 variant) | `<h1>` — keep, ensure only one variant uses H1 |
| 2 | `<h1>` | See Your Business Like a CFO. | Tablet (hidden-1kwuf8k variant) | Change to `<p>` or hide from SSR |
| 3 | `<h1>` | See Your Business Like a CFO. | Mobile (hidden-7lp6c variant) | Change to `<p>` or hide from SSR |

**Note:** Visible text concatenates without spaces in SSR (`See YourBusinessLike a CFO.`) due to line-break styling — cosmetic in browser, but raw HTML extraction shows the spacing issue.

### H2 (deduplicated — correct semantics)

| Current Tag | Text | Recommended |
|---|---|---|
| `<h2>` | Your data tells you what happened. Intelligence tells you what matters. | `<h2>` ✅ |
| `<h2>` | A clear assessment for what's next. | `<h2>` ✅ |
| `<h2>` | Know what needs your attention before you start your day. | `<h2>` ✅ |
| `<h2>` | Built on 35 years of financial and operating experience. | `<h2>` ✅ |

### Section Labels (correctly NOT promoted)

| Current Tag | Text | Recommended |
|---|---|---|
| `<p>` | THE PROBLEM | `<p>` ✅ — decorative label |
| `<p>` | THE FINANCIAL HEALTH CHECK | `<p>` ✅ |
| `<p>` | NAVORA · IN DEVELOPMENT | `<p>` ✅ |
| `<p>` | WILLIAM MOYLAN · FOUNDER | `<p>` ✅ |

### H3 Feature Subheadings (correct)

| Text | Tag | Recommended |
|---|---|---|
| Cash Flow | `<h3>` | `<h3>` ✅ |
| Profitability | `<h3>` | `<h3>` ✅ |
| Forecasting | `<h3>` | `<h3>` ✅ |
| Growth Readiness | `<h3>` | `<h3>` ✅ |
| Decision-Making | `<h3>` | `<h3>` ✅ |

---

## Phase 5 — Search Intent / Content Audit

### Term Coverage (rendered page text)

| Term / Concept | Present? | Count | Notes |
|---|---|---|---|
| CFO services | ✅ | 7 | Strong |
| CFO consulting | ❌ | 0 | Gap — natural addition opportunity |
| fractional CFO | ❌ | 0 | Gap — factually appropriate for advisory positioning |
| financial health review | ❌ | 0 | "Financial Health Check" used instead (11×) |
| financial health check | ✅ | 11 | Strong |
| QuickBooks Online | ✅ | 5 | Strong |
| cash flow | ✅ | 4 | Present; "cash flow analysis" not explicit |
| profitability | ✅ | 5 | Present |
| forecasting | ✅ | 6 | Strong |
| financial dashboard / command center | ✅ | 1+ | Via Navora section |
| financial intelligence | ✅ | 5 | Strong |
| financial reporting | ❌ | 0 | Minor gap |
| small business / growing business | ❌ | 0 | Title says "Business Owners" |
| CFO advisory | ❌ | 0 | Gap |
| Navora Financial Command Center | ✅ | In copy + meta | Visible in Navora section |
| William Moylan | ✅ | 1 | Present in About |
| Accredible / credential | ❌ | 0 | Certificate image present but no text reference |

### Recommended Copy Additions

These preserve brand voice and avoid keyword stuffing.

---

**Recommendation 1 — Hero subcopy (Services intro)**

CURRENT:
> We turn your QuickBooks data into clear priorities, risks and opportunities—so you can make better decisions without spending hours in the reports.

PROPOSED:
> We turn your QuickBooks Online data into clear priorities, risks, and opportunities—so you can make better decisions without spending hours in the reports. Meridian provides CFO advisory and consulting for growing businesses.

WHY:
Adds "QuickBooks Online" (exact product term), "CFO advisory," "CFO consulting," and "growing businesses" in one natural sentence. Supports fractional-CFO search intent without claiming a dedicated fractional-CFO product line.

---

**Recommendation 2 — Financial Health Check CTA area**

CURRENT:
> Schedule a Complimentary Review

PROPOSED:
> Schedule a Complimentary Financial Health Review

WHY:
Introduces "financial health review" (matches Calendly event naming and spec language) alongside existing "Financial Health Check" terminology.

---

**Recommendation 3 — About / founder section (one sentence addition)**

CURRENT:
> Meridian combines real-world CFO judgment with modern financial intelligence to help business owners make clearer, more confident decisions.

PROPOSED:
> Meridian combines real-world CFO judgment with modern financial intelligence to help business owners make clearer, more confident decisions through fractional CFO services, cash flow analysis, profitability review, and forecasting.

WHY:
Covers four high-value search concepts in a services-accurate list. "Fractional CFO services" is appropriate for a solo-practitioner advisory firm without implying a large team.

---

## Phase 6 — Image SEO and Accessibility

### Image Inventory

| Image | Role | Current Alt | Recommended Alt | File Size | Intrinsic | Format | Lazy Load |
|---|---|---|---|---|---|---|---|
| `YfAapS99srShc0j31cFA9yvPj4.png` | Laptop hero / product render | **MISSING** | `Meridian CFO Services dashboard on laptop showing QuickBooks Online financial analysis` | 245 KB | 1536×1024 | PNG | ❌ Not set |
| `9Qz0GskrYdee0lyYASRc158Ww.png` | Navora hero visual | **MISSING** | `Navora Financial Command Center dashboard for QuickBooks Online` | 371 KB | 1744×902 | PNG | ❌ |
| `DfFhEew7Qbamq3rzyqIVQkiLhUA.png` | Navora dashboard detail | `Navora Financial Command Center dashboard` | `Navora Financial Command Center dashboard for QuickBooks Online` (minor refinement) | 309 KB | 1672×941 | PNG | ❌ |
| `VT4byfD1vXd8BIdIF8AwHJsEpU.png` | Navora KPI/risk view | **MISSING** | `Navora KPI scorecard and risk intelligence detail` | 650 KB | 1672×941 | PNG | ❌ |
| `f5BDga0jY7M68IrBNXVGbOFynXE.jpg` | Architectural / editorial | `Sculptural curved staircase used as editorial brand imagery` | `alt=""` (decorative) — current alt is acceptable but unnecessarily verbose | 851 KB | 1330×1173 | JPEG | ❌ |
| `Dz30FvqzYhincIaGX6wusarYg.png` | Accredible certificate | **MISSING** | `William Moylan Accredible certificate in Strategic Finance` | 26 KB | 600×464 | PNG | ❌ |
| `5ILRvlYXf72kHSVHqpa3snGzjU.jpg` | Executive office (background) | N/A (CSS background) | Decorative — no alt possible; ensure surrounding text names William Moylan | 44 KB | — | JPEG | N/A |
| `eVZBqk1O8PUAm5BPb2GyqUi0.png` | OG/social share image | N/A (meta tag) | Compress to <300 KB; no alt needed | **1,661 KB** | — | PNG | N/A |

### Summary

- **4 unique image assets** lack alt text on at least one breakpoint instance.
- **0 of 21** `<img>` tags use `loading="lazy"` — all images load eagerly.
- **Decorative:** architectural staircase → prefer `alt=""`; executive office background → decorative.
- **Informational:** Navora dashboards, laptop hero, Accredible certificate → require descriptive alt.

---

## Phase 7 — Structured Data

### Current State

**No JSON-LD present** on the homepage. Lighthouse SEO score is 100 for basic checks but reports structured data as not evaluated.

### Recommended Implementation

**Type:** `ProfessionalService` (more specific than `Organization` for CFO advisory)

**Do NOT include:** address, telephone, service area, founding date, reviews, ratings, or Navora as a `SoftwareApplication` (product is In Development).

```json
{
  "@context": "https://schema.org",
  "@type": "ProfessionalService",
  "name": "Meridian CFO Services",
  "url": "https://mcfoservices.com/",
  "logo": "https://framerusercontent.com/images/I3eOtHZuU6USiQNlFx29DF7t54.png",
  "description": "Meridian CFO Services helps business owners turn QuickBooks Online data into clearer financial decisions through CFO insight, financial health reviews, and financial intelligence.",
  "founder": {
    "@type": "Person",
    "name": "William Moylan"
  },
  "sameAs": []
}
```

**Framer implementation path:** Site Settings → Custom Code → End of `<head>` tag → paste as:

```html
<script type="application/ld+json">
{ ... JSON above ... }
</script>
```

Add verified social/profile URLs to `sameAs` when available (LinkedIn, etc.). Do not fabricate.

---

## Phase 8 — Performance / Core Web Vitals

### Lighthouse Results (production homepage)

| Metric | Mobile | Desktop |
|---|---|---|
| **Performance** | 74 | 96 |
| **Accessibility** | 91 | — |
| **Best Practices** | 100 | — |
| **SEO** | 100 | — |
| **FCP** | 1.0 s | 0.8 s |
| **LCP** | **9.2 s** | 1.3 s |
| **TBT** | 130 ms | 0 ms |
| **CLS** | 0.003 | 0.003 |
| **TTI** | 9.3 s | — |
| **Speed Index** | 1.0 s | — |

### Top Opportunities (mobile)

| Opportunity | Est. Savings |
|---|---|
| Properly size images | 351 KiB |
| Defer offscreen images | 292 KiB |
| Reduce unused JavaScript | 107 KiB |

### High-Impact Recommendations (preserve premium quality)

1. **Compress OG image** (`eVZBqk1O8PUAm5BPb2GyqUi0.png`) from 1,661 KB → target <300 KB (WebP or optimized PNG). Does not affect on-page LCP but improves social preview load and share experience.
2. **Architecture JPG** (851 KB) — re-export at 80–85% quality or convert to WebP; likely LCP candidate on mobile.
3. **Navora dashboard PNGs** (309–650 KB) — serve responsive sizes; enable Framer's built-in `scale-down-to` variants (partially present in markup but full-size still downloaded on some breakpoints).
4. **Enable lazy loading** on below-fold images (Navora section, About, Contact).
5. **Calendly** — no embedded widget detected (link-out only); minimal third-party impact. ✅
6. **Framer analytics script** (`events.framer.com`) — small; acceptable.
7. **No font preload** detected — monitor if FCP regresses; not a current issue.

---

## Phase 9 — Internal Navigation / Conversion SEO

### Navigation Anchors

| Nav Item | Href | Target Exists | Status |
|---|---|---|---|
| Services | `./#services` | `#services` | ✅ |
| Financial Health Check | `./#financial-health-check` | `#financial-health-check` | ✅ |
| Navora | `./#navora` | `#navora` | ✅ |
| About | `./#about` | `#about` | ✅ |
| Contact | `./#contact` | `#contact` | ✅ |

### CTA / Calendly

| CTA | Destination | Status |
|---|---|---|
| Primary booking CTAs | `https://calendly.com/william-mcfoservices/20min` | ✅ Valid (200) |
| One embedded/month link | Malformed duplicate URL | ❌ **Fix required** |
| `target="_blank"` | Set on Calendly links | ✅ |
| `rel="noopener noreferrer"` | **Missing** on all Calendly links | ⚠️ Add for security |

### Contact Form

| Check | Status |
|---|---|
| Form present | ✅ Framer native form |
| Action endpoint | Framer-managed (no custom action URL) |
| Placeholder: Name | `Jane Smith` — ⚠️ replace with neutral placeholder |
| Placeholder: Email | `jane@mcfoservices.com` — ⚠️ replace; only email in entire page HTML |
| Submit button | "Send Message" ✅ |
| Visible business email | ❌ None displayed (only placeholder) |

---

## Phase 10 — Navora Search Positioning

| Requirement | Status |
|---|---|
| Identified as Navora Financial Command Center | ✅ In section copy and meta description |
| QuickBooks Online relationship | ✅ "turns QuickBooks Online data into a clear executive view" |
| In Development language preserved | ✅ "NAVORA · IN DEVELOPMENT" label; "upcoming" in description |
| Does not imply SaaS availability | ✅ No pricing, signup, or "available now" language |
| Misleading SoftwareApplication schema | ✅ None present |

### Navora Section Copy (verified)

> Navora is Meridian's upcoming Financial Command Center, designed around a concise 15-minute morning review. It turns QuickBooks Online data into a clear executive view of what is healthy, what needs attention, and what to do next.

> QUICKBOOKS ONLINE → MERIDIAN FINANCIAL INTELLIGENCE → CLEAR DECISIONS

**Tagline alignment:** "QuickBooks in. Clear decisions out." — represented as "QUICKBOOKS IN. CLEAR DECISIONS OUT." in hero label. ✅

### Dedicated `/navora` Page?

**Recommendation:** Not required at launch. Current single-page treatment is adequate while In Development. **Revisit when Navora enters beta/public availability** — a dedicated page would then support branded search and product-intent queries without crowding the main advisory page.

---

## Phase 11 — Future Content Architecture

| Candidate URL | Search Intent | Primary Topic | Why Own Page | Priority |
|---|---|---|---|---|
| `/financial-health-check` | "CFO financial health check," "QuickBooks business assessment" | Complimentary assessment offer | Highest-conversion service entry point; supports ad/organic landing | **P1** |
| `/fractional-cfo-services` | "fractional CFO services," "part-time CFO" | CFO advisory offering | Strong commercial intent; currently zero on-page coverage | **P1** |
| `/quickbooks-financial-analysis` | "QuickBooks Online financial analysis," "QBO reporting help" | QBO-specific expertise | Differentiates from generic CFO competitors | **P2** |
| `/about` or `/william-moylan` | "William Moylan CFO," founder credibility | Founder story + credentials | E-E-A-T signal; supports Accredible certificate | **P2** |
| `/navora` | "Navora Financial Command Center," "QuickBooks dashboard" | Product (when available) | Brand protection; defer until public beta | **P3** (post-launch) |
| `/resources/*` (2–3 articles max) | "cash flow analysis small business," "profitability metrics QBO" | Educational financial intelligence | Long-tail; keep premium — no content farm | **P3** |

---

## Findings

### BLOCKER

#### F-001: GoDaddy legacy site still indexable

| Field | Detail |
|---|---|
| **Severity** | BLOCKER |
| **Observed** | `https://mcfoservices.godaddysites.com/` returns HTTP 200 with full HTML content, own sitemap (`/sitemap.website.xml`, lastmod 2026-08-10), and robots.txt allowing crawl |
| **Evidence** | Title: "Meridian CFO Services"; H1: "Your Satisfaction, Our Mission."; meta description differs from production; canonical points to `https://mcfoservices.com/` but page remains accessible |
| **Affected URL** | `https://mcfoservices.godaddysites.com/` |
| **Recommended fix** | Redirect GoDaddy subdomain to `https://mcfoservices.com/` (301) OR unpublish/delete the GoDaddy site entirely. Submit GoDaddy URL removal in Google Search Console after redirect. |
| **Framer path** | N/A — GoDaddy account/DNS action required |

---

### HIGH

#### F-002: Multiple H1 elements in SSR HTML

| Field | Detail |
|---|---|
| **Severity** | HIGH |
| **Observed** | Three `<h1>` tags with identical text across responsive SSR variants |
| **Evidence** | Parsed HTML: H1 #1 (desktop variant), #2 (tablet), #3 (mobile) — all "See Your Business Like a CFO." |
| **Affected URL** | `https://mcfoservices.com/` |
| **Recommended fix** | In Framer, set tablet/mobile hero headline HTML tag to `p` (or use a single H1 with responsive visibility). Only one breakpoint variant should render `<h1>`. |
| **Framer path** | Home → Hero → "See Your Business Like a CFO." → select tablet/mobile breakpoint → Text → HTML Tag → `p` |

#### F-003: Malformed Calendly URL

| Field | Detail |
|---|---|
| **Severity** | HIGH |
| **Observed** | One link href concatenates URL twice: `https://calendly.com/william-mcfoservices/20min?month=2026-09https://calendly.com/william-mcfoservices/20min?month=2026-09` |
| **Evidence** | HTML link audit, 122-char href on one of four Calendly instances |
| **Affected URL** | `https://mcfoservices.com/` (likely Navora or embedded calendar CTA) |
| **Recommended fix** | Re-link the affected button/frame to `https://calendly.com/william-mcfoservices/20min` only |
| **Framer path** | Locate CTA with month parameter → Link → URL → `https://calendly.com/william-mcfoservices/20min` |

#### F-004: No structured data (JSON-LD)

| Field | Detail |
|---|---|
| **Severity** | HIGH |
| **Observed** | Zero `<script type="application/ld+json">` blocks |
| **Evidence** | HTML parse; Lighthouse structured-data audit not evaluated |
| **Affected URL** | `https://mcfoservices.com/` |
| **Recommended fix** | Add ProfessionalService JSON-LD (see Phase 7) |
| **Framer path** | Site Settings → General → Custom Code → Start of `<head>` or End of `<head>` |

#### F-005: Mobile LCP 9.2 seconds

| Field | Detail |
|---|---|
| **Severity** | HIGH |
| **Observed** | Lighthouse mobile LCP 9.2 s (target ≤2.5 s) |
| **Evidence** | Lighthouse mobile run 2026-09-22; architecture JPG 851 KB, Navora PNGs 309–650 KB |
| **Affected URL** | `https://mcfoservices.com/` |
| **Recommended fix** | Compress oversized images; enable lazy load on below-fold assets; verify mobile LCP element and prioritize its optimization |
| **Framer path** | Select image → Compress in asset panel; Settings → SEO → enable lazy loading if available per component |

---

### MEDIUM

#### F-006: Missing alt text on meaningful images

| Field | Detail |
|---|---|
| **Severity** | MEDIUM |
| **Observed** | Laptop hero, Navora hero, Navora KPI view, and Accredible certificate lack alt text |
| **Evidence** | 4 unique assets with `alt=MISSING` on at least one instance |
| **Affected URL** | `https://mcfoservices.com/` |
| **Recommended fix** | Add alt text per Phase 6 table |
| **Framer path** | Select image → Accessibility → Alt Text → [value from table] |

#### F-007: Contact form placeholder values

| Field | Detail |
|---|---|
| **Severity** | MEDIUM |
| **Observed** | Form placeholders show `Jane Smith` and `jane@mcfoservices.com`; latter is the only email string in page HTML |
| **Evidence** | HTML form parse |
| **Affected URL** | `https://mcfoservices.com/#contact` |
| **Recommended fix** | Change placeholders to neutral examples (e.g., "Your name", "you@company.com") or display a real contact email if intended |
| **Framer path** | Contact section → Name input → Placeholder; Email input → Placeholder |

#### F-008: OG image 1,661 KB

| Field | Detail |
|---|---|
| **Severity** | MEDIUM |
| **Observed** | Social share image exceeds reasonable size |
| **Evidence** | Download: 1,660.7 KB PNG |
| **Affected URL** | OG/Twitter meta tags |
| **Recommended fix** | Re-export at 1200×630, optimized PNG or WebP, target <300 KB |
| **Framer path** | Site Settings → SEO → Social → Replace social preview image |

#### F-009: Calendly links missing rel="noopener noreferrer"

| Field | Detail |
|---|---|
| **Severity** | MEDIUM |
| **Observed** | All 4 Calendly links use `target="_blank"` without `rel="noopener noreferrer"` |
| **Evidence** | HTML link attribute audit |
| **Affected URL** | `https://mcfoservices.com/` |
| **Recommended fix** | Framer may not expose rel attribute on link settings — if unavailable, acceptable risk for Calendly; otherwise Custom Code or link component override |
| **Framer path** | Link settings → Open in new tab (verify security option) |

#### F-010: Search-intent term gaps

| Field | Detail |
|---|---|
| **Severity** | MEDIUM |
| **Observed** | Missing: CFO consulting, fractional CFO, financial health review, CFO advisory |
| **Evidence** | Full-text term scan of rendered HTML |
| **Affected URL** | `https://mcfoservices.com/` |
| **Recommended fix** | Apply copy additions from Phase 5 (operator approval required) |
| **Framer path** | Edit text in respective sections per CURRENT/PROPOSED blocks |

#### F-011: Title differs from approved spec target

| Field | Detail |
|---|---|
| **Severity** | MEDIUM |
| **Observed** | Current: "…Business Owners"; Spec: "…Growing Businesses" |
| **Evidence** | Title tag 61 chars |
| **Affected URL** | `https://mcfoservices.com/` |
| **Recommended fix** | Operator decision — update if "Growing Businesses" is preferred |
| **Framer path** | Site Settings → SEO → Title → `Meridian CFO Services | Financial Clarity for Growing Businesses` |

---

### LOW

#### F-012: Meta description slightly long

| Field | Detail |
|---|---|
| **Severity** | LOW |
| **Observed** | 201 characters — may truncate in SERPs |
| **Evidence** | Meta tag length |
| **Affected URL** | `https://mcfoservices.com/` |
| **Recommended fix** | Trim to ~190 chars if truncation observed in Search Console |
| **Framer path** | Site Settings → SEO → Description |

#### F-013: No lazy loading on images

| Field | Detail |
|---|---|
| **Severity** | LOW |
| **Observed** | 0/21 images have `loading="lazy"` |
| **Evidence** | HTML img audit |
| **Affected URL** | `https://mcfoservices.com/` |
| **Recommended fix** | Enable lazy load on below-fold images in Framer |
| **Framer path** | Per-image or site-wide lazy load setting |

#### F-014: No privacy/terms pages

| Field | Detail |
|---|---|
| **Severity** | LOW |
| **Observed** | `/privacy` and `/terms` return 404 |
| **Evidence** | Path probe |
| **Affected URL** | N/A |
| **Recommended fix** | Add minimal privacy policy before paid traffic scales (not launch blocker for organic) |
| **Framer path** | Add new page → publish → include in sitemap |

#### F-015: H1 SSR text spacing

| Field | Detail |
|---|---|
| **Severity** | LOW |
| **Observed** | Raw HTML shows `See YourBusinessLike a CFO.` without spaces |
| **Evidence** | H1 text extraction |
| **Affected URL** | `https://mcfoservices.com/` |
| **Recommended fix** | Ensure screen readers and crawlers get spaced text (may be SSR artifact from line-break styling — verify in browser accessibility tree) |
| **Framer path** | Hero text → remove mid-phrase line break elements if causing concatenation |

---

## Framer Change List

Operator-ready checklist. Apply in order of severity.

### Site Settings

- [ ] **Site Settings → SEO → Title** → `Meridian CFO Services | Financial Clarity for Growing Businesses` *(if adopting spec target)*
- [ ] **Site Settings → SEO → Description** → Keep current or trim per Phase 3
- [ ] **Site Settings → SEO → Social → Image** → Replace with optimized image (<300 KB)
- [ ] **Site Settings → General → Custom Code → End of `<head>`** → Paste ProfessionalService JSON-LD (Phase 7)

### Hero Section

- [ ] **Home → Hero → "See Your Business Like a CFO." → Desktop breakpoint → Text → HTML Tag** → `H1`
- [ ] **Home → Hero → "See Your Business Like a CFO." → Tablet breakpoint → Text → HTML Tag** → `p`
- [ ] **Home → Hero → "See Your Business Like a CFO." → Mobile breakpoint → Text → HTML Tag** → `p`

### Images — Alt Text

- [ ] **Home → Laptop hero image → Accessibility → Alt Text** → `Meridian CFO Services dashboard on laptop showing QuickBooks Online financial analysis`
- [ ] **Home → Navora hero image → Accessibility → Alt Text** → `Navora Financial Command Center dashboard for QuickBooks Online`
- [ ] **Home → Navora KPI image → Accessibility → Alt Text** → `Navora KPI scorecard and risk intelligence detail`
- [ ] **Home → About → Certificate image → Accessibility → Alt Text** → `William Moylan Accredible certificate in Strategic Finance`
- [ ] **Home → Architecture image → Accessibility → Alt Text** → `` (empty — decorative)

### CTAs / Links

- [ ] **Find CTA with malformed Calendly URL** → Link → `https://calendly.com/william-mcfoservices/20min` (remove duplicated URL)
- [ ] **All Calendly CTAs** → Verify link = `https://calendly.com/william-mcfoservices/20min`

### Contact Section

- [ ] **Contact → Name input → Placeholder** → `Your name`
- [ ] **Contact → Email input → Placeholder** → `you@company.com`

### Copy Updates (optional — operator approval)

- [ ] **Hero subcopy** → Apply Phase 5 Recommendation 1
- [ ] **Financial Health Check CTA** → Apply Phase 5 Recommendation 2
- [ ] **About section** → Apply Phase 5 Recommendation 3

### Performance

- [ ] **Compress architecture JPG** (851 KB → target <200 KB)
- [ ] **Compress Navora PNGs** (309–650 KB each)
- [ ] **Enable lazy loading** on below-fold images

### External (not Framer)

- [ ] **GoDaddy** → Unpublish or 301-redirect `mcfoservices.godaddysites.com` → `https://mcfoservices.com/`
- [ ] **Google Search Console** → Request removal of GoDaddy URL after redirect; submit sitemap `https://mcfoservices.com/sitemap.xml`

---

## Code / Technical Changes

### Repository Assessment

This repository (`pulseforge-leadgen`) contains the Pulseforge lead-generation CRM application. **It does not contain the Meridian CFO Services Framer site source.** The production site is hosted on Framer (`server: Framer/2127774`, `framer-site-id: 9103349d76e4719336f75d37039f0c30c97cc11fd0c5e07c047b467925e43552`).

### Changes Implemented

| File | Change | Reason |
|---|---|---|
| `docs/audits/MERIDIAN-SEO-LAUNCH-AUDIT.md` | Created | SPEC-MERIDIAN-SEO-001 deliverable |

**No hosted-site fixes could be applied from this repository.** All production remediations require Framer project access and/or GoDaddy DNS actions listed above.

### Validation Performed

| Check | Method | Result |
|---|---|---|
| Production crawl | curl + HTML parse | ✅ Complete |
| Redirect chains | curl -I -L | ✅ Verified |
| robots.txt / sitemap | curl | ✅ Verified |
| Metadata | Regex extraction from rendered HTML | ✅ Verified |
| Heading structure | HTML tag parse | ✅ Verified |
| Lighthouse mobile | npx lighthouse (mobile) | ✅ Performance 74, SEO 100 |
| Lighthouse desktop | npx lighthouse (desktop preset) | ✅ Performance 96 |
| Calendly URL | curl -I | ✅ Primary URL 200; malformed URL also 200 |
| GoDaddy legacy | curl + sitemap fetch | ❌ Still live |
| Image sizes | curl download | ✅ Measured |
| External link probe | curl -I | ✅ Complete |

---

## Must Fix vs Future Opportunity

### Must Fix Before SEO Handoff

1. GoDaddy legacy site takedown/redirect (F-001)
2. Single H1 in SSR output (F-002)
3. Malformed Calendly URL (F-003)
4. JSON-LD structured data (F-004)
5. Alt text on meaningful images (F-006)
6. Contact form placeholders (F-007)

### Should Fix (High Value, Not Blocking Index)

7. Mobile LCP optimization (F-005)
8. OG image compression (F-008)
9. Search-intent copy additions (F-010)
10. Title alignment with spec (F-011)

### Future Opportunity

11. Dedicated landing pages (Phase 11)
12. Privacy/terms pages (F-014)
13. `/navora` page when product is publicly available
14. Educational resources (limited, premium)

---

## Acceptance Criteria Checklist

| # | Criterion | Status |
|---|---|---|
| 1 | Production crawl/indexability verified | ✅ |
| 2 | Homepage title/meta/canonical verified | ✅ |
| 3 | H1/H2 hierarchy documented | ✅ |
| 4 | No placeholder Framer metadata | ✅ |
| 5 | Meaningful images have alt-text recommendations | ✅ |
| 6 | Decorative imagery identified | ✅ |
| 7 | Sitemap and robots verified | ✅ |
| 8 | www/apex redirect behavior verified | ✅ |
| 9 | Structured-data recommendation + valid JSON-LD | ✅ |
| 10 | Broken/dead links identified | ✅ (1 malformed Calendly) |
| 11 | Performance measured | ✅ (Lighthouse mobile + desktop) |
| 12 | Precise Framer operator checklist produced | ✅ |
| 13 | No speculative/unsupported business claims | ✅ |
| 14 | Must-fix vs future-opportunity distinguished | ✅ |

---

*End of audit.*
