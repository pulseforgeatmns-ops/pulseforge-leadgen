# Deploy to `pulseforgeatmns-ops/anchor-cleaning`

## Production source (verified)

**goanchorcleaning.com** is **not** served from this monorepo. GitHub Pages publishes:

| Setting | Value |
|---|---|
| Repo | `pulseforgeatmns-ops/anchor-cleaning` |
| Branch | `main` |
| Path | `/` (repo root) |
| Build | Legacy Pages (static HTML; no build step) |
| Custom domain | `goanchorcleaning.com` |

Authoritative commercial source in **pulseforge-leadgen**: `sites/anchor-cleaning/`. Sync that tree (or the paths below) into `anchor-cleaning` before merge.

### Service Assurance section

Use the sync script so the header logo and referenced brand assets are included:

```bash
bash deploy/anchor-cleaning-pages/sync-from-monorepo.sh /path/to/anchor-cleaning
```

For a manual sync, include the brand directory alongside the section assets:

```bash
rsync -a sites/anchor-cleaning/index.html /path/to/anchor-cleaning/
rsync -a sites/anchor-cleaning/assets/brand/ /path/to/anchor-cleaning/assets/brand/
rsync -a sites/anchor-cleaning/assets/service-assurance/ /path/to/anchor-cleaning/assets/service-assurance/
rsync -a sites/anchor-cleaning/framer/ /path/to/anchor-cleaning/framer/
# Hero JPGs if missing on Pages repo:
rsync -a sites/anchor-cleaning/assets/*.jpg /path/to/anchor-cleaning/assets/
```

Verify after deploy:

```bash
curl -sL https://goanchorcleaning.com/ | rg 'service-assurance|Service Assurance'
curl -sI https://goanchorcleaning.com/assets/service-assurance/client-dashboard-1200w.webp | head -3
```

---

Copy these files over the GitHub Pages repo and merge to `main` (legacy Clarity patch flow):

| Source | Destination |
|---|---|
| `deploy/anchor-cleaning-pages/index.html` | `index.html` |
| `deploy/anchor-cleaning-pages/residential/index.html` | `residential/index.html` |

Or apply a patch from this folder inside a clone of `pulseforgeatmns-ops/anchor-cleaning`:

```bash
git clone https://github.com/pulseforgeatmns-ops/anchor-cleaning.git
cd anchor-cleaning
git checkout -b cursor/deploy-clarity-tracking-6bcc
git apply ../path/to/0002-Add-Microsoft-Clarity-tracking-only.patch
git add index.html residential/index.html
git commit -m "Add Microsoft Clarity tracking to homepage and residential page"
git push -u origin cursor/deploy-clarity-tracking-6bcc
```

**Clarity-only (recommended):** `0002-Add-Microsoft-Clarity-tracking-only.patch` — adds Microsoft Clarity (`yhnafqbr5k`) only; leaves OpenAI Ads unchanged.

**Legacy (includes OpenAI residential wiring):** `0001-Add-Clarity-OpenAI-tracking-to-homepage-and-resident.patch`

## Verify before merge

```bash
rg 'yhnafqbr5k|clarity\.ms|oaiq|bzrcdn.openai.com|trackOpenAiLeadCreated|lead_created' index.html residential/index.html
```

Each file should contain `yhnafqbr5k` exactly once.

## Verify after GitHub Pages deploy

```bash
curl -sL https://goanchorcleaning.com/ | rg 'yhnafqbr5k|clarity\.ms|oaiq|lead_created'
curl -sL https://goanchorcleaning.com/residential/ | rg 'yhnafqbr5k|clarity\.ms|oaiq|lead_created'
```
