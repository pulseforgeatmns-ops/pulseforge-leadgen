# Publish Service Assurance to goanchorcleaning.com

Production repo: **`pulseforgeatmns-ops/anchor-cleaning`** (`main`, GitHub Pages root).

## 1. Merge monorepo PR

Merge [pulseforge-leadgen #723](https://github.com/pulseforgeatmns-ops/pulseforge-leadgen/pull/723) into `main` (requires branch admin if `revenue-postgresql-required` blocks bot merge).

## 2. Sync and push (requires write access to `anchor-cleaning`)

```bash
git clone https://github.com/pulseforgeatmns-ops/anchor-cleaning.git
cd anchor-cleaning
git pull origin main

/path/to/pulseforge-leadgen/deploy/anchor-cleaning-pages/sync-from-monorepo.sh .

git status
git diff --stat

git add index.html assets/service-assurance framer
git commit -m "Publish Service Assurance portal section"
git push origin main
```

Alternative (apply pre-built patch from monorepo after #723 merge):

```bash
cd anchor-cleaning
git apply /path/to/pulseforge-leadgen/deploy/anchor-cleaning-pages/patches/0001-Publish-Service-Assurance-portal-section.patch
git push origin main
```

## 3. Verify

```bash
# Pages source
gh api repos/pulseforgeatmns-ops/anchor-cleaning/pages --jq '.html_url,.source'

# Live HTML
curl -sL https://goanchorcleaning.com/ | rg 'service-assurance|Service Assurance'

# Optimized assets
curl -sI https://goanchorcleaning.com/assets/service-assurance/client-dashboard-960w.webp | head -3
```

Open https://goanchorcleaning.com/#service-assurance and smoke-test nav, visuals, and `#contact` CTA.
