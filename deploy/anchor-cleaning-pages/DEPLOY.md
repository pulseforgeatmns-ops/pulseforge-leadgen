# Deploy to `pulseforgeatmns-ops/anchor-cleaning`

Copy these files over the GitHub Pages repo and merge to `main`:

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
