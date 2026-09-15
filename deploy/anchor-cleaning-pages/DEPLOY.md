# Deploy to `pulseforgeatmns-ops/anchor-cleaning`

Copy these files over the GitHub Pages repo and merge to `main`:

| Source | Destination |
|---|---|
| `deploy/anchor-cleaning-pages/index.html` | `index.html` |
| `deploy/anchor-cleaning-pages/residential/index.html` | `residential/index.html` |

Or apply `0001-Add-Clarity-OpenAI-tracking-to-homepage-and-resident.patch` from repo root.

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
