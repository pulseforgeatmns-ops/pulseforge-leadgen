# Anchor Cleaning brand assets

**Source of truth:** `anchor-polo-logo-source.png` (approved gold polo logo)

**Primary brand identity:** full gold Anchor Cleaning lockup  
**Secondary compact icon:** anchor symbol only (favicon / small app icons only)

Regenerate derived assets after any source update:

```bash
cd sites/anchor-cleaning
npm install sharp --no-save
node assets/brand/generate-canonical-brand-assets.mjs
```

Then rebuild `anchor-cleaning-brand-assets.zip` from the derived files in this folder, and bump `?v=` query strings and versioned filenames in `index.html`, `residential/index.html`, and `site.webmanifest`.

## Derived files

| File | Use |
|---|---|
| `anchor-logo-canonical.png` | Transparent full lockup — header, JSON-LD `logo` |
| `anchor-symbol-canonical.png` | Anchor symbol only — favicon / small app icons |
| `social-avatar-v20260916.png` | Square social profile avatar — **full lockup** on navy |
| `social-preview-v20260916.jpg` | Open Graph / Twitter link preview — full lockup |
| `favicon-v20260916.ico` | Browser favicon (symbol on navy) |
| `favicon-v20260916.svg` | SVG favicon (embedded symbol raster) |
| `favicon-16x16-v20260916.png` | PNG favicon (symbol on navy) |
| `favicon-32x32-v20260916.png` | PNG favicon (symbol on navy) |
| `apple-touch-icon-v20260916.png` | iOS home-screen icon (symbol on navy) |
| `icon-192-v20260916.png` / `icon-512-v20260916.png` | Web manifest (symbol on navy) |
| `site.webmanifest` | PWA manifest |

## Usage rules

| Context | Asset |
|---|---|
| Facebook, Instagram, LinkedIn, GBP, Yelp profile photos | `social-avatar-v20260916.png` (full lockup) |
| Website header / JSON-LD / OG preview | Full lockup |
| Favicon, Apple touch, manifest icons | Symbol only |

Do **not** use symbol-only for social profile avatars.

All public URLs use absolute paths on `https://goanchorcleaning.com/assets/brand/` with cache-busting query `?v=20260916`.

## Platform profile avatars

Upload `social-avatar-v20260916.png` manually on each platform (full lockup on navy — not symbol-only).

## Download bundle

All canonical derived assets plus usage notes:

- **Zip:** `anchor-cleaning-brand-assets.zip` (same folder)
- **After deploy:** https://goanchorcleaning.com/assets/brand/anchor-cleaning-brand-assets.zip
