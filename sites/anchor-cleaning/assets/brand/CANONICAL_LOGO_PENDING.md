# Anchor Cleaning brand assets

**Source of truth:** `anchor-polo-logo-source.png` (approved gold polo logo)

Regenerate derived assets after any source update:

```bash
cd sites/anchor-cleaning
npm install sharp --no-save
node assets/brand/generate-canonical-brand-assets.mjs
```

Then bump `?v=` query strings and versioned filenames in `index.html`, `residential/index.html`, and `site.webmanifest`.

## Derived files

| File | Use |
|---|---|
| `anchor-logo-canonical.png` | Transparent full lockup — header, JSON-LD `logo` |
| `anchor-symbol-canonical.png` | Anchor symbol only (cropped from source) |
| `social-avatar-v20260916.png` | Square avatar — anchor symbol on navy |
| `social-preview-v20260916.jpg` | Open Graph / Twitter link preview |
| `favicon-v20260916.ico` | Browser favicon |
| `favicon-v20260916.svg` | SVG favicon (embedded symbol raster) |
| `favicon-16x16-v20260916.png` | PNG favicon |
| `favicon-32x32-v20260916.png` | PNG favicon |
| `apple-touch-icon-v20260916.png` | iOS home-screen icon |
| `icon-192-v20260916.png` / `icon-512-v20260916.png` | Web manifest |
| `site.webmanifest` | PWA manifest |

All public URLs use absolute paths on `https://goanchorcleaning.com/assets/brand/` with cache-busting query `?v=20260916`.

## Platform profile avatars

Facebook, Instagram, LinkedIn, Google Business Profile, Yelp, etc. must still be updated manually on each platform using the same canonical mark (`social-avatar-v20260916.png` or source lockup).
