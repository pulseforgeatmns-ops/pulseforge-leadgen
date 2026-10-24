# Service Assurance mockups

| Master (keep) | Role |
|---|---|
| `client-dashboard.png` | Client portal dashboard |
| `cleaner-checklist.png` | Cleaner mobile checklist |
| `issues-resolution.png` | Issues & resolution screen |

Production delivery uses `<picture>` with AVIF/WebP `srcset` variants (`*-{width}w.{avif,webp}`). PNG remains the `<img>` fallback only.

Regenerate variants after replacing a PNG:

```bash
cd sites/anchor-cleaning/assets/service-assurance
node optimize-images.mjs   # requires `sharp` (npm install --no-save sharp from repo root)
```

Target: roughly 150–350 KB per width tier; script lowers quality if a variant exceeds the cap.
