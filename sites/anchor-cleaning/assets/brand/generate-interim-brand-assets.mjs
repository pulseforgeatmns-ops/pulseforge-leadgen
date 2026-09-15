/**
 * Generates interim Anchor Cleaning brand assets without drawing the logo.
 *
 * TEMPORARY_PENDING_CANONICAL_LOGO
 * Replace outputs when the approved gold polo Anchor mark is added as:
 *   assets/brand/anchor-logo-canonical.svg (preferred)
 *   assets/brand/anchor-logo-canonical.png (1024px+ square, transparent)
 *
 * Then regenerate favicon, apple-touch, manifest icons, social preview, and
 * update BRAND_ASSET_VERSION in site HTML + site.webmanifest.
 */
import sharp from 'sharp';
import { writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const VERSION = '20260915';
const NAVY = '#07131F';
const CREAM = '#F6F1E8';
const GOLD = '#C49A55';
const MUTED = '#52606B';

async function writeSolidPng(path, size, color) {
  await sharp({
    create: {
      width: size,
      height: size,
      channels: 3,
      background: color,
    },
  })
    .png()
    .toFile(path);
}

async function writeSocialPreview(path) {
  const svg = `<svg width="1200" height="630" xmlns="http://www.w3.org/2000/svg">
    <rect width="1200" height="630" fill="${NAVY}"/>
    <rect x="0" y="500" width="1200" height="130" fill="${CREAM}"/>
    <rect x="96" y="120" width="4" height="260" fill="${GOLD}"/>
    <text x="132" y="210" fill="${CREAM}" font-family="Georgia, 'Times New Roman', serif" font-size="64" font-weight="500">ANCHOR CLEANING</text>
    <text x="132" y="280" fill="${GOLD}" font-family="Arial, Helvetica, sans-serif" font-size="30" letter-spacing="1">Greater Manchester, New Hampshire</text>
    <text x="96" y="575" fill="${MUTED}" font-family="Arial, Helvetica, sans-serif" font-size="24">Premium commercial &amp; residential cleaning</text>
  </svg>`;
  await sharp(Buffer.from(svg)).jpeg({ quality: 90, mozjpeg: true }).toFile(path);
}

async function writeFaviconSvg(path) {
  writeFileSync(
    path,
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 32 32"><rect width="32" height="32" fill="${NAVY}"/></svg>`
  );
}

async function writeIco(path) {
  const png = await sharp({
    create: { width: 32, height: 32, channels: 4, background: NAVY },
  })
    .png()
    .toBuffer();
  await sharp(png).toFile(path);
}

async function main() {
  const base = __dirname;
  await writeSocialPreview(join(base, `social-preview-v${VERSION}.jpg`));
  await writeSolidPng(join(base, `favicon-16x16-v${VERSION}.png`), 16, NAVY);
  await writeSolidPng(join(base, `favicon-32x32-v${VERSION}.png`), 32, NAVY);
  await writeSolidPng(join(base, `apple-touch-icon-v${VERSION}.png`), 180, NAVY);
  await writeSolidPng(join(base, `icon-192-v${VERSION}.png`), 192, NAVY);
  await writeSolidPng(join(base, `icon-512-v${VERSION}.png`), 512, NAVY);
  await writeFaviconSvg(join(base, `favicon-v${VERSION}.svg`));
  await writeIco(join(base, `favicon-v${VERSION}.ico`));

  const manifest = {
    name: 'Anchor Cleaning',
    short_name: 'Anchor',
    description: 'Premium commercial and residential cleaning in Greater Manchester, New Hampshire.',
    start_url: '/',
    display: 'standalone',
    background_color: NAVY,
    theme_color: NAVY,
    icons: [
      {
        src: `https://goanchorcleaning.com/assets/brand/icon-192-v${VERSION}.png?v=${VERSION}`,
        sizes: '192x192',
        type: 'image/png',
      },
      {
        src: `https://goanchorcleaning.com/assets/brand/icon-512-v${VERSION}.png?v=${VERSION}`,
        sizes: '512x512',
        type: 'image/png',
      },
    ],
  };
  writeFileSync(join(base, `site.webmanifest`), `${JSON.stringify(manifest, null, 2)}\n`);
  console.log(`Generated interim brand assets v${VERSION} in ${base}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
