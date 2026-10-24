/**
 * Purpose-built square business logo for Google Ads Search (small-format rendering).
 * Uses canonical Anchor Navy (#07131F) + extracted gold anchor symbol (no redesign).
 */
import sharp from 'sharp';
import { readFileSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const VERSION = '20260930';
const NAVY = { r: 7, g: 19, b: 31, alpha: 255 };
const GOLD_HEX = '#C49A55';
const ANCHOR_SYMBOL = join(__dirname, 'anchor-symbol-canonical.png');

/** Tighter padding than favicon (0.12–0.16) so the mark reads at ad thumbnail size. */
const ADS_PADDING_RATIO = 0.0625;

async function buildAdsLogoSquare(size, paddingRatio = ADS_PADDING_RATIO) {
  const pad = Math.round(size * paddingRatio);
  const inner = size - pad * 2;
  const symbol = await sharp(ANCHOR_SYMBOL)
    .resize(inner, inner, { fit: 'inside', withoutEnlargement: false })
    .png()
    .toBuffer();
  return sharp({
    create: { width: size, height: size, channels: 4, background: NAVY },
  })
    .composite([{ input: symbol, gravity: 'centre' }])
    .png();
}

async function buildLegacyFaviconStyle(size) {
  return buildAdsLogoSquare(size, 0.12);
}

async function buildTestSheet() {
  const sizes = [24, 32, 40, 64];
  const previewScale = 8;
  const colW = 64 * previewScale + 48;
  const rowH = 64 * previewScale + 120;
  const W = colW * sizes.length + 80;
  const H = rowH * 2 + 100;

  const headerSvg = `
    <svg width="${W}" height="${H}" xmlns="http://www.w3.org/2000/svg">
      <rect width="100%" height="100%" fill="#F6F1E8"/>
      <text x="40" y="48" font-family="Inter, sans-serif" font-size="22" font-weight="700" fill="#07131F">Anchor Google Ads logo — small-format preview</text>
      <text x="40" y="78" font-family="Inter, sans-serif" font-size="14" fill="#46535F">Navy ${NAVY.r === 7 ? '#07131F' : ''} · Gold ${GOLD_HEX} · symbol-only · v${VERSION}</text>
      ${sizes.map((px, i) => `
        <text x="${40 + i * colW}" y="118" font-family="Inter, sans-serif" font-size="13" font-weight="600" fill="#10202D">${px}×${px}px (×${previewScale} preview)</text>
      `).join('')}
      <text x="40" y="${rowH + 48}" font-family="Inter, sans-serif" font-size="15" font-weight="700" fill="#8A6534">Previous (favicon-style padding 12%)</text>
      <text x="40" y="${rowH * 2 + 48}" font-family="Inter, sans-serif" font-size="15" font-weight="700" fill="#0A1723">New Google Ads business logo (padding 6.25%)</text>
    </svg>`;

  const composites = [{ input: Buffer.from(headerSvg), top: 0, left: 0 }];

  for (let i = 0; i < sizes.length; i++) {
    const px = sizes[i];
    const x = 40 + i * colW;
    const legacyBuf = await (await buildLegacyFaviconStyle(px)).png().toBuffer();
    const newBuf = await (await buildAdsLogoSquare(px)).png().toBuffer();
    composites.push(
      {
        input: await sharp(legacyBuf).resize(px * previewScale, px * previewScale, { kernel: sharp.kernel.nearest }).png().toBuffer(),
        top: 130,
        left: x,
      },
      {
        input: await sharp(newBuf).resize(px * previewScale, px * previewScale, { kernel: sharp.kernel.nearest }).png().toBuffer(),
        top: rowH + 130,
        left: x,
      },
    );
  }

  return sharp({
    create: { width: W, height: H, channels: 4, background: { r: 246, g: 241, b: 232, alpha: 255 } },
  })
    .composite(composites)
    .png()
    .toFile(join(__dirname, `google-ads-logo-size-test-v${VERSION}.png`));
}

async function main() {
  const source1200 = join(__dirname, `google-ads-business-logo-v${VERSION}.png`);
  const source512 = join(__dirname, `google-ads-business-logo-512-v${VERSION}.png`);

  await (await buildAdsLogoSquare(1200)).toFile(source1200);
  await (await buildAdsLogoSquare(512)).toFile(source512);
  await buildTestSheet();

  const meta = await sharp(source1200).metadata();
  console.log(JSON.stringify({
    version: VERSION,
    files: [
      `google-ads-business-logo-v${VERSION}.png`,
      `google-ads-business-logo-512-v${VERSION}.png`,
      `google-ads-logo-size-test-v${VERSION}.png`,
    ],
    colors: { navy: '#07131F', gold: GOLD_HEX },
    paddingRatio: ADS_PADDING_RATIO,
    dimensions: { width: meta.width, height: meta.height },
  }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
