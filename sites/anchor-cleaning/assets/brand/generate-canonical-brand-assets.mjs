/**
 * Derive Anchor Cleaning brand assets from the approved polo logo source.
 * Source: anchor-polo-logo-source.png (do not redraw)
 */
import sharp from 'sharp';
import { writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const VERSION = '20260916';
const NAVY = { r: 7, g: 19, b: 31, alpha: 255 };
const CREAM = { r: 246, g: 241, b: 232, alpha: 255 };
const BASE = 'https://goanchorcleaning.com/assets/brand';
const SOURCE = join(__dirname, 'anchor-polo-logo-source.png');

function luminance(r, g, b) {
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
}

async function loadRgba(path) {
  const { data, info } = await sharp(path).ensureAlpha().raw().toBuffer({ resolveWithObject: true });
  return { data, width: info.width, height: info.height };
}

function makeTransparentLockup({ data, width, height }) {
  const out = Buffer.from(data);
  for (let i = 0; i < width * height; i++) {
    const o = i * 4;
    const r = out[o];
    const g = out[o + 1];
    const b = out[o + 2];
    if (luminance(r, g, b) < 18) out[o + 3] = 0;
  }
  return sharp(out, { raw: { width, height, channels: 4 } }).png();
}

function rowGoldStats(data, width, y) {
  let count = 0;
  let left = width;
  let right = 0;
  for (let x = 0; x < width; x++) {
    const o = (y * width + x) * 4;
    if (luminance(data[o], data[o + 1], data[o + 2]) > 40) {
      count++;
      left = Math.min(left, x);
      right = Math.max(right, x);
    }
  }
  return {
    density: count / width,
    span: right >= left ? right - left + 1 : 0,
    left,
    right,
  };
}

function findAnchorCrop({ data, width, height }) {
  const wordmarkSpanMin = Math.max(350, Math.round(width * 0.32));

  let top = 0;
  while (top < height && rowGoldStats(data, width, top).density < 0.01) top++;

  let textStart = height;
  for (let y = top + 40; y < height - 4; y++) {
    const row = rowGoldStats(data, width, y);
    if (row.span >= wordmarkSpanMin && row.density > 0.05) {
      textStart = y;
      break;
    }
  }

  let contentEnd = Math.max(top, textStart - 8);

  let left = width;
  let right = 0;
  for (let y = top; y <= contentEnd; y++) {
    for (let x = 0; x < width; x++) {
      const o = (y * width + x) * 4;
      if (luminance(data[o], data[o + 1], data[o + 2]) > 40) {
        left = Math.min(left, x);
        right = Math.max(right, x);
      }
    }
  }

  const pad = Math.round((right - left) * 0.06);
  return {
    left: Math.max(0, left - pad),
    top: Math.max(0, top - pad),
    width: Math.min(width - Math.max(0, left - pad), right - left + pad * 2),
    height: Math.min(height - Math.max(0, top - pad), contentEnd - top + pad * 2),
  };
}

async function trimLockup(transparentPng) {
  return transparentPng.trim({ threshold: 1 });
}

async function onNavySquare(input, size, paddingRatio = 0.14) {
  const pad = Math.round(size * paddingRatio);
  const inner = size - pad * 2;
  const resized = await input.clone().resize(inner, inner, { fit: 'inside', withoutEnlargement: false }).png().toBuffer();
  return sharp({
    create: { width: size, height: size, channels: 4, background: NAVY },
  })
    .composite([{ input: resized, gravity: 'centre' }])
    .png();
}

/** Social profile avatar: full lockup on navy (Facebook, Instagram, LinkedIn, GBP, Yelp). */
async function buildSocialAvatar(lockupPath) {
  return onNavySquare(sharp(lockupPath), 512, 0.09);
}

async function writeIcoFromPng(pngBuffer, path) {
  await sharp(pngBuffer).resize(32, 32).toFile(path);
}

async function writeSvgEmbed(pngPath, outPath, size) {
  const b64 = (await sharp(pngPath).resize(size, size).png().toBuffer()).toString('base64');
  writeFileSync(
    outPath,
    `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${size} ${size}"><image href="data:image/png;base64,${b64}" width="${size}" height="${size}"/></svg>`
  );
}

async function buildSocialPreview(lockupPath) {
  const W = 1200;
  const H = 630;
  const lockup = await sharp(lockupPath).resize(420, 420, { fit: 'inside', withoutEnlargement: true }).png().toBuffer();
  return sharp({
    create: { width: W, height: H, channels: 3, background: NAVY },
  })
    .composite([
      { input: Buffer.from(`<svg width="${W}" height="${H}" xmlns="http://www.w3.org/2000/svg"><rect y="${H - 96}" width="${W}" height="96" fill="#F6F1E8"/></svg>`), top: 0, left: 0 },
      { input: lockup, top: 72, left: Math.round((W - 420) / 2) },
    ])
    .jpeg({ quality: 92, mozjpeg: true })
    .toFile(join(__dirname, `social-preview-v${VERSION}.jpg`));
}

async function main() {
  const raw = await loadRgba(SOURCE);
  const transparent = await makeTransparentLockup(raw);
  const lockupTrimmed = await trimLockup(transparent);
  const lockupPath = join(__dirname, 'anchor-logo-canonical.png');
  await lockupTrimmed.toFile(lockupPath);

  const transparentRaw = await loadRgba(lockupPath);
  const anchorCrop = findAnchorCrop(transparentRaw);
  const anchorSymbol = sharp(lockupPath).extract(anchorCrop).png();
  const anchorPath = join(__dirname, 'anchor-symbol-canonical.png');
  await anchorSymbol.toFile(anchorPath);

  const avatar512 = await buildSocialAvatar(lockupPath);
  await avatar512.toFile(join(__dirname, `social-avatar-v${VERSION}.png`));

  const fav32 = await onNavySquare(sharp(anchorPath), 32, 0.12);
  const fav16 = await onNavySquare(sharp(anchorPath), 16, 0.1);
  const apple = await onNavySquare(sharp(anchorPath), 180, 0.14);
  const icon192 = await onNavySquare(sharp(anchorPath), 192, 0.14);
  const icon512 = await onNavySquare(sharp(anchorPath), 512, 0.16);

  await fav32.toFile(join(__dirname, `favicon-32x32-v${VERSION}.png`));
  await fav16.toFile(join(__dirname, `favicon-16x16-v${VERSION}.png`));
  await apple.toFile(join(__dirname, `apple-touch-icon-v${VERSION}.png`));
  await icon192.toFile(join(__dirname, `icon-192-v${VERSION}.png`));
  await icon512.toFile(join(__dirname, `icon-512-v${VERSION}.png`));

  const fav32Buf = await fav32.png().toBuffer();
  await writeIcoFromPng(fav32Buf, join(__dirname, `favicon-v${VERSION}.ico`));
  await writeSvgEmbed(anchorPath, join(__dirname, `favicon-v${VERSION}.svg`), 32);

  await buildSocialPreview(lockupPath);

  const manifest = {
    name: 'Anchor Cleaning',
    short_name: 'Anchor',
    description: 'Premium commercial and residential cleaning in Greater Manchester, New Hampshire.',
    start_url: '/',
    display: 'standalone',
    background_color: '#07131F',
    theme_color: '#07131F',
    icons: [
      {
        src: `${BASE}/icon-192-v${VERSION}.png?v=${VERSION}`,
        sizes: '192x192',
        type: 'image/png',
      },
      {
        src: `${BASE}/icon-512-v${VERSION}.png?v=${VERSION}`,
        sizes: '512x512',
        type: 'image/png',
      },
    ],
  };
  writeFileSync(join(__dirname, 'site.webmanifest'), `${JSON.stringify(manifest, null, 2)}\n`);

  console.log(JSON.stringify({ version: VERSION, anchorCrop, files: [
    'anchor-polo-logo-source.png',
    'anchor-logo-canonical.png',
    'anchor-symbol-canonical.png',
    `social-avatar-v${VERSION}.png`,
    `social-preview-v${VERSION}.jpg`,
    `favicon-v${VERSION}.ico`,
  ] }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
