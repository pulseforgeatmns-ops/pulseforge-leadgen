#!/usr/bin/env node
/**
 * Generate WebP + AVIF variants from PNG masters. Re-run after replacing PNGs.
 * Usage: node optimize-images.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import sharp from "sharp";

const dir = path.dirname(fileURLToPath(import.meta.url));

const SPECS = [
  {
    base: "client-dashboard",
    widths: [720, 960, 1200, 1448],
    webpQuality: 86,
    avifQuality: 62,
    maxKb: 350,
  },
  {
    base: "issues-resolution",
    widths: [720, 960, 1200, 1448],
    webpQuality: 86,
    avifQuality: 62,
    maxKb: 350,
  },
  {
    base: "cleaner-checklist",
    widths: [280, 390, 560, 780, 941],
    webpQuality: 88,
    avifQuality: 64,
    maxKb: 320,
  },
];

async function encodeVariant(input, width, format, quality) {
  let pipeline = sharp(input).rotate().resize({ width, withoutEnlargement: true });
  if (format === "webp") {
    pipeline = pipeline.webp({ quality, effort: 6, smartSubsample: true });
  } else {
    pipeline = pipeline.avif({ quality, effort: 6 });
  }
  return pipeline.toBuffer();
}

async function tuneQuality(input, width, format, startQ, maxKb) {
  let q = startQ;
  let buf = await encodeVariant(input, width, format, q);
  while (buf.length > maxKb * 1024 && q > 52) {
    q -= 4;
    buf = await encodeVariant(input, width, format, q);
  }
  return { buffer: buf, quality: q };
}

for (const spec of SPECS) {
  const png = path.join(dir, `${spec.base}.png`);
  if (!fs.existsSync(png)) {
    console.warn("Skip missing", png);
    continue;
  }
  const meta = await sharp(png).metadata();
  const maxW = meta.width || spec.widths.at(-1);
  const widths = spec.widths.filter((w) => w <= maxW);
  if (!widths.includes(maxW)) widths.push(maxW);

  for (const w of widths) {
    for (const format of ["webp", "avif"]) {
      const startQ = format === "webp" ? spec.webpQuality : spec.avifQuality;
      const { buffer, quality } = await tuneQuality(
        png,
        w,
        format,
        startQ,
        spec.maxKb,
      );
      const out = path.join(dir, `${spec.base}-${w}w.${format}`);
      fs.writeFileSync(out, buffer);
      console.log(
        `${path.basename(out)}  ${(buffer.length / 1024).toFixed(1)} KB  q=${quality}`,
      );
    }
  }
}
