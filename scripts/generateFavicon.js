'use strict';

// Generates public/favicon.ico — the Pulseforge browser tab icon.
//
//   node scripts/generateFavicon.js
//
// Dependency-free: builds 16x16 and 32x32 PNG frames by hand (zlib deflate +
// manual chunk CRCs) and wraps them in an ICO container (PNG-in-ICO, which
// every evergreen browser supports). The mark is a gold "P" monogram on the
// deep navy from public/shared/tokens.css. The committed public/favicon.ico
// is the artifact actually served; re-run this script only to regenerate it.

const fs = require('fs');
const path = require('path');
const zlib = require('zlib');

// Phase B visual system (public/shared/tokens.css):
//   --pf-nav-bg / --pf-navy: #10203c
//   --pf-gold:               #9a7b2d (brightened here for 16px legibility)
const NAVY = [0x10, 0x20, 0x3c, 0xff];
const GOLD = [0xc8, 0xa2, 0x4b, 0xff];

// "P" monogram on a 16x16 grid. '#' = gold, '.' = navy.
const GLYPH = [
  '................',
  '................',
  '................',
  '....########....',
  '....########....',
  '....##....##....',
  '....##....##....',
  '....########....',
  '....########....',
  '....##..........',
  '....##..........',
  '....##..........',
  '....##..........',
  '................',
  '................',
  '................',
];

function crc32(buf) {
  let crc = 0xffffffff;
  for (let i = 0; i < buf.length; i++) {
    crc ^= buf[i];
    for (let bit = 0; bit < 8; bit++) {
      crc = (crc >>> 1) ^ (0xedb88320 & -(crc & 1));
    }
  }
  return (crc ^ 0xffffffff) >>> 0;
}

function pngChunk(type, data) {
  const len = Buffer.alloc(4);
  len.writeUInt32BE(data.length);
  const body = Buffer.concat([Buffer.from(type, 'ascii'), data]);
  const crc = Buffer.alloc(4);
  crc.writeUInt32BE(crc32(body));
  return Buffer.concat([len, body, crc]);
}

function buildPng(size) {
  const scale = size / 16;
  const rows = [];
  for (let y = 0; y < size; y++) {
    const row = Buffer.alloc(1 + size * 4); // leading 0 = "None" scanline filter
    for (let x = 0; x < size; x++) {
      const cell = GLYPH[Math.floor(y / scale)][Math.floor(x / scale)];
      const rgba = cell === '#' ? GOLD : NAVY;
      Buffer.from(rgba).copy(row, 1 + x * 4);
    }
    rows.push(row);
  }
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(size, 0);
  ihdr.writeUInt32BE(size, 4);
  ihdr[8] = 8;  // bit depth
  ihdr[9] = 6;  // color type: RGBA
  return Buffer.concat([
    Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    pngChunk('IHDR', ihdr),
    pngChunk('IDAT', zlib.deflateSync(Buffer.concat(rows), { level: 9 })),
    pngChunk('IEND', Buffer.alloc(0)),
  ]);
}

function buildIco(sizes) {
  const pngs = sizes.map(buildPng);
  const header = Buffer.alloc(6);
  header.writeUInt16LE(0, 0); // reserved
  header.writeUInt16LE(1, 2); // type: icon
  header.writeUInt16LE(pngs.length, 4);
  const entries = [];
  let offset = 6 + 16 * pngs.length;
  pngs.forEach((png, i) => {
    const entry = Buffer.alloc(16);
    entry[0] = sizes[i] === 256 ? 0 : sizes[i]; // width
    entry[1] = sizes[i] === 256 ? 0 : sizes[i]; // height
    entry.writeUInt16LE(1, 4);  // color planes
    entry.writeUInt16LE(32, 6); // bits per pixel
    entry.writeUInt32LE(png.length, 8);
    entry.writeUInt32LE(offset, 12);
    offset += png.length;
    entries.push(entry);
  });
  return Buffer.concat([header, ...entries, ...pngs]);
}

const out = path.join(__dirname, '..', 'public', 'favicon.ico');
fs.writeFileSync(out, buildIco([16, 32]));
console.log(`[generate-favicon] wrote ${out} (${fs.statSync(out).size} bytes)`);
