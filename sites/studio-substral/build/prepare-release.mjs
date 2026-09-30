#!/usr/bin/env node
import { cpSync, mkdirSync, existsSync, readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { createHash } from 'node:crypto';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const site = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const destination = process.argv[2] && path.resolve(process.argv[2]);
if (!destination || existsSync(destination)) throw new Error('Provide a new, empty release directory.');
const canonical = readFileSync(path.join(site, 'CNAME'), 'utf8').trim();
if (canonical !== 'studiosubstral.com') throw new Error('Unexpected canonical domain');
mkdirSync(destination, { recursive: true });
for (const item of ['index.html', 'robots.txt', 'sitemap.xml', 'CNAME', '.nojekyll', 'assets', 'public']) {
  const source = path.join(site, item);
  if (!existsSync(source)) continue;
  if (item === 'public') {
    for (const entry of readdirSync(source)) {
      cpSync(path.join(source, entry), path.join(destination, entry), { recursive: true });
    }
    continue;
  }
  cpSync(source, path.join(destination, item), { recursive: true });
}
const manifest = {};
function walk(directory) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const full = path.join(directory, entry.name);
    if (entry.isDirectory()) walk(full);
    else manifest[path.relative(destination, full)] = createHash('sha256').update(readFileSync(full)).digest('hex');
  }
}
walk(destination);
writeFileSync(`${destination}.sha256.json`, JSON.stringify(manifest, null, 2) + '\n');
console.log(`Release: ${destination}\nFiles: ${Object.keys(manifest).length}\nChecksums: ${destination}.sha256.json`);
