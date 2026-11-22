#!/usr/bin/env node
'use strict';
// Cutover-only: repoint every runtime caller in this repository from the
// Railway hostname to the new production host. Run AFTER the final restore is
// verified and Render serves production; running it earlier would send live
// leads to a database that is not the source of truth.
//
//   node scripts/migration/switchPublicHost.js --to pulseforge-api-migration.onrender.com [--write]
//
// Without --write it only reports. Docs/history files are left as records.
// The public sites are published from their own repositories (see
// deploy/anchor-cleaning-pages/DEPLOY.md, sites/studio-substral/LAUNCH.md):
// sync them after this commit lands.
// Studio Substral is deliberately excluded: its doctrine forbids "Pulseforge"
// in page HTML (test/studioSubstralDoctrine.test.js), so it needs a neutral
// custom intake host at launch. It is not launched and has no live traffic.

const fs = require('fs');
const path = require('path');

const FROM = 'pulseforge-leadgen-production.up.railway.app';
const ROOT = path.resolve(__dirname, '..', '..');
const FILES = [
  'sites/anchor-cleaning/index.html',
  'sites/anchor-cleaning/residential/index.html',
  'sites/anchor-cleaning/residential/one-time/index.html',
  'deploy/anchor-cleaning-pages/index.html',
  'deploy/anchor-cleaning-pages/residential/index.html',
  'deploy/anchor-cleaning-pages/residential/one-time/index.html',
  'pulseforge-leadgen.html',
  'public/setter-dashboard.html',
  'public/dashboard.html',
  'calAgent.js',
  'scoutExpansion.js',
  'services/aoFieldService.js',
  'sketchAgent.js',
  'utils/telegram.js',
  'warmRoutingAgent.js',
  '.github/workflows/penny-anchor-daily.yml',
  '.github/workflows/spec252-post-deploy-verify.yml',
  '.github/workflows/spec253-mailbox-poll-verify.yml',
  '.github/workflows/spec254-256-production-acceptance.yml',
];

function main(argv) {
  const at = argv.indexOf('--to');
  const to = at >= 0 ? argv[at + 1] : '';
  if (!/^[a-z0-9.-]+\.[a-z]{2,}$/.test(to || '') || to === FROM) {
    console.error('usage: --to <new-hostname> [--write]');
    return 2;
  }
  const write = argv.includes('--write');
  const escaped = FROM.replace(/[.]/g, '\\.');
  let total = 0;
  for (const rel of FILES) {
    const file = path.join(ROOT, rel);
    if (!fs.existsSync(file)) continue;
    const text = fs.readFileSync(file, 'utf8');
    // The Studio Substral test escapes dots in its regex; replace both spellings.
    const count = text.split(FROM).length - 1 + text.split(escaped).length - 1;
    if (!count) continue;
    total += count;
    console.log(`${write ? 'updated' : 'would update'} ${rel}: ${count}`);
    if (write) {
      const next = text.split(FROM).join(to).split(escaped).join(to.replace(/[.]/g, '\\.'));
      fs.writeFileSync(file, next);
    }
  }
  console.log(`${total} occurrence(s) ${write ? 'replaced' : 'found'} -> ${to}`);
  return 0;
}

if (require.main === module) process.exitCode = main(process.argv.slice(2));
module.exports = { FILES, FROM, main };
