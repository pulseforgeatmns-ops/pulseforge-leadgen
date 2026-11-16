'use strict';
/*
 * Builds the S2 bookmarklets (one line each, "javascript:" + percent-encoded):
 *   bookmarklet.txt             acceptance runner (s2RestAcceptance.js)
 *   diagnostic.bookmarklet.txt  minimal startup diagnostic (s2Diagnostic.js)
 *   sizeprobe.bookmarklet.txt   the diagnostic padded to the acceptance length
 * Usage: node scripts/s2-rest-acceptance/buildBookmarklet.js   (writes files)
 *        node scripts/s2-rest-acceptance/buildBookmarklet.js --check (verifies)
 *
 * Every built line ends with the visible marker S2_END_OF_BOOKMARKLET so a
 * truncated paste can be spotted by looking at the end of the bookmark URL.
 */
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const END = "void 'S2_END_OF_BOOKMARKLET';";

function collapse(src) {
  const code = src
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .split('\n').map((l) => l.trim()).filter(Boolean).join(' ');
  if (/(^|[^:])\/\/(?![^']*'\))/.test(code.replace(/https:\\\/\\\//g, ''))) {
    throw new Error('line comment found; bookmarklet would break');
  }
  return code;
}

function finish(code) {
  new Function(code); // syntax check only, not executed
  const url = 'javascript:' + encodeURIComponent(code);
  if (!/^javascript:[A-Za-z0-9%'()*!~._-]+$/.test(url)) throw new Error('unexpected raw character in bookmarklet');
  return url;
}

function buildId(code) {
  return crypto.createHash('sha256').update(code).digest('hex').slice(0, 12);
}

function buildAcceptance() {
  const code = collapse(fs.readFileSync(path.join(__dirname, 's2RestAcceptance.js'), 'utf8'));
  return finish(code.split('S2_BUILD_ID').join(buildId(code)) + ' ' + END);
}

function buildDiagnostic(kind, padTo) {
  const base = collapse(fs.readFileSync(path.join(__dirname, 's2Diagnostic.js'), 'utf8'))
    .split('S2_BUILD_ID').join(buildId(fs.readFileSync(path.join(__dirname, 's2Diagnostic.js'), 'utf8')))
    .split('S2_KIND').join(kind);
  const make = (pad, len) => finish(base.split('S2_LENGTH').join(String(len)) + (pad ? " void '" + 'x'.repeat(pad) + "';" : '') + ' ' + END);
  let pad = 0;
  let url = make(0, 0);
  if (padTo) pad = Math.max(0, padTo - url.length);
  for (let i = 0; i < 5; i += 1) {
    const next = make(pad, url.length);
    if (next.length === url.length) break;
    url = next;
  }
  url = make(pad, url.length);
  if (!url.includes(encodeURIComponent(String(url.length)))) throw new Error('length self-report did not converge');
  return url;
}

function buildAll() {
  const acceptance = buildAcceptance();
  return {
    'bookmarklet.txt': acceptance,
    'diagnostic.bookmarklet.txt': buildDiagnostic('minimal', 0),
    'sizeprobe.bookmarklet.txt': buildDiagnostic('size-probe', acceptance.length),
  };
}

module.exports = { buildAll, END };

if (require.main === module) {
  const out = buildAll();
  const check = process.argv.includes('--check');
  let stale = false;
  for (const [file, url] of Object.entries(out)) {
    const p = path.join(__dirname, file);
    if (check) {
      const cur = fs.existsSync(p) ? fs.readFileSync(p, 'utf8') : '';
      if (cur !== url + '\n') { stale = true; process.stderr.write(file + ' is stale\n'); }
    } else {
      fs.writeFileSync(p, url + '\n');
    }
    process.stdout.write(file + ': ' + url.length + ' chars\n');
  }
  if (stale) process.exit(1);
}
