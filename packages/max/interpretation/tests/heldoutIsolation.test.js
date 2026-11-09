'use strict';

/**
 * Brief constraint: "Do not hardcode the evaluation prompts into production
 * routing." Production code may not import the held-out set, the semantic
 * scenarios, or contain any held-out utterance.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { CASES } = require('../eval/heldoutInterpretation');

const ROOT = path.resolve(__dirname, '../../../..');
const PRODUCTION_DIRS = ['packages/max', 'packages/acquisition-mission', 'packages/decision-service', 'routes', 'services', 'utils'];
const EXCLUDED = [/\/tests?\//, /\/eval\//, /node_modules/];

function walk(dir, out = []) {
  for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
    const p = path.join(dir, e.name);
    if (EXCLUDED.some((re) => re.test(p + (e.isDirectory() ? '/' : '')))) continue;
    if (e.isDirectory()) walk(p, out);
    else if (e.name.endsWith('.js')) out.push(p);
  }
  return out;
}

const files = PRODUCTION_DIRS.flatMap((d) => (fs.existsSync(path.join(ROOT, d)) ? walk(path.join(ROOT, d)) : []));
const sources = files.map((f) => [path.relative(ROOT, f), fs.readFileSync(f, 'utf8')]);

describe('held-out evaluation isolation', () => {
  it('scans a meaningful set of production files', () => {
    assert.ok(sources.length > 200, `only ${sources.length} files scanned`);
  });

  it('no production module imports an evaluation set', () => {
    const offenders = sources.filter(([, src]) => /heldoutInterpretation|eval\/semantic\/scenarios/.test(src)).map(([f]) => f);
    assert.deepEqual(offenders, []);
  });

  it('no production module contains a held-out utterance', () => {
    const norm = (s) => s.toLowerCase().replace(/[’']/g, "'");
    const offenders = [];
    for (const c of CASES) {
      const needle = norm(c.text);
      if (needle.length < 12) continue; // very short replies ("Nope.") are ordinary vocabulary
      for (const [f, src] of sources) if (norm(src).includes(needle)) offenders.push(`${c.id} in ${f}`);
    }
    assert.deepEqual(offenders, []);
  });
});
