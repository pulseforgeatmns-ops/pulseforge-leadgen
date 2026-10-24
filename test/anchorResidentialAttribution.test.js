const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

for (const rel of [
  'sites/anchor-cleaning/residential/index.html',
  'deploy/anchor-cleaning-pages/residential/index.html',
  'sites/anchor-cleaning/residential/one-time/index.html',
  'deploy/anchor-cleaning-pages/residential/one-time/index.html',
]) {
  test(`${rel} captures and submits paid attribution`, () => {
    const html = fs.readFileSync(path.join(process.cwd(), rel), 'utf8');
    assert.match(html, /gclid/);
    assert.match(html, /gbraid/);
    assert.match(html, /wbraid/);
    assert.match(html, /utm_source/);
    assert.match(html, /landing_page_url/);
    assert.match(html, /referrer/);
    assert.match(html, /getSubmissionAttribution\(\)/);
    assert.match(html, /data\.attribution=attribution/);
  });
}
