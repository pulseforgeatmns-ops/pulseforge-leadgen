'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');

const dashboard = fs.readFileSync(
  path.join(__dirname, '..', 'public', 'dashboard.html'),
  'utf8'
);
const shell = fs.readFileSync(
  path.join(__dirname, '..', 'public', 'shared', 'shell.css'),
  'utf8'
);

function cssBody(source, selector) {
  const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = source.match(new RegExp(`(?:^|\\n)\\s*${escaped}\\s*\\{([^}]*)\\}`));
  assert.ok(match, `missing CSS rule for ${selector}`);
  return match[1];
}

test('refined pipeline scrolls inside the available dashboard height', () => {
  const refined = cssBody(shell, '#pipelineArea.pf-refined');

  assert.match(refined, /min-height:\s*0\s*!important\s*;/);
  assert.doesNotMatch(refined, /min-height:\s*100(?:d|s|l)?vh\s*;/);
  assert.match(refined, /overflow-y:\s*auto\s*!important\s*;/);
  assert.match(refined, /-webkit-overflow-scrolling:\s*touch\s*;/);
});

test('pipeline flex item is allowed to shrink before it becomes the scrollport', () => {
  const flexItem = cssBody(dashboard, '.center-col > .pipeline-area');
  const pipeline = cssBody(dashboard, '.pipeline-area');

  assert.match(flexItem, /flex:\s*1\s*;/);
  assert.match(flexItem, /min-height:\s*0\s*;/);
  assert.match(pipeline, /overflow-y:\s*auto\s*;/);
});
