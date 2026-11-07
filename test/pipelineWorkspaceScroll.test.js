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
const shellScript = fs.readFileSync(
  path.join(__dirname, '..', 'public', 'shared', 'shell.js'),
  'utf8'
);

function cssBody(source, selector) {
  const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const match = source.match(new RegExp(`(?:^|\\n)\\s*${escaped}\\s*\\{([^}]*)\\}`));
  assert.ok(match, `missing CSS rule for ${selector}`);
  return match[1];
}

test('active Pipeline makes the document the vertical scrollport', () => {
  const documentScroll = cssBody(shell, 'html.pf-pipeline-page-scroll,\nbody.pf-pipeline-page-scroll');
  const pipeline = cssBody(shell, 'body.pf-pipeline-page-scroll #pipelineArea.pf-refined');

  assert.match(documentScroll, /height:\s*auto\s*;/);
  assert.match(documentScroll, /overflow-y:\s*auto\s*!important\s*;/);
  assert.match(pipeline, /min-height:\s*0\s*!important\s*;/);
  assert.match(pipeline, /overflow-y:\s*visible\s*!important\s*;/);
});

test('Pipeline page-scroll mode removes clipping from the dashboard flex chain', () => {
  const content = cssBody(shell, 'body.pf-pipeline-page-scroll .content-col,\nbody.pf-pipeline-page-scroll .main,\nbody.pf-pipeline-page-scroll .center-col');
  const main = cssBody(shell, 'body.pf-pipeline-page-scroll .main');
  const flexItem = cssBody(shell, 'body.pf-pipeline-page-scroll .center-col > .pipeline-area');

  assert.match(content, /overflow:\s*visible\s*;/);
  assert.match(main, /flex:\s*0\s+0\s+auto\s*;/);
  assert.match(flexItem, /flex:\s*0\s+0\s+auto\s*;/);
  assert.match(flexItem, /overflow-y:\s*visible\s*!important\s*;/);
});

test('shell tracks Pipeline visibility and toggles page-scroll mode on both roots', () => {
  assert.match(shellScript, /function installDashboardPageScroll\(\)/);
  assert.match(shellScript, /new MutationObserver\(sync\)\.observe\(pipeline/);
  assert.match(shellScript, /document\.documentElement\.classList\.toggle\('pf-pipeline-page-scroll', active\)/);
  assert.match(shellScript, /document\.body\.classList\.toggle\('pf-pipeline-page-scroll', active\)/);
  assert.match(shellScript, /installDashboardPageScroll\(\);\s*\n\s*activateHashTab\(\);/);
});
