'use strict';

/**
 * SPEC-099 — Max Workspace composer viewport layout regression.
 * Guards the height chain that previously clipped Ask Max / Send below
 * the visible modal (content-box panel + second composer scroll region).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const cssPath = path.join(
  __dirname,
  '..',
  'public',
  'command-deck',
  'command-deck.css'
);
const jsPath = path.join(
  __dirname,
  '..',
  'public',
  'command-deck',
  'command-deck.js'
);
const htmlPath = path.join(__dirname, '..', 'public', 'command-deck.html');

const css = fs.readFileSync(cssPath, 'utf8');
const js = fs.readFileSync(jsPath, 'utf8');
const html = fs.readFileSync(htmlPath, 'utf8');

function ruleBody(selector) {
  // Match the first top-level rule for selector (non-greedy until closing brace).
  const escaped = selector.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
  const re = new RegExp(`${escaped}\\s*\\{([^}]*)\\}`);
  const match = css.match(re);
  assert.ok(match, `expected CSS rule for ${selector}`);
  return match[1];
}

describe('SPEC-099 Max composer viewport height chain', () => {
  it('keeps modal DOM structure: header → thread → composer dock', () => {
    const panelIdx = html.indexOf('class="mx-panel"');
    const headerIdx = html.indexOf('class="mx-header"', panelIdx);
    const threadIdx = html.indexOf('id="mxThread"', panelIdx);
    const dockIdx = html.indexOf('class="mx-composer-dock"', panelIdx);
    const formIdx = html.indexOf('id="mxAskForm"', panelIdx);
    assert.ok(panelIdx > 0);
    assert.ok(headerIdx > panelIdx);
    assert.ok(threadIdx > headerIdx);
    assert.ok(dockIdx > threadIdx);
    assert.ok(formIdx > dockIdx);
  });

  it('panel uses border-box and stays within parent height', () => {
    const body = ruleBody('.mx-panel');
    assert.match(body, /box-sizing:\s*border-box/);
    assert.match(body, /overflow:\s*hidden/);
    assert.match(body, /display:\s*flex/);
    assert.match(body, /flex-direction:\s*column/);
    assert.match(body, /min-height:\s*0/);
    // Must not use content-box height:100% + max-height:100dvh without border-box
    // (that pattern made the padded panel taller than the viewport).
    assert.doesNotMatch(body, /max-height:\s*100dvh/);
    assert.match(body, /max-height:\s*100%/);
  });

  it('workspace clips to viewport and does not introduce page scroll for the modal', () => {
    const body = ruleBody('.mx-workspace');
    assert.match(body, /position:\s*fixed/);
    assert.match(body, /inset:\s*0/);
    assert.match(body, /overflow:\s*hidden/);
  });

  it('conversation is the only primary flex scroll region', () => {
    const thread = ruleBody('.mx-thread');
    assert.match(thread, /flex:\s*1\s+1\s+0%/);
    assert.match(thread, /min-height:\s*0/);
    assert.match(thread, /overflow-y:\s*auto/);

    const dock = ruleBody('.mx-composer-dock');
    assert.match(dock, /flex:\s*0\s+0\s+auto/);
    assert.doesNotMatch(dock, /overflow-y:\s*auto/);
    assert.match(dock, /overflow:\s*hidden/);
  });

  it('suggestions are bounded without a second vertical scrollbar', () => {
    const suggestions = ruleBody('.mx-suggestions');
    assert.match(suggestions, /max-height:/);
    assert.match(suggestions, /overflow:\s*hidden/);
    assert.doesNotMatch(suggestions, /overflow-y:\s*auto/);
  });

  it('ask form stays flex-fixed so input+send remain visible', () => {
    const form = ruleBody('.mx-ask-form');
    assert.match(form, /flex:\s*0\s+0\s+auto/);
    const input = ruleBody('.mx-ask-input');
    assert.match(input, /max-height:\s*min\(5\.5rem,\s*14vh\)/);
  });

  it('modal auto-grow caps input height below the old 220px push-off-screen band', () => {
    assert.match(js, /MX_ASK_MAX_PX\s*=\s*88/);
    assert.doesNotMatch(js, /MX_ASK_MAX_PX\s*=\s*220/);
  });
});
