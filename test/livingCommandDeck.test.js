'use strict';

/**
 * SPEC-097 Living Command Deck — UI contract regression tests.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const root = path.join(__dirname, '..');
const html = fs.readFileSync(path.join(root, 'public', 'command-deck.html'), 'utf8');
const css = fs.readFileSync(path.join(root, 'public', 'command-deck', 'command-deck.css'), 'utf8');
const spatialJs = fs.readFileSync(
  path.join(root, 'public', 'command-deck', 'spatial-deck.js'),
  'utf8'
);
const deckJs = fs.readFileSync(
  path.join(root, 'public', 'command-deck', 'command-deck.js'),
  'utf8'
);

describe('SPEC-097 Living Command Deck UI contracts', () => {
  it('includes spatial deck shell, domain drawer, and list fallback controls', () => {
    assert.match(html, /id="cdSpatialDeck"/);
    assert.match(html, /id="cdSpatialOrbit"/);
    assert.match(html, /id="cdSpatialMax"/);
    assert.match(html, /id="cdViewToggle"/);
    assert.match(html, /id="cdDomainDrawer"/);
    assert.match(html, /id="cdPriorityExplain"/);
    assert.match(html, /spatial-deck\.js/);
  });

  it('defines gold intelligence edge and priority motion styles', () => {
    assert.match(css, /\.cd-spatial-node\.cd-intelligence-active/);
    assert.match(css, /\.cd-priority-/);
    assert.match(css, /prefers-reduced-motion/);
    assert.match(css, /\.cd-spatial-list/);
  });

  it('exposes render-only SpatialDeck API without client-side ranking', () => {
    assert.match(spatialJs, /window\.SpatialDeck/);
    assert.match(spatialJs, /function render\(overview\)/);
    assert.doesNotMatch(spatialJs, /sort\s*\(\s*\(.*score/s);
  });

  it('integrates spatial overview into command-deck render path', () => {
    assert.match(deckJs, /renderSpatialOverview/);
    assert.match(deckJs, /model\.spatialOverview/);
    assert.match(deckJs, /SpatialDeck\.init/);
    assert.match(deckJs, /domainId/);
  });

  it('mobile fallback hides spatial canvas under narrow viewport', () => {
    assert.match(css, /@media \(max-width: 640px\)/);
    assert.match(css, /\.cd-spatial-canvas/);
  });
});
