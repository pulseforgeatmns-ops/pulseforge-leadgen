'use strict';

/**
 * SPEC-097 / SPEC-097A Living Command Deck — UI contract regression tests.
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

describe('SPEC-097A Intelligence Field UI contracts', () => {
  it('includes intelligence field layers — background waves and connection SVG', () => {
    assert.match(html, /cd-intel-field/);
    assert.match(html, /id="cdIntelConnections"/);
    assert.match(html, /cd-spatial-max-halo/);
    assert.match(html, /cd-spatial-max-core/);
  });

  it('uses compact field header instead of Command Deck masthead', () => {
    assert.match(html, /cd-field-header/);
    assert.match(html, /id="cdFieldLabel"/);
    assert.match(html, /id="cdFieldState"/);
    assert.doesNotMatch(html, /Command Deck<\/p>/);
  });

  it('mutually excludes spatial canvas and list view via CSS', () => {
    assert.match(css, /\.cd-spatial-deck:not\(\.cd-spatial-list-mode\) \.cd-spatial-list/);
    assert.match(css, /\.cd-spatial-deck\.cd-spatial-list-mode \.cd-spatial-canvas/);
  });

  it('defines priority band visual treatments and Max anchor styles', () => {
    assert.match(css, /\.cd-priority-urgent/);
    assert.match(css, /\.cd-priority-elevated/);
    assert.match(css, /\.cd-priority-monitored/);
    assert.match(css, /\.cd-spatial-max-core/);
    assert.match(css, /\.cd-intel-conn/);
  });

  it('renders intelligence connections and elevation signal travel in spatial-deck.js', () => {
    assert.match(spatialJs, /renderConnections/);
    assert.match(spatialJs, /cd-intel-conn/);
    assert.match(spatialJs, /cd-spatial-node-elevating/);
    assert.match(spatialJs, /cd-spatial-node-settling/);
    assert.match(spatialJs, /cd-intel-paused/);
  });

  it('supports domain focus and contextual Ask Max placeholder', () => {
    assert.match(spatialJs, /setDomainFocus/);
    assert.match(spatialJs, /cd-spatial-node-focused/);
    assert.match(spatialJs, /Ask Max about \$\{domain\.label\}/);
    assert.match(css, /cd-domain-open/);
  });

  it('exposes screen-reader priority semantics on domain nodes', () => {
    assert.match(spatialJs, /buildNodeAriaLabel/);
    assert.match(spatialJs, /cd-spatial-node-priority/);
    assert.match(spatialJs, /New intelligence/);
  });

  it('uses Briefing current status copy in spatial mode', () => {
    assert.match(deckJs, /Briefing current/);
  });

  it('does not render duplicate domain list below spatial canvas in spatial mode', () => {
    assert.match(spatialJs, /syncViewVisibility/);
    assert.match(spatialJs, /els\.canvas\.hidden = listView/);
  });
});
