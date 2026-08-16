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

describe('SPEC-097B Spatial Composition Polish', () => {
  it('builds Max halo from oval rings rather than a rectangular inset glow', () => {
    assert.match(html, /cd-max-halo-ring-inner/);
    assert.match(html, /cd-max-halo-ring-mid/);
    assert.match(html, /cd-max-halo-ring-outer/);
    assert.match(css, /\.cd-max-halo-ring\s*\{[^}]*border-radius:\s*50%/s);
    assert.match(css, /radial-gradient\(\s*ellipse at 50% 50%/);
    assert.doesNotMatch(css, /\.cd-spatial-max-halo\s*\{[^}]*inset:\s*-18px/s);
    assert.doesNotMatch(css, /\.cd-spatial-max-halo\s*\{[^}]*radial-gradient\(\s*circle/s);
  });

  it('expands priority bands and keeps monitored > normal > elevated > urgent', () => {
    assert.match(spatialJs, /monitored:\s*280/);
    assert.match(spatialJs, /normal:\s*235/);
    assert.match(spatialJs, /elevated:\s*178/);
    assert.match(spatialJs, /urgent:\s*142/);
    assert.match(spatialJs, /computeFieldLayout/);
    assert.match(spatialJs, /PROTECTED_GAP_PX/);
    assert.match(css, /--cd-orbit-radius:\s*min\(48vw, 360px\)/);
  });

  it('synthesizes Max aggregate state instead of repeating domain counts', () => {
    assert.match(spatialJs, /function synthesizeMaxCopy/);
    assert.match(spatialJs, /Watching \$\{areaCount\}/);
    assert.match(spatialJs, /needs your attention/);
    assert.match(spatialJs, /function nodeSummaryText/);
    assert.match(spatialJs, /historical\|contained/);
  });

  it('terminates connections at Max halo and domain node edges', () => {
    assert.match(spatialJs, /function ellipseEdge/);
    assert.match(spatialJs, /function rectEdge/);
    assert.match(spatialJs, /HALO_EXTENT_PX/);
  });

  it('reuses domain nodes so priority travel can animate along the expanded orbit', () => {
    assert.match(spatialJs, /@property --cd-node-x|--cd-node-x/);
    assert.match(css, /@property --cd-node-x/);
    assert.match(spatialJs, /nodeByDomain\.get\(domain\.id\)/);
    assert.match(spatialJs, /cd-spatial-node-placing/);
  });

  it('pulses a restrained Max-to-domain signal for sustained urgent state', () => {
    assert.match(spatialJs, /SIGNAL_MS = 8000/);
    assert.match(spatialJs, /function syncSignalLoop/);
    assert.match(spatialJs, /function travelSignal/);
    assert.match(spatialJs, /function pulseMaxHalo/);
    assert.match(css, /cd-max-signaling/);
  });

  it('keeps halo and signal static under reduced motion', () => {
    assert.match(css, /prefers-reduced-motion: reduce/);
    assert.match(css, /cd-max-halo-ring-outer/);
    assert.match(spatialJs, /if \(reducedMotion\(\) \|\| listView\) return/);
  });
});
