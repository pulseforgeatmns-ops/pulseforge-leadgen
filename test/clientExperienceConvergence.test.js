'use strict';

/**
 * SPEC-099 — Client Experience Convergence tests.
 * Presentation + tenant-safe identity; does not weaken SPEC-096 auth.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  softenClientFacingProse,
  presentMaxResultForClient,
  workspaceDisplayName,
} = require('../utils/clientFacingPresentation');

const shellPath = path.join(__dirname, '..', 'public', 'shared', 'shell.js');
const dashPath = path.join(__dirname, '..', 'public', 'dashboard.html');
const ciePath = path.join(__dirname, '..', 'public', 'client-intel.html');
const mxCssPath = path.join(
  __dirname,
  '..',
  'public',
  'command-deck',
  'command-deck.css'
);
const apiPath = path.join(__dirname, '..', 'routes', 'api.js');
const maxWsPath = path.join(__dirname, '..', 'routes', 'maxWorkspace.js');

const shellSource = fs.readFileSync(shellPath, 'utf8');
const dashSource = fs.readFileSync(dashPath, 'utf8');
const cieSource = fs.readFileSync(ciePath, 'utf8');
const mxCss = fs.readFileSync(mxCssPath, 'utf8');
const apiSource = fs.readFileSync(apiPath, 'utf8');
const maxWsSource = fs.readFileSync(maxWsPath, 'utf8');

describe('SPEC-099 client facing presentation helpers', () => {
  it('workspaceDisplayName strips Co. for AS Cleaning', () => {
    assert.equal(
      workspaceDisplayName({ name: 'AS Cleaning Co.', business_name: 'AS Cleaning Co.' }),
      'AS Cleaning'
    );
  });

  it('softens architectural jargon without removing evidence vocabulary', () => {
    const prose =
      'Based on SPEC-093 and the ContextEnvelope, CIE shows Mission Plan IR ' +
      'in the execution domain with Discovery Profile version 2. Evidence and confidence remain.';
    const out = softenClientFacingProse(prose);
    assert.doesNotMatch(out, /SPEC-093/);
    assert.doesNotMatch(out, /ContextEnvelope/);
    assert.doesNotMatch(out, /\bCIE\b/);
    assert.doesNotMatch(out, /Mission Plan IR/);
    assert.doesNotMatch(out, /execution domain/i);
    assert.match(out, /Evidence/);
    assert.match(out, /confidence/);
  });

  it('presentMaxResultForClient softens prose fields only', () => {
    const result = presentMaxResultForClient({
      prose: 'See SPEC-098 ContextEnvelope for CIE.',
      structured: { answer: 'CIE approved', evidence: [{ id: 'e1' }] },
      metadata: { confidence: 0.8 },
    });
    assert.doesNotMatch(result.prose, /SPEC-098/);
    assert.doesNotMatch(result.structured.answer, /\bCIE\b/);
    assert.equal(result.structured.evidence[0].id, 'e1');
    assert.equal(result.metadata.confidence, 0.8);
  });
});

describe('SPEC-099 shell identity + navigation', () => {
  it('client nav uses Max and My Business labels', () => {
    assert.match(shellSource, /clientLabel:\s*'Max'/);
    assert.match(shellSource, /clientLabel:\s*'My Business'/);
    assert.match(shellSource, /pf-nav-workspace/);
    assert.match(shellSource, /\/api\/me/);
    assert.match(shellSource, /display_name/);
  });

  it('api/me returns authoritative client workspace context', () => {
    assert.match(apiSource, /workspaceDisplayName/);
    assert.match(apiSource, /getClientConfig/);
    assert.match(apiSource, /display_name/);
    assert.match(apiSource, /role === 'client'/);
  });
});

describe('SPEC-099 client Home + CIE presentation', () => {
  it('hides operator surfaces and Anchor sample copy for clients', () => {
    assert.match(dashSource, /client-role/);
    assert.match(dashSource, /clientHomePanel/);
    assert.match(dashSource, /Start with Max/);
    assert.match(dashSource, /Continue with Max/);
    assert.match(dashSource, /loadClientHomeOnboarding/);
    assert.doesNotMatch(
      dashSource,
      /No Growth Plans yet — start an interview or load the Anchor sample/
    );
    // Anchor sample CTA remains for operators only
    assert.match(dashSource, /fixturesAllowed/);
    assert.match(dashSource, /_currentUserRole !== 'client'/);
  });

  it('CIE presents My Business for clients and includes Max visual tokens', () => {
    assert.match(cieSource, /My Business/);
    assert.match(cieSource, /Newsreader/);
    assert.match(cieSource, /--pf-gold/);
    assert.match(cieSource, /applyClientRoleUi/);
    assert.match(cieSource, /Start with Max/);
    assert.match(cieSource, /Continue with Max/);
    assert.match(cieSource, /shell\.js/);
  });
});

describe('SPEC-099 Max workspace composer layout', () => {
  it('composer dock does not create a second constrained scroll region', () => {
    // Extract the .mx-composer-dock rule body
    const match = mxCss.match(/\.mx-composer-dock\s*\{([^}]+)\}/);
    assert.ok(match, 'expected .mx-composer-dock rule');
    const body = match[1];
    assert.doesNotMatch(body, /overflow-y:\s*auto/);
    assert.match(body, /max-height:\s*none/);
    assert.match(mxCss, /\.mx-thread[\s\S]*?overflow-y:\s*auto/);
  });

  it('max workspace route applies client presentation boundary', () => {
    assert.match(maxWsSource, /presentMaxResultForClient/);
    assert.match(maxWsSource, /isClientRole/);
  });
});
