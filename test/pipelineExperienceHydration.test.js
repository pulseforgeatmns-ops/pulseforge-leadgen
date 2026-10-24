'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const {
  PIPELINE_EXPERIENCE,
  featuresFromFlag,
  resolvePipelineExperience,
  isPipelineTabVisible,
} = require('../utils/pipelineExperience');

test('featuresFromFlag is the canonical tenant-feature shape', () => {
  assert.deepEqual(featuresFromFlag(10, true), {
    client_id: 10,
    setter_pipeline_v2_enabled: true,
    pipeline_experience: 'pilot_v2',
  });
  assert.deepEqual(featuresFromFlag(1, false), {
    client_id: 1,
    setter_pipeline_v2_enabled: false,
    pipeline_experience: 'legacy',
  });
  assert.equal(featuresFromFlag(10, 'true').setter_pipeline_v2_enabled, false);
});

test('Anchor true resolves to pilot_v2 and stays stable for matching client', () => {
  const resolved = resolvePipelineExperience(
    { client_id: 10, setter_pipeline_v2_enabled: true, pipeline_experience: 'pilot_v2' },
    { expectedClientId: 10, requestId: 2, activeRequestId: 2 }
  );
  assert.equal(resolved.stale, false);
  assert.equal(resolved.enabled, true);
  assert.equal(resolved.experience, PIPELINE_EXPERIENCE.PILOT_V2);
});

test('stale request ids are ignored so client-1 cannot overwrite Anchor', () => {
  const stale = resolvePipelineExperience(
    { client_id: 1, setter_pipeline_v2_enabled: false },
    { expectedClientId: 10, requestId: 1, activeRequestId: 2 }
  );
  assert.equal(stale.stale, true);
  assert.equal(stale.reason, 'stale_request');
});

test('mismatched client_id responses are ignored during hydration', () => {
  const mismatch = resolvePipelineExperience(
    { client_id: 1, setter_pipeline_v2_enabled: false },
    { expectedClientId: 10, requestId: 3, activeRequestId: 3 }
  );
  assert.equal(mismatch.stale, true);
  assert.equal(mismatch.reason, 'client_mismatch');
});

test('missing features fail closed to legacy without becoming pending flash', () => {
  const resolved = resolvePipelineExperience(null, { expectedClientId: 10 });
  assert.equal(resolved.stale, false);
  assert.equal(resolved.enabled, false);
  assert.equal(resolved.experience, PIPELINE_EXPERIENCE.LEGACY);
});

test('pipeline tab visibility requires explicit active panel display', () => {
  assert.equal(isPipelineTabVisible({ inlineDisplay: '', activeTab: 'agents' }), false);
  assert.equal(isPipelineTabVisible({ inlineDisplay: '', activeTab: null }), false);
  assert.equal(isPipelineTabVisible({ inlineDisplay: 'block', activeTab: 'pipeline' }), true);
  assert.equal(isPipelineTabVisible({ inlineDisplay: 'block', activeTab: null }), true);
});

test('dashboard hydration contracts prevent Anchor pilot flash-then-revert', () => {
  const html = fs.readFileSync(path.join(__dirname, '../public/dashboard.html'), 'utf8');
  assert.match(html, /class="pipeline-experience-pending"/);
  assert.match(html, /Resolving client Pipeline experience/);
  assert.match(html, /pipeline-pending-only/);
  assert.match(html, /_clientFeatureMap/);
  assert.match(html, /applyPipelineFeatures/);
  assert.match(html, /isPipelineTabActive/);
  assert.match(html, /stale tenant response/);
  assert.doesNotMatch(
    html,
    /body\.pipeline-legacy \.new-pipeline-only \{ display:none/,
    'legacy-only hide rule must not be the sole default; pending must hide both experiences'
  );
  // Default title must not hardcode Human Setter Ops before hydration.
  assert.match(html, /<div class="area-title">PIPELINE<\/div>/);
  assert.doesNotMatch(
    html,
    /<div class="area-title">HUMAN SETTER OPERATIONS<\/div>/
  );
  // Visibility check must not treat empty inline display as visible.
  assert.doesNotMatch(
    html,
    /pipelineArea\)\?\.style\.display !== 'none'\) loadPipeline\(\)/
  );
});

test('clients API exports include the pilot flag for canonical hydration', () => {
  const api = fs.readFileSync(path.join(__dirname, '../routes/api.js'), 'utf8');
  const clientContext = fs.readFileSync(path.join(__dirname, '../utils/clientContext.js'), 'utf8');
  assert.match(clientContext, /setter_pipeline_v2_enabled/);
  assert.match(api, /featuresFromFlag/);
  assert.match(api, /features: featuresFromFlag/);
});
