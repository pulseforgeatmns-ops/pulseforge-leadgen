'use strict';

/**
 * Content Outcome Intelligence tests (SPEC-092 / planning draft SPEC-085).
 */

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const path = require('path');
const express = require('express');

const {
  createContentOutcomeService,
  createMemoryContentOutcomeStore,
  ContentOutcomeError,
  CHANNELS,
  BUSINESS_OUTCOME_TYPES,
  ATTRIBUTION_LEVELS,
} = require('../services/contentOutcome');
const { createOutcomeEngine, LIFECYCLE } = require('../packages/max/outcome');
const { buildComparisonSummary, median, average } = require('../services/contentOutcome/aggregates');

describe('Content Outcome types & aggregates', () => {
  it('computes median and average without inventing vanity scores', () => {
    assert.equal(median([1, 3, 2]), 2);
    assert.equal(median([1, 2, 3, 4]), 2.5);
    assert.equal(average([2, 4]), 3);
    assert.equal(median([]), null);
  });

  it('builds deterministic comparison summaries', () => {
    const pubs = [{ id: 'a' }, { id: 'b' }];
    const snaps = [
      { publication_id: 'a', observed_at: '2026-08-01T00:00:00Z', impressions: 100, comments: 2 },
      { publication_id: 'a', observed_at: '2026-08-02T00:00:00Z', impressions: 300, comments: 5 },
      { publication_id: 'b', observed_at: '2026-08-01T00:00:00Z', impressions: 50, comments: 1 },
    ];
    const outcomes = [
      { publication_id: 'a', outcome_type: 'partner_conversation' },
      { publication_id: 'a', outcome_type: 'meeting_booked' },
      { publication_id: 'b', outcome_type: 'prospect_conversation' },
    ];
    const summary = buildComparisonSummary(pubs, snaps, outcomes);
    assert.equal(summary.total_publications, 2);
    assert.equal(summary.median_impressions, 175); // latest snaps: 300 and 50
    assert.equal(summary.total_partner_conversations, 1);
    assert.equal(summary.total_meetings, 1);
    assert.equal(summary.total_qualified_conversations, 1);
  });
});

describe('Content Outcome service (memory)', () => {
  /** @type {ReturnType<typeof createContentOutcomeService>} */
  let service;
  /** @type {ReturnType<typeof createOutcomeEngine>} */
  let outcomeEngine;
  let paigeMutated;

  beforeEach(() => {
    paigeMutated = false;
    outcomeEngine = createOutcomeEngine();
    service = createContentOutcomeService({
      store: createMemoryContentOutcomeStore(),
      outcomeEngine,
      knowledge: {
        async writeEvidence(input) {
          return { id: `ev:${input.sourceType}:${Date.now()}`, ...input };
        },
      },
    });
    // Guardrail probe: recording outcomes must not touch Paige config.
    Object.defineProperty(global, '__paigeConfigProbe', {
      configurable: true,
      set() {
        paigeMutated = true;
      },
      get() {
        return null;
      },
    });
  });

  it('creates a publication from a Paige-style artifact', async () => {
    const { publication, artifact } = await service.createPublication({
      client_id: 1,
      pending_comment_id: '42',
      title: 'Software should learn you',
      body: 'Operators deserve software that adapts.',
      channel: 'linkedin',
      objective: 'thought_leadership',
      topic: 'software should learn you',
      thesis: 'Software should learn the operator',
      format: 'text',
      intended_audience: ['builders', 'SMB operators'],
      external_url: 'https://www.linkedin.com/posts/example',
      published_at: '2026-08-01T12:00:00.000Z',
    });

    assert.ok(publication.id);
    assert.equal(publication.tenant_id, '1');
    assert.equal(publication.channel, CHANNELS.LINKEDIN);
    assert.equal(publication.objective, 'thought_leadership');
    assert.equal(artifact.pending_comment_id, '42');
    assert.equal(artifact.metadata.source, 'paige');
  });

  it('stores multiple immutable performance snapshots with partial metrics', async () => {
    const { publication } = await service.createPublication({
      client_id: 1,
      title: 'Breakout',
      channel: 'linkedin',
      published_at: '2026-08-01T12:00:00.000Z',
    });

    const s1 = await service.addPerformanceSnapshot('1', publication.id, {
      observed_at: '2026-08-01T16:00:00.000Z',
      impressions: 8400,
    });
    const s2 = await service.addPerformanceSnapshot('1', publication.id, {
      observed_at: '2026-08-02T12:00:00.000Z',
      impressions: 21300,
      reactions: 410,
      comments: 77,
    });
    const s3 = await service.addPerformanceSnapshot('1', publication.id, {
      observed_at: '2026-08-08T12:00:00.000Z',
      impressions: 31900,
      reactions: 520,
      comments: 91,
      followers_gained: 140,
    });

    assert.notEqual(s1.id, s2.id);
    assert.notEqual(s2.id, s3.id);

    const full = await service.getPublicationOutcome('1', publication.id);
    assert.equal(full.performance_snapshots.length, 3);
    assert.equal(full.performance_snapshots[0].impressions, 8400);
    assert.equal(full.performance_snapshots[2].impressions, 31900);
    // Partial metrics stay null — never invent zeros as facts.
    assert.equal(full.performance_snapshots[0].reactions, null);
  });

  it('records multiple business outcomes with attribution preserved', async () => {
    const { publication } = await service.createPublication({
      client_id: 1,
      title: 'Partner post',
      channel: 'linkedin',
      published_at: '2026-08-01T12:00:00.000Z',
    });

    const o1 = await service.addBusinessOutcome('1', publication.id, {
      outcome_type: 'partner_conversation',
      attribution: 'direct',
      description: 'Muhammad messaged after seeing the post',
      person_id: 'person:muhammad',
      confidence: 0.9,
    });
    const o2 = await service.addBusinessOutcome('1', publication.id, {
      outcome_type: 'prospect_conversation',
      attribution: 'possible',
      description: 'Inbound three days later; may have seen content',
    });

    assert.equal(o1.attribution, 'direct');
    assert.equal(o2.attribution, 'possible');
    assert.ok(o1.evidence_id);
    assert.ok(BUSINESS_OUTCOME_TYPES.includes(o1.outcome_type));
    assert.ok(ATTRIBUTION_LEVELS.includes(o1.attribution));

    const full = await service.getPublicationOutcome('1', publication.id);
    assert.equal(full.business_outcomes.length, 2);
    assert.ok(full.evidence_references.length >= 1);
  });

  it('persists qualitative signals as observations, not conclusions', async () => {
    const { publication } = await service.createPublication({
      client_id: 1,
      title: 'Resonance',
      channel: 'linkedin',
      published_at: '2026-08-01T12:00:00.000Z',
    });

    const signal = await service.addQualitativeSignal('1', publication.id, {
      signal_type: 'language_adoption',
      description: "Several commenters repeated the phrase 'software should learn you.'",
      audience_type: 'builders',
    });

    assert.equal(signal.signal_type, 'language_adoption');
    const full = await service.getPublicationOutcome('1', publication.id);
    assert.equal(full.qualitative_signals.length, 1);
    assert.ok(full.timeline.some((e) => e.kind === 'qualitative_signal'));
  });

  it('enforces tenant isolation', async () => {
    const a = await service.createPublication({
      client_id: 1,
      title: 'Tenant A',
      channel: 'linkedin',
      published_at: '2026-08-01T12:00:00.000Z',
    });
    const b = await service.createPublication({
      client_id: 2,
      title: 'Tenant B',
      channel: 'linkedin',
      published_at: '2026-08-01T12:00:00.000Z',
    });

    await assert.rejects(
      () => service.getPublicationOutcome('1', b.publication.id),
      (err) => err instanceof ContentOutcomeError && err.code === 'publication_not_found'
    );
    await assert.rejects(
      () => service.addPerformanceSnapshot('2', a.publication.id, { impressions: 10 }),
      (err) => err instanceof ContentOutcomeError && err.code === 'publication_not_found'
    );

    const listA = await service.listContentOutcomes({ tenantId: '1' });
    assert.equal(listA.items.length, 1);
    assert.equal(listA.items[0].publication.id, a.publication.id);
  });

  it('retrieves complete history and recent list for intelligence consumers', async () => {
    const { publication } = await service.createPublication({
      client_id: 1,
      title: 'Launch runway',
      objective: 'launch_runway',
      topic: 'Max reveal',
      channel: 'linkedin',
      published_at: '2026-08-01T12:00:00.000Z',
    });
    await service.addPerformanceSnapshot('1', publication.id, {
      impressions: 12000,
      comments: 40,
      observed_at: '2026-08-02T12:00:00.000Z',
    });
    await service.addBusinessOutcome('1', publication.id, {
      outcome_type: 'meeting_booked',
      attribution: 'likely',
    });
    await service.addQualitativeSignal('1', publication.id, {
      signal_type: 'buyer_signal',
      description: 'SMB operators asked about pilot timing',
    });

    const full = await service.getPublicationOutcome('1', publication.id);
    assert.equal(full.performance_snapshots.length, 1);
    assert.equal(full.business_outcomes.length, 1);
    assert.equal(full.qualitative_signals.length, 1);
    assert.ok(full.content_artifact);

    const recent = await service.getRecentContentOutcomes('1', 5);
    assert.equal(recent.items.length, 1);
    assert.equal(recent.comparison.total_meetings, 1);
    assert.ok(recent.grouped.objective.launch_runway);
  });

  it('integrates with SPEC-013 OutcomeEngine without mutating Paige strategy', async () => {
    const { publication } = await service.createPublication({
      client_id: 1,
      title: 'SPEC-013 bridge',
      channel: 'linkedin',
      published_at: '2026-08-01T12:00:00.000Z',
    });

    const existing = outcomeEngine.get('1', `content-pub:${publication.id}`);
    assert.ok(existing);
    assert.equal(existing.lifecycle, LIFECYCLE.EXECUTED);
    assert.equal(existing.meta.kind, 'content_publication');

    await service.addBusinessOutcome('1', publication.id, {
      outcome_type: 'builder_connection',
      attribution: 'direct',
      description: 'Builder reached out',
    });

    // No Paige mutation side effect from recording outcomes.
    assert.equal(paigeMutated, false);
  });

  it('rejects invalid attribution rather than inventing certainty', async () => {
    const { publication } = await service.createPublication({
      client_id: 1,
      title: 'Attribution',
      channel: 'linkedin',
      published_at: '2026-08-01T12:00:00.000Z',
    });
    await assert.rejects(
      () =>
        service.addBusinessOutcome('1', publication.id, {
          outcome_type: 'partner_conversation',
          attribution: 'guaranteed',
        }),
      (err) => err instanceof ContentOutcomeError && err.code === 'invalid_enum'
    );
  });
});

describe('Content Outcome routes (static + smoke)', () => {
  it('registers required endpoints and mounts from server.js', () => {
    const routePath = path.join(__dirname, '..', 'routes', 'contentOutcome.js');
    const serverPath = path.join(__dirname, '..', 'server.js');
    const uiPath = path.join(__dirname, '..', 'public', 'content-outcome.html');
    const packagePath = path.join(__dirname, '..', 'package.json');

    const routeSrc = fs.readFileSync(routePath, 'utf8');
    const serverSrc = fs.readFileSync(serverPath, 'utf8');
    const uiSrc = fs.readFileSync(uiPath, 'utf8');
    const pkg = JSON.parse(fs.readFileSync(packagePath, 'utf8'));

    const expected = [
      '/api/content-publications',
      '/api/content-publications/:id',
      '/api/content-publications/:id/performance',
      '/api/content-publications/:id/outcomes',
      '/api/content-publications/:id/signals',
      '/api/content-outcomes',
      '/api/v1/content-outcomes/recent',
      '/content-outcome',
    ];
    for (const route of expected) {
      assert.match(routeSrc, new RegExp(route.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')));
    }
    assert.match(serverSrc, /require\('\.\/routes\/contentOutcome'\)/);
    assert.match(uiSrc, /Content Outcomes/);
    assert.equal(pkg.scripts['content:outcome'], 'node scripts/contentOutcome.js');
  });

  it('serves create + retrieve over HTTP with tenant scoping', async () => {
    const store = createMemoryContentOutcomeStore();
    const service = createContentOutcomeService({ store });

    // Lightweight router mirror (avoids full auth stack).
    const app = express();
    app.use(express.json());
    app.post('/api/content-publications', async (req, res) => {
      try {
        const clientId = Number(req.query.client_id || req.body.client_id);
        const result = await service.createPublication({
          ...req.body,
          client_id: clientId,
          tenant_id: String(clientId),
        });
        res.status(201).json(result);
      } catch (err) {
        res.status(err.status || 400).json({ error: err.code, message: err.message });
      }
    });
    app.get('/api/content-publications/:id/outcomes', async (req, res) => {
      try {
        const clientId = String(req.query.client_id);
        const full = await service.getPublicationOutcome(clientId, req.params.id);
        res.json(full);
      } catch (err) {
        res.status(err.status || 400).json({ error: err.code, message: err.message });
      }
    });

    const server = await new Promise((resolve) => {
      const s = app.listen(0, '127.0.0.1', () => resolve(s));
    });
    const { port } = server.address();
    const base = `http://127.0.0.1:${port}`;

    try {
      const createRes = await fetch(`${base}/api/content-publications?client_id=1`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          title: 'HTTP post',
          channel: 'linkedin',
          objective: 'awareness',
          published_at: '2026-08-01T12:00:00.000Z',
        }),
      });
      assert.equal(createRes.status, 201);
      const created = await createRes.json();

      const other = await fetch(
        `${base}/api/content-publications/${created.publication.id}/outcomes?client_id=2`
      );
      assert.equal(other.status, 404);

      const ok = await fetch(
        `${base}/api/content-publications/${created.publication.id}/outcomes?client_id=1`
      );
      assert.equal(ok.status, 200);
      const body = await ok.json();
      assert.equal(body.publication.id, created.publication.id);
    } finally {
      await new Promise((r) => server.close(r));
    }
  });
});

describe('Migration presence', () => {
  it('ships additive SQL + rollback', () => {
    const sql = path.join(
      __dirname,
      '..',
      'migrations',
      '2026-08-13-content-outcome-intelligence.sql'
    );
    const rollback = path.join(
      __dirname,
      '..',
      'migrations',
      '2026-08-13-content-outcome-intelligence.rollback.sql'
    );
    assert.equal(fs.existsSync(sql), true);
    assert.equal(fs.existsSync(rollback), true);
    const text = fs.readFileSync(sql, 'utf8');
    assert.match(text, /content_artifacts/);
    assert.match(text, /content_publications/);
    assert.match(text, /content_performance_snapshots/);
    assert.match(text, /content_business_outcomes/);
    assert.match(text, /content_qualitative_signals/);
    assert.doesNotMatch(text, /DROP TABLE/i);
  });
});
