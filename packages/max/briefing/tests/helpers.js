'use strict';

const { createKnowledgeRuntime, NODE_TYPES } = require('../../../knowledge');
const { createMaxReasoningRuntime } = require('../..');
const { snapshot, strat } = require('../../memory/tests/helpers');
const { WATCH_OPS } = require('../../memory/snapshots/MemoryTypes');

const TENANT = '10';
const AS_OF = '2026-07-26T12:00:00.000Z';

/**
 * Seed N companies into knowledge + optional memory snapshots.
 */
async function seedTenant(options = {}) {
  const companyCount = options.companyCount != null ? options.companyCount : 3;
  const withMemory = options.withMemory !== false;
  const runtime = createKnowledgeRuntime({ withSync: false, startIngestor: false });
  const knowledge = runtime.knowledge;
  const max = createMaxReasoningRuntime({ knowledge });

  const companies = [];
  for (let i = 0; i < companyCount; i += 1) {
    const co = await knowledge.createNode({
      tenantId: TENANT,
      type: NODE_TYPES.COMPANY,
      name: `Company ${String(i).padStart(3, '0')}`,
      metadata: { industry: 'cleaning', confidence: 0.7 },
    });
    companies.push(co);
  }

  if (withMemory) {
    for (let i = 0; i < companies.length; i += 1) {
      const co = companies[i];
      const baseScore = 40 + i * 10;
      const t0 = '2026-07-20T12:00:00.000Z';
      const t1 = '2026-07-25T12:00:00.000Z';

      await max.memory.repository.append(
        snapshot({
          tenantId: TENANT,
          companyId: co.id,
          name: co.name,
          timestamp: t0,
          score: baseScore,
          confidence: 50,
          type: baseScore >= 70 ? 'pursue' : 'follow_up',
          priority: baseScore >= 70 ? 'high' : 'medium',
          claims: [`claim-a-${i}`],
          evidence: [`ev-a-${i}`],
          supportingSignals: [
            { kind: 'evidence', id: `ev-a-${i}`, summary: `signal-${i}` },
          ],
          opposingSignals:
            i === 0
              ? [{ kind: 'evidence', id: 'oppose-0', summary: 'contradiction' }]
              : [],
        })
      );

      const scoreAfter = baseScore + (i % 2 === 0 ? 15 : -12);
      const confAfter = i === 0 ? 35 : 55;
      await max.memory.repository.append(
        snapshot({
          tenantId: TENANT,
          companyId: co.id,
          name: co.name,
          timestamp: t1,
          score: scoreAfter,
          confidence: confAfter,
          type: scoreAfter >= 70 ? 'pursue' : scoreAfter >= 55 ? 'follow_up' : 'nurture',
          priority: scoreAfter >= 70 ? 'high' : scoreAfter >= 55 ? 'medium' : 'low',
          claims:
            i === 1
              ? [`claim-a-${i}`, `claim-hire-${i}`]
              : [`claim-a-${i}`],
          evidence: [`ev-a-${i}`, `ev-b-${i}`],
          supportingSignals: [
            { kind: 'evidence', id: `ev-a-${i}`, summary: `signal-${i}` },
            { kind: 'evidence', id: `ev-b-${i}`, summary: `signal-b-${i}` },
          ],
          opposingSignals:
            i === 0
              ? [
                  { kind: 'evidence', id: 'oppose-0', summary: 'contradiction' },
                  { kind: 'evidence', id: 'oppose-1', summary: 'new_contradiction' },
                ]
              : [],
          strategyResults: [
            strat('opportunity', i % 2 === 0 ? 30 : 5, 50, {
              supportingEvidence:
                i === 1
                  ? [
                      {
                        kind: 'evidence',
                        id: `ev-hire-${i}`,
                        summary: 'Hiring Operations Manager',
                      },
                    ]
                  : [],
            }),
            strat('decision_maker', i === 1 ? 25 : 10, 50, {
              supportingEvidence:
                i === 1
                  ? [
                      {
                        kind: 'evidence',
                        id: `ev-dm-${i}`,
                        summary: 'Decision-maker: New Ops Manager',
                      },
                    ]
                  : [],
            }),
            strat('relationship', 10, 40),
            strat('engagement', 10, 40),
            strat('technology', 5, 40),
            strat('overflow', 5, 40),
            strat('risk', i === 0 ? -20 : -5, 40),
          ],
        })
      );
    }
  }

  return { knowledge, max, companies, tenantId: TENANT, asOf: AS_OF };
}

/**
 * Register a score-delta watch for each company.
 */
function registerScoreWatches(max, companies) {
  for (const co of companies) {
    max.memory.watch({
      tenantId: TENANT,
      targetType: 'company',
      targetId: co.id,
      condition: { op: WATCH_OPS.DELTA_ABS_GT, field: 'score', value: 10 },
    });
  }
}

module.exports = {
  TENANT,
  AS_OF,
  seedTenant,
  registerScoreWatches,
};
