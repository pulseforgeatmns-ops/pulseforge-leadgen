'use strict';

/**
 * P0-MAX-REASONING-001 PR 5 — frame-directed reasoning (hermetic).
 * The memory and evidence ports are plain-data fakes that follow the PR 3
 * (ConversationLedger) and PR 4 (EvidenceRegistry) contracts; the real
 * modules are exercised in the cross-PR integration run
 * (packages/max/frameReasoning/integration/combinedArchitecture.integration.cjs).
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const { FrameDirectedReasoner, frameReasoningEnabled, selectDomains } = require('../FrameDirectedReasoner');
const { presentReasoning, renderReasoning, claimsUnrecordedExecution } = require('../ReasoningPresentation');
const { validateTurnFrame } = require('../../interpretation/TurnFrame');

const LEVI = '21';
const NOW = new Date('2026-11-18T12:00:00.000Z');
const days = (n) => new Date(NOW.getTime() - n * 86400000).toISOString();
const FIVE = ['p1', 'p2', 'p3', 'p4', 'p5'];

// ---- PR 4-shaped evidence ---------------------------------------------------
function prospectItem(id, f = {}) {
  return {
    ref: `crm_pipeline:prospect_record:${id}`, kind: 'observed', domain: 'crm_pipeline', tenantId: LEVI,
    source: { name: 'prospects', provenance: 'fixture' }, subject: { type: 'prospect_record', id },
    facts: { pipelineStatus: 'warm', setterStatus: 'new', opportunityState: 'open', contactChannels: { email: true, phone: false, linkedin: false },
      lastContactedAt: days(10), eligible: true, engagementTier: 1, ...f.facts },
    observedAt: f.observedAt || days(5), retrievedAt: NOW.toISOString(), freshness: { ageDays: 5, stale: false, staleAfterDays: 30, basis: 'x' }, missing: f.missing || [],
  };
}
function evidencePort(build) {
  const calls = [];
  return {
    calls,
    async retrieve(req) {
      calls.push(JSON.parse(JSON.stringify(req)));
      return build(req);
    },
    diagnose: {
      scout: () => ({ inferences: [{ kind: 'inference', certainty: 'inferred', claim: '2 Scout run(s) failed in the last 7 days.', basis: ['scout_execution:agent_run:scout'] }, { kind: 'inference', certainty: 'inferred', claim: 'unsupported guess', basis: [] }] }),
      emmett: () => ({ inferences: [{ kind: 'inference', certainty: 'inferred', claim: 'The outbound gate is in "maintenance".', basis: ['outbound_authorization:outbound_gate:gate'] }] }),
      rank: () => ({ ranked: [{ ref: 'crm_pipeline:prospect_record:p1', position: 1 }, { ref: 'crm_pipeline:prospect_record:p2', position: 2 }] }),
    },
  };
}
function report(tenantId, domains, extra = {}) {
  return { kind: 'evidence_report', tenantId, refused: null, domains, contradictions: [], unknowns: [], revalidation: null, ...extra };
}
const crmReport = (req) => {
  const ids = req.referents.flatMap((r) => r.memberIds || []);
  const items = (ids.length ? ids : FIVE).map((id, i) => prospectItem(id, { observedAt: days(i + 1), missing: id === 'p2' ? ['contact channel'] : [] }));
  return report(req.tenantId, { crm_pipeline: { status: 'found', items } }, {
    revalidation: ids.length ? { results: ids.map((id) => (id === 'p5' ? { kind: 'prospect', id, status: 'not_found_in_tenant', detail: 'gone' } : { kind: 'prospect', id, status: 'current', evidence: items.find((x) => x.subject.id === id) })) } : null,
  });
};

// ---- PR 3-shaped memory -----------------------------------------------------
function memory({ focus, stack = [], constraints = [], referents = [] }) {
  return {
    focus: focus ? { referent: focus, remembered: true, verified: false, requiresRevalidation: true, evidenceRefreshRequired: false } : { referent: null },
    view: { remembered: { focus: stack, constraints, decisions: [], openQuestions: [] } },
    referents,
  };
}
const set = (memberIds, over = {}) => ({ id: 'r1', kind: 'prospect_set', label: `${memberIds.length} existing prospect records`, scope: 'existing', memberIds, missionId: null, ...over });
const mission = (id, rid) => ({ id: rid, kind: 'mission', label: 'acquisition mission', scope: null, memberIds: [], missionId: id });

// ---- Model-like frames (validated) -------------------------------------------
function frame(over, mem) {
  const candidates = (mem?.referents || []).map((r) => ({ id: r.id, kind: r.kind, label: r.label }));
  return validateTurnFrame({
    intent: 'question', subjectKind: 'crm_pipeline', relationToPrior: 'new_topic', referents: [], evidenceNeeds: [], facets: [],
    requestedOutcome: 'explanation', requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' }, scope: 'focused',
    foreignTenantReference: false, ambiguities: [], confidence: 0.9, ...over,
  }, { tenantId: LEVI, candidates, pendingDecisionId: over.pending || null, producer: 'model' });
}

describe('default-off and boundaries', () => {
  it('is off unless MAX_FRAME_REASONING=true, and nothing in production imports it', () => {
    assert.equal(frameReasoningEnabled({}), false);
    assert.equal(frameReasoningEnabled({ MAX_FRAME_REASONING: 'true' }), true);
    const root = path.resolve(__dirname, '../../../..');
    const target = path.join(root, 'packages/max/frameReasoning');
    const importers = [];
    const walk = (d) => {
      for (const e of fs.readdirSync(d, { withFileTypes: true })) {
        const p = path.join(d, e.name);
        if (/node_modules|\/tests?\b|\/frameReasoning\b/.test(p)) continue;
        if (e.isDirectory()) walk(p);
        else if (e.name.endsWith('.js')) for (const [, spec] of fs.readFileSync(p, 'utf8').matchAll(/require\(\s*['"]([^'"]+)['"]\s*\)/g)) {
          if (spec.startsWith('.') && path.resolve(path.dirname(p), spec).startsWith(target)) importers.push(path.relative(root, p));
        }
      }
    };
    for (const d of ['packages', 'routes', 'services', 'utils']) walk(path.join(root, d));
    assert.deepEqual(importers, []);
    for (const f of ['FrameDirectedReasoner.js', 'ReasoningPresentation.js']) {
      const src = fs.readFileSync(path.join(target, f), 'utf8');
      assert.doesNotMatch(src, /\.query\(|routeExecutionRequest\(|executeMissionStage|resolvePendingOperatorDecision|require\([^)]*(db|acquisition-mission|anchorEmail|emmett|scout)[^)]*\)/i, f);
    }
  });

  it('requires a valid frame for the same tenant; a foreign-tenant frame is refused before any read', async () => {
    const port = evidencePort(crmReport);
    const r = new FrameDirectedReasoner({ evidence: port });
    assert.equal((await r.reason({ tenantId: LEVI, frame: { ...frame({}), authority: { grantsAuthority: true } } })).refusal, 'invalid_frame');
    assert.equal((await r.reason({ tenantId: '10', frame: frame({}) })).refusal, 'invalid_frame');
    const e = await r.reason({ tenantId: LEVI, frame: frame({ foreignTenantReference: true, intent: 'instruction', requestedAction: { verb: 'modify', targetCandidateId: null, polarity: 'affirmative' } }) });
    assert.equal(e.kind, 'refused');
    assert.equal(e.refusal, 'foreign_tenant_reference');
    assert.equal(port.calls.length, 0);
    assert.match(renderReasoning(e), /nothing was read, transferred or changed/);
  });
});

describe('Scenario A — Levi prospect investigation', () => {
  it('A1: five strongest existing prospects with what is missing', async () => {
    const port = evidencePort(crmReport);
    const res = await new FrameDirectedReasoner({ evidence: port }).reason({ tenantId: LEVI, frame: frame({ evidenceNeeds: ['crm_pipeline'], facets: ['recommendation', 'status'] }) });
    assert.equal(res.kind, 'answer');
    assert.deepEqual(port.calls[0].domains, ['crm_pipeline']);
    assert.equal(res.observed.length, 5);
    assert.ok(res.observed.every((o) => o.epistemic === 'verified_observed' && o.tenantId === LEVI && o.retrievedAt));
    assert.ok(res.calculated.some((c) => /recorded engagement tier/.test(c.method)));
    assert.match(renderReasoning(res), /Missing: contact channel/);
  });

  it('A2: "which of those has the freshest information" revalidates exactly the remembered ids', async () => {
    const port = evidencePort(crmReport);
    const mem = memory({ focus: set(FIVE), referents: [set(FIVE)] });
    const res = await new FrameDirectedReasoner({ evidence: port }).reason({ tenantId: LEVI, memory: mem, frame: frame({ relationToPrior: 'follow_up', facets: ['comparison'] }, mem) });
    assert.deepEqual(port.calls[0].referents, [{ kind: 'prospect_set', memberIds: FIVE }]);
    assert.ok(port.calls[0].domains.includes('crm_pipeline'));
    assert.match(res.calculated[0].claim, /record p1/);
    assert.equal(res.revalidation.find((r) => r.id === 'p5').status, 'not_found_in_tenant');
    const prose = renderReasoning(res);
    assert.match(prose, /could not be confirmed[\s\S]*p5/);
    assert.match(prose, /rechecked against current records/);
  });

  it('A3: after "No, I meant the existing prospects" the corrected (existing) subject is read, never new leads', async () => {
    const port = evidencePort(crmReport);
    const corrected = set([], { id: 'r2', label: 'existing prospect records' });
    const mem = memory({ focus: corrected, stack: [corrected], referents: [corrected] });
    const res = await new FrameDirectedReasoner({ evidence: port }).reason({ tenantId: LEVI, memory: mem, frame: frame({ intent: 'correction', relationToPrior: 'correction' }, mem) });
    assert.equal(res.referent.scope, 'existing');
    assert.ok(port.calls[0].domains.includes('crm_pipeline'));
    assert.ok(!port.calls[0].domains.includes('scout_execution'));
  });
});

describe('Scenario B — execution intent without authority', () => {
  it('identifies the enrichment, its targets and constraints, and reports that nothing changed', async () => {
    const port = evidencePort(crmReport);
    const mem = memory({ focus: set(FIVE), referents: [set(FIVE)], constraints: [{ kind: 'prohibition', action: 'send' }] });
    const f = frame({ intent: 'instruction', relationToPrior: 'follow_up', requestedOutcome: 'action', referents: [{ candidateId: 'r1', role: 'primary' }],
      requestedAction: { verb: 'modify', targetCandidateId: 'r1', polarity: 'affirmative' } }, mem);
    assert.equal(f.executionCandidate, true, 'the frame nominates it; the coordinator still never executes');
    const res = await new FrameDirectedReasoner({ evidence: port }).reason({ tenantId: LEVI, memory: mem, frame: f });
    assert.equal(res.kind, 'not_executed');
    assert.deepEqual([res.execution.requested, res.execution.executed, res.execution.status], ['modify', false, 'not_executed']);
    assert.ok(res.execution.constraints.includes('send'));
    assert.match(res.execution.pathway, /routeExecutionRequest/);
    assert.deepEqual(port.calls[0].referents[0].memberIds, FIVE, 'targets identified by reading, not changed');
    const { prose, structured } = await presentReasoning(res);
    assert.match(prose, /^Not executed: I identified a request to update records but did not carry it out/);
    assert.match(prose, /no records, missions, approvals or outreach were changed/);
    assert.equal(claimsUnrecordedExecution(prose, res), false);
    assert.equal(structured.metadata.executionStatus, 'not_executed');
    assert.deepEqual(Object.keys(port).sort(), ['calls', 'diagnose', 'retrieve'], 'the only capability used is a read');
  });

  it('a prohibition is held and recorded, not executed', async () => {
    const res = await new FrameDirectedReasoner({ evidence: evidencePort(crmReport) }).reason({ tenantId: LEVI, frame: frame({ intent: 'prohibition', requestedAction: { verb: 'send', targetCandidateId: null, polarity: 'negated' } }) });
    assert.equal(res.execution.status, 'held_by_operator');
    assert.match(renderReasoning(res), /^Held: you asked me not to send/);
  });
});

describe('Scenario C — Anchor operational diagnosis', () => {
  const anchorReport = (req) => report(req.tenantId, Object.fromEntries(req.domains.map((d) => [d, { status: 'found', items: [] }])), {
    unknowns: [{ kind: 'unknown', domain: 'outbound_delivery', what: 'emmett execution log', reason: 'no emmett runs recorded in agent_log — absence of a log does not establish that emmett never ran' }],
  });
  it('Scout then Emmett: inferences cite their basis, unsupported ones are dropped, missing logs stay unknown', async () => {
    const port = evidencePort(anchorReport);
    const r = new FrameDirectedReasoner({ evidence: port });
    const scout = await r.reason({ tenantId: LEVI, frame: frame({ subjectKind: 'agent_activity', evidenceNeeds: ['scout_execution'], facets: ['cause'] }) });
    assert.deepEqual(scout.inferences.map((i) => i.claim), ['2 Scout run(s) failed in the last 7 days.']);
    const emmett = await r.reason({ tenantId: LEVI, frame: frame({ subjectKind: 'outbound', relationToPrior: 'follow_up', evidenceNeeds: ['outbound_authorization', 'outbound_delivery'], facets: ['cause'] }) });
    assert.match(emmett.inferences[0].claim, /maintenance/);
    const prose = renderReasoning(emmett);
    assert.match(prose, /Inferred \(not verified\)/);
    assert.match(prose, /does not establish that emmett never ran/);
    // "never ran" may appear only inside the disclosure that absence proves nothing.
    for (const line of prose.split('\n')) if (/\b(?:did not|never) r[au]n\b/i.test(line)) assert.match(line, /does not establish/, line);
  });

  it('an active mission is reported with its classification basis, never as proof of progress', async () => {
    const port = evidencePort((req) => report(req.tenantId, { mission_lifecycle: { status: 'found', items: [{
      ref: 'mission_lifecycle:mission:m1', kind: 'observed', domain: 'mission_lifecycle', tenantId: LEVI, source: { name: 'acquisition_missions', provenance: 'fixture' },
      subject: { type: 'mission', id: 'm1' }, facts: { status: 'active', classification: 'stale', classificationBasis: 'non-terminal, no recorded activity for 40 days (> 14)', awaitingOperatorDecision: false },
      observedAt: days(40), retrievedAt: NOW.toISOString(), freshness: { ageDays: 40, stale: true }, missing: [] }] } },
    { contradictions: [{ kind: 'status_vs_activity', refs: ['mission_lifecycle:mission:m1'], description: 'mission status "active" but no recorded activity for 40 days', resolution: 'unresolved' }] }));
    const res = await new FrameDirectedReasoner({ evidence: port }).reason({ tenantId: LEVI, frame: frame({ subjectKind: 'mission', facets: ['status'] }) });
    const prose = renderReasoning(res);
    assert.match(prose, /status active, classified stale \(non-terminal, no recorded activity for 40 days/);
    assert.match(prose, /Contradictions \(unresolved\)[\s\S]*no recorded activity for 40 days/);
    assert.doesNotMatch(prose, /\b(is progressing|on track)\b/i);
  });
});

describe('Scenario D — correction and continuity', () => {
  const two = [mission('m_a', 'r1'), mission('m_b', 'r2')];
  it('"the active mission" with two candidates asks which one', async () => {
    const mem = memory({ focus: null, referents: two, stack: two });
    const f = frame({ subjectKind: 'mission', ambiguities: [{ kind: 'referent', candidateIds: ['r1', 'r2'] }] }, mem);
    const res = await new FrameDirectedReasoner({ evidence: evidencePort(crmReport) }).reason({ tenantId: LEVI, memory: mem, frame: f });
    assert.equal(res.kind, 'clarification');
    assert.deepEqual(res.clarification.options.map((o) => o.id), ['r1', 'r2']);
  });
  it('"No, the one we discussed earlier" resolves when exactly one earlier mission exists, else asks', async () => {
    const port = evidencePort((req) => report(req.tenantId, { mission_lifecycle: { status: 'found', items: [] } }, { revalidation: { results: req.referents.map((x) => ({ kind: 'mission', id: x.missionId, status: 'current' })) } }));
    const r = new FrameDirectedReasoner({ evidence: port });
    const one = memory({ focus: two[1], stack: [two[1], two[0]], referents: two });
    const res = await r.reason({ tenantId: LEVI, memory: one, frame: frame({ subjectKind: 'mission', intent: 'correction', relationToPrior: 'correction' }, one) });
    assert.equal(res.referent.missionId, 'm_a');
    assert.equal(res.referent.correctedFrom, 'r2');
    assert.deepEqual(port.calls[0].referents, [{ kind: 'mission', missionId: 'm_a' }]);
    const three = [...two, mission('m_c', 'r3')];
    const many = memory({ focus: three[2], stack: [three[2], three[1], three[0]], referents: three });
    const ask = await r.reason({ tenantId: LEVI, memory: many, frame: frame({ subjectKind: 'mission', intent: 'correction', relationToPrior: 'correction' }, many) });
    assert.equal(ask.kind, 'clarification');
    assert.equal(ask.clarification.reason, 'which_earlier_subject');
  });
  it('a follow-up with nothing remembered asks instead of resolving to stale records', async () => {
    const port = evidencePort(crmReport);
    const res = await new FrameDirectedReasoner({ evidence: port }).reason({ tenantId: LEVI, memory: memory({ focus: null }), frame: frame({ relationToPrior: 'follow_up' }) });
    assert.equal(res.kind, 'clarification');
    assert.equal(res.clarification.reason, 'no_current_referent');
    assert.equal(port.calls.length, 0);
  });
});

describe('Scenario F — ambiguous authorization', () => {
  it('"Maybe approve it, but hold the emails until Monday": clarification, nothing approved or executed', async () => {
    const port = evidencePort(crmReport);
    const f = validateTurnFrame({ intent: 'authorization', subjectKind: 'pending_decision', relationToPrior: 'follow_up', referents: [], evidenceNeeds: [], facets: [],
      requestedOutcome: 'action', requestedAction: { verb: 'approve', targetCandidateId: 'pd1', polarity: 'affirmative' }, scope: 'focused',
      foreignTenantReference: false, ambiguities: [{ kind: 'action', candidateIds: ['pd1'] }], confidence: 0.4 },
    { tenantId: LEVI, candidates: [{ id: 'pd1', kind: 'pending_decision' }], pendingDecisionId: 'pd1', producer: 'model' });
    assert.equal(f.executionCandidate, false);
    const res = await new FrameDirectedReasoner({ evidence: port }).reason({ tenantId: LEVI, frame: f });
    assert.equal(res.kind, 'clarification');
    assert.deepEqual([res.execution.requested, res.execution.executed, res.execution.status], ['approve', false, 'not_executed']);
    assert.equal(port.calls.length, 0);
    assert.match(renderReasoning(res), /nothing was approved, consumed or executed/);
  });
});

describe('truthful presentation and failure handling', () => {
  it('rejects a presenter that claims completed actions or invents ids and dates', async () => {
    const res = await new FrameDirectedReasoner({ evidence: evidencePort(crmReport) }).reason({ tenantId: LEVI, frame: frame({ intent: 'instruction', requestedAction: { verb: 'modify', targetCandidateId: null, polarity: 'affirmative' }, evidenceNeeds: ['crm_pipeline'] }) });
    for (const bad of ["I've updated all five records with verified emails.", 'Record 00000000-0000-4000-8000-000000009999 looks strongest.', 'p1 was last touched on 2020-01-01.']) {
      const out = await presentReasoning(res, { presenter: () => bad });
      assert.equal(out.presentation, 'presenter_rejected', bad);
      assert.match(out.prose, /^Not executed/);
    }
    const ok = await presentReasoning(res, { presenter: ({ text }) => text.replace('Observed', 'What the records show') });
    assert.equal(ok.presentation, 'presenter');
    const failing = await presentReasoning(res, { presenter: () => { throw new Error('model down'); } });
    assert.equal(failing.presentation, 'presenter_failed');
  });

  it('a failed, foreign or missing evidence report asserts nothing', async () => {
    const r = (build) => new FrameDirectedReasoner({ evidence: { retrieve: build } }).reason({ tenantId: LEVI, frame: frame({ evidenceNeeds: ['crm_pipeline'] }) });
    for (const build of [async () => { throw new Error('db'); }, async () => report('10', { crm_pipeline: { status: 'found', items: [prospectItem('x')] } }), async () => null]) {
      const res = await r(build);
      assert.equal(res.observed.length, 0);
      assert.equal(res.unknowns.length, 1);
    }
  });

  it('domain selection uses only enum domains, bounded', () => {
    const f = frame({ subjectKind: 'portfolio', facets: ['history', 'cause'] });
    const d = selectDomains(f, null);
    assert.ok(d.length <= 4 && d.every((x) => typeof x === 'string'));
    assert.deepEqual(d, ['mission_lifecycle', 'crm_pipeline', 'agent_execution', 'operator_decisions']);
  });
});

describe('turn subject and same-turn constraints', () => {
  it('merges prohibitions stated in this turn with remembered ones', async () => {
    const res = await new FrameDirectedReasoner({ evidence: evidencePort(crmReport) }).reason({ tenantId: LEVI, constraints: ['send', 'DROP TABLE', 'any_change'],
      memory: memory({ focus: set(FIVE), referents: [set(FIVE)], constraints: [{ kind: 'prohibition', action: 'approve' }] }),
      frame: frame({ intent: 'instruction', requestedAction: { verb: 'modify', targetCandidateId: null, polarity: 'affirmative' }, relationToPrior: 'follow_up' }) });
    assert.deepEqual(res.execution.constraints.sort(), ['any_change', 'approve', 'send']);
  });

  it('a ranking turn emits exactly the ranked records as the subject for conversational memory', async () => {
    const res = await new FrameDirectedReasoner({ evidence: evidencePort(crmReport) }).reason({ tenantId: LEVI, frame: frame({ evidenceNeeds: ['crm_pipeline'], facets: ['recommendation'] }) });
    assert.deepEqual(res.subjectRecordIds, ['p1', 'p2']);
    const { structured } = await presentReasoning(res);
    assert.deepEqual(structured.metadata.items.map((i) => i.recordRef), ['prospect:21:p1', 'prospect:21:p2']);
  });
});

describe('memory port shapes', () => {
  it('a frame naming a stored ledger referent (members[]) resolves to the same exact ids as the focus (memberIds[])', async () => {
    const port = evidencePort(crmReport);
    const stored = { id: 'r1', kind: 'prospect_set', label: '5 existing prospect records', scope: 'existing', members: FIVE, missionId: null, status: 'active' };
    const mem = memory({ focus: null, referents: [stored] });
    const res = await new FrameDirectedReasoner({ evidence: port }).reason({ tenantId: LEVI, memory: mem,
      frame: frame({ relationToPrior: 'follow_up', facets: ['comparison'], referents: [{ candidateId: 'r1', role: 'primary' }] }, mem) });
    assert.deepEqual(port.calls[0].referents, [{ kind: 'prospect_set', memberIds: FIVE }]);
    assert.deepEqual(res.referent.memberIds, FIVE);
  });
});

describe('answers are about the resolved subject', () => {
  it('a remembered record set is answered about those records only', async () => {
    const port = evidencePort((req) => report(req.tenantId, { crm_pipeline: { status: 'found', items: ['p1', 'p2', 'p9'].map((id) => prospectItem(id)) } },
      { revalidation: { results: ['p1', 'p2'].map((id) => ({ kind: 'prospect', id, status: 'current', evidence: prospectItem(id) })) },
        contradictions: [{ kind: 'status_conflict', refs: ['crm_pipeline:prospect_record:p9'], description: 'p9 conflict', resolution: 'unresolved' }] }));
    const mem = memory({ focus: set(['p1', 'p2']), referents: [set(['p1', 'p2'])] });
    const res = await new FrameDirectedReasoner({ evidence: port }).reason({ tenantId: LEVI, memory: mem, frame: frame({ relationToPrior: 'follow_up', facets: ['status'] }, mem) });
    assert.deepEqual(res.observed.map((o) => o.subject.id).sort(), ['p1', 'p2']);
    assert.deepEqual(res.contradictions, [], 'a contradiction about another record is not presented as part of this answer');
  });

  it('a ranking with fewer than five says so and names the exclusions', async () => {
    const port = evidencePort(crmReport);
    port.diagnose.rank = () => ({ ranked: [{ ref: 'crm_pipeline:prospect_record:p1', position: 1 }], excluded: [{ ref: 'crm_pipeline:prospect_record:p3', reason: 'opportunity closed' }] });
    const res = await new FrameDirectedReasoner({ evidence: port }).reason({ tenantId: LEVI, frame: frame({ evidenceNeeds: ['crm_pipeline'], facets: ['recommendation'] }) });
    assert.ok(res.calculated.some((c) => /fewer than the five asked for\. Excluded: p3 \(opportunity closed\)/.test(c.claim)));
  });

  it('clarification options name the missions', async () => {
    const two = [mission('m_a', 'r1'), mission('m_b', 'r2')];
    const mem = memory({ focus: null, referents: two, stack: two });
    const res = await new FrameDirectedReasoner({ evidence: evidencePort(crmReport) }).reason({ tenantId: LEVI, memory: mem, frame: frame({ subjectKind: 'mission', ambiguities: [{ kind: 'referent', candidateIds: ['r1', 'r2'] }] }, mem) });
    assert.deepEqual(res.clarification.options.map((o) => o.label), ['acquisition mission m_a', 'acquisition mission m_b']);
  });
});
