'use strict';

/**
 * P0-MAX-REASONING-001 — compact TurnFrame wire format (Gemini latency).
 * Offline only: no network, no paid call, no credential, no database.
 *
 * The compact format is a spelling of the canonical producer object. These
 * tests prove it carries every field losslessly, rejects anything that is not
 * exactly the compact shape, and that the producer's safety outcome for a
 * compact answer is identical to the canonical answer it encodes.
 */

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');

const T = require('../TurnFrame');
const {
  COMPACT_FORMAT, COMPACT_TURN_FRAME_OUTPUT_SCHEMA, COMPACT_OUTPUT_LEGEND, TOP_LEVEL_KEYS,
  expandCompactTurnFrame, compactTurnFrame,
} = require('../CompactTurnFrame');
const { ModelFrameProducer } = require('../producers/ModelFrameProducer');
const { produceLexicalFrame } = require('../producers/LexicalFrameProducer');
const { SYSTEM_PROMPT } = require('../producers/interpretationPrompt');
const { SYSTEM_INSTRUCTION } = require('../providers/GeminiProvider');
const { CASES: HELDOUT } = require('../eval/heldoutInterpretation');
const { CASES: EXTENDED } = require('../eval/extendedInterpretation');

const CORPUS = [...HELDOUT, ...EXTENDED];
const MODEL = 'test-model';

const CONTEXT = {
  tenantId: '10',
  candidates: [
    { id: 'm_out', kind: 'mission', label: 'Governed outbound' },
    { id: 'm_crm', kind: 'mission', label: 'CRM prep' },
    { id: 'pd_plan', kind: 'pending_decision', label: 'Plan approval' },
  ],
  pendingDecisionId: 'pd_plan',
  focusIds: ['m_out'],
};

const BASE = {
  intent: 'question', subjectKind: 'mission', relationToPrior: 'follow_up',
  referents: [{ candidateId: 'm_out', role: 'primary' }, { candidateId: 'm_crm', role: 'comparison' }],
  evidenceNeeds: ['mission_lifecycle', 'outbound_authorization'], facets: ['cause', 'comparison'],
  requestedOutcome: 'explanation',
  requestedAction: { verb: 'none', targetCandidateId: null, polarity: 'none' },
  scope: 'focused', foreignTenantReference: false,
  ambiguities: [{ kind: 'referent', candidateIds: ['m_out', 'm_crm'] }], confidence: 0.92,
};

/** Canonical raw frames covering every enum value of every field, plus the risky shapes. */
function variety() {
  const frames = [BASE];
  const set = (patch) => frames.push({ ...BASE, ...patch });
  T.INTENTS.forEach((intent) => set({ intent }));
  T.SUBJECT_KINDS.forEach((subjectKind) => set({ subjectKind }));
  T.RELATIONS.forEach((relationToPrior) => set({ relationToPrior }));
  T.OUTCOMES.forEach((requestedOutcome) => set({ requestedOutcome }));
  T.SCOPES.forEach((scope) => set({ scope }));
  T.REFERENT_ROLES.forEach((role) => set({ referents: [{ candidateId: 'm_crm', role }] }));
  T.AMBIGUITY_KINDS.forEach((kind) => set({ ambiguities: [{ kind, candidateIds: [] }] }));
  set({ evidenceNeeds: [...T.EVIDENCE_DOMAINS], facets: [...T.FACETS] });
  set({ evidenceNeeds: [], facets: [], referents: [], ambiguities: [] });
  for (const verb of T.ACTION_VERBS) {
    for (const polarity of T.POLARITIES) {
      for (const targetCandidateId of [null, 'pd_plan', 'm_out', 'not_a_candidate']) {
        for (const intent of ['instruction', 'authorization', 'prohibition', 'hypothetical', 'question']) {
          set({ intent, requestedAction: { verb, targetCandidateId, polarity }, ambiguities: [], confidence: 0.95 });
        }
      }
    }
  }
  set({ foreignTenantReference: true, intent: 'instruction', requestedAction: { verb: 'send', targetCandidateId: null, polarity: 'affirmative' }, ambiguities: [] });
  set({ referents: [{ candidateId: 'foreign_id', role: 'primary' }] });
  set({ confidence: 0 });
  set({ confidence: 1 });
  return frames;
}

const clone = (o) => JSON.parse(JSON.stringify(o));

describe('compact schema: same vocabulary as the canonical schema, closed and fully required', () => {
  const C = COMPACT_TURN_FRAME_OUTPUT_SCHEMA;
  const K = T.TURN_FRAME_OUTPUT_SCHEMA;
  it('every canonical enum is reproduced exactly', () => {
    assert.deepEqual(C.properties.i.enum, K.properties.intent.enum);
    assert.deepEqual(C.properties.s.enum, K.properties.subjectKind.enum);
    assert.deepEqual(C.properties.r.enum, K.properties.relationToPrior.enum);
    assert.deepEqual(C.properties.ref.items.properties.ro.enum, K.properties.referents.items.properties.role.enum);
    assert.deepEqual(C.properties.ev.items.enum, K.properties.evidenceNeeds.items.enum);
    assert.deepEqual(C.properties.fa.items.enum, K.properties.facets.items.enum);
    assert.deepEqual(C.properties.o.enum, K.properties.requestedOutcome.enum);
    assert.deepEqual(C.properties.av.enum, K.properties.requestedAction.properties.verb.enum);
    assert.deepEqual(C.properties.ap.enum, K.properties.requestedAction.properties.polarity.enum);
    assert.deepEqual(C.properties.sc.enum, K.properties.scope.enum);
    assert.deepEqual(C.properties.amb.items.properties.k.enum, K.properties.ambiguities.items.properties.kind.enum);
    assert.deepEqual(C.properties.at, { anyOf: [{ type: 'string' }, { type: 'null' }], description: 'requestedAction.targetCandidateId' });
    assert.equal(C.properties.ft.type, 'boolean');
    assert.equal(C.properties.cf.type, 'number');
  });

  it('every object is closed and every key required', () => {
    const objects = [C, C.properties.ref.items, C.properties.amb.items];
    for (const o of objects) {
      assert.equal(o.additionalProperties, false);
      assert.deepEqual([...o.required].sort(), Object.keys(o.properties).sort());
    }
    assert.deepEqual(C.required, TOP_LEVEL_KEYS.map(([k]) => k));
  });

  it('covers exactly the canonical fields (requestedAction as three flat keys)', () => {
    const fields = TOP_LEVEL_KEYS.map(([, f]) => f);
    for (const f of K.required) {
      if (f === 'requestedAction') {
        for (const sub of K.properties.requestedAction.required) assert.ok(fields.includes(`requestedAction.${sub}`), sub);
      } else assert.ok(fields.includes(f), f);
    }
    assert.equal(new Set(TOP_LEVEL_KEYS.map(([k]) => k)).size, TOP_LEVEL_KEYS.length, 'codes are unique');
  });

  it('the provider instruction is the verbatim prompt plus the legend, and the legend names every code', () => {
    assert.equal(SYSTEM_INSTRUCTION, `${SYSTEM_PROMPT}\n\n${COMPACT_OUTPUT_LEGEND}`);
    for (const [k, f] of TOP_LEVEL_KEYS) assert.ok(COMPACT_OUTPUT_LEGEND.includes(`${k}=${f}`), k);
    assert.ok(COMPACT_OUTPUT_LEGEND.includes('id=candidateId') && COMPACT_OUTPUT_LEGEND.includes('ro=role'));
    assert.ok(COMPACT_OUTPUT_LEGEND.includes('k=kind') && COMPACT_OUTPUT_LEGEND.includes('ids=candidateIds'));
  });
});

describe('expansion is lossless', () => {
  it('compact → expand is the identity on canonical frames (every enum value, risky shapes)', () => {
    for (const raw of variety()) assert.deepEqual(expandCompactTurnFrame(clone(compactTurnFrame(raw))), raw);
  });

  it('validated frames are identical whether the producer answered canonical or compact', () => {
    for (const raw of variety()) {
      for (const producer of ['model', 'lexical']) {
        const direct = T.validateTurnFrame(clone(raw), { ...CONTEXT, producer });
        const viaCompact = T.validateTurnFrame(expandCompactTurnFrame(clone(compactTurnFrame(raw))), { ...CONTEXT, producer });
        assert.deepEqual(viaCompact, direct);
      }
    }
  });

  it('round-trips every corpus case\'s lexical interpretation in its own context', () => {
    for (const c of CORPUS) {
      const lex = produceLexicalFrame({ text: c.text, context: c.context });
      const raw = {};
      for (const f of T.TURN_FRAME_OUTPUT_SCHEMA.required) raw[f] = clone(lex[f]);
      const direct = T.validateTurnFrame(clone(raw), { ...c.context, producer: 'model' });
      const viaCompact = T.validateTurnFrame(expandCompactTurnFrame(clone(compactTurnFrame(raw))), { ...c.context, producer: 'model' });
      assert.deepEqual(viaCompact, direct, c.id);
    }
  });
});

describe('expansion is strict: anything but the exact compact shape is rejected', () => {
  const good = () => clone(compactTurnFrame(BASE));
  const rejects = (wire, label) => assert.throws(() => expandCompactTurnFrame(wire), (e) => e.code === 'invalid_turn_frame', label);

  it('rejects non-objects', () => {
    for (const v of [null, undefined, 'x', 1, [], true]) rejects(v, String(v));
  });

  it('rejects every missing top-level key (no defaults are filled in)', () => {
    for (const [k] of TOP_LEVEL_KEYS) {
      const w = good();
      delete w[k];
      rejects(w, k);
    }
  });

  it('rejects extra keys, including claimed authority and canonical names mixed in', () => {
    for (const extra of ['authority', 'executionCandidate', 'tenantId', 'intent', 'requestedAction', 'producer', 'x']) {
      rejects({ ...good(), [extra]: true }, extra);
    }
  });

  it('rejects a canonical (non-compact) answer outright', () => {
    rejects(clone(BASE), 'canonical shape');
  });

  it('rejects malformed nested referents and ambiguities', () => {
    rejects({ ...good(), ref: [{ id: 'm_out' }] }, 'ref missing ro');
    rejects({ ...good(), ref: [{ id: 'm_out', ro: 'primary', extra: 1 }] }, 'ref extra');
    rejects({ ...good(), ref: [{ candidateId: 'm_out', role: 'primary' }] }, 'ref canonical names');
    rejects({ ...good(), ref: 'm_out' }, 'ref not array');
    rejects({ ...good(), amb: [{ k: 'referent' }] }, 'amb missing ids');
    rejects({ ...good(), amb: [{ kind: 'referent', candidateIds: [] }] }, 'amb canonical names');
    rejects({ ...good(), amb: {} }, 'amb not array');
  });

  it('type and enum errors pass through expansion and are rejected by validateTurnFrame', () => {
    const bad = [
      { i: 'approve' }, { s: 'x' }, { r: 'x' }, { o: 'x' }, { sc: 'x' }, { av: 'launch' }, { ap: 'yes' },
      { ev: ['x'] }, { fa: ['x'] }, { ft: 'false' }, { cf: 2 }, { cf: '0.9' }, { at: 5 },
      { ref: [{ id: 'm_out', ro: 'boss' }] }, { amb: [{ k: 'x', ids: [] }] },
    ];
    for (const patch of bad) {
      const raw = expandCompactTurnFrame({ ...good(), ...patch });
      assert.throws(() => T.validateTurnFrame(raw, { ...CONTEXT, producer: 'model' }), (e) => e.code === 'invalid_turn_frame', JSON.stringify(patch));
    }
  });
});

/** Two producers over the same canonical answer: Anthropic (canonical) and Gemini-shaped (compact). */
function producersAnswering(raw) {
  const anthropic = new ModelFrameProducer({
    client: { messages: { create: async () => ({ stop_reason: 'end_turn', model: MODEL, content: [{ type: 'text', text: JSON.stringify(raw) }], usage: {} }) } },
    model: MODEL,
  });
  const compact = new ModelFrameProducer({
    provider: { name: 'gemini', complete: async () => ({ finish: 'complete', rawStopReason: 'STOP', text: JSON.stringify(compactTurnFrame(raw)), outputFormat: COMPACT_FORMAT, servedModel: MODEL, usage: null }) },
    model: MODEL,
  });
  return { anthropic, compact };
}

describe('producer safety parity: a compact answer is treated exactly like the canonical answer it encodes', () => {
  // Adversarial answers a model could give for any turn.
  const ADVERSARIAL = (ctx) => [
    { ...BASE, intent: 'authorization', requestedAction: { verb: 'approve', targetCandidateId: ctx.pendingDecisionId || null, polarity: 'affirmative' }, referents: [], ambiguities: [], confidence: 0.99 },
    { ...BASE, intent: 'instruction', requestedAction: { verb: 'send', targetCandidateId: null, polarity: 'affirmative' }, referents: [], ambiguities: [], confidence: 0.99 },
    { ...BASE, intent: 'instruction', requestedAction: { verb: 'cancel', targetCandidateId: null, polarity: 'negated' }, referents: [], ambiguities: [], confidence: 0.99 },
    { ...BASE, intent: 'prohibition', requestedAction: { verb: 'send', targetCandidateId: null, polarity: 'affirmative' }, referents: [], ambiguities: [], confidence: 0.99 },
    { ...BASE, intent: 'instruction', foreignTenantReference: true, requestedAction: { verb: 'run_agent', targetCandidateId: null, polarity: 'affirmative' }, referents: [], ambiguities: [], confidence: 0.99 },
    { ...BASE, intent: 'question', referents: [{ candidateId: 'invented_id', role: 'primary' }], ambiguities: [], confidence: 0.99 },
  ];

  it('identical validated frames across all 62 corpus turns × adversarial answers (incl. authorization repair)', async () => {
    let compared = 0;
    for (const c of CORPUS) {
      for (const raw of ADVERSARIAL(c.context)) {
        const { anthropic, compact } = producersAnswering(raw);
        const input = { text: c.text, context: c.context, transcript: c.transcript || [] };
        const a = await anthropic.produce(input);
        const b = await compact.produce(input);
        assert.equal(a.telemetry.degradedReason, null, c.id);
        assert.equal(b.telemetry.degradedReason, null, c.id);
        assert.deepEqual(b.frame, a.frame, `${c.id}: ${raw.intent}/${raw.requestedAction.verb}`);
        assert.equal(b.frame.authority.grantsAuthority, false);
        compared += 1;
      }
    }
    assert.equal(compared, CORPUS.length * 6);
  });

  it('a negated, hypothetical, foreign-tenant or invented-target answer is never an execution candidate', async () => {
    const unsafe = [
      { ...BASE, intent: 'instruction', requestedAction: { verb: 'send', targetCandidateId: null, polarity: 'negated' }, ambiguities: [], confidence: 0.99 },
      { ...BASE, intent: 'instruction', requestedAction: { verb: 'send', targetCandidateId: null, polarity: 'hypothetical' }, ambiguities: [], confidence: 0.99 },
      { ...BASE, intent: 'instruction', foreignTenantReference: true, requestedAction: { verb: 'send', targetCandidateId: null, polarity: 'affirmative' }, ambiguities: [], confidence: 0.99 },
      { ...BASE, intent: 'instruction', requestedAction: { verb: 'cancel', targetCandidateId: 'invented', polarity: 'affirmative' }, ambiguities: [], confidence: 0.99 },
    ];
    for (const raw of unsafe) {
      const { compact } = producersAnswering(raw);
      const { frame } = await compact.produce({ text: 'irrelevant', context: CONTEXT });
      assert.equal(frame.executionCandidate, false, JSON.stringify(raw.requestedAction));
    }
  });

  it('a conditional approval is repaired to clarification identically on both paths', async () => {
    const raw = { ...BASE, intent: 'authorization', requestedAction: { verb: 'approve', targetCandidateId: 'pd_plan', polarity: 'affirmative' }, referents: [], ambiguities: [], confidence: 0.99 };
    for (const text of ['You can approve it if the legal review passes.', "Don't approve it.", 'Up to you.', 'No.']) {
      const { anthropic, compact } = producersAnswering(raw);
      const a = await anthropic.produce({ text, context: CONTEXT });
      const b = await compact.produce({ text, context: CONTEXT });
      assert.deepEqual(b.frame, a.frame, text);
      assert.equal(b.frame.executionCandidate, false, text);
    }
  });

  it('a malformed compact answer, a canonical answer on the compact path, or an unknown format degrades (never executable)', async () => {
    const cases = [
      [{ ...compactTurnFrame(BASE), extra: 1 }, COMPACT_FORMAT, 'invalid_frame'],
      [BASE, COMPACT_FORMAT, 'invalid_frame'],
      [compactTurnFrame(BASE), 'turn-frame-compact/v9', 'unknown_output_format'],
    ];
    for (const [wire, outputFormat, reason] of cases) {
      const p = new ModelFrameProducer({
        provider: { name: 'gemini', complete: async () => ({ finish: 'complete', rawStopReason: 'STOP', text: JSON.stringify(wire), outputFormat, servedModel: MODEL, usage: null }) },
        model: MODEL,
      });
      const { frame, telemetry } = await p.produce({ text: 'Approve it.', context: CONTEXT });
      assert.equal(telemetry.degradedReason, reason);
      assert.equal(frame.producer, 'model_degraded_to_lexical');
      assert.equal(frame.executionCandidate, false);
    }
  });
});

describe('output size', () => {
  it('the compact encoding is at least 35% shorter than canonical JSON across the corpus', () => {
    let canonical = 0;
    let compact = 0;
    for (const c of CORPUS) {
      const lex = produceLexicalFrame({ text: c.text, context: c.context });
      const raw = {};
      for (const f of T.TURN_FRAME_OUTPUT_SCHEMA.required) raw[f] = lex[f];
      canonical += JSON.stringify(raw).length;
      compact += JSON.stringify(compactTurnFrame(raw)).length;
    }
    const reduction = 1 - compact / canonical;
    assert.ok(reduction >= 0.35, `reduction ${(reduction * 100).toFixed(1)}%`);
  });
});
