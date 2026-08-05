'use strict';

/**
 * SPEC-064 — Relationship Intelligence Interview (Max-owned capture).
 * State-machine debrief + notes mode. Review before commit.
 * Writes only relationship_* tables — never CRM / opportunities.
 */

const crypto = require('crypto');
const defaultPool = require('../db');

const INTERACTION_TYPES = Object.freeze([
  'cold_call',
  'discovery_call',
  'walkthrough',
  'estimate',
  'meeting',
  'demo',
  'proposal_review',
  'follow_up',
  'other',
]);

const INTERACTION_STATUSES = Object.freeze(['draft', 'reviewed', 'committed']);

const INSIGHT_KINDS = Object.freeze([
  'pain',
  'goal',
  'objection',
  'timeline',
  'budget',
  'decision_maker',
  'stakeholder',
  'competitor',
  'next_step',
  'commitment',
  'risk',
  'buying_signal',
  'open_question',
  'preference',
  'context',
]);

const BASE_CONFIDENCE = 0.7;
const THIN_CONFIDENCE = 0.4;
const MIN_NOTES_CHARS = 40;

/** Allowed SQL table tokens for Postgres path (CRM guard). */
const ALLOWED_SQL_TABLES = Object.freeze([
  'relationship_interactions',
  'relationship_interaction_insights',
]);

const CRM_SQL_DENY = Object.freeze([
  'prospects',
  'companies',
  'opportunities',
  'customers',
  'jobs',
  'commissions',
  'activity_log',
  'touchpoints',
]);

const QUESTION_BANK = Object.freeze([
  {
    id: 'what_happened',
    prompt: 'What happened?',
    insightKind: 'context',
    label: 'What happened',
  },
  {
    id: 'who_involved',
    prompt: 'Who was involved?',
    insightKind: 'stakeholder',
    label: 'Who was involved',
  },
  {
    id: 'cared_most',
    prompt: 'What did they care about most?',
    insightKind: 'goal',
    label: 'Cared about most',
  },
  {
    id: 'objections',
    prompt: 'What objections or concerns came up?',
    insightKind: 'objection',
    label: 'Objections or concerns',
  },
  {
    id: 'budget_timeline_decision',
    prompt: 'Was budget, timeline, or decision process discussed?',
    insightKind: null,
    label: 'Budget / timeline / decision',
    adaptiveSkipIf: (answers) =>
      hasSignal(answers, /budget|timeline|decision.?maker|decision process|procurement/i),
  },
  {
    id: 'promised',
    prompt: 'What did we promise?',
    insightKind: 'commitment',
    label: 'What we promised',
  },
  {
    id: 'next_step',
    prompt: 'What is the next step?',
    insightKind: 'next_step',
    label: 'Next step',
  },
  {
    id: 'remember',
    prompt: 'Is there anything Max should remember before we talk to them again?',
    insightKind: 'preference',
    label: 'Remember for next time',
  },
]);

class RelationshipIntelligenceError extends Error {
  /**
   * @param {string} code
   * @param {string} message
   * @param {number} [status]
   */
  constructor(code, message, status = 400) {
    super(message);
    this.name = 'RelationshipIntelligenceError';
    this.code = code;
    this.status = status;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function newId() {
  return crypto.randomUUID();
}

function asText(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s || null;
}

function asClientId(value) {
  if (value == null || value === '') return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function assertInteractionType(type) {
  const t = String(type || '').trim();
  if (!INTERACTION_TYPES.includes(t)) {
    throw new RelationshipIntelligenceError(
      'invalid_interaction_type',
      `interaction_type must be one of: ${INTERACTION_TYPES.join(', ')}`
    );
  }
  return t;
}

function assertInsightKind(kind) {
  const k = String(kind || '').trim();
  if (!INSIGHT_KINDS.includes(k)) {
    throw new RelationshipIntelligenceError(
      'invalid_insight_kind',
      `insight kind must be one of: ${INSIGHT_KINDS.join(', ')}`
    );
  }
  return k;
}

function parseOccurredAt(value) {
  if (value == null || value === '') return new Date();
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) {
    throw new RelationshipIntelligenceError(
      'invalid_occurred_at',
      'occurred_at must be a valid ISO timestamp'
    );
  }
  return d;
}

function hasSignal(answers, pattern) {
  for (const v of Object.values(answers || {})) {
    if (v && pattern.test(String(v))) return true;
  }
  return false;
}

function answerLooksEmpty(text) {
  const s = String(text || '').trim().toLowerCase();
  if (!s) return true;
  return /^(n\/?a|none|no|nothing|nope|nil|-)$/i.test(s);
}

/**
 * Guard Postgres SQL — only relationship_* tables may be mutated/read here.
 * @param {string} sql
 */
function assertAllowedSql(sql) {
  const normalized = String(sql || '').replace(/\/\*[\s\S]*?\*\//g, ' ').replace(/--.*$/gm, ' ');
  const lower = normalized.toLowerCase();
  for (const table of CRM_SQL_DENY) {
    const re = new RegExp(`\\b${table}\\b`, 'i');
    if (re.test(lower)) {
      throw new RelationshipIntelligenceError(
        'crm_write_forbidden',
        `Relationship Intelligence must not touch CRM table: ${table}`,
        500
      );
    }
  }
  const mutates =
    /\b(insert|update|delete|merge|truncate|alter|drop|create)\b/i.test(lower);
  if (!mutates) return;
  const touchesAllowed = ALLOWED_SQL_TABLES.some((t) =>
    new RegExp(`\\b${t}\\b`, 'i').test(lower)
  );
  if (!touchesAllowed) {
    throw new RelationshipIntelligenceError(
      'crm_write_forbidden',
      'Mutation SQL must target relationship_interactions or relationship_interaction_insights',
      500
    );
  }
}

function initialInterviewState({ notes } = {}) {
  return {
    mode: notes ? 'notes' : 'interactive',
    stepIndex: 0,
    done: Boolean(notes),
    answers: {},
    messages: [],
    notes: notes ? String(notes) : null,
    summarized: false,
  };
}

function currentQuestion(state) {
  if (!state || state.done || state.mode === 'notes') return null;
  let idx = Number(state.stepIndex) || 0;
  while (idx < QUESTION_BANK.length) {
    const q = QUESTION_BANK[idx];
    if (typeof q.adaptiveSkipIf === 'function' && q.adaptiveSkipIf(state.answers || {})) {
      idx += 1;
      continue;
    }
    return { index: idx, question: q };
  }
  return null;
}

function advanceAfterAnswer(state, questionId, message) {
  const next = {
    ...state,
    answers: { ...(state.answers || {}), [questionId]: message },
    messages: [
      ...(state.messages || []),
      { role: 'user', questionId, text: message, at: nowIso() },
    ],
  };
  let idx = (Number(state.stepIndex) || 0) + 1;
  while (idx < QUESTION_BANK.length) {
    const q = QUESTION_BANK[idx];
    if (typeof q.adaptiveSkipIf === 'function' && q.adaptiveSkipIf(next.answers)) {
      idx += 1;
      continue;
    }
    break;
  }
  next.stepIndex = idx;
  if (idx >= QUESTION_BANK.length) {
    next.done = true;
  }
  return next;
}

/**
 * Split notes into clauses so comma-separated debriefs yield multiple insights.
 * Does not mutate the original notes / raw_summary.
 * @param {string} text
 * @returns {string[]}
 */
function splitNotesClauses(text) {
  return String(text || '')
    .split(/(?<=[.!?])\s+|\n+|;\s*|,\s+|\s+&\s+|\s+and\s+/i)
    .map((s) =>
      s
        .trim()
        .replace(/^[-•*]+\s*/, '')
        .replace(/^(?:and|&)\s+/i, '')
        .replace(/[.!?]+$/g, '')
        .trim()
    )
    .filter((s) => s.length > 1);
}

/**
 * Heuristic notes → insights.
 * @param {string} notes
 */
function extractInsightsFromNotes(notes) {
  const text = String(notes || '').trim();
  const insights = [];
  const caveats = [];
  const clauses = splitNotesClauses(text);

  // Order matters: more specific commercial phrases before generic tokens
  // like "owner", "budget", or "timeline".
  const patterns = [
    {
      re: /\bexpressed interest\b|\basked for more (?:info|information|details)\b|\bwants? more (?:info|information|details)\b|\b(interested|excited|ready to (?:move|buy|proceed)|let'?s do|sounds good)\b/i,
      kind: 'buying_signal',
      label: 'Buying signal',
    },
    {
      re: /\b(?:received|sent|delivered|shared)\b[\s\w-]*\b(?:personalized\s+)?(?:\d+[- ]page\s+)?(?:overview|proposal|one[- ]pager|deck|brief)\b/i,
      kind: 'commitment',
      label: 'Commitment',
    },
    {
      re: /\b(?:need(?:s)?(?:\s+to)?\s+follow[- ]?up|follow[- ]?up(?:\s+to|\s+needed|\s+required)?|next step|schedule|book)\b/i,
      kind: 'next_step',
      label: 'Next step',
    },
    {
      re: /^(?:clarify\s+|confirm\s+|open:\s*)?(?:budget(?:\s*\/\s*timeline)?|timeline|decision(?:\s+process)?|target client(?:\s+type)?|client type|lead[- ]?gen(?:eration)?(?:\s+help)?)$/i,
      kind: 'open_question',
      label: 'Open question',
    },
    {
      re: /\b(?:clarify|confirm|unknown|tbd|still need|need to (?:confirm|clarify|know)|whether they want)\b[\s\w/,-]*\b(?:budget|timeline|decision(?:\s+process)?|target client|lead[- ]?gen|help generating|commercial cleaning leads)/i,
      kind: 'open_question',
      label: 'Open question',
    },
    {
      re: /\b(?:under\s+\d+\s+months?|less than\s+\d+\s+months?|early[- ]stage|just started|new company|company (?:is )?(?:young|new))\b/i,
      kind: 'context',
      label: 'Company stage',
    },
    {
      re: /\b(?:\$?\d[\d,]*(?:\.\d+)?\s*k?\s*mrr|mrr\s*(?:of\s*)?\$?\d[\d,]*(?:\.\d+)?k?|about\s+[\$,]?\d[\d,]*\s*(?:mrr|in monthly)|monthly recurring revenue)\b/i,
      kind: 'budget',
      label: 'MRR / budget',
    },
    {
      re: /\bfocused on\b|\bgrowing\b.*\bcommercial\b|\bcommercial cleaning\b|\bcommercial focus\b/i,
      kind: 'goal',
      label: 'Focus / goal',
    },
    {
      re: /\b(?:is|as)\s+owner\b|\bowner\b|\bdecision.?maker\b|\bpartner\b|\bceo\b|\bcfo\b|\bapprover\b/i,
      kind: 'decision_maker',
      label: 'Decision maker',
    },
    { re: /\b(pain|struggle|problem|frustrated|issue)\b/i, kind: 'pain', label: 'Pain' },
    { re: /\b(goal|want|hoping|looking to)\b/i, kind: 'goal', label: 'Goal' },
    {
      re: /\b(object|concern|worried|hesitat|pushback)\b/i,
      kind: 'objection',
      label: 'Objection',
    },
    {
      re: /\b(timeline|by\s+\w+|next\s+(week|month|quarter)|asap)\b/i,
      kind: 'timeline',
      label: 'Timeline',
    },
    { re: /\b(budget|price|cost|\$\d+)\b/i, kind: 'budget', label: 'Budget' },
    {
      re: /\b(stakeholder|team|committee|office manager)\b/i,
      kind: 'stakeholder',
      label: 'Stakeholder',
    },
    {
      re: /\b(competitor|versus|vs\.?|alternative)\b/i,
      kind: 'competitor',
      label: 'Competitor',
    },
    {
      re: /\b(promis|commit|we will|i will|agreed to)\b/i,
      kind: 'commitment',
      label: 'Commitment',
    },
    { re: /\b(risk|block|blocker|deal.?breaker)\b/i, kind: 'risk', label: 'Risk' },
    {
      re: /\b(prefer|preference|don'?t call|email only)\b/i,
      kind: 'preference',
      label: 'Preference',
    },
  ];

  // Allow multiple buying_signal / next_step / commitment / open_question / context
  // insights; keep other kinds singleton so noise stays low.
  const MULTI_OK = new Set([
    'buying_signal',
    'next_step',
    'commitment',
    'open_question',
    'context',
  ]);
  const usedSingleton = new Set();
  const seenValue = new Set();

  for (const clause of clauses) {
    for (const p of patterns) {
      if (!MULTI_OK.has(p.kind) && usedSingleton.has(p.kind)) continue;
      if (!p.re.test(clause)) continue;
      const key = `${p.kind}::${clause.toLowerCase()}`;
      if (seenValue.has(key)) continue;
      seenValue.add(key);
      insights.push({
        kind: p.kind,
        label: p.label,
        value: clause,
        confidence: BASE_CONFIDENCE,
        sourceQuote: clause,
      });
      if (!MULTI_OK.has(p.kind)) usedSingleton.add(p.kind);
      break;
    }
  }

  if (text) {
    insights.push({
      kind: 'context',
      label: 'Interaction notes',
      value: text.length > 500 ? `${text.slice(0, 497)}...` : text,
      confidence: BASE_CONFIDENCE,
      sourceQuote: text.slice(0, 280),
    });
  }

  const structuredCount = insights.filter((i) => i.kind !== 'context').length;
  const thin = text.length < MIN_NOTES_CHARS || structuredCount === 0;
  if (thin) {
    caveats.push('Notes were thin; several relationship fields remain unknown.');
    insights.push({
      kind: 'open_question',
      label: 'Needs follow-up',
      value: 'Clarify who was involved, what they cared about, objections, and next step.',
      confidence: THIN_CONFIDENCE,
      sourceQuote: text.slice(0, 120) || null,
    });
  }

  const confidence = thin ? THIN_CONFIDENCE : BASE_CONFIDENCE;
  return { insights, caveats, confidence, thin };
}

/**
 * Map interactive answers → insights.
 * @param {object} state
 */
function extractInsightsFromAnswers(state) {
  const answers = state.answers || {};
  const insights = [];
  const caveats = [];
  const nextSteps = [];

  for (const q of QUESTION_BANK) {
    const raw = answers[q.id];
    if (raw == null || answerLooksEmpty(raw)) continue;
    const text = String(raw).trim();

    if (q.id === 'budget_timeline_decision') {
      if (/\bbudget\b/i.test(text)) {
        insights.push({
          kind: 'budget',
          label: 'Budget',
          value: text,
          confidence: BASE_CONFIDENCE,
          sourceQuote: text,
        });
      }
      if (/\btimeline\b/i.test(text)) {
        insights.push({
          kind: 'timeline',
          label: 'Timeline',
          value: text,
          confidence: BASE_CONFIDENCE,
          sourceQuote: text,
        });
      }
      if (/\bdecision\b/i.test(text)) {
        insights.push({
          kind: 'decision_maker',
          label: 'Decision process',
          value: text,
          confidence: BASE_CONFIDENCE,
          sourceQuote: text,
        });
      }
      if (!/\b(budget|timeline|decision)\b/i.test(text)) {
        insights.push({
          kind: 'context',
          label: q.label,
          value: text,
          confidence: BASE_CONFIDENCE,
          sourceQuote: text,
        });
      }
      continue;
    }

    if (!q.insightKind) continue;
    const kind = assertInsightKind(q.insightKind);
    insights.push({
      kind,
      label: q.label,
      value: text,
      confidence: BASE_CONFIDENCE,
      sourceQuote: text,
    });
    if (kind === 'next_step') {
      nextSteps.push(text);
    }
  }

  const filled = Object.values(answers).filter((v) => !answerLooksEmpty(v)).length;
  const thin = filled < 3;
  if (thin) {
    caveats.push('Few answers captured; confidence is reduced until more detail is added.');
    insights.push({
      kind: 'open_question',
      label: 'Incomplete debrief',
      value: 'More detail needed on pain, objections, and next step.',
      confidence: THIN_CONFIDENCE,
      sourceQuote: null,
    });
  }

  return {
    insights,
    caveats,
    nextSteps,
    confidence: thin ? THIN_CONFIDENCE : BASE_CONFIDENCE,
    thin,
  };
}

function normalizeInsight(row) {
  const kind = assertInsightKind(row.kind);
  return {
    kind,
    label: String(row.label || ''),
    value: String(row.value || ''),
    confidence:
      row.confidence == null || Number.isNaN(Number(row.confidence))
        ? BASE_CONFIDENCE
        : Number(row.confidence),
    sourceQuote: row.sourceQuote != null ? String(row.sourceQuote) : row.source_quote != null ? String(row.source_quote) : null,
  };
}

function buildPayload(interaction, insights, { caveats = [], nextSteps = [] } = {}) {
  return {
    ok: true,
    kind: 'relationship_intelligence_interview',
    isEvidence: true,
    status: interaction.status,
    interaction: {
      interactionType: interaction.interaction_type,
      companyId: interaction.company_id,
      contactId: interaction.contact_id,
      opportunityId: interaction.opportunity_id,
      occurredAt:
        interaction.occurred_at instanceof Date
          ? interaction.occurred_at.toISOString()
          : interaction.occurred_at,
      rawSummary: interaction.raw_summary,
      structuredSummary: interaction.structured_summary || {},
      confidence:
        interaction.confidence == null ? null : Number(interaction.confidence),
    },
    insights: (insights || []).map(normalizeInsight),
    nextSteps: nextSteps.length
      ? nextSteps
      : (insights || [])
          .filter((i) => i.kind === 'next_step')
          .map((i) => i.value),
    caveats: [...caveats],
  };
}

function buildRawSummary(state, extraction) {
  if (state.mode === 'notes' && state.notes) {
    return String(state.notes).trim();
  }
  const parts = [];
  for (const q of QUESTION_BANK) {
    const a = state.answers && state.answers[q.id];
    if (a && !answerLooksEmpty(a)) {
      parts.push(`${q.prompt} ${String(a).trim()}`);
    }
  }
  if (!parts.length && extraction.thin) {
    return 'Sparse debrief — limited detail captured.';
  }
  return parts.join(' ');
}

/* -------------------------------------------------------------------------- */
/* Stores                                                                     */
/* -------------------------------------------------------------------------- */

function createMemoryStore() {
  /** @type {Map<string, object>} */
  const interactions = new Map();
  /** @type {Map<string, object[]>} */
  const insightsByInteraction = new Map();
  const sqlLog = [];

  return {
    kind: 'memory',
    sqlLog,
    async insertInteraction(row) {
      sqlLog.push({ op: 'insert', table: 'relationship_interactions' });
      const copy = { ...row };
      interactions.set(copy.id, copy);
      insightsByInteraction.set(copy.id, []);
      return copy;
    },
    async getInteraction(id) {
      sqlLog.push({ op: 'select', table: 'relationship_interactions' });
      const row = interactions.get(String(id));
      return row ? { ...row } : null;
    },
    async updateInteraction(id, patch) {
      sqlLog.push({ op: 'update', table: 'relationship_interactions' });
      const cur = interactions.get(String(id));
      if (!cur) return null;
      const next = { ...cur, ...patch, updated_at: new Date() };
      interactions.set(String(id), next);
      return { ...next };
    },
    async replaceInsights(interactionId, insights) {
      sqlLog.push({ op: 'delete', table: 'relationship_interaction_insights' });
      sqlLog.push({ op: 'insert', table: 'relationship_interaction_insights' });
      const rows = (insights || []).map((i) => ({
        id: newId(),
        interaction_id: String(interactionId),
        kind: i.kind,
        label: i.label || '',
        value: i.value || '',
        confidence: i.confidence,
        source_quote: i.sourceQuote != null ? i.sourceQuote : null,
        created_at: new Date(),
      }));
      insightsByInteraction.set(String(interactionId), rows);
      return rows.map((r) => ({ ...r }));
    },
    async listInsights(interactionId) {
      sqlLog.push({ op: 'select', table: 'relationship_interaction_insights' });
      return (insightsByInteraction.get(String(interactionId)) || []).map((r) => ({
        ...r,
      }));
    },
    async listInteractions(filters = {}) {
      sqlLog.push({ op: 'select', table: 'relationship_interactions' });
      let rows = [...interactions.values()];
      if (filters.clientId != null) {
        rows = rows.filter((r) => r.client_id === filters.clientId);
      }
      if (filters.status) {
        rows = rows.filter((r) => r.status === filters.status);
      }
      if (filters.companyId) {
        rows = rows.filter((r) => r.company_id === String(filters.companyId));
      }
      rows.sort((a, b) => new Date(b.occurred_at) - new Date(a.occurred_at));
      const limit = Math.min(Math.max(Number(filters.limit) || 50, 1), 200);
      return rows.slice(0, limit).map((r) => ({ ...r }));
    },
  };
}

function createPostgresStore(pool) {
  async function query(sql, params) {
    assertAllowedSql(sql);
    return pool.query(sql, params);
  }

  return {
    kind: 'postgres',
    async insertInteraction(row) {
      const result = await query(
        `INSERT INTO relationship_interactions (
           id, client_id, company_id, contact_id, opportunity_id, user_id,
           interaction_type, occurred_at, source, status, raw_summary,
           structured_summary, confidence, interview_state, created_at, updated_at
         ) VALUES (
           $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14::jsonb,NOW(),NOW()
         )
         RETURNING *`,
        [
          row.id,
          row.client_id,
          row.company_id,
          row.contact_id,
          row.opportunity_id,
          row.user_id,
          row.interaction_type,
          row.occurred_at,
          row.source,
          row.status,
          row.raw_summary,
          JSON.stringify(row.structured_summary || {}),
          row.confidence,
          JSON.stringify(row.interview_state || {}),
        ]
      );
      return result.rows[0];
    },
    async getInteraction(id) {
      const result = await query(
        `SELECT * FROM relationship_interactions WHERE id = $1`,
        [String(id)]
      );
      return result.rows[0] || null;
    },
    async updateInteraction(id, patch) {
      const fields = [];
      const params = [];
      let n = 1;
      const map = {
        status: 'status',
        raw_summary: 'raw_summary',
        structured_summary: 'structured_summary',
        confidence: 'confidence',
        interview_state: 'interview_state',
      };
      for (const [key, col] of Object.entries(map)) {
        if (Object.prototype.hasOwnProperty.call(patch, key)) {
          if (key === 'structured_summary' || key === 'interview_state') {
            fields.push(`${col} = $${n}::jsonb`);
            params.push(JSON.stringify(patch[key] || {}));
          } else {
            fields.push(`${col} = $${n}`);
            params.push(patch[key]);
          }
          n += 1;
        }
      }
      if (!fields.length) {
        return this.getInteraction(id);
      }
      fields.push('updated_at = NOW()');
      params.push(String(id));
      const result = await query(
        `UPDATE relationship_interactions SET ${fields.join(', ')} WHERE id = $${n} RETURNING *`,
        params
      );
      return result.rows[0] || null;
    },
    async replaceInsights(interactionId, insights) {
      await query(
        `DELETE FROM relationship_interaction_insights WHERE interaction_id = $1`,
        [String(interactionId)]
      );
      const rows = [];
      for (const i of insights || []) {
        const result = await query(
          `INSERT INTO relationship_interaction_insights (
             id, interaction_id, kind, label, value, confidence, source_quote, created_at
           ) VALUES ($1,$2,$3,$4,$5,$6,$7,NOW())
           RETURNING *`,
          [
            newId(),
            String(interactionId),
            i.kind,
            i.label || '',
            i.value || '',
            i.confidence,
            i.sourceQuote != null ? i.sourceQuote : null,
          ]
        );
        rows.push(result.rows[0]);
      }
      return rows;
    },
    async listInsights(interactionId) {
      const result = await query(
        `SELECT * FROM relationship_interaction_insights
         WHERE interaction_id = $1
         ORDER BY created_at ASC`,
        [String(interactionId)]
      );
      return result.rows;
    },
    async listInteractions(filters = {}) {
      const where = [];
      const params = [];
      let n = 1;
      if (filters.clientId != null) {
        where.push(`client_id = $${n++}`);
        params.push(filters.clientId);
      }
      if (filters.status) {
        where.push(`status = $${n++}`);
        params.push(String(filters.status));
      }
      if (filters.companyId) {
        where.push(`company_id = $${n++}`);
        params.push(String(filters.companyId));
      }
      const limit = Math.min(Math.max(Number(filters.limit) || 50, 1), 200);
      params.push(limit);
      const sql = `
        SELECT * FROM relationship_interactions
        ${where.length ? `WHERE ${where.join(' AND ')}` : ''}
        ORDER BY occurred_at DESC
        LIMIT $${n}`;
      const result = await query(sql, params);
      return result.rows;
    },
  };
}

function resolveStore(options = {}) {
  if (options.store) return options.store;
  return createPostgresStore(options.pool || defaultPool);
}

function parseInterviewState(row) {
  const raw = row && row.interview_state;
  if (!raw) return initialInterviewState();
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw);
    } catch (_) {
      return initialInterviewState();
    }
  }
  return { ...raw };
}

function parseStructuredSummary(row) {
  const raw = row && row.structured_summary;
  if (!raw) return {};
  if (typeof raw === 'string') {
    try {
      return JSON.parse(raw);
    } catch (_) {
      return {};
    }
  }
  return raw;
}

/* -------------------------------------------------------------------------- */
/* Public API                                                                 */
/* -------------------------------------------------------------------------- */

/**
 * @param {object} context
 * @param {object} [options]
 */
async function startRelationshipInterview(context = {}, options = {}) {
  const store = resolveStore(options);
  const interactionType = assertInteractionType(
    context.interactionType || context.type || 'other'
  );
  const notes = context.notes != null ? String(context.notes) : null;
  const state = initialInterviewState({ notes });
  const id = newId();
  const row = {
    id,
    client_id: asClientId(context.clientId),
    company_id: asText(context.companyId),
    contact_id: asText(context.contactId),
    opportunity_id: asText(context.opportunityId),
    user_id: asText(context.userId),
    interaction_type: interactionType,
    occurred_at: parseOccurredAt(context.occurredAt),
    source: asText(context.source) || (notes ? 'cli_notes' : 'max_interview'),
    status: 'draft',
    raw_summary: null,
    structured_summary: {},
    confidence: null,
    interview_state: state,
    created_at: new Date(),
    updated_at: new Date(),
  };

  const saved = await store.insertInteraction(row);
  const liveState = parseInterviewState(saved);
  const current = currentQuestion(liveState);

  return {
    interviewId: saved.id,
    status: saved.status,
    mode: liveState.mode,
    done: Boolean(liveState.done),
    question: current
      ? { id: current.question.id, prompt: current.question.prompt }
      : null,
    message: current
      ? current.question.prompt
      : notes
        ? 'Notes captured. Ready to summarize.'
        : 'Interview complete. Ready to summarize.',
  };
}

/**
 * @param {string} sessionId
 * @param {string} message
 * @param {object} [options]
 */
async function answerRelationshipInterview(sessionId, message, options = {}) {
  const store = resolveStore(options);
  const row = await store.getInteraction(sessionId);
  if (!row) {
    throw new RelationshipIntelligenceError('not_found', 'Interview not found', 404);
  }
  if (row.status === 'committed') {
    throw new RelationshipIntelligenceError(
      'already_committed',
      'Committed interviews cannot accept new answers',
      409
    );
  }

  const state = parseInterviewState(row);
  if (state.mode === 'notes') {
    throw new RelationshipIntelligenceError(
      'notes_mode',
      'This interview used notes mode; call summarize instead of answering prompts'
    );
  }
  if (state.done) {
    return {
      interviewId: row.id,
      done: true,
      question: null,
      message: 'Interview complete. Ready to summarize.',
    };
  }

  const current = currentQuestion(state);
  if (!current) {
    const doneState = { ...state, done: true };
    await store.updateInteraction(row.id, { interview_state: doneState });
    return {
      interviewId: row.id,
      done: true,
      question: null,
      message: 'Interview complete. Ready to summarize.',
    };
  }

  const text = String(message == null ? '' : message).trim();
  if (!text) {
    throw new RelationshipIntelligenceError('empty_message', 'message is required');
  }

  const nextState = advanceAfterAnswer(state, current.question.id, text);
  await store.updateInteraction(row.id, { interview_state: nextState });

  if (nextState.done) {
    return {
      interviewId: row.id,
      done: true,
      question: null,
      message: 'Interview complete. Ready to summarize.',
    };
  }

  const nextQ = currentQuestion(nextState);
  return {
    interviewId: row.id,
    done: false,
    question: nextQ
      ? { id: nextQ.question.id, prompt: nextQ.question.prompt }
      : null,
    message: nextQ ? nextQ.question.prompt : 'Interview complete. Ready to summarize.',
  };
}

/**
 * @param {string} sessionId
 * @param {object} [options]
 */
async function summarizeRelationshipInterview(sessionId, options = {}) {
  const store = resolveStore(options);
  const row = await store.getInteraction(sessionId);
  if (!row) {
    throw new RelationshipIntelligenceError('not_found', 'Interview not found', 404);
  }
  if (row.status === 'committed') {
    const insights = await store.listInsights(row.id);
    return buildPayload(
      { ...row, structured_summary: parseStructuredSummary(row) },
      insights.map((i) => ({
        kind: i.kind,
        label: i.label,
        value: i.value,
        confidence: i.confidence,
        sourceQuote: i.source_quote,
      }))
    );
  }

  const state = parseInterviewState(row);
  const extraction =
    state.mode === 'notes'
      ? extractInsightsFromNotes(state.notes || '')
      : extractInsightsFromAnswers(state);

  for (const insight of extraction.insights) {
    assertInsightKind(insight.kind);
  }

  const rawSummary = buildRawSummary(state, extraction);
  const structuredSummary = {
    mode: state.mode,
    answerCount: Object.keys(state.answers || {}).length,
    insightCount: extraction.insights.length,
    thin: Boolean(extraction.thin),
    generatedAt: nowIso(),
  };

  const nextState = { ...state, summarized: true, done: true };
  const updated = await store.updateInteraction(row.id, {
    raw_summary: rawSummary,
    structured_summary: structuredSummary,
    confidence: extraction.confidence,
    interview_state: nextState,
    status: 'draft',
  });

  const insightRows = await store.replaceInsights(
    row.id,
    extraction.insights.map(normalizeInsight)
  );

  return buildPayload(
    {
      ...updated,
      structured_summary: parseStructuredSummary(updated),
    },
    insightRows.map((i) => ({
      kind: i.kind,
      label: i.label,
      value: i.value,
      confidence: i.confidence,
      sourceQuote: i.source_quote,
    })),
    {
      caveats: extraction.caveats || [],
      nextSteps: extraction.nextSteps || [],
    }
  );
}

/**
 * @param {string} sessionId
 * @param {object} [options]
 */
async function commitRelationshipInterview(sessionId, options = {}) {
  const store = resolveStore(options);
  const row = await store.getInteraction(sessionId);
  if (!row) {
    throw new RelationshipIntelligenceError('not_found', 'Interview not found', 404);
  }
  if (row.status === 'committed') {
    const insights = await store.listInsights(row.id);
    return buildPayload(
      { ...row, structured_summary: parseStructuredSummary(row) },
      insights.map((i) => ({
        kind: i.kind,
        label: i.label,
        value: i.value,
        confidence: i.confidence,
        sourceQuote: i.source_quote,
      }))
    );
  }

  const state = parseInterviewState(row);
  if (!state.summarized && !row.raw_summary) {
    throw new RelationshipIntelligenceError(
      'summary_required',
      'Summarize the interview before commit'
    );
  }

  const updated = await store.updateInteraction(row.id, { status: 'committed' });
  const insights = await store.listInsights(row.id);
  const payload = buildPayload(
    { ...updated, structured_summary: parseStructuredSummary(updated) },
    insights.map((i) => ({
      kind: i.kind,
      label: i.label,
      value: i.value,
      confidence: i.confidence,
      sourceQuote: i.source_quote,
    }))
  );
  payload.status = 'committed';
  return payload;
}

/**
 * @param {string} sessionId
 * @param {object} [options]
 */
async function getInterview(sessionId, options = {}) {
  const store = resolveStore(options);
  const row = await store.getInteraction(sessionId);
  if (!row) {
    throw new RelationshipIntelligenceError('not_found', 'Interview not found', 404);
  }
  const state = parseInterviewState(row);
  const insights = await store.listInsights(row.id);
  const current = currentQuestion(state);
  return {
    interviewId: row.id,
    status: row.status,
    mode: state.mode,
    done: Boolean(state.done),
    summarized: Boolean(state.summarized),
    question: current
      ? { id: current.question.id, prompt: current.question.prompt }
      : null,
    interaction: {
      interactionType: row.interaction_type,
      companyId: row.company_id,
      contactId: row.contact_id,
      opportunityId: row.opportunity_id,
      occurredAt:
        row.occurred_at instanceof Date
          ? row.occurred_at.toISOString()
          : row.occurred_at,
      rawSummary: row.raw_summary,
      structuredSummary: parseStructuredSummary(row),
      confidence: row.confidence == null ? null : Number(row.confidence),
    },
    insights: insights.map((i) =>
      normalizeInsight({
        kind: i.kind,
        label: i.label,
        value: i.value,
        confidence: i.confidence,
        sourceQuote: i.source_quote,
      })
    ),
    interviewState: {
      stepIndex: state.stepIndex,
      answers: state.answers || {},
      notes: state.notes || null,
    },
  };
}

/**
 * @param {object} [filters]
 * @param {object} [options]
 */
async function listInteractions(filters = {}, options = {}) {
  const store = resolveStore(options);
  const rows = await store.listInteractions({
    clientId: asClientId(filters.clientId),
    status: filters.status || undefined,
    companyId: asText(filters.companyId),
    limit: filters.limit,
  });
  return rows.map((row) => ({
    id: row.id,
    status: row.status,
    interactionType: row.interaction_type,
    companyId: row.company_id,
    contactId: row.contact_id,
    opportunityId: row.opportunity_id,
    occurredAt:
      row.occurred_at instanceof Date
        ? row.occurred_at.toISOString()
        : row.occurred_at,
    confidence: row.confidence == null ? null : Number(row.confidence),
    rawSummary: row.raw_summary,
    source: row.source,
  }));
}

/**
 * @param {string} id
 * @param {object} [options]
 */
async function getInteraction(id, options = {}) {
  const store = resolveStore(options);
  const row = await store.getInteraction(id);
  if (!row) {
    throw new RelationshipIntelligenceError('not_found', 'Interaction not found', 404);
  }
  const insights = await store.listInsights(row.id);
  return buildPayload(
    { ...row, structured_summary: parseStructuredSummary(row) },
    insights.map((i) => ({
      kind: i.kind,
      label: i.label,
      value: i.value,
      confidence: i.confidence,
      sourceQuote: i.source_quote,
    }))
  );
}

module.exports = {
  INTERACTION_TYPES,
  INTERACTION_STATUSES,
  INSIGHT_KINDS,
  QUESTION_BANK,
  RelationshipIntelligenceError,
  createMemoryStore,
  createPostgresStore,
  assertAllowedSql,
  assertInsightKind,
  extractInsightsFromNotes,
  splitNotesClauses,
  startRelationshipInterview,
  answerRelationshipInterview,
  summarizeRelationshipInterview,
  commitRelationshipInterview,
  getInterview,
  listInteractions,
  getInteraction,
};
