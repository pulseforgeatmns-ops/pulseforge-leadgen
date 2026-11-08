'use strict';

/**
 * P0 question-directed reasoning — mission-level answers over operational
 * evidence.
 *
 * Given an analytical frame (OperationalQuestionFrame) whose subject is one or
 * more missions, compose the shortest complete answer to the facets the
 * operator asked about, grounded only in the mission records of the evidence
 * (or a tenant-scoped read-only lookup). Each facet is a small reasoning
 * function over the same four layers, kept explicit so a database
 * classification is never presented as a business conclusion:
 *
 *   observed fact        — persisted timestamps, stage/status, blockers
 *   classification rule  — the documented inspection criteria
 *   interpretation       — what the fact can and cannot mean for the business
 *   decision requirement — what evidence / authority a change would need
 *
 * Nothing here reads a database, calls a mission runtime, or touches a
 * pending decision. It returns prose plus structured conclusions and
 * uncertainties for the response metadata.
 */

const { CATEGORIES } = require('./OperationalInspection');
const { MISSION_FACETS } = require('./OperationalQuestionFrame');

const DAY_MS = 24 * 60 * 60 * 1000;

function fmt(ts) {
  return ts ? String(ts).replace('T', ' ').replace(/\.\d+Z$/, 'Z') : 'not recorded';
}

function day(ts) {
  if (!ts) return 'an unrecorded date';
  const d = new Date(ts);
  if (Number.isNaN(d.getTime())) return 'an unrecorded date';
  return d.toLocaleDateString('en-US', { month: 'long', day: 'numeric', year: 'numeric', timeZone: 'UTC' });
}

/** Quote persisted text without doubling its closing punctuation. */
function q(text) {
  return `"${String(text).trim().replace(/[.\s]+$/, '')}"`;
}

function hours(n) {
  const h = Math.max(1, Math.round(n));
  return `${h} hour${h === 1 ? '' : 's'}`;
}

function name(m) {
  return m.title ? `"${m.title}" [${m.id}]` : `[${m.id}]`;
}

function shortName(m) {
  return m.title ? `"${m.title}"` : `mission ${m.id}`;
}

const CATEGORY_WORDS = Object.freeze({
  [CATEGORIES.ACTIVE]: 'active',
  [CATEGORIES.STALE]: 'stale',
  [CATEGORIES.COMPLETED]: 'completed / closed',
  [CATEGORIES.DEVELOPMENT_TEST]: 'development / test',
  [CATEGORIES.UNCERTAIN]: 'uncertain',
});

// ---------------------------------------------------------------------------
// Facts derived from one mission record
// ---------------------------------------------------------------------------

function activitySource(m) {
  const last = m.lastActivityAt;
  if (!last) return null;
  const same = (ts) => ts && new Date(ts).getTime() === new Date(last).getTime();
  if (m.lastEvent && same(m.lastEvent.at)) {
    const what = [m.lastEvent.label, m.lastEvent.kind].filter(Boolean);
    return `latest mission event${what.length ? ` (${what.map((w) => `"${w}"`).join(', ')})` : ''}`;
  }
  if (same(m.updatedAt)) return m.system === 'mission_engine' ? 'missions.updated_at' : 'acquisition_missions.updated_at';
  if (same(m.completedAt)) return 'missions.completed_at';
  return 'persisted mission timestamps';
}

function missionFacts(m, asOf, staleAfterDays) {
  const lastMs = m.lastActivityAt ? new Date(m.lastActivityAt).getTime() : NaN;
  const asOfMs = new Date(asOf).getTime();
  const ageDays = Number.isFinite(lastMs) && Number.isFinite(asOfMs) ? Math.max(0, (asOfMs - lastMs) / DAY_MS) : null;
  const pending = (m.blockers || []).find((b) => b.kind === 'awaiting_operator_decision') || null;
  return {
    ageDays,
    ageText: ageDays == null ? null : ageDays < 2 ? hours(ageDays * 24) : `${ageDays.toFixed(1)} days`,
    margin: ageDays == null ? null : ageDays - staleAfterDays,
    source: activitySource(m),
    pending,
    lifecycle: [m.stage && `stage ${m.stage}`, m.status && `status ${m.status}`].filter(Boolean).join(' / ') || 'no stage or status recorded',
    terminal: m.category === CATEGORIES.COMPLETED,
  };
}

function marginText(f, staleAfterDays) {
  if (f.margin == null) return '';
  if (f.margin <= 0) return `within the ${staleAfterDays}-day threshold`;
  if (f.margin < 1) {
    return `just past the ${staleAfterDays}-day inactivity threshold (by about ${hours(f.margin * 24)}) — the classification is marginal and turns entirely on that one timestamp`;
  }
  return `${f.margin.toFixed(1)} days past the ${staleAfterDays}-day inactivity threshold`;
}

function canonicalOf(record) {
  const active = record.missions.filter((m) => m.category === CATEGORIES.ACTIVE && !m.synthetic && m.objective);
  return active.length === 1 ? active[0] : null;
}

const OVERLAP_STOP = new Set([
  'the', 'and', 'for', 'with', 'from', 'into', 'that', 'this', 'are', 'our', 'their', 'win', 'get', 'new', 'more',
  'mission', 'missions', 'anchor', 'cleaning', 'records', 'record', 'work', 'prepare', 'preparation',
]);

function terms(text) {
  return new Set(
    String(text || '')
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((t) => t.length > 3 && !OVERLAP_STOP.has(t))
      .map((t) => (t.length > 6 ? t.slice(0, 6) : t))
  );
}

function sharedTerms(a, b) {
  const tb = terms(b);
  const words = String(a || '').toLowerCase().split(/[^a-z0-9]+/).filter((t) => t.length > 3 && !OVERLAP_STOP.has(t));
  const out = [];
  for (const w of words) {
    const s = w.length > 6 ? w.slice(0, 6) : w;
    if (tb.has(s) && !out.includes(w)) out.push(w);
  }
  return out;
}

// ---------------------------------------------------------------------------
// Facets — each returns { lead, heading, lines, uncertainties, conclusions }
// ---------------------------------------------------------------------------

function classificationFacet(m, ctx) {
  const { record } = ctx;
  const n = record.staleAfterDays;
  const f = missionFacts(m, record.asOf, n);
  const lines = [];
  const uncertainties = [];
  let lead;
  if (m.category === CATEGORIES.STALE) {
    lead = `${name(m)} is classified as stale because its last recorded activity was ${fmt(m.lastActivityAt)}, and the inspection ran at ${fmt(record.asOf)} — ${f.ageText} later, ${marginText(f, n)}.`;
    lines.push(`- Observed fact: last recorded activity ${fmt(m.lastActivityAt)}${f.source ? ` (${f.source})` : ''}; ${f.lifecycle}.`);
    lines.push(`- Classification rule: a non-terminal mission with no recorded activity for more than ${n} days is stale. It has no completed, cancelled, or archived status, so the inactivity rule decides.`);
    lines.push('- What that does not establish: staleness only says no sufficiently recent activity was recorded. It does not show the mission is abandoned, invalid, or superseded — no lifecycle marker for any of those is persisted.');
    uncertainties.push('whether the mission is deliberately parked, abandoned, or superseded (no persisted lifecycle marker)');
  } else if (m.category === CATEGORIES.ACTIVE) {
    lead = `${name(m)} is classified as active: its last recorded activity (${fmt(m.lastActivityAt)}) was ${f.ageText} before the inspection, within the ${n}-day threshold.`;
    lines.push(`- Observed fact: last recorded activity ${fmt(m.lastActivityAt)}${f.source ? ` (${f.source})` : ''}; ${f.lifecycle}.`);
    lines.push('- What that does not establish: recent activity is not proof the work is valuable or still aligned with the current objective.');
  } else if (m.category === CATEGORIES.UNCERTAIN) {
    lead = `${name(m)} is classified as uncertain: ${m.basis}.`;
    lines.push(`- Observed fact: ${m.lastActivityAt ? `last recorded activity ${fmt(m.lastActivityAt)}${f.source ? ` (${f.source})` : ''}` : 'no activity timestamp is persisted'}; ${f.lifecycle}.`);
    lines.push('- Rule: the inspection labels a mission uncertain rather than guessing when its persisted markers are insufficient — it is not classified as test, stale, or active from its name alone.');
    uncertainties.push('the true status of this mission (it needs operator confirmation)');
  } else {
    lead = `${name(m)} is classified as ${CATEGORY_WORDS[m.category] || m.category}: ${m.basis}.`;
    lines.push(`- Observed fact: ${f.lifecycle}; last recorded activity ${fmt(m.lastActivityAt)}.`);
  }
  return { lead, heading: 'Why it has this classification', lines, uncertainties };
}

function objectiveContext(m, ctx) {
  const canonical = canonicalOf(ctx.record);
  const lines = [];
  if (canonical && canonical.id === m.id) {
    lines.push('- It is the only active, non-synthetic mission with a persisted objective, so it carries the current canonical objective.');
    return { lines, canonical, isCanonical: true };
  }
  if (canonical) {
    lines.push(`- The current canonical objective is persisted on ${name(canonical)}: ${q(canonical.objective)}.`);
  } else {
    lines.push('- No active, non-synthetic mission carries a persisted objective, so there is no persisted current objective to measure it against.');
  }
  if (ctx.blueprintGoal) lines.push(`- Declared business goal (approved Client Blueprint — operator intent, not execution evidence): ${q(ctx.blueprintGoal)}.`);
  return { lines, canonical, isCanonical: false };
}

function reconsiderationFacet(m, ctx) {
  const { record } = ctx;
  const f = missionFacts(m, record.asOf, record.staleAfterDays);
  const uncertainties = [];
  if (m.category === CATEGORIES.ACTIVE) {
    return {
      lead: `${name(m)} is already classified as active, so nothing is needed to treat it as current by the inspection's rule; whether it is the right work is a separate judgment.`,
      heading: 'What would justify treating it as current',
      lines: objectiveContext(m, ctx).lines,
      uncertainties,
    };
  }
  if (m.category === CATEGORIES.COMPLETED || m.category === CATEGORIES.DEVELOPMENT_TEST) {
    return {
      lead: `${name(m)} is ${CATEGORY_WORDS[m.category]} by a persisted marker (${m.basis}); treating it as current would contradict that record and would need an explicit operator decision, not new evidence alone.`,
      heading: 'What would justify treating it as current',
      lines: [],
      uncertainties,
    };
  }
  const obj = objectiveContext(m, ctx);
  const lines = [];
  lines.push(`- Relationship to the present business objective. Its persisted objective: ${m.objective ? q(m.objective) : 'none recorded'}.`);
  lines.push(...obj.lines.map((l) => `  ${l}`));
  lines.push('  Whether these describe the same work is a judgment the persisted text does not settle.');
  lines.push(`- Its latest persisted lifecycle state: ${f.lifecycle}. That is non-terminal, which fits parked work and abandoned work equally; it does not distinguish them.`);
  lines.push(`- Whether the work it prepared is still valid: the inspection does not read the mission's artifacts or contributions, so I cannot say whether what it prepared${m.stage ? ` up to stage ${m.stage}` : ''} still matches today's data.`);
  lines.push('- A deliberate operator decision to retain it as active work: none is persisted.');
  const notEnough = [];
  if (f.pending) {
    notEnough.push(`- Its pending decision (${f.pending.label.replace(/^Awaiting operator decision:\s*/, '')}) has stayed open with no recorded activity since ${fmt(m.lastActivityAt)}. That makes it historical state: an open approval request is not evidence that the mission should resume.`);
  }
  notEnough.push('- New activity alone: any new event would reset the inactivity clock and change the classification, but activity does not establish business relevance.');
  uncertainties.push('the relationship of this mission to the present objective', 'whether its prepared work is still valid (artifacts not read)');
  return {
    lead: m.category === CATEGORIES.UNCERTAIN
      ? `To treat ${shortName(m)} as current I would first need the marker its classification is missing (confirmation from you, or a persisted lifecycle/test marker), and then the same relevance evidence as any stale mission:`
      : `To determine whether ${shortName(m)} should be treated as current again, I would need evidence the inspection does not hold:`,
    heading: 'What would justify treating it as current again',
    lines,
    extra: { heading: 'Not sufficient on its own', lines: notEnough },
    uncertainties,
  };
}

function relevanceFacet(m, ctx) {
  const { record } = ctx;
  const f = missionFacts(m, record.asOf, record.staleAfterDays);
  const obj = objectiveContext(m, ctx);
  const supports = [];
  const missing = [];
  if (obj.isCanonical) {
    supports.push('- It is the only active, non-synthetic mission with a persisted objective, so it carries the canonical objective.');
  }
  if (!f.terminal && m.category !== CATEGORIES.DEVELOPMENT_TEST) {
    supports.push('- It is non-terminal: no completed, cancelled, or archived status is persisted, so it has not been closed.');
  }
  const goal = (obj.canonical && !obj.isCanonical && obj.canonical.objective) || ctx.blueprintGoal || null;
  if (!obj.isCanonical && m.objective && goal) {
    const shared = sharedTerms(`${m.title || ''} ${m.objective}`, goal);
    supports.push(
      shared.length
        ? `- Its title/objective shares terms with the ${obj.canonical ? 'canonical objective' : 'declared business goal'} (${shared.slice(0, 5).join(', ')}). That is textual overlap, not proof that it is the same work.`
        : `- Its persisted objective (${q(m.objective)}) shares no significant terms with the ${obj.canonical ? 'canonical objective' : 'declared business goal'}; that alone does not make it irrelevant.`
    );
  }
  if (f.pending) supports.push(`- It holds a pending decision (${f.pending.label.replace(/^Awaiting operator decision:\s*/, '')}) — evidence that it once reached that gate, not that it is still wanted.`);

  if (m.category === CATEGORIES.STALE) missing.push(`- Recent activity: none for ${f.ageText} (${CATEGORY_WORDS[m.category]} by the ${record.staleAfterDays}-day rule).`);
  if (m.category === CATEGORIES.UNCERTAIN) missing.push(`- A reliable classification: ${m.basis}.`);
  if (!obj.canonical) missing.push('- A persisted current objective: no active, non-synthetic mission carries one, so relevance cannot be measured against execution state.');
  if (!obj.isCanonical) missing.push('- Any persisted link between this mission and the current objective (for example, an operator note that it is retained or superseded).');
  missing.push('- Whether the work it prepared is still valid (its artifacts and contributions were not read).');
  if (!m.objective) missing.push('- Its own objective: none is persisted.');

  const lead = obj.isCanonical
    ? `The evidence ties ${shortName(m)} directly to ${ctx.tenantName ? `${ctx.tenantName}'s` : 'the tenant\'s'} current objective: it is the mission that carries it.`
    : `The persisted evidence neither establishes that ${shortName(m)} is relevant to the current objective nor that it is irrelevant.`;
  const lines = [...obj.lines, 'Evidence that supports relevance:', ...(supports.length ? supports : ['- none beyond its existence in the tenant\'s mission store'])];
  return {
    lead,
    heading: 'Relevance to the current objective',
    lines,
    extra: { heading: 'Evidence that is missing', lines: missing },
    uncertainties: obj.isCanonical ? [] : ['whether this mission still serves the current objective'],
  };
}

function abandonmentFacet(m, ctx) {
  const { record } = ctx;
  const n = record.staleAfterDays;
  const f = missionFacts(m, record.asOf, n);
  if (m.category === CATEGORIES.COMPLETED) {
    return { lead: `${name(m)} is already closed by a persisted marker (${m.basis}); there is nothing to abandon.`, heading: null, lines: [], uncertainties: [] };
  }
  const lead = m.category === CATEGORIES.STALE
    ? `No. Stale means ${shortName(m)} has had no recorded activity for more than ${n} days; that alone does not establish that it should be abandoned.`
    : `No. ${shortName(m)} is ${CATEGORY_WORDS[m.category] || m.category}, and nothing in the evidence establishes that it should be abandoned.`;
  const lines = [
    `- Observed fact: last recorded activity ${fmt(m.lastActivityAt)}${f.ageText ? ` (${f.ageText} before the inspection)` : ''}; ${f.lifecycle}.`,
    `- Classification rule: ${m.category === CATEGORIES.STALE ? `inactivity beyond ${n} days on a non-terminal mission → stale` : m.basis}.`,
    '- Business interpretation: the mission may be parked, abandoned, superseded, or still relevant. Persisted state does not distinguish these.',
    '- Decision requirement: abandoning it (closing or cancelling it) is an operator decision made through the mission\'s own controls, ideally after checking its relevance to the current objective and whether newer work supersedes it. I have not changed it.',
  ];
  return { lead, heading: 'Classification versus business judgment', lines, uncertainties: ['whether the mission is still wanted (an operator judgment)'] };
}

function historyFacet(m, ctx) {
  const { record } = ctx;
  const f = missionFacts(m, record.asOf, record.staleAfterDays);
  const lines = [
    `- Created: ${fmt(m.createdAt)}.`,
    `- Last recorded activity: ${fmt(m.lastActivityAt)}${f.source ? ` (${f.source})` : ''}.`,
    `- Lifecycle: ${f.lifecycle}; classified ${CATEGORY_WORDS[m.category] || m.category} (${m.basis}).`,
  ];
  if (f.pending) lines.push(`- Open decision: ${f.pending.label.replace(/^Awaiting operator decision:\s*/, '')}.`);
  for (const b of (m.blockers || []).filter((x) => x.kind !== 'awaiting_operator_decision')) lines.push(`- Recorded: ${b.label}.`);
  return {
    lead: `What the evidence records for ${name(m)}: last activity ${fmt(m.lastActivityAt)}, ${f.lifecycle}, currently ${CATEGORY_WORDS[m.category] || m.category}. The inspection reads the latest event per mission, not the full event history.`,
    heading: 'Recorded history',
    lines,
    uncertainties: ['events before the latest one (the inspection reads only the latest event per mission)'],
  };
}

function evidenceFacet(m, ctx) {
  const c = classificationFacet(m, ctx);
  const h = historyFacet(m, ctx);
  return { lead: c.lead, heading: 'What the evidence records', lines: h.lines, uncertainties: [...c.uncertainties, ...h.uncertainties] };
}

function comparisonFacet(ms, ctx) {
  const { record } = ctx;
  const facts = ms.map((m) => ({ m, f: missionFacts(m, record.asOf, record.staleAfterDays) }));
  const lines = facts.map(({ m, f }) =>
    `- ${name(m)}: last recorded activity ${fmt(m.lastActivityAt)}${f.source ? ` (${f.source})` : ''}; created ${fmt(m.createdAt)}; ${f.lifecycle}; ${CATEGORY_WORDS[m.category] || m.category}.`
  );
  const timed = facts.filter((x) => x.m.lastActivityAt).sort((a, b) => new Date(b.m.lastActivityAt) - new Date(a.m.lastActivityAt));
  let lead;
  const conclusions = [];
  if (timed.length < 2) {
    lead = 'I cannot compare their recency: at least one of them has no persisted activity timestamp.';
  } else {
    const [a, b] = timed;
    const gap = (new Date(a.m.lastActivityAt) - new Date(b.m.lastActivityAt)) / DAY_MS;
    lead = gap === 0
      ? `${shortName(a.m)} and ${shortName(b.m)} have the same last recorded activity (${fmt(a.m.lastActivityAt)}).`
      : `${shortName(a.m)} has the more recent recorded activity: ${fmt(a.m.lastActivityAt)}, ${gap < 2 ? `${(gap * 24).toFixed(0)} hours` : `${gap.toFixed(1)} days`} after ${shortName(b.m)} (${fmt(b.m.lastActivityAt)}).`;
    const cats = new Set(ms.map((m) => m.category));
    if (cats.size === 1) {
      const cat = ms[0].category;
      conclusions.push(`- Both are ${CATEGORY_WORDS[cat] || cat}${cat === CATEGORIES.STALE ? ` — both are beyond the ${record.staleAfterDays}-day threshold, so the gap does not change either classification` : ''}.`);
    } else {
      conclusions.push(`- Their classifications differ (${facts.map(({ m }) => `${shortName(m)}: ${CATEGORY_WORDS[m.category] || m.category}`).join('; ')}).`);
    }
  }
  conclusions.push('- What this establishes: only which mission was touched more recently.');
  conclusions.push('- What it does not establish: which is more valuable or more relevant to the current objective, whether either is still needed, or whether one superseded the other — no persisted link between them was read.');
  return { lead, heading: 'Recorded activity', lines, extra: { heading: 'What we can conclude', lines: conclusions }, uncertainties: ['relative business value or relevance of the compared missions'] };
}

const FACET_COMPOSERS = {
  [MISSION_FACETS.CLASSIFICATION]: classificationFacet,
  [MISSION_FACETS.RECONSIDERATION]: reconsiderationFacet,
  [MISSION_FACETS.RELEVANCE]: relevanceFacet,
  [MISSION_FACETS.ABANDONMENT]: abandonmentFacet,
  [MISSION_FACETS.HISTORY]: historyFacet,
  [MISSION_FACETS.EVIDENCE]: evidenceFacet,
};

// ---------------------------------------------------------------------------
// Compose
// ---------------------------------------------------------------------------

/**
 * @param {object} frame — OperationalQuestionFrame (subject 'mission')
 * @param {object} ctx
 * @param {object} ctx.record — evidence record
 * @param {object[]} ctx.missions — resolved mission records ({ …, origin: 'evidence'|'read_now' })
 * @param {object[]} [ctx.lookups] — unresolved ids looked up read-only ({ text, found, sources })
 * @param {string|null} [ctx.blueprintGoal]
 * @param {string} ctx.evidenceUse — 'reused' | 'refreshed'
 * @param {string|null} [ctx.refreshedReason]
 * @param {string[]} [ctx.constraints]
 * @param {string} [ctx.tenantLabel]
 */
function composeMissionAnswer(frame, ctx) {
  const { record, missions = [], lookups = [] } = ctx;
  const paragraphs = [];
  const uncertainties = [];
  const conclusions = [];
  const leads = [];

  const sections = [];
  if (missions.length >= 2 && frame.facets.includes(MISSION_FACETS.COMPARISON)) {
    const c = comparisonFacet(missions, ctx);
    leads.push(c.lead);
    sections.push(c);
    uncertainties.push(...c.uncertainties);
  }
  const singleFacets = frame.facets.filter((f) => f !== MISSION_FACETS.COMPARISON);
  const perMission = missions.length >= 2 && sections.length ? [] : missions;
  for (const m of perMission) {
    const facets = singleFacets.length ? singleFacets : [MISSION_FACETS.EVIDENCE];
    for (const facet of facets) {
      const out = FACET_COMPOSERS[facet](m, ctx);
      leads.push(out.lead);
      sections.push(out);
      uncertainties.push(...(out.uncertainties || []));
    }
  }
  // Second and later facets on a comparison turn (e.g. "… and is either still relevant?").
  if (missions.length >= 2 && sections.length === 1 && singleFacets.length) {
    for (const m of missions) {
      for (const facet of singleFacets.filter((f) => f !== MISSION_FACETS.EVIDENCE && f !== MISSION_FACETS.HISTORY)) {
        const out = FACET_COMPOSERS[facet](m, ctx);
        sections.push({ ...out, heading: out.heading ? `${out.heading} — ${shortName(m)}` : null });
        leads.push(out.lead);
        uncertainties.push(...(out.uncertainties || []));
      }
    }
  }

  // Disclosures for what could not be resolved.
  const disclosures = [];
  for (const l of lookups.filter((x) => !x.found)) {
    const unavailable = (l.sources || []).filter((s) => !s.available);
    disclosures.push(
      unavailable.length && unavailable.length === (l.sources || []).length
        ? `I cannot verify mission ${l.text}: it is not in the evidence from this conversation, and the mission stores could not be read now (${unavailable.map((s) => `${s.source}: ${s.reason}`).join('; ')}).`
        : `I cannot find a mission with id ${l.text} in ${ctx.tenantLabel || 'this tenant'}'s persisted mission stores (acquisition_missions, missions), so I cannot verify anything about it. Only this workspace's tenant was searched.`
    );
    uncertainties.push(`mission ${l.text} (not found for this tenant)`);
  }
  for (const u of frame.unresolved.filter((x) => x.kind === 'description')) {
    disclosures.push(`I cannot match ${u.text.replace(/^(?:that|this|our)\b/i, 'the')} to a mission in the evidence (as of ${fmt(record.asOf)}). Give me its mission id and I will read it, read-only.`);
    uncertainties.push(`which mission "${u.text}" refers to`);
  }
  for (const c of frame.candidates) {
    const options = c.missionIds
      .map((id) => record.missions.find((m) => m.id === id))
      .filter(Boolean)
      .slice(0, 6)
      .map((m) => `  • ${name(m)} — last activity ${fmt(m.lastActivityAt)}; ${CATEGORY_WORDS[m.category] || m.category}`);
    disclosures.push([`"${c.text}" matches ${c.missionIds.length} missions in the evidence; which one do you mean?`, ...options].join('\n'));
    uncertainties.push(`which mission "${c.text}" refers to (ambiguous)`);
  }

  if (leads.length) paragraphs.push(leads[0]);
  if (disclosures.length) paragraphs.push(disclosures.join('\n\n'));
  sections.forEach((s, i) => {
    const block = [];
    // A later facet with its own heading does not need its lead sentence too.
    if (i > 0 && s.lead && s.lead !== leads[0] && !(s.heading && s.lines.length)) block.push(s.lead);
    if (s.heading && s.lines.length) block.push(s.heading, ...s.lines);
    else if (s.lines.length) block.push(...s.lines);
    if (block.length) paragraphs.push(block.join('\n'));
    if (s.extra && s.extra.lines.length) paragraphs.push([s.extra.heading, ...s.extra.lines].join('\n'));
  });
  for (const s of sections) conclusions.push(s.lead);

  // Evidence basis — short; cites provenance, discloses refreshed/read-now records.
  const basis = [];
  basis.push(
    ctx.evidenceUse === 'reused'
      ? `Evidence: the read-only inspection retrieved earlier in this conversation (as of ${fmt(record.asOf)}); state may have changed since.`
      : `Evidence: a fresh read-only inspection as of ${fmt(record.asOf)} (${ctx.refreshedReason}).`
  );
  const readNow = missions.filter((m) => m.origin === 'read_now');
  for (const m of readNow) basis.push(`${name(m)} was not in that evidence; I read its record just now, read-only and scoped to this tenant (as of ${fmt(m.readAt)}).`);
  const missionSources = record.sources.filter((s) => s.source === 'acquisition_missions' || s.source === 'missions');
  const down = missionSources.filter((s) => !s.available);
  if (down.length) basis.push(`Unavailable mission source(s): ${down.map((s) => `${s.source} (${s.reason})`).join(', ')} — missions held only there are not covered.`);
  if (missions.some((m) => m.category !== CATEGORIES.COMPLETED)) {
    basis.push('Changing a classification, resuming, or closing a mission would need your explicit authorization through that mission\'s own approval path.');
  }
  if (ctx.constraints && ctx.constraints.length) {
    basis.push(`Your constraints were honoured: ${ctx.constraints.map((c) => `"${c.replace(/[.\s]+$/, '')}"`).join('; ')}.`);
  }
  basis.push('No changes were made.');
  paragraphs.push(basis.join('\n'));

  return {
    prose: paragraphs.join('\n\n'),
    conclusions,
    uncertainties: [...new Set(uncertainties)],
  };
}

module.exports = {
  composeMissionAnswer,
  missionFacts,
};
