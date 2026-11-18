'use strict';

/**
 * Diagnoses over an EvidenceRegistry report (P0-MAX-REASONING-001 PR 4).
 *
 * The ONLY place inferences are made. Every inference names the observed
 * items it rests on (`basis`), is labelled `certainty: 'inferred'`, and never
 * becomes an observed fact. Unknowns from the report are carried through, so
 * a diagnosis can say "this could explain it" but never "this is why" when
 * the evidence needed to say so is missing.
 */

const items = (report, domain, type) => ((report.domains[domain] || {}).items || []).filter((i) => !type || i.subject.type === type);
const unknownsFor = (report, ...domains) => report.unknowns.filter((u) => domains.includes(u.domain));
const infer = (claim, basis, extra = {}) => ({ kind: 'inference', certainty: 'inferred', claim, basis: basis.map((b) => (typeof b === 'string' ? b : b.ref)), ...extra });

function envelope(question, report, observedItems, inferences, domains) {
  return {
    question,
    tenantId: report.tenantId,
    refused: report.refused,
    observed: observedItems.map((i) => i.ref),
    inferences,
    contradictions: report.contradictions.filter((c) => c.refs.some((r) => observedItems.some((i) => i.ref === r))),
    unknowns: unknownsFor(report, ...domains),
    // A diagnosis is never a verified cause.
    verdict: inferences.length ? 'candidate_explanations' : 'insufficient_evidence',
    authority: { grantsAuthority: false },
  };
}

/** "Why hasn't Scout generated qualified prospects?" */
function diagnoseScout(report, now = new Date()) {
  const runs = items(report, 'scout_execution', 'agent_run');
  const agg = items(report, 'scout_execution', 'prospect_aggregate')[0];
  const out = [];
  if (!runs.length) {
    // Absence of a log is reported as an unknown by the registry, never as "did not run".
    out.push(infer('No Scout run is recorded; if Scout has not run, no new prospects would be expected. Whether it ran outside this log is unknown.', agg ? [agg] : []));
  }
  for (const r of runs) {
    if (r.freshness.stale) out.push(infer(`Scout's last recorded run was ${r.freshness.ageDays} days ago; recent discovery is unlikely without new runs.`, [r]));
    if (r.facts.errors7d > 0) out.push(infer(`${r.facts.errors7d} Scout run(s) failed in the last 7 days; failures may be preventing new prospects.`, [r]));
  }
  if (agg) {
    if (agg.facts.total > 0 && agg.facts.qualified === 0) out.push(infer('Prospects exist but none meet the qualification threshold; the targeting or scoring may exclude what Scout finds.', [agg]));
    if (agg.freshness.stale) out.push(infer(`No prospect has been added for ${agg.freshness.ageDays} days.`, [agg]));
  }
  return envelope('scout_no_qualified_prospects', report, [...runs, ...(agg ? [agg] : [])], out, ['scout_execution']);
}

/** "Why hasn't Emmett sent outreach?" */
function diagnoseEmmett(report, now = new Date()) {
  const gate = items(report, 'outbound_authorization', 'outbound_gate')[0];
  const programs = items(report, 'outbound_authorization', 'outbound_program');
  const runs = items(report, 'outbound_delivery', 'agent_run');
  const channels = items(report, 'outbound_delivery', 'outbound_channel');
  const crm = items(report, 'crm_pipeline', 'prospect_record');
  const out = [];
  if (gate && gate.facts.sendingBlocked) out.push(infer(`The outbound gate is in "${gate.facts.mode}" (${gate.facts.reason || 'no reason recorded'}); sending is blocked while it is.`, [gate]));
  for (const p of programs) {
    if (p.facts.authorizationExpired) out.push(infer(`The outbound program's authorization expired at ${p.facts.expiresAt}; sending would require renewed authorization.`, [p]));
    if (p.facts.mode !== 'active') out.push(infer(`The outbound program is in "${p.facts.mode}" mode, not active.`, [p]));
    if (p.facts.lastErrorRecorded) out.push(infer('The outbound program recorded an error on its last tick.', [p]));
  }
  for (const r of runs) if (r.facts.errors7d > 0) out.push(infer(`${r.facts.errors7d} Emmett run(s) failed in the last 7 days.`, [r]));
  const email = channels.find((c) => /email/i.test(c.subject.id));
  if (email && email.facts.last7d === 0) out.push(infer('No email touchpoints are recorded in the last 7 days.', [email]));
  if (crm.length && !crm.some((c) => c.facts.eligible)) out.push(infer('No current prospect record meets the recorded outreach gates, so there may be nothing eligible to send.', crm.slice(0, 10)));
  return envelope('emmett_no_outreach', report, [...(gate ? [gate] : []), ...programs, ...runs, ...channels], out, ['outbound_authorization', 'outbound_delivery', 'crm_pipeline']);
}

/**
 * Highest-priority ACTIVE opportunities from current records. Priority is the
 * recorded engagement tier (hot > warm > cold); ties share a position and the
 * id only orders display. Closed/expired records are excluded with a reason.
 */
function rankOpportunities(report, { top = 5 } = {}) {
  const crm = items(report, 'crm_pipeline', 'prospect_record');
  const excluded = [];
  const open = [];
  for (const c of crm) {
    if (c.facts.opportunityState !== 'open') excluded.push({ ref: c.ref, reason: `opportunity ${c.facts.opportunityState}` });
    else if (!c.facts.eligible) excluded.push({ ref: c.ref, reason: 'does not meet recorded outreach gates', missing: c.missing });
    else open.push(c);
  }
  open.sort((a, b) => b.facts.engagementTier - a.facts.engagementTier || a.subject.id.localeCompare(b.subject.id));
  const cutoff = open.length ? open[Math.min(top, open.length) - 1].facts.engagementTier : null;
  const ranked = open.filter((c) => c.facts.engagementTier >= cutoff).map((c) => ({
    ref: c.ref, position: 1 + open.filter((o) => o.facts.engagementTier > c.facts.engagementTier).length,
    basis: 'recorded engagement tier (not estimated value)', tier: c.facts.engagementTier, missing: c.missing,
    stale: c.freshness.stale, contactStale: c.facts.contactStale,
  }));
  return {
    tenantId: report.tenantId, ranked, excluded, complete: (report.domains.crm_pipeline || {}).complete === true,
    unknowns: unknownsFor(report, 'crm_pipeline'),
    contradictions: report.contradictions.filter((x) => x.refs.some((r) => ranked.some((k) => k.ref === r))),
  };
}

/** Is this opportunity stale, closed or expired? Observed state plus freshness, never a guess. */
function opportunityStaleness(item) {
  if (!item || item.subject.type !== 'prospect_record') return null;
  const reasons = [];
  if (item.facts.opportunityState === 'closed' || item.facts.opportunityState === 'expired') reasons.push(`opportunity ${item.facts.opportunityState}`);
  if (item.freshness.stale) reasons.push(`record not updated for ${item.freshness.ageDays} days`);
  if (item.facts.contactStale) reasons.push('last contact older than 60 days');
  if (item.facts.lastContactedAt == null) reasons.push('no contact date recorded');
  return { ref: item.ref, stale: reasons.length > 0, reasons, observedAt: item.observedAt, basis: 'recorded fields only' };
}

module.exports = { diagnoseScout, diagnoseEmmett, rankOpportunities, opportunityStaleness };
