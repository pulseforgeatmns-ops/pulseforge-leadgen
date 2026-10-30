'use strict';

// Existing evidence only. No application bootstrap, provider, grant claim or
// candidate admission module is loaded here. The CLI and guarded admin download
// share this same repeatable-read, read-only snapshot.
const LIMITS = Object.freeze({ tickets: 500, observations: 5000, bytes: 10 * 1024 * 1024 });
const fail = code => { throw Object.assign(new Error(code), { code }); };
const domainOf = value => {
  if (typeof value !== 'string' || !value.trim()) return '';
  try { return new URL(/^[a-z]+:\/\//i.test(value || '') ? value : `https://${value}`).hostname.toLowerCase().replace(/^www\./, ''); }
  catch { return ''; }
};
const nameOf = raw => String(raw.name || raw.companyName || raw.company || '').trim();

async function exportCohort(pool, grantId) {
  if (!/^[a-f0-9]{64}$/.test(grantId || '')) fail('research_only_retrospective_grant_required');
  const db = await pool.connect();
  try {
    await db.query('BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY');
    await db.query("SET LOCAL statement_timeout='15s'");
    const grant = (await db.query(`SELECT grant_id,search_fingerprint,research_mission_id,
      manifest->>'expectedRevision' AS baseline_revision,transaction_timestamp() AS observed_at
      FROM scout_research_only_grants WHERE tenant_id='10' AND grant_id=$1`, [grantId])).rows[0];
    if (!grant) fail('research_only_grant_not_found');
    const mission = (await db.query("SELECT payload FROM scout_research_missions WHERE tenant_id='10' AND id=$1", [grant.research_mission_id])).rows[0];
    const structured = mission?.payload?.structuredMission;
    if (!Array.isArray(structured?.market?.eligibleSubsegments) || !structured.market.eligibleSubsegments.length
      || !Array.isArray(structured?.geography?.cities) || !structured.geography.cities.length)
      fail('research_only_retrospective_mission_unavailable');
    const scope = [grant.search_fingerprint];
    const counts = (await db.query(`SELECT
      (SELECT count(*)::int FROM scout_candidate_work WHERE tenant_id='10' AND search_fingerprint=$1) AS tickets,
      (SELECT count(*)::int FROM scout_candidate_memberships m JOIN scout_candidate_work w
        ON m.tenant_id=w.tenant_id AND m.search_fingerprint=w.search_fingerprint AND m.plan_version=w.plan_version
          AND m.ticket_version=w.ticket_version AND m.candidate_key=w.candidate_key
        WHERE w.tenant_id='10' AND w.search_fingerprint=$1) AS observations,
      (SELECT COALESCE(sum(octet_length(m.raw_candidate::text)),0)::bigint FROM scout_candidate_memberships m JOIN scout_candidate_work w
        ON m.tenant_id=w.tenant_id AND m.search_fingerprint=w.search_fingerprint AND m.plan_version=w.plan_version
          AND m.ticket_version=w.ticket_version AND m.candidate_key=w.candidate_key
        WHERE w.tenant_id='10' AND w.search_fingerprint=$1) AS raw_bytes`, scope)).rows[0];
    if (!Number.isSafeInteger(counts?.tickets) || !Number.isSafeInteger(counts?.observations)
      || !Number.isSafeInteger(Number(counts?.raw_bytes)) || Number(counts.raw_bytes) > LIMITS.bytes
      || counts.tickets > LIMITS.tickets || counts.observations > LIMITS.observations)
      fail('research_only_retrospective_limit_exceeded');
    const work = (await db.query(`SELECT w.candidate_key,w.plan_version,w.ticket_version,w.state,w.reason,
      (SELECT jsonb_agg(jsonb_build_object('raw',m.raw_candidate,'queryKey',m.query_key,'ordinal',m.ordinal,'createdAt',m.created_at)
        ORDER BY m.created_at,m.query_key,m.ordinal) FROM scout_candidate_memberships m
        WHERE m.tenant_id=w.tenant_id AND m.search_fingerprint=w.search_fingerprint AND m.plan_version=w.plan_version
          AND m.ticket_version=w.ticket_version AND m.candidate_key=w.candidate_key) AS observations
      FROM scout_candidate_work w WHERE w.tenant_id='10' AND w.search_fingerprint=$1
      ORDER BY w.candidate_key,w.plan_version,w.ticket_version`, scope)).rows;
    const retained = (await db.query(`SELECT company,domain,vertical,location,cycle_number
      FROM scout_research_candidates WHERE tenant_id='10' AND grant_id=$1 ORDER BY id`, [grantId])).rows;
    if (work.length !== counts.tickets || work.reduce((sum, row) => sum + (row.observations || []).length, 0) !== counts.observations)
      fail('research_only_retrospective_snapshot_incomplete');
    const matchesByTicket = work.map(row => row.reason === 'queued' ? retained.filter(stored => (row.observations || []).some(({ raw: observed }) =>
      domainOf(observed?.website || observed?.url || observed?.domain) === domainOf(stored.domain)
      && nameOf(observed || {}).toLowerCase() === stored.company.trim().toLowerCase())) : []);
    const records = work.map((row, index) => {
      const memberships = row.observations || [], observations = memberships.map(x => x.raw || {}), raw = observations[0] || {};
      const matches = matchesByTicket[index];
      const unambiguous = matches.length === 1 && matchesByTicket.filter(rows => rows.includes(matches[0])).length === 1;
      return { candidate: { ...raw, name: nameOf(raw) || undefined, location: raw.address || raw.location,
        classificationObservations: observations.map(x => ({ ...x, name: nameOf(x) || undefined })) },
        previous: { candidateKey: row.candidate_key, planVersion: row.plan_version, ticketVersion: row.ticket_version,
          state: row.state, reason: row.reason, vertical: unambiguous ? matches[0].vertical : null,
          status: unambiguous ? 'retained' : ['business_evidence_not_authorized', 'evidence_budget_deferred'].includes(row.reason)
            ? 'awaiting_website_evidence' : matches.length ? 'accepted_retention_unresolved' : row.reason === 'queued' ? 'accepted_not_retained' : row.state,
          retainedMatchBasis: unambiguous ? 'unique_observed_name_and_domain' : null },
        source: { kind: 'production_read_only_ledger_export', candidateKey: row.candidate_key,
          memberships: memberships.map(({ raw: _raw, ...provenance }) => provenance) },
        missingEvidence: memberships.length ? [] : ['raw_membership_evidence_missing'] };
    });
    const data = { version: 2, tenantId: '10', cohortExpected: 79,
      authoritativeRawExport: records.every(row => row.candidate.classificationObservations.length > 0),
      sourceKind: 'production_read_only_ledger_export', baselineRevision: grant.baseline_revision || null,
      observedAt: grant.observed_at, grantId, researchMissionId: grant.research_mission_id,
      scope: { eligibleSubsegments: structured.market.eligibleSubsegments, cities: structured.geography.cities },
      exportScope: { kind: 'search_fingerprint', fingerprint: grant.search_fingerprint, ...counts,
        note: 'Other passes on the identical search fingerprint share these tickets. Memberships are existing immutable evidence.' },
      baseline: { uniqueBusinesses: records.length, retainedCandidates: retained.length,
        acceptedTickets: work.filter(row => row.state === 'handled' && row.reason === 'queued').length,
        awaitingWebsiteEvidence: work.filter(row => ['business_evidence_not_authorized', 'evidence_budget_deferred'].includes(row.reason)).length },
      limitations: ['Retained-row matching uses observed name and domain; there is no durable ticket foreign key in the retained table.',
        'Procurement authority, facility suitability and suppression evidence are not inferred from a prior ticket outcome.'],
      retained: retained.map(row => ({ company: row.company, domain: row.domain, vertical: row.vertical, location: row.location, cycleNumber: row.cycle_number })),
      records };
    if (Buffer.byteLength(JSON.stringify(data), 'utf8') > LIMITS.bytes) fail('research_only_retrospective_limit_exceeded');
    await db.query('COMMIT');
    return data;
  } catch (error) { await db.query('ROLLBACK').catch(() => {}); throw error; }
  finally { db.release(); }
}

function exportForAdmin(pool, input) {
  if (!input || Object.keys(input).length !== 1 || typeof input.grantId !== 'string')
    fail('research_only_retrospective_grant_required');
  return exportCohort(pool, input.grantId);
}

module.exports = { exportCohort, exportForAdmin, LIMITS };
