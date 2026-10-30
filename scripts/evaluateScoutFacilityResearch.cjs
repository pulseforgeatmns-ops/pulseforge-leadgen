'use strict';
// Offline evaluator by default. The explicit export subcommand only reads the
// existing Tenant 10 ledger in a REPEATABLE READ READ ONLY transaction.
const fs = require('node:fs');
const { createHash } = require('node:crypto');
const facility = require('../utils/anchorFacilityResearch');
const legacy = require('../utils/replenishmentVertical');
const coverage = require('../services/scoutFacilityResearchCoverage');
const { buildQueriesForEvidence } = require('../packages/capabilities/discovery/providers/PlacesProvider');
const hash = value => createHash('sha256').update(value).digest('hex');

function coverageComparison(scope) {
  const history = [], proposed = [];
  for (let n = 0; n < 8; n++) {
    const row = coverage.rankQueries(coverage.approvedQueryPlan(scope, 0, buildQueriesForEvidence), history).ordered[0];
    proposed.push({ cycle: n + 1, query: row.query, category: row.category, city: row.city });
    history.push({ query_text: row.query, attempts: 1 });
  }
  const priorRoots = buildQueriesForEvidence({ segment: scope.eligibleSubsegments[0], evidenceType: 'identity', cities: scope.cities, state: 'NH' });
  const previous = priorRoots.flatMap(query => [0, 1, 2].map(pageIndex => ({ query, pageIndex,
    category: coverage.category(query), city: scope.cities.find(city => query.includes(city)) || null }))).slice(0, 8);
  return { assumptions: 'Code-derived planning comparison with empty history and three available pages per old root; not a reconstruction of the actual eight paid query texts.',
    previous, proposed, previousCategories: new Set(previous.map(x => x.category)).size,
    proposedCategories: new Set(proposed.map(x => x.category)).size,
    previousTowns: new Set(previous.map(x => x.city)).size, proposedTowns: new Set(proposed.map(x => x.city)).size };
}

function evaluate(input, inputBytes = JSON.stringify(input)) {
  if (input.tenantId !== '10' || !Array.isArray(input.records) || !input.scope) throw Error('evaluation_explicit_tenant_10_input_required');
  const context = { tenantId: '10', missionSegments: input.scope.eligibleSubsegments, missionCities: input.scope.cities };
  const rows = input.records.map((record, index) => {
    const raw = structuredClone(record.candidate);
    const proposed = facility.evaluateFacilityResearch(raw, context);
    return { observationIndex: index + 1, name: raw.name || raw.companyName || raw.company,
      previous: record.previous, recomputedLegacyVertical: legacy.resolveReplenishmentVertical(raw, context),
      proposed, source: record.source || null, missingEvidence: record.missingEvidence || [] };
  });
  const priorDeferred = row => row.previous?.status === 'awaiting_website_evidence';
  const isRetained = row => row.previous?.status === 'retained';
  return { version: 'SPEC-SCOUT-P0-004-evaluation-v1', inputSha256: hash(inputBytes), tenantId: '10',
    baselineRevision: input.baselineRevision || null, grantId: input.grantId || null, sourceKind: input.sourceKind,
    cohort: { expected: input.cohortExpected, evaluated: rows.length, missing: Math.max(0, input.cohortExpected - rows.length),
      completeRawCohort: input.authoritativeRawExport === true && rows.length === input.cohortExpected },
    limitations: input.limitations || [], productionBaseline: input.baseline || null,
    summary: {
      newlyRecognizedAwaiting: rows.filter(row => priorDeferred(row) && row.proposed.facilityDecision.recognition.status === 'recognized').map(row => row.name),
      awaitingWithSuitableCategoryAndGeography: rows.filter(row => priorDeferred(row)
        && row.proposed.facilityDecision.suitability.status === 'suitable' && row.proposed.facilityDecision.geography.status === 'eligible').map(row => row.name),
      remainingDeferred: rows.filter(row => row.proposed.facilityDecision.admission.status === 'deferred').map(row => row.name),
      demonstrablyUnsuitableRetained: rows.filter(row => isRetained(row) && row.proposed.facilityDecision.suitability.status === 'unsuitable').map(row => row.name),
      geographicCorrections: rows.filter(row => row.proposed.facilityDecision.geography.status === 'ineligible').map(row => row.name),
      proposedResearchEligible: rows.filter(row => row.proposed.admitted).map(row => row.name),
      downstreamPromotionsApplied: 0, googleCalls: 0, databaseWrites: 0,
    }, potentialProcurementGroups: facility.potentialProcurementGroups(input.records.map(x => x.candidate)),
    queryCoverage: coverageComparison(input.scope), rows };
}

function markdown(report) {
  const escape = value => String(value ?? 'unknown').replace(/\|/g, '\\|').replace(/\n/g, ' ');
  const rawExport = report.sourceKind === 'production_read_only_ledger_export';
  const lines = ['# SPEC-SCOUT-P0-004 offline retrospective', '',
    `Input SHA-256: \`${report.inputSha256}\`. Base revision: \`${report.baselineRevision}\`.`, '',
    `**Cohort: ${report.cohort.evaluated}/${report.cohort.expected} records evaluated; ${report.cohort.missing} unavailable. Complete raw-cohort acceptance: ${report.cohort.completeRawCohort ? 'passed' : 'PENDING'}.**`, '',
    rawExport ? 'The input is an exact-grant, read-only ledger snapshot. Original ticket outcomes and contradictory observations are preserved. Missing evidence remains explicit.'
      : 'The supplied screenshots are production observations, not a complete raw ledger export. No missing record, Google type, website, facility size, owner, purchasing authority or cleaning need was invented.', '',
    ...report.limitations.map(x => `- ${x}`), '',
    '## Before / after', '',
    `- Newly recognized among the awaiting records: ${report.summary.newlyRecognizedAwaiting.length}. Recognition alone is not admission.`,
    `- Awaiting records with suitable category and eligible geography: ${report.summary.awaitingWithSuitableCategoryAndGeography.length}. ${rawExport
      ? 'Existing ownership, suppression and prior holds remain separate from proposed facility fit.' : 'Website values and ownership/suppression readback are still missing from the screenshots.'}`,
    `- Demonstrably unsuitable retained records: ${report.summary.demonstrablyUnsuitableRetained.join('; ') || 'none observed'}.`,
    `- Geographic corrections: ${report.summary.geographicCorrections.join('; ') || 'none observed'}.`,
    `- Still deferred in this evidence-limited evaluation: ${report.summary.remainingDeferred.length}.`,
    '- No automatic promotion, database write, paid call, website investigation, preparation or sending occurred.', '',
    'False positives above are supported by observed business labels or the address. Structured school categories supply evidence for the previous classifier false negatives. Missing evidence is not a measured model false negative. No whole-cohort precision/recall is claimed without verified suitability labels.', '',
    '| Observed business | Previous | Proposed category | Suitability | Geography | Research admission | Missing/held evidence |',
    '| --- | --- | --- | --- | --- | --- | --- |'];
  for (const row of report.rows) {
    const d = row.proposed.facilityDecision;
    lines.push(`| ${[row.name, `${row.previous?.status || 'unknown'} / ${row.previous?.vertical || 'unclassified'}`,
      d.recognition.category || 'ambiguous', `${d.suitability.status}: ${d.suitability.reason || ''}`, d.geography.status,
      d.admission.status, [...row.missingEvidence, ...d.suitability.missingEvidence, ...d.procurement.missingEvidence].join(', ')].map(escape).join(' | ')} |`);
  }
  lines.push('', '## Potential procurement relationships', '',
    'These are review hints, never verified opportunity groups. Physical locations remain distinct. Public-school facilities are not represented as independent purchasing authorities.', '');
  for (const group of report.potentialProcurementGroups) lines.push(`- ${group.reason}: ${group.facilities.map(x => x.name).join('; ')}. Verified: ${group.verified}. Merge authorized: false.`);
  lines.push('', '## First eight planned queries', '', report.queryCoverage.assumptions, '',
    `Category coverage: ${report.queryCoverage.previousCategories} → ${report.queryCoverage.proposedCategories}. Town coverage: ${report.queryCoverage.previousTowns} → ${report.queryCoverage.proposedTowns}. Existing page/meter/grant guards remain mandatory.`, '',
    '| Cycle | Previous prefix (page) | Proposed query | Category | Town |', '| --- | --- | --- | --- | --- |');
  report.queryCoverage.proposed.forEach((row, index) => lines.push(`| ${index + 1} | ${escape(report.queryCoverage.previous[index].query)} (${report.queryCoverage.previous[index].pageIndex}) | ${escape(row.query)} | ${row.category} | ${row.city} |`));
  lines.push('', '## Activation gate and evidence limits', '',
    'Obtain the authorized read-only 79-record export, rerun this same evaluator, inspect every changed admission and verify procurement evidence before any production activation. The explicit export subcommand uses the existing DB credential in a read-only transaction; it cannot install grants, fetch websites, search Google or update records.', '',
    'Historical rows and parked dependency keys are unchanged. New classification does not proactively requeue the 34 deferred tickets. Existing unique research-domain/name guards may retain one row per domain; all physical memberships remain available and no independent sales opportunities are created.', '');
  return lines.join('\n');
}

const { exportCohort } = require('../services/scoutResearchRetrospective');

async function main(args = process.argv.slice(2)) {
  if (args.length === 3 && args[0] === 'export') {
    if (!process.env.DATABASE_URL) throw Error('evaluation_database_credential_required');
    const { Pool } = require('pg');
    const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 1, connectionTimeoutMillis: 5000,
      ssl: process.env.DATABASE_SSL === 'false' ? false : undefined });
    try { fs.writeFileSync(args[2], JSON.stringify(await exportCohort(pool, args[1]), null, 2) + '\n'); }
    finally { await pool.end(); }
    return;
  }
  if (args.length !== 3 || args[0] !== 'evaluate') throw Error('usage: evaluate INPUT_JSON OUTPUT_PREFIX | export EXACT_GRANT_ID OUTPUT_JSON');
  const bytes = fs.readFileSync(args[1], 'utf8'), report = evaluate(JSON.parse(bytes), bytes);
  fs.writeFileSync(args[2] + '.json', JSON.stringify(report, null, 2) + '\n');
  fs.writeFileSync(args[2] + '.md', markdown(report));
}
if (require.main === module) main().catch(error => { process.stderr.write(String(error.message) + '\n'); process.exitCode = 1; });
module.exports = { evaluate, markdown, coverageComparison, exportCohort, main };
