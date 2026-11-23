'use strict';

const { TENANT_ID, RESEARCH_SOURCE, evaluateProspect } = require('./leviScoutProductionContract');
const { inspectApprovedBlueprint } = require('./leviScoutBlueprintReadiness');
const { summarizeLayers, LAYERS } = require('./leviScoutSignalModel');

const RESEARCH_CANDIDATE = 'research_candidate';
const QUALIFIED_PROSPECT = 'qualified_prospect';

function text(value) {
  return value == null ? '' : String(value).trim();
}

function metadataOf(row) {
  const value = row && row.acquisition_metadata;
  if (!value) return {};
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch (_) { return {}; }
}

function missingFromBlockers(blockers = []) {
  const labels = {
    borrower_identity_unverified: 'Borrower identity is not verified as a real-estate operator or borrower.',
    borrower_intent_unverified: 'Verified borrower intent is missing. A permit is not intent.',
    contact_path_missing: 'No verifiable public business phone, email, or registered license address.',
    current_project_evidence_missing: 'No explicit current-project claim is recorded.',
    financing_criteria_unverified: 'Approved Blueprint financing range is missing, so amount fit cannot be assessed.',
    financing_amount_missing: 'No explicit financing amount is recorded.',
    financing_amount_outside_blueprint: 'Recorded amount is outside the approved Blueprint range.',
    project_location_missing: 'Project location is missing.',
    source_reference_missing: 'Source URL is missing.',
    evidence_timestamp_missing: 'Evidence timestamp is missing.',
    duplicate: 'Duplicate of an existing tenant-21 record.',
    suppressed: 'Record is suppressed.',
    contradictory_deal_evidence: 'Recorded deal claims contradict each other.',
  };
  return blockers.map(code => labels[code] || code);
}

function nextResearchAction({ blockers = [], contact } = {}) {
  if (blockers.includes('financing_criteria_unverified')) {
    return 'Obtain Levi’s approval of the missing Blueprint lending criteria before any qualification decision.';
  }
  if (blockers.includes('borrower_intent_unverified')) {
    return 'Search lawful public records for an explicit financing request. Do not treat the permit as borrower intent.';
  }
  if (blockers.includes('contact_path_missing') && !(contact && contact.verifiable)) {
    return 'Look up the operating entity on public business-license or owner filings for a verifiable business address or phone.';
  }
  if (blockers.includes('financing_amount_missing')) {
    return 'Find a public, dated financing-amount statement before comparing the record to approved criteria.';
  }
  if (blockers.includes('duplicate')) {
    return 'Keep the existing tenant-21 record. Do not insert a duplicate.';
  }
  return 'Hold as a research candidate. Do not enroll in a campaign or start outreach.';
}

function evidenceSummary({ candidate, evaluation, contact, signals }) {
  const location = candidate.service_area_match || (candidate.acquisition_metadata && candidate.acquisition_metadata.city) || 'unspecified location';
  const layers = summarizeLayers(signals);
  const parts = [
    `${candidate.company || candidate.name || 'Unnamed entity'} at ${location}.`,
    layers[LAYERS.PROPERTY_ACTIVITY] ? 'Public permit supports property activity only.' : 'No property-activity signal.',
    layers[LAYERS.POTENTIAL_FINANCING_NEED] ? 'A possible financing need is inferred from permit cost or rehab language, not from a request.' : 'No inferred financing-need signal.',
    layers[LAYERS.VERIFIED_BORROWER_INTENT]
      ? 'Verified borrower intent is present from a public financing request.'
      : 'Borrower intent is unverified. A permit does not establish intent.',
    contact && contact.verifiable
      ? 'A verifiable public business contact path was found.'
      : 'No verifiable public business contact path was found.',
    evaluation && evaluation.qualified
      ? 'Meets approved qualification criteria.'
      : 'Does not meet approved qualification criteria.',
  ];
  return parts.join(' ');
}

function composeRecordReview({ candidate = {}, evaluation, criteria } = {}) {
  const metadata = metadataOf(candidate);
  const assessed = evaluation || evaluateProspect(candidate, { criteria });
  const contact = metadata.contactResearch || null;
  const signals = Array.isArray(metadata.signals) ? metadata.signals : [];
  const bucket = assessed.qualified ? QUALIFIED_PROSPECT : RESEARCH_CANDIDATE;
  const missingInformation = missingFromBlockers(assessed.blockers);
  return Object.freeze({
    company: candidate.company || candidate.name || null,
    city: metadata.city || null,
    bucket,
    qualificationStatus: assessed.qualified ? 'qualified' : 'not_qualified',
    classification: assessed.classification,
    blockers: Object.freeze([...(assessed.blockers || [])]),
    missingInformation: Object.freeze(missingInformation),
    nextResearchAction: nextResearchAction({ blockers: assessed.blockers, contact }),
    evidenceSummary: evidenceSummary({ candidate, evaluation: assessed, contact, signals }),
    verifiedBorrowerIntent: false,
    permitDoesNotEstablishIntent: metadata.sourceType === 'property_record_signal' || metadata.permitDoesNotEstablishIntent === true,
    verifiableBusinessContact: Boolean(contact && contact.verifiable),
    operatingBusiness: contact && contact.operatingBusiness ? contact.operatingBusiness.value : (candidate.company || null),
    decisionMakerRole: contact && contact.decisionMakerRole ? contact.decisionMakerRole.value : null,
    outreachAuthorized: false,
    campaignEnrollmentAuthorized: false,
  });
}

function composeReviewBundle({
  tenantId = TENANT_ID,
  records = [],
  understanding,
  briefing = null,
  criteria,
} = {}) {
  const readiness = inspectApprovedBlueprint(understanding, tenantId);
  const reviews = records.map((row) => {
    const candidate = row.candidate || row;
    const evaluation = row.evaluation || evaluateProspect(candidate, { criteria });
    return composeRecordReview({ candidate, evaluation, criteria });
  });
  const qualifiedProspects = reviews.filter(item => item.bucket === QUALIFIED_PROSPECT);
  const researchCandidates = reviews.filter(item => item.bucket === RESEARCH_CANDIDATE);
  const verifiableBusinessContacts = reviews.filter(item => item.verifiableBusinessContact).length;
  return Object.freeze({
    tenantId: String(tenantId),
    source: RESEARCH_SOURCE,
    outreachAttempted: false,
    paidProvidersUsed: false,
    inventedCriteria: false,
    blueprint: {
      trusted: readiness.trusted,
      knowledgeState: readiness.knowledgeState,
      missingDecisions: readiness.missing,
      questionsForLevi: readiness.questionsForLevi,
    },
    counts: Object.freeze({
      enriched: reviews.length,
      researchCandidates: researchCandidates.length,
      qualifiedProspects: qualifiedProspects.length,
      verifiableBusinessContacts,
    }),
    researchCandidates: Object.freeze(researchCandidates),
    qualifiedProspects: Object.freeze(qualifiedProspects),
    records: Object.freeze(reviews),
    lastBriefing: briefing,
  });
}

function formatReviewAnswer(bundle) {
  const counts = bundle.counts || {};
  const questions = (bundle.blueprint && bundle.blueprint.questionsForLevi) || [];
  const lines = [
    `Levi Scout research review for tenant ${bundle.tenantId}.`,
    `Research candidates: ${counts.researchCandidates || 0}. Qualified prospects: ${counts.qualifiedProspects || 0}. Verifiable public business contacts: ${counts.verifiableBusinessContacts || 0}.`,
    'Qualified prospects are separated from research candidates. A permit never establishes borrower intent.',
    'Outreach, campaign enrollment, and paid enrichment providers stayed off.',
  ];
  if (questions.length) {
    lines.push('Missing Blueprint decisions requiring Levi’s approval:');
    questions.forEach((question, index) => lines.push(`${index + 1}. ${question}`));
  } else {
    lines.push('Approved Blueprint lending criteria required for qualification are present.');
  }
  const preview = (bundle.researchCandidates || []).slice(0, 5);
  if (preview.length) {
    lines.push('Research candidate summaries:');
    for (const item of preview) {
      lines.push(`- ${item.company}: ${item.evidenceSummary} Missing: ${item.missingInformation.join(' ')} Next: ${item.nextResearchAction}`);
    }
  }
  if ((bundle.qualifiedProspects || []).length) {
    lines.push('Qualified prospects:');
    for (const item of bundle.qualifiedProspects) {
      lines.push(`- ${item.company}: ${item.evidenceSummary}`);
    }
  } else {
    lines.push('No records meet approved qualification criteria.');
  }
  return lines.join('\n');
}

async function loadPersistedResearchRecords({ db, tenantId = TENANT_ID } = {}) {
  if (!db || typeof db.query !== 'function') return [];
  const result = await db.query(
    `SELECT p.id, p.first_name, p.last_name, p.email, p.phone, p.job_title, p.notes,
            p.service_area_match, p.source, p.discovery_method, p.acquisition_metadata,
            p.created_at, p.setter_visible, p.do_not_contact, p.ao_outreach_review_required,
            c.name AS company_name
       FROM prospects p
       LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.client_id = $1 AND p.source = $2
      ORDER BY p.created_at DESC
      LIMIT 50`,
    [Number(tenantId), RESEARCH_SOURCE]
  );
  return (result.rows || []).map(row => ({
    id: row.id,
    name: [row.first_name, row.last_name].filter(Boolean).join(' '),
    company: row.company_name,
    email: row.email,
    phone: row.phone,
    job_title: row.job_title,
    notes: row.notes,
    service_area_match: row.service_area_match,
    source: row.source,
    created_at: row.created_at,
    setter_visible: row.setter_visible,
    do_not_contact: row.do_not_contact,
    ao_outreach_review_required: row.ao_outreach_review_required,
    acquisition_metadata: row.acquisition_metadata,
  }));
}

async function loadLastBriefing({ db, tenantId = TENANT_ID } = {}) {
  if (!db || typeof db.query !== 'function') return null;
  const result = await db.query(
    `SELECT payload, description
       FROM agent_actions
      WHERE client_id = $1 AND action_type = 'levi_scout_research_briefing'
      ORDER BY id DESC
      LIMIT 1`,
    [Number(tenantId)]
  );
  return result.rows[0] || null;
}

module.exports = {
  RESEARCH_CANDIDATE,
  QUALIFIED_PROSPECT,
  composeRecordReview,
  composeReviewBundle,
  formatReviewAnswer,
  loadPersistedResearchRecords,
  loadLastBriefing,
  nextResearchAction,
};
