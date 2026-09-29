'use strict';

/**
 * Canonical CRM email-provenance writes.
 * Never sends mail. Never revises CAPACITY.
 * Does not delete legitimate historical emails; invalidates tainted ones.
 */

const { appendQuarantineNote } = require('./emailGuard');
const {
  TAINTED_EMAIL_ACTIONS,
  planTaintedCrmEmailRemediation,
  stampEmailProvenance,
  isReadPathProvenanceLabel,
  resolveEmailProvenanceSource,
  canonicalOutboundEmailIneligibilityReason,
  isCanonicallyOutboundEligible,
} = require('./canonicalEmailEligibility');

const AUDIT_CONTACTS = Object.freeze([
  {
    label: 'Solomon Law Firm',
    prospectId: 'cde8f588-6969-47c3-8219-3f539b2b23cc',
  },
  {
    label: 'Law Offices of Michael R. St. Louis',
    prospectId: '5418dbf1-0ed1-4dab-854b-df9a4dfcf8d3',
  },
  {
    label: 'Backus, Meyer & Branch',
    prospectId: '7adbb294-b94c-45c0-85df-e040f027ece0',
  },
]);

function snapshotRow(row, extras = {}) {
  if (!row) return null;
  const company = extras.company || row.company_name || row.company || null;
  const excluded = extras.excluded === true;
  return {
    prospectId: row.prospect_id || row.id || extras.prospectId || null,
    company,
    email: row.email || null,
    emailStatus: row.email_status || row.emailStatus || null,
    emailVerified: row.email_verified === true || row.emailVerified === true,
    emailVerificationMethod: row.email_verification_method || row.emailVerificationMethod || null,
    dnc: row.do_not_contact === true || row.dnc === true,
    excluded,
    storedProvenance: resolveEmailProvenanceSource(row),
    enrichmentProvenanceEmail: row.enrichment_provenance?.email || null,
    ineligibility: canonicalOutboundEmailIneligibilityReason(row),
    projectable: extras.projectable != null
      ? extras.projectable
      : isCanonicallyOutboundEligible(row),
  };
}

function nowIso() {
  return new Date().toISOString();
}

function persistableEmailSource(source) {
  const normalized = String(source || '').trim().toLowerCase();
  if (!normalized || isReadPathProvenanceLabel(normalized)) return null;
  return normalized;
}

function provenancePatchForPreserve(row, plan) {
  return stampEmailProvenance(row.enrichment_provenance, plan.provenance, {
    outbound_eligible: false,
    preserved_at: nowIso(),
    preservation_reason: plan.reason,
  });
}

function provenancePatchForInvalidate(row, plan) {
  return stampEmailProvenance(row.enrichment_provenance, plan.provenance, {
    outbound_eligible: false,
    invalidated: true,
    invalidation_reason: plan.reason,
    quarantined_email: plan.email,
    invalidated_at: nowIso(),
  });
}

async function applyTaintedCrmEmailRemediation(db, row, plan, options = {}) {
  if (!row || !(row.prospect_id || row.id)) {
    return { applied: false, reason: 'missing_row' };
  }
  const prospectId = row.prospect_id || row.id;
  const clientId = row.client_id;
  if (!plan || plan.action === TAINTED_EMAIL_ACTIONS.NONE) {
    return { applied: false, reason: plan?.reason || 'none' };
  }
  if (options.dryRun || typeof db?.query !== 'function') {
    return { applied: false, dryRun: options.dryRun === true, plan };
  }

  if (plan.action === TAINTED_EMAIL_ACTIONS.PRESERVE_UNTRUSTED_PROVENANCE) {
    const provenance = provenancePatchForPreserve(row, plan);
    await db.query(
      `UPDATE prospects
          SET enrichment_provenance = COALESCE(enrichment_provenance, '{}'::jsonb) || $1::jsonb,
              updated_at = NOW()
        WHERE id = $2
          AND client_id = $3`,
      [JSON.stringify(provenance), prospectId, clientId]
    );
    await logRemediation(db, row, plan, provenance);
    return { applied: true, plan, email: plan.email, provenance };
  }

  if (plan.action === TAINTED_EMAIL_ACTIONS.INVALIDATE_CONTAMINATED) {
    const provenance = provenancePatchForInvalidate(row, plan);
    const notes = appendQuarantineNote(row.notes, plan.reason);
    await db.query(
      `UPDATE prospects
          SET email = NULL,
              email_verified = false,
              email_status = 'quarantined',
              notes = $1,
              enrichment_provenance = COALESCE(enrichment_provenance, '{}'::jsonb) || $2::jsonb,
              updated_at = NOW()
        WHERE id = $3
          AND client_id = $4`,
      [notes, JSON.stringify(provenance), prospectId, clientId]
    );
    await logRemediation(db, row, plan, provenance);
    return { applied: true, plan, email: null, quarantinedEmail: plan.email, provenance };
  }

  return { applied: false, reason: 'unknown_action' };
}

async function remediateTaintedCrmEmail(db, row, options = {}) {
  const plan = planTaintedCrmEmailRemediation(row);
  const result = await applyTaintedCrmEmailRemediation(db, row, plan, options);
  return { plan, ...result };
}

async function logRemediation(db, row, plan, provenance) {
  if (typeof db?.query !== 'function') return;
  const prospectId = row.prospect_id || row.id;
  await db.query(
    `INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
     VALUES ($1, $2, $3, $4::jsonb, $5, NOW(), $6)`,
    [
      'crm_email_provenance',
      plan.action,
      prospectId,
      JSON.stringify({
        reason: plan.reason,
        email: plan.email,
        provenance: resolveEmailProvenanceSource({
          enrichment_provenance: provenance,
          email_provenance_source: plan.provenance,
        }),
      }),
      'success',
      row.client_id,
    ]
  );
}

module.exports = {
  AUDIT_CONTACTS,
  persistableEmailSource,
  planTaintedCrmEmailRemediation,
  applyTaintedCrmEmailRemediation,
  remediateTaintedCrmEmail,
  snapshotRow,
};
