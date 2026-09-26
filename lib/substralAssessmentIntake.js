/**
 * Studio Substral website assessment intake.
 *
 * Studio Substral is its own customer-facing brand (Design & Experience
 * Doctrine §24). Its public assessment request is captured here and surfaced to
 * the operator as an `agent_actions` row, the same way the Anchor Cleaning
 * funnel works, so a request never sits in an inbox.
 *
 * This module deliberately does not run the assessment. Website Opportunity
 * Intelligence is a separate capability with its own evidence-integrity rules
 * (packages/capabilities/websiteOpportunityIntelligence); an intake row is a
 * request for an assessment, not a finding about the domain. Nothing here may
 * imply a result.
 */

const {
  isSearchOrMapsDomain,
  isDirectoryDomain,
} = require('../packages/capabilities/websiteOpportunityIntelligence/discoveryAdmission');
const { classifyCompanyUrl } = require('../utils/canonicalEmailEligibility');

const CREATED_BY = 'studio_substral_site';
const ACTION_TYPE = 'website_assessment_request';
const SOURCE = 'studio_substral_assessment';

/** Studio Substral's requests land in the Pulseforge operator queue. */
const DEFAULT_CLIENT_ID = 1;

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;
const LABEL_RE = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const TLD_RE = /^[a-z]{2,24}$/;

/** Reject the operator's own properties: they are not assessment subjects. */
const OWN_DOMAINS = new Set([
  'studiosubstral.com',
  'gopulseforge.com',
  'goanchorcleaning.com',
]);

function clientId() {
  const raw = Number.parseInt(process.env.STUDIO_SUBSTRAL_CLIENT_ID || '', 10);
  return Number.isInteger(raw) && raw > 0 ? raw : DEFAULT_CLIENT_ID;
}

function cleanText(value, max) {
  const text = String(value == null ? '' : value).replace(/\s+/g, ' ').trim();
  return text ? text.slice(0, max) : '';
}

/**
 * Reduce anything a person might paste to a bare registrable host.
 *
 * Mirrors the client-side pass in
 * sites/studio-substral/assets/js/assessment.js, but this side is
 * authoritative — the browser check exists only to save a round trip.
 *
 * @returns {{ ok: true, domain: string } | { ok: false, reason: string }}
 */
function normalizeAssessmentDomain(raw) {
  let value = String(raw == null ? '' : raw)
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '');

  if (!value) return { ok: false, reason: 'empty' };

  value = value
    .replace(/^[a-z][a-z0-9+.-]*:\/\//, '')
    .replace(/^[^@/]*@/, '')
    .split(/[/?#]/)[0]
    .replace(/:\d+$/, '')
    .replace(/\.$/, '');

  if (value.startsWith('www.')) value = value.slice(4);
  if (!value) return { ok: false, reason: 'empty' };
  if (value.length > 253) return { ok: false, reason: 'malformed' };

  if (
    value === 'localhost' ||
    value.endsWith('.localhost') ||
    value.includes(':') ||
    /^\d{1,3}(\.\d{1,3}){3}$/.test(value)
  ) {
    return { ok: false, reason: 'not_public' };
  }

  const labels = value.split('.');
  if (labels.length < 2) return { ok: false, reason: 'no_tld' };
  if (!labels.every((label) => LABEL_RE.test(label))) {
    return { ok: false, reason: 'malformed' };
  }
  if (!TLD_RE.test(labels[labels.length - 1])) return { ok: false, reason: 'no_tld' };

  if (OWN_DOMAINS.has(value)) return { ok: false, reason: 'own_domain' };
  if (isSearchOrMapsDomain(value) || isDirectoryDomain(value)) {
    return { ok: false, reason: 'not_a_subject' };
  }

  const classification = classifyCompanyUrl(`https://${value}`);
  if (classification === 'social_profile' || classification === 'url_shortener') {
    return { ok: false, reason: 'not_a_subject' };
  }

  return { ok: true, domain: value };
}

/**
 * @returns {{ ok: true, values: object } | { ok: false, errors: object }}
 */
function validateAssessmentPayload(body = {}) {
  const errors = {};

  const parsed = normalizeAssessmentDomain(body.domain);
  if (!parsed.ok) errors.domain = parsed.reason;

  const email = cleanText(body.email, 320).toLowerCase();
  if (!EMAIL_RE.test(email)) errors.email = 'email';

  if (Object.keys(errors).length) return { ok: false, errors };

  return {
    ok: true,
    values: {
      domain: parsed.domain,
      email,
      context: cleanText(body.context, 300) || null,
      referer: cleanText(body.referer, 500) || null,
    },
  };
}

function buildAssessmentActionPayload(values) {
  return {
    source: SOURCE,
    brand: 'Studio Substral',
    // Requested, never assessed. The evidence classes belong to the
    // capability that actually measures the domain.
    stage: 'requested',
    subject_domain: values.domain,
    reply_to: values.email,
    stated_context: values.context,
    referer: values.referer,
    requested_at: new Date().toISOString(),
  };
}

/**
 * @param {object} pool  pg pool (injected so the intake is testable)
 */
async function captureAssessmentRequest(pool, values) {
  const payload = buildAssessmentActionPayload(values);
  const description = [values.domain, values.email, values.context]
    .filter(Boolean)
    .join(' · ');

  const inserted = await pool.query(
    `INSERT INTO agent_actions
       (created_by, action_type, title, description, payload, status, client_id)
     VALUES ($1, $2, $3, $4, $5::jsonb, 'pending', $6)
     RETURNING id`,
    [
      CREATED_BY,
      ACTION_TYPE,
      `Assessment request — ${values.domain}`,
      description,
      JSON.stringify(payload),
      clientId(),
    ]
  );

  return {
    id: inserted.rows[0]?.id || null,
    stored: Boolean(inserted.rows[0]?.id),
    domain: values.domain,
    client_id: clientId(),
  };
}

module.exports = {
  ACTION_TYPE,
  CREATED_BY,
  SOURCE,
  DEFAULT_CLIENT_ID,
  OWN_DOMAINS,
  normalizeAssessmentDomain,
  validateAssessmentPayload,
  buildAssessmentActionPayload,
  captureAssessmentRequest,
  resolveClientId: clientId,
};
