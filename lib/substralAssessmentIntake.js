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
const { createHash } = require('node:crypto');

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
  const configured = process.env.STUDIO_SUBSTRAL_CLIENT_ID;
  if (!configured) return DEFAULT_CLIENT_ID;
  const raw = Number(configured);
  if (!Number.isSafeInteger(raw) || raw < 1) throw new Error('Invalid Studio Substral review tenant');
  return raw;
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
  if (!body || typeof body !== 'object' || Array.isArray(body)) body = {};
  const errors = {};

  const parsed = normalizeAssessmentDomain(body.domain);
  if (!parsed.ok) errors.domain = parsed.reason;

  const email = typeof body.email === 'string' ? body.email.trim().toLowerCase() : '';
  if (email.length > 254 || !EMAIL_RE.test(email)) errors.email = 'email';
  const requestKey = body.request_key;
  if (requestKey != null && (typeof requestKey !== 'string' || !/^[a-zA-Z0-9_-]{16,80}$/.test(requestKey))) {
    errors.request_key = 'request_key';
  }

  if (Object.keys(errors).length) return { ok: false, errors };

  return {
    ok: true,
    values: {
      domain: parsed.domain,
      email,
      context: cleanText(body.context, 300) || null,
      referer: cleanText(body.referer, 500) || null,
      ...(requestKey ? { request_key: requestKey } : {}),
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
    review_mode: 'human',
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
  const tenant = clientId();
  // Native forms have no browser-generated key. Repeat submissions of the
  // same fields within a UTC day still resolve to one durable queue item.
  const fingerprint = createHash('sha256').update(JSON.stringify([
    values.domain, values.email, values.context || null,
  ])).digest('hex');
  payload.request_key = values.request_key || createHash('sha256')
    .update(`${fingerprint}:${new Date().toISOString().slice(0, 10)}`).digest('hex');
  payload.request_fingerprint = fingerprint;
  const description = [values.domain, values.email, values.context]
    .filter(Boolean)
    .join(' · ');

  const inserted = await pool.query(
    `INSERT INTO agent_actions
       (created_by, action_type, title, description, payload, status, client_id)
     VALUES ($1, $2, $3, $4, $5::jsonb, 'pending', $6)
     ON CONFLICT (client_id, (payload->>'request_key'))
       WHERE created_by = 'studio_substral_site'
         AND action_type = 'website_assessment_request'
     DO NOTHING
     RETURNING id`,
    [
      CREATED_BY,
      ACTION_TYPE,
      `Assessment request — ${values.domain}`,
      description,
      JSON.stringify(payload),
      tenant,
    ]
  );

  let row = inserted.rows[0];
  const duplicate = !row;
  if (!row) {
    // A separate READ COMMITTED statement sees a concurrent winning insert.
    const existing = await pool.query(
      `SELECT id, payload FROM agent_actions WHERE client_id = $1
       AND created_by = 'studio_substral_site' AND action_type = 'website_assessment_request'
       AND payload->>'request_key' = $2`, [tenant, payload.request_key]
    );
    row = existing.rows[0];
    if (!row) throw new Error('Assessment request was not persisted');
    if (row.payload.request_fingerprint !== fingerprint) {
      const error = new Error('This request key belongs to different form details. Reload the page and try again.');
      error.code = 'REQUEST_KEY_CONFLICT';
      throw error;
    }
  }
  if (!row.id) throw new Error('Assessment request was not persisted');
  return {
    id: row.id,
    stored: true,
    duplicate,
    domain: values.domain,
    client_id: tenant,
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
