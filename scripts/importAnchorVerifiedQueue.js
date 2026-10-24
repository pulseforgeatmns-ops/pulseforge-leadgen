'use strict';

// Imports an operator-verified call queue for Anchor. This script deliberately
// never discovers leads, sends outreach, or records a call/disposition.
require('dotenv').config();

const fs = require('fs');
const path = require('path');
const pool = require('../db');
const { ANCHOR_PHONE_SETTER_CATEGORIES } = require('../utils/anchorPhoneSetter');
const { setSetterVisibility } = require('../utils/setterVisibility');

const CLIENT_ID = 10;
const APPLY_CONFIRMATION = 'client_10-anchor-verified-queue-2026-07-18';
const ALLOWED_FIELDS = new Set([
  'company', 'phone', 'vertical', 'contact_name', 'website', 'location',
  'notes', 'verification_source', 'verified_at', 'manual_verified',
]);

function normalizePhone(value) {
  const digits = String(value || '').replace(/\D/g, '');
  if (digits.length === 10) return `+1${digits}`;
  if (digits.length === 11 && digits.startsWith('1')) return `+${digits}`;
  return null;
}

function normalizeDomain(value) {
  if (!value) return null;
  try {
    return new URL(/^https?:\/\//i.test(value) ? value : `https://${value}`)
      .hostname.toLowerCase().replace(/^www\./, '');
  } catch (_err) {
    return null;
  }
}

function contactParts(value) {
  const parts = String(value || '').trim().split(/\s+/).filter(Boolean);
  return { firstName: parts[0] || null, lastName: parts.slice(1).join(' ') || null };
}

function validateRow(raw, index) {
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) {
    return { valid: false, index, errors: ['must be an object'] };
  }
  const unsupported = Object.keys(raw).filter(key => !ALLOWED_FIELDS.has(key));
  const company = String(raw.company || '').trim();
  const phone = normalizePhone(raw.phone);
  const vertical = String(raw.vertical || '').trim();
  const source = String(raw.verification_source || '').trim();
  const verifiedAt = new Date(raw.verified_at);
  const errors = [];
  if (unsupported.length) errors.push(`unsupported fields: ${unsupported.join(', ')}`);
  if (!company || company.length > 160) errors.push('company is required and must be 160 characters or fewer');
  if (!phone) errors.push('phone must be a valid North American number');
  if (!ANCHOR_PHONE_SETTER_CATEGORIES.includes(vertical)) errors.push('vertical must be an approved Anchor category');
  if (raw.manual_verified !== true) errors.push('manual_verified must be true');
  if (!source || source.length > 160) errors.push('verification_source is required');
  if (Number.isNaN(verifiedAt.getTime())) errors.push('verified_at must be an ISO timestamp');
  if (raw.website && !normalizeDomain(raw.website)) errors.push('website must be a valid URL or domain');
  return errors.length ? { valid: false, index, errors } : {
    valid: true,
    index,
    lead: {
      company,
      phone,
      vertical,
      contactName: String(raw.contact_name || '').trim() || null,
      website: raw.website ? String(raw.website).trim() : null,
      domain: normalizeDomain(raw.website),
      location: String(raw.location || '').trim() || null,
      notes: String(raw.notes || '').trim() || null,
      verificationSource: source,
      verifiedAt: verifiedAt.toISOString(),
    },
  };
}

function parseQueue(contents) {
  let parsed;
  try {
    parsed = JSON.parse(contents);
  } catch (_err) {
    throw new Error('Queue file must be valid JSON');
  }
  const rows = Array.isArray(parsed) ? parsed : parsed?.leads;
  if (!Array.isArray(rows) || !rows.length) throw new Error('Queue must be a non-empty JSON array or {"leads": [...]}');
  return rows.map(validateRow);
}

function duplicateKey(lead) {
  return `${lead.phone}|${lead.domain || ''}|${lead.company.toLowerCase().replace(/[^a-z0-9]/g, '')}`;
}

function findQueueDuplicates(leads) {
  const seen = new Map();
  return leads.map((lead, index) => {
    const key = duplicateKey(lead);
    const prior = seen.get(key);
    if (prior !== undefined) return { index, duplicateOf: prior };
    seen.set(key, index);
    return null;
  }).filter(Boolean);
}

async function existingMatches(db, lead, { lock = false } = {}) {
  const result = await db.query(`
    SELECT p.id AS prospect_id, p.phone, c.id AS company_id, c.name AS company_name, c.domain, c.website
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.client_id = $1
      AND (
        regexp_replace(COALESCE(p.phone, ''), '\\D', '', 'g') = regexp_replace($2, '\\D', '', 'g')
        OR lower(regexp_replace(COALESCE(c.name, ''), '[^a-z0-9]', '', 'g')) = lower(regexp_replace($3, '[^a-z0-9]', '', 'g'))
        OR ($4::text IS NOT NULL AND (lower(c.domain) = lower($4) OR lower(c.website) LIKE '%' || lower($4) || '%'))
      )
    ${lock ? 'FOR UPDATE OF p' : ''}
  `, [CLIENT_ID, lead.phone, lead.company, lead.domain]);
  return result.rows;
}

function importNotes(lead) {
  return [
    'Anchor verified queue import',
    `verification source: ${lead.verificationSource}`,
    `verified at: ${lead.verifiedAt}`,
    lead.notes,
  ].filter(Boolean).join(' | ');
}

async function insertLead(db, lead) {
  const company = await db.query(`
    INSERT INTO companies (name, domain, website, industry, location, client_id, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, NOW())
    RETURNING id
  `, [lead.company, lead.domain, lead.website, lead.vertical, lead.location, CLIENT_ID]);
  const contact = contactParts(lead.contactName);
  const prospect = await db.query(`
    INSERT INTO prospects (
      company_id, first_name, last_name, phone, status, source, notes, vertical,
      client_id, discovery_method, has_website, website_url, do_not_contact,
      preferred_channel, setter_status, is_hot
    ) VALUES ($1, $2, $3, $4, 'cold', 'scout', $5, $6, $7, 'operator_verified_queue',
              $8, $9, false, 'phone', 'new', false)
    RETURNING id
  `, [company.rows[0].id, contact.firstName, contact.lastName, lead.phone, importNotes(lead),
    lead.vertical, CLIENT_ID, Boolean(lead.website), lead.website]);
  await setSetterVisibility(db, prospect.rows[0].id, { reason: 'manual', clientId: CLIENT_ID, source: 'scout' });
  return prospect.rows[0].id;
}

async function run({ input, apply = false } = {}) {
  if (!input) throw new Error('Queue input is required. Use --input=/absolute/path/to/anchor-verified-queue.json');
  const queuePath = path.resolve(input);
  const validations = parseQueue(fs.readFileSync(queuePath, 'utf8'));
  const invalid = validations.filter(result => !result.valid);
  if (invalid.length) return { mode: apply ? 'APPLY' : 'REVIEW_ONLY', clientId: CLIENT_ID, input: queuePath, ok: false, invalid };
  const leads = validations.map(result => result.lead);
  const queueDuplicates = findQueueDuplicates(leads);
  if (queueDuplicates.length) return { mode: apply ? 'APPLY' : 'REVIEW_ONLY', clientId: CLIENT_ID, input: queuePath, ok: false, queueDuplicates };

  const results = [];
  for (const lead of leads) {
    const existing = await existingMatches(pool, lead);
    if (existing.length) {
      results.push({ company: lead.company, phone: lead.phone, action: 'dedupe_skip', existing });
      continue;
    }
    if (!apply) {
      results.push({ company: lead.company, phone: lead.phone, vertical: lead.vertical, action: 'would_insert' });
      continue;
    }
    const db = await pool.connect();
    try {
      await db.query('BEGIN');
      const recheck = await existingMatches(db, lead, { lock: true });
      if (recheck.length) {
        await db.query('ROLLBACK');
        results.push({ company: lead.company, phone: lead.phone, action: 'dedupe_skip_race', existing: recheck });
        continue;
      }
      const prospectId = await insertLead(db, lead);
      await db.query('COMMIT');
      results.push({ company: lead.company, phone: lead.phone, vertical: lead.vertical, action: 'inserted', prospectId });
    } catch (err) {
      await db.query('ROLLBACK');
      throw err;
    } finally {
      db.release();
    }
  }
  return { mode: apply ? 'APPLY' : 'REVIEW_ONLY', clientId: CLIENT_ID, input: queuePath, ok: true, results };
}

if (require.main === module) {
  const apply = process.argv.includes('--apply');
  const confirmation = process.argv.find(arg => arg.startsWith('--confirm='))?.slice('--confirm='.length);
  const input = process.argv.find(arg => arg.startsWith('--input='))?.slice('--input='.length);
  if (apply && confirmation !== APPLY_CONFIRMATION) {
    console.error(`Refusing writes. Use --apply --confirm=${APPLY_CONFIRMATION}`);
    process.exit(1);
  }
  run({ input, apply }).then(output => {
    console.log(JSON.stringify(output, null, 2));
    process.exit(output.ok ? 0 : 1);
  }).catch(err => {
    console.error(err.message);
    process.exit(1);
  });
}

module.exports = { APPLY_CONFIRMATION, findQueueDuplicates, normalizeDomain, normalizePhone, parseQueue, validateRow, run };
