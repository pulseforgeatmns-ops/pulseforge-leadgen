'use strict';

const REVIEW_STATES = new Set(['contact_now', 'qualify_first', 'hold', 'reject']);

function text(value) {
  return value == null ? '' : String(value).trim();
}

function normalizeEmail(value) {
  const email = text(value).toLowerCase();
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) ? email : null;
}

function normalizePhone(value) {
  const raw = text(value);
  if (!raw) return null;
  const digits = raw.replace(/\D/g, '');
  if (digits.length === 10) return `(${digits.slice(0, 3)}) ${digits.slice(3, 6)}-${digits.slice(6)}`;
  if (digits.length === 11 && digits.startsWith('1')) {
    return `(${digits.slice(1, 4)}) ${digits.slice(4, 7)}-${digits.slice(7)}`;
  }
  return null;
}

function phoneDigits(value) {
  const digits = text(value).replace(/\D/g, '');
  return digits.length === 11 && digits.startsWith('1') ? digits.slice(1) : digits;
}

function normalizeUrl(value) {
  const raw = text(value);
  if (!raw) return null;
  try {
    const url = new URL(/^https?:\/\//i.test(raw) ? raw : `https://${raw}`);
    return ['http:', 'https:'].includes(url.protocol) ? url.toString() : null;
  } catch (_) {
    return null;
  }
}

function splitName(value) {
  const parts = text(value).split(/\s+/).filter(Boolean);
  if (!parts.length) return { firstName: '', lastName: '' };
  if (parts.length === 1) return { firstName: parts[0], lastName: '' };
  return { firstName: parts.slice(0, -1).join(' '), lastName: parts.at(-1) };
}

function normalizedState(value) {
  const compact = text(value).toLowerCase().replace(/[^a-z0-9]+/g, '_').replace(/^_|_$/g, '');
  if (compact.startsWith('contact_now')) return 'contact_now';
  if (compact.startsWith('qualify_first')) return 'qualify_first';
  if (compact.startsWith('hold')) return 'hold';
  if (compact.startsWith('reject')) return 'reject';
  return REVIEW_STATES.has(compact) ? compact : 'qualify_first';
}

function normalizeContact(input, index) {
  const name = text(input.name || input.contact || input.prospect);
  if (!name) throw Object.assign(new Error(`Contact ${index + 1} needs a name.`), { code: 'contact_name_required' });
  const { firstName, lastName } = splitName(name);
  const state = normalizedState(input.reviewState || input.status || input.recommendation);
  const sourceUrls = [...new Set([
    input.sourceUrl,
    input.dealSource,
    ...(Array.isArray(input.sourceUrls) ? input.sourceUrls : []),
  ].map(normalizeUrl).filter(Boolean))];
  const linkedinUrl = normalizeUrl(input.linkedinUrl || input.linkedin);
  const profileUrl = normalizeUrl(input.profileUrl || input.biggerPocketsUrl || input.biggerPockets);
  const email = normalizeEmail(input.email);
  const phone = normalizePhone(input.phone);
  const company = text(input.company || input.business) || `${name} — independent`;
  const market = text(input.market || input.location);
  const fit = text(input.fit || input.fitEvidence);
  const risks = text(input.risks || input.risk);
  const nextAction = text(input.nextAction || input.next_action);
  const category = text(input.category) || 'borrower';
  const score = { contact_now: 80, qualify_first: 65, hold: 40, reject: 10 }[state];

  return {
    name,
    firstName,
    lastName,
    company,
    market,
    email,
    phone,
    linkedinUrl,
    profileUrl,
    sourceUrls,
    fit,
    risks,
    nextAction,
    category,
    reviewState: state,
    status: state === 'reject' ? 'disqualified' : 'cold',
    icpScore: score,
  };
}

function noteFor(contact) {
  return [
    'LA Endeavors review-only prospect import. Automated outreach, calls, and campaign enrollment are not authorized.',
    `Review state: ${contact.reviewState.replace(/_/g, ' ')}.`,
    contact.market ? `Market: ${contact.market}.` : '',
    contact.fit ? `Fit evidence: ${contact.fit}` : '',
    contact.risks ? `Risks: ${contact.risks}` : '',
    contact.nextAction ? `Next action: ${contact.nextAction}` : '',
    contact.profileUrl ? `Public profile: ${contact.profileUrl}` : '',
    contact.sourceUrls.length ? `Sources: ${contact.sourceUrls.join(' | ')}` : '',
  ].filter(Boolean).join('\n');
}

function previewReviewOnlyContacts(inputs) {
  if (!Array.isArray(inputs) || !inputs.length) {
    throw Object.assign(new Error('Provide at least one contact.'), { code: 'contacts_required' });
  }
  if (inputs.length > 100) {
    throw Object.assign(new Error('A review-only import is limited to 100 contacts.'), { code: 'contact_limit_exceeded' });
  }
  const contacts = inputs.map(normalizeContact);
  const seen = new Set();
  for (const contact of contacts) {
    const key = contact.email
      ? `email:${contact.email}`
      : contact.phone
        ? `phone:${phoneDigits(contact.phone)}`
        : `identity:${contact.name.toLowerCase()}|${contact.company.toLowerCase()}`;
    if (seen.has(key)) {
      throw Object.assign(new Error(`Duplicate contact in import: ${contact.name}.`), { code: 'duplicate_contact' });
    }
    seen.add(key);
  }
  return contacts;
}

function matchesExisting(contact, row) {
  if (contact.email && normalizeEmail(row.email) === contact.email) return true;
  if (contact.phone && phoneDigits(row.phone) === phoneDigits(contact.phone)) return true;
  if (contact.linkedinUrl && normalizeUrl(row.linkedin_url) === contact.linkedinUrl) return true;
  return text(row.first_name).toLowerCase() === contact.firstName.toLowerCase()
    && text(row.last_name).toLowerCase() === contact.lastName.toLowerCase()
    && text(row.company_name).toLowerCase() === contact.company.toLowerCase();
}

async function importReviewOnlyContacts(inputs, { clientId, db, actorId }) {
  const contacts = previewReviewOnlyContacts(inputs);
  const connection = await db.connect();
  try {
    await connection.query('BEGIN');
    await connection.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`review-only-prospect-import:${clientId}`]);
    const existing = await connection.query(
      `SELECT p.id, p.first_name, p.last_name, p.email, p.phone, p.linkedin_url,
              c.name AS company_name
         FROM prospects p
         LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
        WHERE p.client_id = $1`,
      [clientId]
    );
    const inserted = [];
    const skipped = [];

    for (const contact of contacts) {
      const duplicate = existing.rows.find(row => matchesExisting(contact, row));
      if (duplicate) {
        skipped.push({ name: contact.name, prospectId: duplicate.id, reason: 'already_exists' });
        continue;
      }

      let company = await connection.query(
        'SELECT id, name FROM companies WHERE client_id = $1 AND lower(name) = lower($2) ORDER BY id ASC LIMIT 1',
        [clientId, contact.company]
      );
      if (!company.rows[0]) {
        company = await connection.query(
          'INSERT INTO companies(name, client_id) VALUES($1, $2) RETURNING id, name',
          [contact.company, clientId]
        );
      }

      const metadata = {
        importMode: 'review_only',
        outreachAuthorized: false,
        reviewState: contact.reviewState,
        category: contact.category,
        profileUrl: contact.profileUrl,
        sourceUrls: contact.sourceUrls,
        importedBy: actorId || null,
        importedAt: new Date().toISOString(),
      };
      const created = await connection.query(
        `INSERT INTO prospects(
           client_id, company_id, first_name, last_name, email, phone, job_title,
           linkedin_url, source, icp_score, status, notes, vertical,
           service_area_match, setter_visible, do_not_contact,
           ao_outreach_review_required, discovery_method, acquisition_metadata
         ) VALUES(
           $1,$2,$3,$4,$5,$6,$7,$8,'review_only_import',$9,$10,$11,'private_lending',
           $12,false,false,true,'operator_import',$13::jsonb
         )
         RETURNING id, first_name, last_name, email, phone, status,
                   ao_outreach_review_required, setter_visible`,
        [
          clientId,
          company.rows[0].id,
          contact.firstName,
          contact.lastName || null,
          contact.email,
          contact.phone,
          contact.category === 'capital_partner' ? 'Private capital partner' : 'Real-estate operator',
          contact.linkedinUrl,
          contact.icpScore,
          contact.status,
          noteFor(contact),
          contact.market || null,
          JSON.stringify(metadata),
        ]
      );
      const row = { ...created.rows[0], company_name: company.rows[0].name, review_state: contact.reviewState };
      inserted.push(row);
      existing.rows.push({ ...row, linkedin_url: contact.linkedinUrl });
    }

    await connection.query('COMMIT');
    return { inserted, skipped, requested: contacts.length };
  } catch (error) {
    await connection.query('ROLLBACK').catch(() => {});
    throw error;
  } finally {
    connection.release();
  }
}

module.exports = {
  normalizeContact,
  previewReviewOnlyContacts,
  importReviewOnlyContacts,
  noteFor,
};
