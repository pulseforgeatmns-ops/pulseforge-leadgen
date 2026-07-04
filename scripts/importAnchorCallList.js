'use strict';

require('dotenv').config();

const axios = require('axios');
const pool = require('../db');
const { scoreCleaningLead } = require('../leadgen');
const { applyProspectDisposition, resolveCallbackAt } = require('../utils/callDispositions');
const { setSetterVisibility } = require('../utils/setterVisibility');

const CLIENT_ID = 10;
const APPLY_CONFIRMATION = 'client_10-anchor-phase3b';
const PLACES_URL = 'https://places.googleapis.com/v1/places:searchText';
const SERVICE_AREA = Object.freeze(['Manchester', 'Bedford', 'Goffstown', 'Hooksett', 'Londonderry', 'Auburn']);
const NOW = new Date('2026-07-02T14:00:00.000Z'); // 10:00 America/New_York on manifest day.

const manifest = Object.freeze([
  // Group A — import cold.
  { group: 'A', firm: 'Porter Tax Preparation', phone: '(603) 421-6895', city: 'Londonderry', contact: 'Roger Porter', vertical: 'accounting', address: '80 Nashua Rd', notes: '59 reviews / 5.0' },
  { group: 'A', firm: 'Picchi & Martel CPA', phone: '(603) 432-3394', city: 'Londonderry', contact: 'Ben', vertical: 'accounting', address: '50 Nashua Rd', cluster: '50_nashua_rd' },
  { group: 'A', firm: 'David Picchi CPA', phone: '(603) 432-3394', city: 'Londonderry', contact: 'David Picchi', vertical: 'accounting', address: '50 Nashua Rd', cluster: '50_nashua_rd', notes: 'shared phone' },
  { group: 'A', firm: 'Prime Business Services', phone: '(603) 432-2542', city: 'Londonderry', contact: 'Kurt', vertical: 'accounting', address: '50 Nashua Rd', cluster: '50_nashua_rd' },
  { group: 'A', firm: 'Lekas, Edgar & Co.', phone: '(603) 434-2889', city: 'Londonderry', contact: 'Kerry Lekas', vertical: 'accounting', address: '12 Parmenter Rd' },
  { group: 'A', firm: "Williamson's Tax & Financial", phone: '(603) 232-5913', city: 'Bedford', contact: 'Bruce / Manon', vertical: 'accounting', address: '288 S River Rd', notes: 'open to 7pm' },
  { group: 'A', firm: 'Collette Professional Accounting', phone: '(603) 232-7436', city: 'Bedford', contact: 'Kathleen Collette', vertical: 'accounting', address: '19 Kilton Rd', notes: 'odd hours Tue/Thu/Fri/Sat' },
  { group: 'A', firm: 'Druke Accounting', phone: '(603) 226-2226', city: 'Bedford', contact: null, vertical: 'accounting', address: '3 Executive Park Dr', cluster: '3_executive_park_dr', notes: 'same building as NH Tax Advisors' },
  { group: 'A', firm: 'J. Edward Meyer CPA', phone: '(603) 488-2455', city: 'Bedford', contact: 'J. Edward Meyer', vertical: 'accounting', address: '4 Bell Hill Rd', cluster: '4_bell_hill_rd' },
  { group: 'A', firm: 'Michael R. St. Louis', phone: '(781) 816-3950', city: 'Manchester', contact: 'Michael St. Louis', vertical: 'accounting', verticalAmbiguous: true, address: '875 Elm St', notes: 'solo, answers own phone' },
  { group: 'A', firm: 'Brian T. Lee', phone: '(603) 858-3500', city: 'Goffstown', contact: 'Brian T. Lee', vertical: 'accounting', verticalAmbiguous: true, address: '51 W Union St', notes: 'solo' },
  { group: 'A', firm: 'Bryan Clickner', phone: '(877) 723-7200', city: 'Goffstown', contact: 'Bryan Clickner', vertical: 'law_firm', verticalAmbiguous: true, address: '152 S Mast St', notes: 'bankruptcy/general' },
  { group: 'A', firm: 'R. John Roy', phone: '(603) 669-3363', city: 'Hooksett', contact: 'John Roy', vertical: 'accounting', verticalAmbiguous: true, address: '146 Londonderry Tpke', notes: 'solo, 5.0' },
  { group: 'A', firm: 'Klug Law Offices', phone: '(603) 606-2078', city: 'Manchester', contact: 'Achsa Klug', vertical: 'law_firm', address: '37 Bay St', notes: 'family law, closed Fri' },
  { group: 'A', firm: 'Solomon Law Firm', phone: '(603) 945-9977', city: 'Manchester', contact: 'Peter Solomon', vertical: 'law_firm', address: '36 Salmon St', cluster: 'salmon_st_manchester', notes: 'PI, 5.0' },

  // Group B — import and transfer the reviewed disposition.
  { group: 'B', firm: 'Freedom Accounting', phone: '(603) 232-5153', city: null, contact: 'Keisha', vertical: 'accounting', disposition: 'answered_callback', callbackAt: '2026-07-06T14:00:00.000Z', notes: 'Keisha unavailable last call; retry Mon-Thu' },
  { group: 'B', firm: 'NH Tax Advisors', phone: '(603) 860-6000', city: 'Bedford', contact: 'Chris Brown', vertical: 'accounting', address: '3 Executive Park Dr', cluster: '3_executive_park_dr', disposition: 'voicemail', notes: 'confirm number is the firm; ask for Chris Brown' },
  { group: 'B', firm: 'Altair Group', phone: '(603) 621-6188', city: 'Bedford', contact: 'Shannon Hudson', vertical: 'accounting', disposition: 'voicemail', notes: 'boutique CPA' },
  { group: 'B', firm: 'Karr & Boucher', phone: '(603) 625-8286', city: null, contact: null, vertical: 'accounting', verticalAmbiguous: true, cluster: 'salmon_st_manchester', disposition: 'voicemail', notes: 'open 7am-7pm, easy to catch live' },
  { group: 'B', firm: 'Lombardi Law', phone: '(603) 471-9110', city: 'Bedford', contact: 'Attorney Lombardi', vertical: 'law_firm', cluster: '4_bell_hill_rd', disposition: 'voicemail', notes: 'solo trust/estate' },
  { group: 'B', firm: 'Andrew Sullivan', phone: '(603) 644-5291', city: 'Bedford', contact: 'Andrew Sullivan', vertical: 'law_firm', disposition: 'gatekeeper_relayed', callbackAt: '2026-07-09T14:00:00.000Z', notes: '~1wk follow-up' },
  { group: 'B', firm: 'Corallino', phone: '(603) 623-5557', city: null, contact: 'Bob Corallino (gk: Meg)', vertical: 'law_firm', verticalAmbiguous: true, disposition: 'gatekeeper_relayed' },
  { group: 'B', firm: 'Gelinas & Pratte', phone: '(603) 625-8931', city: null, contact: null, vertical: 'accounting', cluster: 'salmon_st_manchester', disposition: 'incumbent_all_set', callbackAt: '2026-09-30T14:00:00.000Z', notes: 'has cleaner; 90-day nurture' },
  { group: 'B', firm: 'Ansell & Anderson', phone: '(603) 644-8211', city: 'Bedford', contact: null, vertical: 'law_firm', disposition: 'incumbent_all_set', callbackAt: '2026-09-30T14:00:00.000Z', notes: 'estate planning; has cleaner; 90-day nurture' },
  { group: 'B', firm: 'Patrick Kelly', phone: null, city: null, contact: 'Patrick Kelly', vertical: 'law_firm', disposition: 'incumbent_all_set', decision: 'skip_retiring', notes: 'retiring soon' },

  // Group C — existing rows only.
  { group: 'C', firm: 'Tenn And Tenn', phone: '(603) 614-5055', contact: 'John Tenn', disposition: 'gatekeeper_relayed' },
  { group: 'C', firm: 'Ward Law Group', contact: 'John Ward', disposition: 'gatekeeper_relayed', emailTouchpoint: true },
  { group: 'C', firm: 'Manning Zimmerman & Oliveira', contact: 'Michaila Oliveira', disposition: 'gatekeeper_relayed', callbackAt: '2026-07-09T14:00:00.000Z', notes: 'Gatekeeper said all set; likely not relayed. Preserve as follow-up for email batch 2.' },
  { group: 'C', firm: 'Cohen & Winters', contact: 'Dorothy Darby', special: 'callback_note_only', callbackAt: '2026-07-09T14:00:00.000Z', notes: 'Emailed walkthrough offer; callback if no reply. Has a cleaner (backup play). No completed call.' },
]);

function digits(value) {
  return String(value || '').replace(/\D/g, '');
}

function normalizeName(value) {
  return String(value || '').toLowerCase().replace(/\b(llc|pllc|pc|p c|inc|the)\b/g, '').replace(/[^a-z0-9]/g, '');
}

function nameTokens(value) {
  const ignored = new Set(['the', 'of', 'and', 'law', 'office', 'offices', 'firm', 'group', 'llc', 'pllc', 'pa', 'pc', 'cpa']);
  return String(value || '').toLowerCase().replace(/[^a-z0-9 ]/g, ' ').split(/\s+/).filter(token => token.length > 1 && !ignored.has(token));
}

function normalizeDomain(value) {
  if (!value) return null;
  try {
    return new URL(/^https?:\/\//i.test(value) ? value : `https://${value}`).hostname.toLowerCase().replace(/^www\./, '');
  } catch (_err) {
    return null;
  }
}

function addressKey(value) {
  return String(value || '').toLowerCase().replace(/\b(road)\b/g, 'rd').replace(/\b(street)\b/g, 'st').replace(/\b(turnpike)\b/g, 'tpke').replace(/[^a-z0-9]/g, '');
}

function resolveVertical(place, fallback) {
  const category = [place.primaryType, place.primaryTypeDisplayName?.text, ...(place.types || [])].filter(Boolean).join(' ').toLowerCase();
  if (/lawyer|law firm|attorney|legal/.test(category)) return { vertical: 'law_firm', basis: `Places category: ${category}` };
  if (/account|tax|finance|financial|bookkeep|certified public/.test(category)) return { vertical: 'accounting', basis: `Places category: ${category}` };
  return { vertical: fallback, basis: `manifest fallback; Places category unresolved: ${category || 'none'}` };
}

function choosePlace(row, places) {
  const expectedPhone = digits(row.phone);
  const expectedAddress = addressKey(row.address);
  const expectedName = normalizeName(row.firm);
  const ranked = places.map(place => {
    const phoneMatch = expectedPhone && [place.nationalPhoneNumber, place.internationalPhoneNumber].some(p => digits(p) === expectedPhone);
    const placeAddress = addressKey(place.formattedAddress);
    const addressMatch = expectedAddress && placeAddress.includes(expectedAddress);
    const placeName = normalizeName(place.displayName?.text);
    const expectedTokens = nameTokens(row.firm);
    const placeTokens = new Set(nameTokens(place.displayName?.text));
    const tokenMatch = expectedTokens.length > 0 && expectedTokens.every(token => placeTokens.has(token));
    const nameMatch = placeName === expectedName || placeName.includes(expectedName) || expectedName.includes(placeName) || tokenMatch;
    const cityMatch = row.city && String(place.formattedAddress || '').toLowerCase().includes(row.city.toLowerCase());
    const confidence = (addressMatch ? 5 : 0) + (phoneMatch ? 4 : 0) + (nameMatch ? 2 : 0) + (cityMatch ? 1 : 0);
    return { place, confidence, addressMatch: Boolean(addressMatch), phoneMatch: Boolean(phoneMatch), nameMatch: Boolean(nameMatch), cityMatch: Boolean(cityMatch) };
  }).sort((a, b) => b.confidence - a.confidence);
  const best = ranked[0];
  if (!best) return { verified: false, reason: 'no Places results' };
  const verified = row.address
    ? best.addressMatch && (best.nameMatch || best.phoneMatch)
    : best.phoneMatch && best.nameMatch;
  return { ...best, verified, reason: verified ? (row.address ? 'address + name/phone match' : 'manifest omitted address; exact phone + name match') : 'high-confidence match not established' };
}

async function verifyPlace(row) {
  const key = process.env.GOOGLE_PLACES_KEY;
  if (!key) throw new Error('GOOGLE_PLACES_KEY is required');
  const query = [row.firm, row.address, row.city, 'NH'].filter(Boolean).join(' ');
  const request = textQuery => axios.post(PLACES_URL, { textQuery, maxResultCount: 5 }, {
    headers: {
      'Content-Type': 'application/json',
      'X-Goog-Api-Key': key,
      'X-Goog-FieldMask': 'places.id,places.displayName,places.formattedAddress,places.nationalPhoneNumber,places.internationalPhoneNumber,places.websiteUri,places.rating,places.userRatingCount,places.primaryType,places.primaryTypeDisplayName,places.types',
    },
    timeout: 30000,
  });
  const response = await request(query);
  const primary = { query, ...choosePlace(row, response.data?.places || []) };
  if (primary.verified || !row.address) return primary;

  const alternateQuery = [row.firm, row.phone, row.city, 'NH'].filter(Boolean).join(' ');
  const alternateResponse = await request(alternateQuery);
  const alternate = choosePlace({ ...row, address: null }, alternateResponse.data?.places || []);
  if (!alternate.verified) return { ...primary, alternateQuery, alternateCandidate: alternate.place || null };
  return {
    ...alternate,
    verified: false,
    query,
    alternateQuery,
    reason: 'manifest address mismatch; alternate query found exact phone + name match',
  };
}

function serviceAreaCity(place) {
  const address = String(place?.formattedAddress || '').toLowerCase();
  return SERVICE_AREA.find(city => address.includes(city.toLowerCase())) || null;
}

async function loadClientProspects(db, { lock = false } = {}) {
  const result = await db.query(`
    SELECT p.id AS prospect_id, p.phone, p.setter_visible, c.id AS company_id,
           c.name AS company_name, c.domain, c.website
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.client_id = $1
    ORDER BY c.name, p.created_at
    ${lock ? 'FOR UPDATE OF p' : ''}
  `, [CLIENT_ID]);
  return result.rows;
}

function findExistingRows(rows, row, place) {
  const names = [row.firm, place?.displayName?.text].filter(Boolean).map(normalizeName);
  const phone = digits(row.phone);
  const domain = normalizeDomain(place?.websiteUri);
  return rows.filter(existing =>
    names.includes(normalizeName(existing.company_name)) ||
    (phone && digits(existing.phone) === phone) ||
    (domain && [existing.domain, existing.website].some(value => normalizeDomain(value) === domain))
  );
}

async function findExisting(db, row, place, { lock = false } = {}) {
  return findExistingRows(await loadClientProspects(db, { lock }), row, place);
}

function contactParts(contact) {
  const clean = String(contact || '').replace(/\s*\(.*$/, '').trim();
  if (!clean) return { firstName: null, lastName: null };
  if (clean.includes('/')) return { firstName: clean.split('/')[0].trim(), lastName: null };
  const parts = clean.split(/\s+/);
  return { firstName: parts[0] || null, lastName: parts.slice(1).join(' ') || null };
}

function buildLead(row, place, vertical, city) {
  return {
    company: place.displayName?.text || row.firm,
    url: place.websiteUri || '',
    phone: row.phone,
    contact: row.contact || '—',
    address: place.formattedAddress,
    snippet: `${vertical.replaceAll('_', ' ')} ${row.notes || ''}`,
    google_rating: place.rating ?? null,
    google_review_count: place.userRatingCount ?? null,
    city,
  };
}

function importNotes(row, place, verification, scoreResult) {
  return [
    `Anchor Phase 3b manifest; contact: ${row.contact || 'unknown'}`,
    `Google Place ID: ${place.id}`,
    `Places category: ${place.primaryTypeDisplayName?.text || place.primaryType || (place.types || []).join(', ') || 'unknown'}`,
    `Places verification: ${verification.reason}`,
    row.cluster ? `cluster:${row.cluster}` : null,
    row.notes || null,
    scoreResult.flags.length ? `ICP flags: ${scoreResult.flags.join('; ')}` : null,
  ].filter(Boolean).join(' | ');
}

async function insertProspect(db, row, place, verticalInfo, city, scoreResult, verification) {
  const domain = normalizeDomain(place.websiteUri);
  const companyResult = await db.query(`
    INSERT INTO companies (name, domain, website, industry, location, icp_score, client_id, created_at)
    VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
    RETURNING id
  `, [place.displayName?.text || row.firm, domain, place.websiteUri || null, verticalInfo.vertical, place.formattedAddress, scoreResult.total, CLIENT_ID]);
  const contact = contactParts(row.contact);
  const prospectResult = await db.query(`
    INSERT INTO prospects (
      company_id, first_name, last_name, phone, status, source, icp_score, notes, vertical,
      client_id, service_area_match, discovery_method, has_website, google_review_count,
      google_rating, website_url, do_not_contact, preferred_channel, setter_status, is_hot
    ) VALUES ($1, $2, $3, $4, 'cold', 'scout', $5, $6, $7, $8, $9,
              'google_places', $10, $11, $12, $13, false, 'phone', 'new', false)
    RETURNING id
  `, [companyResult.rows[0].id, contact.firstName, contact.lastName, row.phone, scoreResult.total,
    importNotes(row, place, verification, scoreResult), verticalInfo.vertical, CLIENT_ID, city,
    Boolean(place.websiteUri), place.userRatingCount ?? null, place.rating ?? null, place.websiteUri || null]);
  const prospectId = prospectResult.rows[0].id;
  await setSetterVisibility(db, prospectId, { reason: 'manual', clientId: CLIENT_ID, source: 'scout' });
  return prospectId;
}

function dispositionSentiment(disposition) {
  if (disposition === 'answered_interested') return 'positive';
  if (['answered_not_interested', 'wrong_number', 'disconnected', 'gatekeeper_blocked'].includes(disposition)) return 'negative';
  return 'neutral';
}

async function writeDisposition(db, prospectId, row) {
  const callbackAt = resolveCallbackAt(row.disposition, row.callbackAt || null, NOW);
  const externalRef = `anchor_phase3b:call:${normalizeName(row.firm)}`;
  const prior = await db.query(`
    SELECT id FROM touchpoints
    WHERE prospect_id = $1 AND client_id = $2 AND external_ref = $3
    ORDER BY created_at
  `, [prospectId, CLIENT_ID, externalRef]);
  if (prior.rows.length) return callbackAt;
  const inserted = await db.query(`
    INSERT INTO call_dispositions
      (prospect_id, client_id, disposition, notes, source, callback_at)
    VALUES ($1, $2, $3, $4, 'setter', $5)
    RETURNING id
  `, [prospectId, CLIENT_ID, row.disposition, row.notes || null, callbackAt]);
  await db.query(`
    INSERT INTO touchpoints
      (prospect_id, channel, action_type, content_summary, outcome, sentiment, external_ref, client_id)
    VALUES ($1, 'call', 'call_disposition', $2, $3, $4, $5, $6)
  `, [prospectId, `Manifest call: ${row.disposition.replaceAll('_', ' ')}${row.notes ? ` — ${row.notes}` : ''}`,
    JSON.stringify({ disposition: row.disposition, callback_at: callbackAt?.toISOString() || null, source: 'setter' }),
    dispositionSentiment(row.disposition), externalRef, CLIENT_ID]);
  await applyProspectDisposition(db, { prospectId, clientId: CLIENT_ID, disposition: row.disposition, callbackAt });
  return callbackAt;
}

async function writeEmailTouchpoint(db, prospectId) {
  await db.query(`
    INSERT INTO touchpoints
      (prospect_id, channel, action_type, content_summary, outcome, sentiment, external_ref, client_id)
    VALUES ($1, 'email', 'manual_email_sent', $2, $3, 'neutral', $4, $5)
  `, [prospectId, 'Hand-sent email logged from Anchor Phase 3b manifest',
    JSON.stringify({ source: 'setter', sent_on: '2026-07-02' }), 'anchor_phase3b:ward_email:2026-07-02', CLIENT_ID]);
}

async function writeCallbackNoteOnly(db, prospectId, row) {
  await db.query(`
    UPDATE prospects
    SET notes = concat_ws(' | ', nullif(notes, ''), $1::text), callback_at = $2,
        setter_status = 'follow_up', setter_updated_at = NOW(), updated_at = NOW()
    WHERE id = $3 AND client_id = $4
  `, [`Anchor Phase 3b: ${row.notes}`, new Date(row.callbackAt), prospectId, CLIENT_ID]);
}

function resolveExistingGroupC(rows, row) {
  const names = [normalizeName(row.firm), ...(row.aliases || []).map(normalizeName)];
  const expectedTokens = nameTokens(row.firm);
  const phone = digits(row.phone);
  return rows.filter(existing =>
    names.includes(normalizeName(existing.company_name)) ||
    (expectedTokens.length > 0 && expectedTokens.every(token => new Set(nameTokens(existing.company_name)).has(token))) ||
    (phone && digits(existing.phone) === phone)
  );
}

function plannedDuplicate(row, place, planned) {
  const names = [row.firm, place?.displayName?.text].filter(Boolean).map(normalizeName);
  const phone = digits(row.phone);
  const domain = normalizeDomain(place?.websiteUri);
  return planned.find(item =>
    item.names.some(name => names.includes(name)) ||
    (phone && item.phone === phone) ||
    (domain && item.domain === domain)
  ) || null;
}

async function processImportRow(row, apply, planned, existingSnapshot) {
  if (row.decision === 'skip_retiring') return { firm: row.firm, group: row.group, action: 'skip', reason: 'retiring; excluded before Places or DB work' };
  const verification = await verifyPlace(row);
  if (!verification.verified) return {
    firm: row.firm,
    group: row.group,
    action: 'blocked_unverified',
    query: verification.query,
    reason: verification.reason,
    bestCandidate: verification.place ? {
      name: verification.place.displayName?.text || null,
      address: verification.place.formattedAddress || null,
      phone: verification.place.nationalPhoneNumber || verification.place.internationalPhoneNumber || null,
      placeId: verification.place.id || null,
      confidence: verification.confidence,
      addressMatch: verification.addressMatch,
      phoneMatch: verification.phoneMatch,
      nameMatch: verification.nameMatch,
    } : null,
    alternateQuery: verification.alternateQuery || null,
    alternateCandidate: verification.alternateCandidate ? {
      name: verification.alternateCandidate.displayName?.text || null,
      address: verification.alternateCandidate.formattedAddress || null,
      phone: verification.alternateCandidate.nationalPhoneNumber || verification.alternateCandidate.internationalPhoneNumber || null,
      placeId: verification.alternateCandidate.id || null,
    } : null,
  };
  const place = verification.place;
  const city = serviceAreaCity(place);
  const vertical = resolveVertical(place, row.vertical);
  const lead = buildLead(row, place, vertical.vertical, city);
  const scoreResult = scoreCleaningLead(lead);
  const existing = findExistingRows(existingSnapshot, row, place);
  const manifestDuplicate = plannedDuplicate(row, place, planned);
  const report = {
    firm: row.firm, group: row.group,
    action: existing.length ? 'dedupe_skip' : manifestDuplicate ? 'manifest_dedupe_skip' : (apply ? 'inserted' : 'would_insert'),
    match: verification.reason, canonicalName: place.displayName?.text, address: place.formattedAddress,
    placeId: place.id, category: place.primaryTypeDisplayName?.text || place.primaryType || null,
    website: place.websiteUri || null, rating: place.rating ?? null,
    vertical: vertical.vertical, verticalBasis: vertical.basis, icpScore: scoreResult.total,
    icpComponents: scoreResult.components, serviceArea: city, existing,
    manifestDuplicate: manifestDuplicate?.firm || null,
    disposition: row.disposition || null,
    callbackAt: row.disposition ? resolveCallbackAt(row.disposition, row.callbackAt || null, NOW)?.toISOString() || null : null,
  };
  if (!city) return { ...report, action: 'blocked_out_of_area' };
  if (existing.length || manifestDuplicate) return report;
  planned.push({ firm: row.firm, names: [row.firm, place.displayName?.text].filter(Boolean).map(normalizeName), phone: digits(row.phone), domain: normalizeDomain(place.websiteUri) });
  if (!apply) return report;

  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    const recheck = await findExisting(db, row, place, { lock: true });
    if (recheck.length) {
      await db.query('ROLLBACK');
      return { ...report, action: 'dedupe_skip_race', existing: recheck };
    }
    const prospectId = await insertProspect(db, row, place, vertical, city, scoreResult, verification);
    let callbackAt = null;
    if (row.disposition) callbackAt = await writeDisposition(db, prospectId, row);
    await db.query('COMMIT');
    return { ...report, prospectId, disposition: row.disposition || null, callbackAt: callbackAt?.toISOString() || null };
  } catch (err) {
    await db.query('ROLLBACK');
    throw err;
  } finally {
    db.release();
  }
}

async function processGroupCRow(row, apply, existingSnapshot) {
  const existing = resolveExistingGroupC(existingSnapshot, row);
  const previewCallback = row.disposition ? resolveCallbackAt(row.disposition, row.callbackAt || null, NOW)?.toISOString() || null : row.callbackAt || null;
  const report = { firm: row.firm, group: row.group, action: existing.length === 1 ? (apply ? 'updated_existing' : 'would_update_existing') : 'blocked_existing_match', existing };
  if (existing.length !== 1 || !apply) return { ...report, disposition: row.disposition || null, callbackAt: previewCallback, emailTouchpoint: Boolean(row.emailTouchpoint), special: row.special || null };
  const db = await pool.connect();
  try {
    await db.query('BEGIN');
    const prospectId = existing[0].prospect_id;
    let callbackAt = null;
    if (row.disposition) callbackAt = await writeDisposition(db, prospectId, row);
    if (row.emailTouchpoint) await writeEmailTouchpoint(db, prospectId);
    if (row.special === 'callback_note_only') await writeCallbackNoteOnly(db, prospectId, row);
    await db.query('COMMIT');
    return { ...report, prospectId, disposition: row.disposition || null, callbackAt: callbackAt?.toISOString() || row.callbackAt || null, emailTouchpoint: Boolean(row.emailTouchpoint), special: row.special || null };
  } catch (err) {
    await db.query('ROLLBACK');
    throw err;
  } finally {
    db.release();
  }
}

async function run({ apply = false, firm = null } = {}) {
  const results = [];
  const planned = [];
  const existingSnapshot = await loadClientProspects(pool);
  const selected = firm ? manifest.filter(row => row.firm.toLowerCase() === firm.toLowerCase()) : manifest;
  if (!selected.length) throw new Error(`Manifest firm not found: ${firm}`);
  for (const row of selected) {
    try {
      results.push(row.group === 'C'
        ? await processGroupCRow(row, apply, existingSnapshot)
        : await processImportRow(row, apply, planned, existingSnapshot));
    } catch (err) {
      results.push({ firm: row.firm, group: row.group, action: 'error', error: err.message });
    }
  }
  const output = { mode: apply ? 'APPLY' : 'REVIEW_ONLY', clientId: CLIENT_ID, scoutRotationCommissioned: false, results };
  console.log(JSON.stringify(output, null, 2));
  return output;
}

if (require.main === module) {
  const applyArg = process.argv.includes('--apply');
  const confirmation = process.argv.find(arg => arg.startsWith('--confirm='))?.split('=')[1];
  const firm = process.argv.find(arg => arg.startsWith('--firm='))?.slice('--firm='.length) || null;
  if (applyArg && confirmation !== APPLY_CONFIRMATION) {
    console.error(`Refusing writes. Use --apply --confirm=${APPLY_CONFIRMATION}`);
    process.exit(1);
  }
  run({ apply: applyArg, firm }).then(() => process.exit(0)).catch(err => {
    console.error(err);
    process.exit(1);
  });
}

module.exports = { APPLY_CONFIRMATION, SERVICE_AREA, choosePlace, manifest, normalizeDomain, resolveVertical, run };
