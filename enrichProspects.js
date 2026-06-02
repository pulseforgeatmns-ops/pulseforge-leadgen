require('dotenv').config();

const axios = require('axios');
const pool = require('./db');
const { normalizeClientId } = require('./utils/clientContext');
const { invalidOutreachEmailReason } = require('./utils/emailGuard');

const AGENT_NAME = 'enrich_prospects';

const GENERIC_EMAIL_PREFIX_RE = /^(?:info|hello|contact|admin|support|sales|office|team|service|customerservice|customer\.?service|no-?reply|noreply|mail|inquir(?:y|ies))[\w.+-]*$/i;
const EMAIL_PLACEHOLDER_DOMAINS = ['godaddy.com', 'example.com', 'test.com'];

function delay(ms) {
  return new Promise(r => setTimeout(r, ms));
}

function normalizeDomain(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  try {
    return new URL(raw.includes('://') ? raw : `https://${raw}`)
      .hostname
      .replace(/^www\./i, '')
      .toLowerCase();
  } catch {
    const domain = raw
      .replace(/^https?:\/\//i, '')
      .replace(/^www\./i, '')
      .split(/[/?#\s]/)[0]
      .replace(/[.,;:]+$/g, '')
      .toLowerCase();
    return domain || null;
  }
}

function emailRejection(email) {
  const guardReason = invalidOutreachEmailReason(email);
  if (guardReason) return guardReason;
  if (typeof email !== 'string' || !email.trim()) return 'empty';
  const e = email.trim();
  if (/\s/.test(e)) return 'contains spaces';
  if (e.length < 6) return 'too short';
  if ((e.match(/@/g) || []).length !== 1) return 'invalid @ count';
  if (/\.(webp|png|jpg|gif|svg|pdf)$/i.test(e)) return 'file extension domain';
  const [local, domain] = e.split('@');
  if (!local || !domain) return 'invalid format';
  if (GENERIC_EMAIL_PREFIX_RE.test(local)) return 'generic inbox';
  if (EMAIL_PLACEHOLDER_DOMAINS.includes(domain.toLowerCase())) return 'placeholder domain';
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(e)) return 'invalid format';
  return null;
}

function pickBestHunterEmail(emails = []) {
  const candidates = (Array.isArray(emails) ? emails : [])
    .map(e => ({
      value: e?.value || null,
      type: e?.type || null,
      confidence: Number(e?.confidence || 0),
      first_name: e?.first_name || null,
      last_name: e?.last_name || null,
      position: e?.position || null,
      phone_number: e?.phone_number || null,
    }))
    .filter(e => e.value);

  const valid = candidates
    .map(e => ({ ...e, rejection: emailRejection(e.value) }))
    .filter(e => !e.rejection);

  if (!valid.length) return null;

  // Prefer personal > generic; then highest confidence.
  valid.sort((a, b) => {
    const ap = a.type === 'personal' ? 1 : 0;
    const bp = b.type === 'personal' ? 1 : 0;
    if (bp !== ap) return bp - ap;
    return (b.confidence || 0) - (a.confidence || 0);
  });

  return valid[0];
}

function pickBestHunterPhone(emails = []) {
  const phones = (Array.isArray(emails) ? emails : [])
    .map(e => String(e?.phone_number || '').trim())
    .filter(Boolean);
  return phones[0] || null;
}

async function logEnrichmentAttempt(clientId, payload, status = 'success') {
  try {
    await pool.query(`
      INSERT INTO agent_log (agent_name, action, payload, status, ran_at, client_id)
      VALUES ($1, $2, $3, $4, NOW(), $5)
    `, [AGENT_NAME, 'enrichment_attempt', JSON.stringify(payload), status, clientId]);
  } catch (err) {
    console.error('[enrich] agent_log write failed:', err.message);
  }
}

async function hunterDomainSearch(domain) {
  const apiKey = process.env.HUNTER_API_KEY;
  if (!apiKey) throw new Error('HUNTER_API_KEY is not set');
  const res = await axios.get('https://api.hunter.io/v2/domain-search', {
    params: {
      domain,
      api_key: apiKey,
      limit: 10,
      type: 'personal',
    },
    timeout: 15000,
  });
  return res.data?.data || null;
}

async function enrichProspectRow(row, clientId) {
  const prospectId = row.prospect_id;
  const companyName = row.company_name || 'Unknown';
  const domain = normalizeDomain(row.domain || row.website);

  const attemptPayload = {
    prospect_id: prospectId,
    company: companyName,
    domain: domain,
    found: { phone: null, email: null },
    updated: { phone: false, email: false },
  };

  if (!domain) {
    await logEnrichmentAttempt(clientId, { ...attemptPayload, skipped: 'no_domain' }, 'skipped');
    return { skipped: true };
  }

  let data;
  try {
    data = await hunterDomainSearch(domain);
  } catch (err) {
    await logEnrichmentAttempt(clientId, { ...attemptPayload, error: err.message }, 'failed');
    return { failed: true };
  }

  const emails = data?.emails || [];
  const bestEmail = pickBestHunterEmail(emails);
  const bestPhone = pickBestHunterPhone(emails);

  attemptPayload.found.email = bestEmail?.value || null;
  attemptPayload.found.phone = bestPhone || null;

  const updates = {};
  if (bestPhone) updates.phone = bestPhone;
  if (bestEmail?.value) updates.email = bestEmail.value;

  if (!Object.keys(updates).length) {
    await logEnrichmentAttempt(clientId, { ...attemptPayload, result: 'nothing_found' }, 'success');
    return { updated: false };
  }

  try {
    const sets = [];
    const params = [];
    let idx = 1;
    if (updates.phone) {
      sets.push(`phone = $${idx++}`);
      params.push(updates.phone);
      attemptPayload.updated.phone = true;
    }
    if (updates.email) {
      sets.push(`email = $${idx++}`);
      params.push(updates.email);
      attemptPayload.updated.email = true;
    }
    params.push(prospectId, clientId);
    await pool.query(
      `UPDATE prospects
       SET ${sets.join(', ')},
           updated_at = NOW()
       WHERE id = $${idx++}
         AND client_id = $${idx}
         AND phone IS NULL
         AND email IS NULL`,
      params
    );

    await logEnrichmentAttempt(clientId, attemptPayload, 'success');
    return { updated: true, ...updates };
  } catch (err) {
    await logEnrichmentAttempt(clientId, { ...attemptPayload, error: err.message }, 'failed');
    return { failed: true };
  }
}

async function run(params = {}) {
  const clientId = normalizeClientId(params.client_id || params.clientId || 1);
  const lookbackDays = Math.max(1, Number(params.lookbackDays || 7));

  if (clientId !== 1) {
    console.log(`[enrich] Skipping client ${clientId}: enrichment job is scoped to client_id=1`);
    return { skipped: true, client_id: clientId };
  }

  console.log(`[enrich] Starting phone/email enrichment for client ${clientId} (lookback ${lookbackDays}d)`);

  const res = await pool.query(`
    SELECT
      p.id AS prospect_id,
      COALESCE(c.name, 'Unknown') AS company_name,
      c.domain AS domain,
      c.website AS website,
      p.created_at
    FROM prospects p
    LEFT JOIN companies c
      ON c.id = p.company_id
      AND c.client_id = p.client_id
    WHERE p.client_id = 1
      AND p.email IS NULL
      AND p.phone IS NULL
      AND COALESCE(p.do_not_contact, false) = false
      AND p.created_at >= NOW() - ($1::text || ' days')::interval
    ORDER BY p.created_at DESC
  `, [String(lookbackDays)]);

  console.log(`[enrich] Found ${res.rows.length} prospect(s) needing enrichment`);

  let attempted = 0;
  let updated = 0;
  let failed = 0;
  let skipped = 0;

  for (const row of res.rows) {
    attempted++;
    const outcome = await enrichProspectRow(row, clientId);
    if (outcome?.failed) failed++;
    else if (outcome?.skipped) skipped++;
    else if (outcome?.updated) updated++;
    await delay(500);
  }

  console.log(`[enrich] Done. attempted=${attempted} updated=${updated} skipped=${skipped} failed=${failed}`);
  return { client_id: clientId, attempted, updated, skipped, failed };
}

module.exports = { run };

if (require.main === module) {
  run({ client_id: 1 }).catch(err => {
    console.error('[enrich] Fatal error:', err.message);
    process.exit(1);
  });
}
