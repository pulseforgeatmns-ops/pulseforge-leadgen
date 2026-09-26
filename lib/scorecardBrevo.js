/**
 * Opt-in-only Brevo handoff for Revenue Leak Scorecard leads.
 *
 * This deliberately creates/updates a contact only. Follow-up delivery stays
 * in Brevo automation, where it can be reviewed and stopped without changing
 * the public scorecard.
 */

const axios = require('axios');

const BREVO_CONTACTS_URL = 'https://api.brevo.com/v3/contacts';

function contactAttributes(answers, result) {
  return {
    FIRSTNAME: answers.name,
    BUSINESS_NAME: answers.business_name,
    PHONE: answers.mobile,
    SCORECARD_RESULT: result.category,
    SCORECARD_INTENT: result.high_intent ? 'high' : 'standard',
    SCORECARD_SOURCE: 'revenue_leak_scorecard',
  };
}

function configuredListIds() {
  return String(process.env.SCORECARD_BREVO_LIST_ID || '')
    .split(',')
    .map((value) => Number(value.trim()))
    .filter((value) => Number.isInteger(value) && value > 0);
}

/**
 * Create or update an opted-in Scorecard lead in Brevo.
 * No API key or consent is a normal, successful no-op.
 */
async function syncScorecardContact(answers, result, { http = axios } = {}) {
  if (!answers.marketing_consent) return { synced: false, reason: 'no_marketing_consent' };
  if (process.env.SCORECARD_BREVO_SYNC_ENABLED !== 'true') {
    return { synced: false, reason: 'brevo_sync_disabled' };
  }
  if (!process.env.BREVO_API_KEY) return { synced: false, reason: 'brevo_not_configured' };

  const headers = {
    'api-key': process.env.BREVO_API_KEY,
    'Content-Type': 'application/json',
  };
  const attributes = contactAttributes(answers, result);
  const email = String(answers.email).trim().toLowerCase();
  const listIds = configuredListIds();

  try {
    await http.put(`${BREVO_CONTACTS_URL}/${encodeURIComponent(email)}`, { attributes }, { headers });
    return { synced: true, action: 'updated' };
  } catch (err) {
    if (err.response?.status !== 404) throw err;
  }

  await http.post(BREVO_CONTACTS_URL, {
    email,
    attributes,
    ...(listIds.length ? { listIds } : {}),
    updateEnabled: true,
  }, { headers });
  return { synced: true, action: 'created' };
}

module.exports = { syncScorecardContact, contactAttributes, configuredListIds };
