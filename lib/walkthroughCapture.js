/**
 * Anchor Cleaning walkthrough capture.
 *
 * Primary write is agent_actions so inbound requests surface on the
 * operator dashboard without a new table. Prospect insert is best-effort
 * so a schema mismatch never drops the request.
 */

const axios = require('axios');
const pool = require('../db');
const { addProspect } = require('../dbClient');
const { setSetterVisibility } = require('../utils/setterVisibility');

const ANCHOR_CLIENT_ID = 10;
const CREATED_BY = 'website';
const ACTION_TYPE = 'walkthrough_request';
const SOURCE = 'website_walkthrough';
const NOTIFY_TO = process.env.ANCHOR_WALKTHROUGH_NOTIFY_EMAIL || 'jacob@goanchorcleaning.com';

const SPACE_VERTICAL = Object.freeze({
  law_office: 'law_firm',
  accounting: 'accounting',
  medical_office: 'medical_office',
  general_office: 'commercial_office',
  retail: 'retail',
  other: 'commercial_office',
});

const SERVICE_AREA = new Set([
  'manchester',
  'bedford',
  'goffstown',
  'hooksett',
  'londonderry',
  'auburn',
]);

function splitName(name) {
  const parts = String(name || '').trim().split(/\s+/);
  if (parts.length < 2) return { first_name: parts[0] || name, last_name: '' };
  return { first_name: parts[0], last_name: parts.slice(1).join(' ') };
}

function serviceAreaMatch(city) {
  const token = String(city || '').toLowerCase().replace(/[^a-z\s]/g, '').trim();
  const first = token.split(/\s+/)[0];
  return SERVICE_AREA.has(first) ? city : null;
}

async function captureWalkthroughLead(values) {
  const title = `Walkthrough request — ${values.business_name}`;
  const description = [
    values.name,
    values.business_name,
    values.phone,
    values.email,
    values.city,
    values.space_type_label,
  ].join(' · ');

  const payload = {
    source: SOURCE,
    contact: {
      name: values.name,
      business_name: values.business_name,
      phone: values.phone,
      email: values.email,
      city: values.city,
      space_type: values.space_type,
      space_type_label: values.space_type_label,
    },
  };

  const inserted = await pool.query(
    `INSERT INTO agent_actions
       (created_by, action_type, title, description, payload, status, client_id)
     VALUES ($1, $2, $3, $4, $5::jsonb, 'pending', $6)
     RETURNING id`,
    [CREATED_BY, ACTION_TYPE, title, description, JSON.stringify(payload), ANCHOR_CLIENT_ID]
  );

  const actionId = inserted.rows[0]?.id || null;
  let prospectId = null;

  try {
    const { first_name, last_name } = splitName(values.name);
    prospectId = await addProspect({
      first_name,
      last_name,
      email: values.email,
      phone: values.phone,
      source: SOURCE,
      icp_score: 80,
      client_id: ANCHOR_CLIENT_ID,
      service_area_match: serviceAreaMatch(values.city),
    });
    if (prospectId) {
      await pool.query(
        `UPDATE prospects
            SET vertical = $2,
                notes = $3,
                status = 'warm',
                updated_at = NOW()
          WHERE id = $1 AND client_id = $4`,
        [
          prospectId,
          SPACE_VERTICAL[values.space_type] || 'commercial_office',
          `Website walkthrough request. ${values.space_type_label} in ${values.city}.`,
          ANCHOR_CLIENT_ID,
        ]
      );
      await setSetterVisibility(pool, prospectId, {
        reason: 'engagement',
        clientId: ANCHOR_CLIENT_ID,
        source: SOURCE,
      });
    }
  } catch (err) {
    console.error('[walkthrough] prospect write failed:', err.message);
  }

  notifyWalkthrough(values, actionId).catch(err => {
    console.error('[walkthrough] notify failed:', err.message);
  });

  return {
    id: actionId,
    stored: Boolean(actionId),
    client_id: ANCHOR_CLIENT_ID,
    prospect_id: prospectId,
  };
}

async function notifyWalkthrough(values, actionId) {
  if (!process.env.BREVO_API_KEY) return false;
  const lines = [
    'New walkthrough request from goanchorcleaning.com',
    '',
    `Name: ${values.name}`,
    `Business: ${values.business_name}`,
    `Phone: ${values.phone}`,
    `Email: ${values.email}`,
    `City / town: ${values.city}`,
    `Type of space: ${values.space_type_label}`,
    actionId ? `Action id: ${actionId}` : '',
  ].filter(Boolean);

  await axios.post(
    'https://api.brevo.com/v3/smtp/email',
    {
      sender: { name: 'Anchor Cleaning Site', email: NOTIFY_TO },
      to: [{ email: NOTIFY_TO }],
      subject: `Walkthrough request — ${values.business_name}`,
      textContent: lines.join('\n'),
    },
    {
      headers: {
        'api-key': process.env.BREVO_API_KEY,
        'Content-Type': 'application/json',
      },
    }
  );
  return true;
}

module.exports = {
  ANCHOR_CLIENT_ID,
  ACTION_TYPE,
  SOURCE,
  captureWalkthroughLead,
};
