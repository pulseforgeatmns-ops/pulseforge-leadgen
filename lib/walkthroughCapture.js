/**
 * Anchor Cleaning walkthrough capture.
 *
 * Primary write is agent_actions so inbound requests surface on the
 * operator dashboard without a new table. Prospect resolution is
 * best-effort (SPEC-257) so a schema mismatch never drops the request.
 */

const axios = require('axios');
const pool = require('../db');
const { setSetterVisibility } = require('../utils/setterVisibility');
const {
  PROSPECT_LINK_STATUS,
  resolveWalkthroughProspect,
  buildWalkthroughIdentityPayload,
  emitWalkthroughLeadCreatedEvent,
} = require('./walkthroughProspectIdentity');
const {
  buildContactSummary,
  ensureQualificationReviewForWalkthrough,
} = require('./leadQualificationReview');

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

function serviceAreaMatch(city) {
  const token = String(city || '').toLowerCase().replace(/[^a-z\s]/g, '').trim();
  const first = token.split(/\s+/)[0];
  return SERVICE_AREA.has(first) ? city : null;
}

async function mirrorAttributionToProspect(prospectId, attributionRecord) {
  if (!prospectId || !attributionRecord) return;
  try {
    const metaPatch = JSON.stringify({ attribution: attributionRecord });
    const params = [prospectId, metaPatch, ANCHOR_CLIENT_ID];
    let sql = `UPDATE prospects
                  SET acquisition_metadata = COALESCE(acquisition_metadata, '{}'::jsonb) || $2::jsonb,
                      updated_at = NOW()`;
    const leadSource = attributionRecord.normalized?.lead_source;
    const status = attributionRecord.normalized?.attribution_status;
    if (
      leadSource
      && leadSource !== 'unknown'
      && status === 'deterministic'
    ) {
      sql += `, acquisition_source = COALESCE(acquisition_source, $4)`;
      params.push(leadSource);
    }
    sql += ` WHERE id = $1 AND client_id = $3`;
    await pool.query(sql, params);
  } catch (err) {
    console.error('[walkthrough] acquisition_metadata write failed:', err.message);
  }
}

async function applyNewProspectWalkthroughFields(prospectId, values) {
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
      `Website facilities assessment request. ${values.space_type_label} in ${values.city}.`,
      ANCHOR_CLIENT_ID,
    ]
  );
  await setSetterVisibility(pool, prospectId, {
    reason: 'engagement',
    clientId: ANCHOR_CLIENT_ID,
    source: SOURCE,
  });
  try {
    const { safeWriteProspect } = require('../utils/knowledgeDualWrite');
    safeWriteProspect(
      {
        id: prospectId,
        client_id: ANCHOR_CLIENT_ID,
        first_name: values.name.split(/\s+/)[0],
        last_name: values.name.split(/\s+/).slice(1).join(' '),
        email: values.email,
        phone: values.phone,
        source: SOURCE,
      },
      { source: SOURCE }
    );
  } catch (err) {
    console.error('[walkthrough] knowledge dual-write failed:', err.message);
  }
}

function buildWalkthroughActionPayload(values, attributionRecord, resolution, linkedAt) {
  const payload = {
    source: SOURCE,
    prospect_id: resolution.prospectId,
    contact: {
      name: values.name,
      business_name: values.business_name,
      phone: values.phone,
      email: values.email,
      city: values.city,
      space_type: values.space_type,
      space_type_label: values.space_type_label,
    },
    identity: buildWalkthroughIdentityPayload(resolution, linkedAt),
  };
  if (attributionRecord) {
    payload.attribution = attributionRecord;
  }
  return payload;
}

async function captureWalkthroughLead(values, attributionRecord = null) {
  const linkedAt = new Date().toISOString();
  const title = `Facilities assessment request — ${values.business_name}`;
  const description = [
    values.name,
    values.business_name,
    values.phone,
    values.email,
    values.city,
    values.space_type_label,
  ].join(' · ');

  let resolution;
  try {
    resolution = await resolveWalkthroughProspect(pool, values, ANCHOR_CLIENT_ID, {
      serviceAreaMatch,
    });
  } catch (err) {
    console.error('[walkthrough] prospect resolution failed:', err.message);
    resolution = {
      prospectId: null,
      linkStatus: PROSPECT_LINK_STATUS.UNRESOLVED,
      isNew: false,
      unresolvedReason: 'PROSPECT_RESOLUTION_FAILED',
    };
  }

  const payload = buildWalkthroughActionPayload(values, attributionRecord, resolution, linkedAt);

  const inserted = await pool.query(
    `INSERT INTO agent_actions
       (created_by, action_type, title, description, payload, status, client_id)
     VALUES ($1, $2, $3, $4, $5::jsonb, 'pending', $6)
     RETURNING id`,
    [CREATED_BY, ACTION_TYPE, title, description, JSON.stringify(payload), ANCHOR_CLIENT_ID]
  );

  const actionId = inserted.rows[0]?.id || null;
  const prospectId = resolution.prospectId;

  if (prospectId && resolution.linkStatus === PROSPECT_LINK_STATUS.LINKED_NEW) {
    try {
      await applyNewProspectWalkthroughFields(prospectId, values);
    } catch (err) {
      console.error('[walkthrough] new prospect field update failed:', err.message);
    }
  }

  if (prospectId && attributionRecord) {
    try {
      await mirrorAttributionToProspect(prospectId, attributionRecord);
    } catch (err) {
      console.error('[walkthrough] attribution mirror failed:', err.message);
    }
  }

  if (prospectId && actionId) {
    await emitWalkthroughLeadCreatedEvent(pool, {
      clientId: ANCHOR_CLIENT_ID,
      prospectId,
      actionId,
      attributionRecord,
      linkStatus: resolution.linkStatus,
    });

    try {
      await ensureQualificationReviewForWalkthrough(pool, {
        clientId: ANCHOR_CLIENT_ID,
        prospectId,
        originatingActionId: actionId,
        attributionRecord,
        contactSummary: buildContactSummary(values),
      });
    } catch (err) {
      console.error('[walkthrough] qualification review ensure failed:', err.message);
    }
  }

  notifyWalkthrough(values, actionId).catch(err => {
    console.error('[walkthrough] notify failed:', err.message);
  });

  return {
    id: actionId,
    stored: Boolean(actionId),
    client_id: ANCHOR_CLIENT_ID,
    prospect_id: prospectId,
    prospect_link_status: resolution.linkStatus,
  };
}

async function notifyWalkthrough(values, actionId) {
  if (!process.env.BREVO_API_KEY) return false;
  const lines = [
    'New facilities assessment request from goanchorcleaning.com',
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
      subject: `Facilities assessment request — ${values.business_name}`,
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
  mirrorAttributionToProspect,
  buildWalkthroughActionPayload,
};
