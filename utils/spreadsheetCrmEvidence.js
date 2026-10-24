'use strict';

function isoTimestamp(value) {
  if (!value) return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date.toISOString();
}

function sourceActivityDate(activity = {}) {
  const value = activity.occurredOn || activity.metadata?.occurredOn;
  if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const timestamp = isoTimestamp(`${value}T00:00:00.000Z`);
  return timestamp?.slice(0, 10) === value ? value : null;
}

function activityReadModel(activity = {}) {
  const occurredOn = sourceActivityDate(activity);
  const recordedAt = isoTimestamp(activity.created_at);
  return { ...activity, occurredOn, occurredAt: occurredOn ? `${occurredOn}T00:00:00.000Z` : recordedAt, recordedAt };
}

async function fetchSpreadsheetCrmEvidence({ db, clientId, prospectId, aoId = null, includeActivities = false }) {
  const params = [clientId, prospectId, aoId];
  // Read models remain usable before this optional import migration is deployed.
  // Missing tables are distinct from query failures, which must still surface.
  const available = (await db.query(`SELECT to_regclass('max_spreadsheet_contacts') AS contacts,
    to_regclass('max_spreadsheet_relationships') AS relationships,
    to_regclass('ao_prospect_activity') AS activities`)).rows[0] || {};
  const [contacts, relationships, activities] = await Promise.all([
    available.contacts ? db.query(`SELECT r.id, r.data FROM max_spreadsheet_contacts r
      JOIN prospects p ON p.id=r.prospect_id AND p.client_id=r.client_id
      WHERE r.client_id=$1 AND r.prospect_id=$2 AND ($3::integer IS NULL OR p.assigned_ao_id=$3)
      ORDER BY r.id`, params) : { rows: [] },
    available.relationships ? db.query(`SELECT r.id, r.provider_id, r.data, c.name AS provider_name FROM max_spreadsheet_relationships r
      JOIN prospects p ON p.id=r.prospect_id AND p.client_id=r.client_id
      JOIN prospects provider ON provider.id=r.provider_id AND provider.client_id=r.client_id
        AND provider.assigned_ao_id=p.assigned_ao_id
      LEFT JOIN companies c ON c.id=provider.company_id AND c.client_id=provider.client_id
      WHERE r.client_id=$1 AND r.prospect_id=$2 AND ($3::integer IS NULL OR p.assigned_ao_id=$3)
      ORDER BY r.id`, params) : { rows: [] },
    includeActivities && available.activities ? db.query(`SELECT a.* FROM ao_prospect_activity a
      JOIN prospects p ON p.id=a.prospect_id AND p.client_id=a.tenant_id
      WHERE a.tenant_id=$1 AND a.prospect_id=$2 AND ($3::integer IS NULL OR p.assigned_ao_id=$3)
      ORDER BY a.created_at DESC`, params) : { rows: [] },
  ]);
  return {
    contacts: contacts.rows.map(row => ({ ...row.data, id: row.id, source: 'approved_spreadsheet' })),
    relationships: relationships.rows.map(row => ({ ...row.data, id: row.id, providerId: row.provider_id, providerName: row.provider_name, source: 'approved_spreadsheet' })),
    activities: activities.rows.map(activityReadModel).sort((a, b) => String(b.occurredAt || '').localeCompare(String(a.occurredAt || ''))),
  };
}

module.exports = { sourceActivityDate, activityReadModel, fetchSpreadsheetCrmEvidence };
