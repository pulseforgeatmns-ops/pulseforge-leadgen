'use strict';

const pool = require('../db');
const { addCompany, addProspect } = require('../dbClient');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const { logAoAuditEvent } = require('../utils/aoAuditEvents');
const { normalizeAccountName } = require('../utils/aoAccountResolution');

function crmLinkageStatus(leadRow) {
  return leadRow?.crm_prospect_id ? 'linked' : 'unlinked';
}

function normalizeDomain(value) {
  const raw = String(value || '').trim().toLowerCase();
  if (!raw) return '';
  return raw.replace(/^https?:\/\//, '').replace(/^www\./, '').split('/')[0];
}

function parseAddressParts(address) {
  const text = String(address || '').trim();
  if (!text) {
    return { street: '', city: '', state: '', postal: '' };
  }
  const match = text.match(/^(.+?),\s*([^,]+),\s*([A-Z]{2})\s+(\d{5}(?:-\d{4})?)$/i);
  if (match) {
    return {
      street: match[1].trim(),
      city: match[2].trim(),
      state: match[3].trim().toUpperCase(),
      postal: match[4].trim(),
    };
  }
  return { street: text, city: '', state: '', postal: '' };
}

function formatAddress({ street, city, state, postal }) {
  const parts = [street, city, state].filter(Boolean);
  const base = parts.join(', ');
  if (postal) return base ? `${base} ${postal}` : postal;
  return base || null;
}

async function loadLeadForAo({ leadId, aoOwnerId, db = pool }) {
  const { rows } = await db.query(`
    SELECT l.*,
      c.id AS contact_id,
      c.contact_name,
      c.contact_title,
      c.phone AS contact_phone,
      c.email AS contact_email,
      c.is_decision_maker
    FROM ao_leads l
    LEFT JOIN LATERAL (
      SELECT * FROM ao_contacts
      WHERE lead_id = l.id
      ORDER BY is_decision_maker DESC, created_at ASC
      LIMIT 1
    ) c ON true
    WHERE l.id = $1 AND l.ao_owner_id = $2
    LIMIT 1
  `, [leadId, aoOwnerId]);
  return rows[0] || null;
}

function formatSearchRow(row) {
  const parts = parseAddressParts(row.company_location);
  return {
    prospect_id: row.prospect_id,
    company_id: row.company_id,
    company_name: row.company_name,
    address: row.company_location || null,
    city: parts.city || null,
    state: parts.state || null,
    phone: row.phone || null,
    domain: normalizeDomain(row.website) || null,
  };
}

async function searchCrmAccounts({
  clientId,
  q = null,
  businessName = null,
  address = null,
  city = null,
  domain = null,
  phone = null,
  limit = 25,
  db = pool,
}) {
  await ensureAoFieldSchema();
  const params = [clientId];
  const clauses = ['c.client_id = $1'];
  const trimmedQ = String(q || '').trim();
  const hasStructured = [businessName, address, city, domain, phone]
    .some(v => String(v || '').trim());

  if (trimmedQ && !hasStructured) {
    params.push(`%${trimmedQ}%`);
    const idx = params.length;
    clauses.push(`(
      c.name ILIKE $${idx}
      OR c.location ILIKE $${idx}
      OR c.website ILIKE $${idx}
      OR p.phone ILIKE $${idx}
    )`);
  } else {
    const pushLike = (columnExpr, value) => {
      const trimmed = String(value || '').trim();
      if (!trimmed) return;
      params.push(`%${trimmed}%`);
      clauses.push(`${columnExpr} ILIKE $${params.length}`);
    };
    pushLike('c.name', businessName);
    pushLike('c.location', address);
    pushLike('c.location', city);
    pushLike('c.website', domain);
    pushLike('p.phone', phone);
    if (clauses.length === 1) {
      return { accounts: [], query: { q, businessName, address, city, domain, phone } };
    }
  }

  params.push(Math.min(Math.max(Number(limit) || 25, 1), 50));
  const { rows } = await db.query(`
    SELECT DISTINCT ON (p.id)
      p.id AS prospect_id,
      c.id AS company_id,
      c.name AS company_name,
      c.location AS company_location,
      c.website,
      p.phone
    FROM companies c
    JOIN prospects p ON p.company_id = c.id AND p.client_id = c.client_id
    WHERE ${clauses.join(' AND ')}
    ORDER BY p.id, p.created_at DESC
    LIMIT $${params.length}
  `, params);

  return {
    accounts: rows.map(formatSearchRow),
    query: { q, businessName, address, city, domain, phone },
  };
}

async function findDuplicateCandidatesForLead(lead, clientId, db = pool) {
  const search = await searchCrmAccounts({
    clientId,
    businessName: lead.business_name,
    address: lead.address,
    phone: lead.contact_phone,
    limit: 10,
    db,
  });
  const seen = new Set();
  return search.accounts.filter((row) => {
    const key = String(row.prospect_id);
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  }).slice(0, 5);
}

async function getLeadLinkageContext({ leadId, aoOwnerId, db = pool }) {
  const lead = await loadLeadForAo({ leadId, aoOwnerId, db });
  if (!lead) return null;

  const duplicates = lead.crm_prospect_id
    ? []
    : await findDuplicateCandidatesForLead(lead, lead.client_id, db);

  const addressParts = parseAddressParts(lead.address);

  return {
    lead_id: lead.id,
    task_id: null,
    crm_linkage_status: crmLinkageStatus(lead),
    crm_prospect_id: lead.crm_prospect_id || null,
    disposition: {
      lead_status: lead.status,
      interest_level: lead.interest_level,
    },
    prospect: {
      business_name: lead.business_name,
      street_address: addressParts.street || null,
      city: addressParts.city || null,
      state: addressParts.state || null,
      postal: addressParts.postal || null,
      address: lead.address || null,
      phone: lead.contact_phone || null,
      website: null,
      business_type: lead.business_type || null,
      contact_name: lead.contact_name || null,
      contact_title: lead.contact_title || null,
      contact_email: lead.contact_email || null,
    },
    duplicate_candidates: duplicates,
  };
}

async function linkLeadToExistingAccount({
  clientId,
  aoOwnerId,
  leadId,
  prospectId,
  taskId = null,
  db = pool,
}) {
  await ensureAoFieldSchema();
  const lead = await loadLeadForAo({ leadId, aoOwnerId, db });
  if (!lead) return { error: 'Lead not found', status: 404 };

  const { rows: targetRows } = await db.query(`
    SELECT p.id, p.assigned_ao_id, c.name AS company_name
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.id = $1::uuid AND p.client_id = $2
    LIMIT 1
  `, [prospectId, clientId]);
  const target = targetRows[0];
  if (!target) return { error: 'CRM account not found', status: 404 };

  if (lead.crm_prospect_id && String(lead.crm_prospect_id) === String(prospectId)) {
    return {
      ok: true,
      crm_prospect_id: prospectId,
      crm_linkage_status: 'linked',
      company_name: target.company_name || null,
      disposition: { lead_status: lead.status },
    };
  }

  if (lead.crm_prospect_id && String(lead.crm_prospect_id) !== String(prospectId)) {
    const { rows: linkedRows } = await db.query(`
      SELECT p.id AS prospect_id, c.name AS company_name
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.id = $1::uuid AND p.client_id = $2
      LIMIT 1
    `, [lead.crm_prospect_id, clientId]);
    return {
      error: 'Lead is already linked to a different CRM account',
      status: 409,
      code: 'CRM_LINK_CONFLICT',
      crm_prospect_id: lead.crm_prospect_id,
      company_name: linkedRows[0]?.company_name || null,
    };
  }

  const client = await db.connect();
  try {
    await client.query('BEGIN');

    const { rows: linkRows } = await client.query(`
      UPDATE ao_leads
      SET crm_prospect_id = $1, updated_at = NOW()
      WHERE id = $2 AND ao_owner_id = $3 AND crm_prospect_id IS NULL
      RETURNING id, status, crm_prospect_id
    `, [prospectId, leadId, aoOwnerId]);

    if (!linkRows.length) {
      const current = await loadLeadForAo({ leadId, aoOwnerId, db: client });
      await client.query('ROLLBACK');
      if (current?.crm_prospect_id) {
        const { rows: currentAccount } = await db.query(`
          SELECT c.name AS company_name
          FROM prospects p
          LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
          WHERE p.id = $1::uuid AND p.client_id = $2
          LIMIT 1
        `, [current.crm_prospect_id, clientId]);
        return {
          error: 'CRM linkage changed while you were working',
          status: 409,
          code: 'CRM_LINK_CONFLICT',
          crm_prospect_id: current.crm_prospect_id,
          company_name: currentAccount[0]?.company_name || null,
        };
      }
      return { error: 'Lead not found', status: 404 };
    }

    await client.query(`
      UPDATE prospects
      SET assigned_ao_id = COALESCE(assigned_ao_id, $3), updated_at = NOW()
      WHERE id = $1::uuid AND client_id = $2
    `, [prospectId, clientId, aoOwnerId]);

    await client.query('COMMIT');

    await logAoAuditEvent({
      event: 'ao_prospect_crm_linked',
      clientId,
      aoUserId: aoOwnerId,
      prospectId,
      payload: {
        prospect_id: prospectId,
        queue_item_id: taskId,
        lead_id: leadId,
        crm_account_id: prospectId,
        actor_id: aoOwnerId,
        link_method: 'existing',
        occurred_at: new Date().toISOString(),
      },
    });

    return {
      ok: true,
      crm_prospect_id: prospectId,
      crm_linkage_status: 'linked',
      company_name: target.company_name,
      disposition: { lead_status: linkRows[0].status },
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function createCrmAccountForLead({
  clientId,
  aoOwnerId,
  leadId,
  taskId = null,
  forceCreate = false,
  db = pool,
}) {
  await ensureAoFieldSchema();
  const lead = await loadLeadForAo({ leadId, aoOwnerId, db });
  if (!lead) return { error: 'Lead not found', status: 404 };
  if (lead.crm_prospect_id) {
    return {
      error: 'Lead is already linked',
      status: 409,
      crm_prospect_id: lead.crm_prospect_id,
    };
  }

  const duplicates = await findDuplicateCandidatesForLead(lead, clientId, db);
  if (duplicates.length && !forceCreate) {
    return {
      error: 'Likely existing CRM matches found — link an existing account or confirm create',
      status: 409,
      code: 'DUPLICATE_CANDIDATES',
      duplicate_candidates: duplicates,
    };
  }

  const nameParts = String(lead.contact_name || 'Field Contact').trim().split(/\s+/);
  const firstName = nameParts[0] || 'Field';
  const lastName = nameParts.slice(1).join(' ') || 'Contact';
  const email = lead.contact_email
    || `ao+${String(lead.id).slice(0, 8)}@placeholder.local`;

  const companyId = await addCompany({
    name: lead.business_name,
    industry: lead.business_type || null,
    location: lead.address || null,
    client_id: clientId,
    icp_score: 0,
  });

  const prospectId = await addProspect({
    company_id: companyId,
    first_name: firstName,
    last_name: lastName,
    email,
    phone: lead.contact_phone || null,
    job_title: lead.contact_title || null,
    decision_maker: Boolean(lead.is_decision_maker),
    source: `ao_field:${lead.attribution_source || 'ao_field_visit'}`,
    icp_score: 0,
    client_id: clientId,
  });

  if (!prospectId) {
    return { error: 'Prospect creation failed — contact email may already exist', status: 409 };
  }

  await db.query(`
    UPDATE prospects
    SET assigned_ao_id = COALESCE(assigned_ao_id, $3),
        vertical = COALESCE(vertical, $4),
        updated_at = NOW()
    WHERE id = $1::uuid AND client_id = $2
  `, [prospectId, clientId, aoOwnerId, lead.business_type || null]);

  const linkResult = await linkLeadToExistingAccount({
    clientId,
    aoOwnerId,
    leadId,
    prospectId,
    taskId,
    db,
  });
  if (linkResult.error) return linkResult;

  await logAoAuditEvent({
    event: 'ao_prospect_crm_account_created',
    clientId,
    aoUserId: aoOwnerId,
    prospectId,
    payload: {
      prospect_id: prospectId,
      queue_item_id: taskId,
      lead_id: leadId,
      crm_account_id: prospectId,
      company_id: companyId,
      actor_id: aoOwnerId,
      link_method: 'created',
      occurred_at: new Date().toISOString(),
    },
  });

  return {
    ok: true,
    crm_prospect_id: prospectId,
    company_id: companyId,
    crm_linkage_status: 'linked',
    disposition: linkResult.disposition,
  };
}

const EDITABLE_LEAD_FIELDS = new Set([
  'business_name',
  'address',
  'street_address',
  'city',
  'state',
  'postal',
  'business_type',
  'phone',
  'website',
  'contact_name',
  'contact_title',
  'contact_email',
]);

async function editQueueProspect({
  clientId,
  aoOwnerId,
  leadId,
  patch = {},
  db = pool,
}) {
  await ensureAoFieldSchema();
  const lead = await loadLeadForAo({ leadId, aoOwnerId, db });
  if (!lead) return { error: 'Lead not found', status: 404 };

  const changedFields = {};
  const leadUpdates = [];
  const leadParams = [];

  if (patch.business_name != null && String(patch.business_name).trim()) {
    const value = String(patch.business_name).trim();
    if (value !== lead.business_name) {
      changedFields.business_name = { from: lead.business_name, to: value };
      leadParams.push(value);
      leadUpdates.push(`business_name = $${leadParams.length}`);
    }
  }

  if (patch.business_type !== undefined) {
    const value = patch.business_type == null ? null : String(patch.business_type).trim() || null;
    if (value !== (lead.business_type || null)) {
      changedFields.business_type = { from: lead.business_type || null, to: value };
      leadParams.push(value);
      leadUpdates.push(`business_type = $${leadParams.length}`);
    }
  }

  const hasAddressParts = ['street_address', 'city', 'state', 'postal', 'address']
    .some(key => patch[key] !== undefined);
  if (hasAddressParts) {
    const currentParts = parseAddressParts(lead.address);
    const nextAddress = formatAddress({
      street: patch.street_address != null ? String(patch.street_address).trim() : currentParts.street,
      city: patch.city != null ? String(patch.city).trim() : currentParts.city,
      state: patch.state != null ? String(patch.state).trim().toUpperCase() : currentParts.state,
      postal: patch.postal != null ? String(patch.postal).trim() : currentParts.postal,
    }) || (patch.address != null ? String(patch.address).trim() : lead.address);
    if (nextAddress !== (lead.address || null)) {
      changedFields.address = { from: lead.address || null, to: nextAddress };
      leadParams.push(nextAddress);
      leadUpdates.push(`address = $${leadParams.length}`);
    }
  }

  const contactPatch = {};
  if (patch.contact_name !== undefined) {
    const value = patch.contact_name == null ? null : String(patch.contact_name).trim() || null;
    if (value !== (lead.contact_name || null)) {
      changedFields.contact_name = { from: lead.contact_name || null, to: value };
      contactPatch.contact_name = value;
    }
  }
  if (patch.contact_title !== undefined) {
    const value = patch.contact_title == null ? null : String(patch.contact_title).trim() || null;
    if (value !== (lead.contact_title || null)) {
      changedFields.contact_title = { from: lead.contact_title || null, to: value };
      contactPatch.contact_title = value;
    }
  }
  if (patch.contact_email !== undefined) {
    const value = patch.contact_email == null ? null : String(patch.contact_email).trim() || null;
    if (value !== (lead.contact_email || null)) {
      changedFields.contact_email = { from: lead.contact_email || null, to: value };
      contactPatch.contact_email = value;
    }
  }
  if (patch.phone !== undefined) {
    const value = patch.phone == null ? null : String(patch.phone).trim() || null;
    if (value !== (lead.contact_phone || null)) {
      changedFields.phone = { from: lead.contact_phone || null, to: value };
      contactPatch.phone = value;
    }
  }

  if (!leadUpdates.length && !Object.keys(contactPatch).length) {
    return {
      ok: true,
      changed_fields: {},
      crm_linkage_status: crmLinkageStatus(lead),
      crm_prospect_id: lead.crm_prospect_id || null,
      duplicate_candidates: lead.crm_prospect_id
        ? []
        : await findDuplicateCandidatesForLead(lead, clientId, db),
    };
  }

  const client = await db.connect();
  try {
    await client.query('BEGIN');
    if (leadUpdates.length) {
      leadParams.push(leadId, aoOwnerId);
      await client.query(`
        UPDATE ao_leads
        SET ${leadUpdates.join(', ')}, updated_at = NOW()
        WHERE id = $${leadParams.length - 1} AND ao_owner_id = $${leadParams.length}
      `, leadParams);
    }

    if (Object.keys(contactPatch).length && lead.contact_id) {
      const sets = [];
      const params = [];
      for (const [column, value] of Object.entries(contactPatch)) {
        params.push(value);
        sets.push(`${column} = $${params.length}`);
      }
      params.push(lead.contact_id);
      await client.query(`
        UPDATE ao_contacts
        SET ${sets.join(', ')}, updated_at = NOW()
        WHERE id = $${params.length}
      `, params);
    } else if (Object.keys(contactPatch).length) {
      await client.query(`
        INSERT INTO ao_contacts (lead_id, contact_name, contact_title, phone, email)
        VALUES ($1, $2, $3, $4, $5)
      `, [
        leadId,
        contactPatch.contact_name || 'Contact',
        contactPatch.contact_title || null,
        contactPatch.phone || null,
        contactPatch.contact_email || null,
      ]);
    }

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }

  const sanitizedChanged = Object.fromEntries(
    Object.entries(changedFields).map(([key, val]) => [key, { changed: true }])
  );

  await logAoAuditEvent({
    event: 'ao_prospect_edited',
    clientId,
    aoUserId: aoOwnerId,
    prospectId: lead.crm_prospect_id || null,
    payload: {
      prospect_id: lead.crm_prospect_id || null,
      lead_id: leadId,
      actor_id: aoOwnerId,
      changed_fields: Object.keys(sanitizedChanged),
      occurred_at: new Date().toISOString(),
    },
  });

  const refreshed = await loadLeadForAo({ leadId, aoOwnerId, db });
  let autoLinked = null;
  if (!refreshed.crm_prospect_id) {
    const matches = await findDuplicateCandidatesForLead(refreshed, clientId, db);
    if (matches.length === 1 && normalizeAccountName(matches[0].company_name)
      === normalizeAccountName(refreshed.business_name)) {
      autoLinked = await linkLeadToExistingAccount({
        clientId,
        aoOwnerId,
        leadId,
        prospectId: matches[0].prospect_id,
        db,
      });
    }
  }

  const finalLead = autoLinked?.ok
    ? await loadLeadForAo({ leadId, aoOwnerId, db })
    : refreshed;

  return {
    ok: true,
    changed_fields: sanitizedChanged,
    crm_linkage_status: crmLinkageStatus(finalLead),
    crm_prospect_id: finalLead.crm_prospect_id || null,
    auto_linked: Boolean(autoLinked?.ok),
    duplicate_candidates: finalLead.crm_prospect_id
      ? []
      : await findDuplicateCandidatesForLead(finalLead, clientId, db),
  };
}

module.exports = {
  EDITABLE_LEAD_FIELDS,
  crmLinkageStatus,
  parseAddressParts,
  formatAddress,
  searchCrmAccounts,
  getLeadLinkageContext,
  linkLeadToExistingAccount,
  createCrmAccountForLead,
  editQueueProspect,
  findDuplicateCandidatesForLead,
};
