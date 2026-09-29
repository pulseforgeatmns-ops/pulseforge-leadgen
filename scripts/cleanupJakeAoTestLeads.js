'use strict';

/**
 * Remove Jake's five obvious AO test leads on client_id=10 (dry-run default).
 *
 * Dry-run:
 *   node scripts/cleanupJakeAoTestLeads.js
 *
 * Apply:
 *   node scripts/cleanupJakeAoTestLeads.js \
 *     --confirm-production \
 *     --confirm=client_10-jake-test-cleanup-2026-09-16
 */

require('dotenv').config();

const pool = require('../db');
const { ensureAoFieldSchema } = require('../utils/aoFieldSchema');
const aoField = require('../services/aoFieldService');
const {
  CLIENT_ID,
} = require('./data/jakeAoProspectBook');
const {
  isAllowlistedJakeAoTestLead,
  JAKE_AO_TEST_LEAD_NAMES,
} = require('./data/jakeAoTestLeadAllowlist');

const APPLY_CONFIRMATION = 'client_10-jake-test-cleanup-2026-09-16';

function normalizeBusinessKey(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[^a-z0-9]/g, '');
}

async function inspectLeadDependents(leadIds) {
  if (!leadIds.length) {
    return {
      contacts: 0,
      follow_up_tasks: 0,
      escalations: 0,
      route_stops: 0,
      by_lead: [],
    };
  }

  const [
    contacts,
    tasks,
    escalations,
    routeStops,
    byLeadRows,
  ] = await Promise.all([
    pool.query(
      'SELECT COUNT(*)::int AS count FROM ao_contacts WHERE lead_id = ANY($1::uuid[])',
      [leadIds],
    ),
    pool.query(
      'SELECT COUNT(*)::int AS count FROM ao_follow_up_tasks WHERE lead_id = ANY($1::uuid[])',
      [leadIds],
    ),
    pool.query(
      'SELECT COUNT(*)::int AS count FROM ao_escalations WHERE lead_id = ANY($1::uuid[])',
      [leadIds],
    ),
    pool.query(
      'SELECT COUNT(*)::int AS count FROM ao_route_stops WHERE lead_id = ANY($1::uuid[])',
      [leadIds],
    ),
    pool.query(`
      SELECT
        l.id,
        l.business_name,
        (SELECT COUNT(*)::int FROM ao_contacts c WHERE c.lead_id = l.id) AS contacts,
        (SELECT COUNT(*)::int FROM ao_follow_up_tasks t WHERE t.lead_id = l.id) AS follow_up_tasks,
        (SELECT COUNT(*)::int FROM ao_escalations e WHERE e.lead_id = l.id) AS escalations,
        (SELECT COUNT(*)::int FROM ao_route_stops rs WHERE rs.lead_id = l.id) AS route_stops,
        l.crm_prospect_id
      FROM ao_leads l
      WHERE l.id = ANY($1::uuid[])
      ORDER BY l.business_name ASC
    `, [leadIds]),
  ]);

  return {
    contacts: contacts.rows[0].count,
    follow_up_tasks: tasks.rows[0].count,
    escalations: escalations.rows[0].count,
    route_stops: routeStops.rows[0].count,
    by_lead: byLeadRows.rows.map(row => ({
      id: row.id,
      business_name: row.business_name,
      contacts: row.contacts,
      follow_up_tasks: row.follow_up_tasks,
      escalations: row.escalations,
      route_stops: row.route_stops,
      crm_prospect_id: row.crm_prospect_id,
      crm_action: row.crm_prospect_id
        ? 'preserved — CRM not modified (AO row only)'
        : 'none',
    })),
  };
}

async function findJakeLeads(clientId, jakeId) {
  const { rows } = await pool.query(`
    SELECT id, business_name, ao_owner_id, client_id, crm_prospect_id, created_at
    FROM ao_leads
    WHERE client_id = $1 AND ao_owner_id = $2
    ORDER BY business_name ASC
  `, [clientId, jakeId]);
  return rows;
}

function partitionJakeLeads(allJakeLeads) {
  const targets = [];
  const preserved = [];

  for (const lead of allJakeLeads) {
    if (isAllowlistedJakeAoTestLead(lead.business_name)) {
      targets.push(lead);
    } else {
      preserved.push(lead);
    }
  }

  return { targets, preserved };
}

async function deleteTargetLeads(leadIds) {
  if (!leadIds.length) {
    return {
      route_stops_deleted: 0,
      leads_deleted: 0,
    };
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const routeStopResult = await client.query(
      'DELETE FROM ao_route_stops WHERE lead_id = ANY($1::uuid[])',
      [leadIds],
    );

    const leadResult = await client.query(
      'DELETE FROM ao_leads WHERE id = ANY($1::uuid[])',
      [leadIds],
    );

    await client.query('COMMIT');
    return {
      route_stops_deleted: routeStopResult.rowCount,
      leads_deleted: leadResult.rowCount,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function run({ apply = false } = {}) {
  await ensureAoFieldSchema();

  const jake = await aoField.resolveJakeAoOwner(CLIENT_ID);
  if (!jake) {
    throw new Error(
      'Jake AO identity not found — verify jzmaynard7@gmail.com (or JAKE_EMAIL) on client_id=10',
    );
  }

  const allJakeLeads = await findJakeLeads(CLIENT_ID, jake.id);
  const { targets, preserved } = partitionJakeLeads(allJakeLeads);
  const targetIds = targets.map(l => l.id);
  const dependentsBefore = await inspectLeadDependents(targetIds);

  const beforeMutation = {
    jake_identity: {
      id: jake.id,
      name: jake.name,
      email: jake.email,
      role: jake.role,
      client_id: jake.client_id,
    },
    allowlist: JAKE_AO_TEST_LEAD_NAMES,
    target_leads: targets.map(l => ({
      id: l.id,
      business_name: l.business_name,
      normalized_key: normalizeBusinessKey(l.business_name),
    })),
    dependents: dependentsBefore,
    preserved_jake_leads: preserved.map(l => ({
      id: l.id,
      business_name: l.business_name,
    })),
  };

  let mutation = null;
  if (apply) {
    const deleted = await deleteTargetLeads(targetIds);
    const remainingTargets = (await findJakeLeads(CLIENT_ID, jake.id))
      .filter(l => isAllowlistedJakeAoTestLead(l.business_name));

    mutation = {
      deleted_leads: targets.map(l => ({
        id: l.id,
        business_name: l.business_name,
      })),
      deleted_child_rows: {
        route_stops: deleted.route_stops_deleted,
        contacts: dependentsBefore.contacts,
        follow_up_tasks: dependentsBefore.follow_up_tasks,
        escalations: dependentsBefore.escalations,
        note: 'contacts/tasks/escalations removed via ao_leads ON DELETE CASCADE',
      },
      preserved_jake_leads: (await findJakeLeads(CLIENT_ID, jake.id))
        .filter(l => !isAllowlistedJakeAoTestLead(l.business_name))
        .map(l => ({ id: l.id, business_name: l.business_name })),
      failures: remainingTargets.length
        ? remainingTargets.map(l => ({
          id: l.id,
          business_name: l.business_name,
          error: 'still_present_after_delete',
        }))
        : [],
    };
  }

  const afterJakeLeads = apply
    ? await findJakeLeads(CLIENT_ID, jake.id)
    : allJakeLeads.filter(l => !isAllowlistedJakeAoTestLead(l.business_name));

  return {
    mode: apply ? 'APPLY' : 'DRY_RUN',
    client_id: CLIENT_ID,
    before_mutation: beforeMutation,
    would_delete: apply ? undefined : {
      lead_count: targets.length,
      leads: targets.map(l => ({ id: l.id, business_name: l.business_name })),
      dependents: dependentsBefore,
    },
    mutation,
    jake_lead_count_after: afterJakeLeads.length,
    ok: apply
      ? (mutation?.failures?.length || 0) === 0
      : true,
  };
}

if (require.main === module) {
  const apply = process.argv.includes('--confirm-production');
  const confirmation = process.argv.find(arg => arg.startsWith('--confirm='))?.slice('--confirm='.length);

  if (apply && confirmation !== APPLY_CONFIRMATION) {
    console.error(`Refusing writes. Use --confirm-production --confirm=${APPLY_CONFIRMATION}`);
    process.exit(1);
  }

  run({ apply })
    .then(output => {
      console.log(JSON.stringify(output, null, 2));
      process.exit(output.ok ? 0 : 1);
    })
    .catch(err => {
      console.error(err.message || err.cause?.message || String(err));
      process.exit(1);
    });
}

module.exports = {
  APPLY_CONFIRMATION,
  partitionJakeLeads,
  isAllowlistedJakeAoTestLead,
  inspectLeadDependents,
  deleteTargetLeads,
  run,
};
