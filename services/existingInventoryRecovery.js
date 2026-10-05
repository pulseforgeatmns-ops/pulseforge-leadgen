'use strict';

const { discoverGovernedInventory } = require('./acquisitionMissionInventory');
const { normalizeVertical } = require('../utils/normalize');

function sourceScope(source) {
  const payload = source?.mission || source?.payload || source || {};
  const structured = payload.structuredMission || {};
  const market = structured.market || {};
  const geography = structured.geography || {};
  return {
    segment: market.segment || payload.targetSegment || source?.target_segment || '',
    region: geography.region || null,
    scope: geography.scope || null,
    cities: Array.isArray(geography.cities) ? geography.cities.map(x => String(x).toLowerCase()) : [],
  };
}

function isNationwideScope(scope = {}) {
  if (String(scope.scope || '').trim().toLowerCase() === 'nationwide') return true;
  const region = String(scope.region || '').trim().toLowerCase();
  const cities = Array.isArray(scope.cities) ? scope.cities : [];
  if (!cities.length && (region === 'united states' || region === 'us' || region === 'u.s.')) return true;
  return false;
}

function verticalFromKnowledgeContent(content = {}) {
  const direct = normalizeVertical(content.industry || content.vertical || '');
  if (direct && direct !== 'unknown') return direct;
  const tags = Array.isArray(content.tags) ? content.tags : [];
  for (const tag of tags) {
    const normalized = normalizeVertical(tag);
    if (!normalized || normalized === 'unknown') continue;
    if (normalized === 'babrun' || normalized.includes('cohort')) continue;
    return normalized;
  }
  return null;
}

/**
 * Promote AK geography/vertical onto operational prospects so nationwide missions
 * can project existing Scout inventory without net-new discovery.
 */
async function backfillOperationalFieldsFromKnowledge(pool, clientId, scope = {}) {
  if (!pool || !Number.isFinite(Number(clientId)) || !isNationwideScope(scope)) {
    return { updatedGeography: 0, updatedVertical: 0 };
  }
  const { rows } = await pool.query(
    `SELECT p.id, p.vertical, p.service_area_match, k.content AS knowledge_content
       FROM prospects p
       JOIN acquisition_knowledge_objects k
         ON k.id = p.acquisition_knowledge_object_id
        AND k.tenant_id = p.client_id::text
      WHERE p.client_id = $1
        AND k.object_type = 'prospect_intelligence'
        AND k.lifecycle_state NOT IN ('RETIRED','ARCHIVED')`,
    [Number(clientId)]
  );
  let updatedGeography = 0;
  let updatedVertical = 0;
  for (const row of rows) {
    const content = row.knowledge_content || {};
    const location = String(content.location || '').trim();
    const vertical = verticalFromKnowledgeContent(content);
    if (location && !String(row.service_area_match || '').trim()) {
      await pool.query(
        `UPDATE prospects SET service_area_match = $2, updated_at = NOW()
          WHERE id = $1 AND client_id = $3
            AND (service_area_match IS NULL OR BTRIM(service_area_match::text) = '')`,
        [row.id, location, Number(clientId)]
      );
      updatedGeography += 1;
    }
    if (vertical && (!row.vertical || row.vertical === 'unknown')) {
      await pool.query(
        `UPDATE prospects SET vertical = $2, updated_at = NOW()
          WHERE id = $1 AND client_id = $3 AND (vertical IS NULL OR vertical = 'unknown')`,
        [row.id, vertical, Number(clientId)]
      );
      updatedVertical += 1;
    }
  }
  return { updatedGeography, updatedVertical };
}

async function recoverExistingGovernedInventory({
  pool,
  program,
  source,
  tenantId,
} = {}) {
  if (!pool || !program?.source_mission_id) return null;
  const tid = String(tenantId || program.tenant_id || '');
  const scope = sourceScope(source);
  await backfillOperationalFieldsFromKnowledge(pool, Number(tid), scope);
  const mission = {
    id: program.source_mission_id,
    tenantId: tid,
    orchestrationMissionId: program.source_mission_id,
    objective: source?.objective || null,
    structuredMission: source?.payload?.structuredMission || source?.payload || {},
  };
  return discoverGovernedInventory(mission, { pool, governedProgram: program });
}

module.exports = {
  isNationwideScope,
  verticalFromKnowledgeContent,
  backfillOperationalFieldsFromKnowledge,
  recoverExistingGovernedInventory,
};
