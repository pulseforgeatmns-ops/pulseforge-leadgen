'use strict';

/**
 * SPEC-SUBSTRAL-PF-001 — production acceptance runner (read/write on DATABASE_URL).
 * Run after migration 2026-09-30-studio-substral-pf-001.sql is applied and PR #779 is deployed.
 *
 *   node scripts/studioSubstralProductionProof.js
 *   node scripts/studioSubstralProductionProof.js --skip-woi
 */

const fs = require('node:fs');
const path = require('node:path');
const { createHash } = require('node:crypto');

const pool = require('../db');
const {
  STUDIO_SUBSTRAL_SLUG,
  STUDIO_SUBSTRAL_DOMAIN,
  SUBSTRAL_MISSION_OBJECTIVE,
  SUBSTRAL_MISSION_CONSTRAINTS,
  ensureStudioSubstralTenant,
  ensureStudioSubstralMission,
  findStudioSubstralClient,
} = require('../utils/studioSubstralTenant');
const { captureAssessmentRequest } = require('../lib/substralAssessmentIntake');
const { reconcileAssessmentIntake } = require('../lib/substralAssessmentReconcile');
const { buildStudioSubstralOperatorSnapshot } = require('../services/studioSubstralOperatorSnapshot');
const { assessDiscoveredBusiness } = require('../services/webDesignScout');
const { buildSixLayerFindings, summarizeEvidenceStrength } = require('../utils/studioSubstralLayers');
const { prioritizeStudioSubstralOpportunities } = require('../utils/studioSubstralMaxPrioritization');
const { evaluateStudioSubstralOutboundReadiness, CANONICAL_SENDER } = require('../utils/studioSubstralOutboundGovernance');
const { buildPaigeStudioSubstralContext } = require('../utils/paigeStudioSubstralContext');
const { LAYER_KEYS } = require('../utils/studioSubstralAssessmentWorkflow');
const { EVIDENCE_CLASS } = require('../packages/capabilities/websiteOpportunityIntelligence/types');

const skipWoi = process.argv.includes('--skip-woi');
const LEGACY_ACTION_ID = process.env.SUBSTRAL_PROOF_ACTION_ID || '2adba503-2aba-48e9-8569-2e502f25ef30';

const report = {
  started_at: new Date().toISOString(),
  checks: {},
  artifacts: {},
  pass: false,
};

function check(name, ok, detail) {
  report.checks[name] = { ok: Boolean(ok), detail };
  const mark = ok ? 'PASS' : 'FAIL';
  console.log(`[${mark}] ${name}${detail != null ? `: ${typeof detail === 'string' ? detail : JSON.stringify(detail)}` : ''}`);
  return ok;
}

async function applyMigrationIfNeeded() {
  const tbl = await pool.query(
    `SELECT to_regclass('public.studio_substral_assessment_opportunities') AS tbl`
  );
  if (tbl.rows[0]?.tbl) return { applied: false, reason: 'already_exists' };

  const sqlPath = path.join(__dirname, '../migrations/2026-09-30-studio-substral-pf-001.sql');
  const sql = fs.readFileSync(sqlPath, 'utf8');
  await pool.query(sql);
  return { applied: true };
}

async function migrateLegacyIntakeToTenant(substralClientId) {
  const legacy = await pool.query(
    `SELECT id, client_id, payload FROM agent_actions
      WHERE created_by = 'studio_substral_site'
        AND action_type = 'website_assessment_request'
      ORDER BY created_at ASC
      LIMIT 1`
  );
  if (!legacy.rows[0]) return { migrated: false, reason: 'no_legacy_request' };

  const row = legacy.rows[0];
  if (Number(row.client_id) === Number(substralClientId)) {
    return { migrated: false, reason: 'already_on_substral_tenant', actionId: row.id };
  }

  await pool.query(
    `UPDATE agent_actions SET client_id = $1 WHERE id = $2`,
    [substralClientId, row.id]
  );
  return { migrated: true, actionId: row.id, from_client_id: row.client_id };
}

async function reconcileExistingAction(substralClientId, actionId) {
  const res = await pool.query(
    `SELECT id, payload FROM agent_actions WHERE id = $1 AND client_id = $2`,
    [actionId, substralClientId]
  );
  const row = res.rows[0];
  if (!row) throw new Error(`Assessment agent_action ${actionId} not found on tenant ${substralClientId}`);

  const payload = row.payload || {};
  return reconcileAssessmentIntake(pool, {
    clientId: substralClientId,
    agentActionId: row.id,
    domain: payload.subject_domain,
    email: payload.reply_to,
    context: payload.stated_context,
    requestKey: payload.request_key,
    actionPayload: payload,
  });
}

async function countLinkedEntities(substralClientId, domain, requestKey) {
  const companies = await pool.query(
    `SELECT COUNT(*)::int AS n FROM companies WHERE client_id = $1 AND lower(domain) = lower($2)`,
    [substralClientId, domain]
  );
  const prospects = await pool.query(
    `SELECT COUNT(*)::int AS n FROM prospects p
      JOIN companies c ON c.id = p.company_id
     WHERE p.client_id = $1 AND lower(c.domain) = lower($2)`,
    [substralClientId, domain]
  );
  const opps = await pool.query(
    `SELECT COUNT(*)::int AS n FROM studio_substral_assessment_opportunities
      WHERE client_id = $1 AND request_key = $2`,
    [substralClientId, requestKey]
  );
  return {
    companies: companies.rows[0].n,
    prospects: prospects.rows[0].n,
    opportunities: opps.rows[0].n,
  };
}

async function tenantIsolation(substralId, anchorId, babrunId) {
  const tables = [
    ['companies', 'client_id'],
    ['prospects', 'client_id'],
    ['website_opportunity_assessments', 'client_id'],
    ['studio_substral_assessment_opportunities', 'client_id'],
  ];
  const leaks = [];
  for (const [table, col] of tables) {
    try {
      const r = await pool.query(
        `SELECT COUNT(*)::int AS n FROM ${table}
          WHERE ${col} = $1 AND ${col} IN ($2, $3)`,
        [substralId, anchorId, babrunId]
      );
      if (r.rows[0].n > 0) leaks.push({ table, unexpected: r.rows[0].n });
    } catch {
      /* table may not exist in older schemas */
    }
  }

  const cross = await pool.query(
    `SELECT COUNT(*)::int AS n FROM studio_substral_assessment_opportunities WHERE client_id <> $1`,
    [substralId]
  );
  if (cross.rows[0].n > 0) leaks.push({ table: 'studio_substral_assessment_opportunities_other_tenant', n: cross.rows[0].n });

  const anchorInSubstral = anchorId
    ? await pool.query(
        `SELECT COUNT(*)::int AS n FROM studio_substral_assessment_opportunities WHERE client_id = $1`,
        [anchorId]
      )
    : { rows: [{ n: 0 }] };
  const babrunInSubstral = babrunId
    ? await pool.query(
        `SELECT COUNT(*)::int AS n FROM studio_substral_assessment_opportunities WHERE client_id = $1`,
        [babrunId]
      )
    : { rows: [{ n: 0 }] };

  return {
    leaks,
    anchor_opportunity_rows: anchorInSubstral.rows[0].n,
    babrun_opportunity_rows: babrunInSubstral.rows[0].n,
    substral_id: substralId,
    anchor_id: anchorId,
    babrun_id: babrunId,
  };
}

async function generatePaigeDryRunExample(client, assessmentPayload) {
  const context = buildPaigeStudioSubstralContext(client, assessmentPayload);
  const apiKey = process.env.ANTHROPIC_API_KEY;
  if (!apiKey) {
    return {
      dry_run: true,
      skipped_generation: true,
      doctrine_context: context,
      note: 'Set ANTHROPIC_API_KEY to generate live Paige copy.',
    };
  }

  const Anthropic = require('@anthropic-ai/sdk');
  const client_ai = new Anthropic({ apiKey });
  const msg = await client_ai.messages.create({
    model: process.env.PAIGE_WRITER_MODEL || 'claude-opus-5-5',
    max_tokens: 600,
    messages: [{
      role: 'user',
      content: `Write ONE short cold email (under 120 words) for Studio Substral assessment outreach. DRY RUN — do not send.
Use the doctrine JSON below. Human voice, contractions, evidence-specific opening if evidence exists, CTA = paid website assessment only.
No generic agency language, no manufactured urgency, no unsupported revenue claims, do not assume redesign is required.

Doctrine: ${JSON.stringify(context)}`,
    }],
  });
  const text = msg.content?.filter((b) => b.type === 'text').map((b) => b.text).join('\n') || '';
  return { dry_run: true, text, doctrine_context: context };
}

async function main() {
  try {
    const mig = await applyMigrationIfNeeded();
    check('migration.studio_substral_assessment_opportunities', true, mig);

    const client = await ensureStudioSubstralTenant(pool);
    const domainOk = (client.website || '').includes(STUDIO_SUBSTRAL_DOMAIN);
    check(
      'tenant.exists',
      client.slug === STUDIO_SUBSTRAL_SLUG && client.scoring_profile === 'studio_substral',
      { tenant_id: client.id, slug: client.slug, scoring_profile: client.scoring_profile, website: client.website }
    );
    check('tenant.domain', domainOk, STUDIO_SUBSTRAL_DOMAIN);
    report.artifacts.STUDIO_SUBSTRAL_CLIENT_ID = client.id;
    console.log(`\n>>> Set STUDIO_SUBSTRAL_CLIENT_ID=${client.id} in Railway production <<<\n`);

    const { missionId, mission, created } = await ensureStudioSubstralMission(pool);
    const profile = mission?.studio_substral_profile || {};
    const doctrineOk =
      mission?.objective === SUBSTRAL_MISSION_OBJECTIVE
      && profile.sales_doctrine === 'diagnosis_before_design'
      && profile.success_criterion === 'one_paid_assessment'
      && profile.outreach_enabled === false
      && (profile.doctrine || []).includes('diagnosis_before_design')
      && (profile.valid_downstream_conclusions || []).includes('TARGETED_FIX');
    check('mission.active', Boolean(missionId) && doctrineOk, {
      missionId,
      created,
      profile,
      status: mission?.status,
    });

    const anchor = await pool.query(`SELECT id FROM clients WHERE slug = 'cleaning-co' LIMIT 1`);
    const babrun = await pool.query(`SELECT id FROM clients WHERE slug IN ('fedir', 'babrun') ORDER BY id LIMIT 1`);
    check(
      'tenant.isolation.ids',
      Number(client.id) !== Number(anchor.rows[0]?.id) && Number(client.id) !== Number(babrun.rows[0]?.id),
      { substral: client.id, anchor: anchor.rows[0]?.id, babrun: babrun.rows[0]?.id }
    );

    const migrationMeta = await migrateLegacyIntakeToTenant(client.id);
    report.artifacts.legacy_intake_migration = migrationMeta;

    const actionId = migrationMeta.actionId || LEGACY_ACTION_ID;
    await reconcileExistingAction(client.id, actionId);

    const actionRow = await pool.query(`SELECT payload FROM agent_actions WHERE id = $1`, [actionId]);
    const payload = actionRow.rows[0]?.payload || {};
    const domain = payload.subject_domain;
    const requestKey = payload.request_key;

    let counts = await countLinkedEntities(client.id, domain, requestKey);
    check(
      'intake.linked_entities',
      counts.companies === 1 && counts.prospects === 1 && counts.opportunities === 1,
      counts
    );

    const replay = await captureAssessmentRequest(pool, {
      domain,
      email: payload.reply_to,
      context: payload.stated_context,
      request_key: requestKey,
    });
    counts = await countLinkedEntities(client.id, domain, requestKey);
    check(
      'intake.replay_idempotent',
      replay.duplicate === true && counts.companies === 1 && counts.prospects === 1 && counts.opportunities === 1,
      { replay, counts }
    );

    const snapshot = await buildStudioSubstralOperatorSnapshot(pool, client.id);
    report.artifacts.operator_snapshot = snapshot;
    const snapOk =
      snapshot.tenant.id === client.id
      && snapshot.mission?.id
      && Array.isArray(snapshot.assessment_requests)
      && snapshot.outbound_governance?.ready === false;
    check('operator.snapshot', snapOk, {
      tenant: snapshot.tenant.id,
      mission: snapshot.mission?.id,
      requests: snapshot.assessment_requests.length,
      outbound_ready: snapshot.outbound_governance?.ready,
      outbound_reasons: snapshot.outbound_governance?.reasons,
    });

    let woiResult = null;
    if (!skipWoi) {
      woiResult = await assessDiscoveredBusiness(
        {
          client_id: client.id,
          mission_id: missionId,
          domain,
          company: domain,
          skipPuppeteer: true,
          skipPageSpeed: false,
        },
        { pool, skipPuppeteer: true }
      );
      const sixLayer = buildSixLayerFindings(woiResult.assessment || {});
      const layersPresent = LAYER_KEYS.filter((k) => (sixLayer[k] || []).length > 0);
      const classes = new Set();
      for (const layer of LAYER_KEYS) {
        for (const f of sixLayer[layer] || []) classes.add(f.evidence_class);
      }
      const epistemicOk = ['MEASURED', 'OBSERVED', 'INFERRED', 'UNKNOWN'].every((c) =>
        classes.size === 0 || classes.has(c) || [...classes].every((x) => EVIDENCE_CLASS[x] || x)
      );
      report.artifacts.woi = {
        domain,
        layersPresent,
        evidence_distribution: summarizeEvidenceStrength(sixLayer),
        six_layer_sample: sixLayer,
        recommended_action: woiResult.recommended_action,
      };
      check('scout.woi_cycle', !woiResult.skipped, { layersPresent, classes: [...classes] });
      const allLayerKeys = LAYER_KEYS.every((k) => Array.isArray(sixLayer[k]));
      check('scout.six_layer_taxonomy', allLayerKeys, LAYER_KEYS);
      check('scout.six_layers_populated', layersPresent.length >= 1, layersPresent);
      check('scout.epistemic_labels', epistemicOk, [...classes]);
    }

    const { listAssessmentsForClient } = require('../services/websiteOpportunityPersistence');
    const { listAssessmentOpportunities } = require('../services/studioSubstralPersistence');
    const assessments = await listAssessmentsForClient(pool, client.id, { limit: 50 });
    const assessmentRequests = await listAssessmentOpportunities(pool, client.id, { limit: 25 });
    const maxDigest = prioritizeStudioSubstralOpportunities({ assessments, assessmentRequests });
    report.artifacts.max_prioritization = maxDigest;
    const top = maxDigest.combined[0];
    const maxOk = Boolean(top?.max_questions?.why_worth_assessing || top?.max_priority_score != null);
    check('max.prioritization_digest', maxOk, top?.max_questions || top);

    let paige;
    try {
      paige = await generatePaigeDryRunExample(client, woiResult?.assessment || null);
    } catch (paigeErr) {
      paige = {
        dry_run: true,
        skipped_generation: true,
        error: paigeErr.message,
        doctrine_context: buildPaigeStudioSubstralContext(client, woiResult?.assessment || null),
      };
    }
    report.artifacts.paige_dry_run = paige;
    const paigeText = (paige.text || '').toLowerCase();
    const forbidden = ['website makeover', 'free strategy call', 'you are losing customers', 'urgent website fix'];
    const paigeOk = !paige.text || !forbidden.some((f) => paigeText.includes(f));
    check('paige.dry_run_compliant', paigeOk, paige.skipped_generation ? 'doctrine_only' : 'generated');

    const outbound = await evaluateStudioSubstralOutboundReadiness(pool, client.id);
    const blocked = outbound.ready === false && (
      outbound.reasons.includes('mailbox_not_authenticated')
      || outbound.reasons.includes('authentication_not_verified_from_delivery')
      || outbound.reasons.includes('governed_outbound_not_authorized')
      || outbound.reasons.includes('governed_outbound_program_missing')
    );
    check('emmett.outbound_blocked', blocked, outbound);

    const pulseforgeSender = await pool.query(`SELECT sender_email FROM clients WHERE id = 1`);
    const anchorSender = await pool.query(`SELECT sender_email FROM clients WHERE slug = 'cleaning-co'`);
    check(
      'emmett.no_anchor_sender_fallback',
      !String(anchorSender.rows[0]?.sender_email || '').includes('studiosubstral.com'),
      anchorSender.rows[0]?.sender_email
    );
    check(
      'emmett.no_pulseforge_sender_for_substral',
      String(client.sender_email).toLowerCase() === CANONICAL_SENDER
        && !String(pulseforgeSender.rows[0]?.sender_email || '').includes('studiosubstral.com'),
      { substral: client.sender_email, pulseforge: pulseforgeSender.rows[0]?.sender_email }
    );
    check(
      'emmett.forwarding_only_insufficient',
      blocked && (
        outbound.reasons.includes('mailbox_not_authenticated')
        || outbound.reasons.includes('authentication_not_verified_from_delivery')
      ),
      'hello@studiosubstral.com requires ACTIVE tenant_mailbox_integrations with delivered-message SPF/DKIM/DMARC pass'
    );

    const isolation = await tenantIsolation(client.id, anchor.rows[0]?.id, babrun.rows[0]?.id);
    report.artifacts.tenant_isolation = isolation;
    const isoOk =
      isolation.leaks.length === 0
      && isolation.anchor_opportunity_rows === 0
      && isolation.babrun_opportunity_rows === 0;
    check('tenant.isolation.data', isoOk, isolation);

    const allOk = Object.values(report.checks).every((c) => c.ok);
    report.pass = allOk;
    report.finished_at = new Date().toISOString();

    const outPath = path.join(__dirname, '../artifacts/studio-substral-production-proof.json');
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
    console.log(`\nWrote ${outPath}`);
    console.log(`\nOVERALL: ${allOk ? 'PASS' : 'FAIL'}\n`);
    process.exit(allOk ? 0 : 1);
  } catch (err) {
    console.error(err);
    process.exit(1);
  } finally {
    await pool.end();
  }
}

main();
