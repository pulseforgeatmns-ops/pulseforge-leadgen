#!/usr/bin/env node
'use strict';

require('dotenv').config({ quiet: true });

const pool = require('../db');
const {
  PostgresTenantMailboxStore,
  pulseforgeMailboxConfig,
  publicIntegration,
  publicIdentity,
  verifyTenantMailbox,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
} = require('../services/tenantMailbox');
const { ensurePulseforgeCashflowMission, pulseforgeStructuredMission } = require('../utils/pulseforgeCashflowTenant');
const { produceTenantMailboxCapacityEnvelope } = require('../services/emmettTenantMailboxCapacity');
const { productionService } = require('../services/governedOutbound');
const { TENANT_ID, PULSEFORGE_MAILBOX } = require('./lib/pulseforgeCashflowOutbound');

const CONFIRM = 'pulseforge-missed-revenue-review-2026-10-13';
const ACTOR = Object.freeze({ id: 'jacob@pulseforge.com', role: 'admin' });

async function bootstrapProductionState() {
  // Draft creation only. Never replay historical shared trigger migrations or
  // overwrite live tenant settings as an incidental bootstrap step.
  return ensurePulseforgeCashflowMission(pool);
}

function parseArgs(argv = process.argv.slice(2)) {
  const args = { _: [] };
  for (const arg of argv) {
    if (!arg.startsWith('--')) args._.push(arg);
    else {
      const [key, ...rest] = arg.slice(2).split('=');
      args[key] = rest.length ? rest.join('=') : true;
    }
  }
  return args;
}

function confirmed(args) {
  if (args.confirm !== CONFIRM) {
    throw Object.assign(new Error(`Refusing without --confirm=${CONFIRM}`), { code: 'confirm_required' });
  }
}

function print(value) {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

async function configureMailbox(store, active = false) {
  const cfg = pulseforgeMailboxConfig(TENANT_ID);
  const integration = await store.saveIntegration({
    ...cfg.integration,
    status: active ? MAILBOX_STATUS.ACTIVE : cfg.integration.status,
  });
  const identity = await store.saveIdentity({
    ...cfg.identity,
    mailboxIntegrationId: integration.id,
    status: active ? IDENTITY_STATUS.ACTIVE : cfg.identity.status,
  });
  return {
    integration: publicIntegration(integration),
    identity: publicIdentity(identity),
    oauthRefreshSecretRef: cfg.integration.oauthRefreshSecretRef,
  };
}

async function verifyMailbox(store) {
  const result = await verifyTenantMailbox({
    tenantId: TENANT_ID,
    integrationId: PULSEFORGE_MAILBOX.inboxIntegrationId,
    dkimSelector: process.env.PULSEFORGE_DKIM_SELECTOR,
  }, { store });
  const state = result.verificationState || {};
  return {
    ...result,
    pass: state.smtp?.status === 'verified'
      && state.imap?.status === 'verified'
      && state.spf?.status === 'present'
      && state.dmarc?.status === 'present'
      && (!process.env.PULSEFORGE_DKIM_SELECTOR || state.dkim?.status === 'present'),
  };
}

async function activateMailbox(store) {
  const verified = await verifyMailbox(store);
  if (!verified.pass) {
    throw Object.assign(new Error('Pulseforge mailbox verification did not pass.'), {
      code: 'pulseforge_mailbox_verification_failed',
      detail: verified.verificationState,
    });
  }
  const integration = await store.getIntegration(TENANT_ID, PULSEFORGE_MAILBOX.inboxIntegrationId);
  const identity = await store.getIdentity(TENANT_ID, PULSEFORGE_MAILBOX.sendingIdentityId);
  const activeIntegration = await store.saveIntegration({ ...integration, status: MAILBOX_STATUS.ACTIVE });
  const activeIdentity = await store.saveIdentity({ ...identity, status: IDENTITY_STATUS.ACTIVE });
  return {
    integration: publicIntegration(activeIntegration),
    identity: publicIdentity(activeIdentity),
    status: 'ACTIVE',
  };
}

async function emmettReadiness() {
  const { snapshot, assessment, envelope } = await produceTenantMailboxCapacityEnvelope(
    TENANT_ID,
    PULSEFORGE_MAILBOX.sendingIdentityId,
    { pool }
  );
  const recommended = Number(envelope?.maxSendsPerDay ?? assessment?.maxSendsPerDay ?? 0);
  const governor = envelope?.governorState || assessment?.governor?.outcome;
  return {
    tenantId: TENANT_ID,
    sendingIdentityId: PULSEFORGE_MAILBOX.sendingIdentityId,
    senderEmail: snapshot.senderEmail,
    mailboxStatus: snapshot.mailboxStatus,
    identityStatus: snapshot.identityStatus,
    authentication: snapshot.authentication,
    recommendedSafeDailyCapacity: recommended,
    minimumSpacingMinutes: envelope?.minimumSpacingMinutes,
    allowedSendWindow: envelope?.allowedSendWindow,
    governor,
    riskFlags: envelope?.riskFlags || assessment?.riskFlags || [],
    pass: ['proceed', 'slow'].includes(String(governor || '').toLowerCase()) && recommended > 0,
  };
}

function service() {
  return productionService(pool, { tenantId: TENANT_ID });
}

async function createGovernedProgram({ missionId, reviewHash, startsAt, expiresAt } = {}) {
  if (!missionId) throw Object.assign(new Error('Select the separately reviewed $297 source mission with --mission=<id>.'), { code: 'reviewed_mission_id_required' });
  const emmett = await emmettReadiness();
  if (!emmett.pass) {
    throw Object.assign(new Error('Emmett readiness did not pass.'), { code: 'emmett_readiness_failed', detail: emmett });
  }
  const operatorCeiling = Math.min(3, Math.max(1, emmett.recommendedSafeDailyCapacity));
  const policyStartsAt = startsAt || new Date().toISOString();
  const input = {
    tenantId: TENANT_ID,
    sourceMissionId: missionId,
    senderEmail: PULSEFORGE_MAILBOX.senderEmail,
    inboxIntegrationId: PULSEFORGE_MAILBOX.inboxIntegrationId,
    sendingIdentityId: PULSEFORGE_MAILBOX.sendingIdentityId,
    allowedContactClassifications: ['VERIFIED_FOUNDER_EMAIL', 'VERIFIED_ROLE_EMAIL'],
    dailyCap: operatorCeiling,
    operatorDelegatedMaximumDailyCapacity: operatorCeiling,
    totalCap: 30,
    spacingMinutes: Math.max(240, Number(emmett.minimumSpacingMinutes || 0)),
    startHour: 9,
    endHour: 17,
    startsAt: policyStartsAt,
    expiresAt: expiresAt || new Date(Date.parse(policyStartsAt) + 30 * 86400000).toISOString(),
  };
  const svc = service();
  const program = await svc.authorize({ ...input, reviewHash }, ACTOR);
  // Show the exact bounded policy for operator review. Never approve our own
  // returned review hash or reuse a $500 program's standing authority.
  if (program.reviewRequired) return { ...program, mode: null, emmett };
  await svc.setMode(program.id, 'shadow', program.policy_hash, ACTOR);
  return { programId: program.id, mode: 'shadow', sourceMissionId: missionId, policy: program.policy, emmett };
}

async function activateProgram(programId, policyHash) {
  if (!programId || !policyHash) throw Object.assign(new Error('Activation requires the explicitly reviewed program ID and policy hash.'), { code: 'reviewed_program_binding_required' });
  const svc = service();
  const status = await svc.status();
  const program = status.program;
  if (!program || program.id !== programId) {
    throw Object.assign(new Error('Pulseforge governed program not found.'), { code: 'program_missing' });
  }
  const emmett = await emmettReadiness();
  if (!emmett.pass) throw Object.assign(new Error('Emmett readiness did not pass.'), { code: 'emmett_readiness_failed', detail: emmett });
  await svc.setMode(program.id, 'active', policyHash, ACTOR);
  return {
    ...(await svc.status()),
    standingAuthorization: true,
    note: 'This 30-day bounded program is the standing execution authority; no daily operator approval is required.',
  };
}

async function main() {
  const args = parseArgs();
  const command = args._[0] || 'help';
  const store = new PostgresTenantMailboxStore(pool);
  if (command === 'help') {
    process.stdout.write([
      'Pulseforge cash-flow offer activation (tenant 1)',
      '',
      '  review (read-only proposal)',
      `  bootstrap --confirm=${CONFIRM} (creates an unapproved draft only)`,
      `  configure-mailbox --confirm=${CONFIRM}`,
      '  verify-mailbox',
      `  activate-mailbox --confirm=${CONFIRM}`,
      '  emmett-readiness',
      `  create-program --confirm=${CONFIRM} --mission=<reviewed-id> [--review-hash=<hash> --starts-at=<reviewed-time> --expires-at=<reviewed-time>]`,
      `  activate-program --confirm=${CONFIRM} --program=<reviewed-id> --policy-hash=<reviewed-hash>`,
      '  status',
      '',
      'Required runtime env: DATABASE_URL, PULSEFORGE_GOOGLE_REFRESH_TOKEN, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET.',
      'Automated execution also requires GOVERNED_OUTBOUND_TENANT_IDS to include 1 and PULSEFORGE_GOVERNED_OUTBOUND_ENABLED=true.',
    ].join('\n') + '\n');
    return;
  }
  if (command === 'review') return print({ tenantId: TENANT_ID, reviewRequired: true, structuredMission: pulseforgeStructuredMission(), sent: 0 });
  if (command === 'bootstrap') { confirmed(args); return print(await bootstrapProductionState()); }
  if (command === 'configure-mailbox') { confirmed(args); return print(await configureMailbox(store)); }
  if (command === 'verify-mailbox') return print(await verifyMailbox(store));
  if (command === 'activate-mailbox') { confirmed(args); return print(await activateMailbox(store)); }
  if (command === 'emmett-readiness') return print(await emmettReadiness());
  if (command === 'create-program') { confirmed(args); return print(await createGovernedProgram({ missionId: args.mission, reviewHash: args['review-hash'], startsAt: args['starts-at'], expiresAt: args['expires-at'] })); }
  if (command === 'activate-program') { confirmed(args); return print(await activateProgram(args.program, args['policy-hash'])); }
  if (command === 'status') return print(await service().status());
  throw Object.assign(new Error(`Unknown command: ${command}`), { code: 'unknown_command' });
}

if (require.main === module) {
  main().catch((error) => {
    console.error(JSON.stringify({ error: error.code || error.message, detail: error.detail || null }));
    process.exitCode = 1;
  }).finally(() => pool.end().catch(() => {}));
}

module.exports = {
  parseArgs,
  confirmed,
  configureMailbox,
  bootstrapProductionState,
  verifyMailbox,
  activateMailbox,
  emmettReadiness,
  createGovernedProgram,
  activateProgram,
};
