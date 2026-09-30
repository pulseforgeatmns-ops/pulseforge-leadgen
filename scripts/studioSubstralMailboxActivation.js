#!/usr/bin/env node
'use strict';

/**
 * Studio Substral (tenant 17) — Google Workspace mailbox activation + Emmett readiness.
 * Does not authorize live governed sending (SUBSTRAL_GOVERNED_OUTBOUND_ENABLED stays false).
 *
 *   node scripts/studioSubstralMailboxActivation.js configure
 *   node scripts/studioSubstralMailboxActivation.js verify
 *   node scripts/studioSubstralMailboxActivation.js capture-delivered-auth --headers-file=/path/to/original.txt
 *   node scripts/studioSubstralMailboxActivation.js activate --confirm=substral-mailbox-2026-09-30
 *   node scripts/studioSubstralMailboxActivation.js emmett-readiness
 *   node scripts/studioSubstralMailboxActivation.js create-governed-program --confirm=substral-mailbox-2026-09-30
 */

require('dotenv').config({ quiet: true });

const crypto = require('crypto');
const fs = require('fs');
const pool = require('../db');
const {
  PostgresTenantMailboxStore,
  studioSubstralMailboxConfig,
  publicIntegration,
  publicIdentity,
  verifyTenantMailbox,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
} = require('../services/tenantMailbox');
const {
  verificationStateFromDeliveredHeaders,
  deliveredAuthenticationPasses,
  parseDeliveredAuthenticationEvidence,
} = require('../utils/mailAuthenticationResults');
const { diagnoseGoogleMailboxOAuth } = require('../utils/googleMailboxOAuth');
const { resolveSecretRef } = require('../services/tenantMailbox');
const { parseRawMailHeaders } = require('../utils/mailHeaders');
const { CANONICAL_SENDER } = require('../utils/studioSubstralOutboundGovernance');
const {
  TENANT_ID,
  STUDIO_SUBSTRAL_MAILBOX,
} = require('./lib/studioSubstralCanonicalOutbound');
const { ensureStudioSubstralMission } = require('../utils/studioSubstralTenant');
const { produceTenantMailboxCapacityEnvelope } = require('../services/emmettTenantMailboxCapacity');
const { productionService } = require('../services/governedOutbound');
const { adapters } = require('../services/governedOutboundAdapters');

const ACTIVATION_CONFIRM = 'substral-mailbox-2026-09-30';

function parseArgs(argv = process.argv.slice(2)) {
  const args = { _: [] };
  for (const arg of argv) {
    if (arg.startsWith('--')) {
      const [key, ...parts] = arg.slice(2).split('=');
      args[key] = parts.length ? parts.join('=') : true;
    } else {
      args._.push(arg);
    }
  }
  return args;
}

function printJson(value) {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

function readHeadersFile(args) {
  if (args['headers-file']) {
    return fs.readFileSync(String(args['headers-file']), 'utf8');
  }
  if (args['headers-stdin'] === true) {
    return fs.readFileSync(0, 'utf8');
  }
  throw Object.assign(new Error('Provide --headers-file= or --headers-stdin with Authentication-Results from the delivered test message.'), { code: 'headers_required' });
}

async function configure(store, args) {
  const cfg = studioSubstralMailboxConfig(TENANT_ID);
  const active = args.active === true;
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

async function mergeDeliveredAuth(store, headersRaw) {
  const headerMap = parseRawMailHeaders(headersRaw);
  const headers = Object.fromEntries(headerMap.entries());
  const delivered = verificationStateFromDeliveredHeaders(headers);
  const parsed = parseDeliveredAuthenticationEvidence(headers);
  const integration = await store.getIntegration(TENANT_ID, STUDIO_SUBSTRAL_MAILBOX.inboxIntegrationId);
  if (!integration) throw Object.assign(new Error('Configure mailbox integration first.'), { code: 'integration_missing' });

  const fromOk = !parsed.from || parsed.from === CANONICAL_SENDER;
  const replyOk = !parsed.replyTo || parsed.replyTo === CANONICAL_SENDER;
  if (!fromOk) {
    throw Object.assign(new Error(`From address mismatch: expected ${CANONICAL_SENDER}, got ${parsed.from}`), { code: 'from_mismatch' });
  }
  if (parsed.replyTo && !replyOk) {
    throw Object.assign(new Error(`Reply-To mismatch: expected ${CANONICAL_SENDER}, got ${parsed.replyTo}`), { code: 'reply_to_mismatch' });
  }
  if (!deliveredAuthenticationPasses(delivered)) {
    throw Object.assign(new Error('SPF/DKIM/DMARC pass evidence missing from delivered message headers.'), {
      code: 'authentication_incomplete',
      detail: { spf: delivered.spf, dkim: delivered.dkim, dmarc: delivered.dmarc },
    });
  }

  const prior = integration.verificationState || {};
  const verificationState = {
    ...prior,
    spf: delivered.spf,
    dkim: delivered.dkim,
    dmarc: delivered.dmarc,
    from: parsed.from,
    replyTo: parsed.replyTo,
    deliveredAuthCapturedAt: delivered.capturedAt,
  };

  const updated = await store.saveIntegration({
    ...integration,
    verificationState,
  });
  return { integration: publicIntegration(updated), authentication: { spf: 'PASS', dkim: 'PASS', dmarc: 'PASS', from: parsed.from, replyTo: parsed.replyTo || CANONICAL_SENDER } };
}

async function activateMailbox(store) {
  const integration = await store.getIntegration(TENANT_ID, STUDIO_SUBSTRAL_MAILBOX.inboxIntegrationId);
  const identity = await store.getIdentity(TENANT_ID, STUDIO_SUBSTRAL_MAILBOX.sendingIdentityId);
  if (!integration || !identity) throw Object.assign(new Error('Missing integration or identity.'), { code: 'integration_missing' });
  if (!deliveredAuthenticationPasses(integration.verificationState || {})) {
    throw Object.assign(new Error('Run capture-delivered-auth before activate.'), { code: 'authentication_incomplete' });
  }
  const verify = await verifyTenantMailbox({
    tenantId: TENANT_ID,
    integrationId: integration.id,
  }, { store });
  if (verify.verificationState?.imap?.status !== 'verified') {
    throw Object.assign(new Error('IMAP verification failed.'), { code: 'imap_verification_failed', detail: verify.verificationState?.imap });
  }
  if (verify.verificationState?.smtp?.status !== 'verified') {
    throw Object.assign(new Error('SMTP verification failed.'), { code: 'smtp_verification_failed', detail: verify.verificationState?.smtp });
  }

  const mergedState = {
    ...verify.verificationState,
    spf: integration.verificationState.spf,
    dkim: integration.verificationState.dkim,
    dmarc: integration.verificationState.dmarc,
    from: integration.verificationState.from,
    replyTo: integration.verificationState.replyTo,
    deliveredAuthCapturedAt: integration.verificationState.deliveredAuthCapturedAt,
  };

  const activeIntegration = await store.saveIntegration({
    ...integration,
    verificationState: mergedState,
    status: MAILBOX_STATUS.ACTIVE,
  });
  const activeIdentity = await store.saveIdentity({
    ...identity,
    status: IDENTITY_STATUS.ACTIVE,
  });
  return {
    integration: publicIntegration(activeIntegration),
    identity: publicIdentity(activeIdentity),
    status: 'ACTIVE',
  };
}

async function diagnoseOAuth(env = process.env) {
  const cfg = studioSubstralMailboxConfig(TENANT_ID);
  const secretRef = cfg.integration.oauthRefreshSecretRef;
  let refreshToken = null;
  let secretResolved = false;
  try {
    refreshToken = resolveSecretRef(secretRef, { env });
    secretResolved = Boolean(refreshToken);
  } catch (err) {
    secretResolved = false;
  }

  const anchorToken = cleanEnv(env.ANCHOR_GOOGLE_REFRESH_TOKEN);
  const substralToken = cleanEnv(refreshToken);
  const anchorPrefix = anchorToken
    ? crypto.createHash('sha256').update(anchorToken).digest('hex').slice(0, 12)
    : null;
  const substralPrefix = substralToken
    ? crypto.createHash('sha256').update(substralToken).digest('hex').slice(0, 12)
    : null;

  const diagnostic = await diagnoseGoogleMailboxOAuth({
    refreshToken: substralToken,
    env,
    expectedMailbox: CANONICAL_SENDER,
  });

  return {
    integrationId: cfg.integration.id,
    oauthRefreshSecretRef: secretRef,
    secretRefConfigured: secretResolved,
    railwayEnvChecks: {
      STUDIO_SUBSTRAL_GOOGLE_REFRESH_TOKEN: Boolean(cleanEnv(env.STUDIO_SUBSTRAL_GOOGLE_REFRESH_TOKEN)),
      GOOGLE_CLIENT_ID: Boolean(cleanEnv(env.GOOGLE_CLIENT_ID)),
      GOOGLE_CLIENT_SECRET: Boolean(cleanEnv(env.GOOGLE_CLIENT_SECRET)),
    },
    anchorTokenReuseDetected: Boolean(
      anchorPrefix && substralPrefix && anchorPrefix === substralPrefix
    ),
    ...diagnostic,
    remediation: buildOAuthRemediation(diagnostic),
  };
}

function cleanEnv(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function buildOAuthRemediation(diagnostic) {
  if (diagnostic.refresh?.ok) {
    if (diagnostic.mailboxMatchesExpected === false) {
      return 'Regenerate refresh token signed in as hello@studiosubstral.com (wrong Google account).';
    }
    if (diagnostic.mailScopeGranted === false) {
      return 'Regenerate refresh token with scope https://mail.google.com/ (prompt=consent).';
    }
    return 'OAuth refresh OK — rerun verify and activate.';
  }
  const err = diagnostic.refresh?.error;
  if (err === 'invalid_grant' || /revoked|expired/i.test(String(diagnostic.refresh?.error_description || ''))) {
    return 'Regenerate STUDIO_SUBSTRAL_GOOGLE_REFRESH_TOKEN with node getStudioSubstralMailboxToken.js using the same GOOGLE_CLIENT_ID/SECRET as Railway, then rerun verify.';
  }
  if (err === 'invalid_client') {
    return 'Align GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET on Railway with the OAuth client used to mint the refresh token.';
  }
  if (diagnostic.refresh?.code === 'google_oauth_refresh_missing' || !diagnostic.secretPresent) {
    return 'Set STUDIO_SUBSTRAL_GOOGLE_REFRESH_TOKEN on the Railway service that runs substral:mailbox:verify.';
  }
  return 'Inspect refresh.error and refresh.error_description; regenerate token if credentials or scopes are wrong.';
}

async function emmettReadiness() {
  const { snapshot, assessment, envelope } = await produceTenantMailboxCapacityEnvelope(
    TENANT_ID,
    STUDIO_SUBSTRAL_MAILBOX.sendingIdentityId,
    { pool }
  );
  const auth = snapshot.authentication || {};
  const governor = envelope?.governor || assessment?.governor;
  const recommended = envelope?.recommended ?? assessment?.recommended;
  return {
    tenantId: TENANT_ID,
    sendingIdentityId: STUDIO_SUBSTRAL_MAILBOX.sendingIdentityId,
    authentication: {
      spf: auth.spf?.state || auth.spf,
      dkim: auth.dkim?.state || auth.dkim,
      dmarc: auth.dmarc?.state || auth.dmarc,
      provenance: auth.spf?.provenance || auth.provenance,
    },
    domainAge: {
      inboxAgeDays: snapshot.inboxAgeDays,
      inboxAgeSource: snapshot.inboxAgeSource,
    },
    senderReadiness: {
      senderEmail: snapshot.senderEmail,
      mailboxStatus: snapshot.mailboxStatus,
      identityStatus: snapshot.identityStatus,
    },
    mailboxHealth: assessment?.health || envelope?.health,
    recommendedSafeDailyCapacity: recommended,
    limitingFactor: envelope?.decisiveReasoning?.limitingFactor || assessment?.limitingFactor,
    governor,
    capacityStatement: envelope?.statement,
    pass: governor?.outcome !== 'BLOCK' && Number.isFinite(Number(recommended)),
  };
}

async function createGovernedProgram(actorId = 'substral-mailbox-activation') {
  const { missionId } = await ensureStudioSubstralMission(pool);
  if (!missionId) throw Object.assign(new Error('Studio Substral mission missing.'), { code: 'mission_missing' });

  const emmett = await emmettReadiness();
  if (!emmett.pass) {
    throw Object.assign(new Error('Emmett readiness did not pass.'), { code: 'emmett_readiness_failed', detail: emmett });
  }
  const dailyCap = Math.max(1, Number(emmett.recommendedSafeDailyCapacity || 1));
  const expiresAt = new Date(Date.now() + 30 * 86400000).toISOString();
  const svc = productionService({
    pool,
    tenantId: TENANT_ID,
    adapters: adapters(pool, { tenantId: TENANT_ID }),
  });
  const actor = { id: actorId, role: 'admin' };
  const input = {
    tenantId: TENANT_ID,
    sourceMissionId: missionId,
    senderEmail: STUDIO_SUBSTRAL_MAILBOX.senderEmail,
    inboxIntegrationId: STUDIO_SUBSTRAL_MAILBOX.inboxIntegrationId,
    sendingIdentityId: STUDIO_SUBSTRAL_MAILBOX.sendingIdentityId,
    dailyCap,
    totalCap: Math.max(dailyCap * 14, 14),
    spacingMinutes: 240,
    startHour: 9,
    endHour: 17,
    expiresAt,
  };
  const review = await svc.authorize(input, actor);
  if (review.reviewRequired) {
    const program = await svc.authorize({ ...review.policy, reviewHash: review.reviewHash }, actor);
    await svc.setMode(program.id, 'shadow', program.policy_hash, actor);
    return { programId: program.id, mode: 'shadow', dailyCap, emmett, note: 'Governed program created in shadow mode. Sending remains disabled until SUBSTRAL_GOVERNED_OUTBOUND_ENABLED=true and operator activates the program.' };
  }
  return { program: review, emmett };
}

async function main() {
  const args = parseArgs();
  const command = args._[0] || 'help';
  const store = new PostgresTenantMailboxStore(pool);

  if (command === 'help' || args.help) {
    process.stdout.write([
      'Studio Substral mailbox activation (tenant 17, hello@studiosubstral.com)',
      '',
      '  configure',
      '  verify [--configure]',
      '  capture-delivered-auth --headers-file=<path>  (Authentication-Results from delivered test mail)',
      `  activate --confirm=${ACTIVATION_CONFIRM}`,
      '  diagnose-oauth   (safe Google refresh audit — no secrets logged)',
      '  emmett-readiness',
      `  create-governed-program --confirm=${ACTIVATION_CONFIRM}`,
      '',
      'Env: STUDIO_SUBSTRAL_GOOGLE_REFRESH_TOKEN, GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, DATABASE_URL',
      'Does not enable SUBSTRAL_GOVERNED_OUTBOUND_ENABLED or send outreach.',
    ].join('\n') + '\n');
    return;
  }

  if (command === 'configure') {
    printJson(await configure(store, args));
    return;
  }

  if (command === 'verify') {
    if (args.configure === true) await configure(store, args);
    const result = await verifyTenantMailbox({
      tenantId: TENANT_ID,
      integrationId: STUDIO_SUBSTRAL_MAILBOX.inboxIntegrationId,
    }, { store });
    printJson(result);
    if (result.verificationState?.imap?.status !== 'verified' || result.verificationState?.smtp?.status !== 'verified') {
      process.exitCode = 1;
    }
    return;
  }

  if (command === 'capture-delivered-auth') {
    const raw = readHeadersFile(args);
    printJson(await mergeDeliveredAuth(store, raw));
    return;
  }

  if (command === 'activate') {
    if (args.confirm !== ACTIVATION_CONFIRM) {
      throw Object.assign(new Error(`Refusing without --confirm=${ACTIVATION_CONFIRM}`), { code: 'confirm_required' });
    }
    if (args.configure === true) await configure(store, args);
    printJson(await activateMailbox(store));
    return;
  }

  if (command === 'diagnose-oauth') {
    const report = await diagnoseOAuth(process.env);
    printJson(report);
    if (!report.refresh?.ok) process.exitCode = 1;
    return;
  }

  if (command === 'emmett-readiness') {
    const report = await emmettReadiness();
    printJson(report);
    if (!report.pass) process.exitCode = 1;
    return;
  }

  if (command === 'create-governed-program') {
    if (args.confirm !== ACTIVATION_CONFIRM) {
      throw Object.assign(new Error(`Refusing without --confirm=${ACTIVATION_CONFIRM}`), { code: 'confirm_required' });
    }
    printJson(await createGovernedProgram());
    return;
  }

  throw Object.assign(new Error(`Unknown command: ${command}`), { code: 'unknown_command' });
}

if (require.main === module) {
  main()
    .then(() => pool.end())
    .catch((err) => {
      console.error(JSON.stringify({ error: err.code || err.message, detail: err.detail || null }));
      process.exit(1);
    });
}

module.exports = {
  configure,
  mergeDeliveredAuth,
  activateMailbox,
  diagnoseOAuth,
  emmettReadiness,
  createGovernedProgram,
};
