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
 *   node scripts/studioSubstralMailboxActivation.js review-governed-program --policy-file=/path/to/policy.json
 *   node scripts/studioSubstralMailboxActivation.js create-governed-program --policy-file=/path/to/policy.json --review-hash=<reviewed-hash> --confirm=substral-mailbox-2026-09-30
 */

require('dotenv').config({ quiet: true });

const fs = require('fs');
const pool = require('../db');
const {
  PostgresTenantMailboxStore,
  studioSubstralMailboxConfig,
  publicIntegration,
  publicIdentity,
  createSmtpTransport,
  resolveImapAuth,
  resolveSmtpAuth,
  verifyTenantMailbox,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
} = require('../services/tenantMailbox');
const {
  verificationStateFromDeliveredHeaders,
  deliveredAuthenticationPasses,
  parseDeliveredAuthenticationEvidence,
} = require('../utils/mailAuthenticationResults');
const { parseRawMailHeaders } = require('../utils/mailHeaders');
const { CANONICAL_SENDER } = require('../utils/studioSubstralOutboundGovernance');
const {
  TENANT_ID,
  STUDIO_SUBSTRAL_MAILBOX,
} = require('./lib/studioSubstralCanonicalOutbound');
const { reviewStudioSubstralProgram } = require('./lib/studioSubstralProgramReview');
const { produceTenantMailboxCapacityEnvelope } = require('../services/emmettTenantMailboxCapacity');
const { productionService } = require('../services/governedOutbound');

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

async function loadDeliveredSelfTest(integration, subject, opts = {}) {
  if (opts.loadDeliveredMessage) return opts.loadDeliveredMessage({ integration, subject });
  const { ImapFlow } = require('imapflow');
  const imapAuth = await resolveImapAuth(integration, opts);
  const client = new ImapFlow({
    host: integration.imapHost,
    port: Number(integration.imapPort || 993),
    secure: true,
    auth: imapAuth.auth,
    logger: false,
  });
  await client.connect();
  try {
    const mailboxes = await client.list();
    const allMail = mailboxes.find((mailbox) => mailbox.specialUse === '\\All');
    await client.mailboxOpen(allMail?.path || 'INBOX');
    for (let attempt = 0; attempt < 12; attempt += 1) {
      const matches = await client.search({ subject }, { uid: true });
      if (Array.isArray(matches) && matches.length) {
        const message = await client.fetchOne(matches[matches.length - 1], { source: true }, { uid: true });
        if (message?.source) return message.source.toString('utf8');
      }
      await new Promise((resolve) => setTimeout(resolve, 2500));
    }
    throw Object.assign(new Error('Timed out waiting for the Studio Substral self-test message.'), {
      code: 'delivered_auth_message_timeout',
    });
  } finally {
    await client.logout().catch(() => {});
  }
}

async function captureSelfTestAuth(store, opts = {}) {
  const integration = await store.getIntegration(TENANT_ID, STUDIO_SUBSTRAL_MAILBOX.inboxIntegrationId);
  if (!integration) {
    throw Object.assign(new Error('Configure mailbox integration first.'), { code: 'integration_missing' });
  }
  const marker = `substral-auth-${Date.now()}`;
  const subject = `[Studio Substral authentication check] ${marker}`;
  const smtpAuth = await resolveSmtpAuth(integration, opts);
  const transport = opts.transport || createSmtpTransport(integration, smtpAuth, opts);
  try {
    await transport.sendMail({
      from: `"Studio Substral" <${CANONICAL_SENDER}>`,
      replyTo: CANONICAL_SENDER,
      to: CANONICAL_SENDER,
      subject,
      text: 'Automated mailbox authentication check for Studio Substral. No reply is needed.',
    });
  } finally {
    transport.close?.();
  }
  const rawMessage = await loadDeliveredSelfTest(integration, subject, opts);
  const result = await mergeDeliveredAuth(store, rawMessage);
  return {
    ...result,
    selfTest: {
      recipient: CANONICAL_SENDER,
      subject,
      externalProspectContacted: false,
    },
  };
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

async function emmettReadiness(opts = {}) {
  const { snapshot, assessment, envelope } = await (opts.produceCapacity || produceTenantMailboxCapacityEnvelope)(
    TENANT_ID,
    STUDIO_SUBSTRAL_MAILBOX.sendingIdentityId,
    { pool: opts.pool || pool }
  );
  const auth = snapshot.authentication || {};
  const governor = assessment?.governor;
  // Use the governor-adjusted envelope, not the raw recommendation. The producer
  // exposes maxSendsPerDay; neither envelope nor assessment has .recommended.
  const recommended = envelope?.maxSendsPerDay ?? assessment?.maxSendsPerDay;
  const allowedOutcomes = ['proceed', 'slow'];
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
    pass: governor?.halt !== true && allowedOutcomes.includes(governor?.outcome)
      && allowedOutcomes.includes(envelope?.governorState ?? governor?.outcome)
      && Number.isFinite(Number(recommended)) && Number(recommended) > 0,
  };
}

async function createGovernedProgram(input = {}, actorId = 'substral-mailbox-activation', opts = {}) {
  const db = opts.pool || pool;
  const now = opts.now || new Date();
  const review = await reviewStudioSubstralProgram(db, input, now);
  if (!input.reviewHash || input.reviewHash !== review.reviewHash) {
    throw Object.assign(new Error('Review the current policy and source mission before creating a shadow program.'), {
      code: 'program_review_required', detail: review,
    });
  }
  const existing = await db.query(
    "SELECT id, mode FROM acquisition_outbound_programs WHERE tenant_id=$1 AND mode <> 'revoked' LIMIT 1",
    [TENANT_ID]
  );
  if (existing.rows.length) {
    throw Object.assign(new Error('An existing governed program must be reviewed separately; activation does not replace it.'), {
      code: 'governed_program_exists', detail: existing.rows[0],
    });
  }
  const emmett = await (opts.emmettReadiness || emmettReadiness)({ pool: db });
  if (!emmett.pass || !Number.isFinite(Number(emmett.recommendedSafeDailyCapacity))
    || Number(emmett.recommendedSafeDailyCapacity) <= 0) {
    throw Object.assign(new Error('Emmett readiness did not pass.'), { code: 'emmett_readiness_failed', detail: emmett });
  }
  const svc = (opts.productionService || productionService)(db, { tenantId: TENANT_ID });
  const actor = { id: actorId, role: 'admin' };
  const program = await svc.authorize({ ...review.policy, reviewHash: input.reviewHash }, actor);
  if (program.reviewRequired) {
    throw Object.assign(new Error('The source mission changed after review.'), { code: 'program_review_required', detail: program });
  }
  // The governed store creates programs in shadow mode. Never call setMode here:
  // an existing active grant must not be silently changed on a repeated command.
  if (program.mode !== 'shadow') {
    throw Object.assign(new Error('Unexpected program mode; no mode changes were made.'), { code: 'program_not_shadow' });
  }
  return { programId: program.id, mode: program.mode, policy: program.policy, emmett,
    note: 'Shadow program created. Live activation and sending still require separate operator authorization.' };
}

function readProgramPolicy(args) {
  if (typeof args['policy-file'] !== 'string' || !args['policy-file'].trim()) {
    throw Object.assign(new Error('Provide --policy-file with an explicit source mission and bounded policy.'), { code: 'policy_file_required' });
  }
  const input = JSON.parse(fs.readFileSync(args['policy-file'], 'utf8'));
  if (!input || typeof input !== 'object' || Array.isArray(input)) {
    throw Object.assign(new Error('Policy file must contain a JSON object.'), { code: 'invalid_policy_file' });
  }
  return { ...input, reviewHash: args['review-hash'] };
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
      `  capture-self-test-auth --confirm=${ACTIVATION_CONFIRM}  (sends only to hello@studiosubstral.com)`,
      `  activate --confirm=${ACTIVATION_CONFIRM}`,
      '  emmett-readiness',
      '  review-governed-program --policy-file=<path>  (read-only; no bootstrap or sends)',
      `  create-governed-program --policy-file=<path> --review-hash=<hash> --confirm=${ACTIVATION_CONFIRM}`,
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

  if (command === 'capture-self-test-auth') {
    if (args.confirm !== ACTIVATION_CONFIRM) {
      throw Object.assign(new Error(`Refusing without --confirm=${ACTIVATION_CONFIRM}`), { code: 'confirm_required' });
    }
    printJson(await captureSelfTestAuth(store));
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
    printJson(await createGovernedProgram(readProgramPolicy(args)));
    return;
  }

  if (command === 'review-governed-program') {
    printJson(await reviewStudioSubstralProgram(pool, readProgramPolicy(args)));
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
  captureSelfTestAuth,
  activateMailbox,
  emmettReadiness,
  createGovernedProgram,
  readProgramPolicy,
};
