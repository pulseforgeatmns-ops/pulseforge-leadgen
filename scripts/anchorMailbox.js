'use strict';

/**
 * Anchor Cleaning (tenant 10) Google Workspace reply mailbox — configure, verify, poll.
 * Outbound remains on Brevo; this integration is IMAP reply ingestion only.
 */

require('dotenv').config({ quiet: true });

const {
  PostgresTenantMailboxStore,
  anchorMailboxConfig,
  publicIntegration,
  publicIdentity,
  verifyTenantMailbox,
  pollTenantMailbox,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
  AUTH_MODES,
} = require('../services/tenantMailbox');

const ANCHOR_TENANT_ID = '10';
const APPLY_CONFIRMATION = 'anchor-mailbox-oauth-2026-09-19';

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

async function configureAnchor(store, args) {
  const cfg = anchorMailboxConfig(ANCHOR_TENANT_ID);
  const integration = await store.saveIntegration({
    ...cfg.integration,
    status: args.active === true ? MAILBOX_STATUS.ACTIVE : cfg.integration.status,
  });
  const identity = await store.saveIdentity({
    ...cfg.identity,
    mailboxIntegrationId: integration.id,
    status: args.active === true ? IDENTITY_STATUS.ACTIVE : cfg.identity.status,
  });
  return {
    integration: publicIntegration(integration),
    identity: publicIdentity(identity),
    imapAuthMode: AUTH_MODES.GOOGLE_OAUTH2,
    oauthRefreshSecretRef: 'ANCHOR_GOOGLE_REFRESH_TOKEN',
  };
}

async function main() {
  const args = parseArgs();
  const command = args._[0] || 'help';
  const store = new PostgresTenantMailboxStore();

  if (command === 'help' || args.help) {
    process.stdout.write([
      'Anchor mailbox (Google OAuth IMAP reply ingestion)',
      '',
      'Usage:',
      '  node scripts/anchorMailbox.js configure',
      '  node scripts/anchorMailbox.js verify',
      '  node scripts/anchorMailbox.js poll',
      '  node scripts/anchorMailbox.js apply --confirm=anchor-mailbox-oauth-2026-09-19',
      '',
      'Required env (production):',
      '  GOOGLE_CLIENT_ID',
      '  GOOGLE_CLIENT_SECRET',
      '  ANCHOR_GOOGLE_REFRESH_TOKEN',
      '',
      'Obtain refresh token locally:',
      '  node getAnchorMailboxToken.js',
      '',
      'Does not send email. Does not authorize governed outbound.',
    ].join('\n') + '\n');
    return;
  }

  if (command === 'configure') {
    printJson(await configureAnchor(store, args));
    return;
  }

  const integrationId = args['integration-id'] || 'tmi_10_anchor_jacob';

  if (command === 'verify') {
    if (args.configure === true) {
      await configureAnchor(store, args);
    }
    const result = await verifyTenantMailbox({
      tenantId: ANCHOR_TENANT_ID,
      integrationId,
    }, { store });
    printJson(result);
    if (result.verificationState?.imap?.status !== 'verified') {
      process.exitCode = 1;
    }
    return;
  }

  if (command === 'poll') {
    const result = await pollTenantMailbox({
      tenantId: ANCHOR_TENANT_ID,
      integrationId,
    }, { store });
    printJson(result);
    return;
  }

  if (command === 'apply') {
    if (args.confirm !== APPLY_CONFIRMATION) {
      throw new Error(`apply requires --confirm=${APPLY_CONFIRMATION}`);
    }
    const configured = await configureAnchor(store, args);
    const verified = await verifyTenantMailbox({
      tenantId: ANCHOR_TENANT_ID,
      integrationId: configured.integration.id,
    }, { store });
    printJson({ configured, verified });
    if (verified.verificationState?.imap?.status !== 'verified') {
      process.exitCode = 1;
    }
    return;
  }

  throw new Error(`Unknown anchor mailbox command: ${command}`);
}

main().catch((err) => {
  process.stderr.write(`${err.code || 'anchor_mailbox_failed'}: ${err.message}\n`);
  process.exit(1);
});
