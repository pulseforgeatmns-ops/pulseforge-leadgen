'use strict';

require('dotenv').config();

const {
  PostgresTenantMailboxStore,
  babrunMailboxConfig,
  publicIntegration,
  publicIdentity,
  sendTenantEmail,
  pollTenantMailbox,
  verifyTenantMailbox,
  MAILBOX_STATUS,
  IDENTITY_STATUS,
} = require('../services/tenantMailbox');

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

function requireArg(args, key) {
  if (args[key] == null || args[key] === '') {
    throw new Error(`Missing required --${key}`);
  }
  return args[key];
}

function printJson(value) {
  process.stdout.write(`${JSON.stringify(value, null, 2)}\n`);
}

async function getIntegrationAndIdentity(store, args) {
  const tenantId = requireArg(args, 'tenant-id');
  const integrationId = args['integration-id'];
  const identityId = args['sending-identity-id'];
  if (integrationId || identityId) return { tenantId, integrationId, identityId };

  const integrations = await store.listIntegrations(tenantId);
  if (integrations.length !== 1) {
    throw new Error(`Expected exactly one mailbox integration for tenant ${tenantId}; found ${integrations.length}. Pass --integration-id.`);
  }
  return { tenantId, integrationId: integrations[0].id, identityId };
}

async function maybeConfigureBabrun(store, args) {
  if (args['configure-babrun'] !== true) return null;
  const tenantId = requireArg(args, 'tenant-id');
  const cfg = babrunMailboxConfig(tenantId);
  const integration = await store.saveIntegration({
    ...cfg.integration,
    status: args.active === true ? MAILBOX_STATUS.ACTIVE : cfg.integration.status,
  });
  const identity = await store.saveIdentity({
    ...cfg.identity,
    mailboxIntegrationId: integration.id,
    status: args.active === true ? IDENTITY_STATUS.ACTIVE : cfg.identity.status,
  });
  return { integration: publicIntegration(integration), identity: publicIdentity(identity) };
}

async function main() {
  const args = parseArgs();
  const command = args._[0] || 'help';
  const store = new PostgresTenantMailboxStore();

  if (command === 'help' || args.help) {
    process.stdout.write([
      'Usage:',
      '  npm run acquisition:mailbox:verify -- --tenant-id=<id> --integration-id=<id>',
      '  npm run acquisition:mailbox:test-send -- --tenant-id=<id> --sending-identity-id=<id> --to=<email>',
      '  npm run acquisition:mailbox:poll -- --tenant-id=<id> --integration-id=<id> --once',
      '',
      'Optional:',
      '  --configure-babrun writes only Babrun non-secret mailbox config for the supplied authoritative tenant id.',
    ].join('\n') + '\n');
    return;
  }

  if (command === 'verify') {
    const configured = await maybeConfigureBabrun(store, args);
    const { tenantId, integrationId } = await getIntegrationAndIdentity(store, args);
    const result = await verifyTenantMailbox({ tenantId, integrationId }, { store });
    printJson({ configured, ...result });
    return;
  }

  if (command === 'test-send') {
    await maybeConfigureBabrun(store, args);
    const tenantId = requireArg(args, 'tenant-id');
    const sendingIdentityId = requireArg(args, 'sending-identity-id');
    const to = requireArg(args, 'to');
    const subject = args.subject || 'PulseForge mailbox verification';
    const body = args.body || 'This is a safe tenant mailbox verification send.';
    const result = await sendTenantEmail({
      tenantId,
      sendingIdentityId,
      missionId: args['mission-id'] || 'mailbox-verification',
      prospectId: args['prospect-id'] || 'safe-test',
      outreachAssetId: args['outreach-asset-id'] || 'mailbox-test-send',
      to,
      subject,
      body,
      metadata: { operatorCommand: 'acquisition:mailbox:test-send' },
    }, { store });
    printJson({ sent: result.sent, duplicate: result.duplicate, message: result.message, thread: result.thread });
    return;
  }

  if (command === 'poll') {
    const { tenantId, integrationId } = await getIntegrationAndIdentity(store, args);
    const result = await pollTenantMailbox({ tenantId, integrationId, once: args.once === true }, { store });
    printJson(result);
    return;
  }

  throw new Error(`Unknown acquisition mailbox command: ${command}`);
}

main().catch((err) => {
  process.stderr.write(`${err.code || 'mailbox_command_failed'}: ${err.message}\n`);
  process.exit(1);
});
