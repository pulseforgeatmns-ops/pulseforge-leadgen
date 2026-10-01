'use strict';
const { fail } = require('../packages/acquisition-mission/DailyOutboundPolicy');

// There is intentionally no direct SMTP dependency in governed transport.
function createGovernedTenantMailboxSend(program = {}, binding = {}, dependencies = {}) {
  return async function sendEmail(command = {}) {
    const { envelope, item } = binding;
    if (!envelope || !item || !program.pool) fail('governed_schedule_binding_required');
    if (command.toEmail !== item.email || command.subject !== item.snapshot.message.subject
      || command.body !== item.snapshot.message.body) fail('provider_payload_changed');
    const bridge = dependencies.bridge || require('../services/governedTenantSchedule');
    const scheduler = dependencies.scheduler || require('../services/tenantOutreachScheduler');
    const asset = await bridge.createGovernedOutreachAsset(program, envelope, item, program.pool);
    const result = await scheduler.authorizeAndExecuteScheduledSend({
      tenantId: String(program.tenant_id), prospectId: item.prospect_id,
      outreachAssetId: asset.id, outreachAssetVersion: asset.version,
      sendingIdentityId: program.policy.sendingIdentityId, recipientEmail: item.email,
      missionId: envelope.mission_id, scheduledFor: new Date().toISOString(), timezone: program.policy.timeZone,
      subject: command.subject, body: command.body, sequenceStep: 1,
      authorizationSource: 'governed_outbound', authorizedBy: program.authorized_by,
      idempotencyKey: command.idempotencyKey,
      governed: { programId: program.id, policyHash: program.policy_hash, envelopeId: envelope.id,
        itemId: item.id, manifestHash: envelope.manifest_hash, approvalId: envelope.approval_id,
        revision: envelope.revision, mailboxIntegrationId: program.policy.inboxIntegrationId, outreachAssetId: asset.id },
    }, { pool: program.pool, providerBoundary: command.providerBoundary });
    if (!['sent', 'recovered_sent'].includes(result.result)) fail(result.reason || result.error?.code || 'governed_schedule_not_sent');
    const message = result.message;
    const messageId = message?.providerMessageId || message?.rfcMessageId;
    if (!messageId) fail('provider_acceptance_unknown');
    return { success: true, messageId, providerMessageId: messageId, scheduleId: result.schedule.id,
      canonicalMessageId: message.id, rfcMessageId: message.rfcMessageId, threadId: message.threadId };
  };
  sendEmail.isGovernedTenantMailboxTransport = true;
  return sendEmail;
}
module.exports = { createGovernedTenantMailboxSend };
