'use strict';

const { sendTenantEmail } = require('../services/tenantMailbox');

/**
 * Wrap tenant SMTP send for canonical EXECUTE_OUTBOUND (same guarded beforeAttempt contract as Brevo).
 */
function createGovernedTenantMailboxSend(program = {}) {
  const tenantId = String(program.tenant_id || program.policy?.tenantId || '');
  const sendingIdentityId = program.policy?.sendingIdentityId || null;
  const mailboxIntegrationId = program.policy?.inboxIntegrationId || null;

  async function sendEmail(command = {}) {
    if (typeof sendEmail.beforeAttempt === 'function') {
      await sendEmail.beforeAttempt(command);
    }
    const result = await sendTenantEmail({
      tenantId,
      sendingIdentityId,
      to: command.toEmail,
      subject: command.subject,
      body: command.body,
      prospectId: command.prospectId,
      metadata: {
        governedProgramId: program.id,
        idempotencyKey: command.idempotencyKey,
      },
    }, { pool: program.pool });
    const messageId = result?.providerResult?.messageId || result?.message?.providerMessageId;
    return {
      success: true,
      messageId,
      providerMessageId: messageId,
    };
  }

  sendEmail.beforeAttempt = null;
  return sendEmail;
}

module.exports = { createGovernedTenantMailboxSend };
