'use strict';

/**
 * Emmett / outbound readiness for Studio Substral — isolated sender context.
 * No implicit fallback to Anchor or Pulseforge identities.
 */

const {
  STUDIO_SUBSTRAL_DOMAIN,
  findStudioSubstralClient,
} = require('./studioSubstralTenant');
const { deliveredAuthenticationPasses } = require('./mailAuthenticationResults');
const { governedOutboundEnabledForTenant } = require('../services/governedOutboundTenant');

const CANONICAL_SENDER = 'hello@studiosubstral.com';
const STUDIO_SUBSTRAL_TENANT_ID = '17';

function asJson(value) {
  if (value == null) return {};
  if (typeof value === 'object') return value;
  try {
    return JSON.parse(value);
  } catch {
    return {};
  }
}

async function evaluateStudioSubstralOutboundReadiness(db, clientId) {
  const client = await findStudioSubstralClient(db);
  const reasons = [];
  if (!client || Number(client.id) !== Number(clientId)) {
    return { ready: false, reasons: ['tenant_mismatch'] };
  }

  const sender = String(client.sender_email || '').trim().toLowerCase();
  const domain = String(client.sending_domain || '').trim().toLowerCase();
  if (sender !== CANONICAL_SENDER) reasons.push('sender_not_canonical');
  if (domain !== STUDIO_SUBSTRAL_DOMAIN) reasons.push('sending_domain_not_canonical');

  const agents = client.enabled_agents || [];
  if (agents.includes('emmett')) reasons.push('emmett_still_listed_in_enabled_agents');

  let mailboxRow = null;
  try {
    const mailbox = await db.query(
      `SELECT id, status, verification_state FROM tenant_mailbox_integrations
        WHERE tenant_id = $1 AND lower(mailbox_address) = lower($2)
        LIMIT 1`,
      [String(client.id), CANONICAL_SENDER]
    );
    mailboxRow = mailbox.rows[0] || null;
    const status = String(mailboxRow?.status || '').toLowerCase();
    if (status !== 'active') reasons.push('mailbox_not_authenticated');
    else if (!deliveredAuthenticationPasses(asJson(mailboxRow.verification_state))) {
      reasons.push('authentication_not_verified_from_delivery');
    }
  } catch {
    reasons.push('mailbox_schema_unavailable');
  }

  let governedProgram = null;
  try {
    const program = await db.query(
      `SELECT id, mode FROM acquisition_outbound_programs
        WHERE tenant_id = $1 AND mode <> 'revoked'
        ORDER BY authorized_at DESC LIMIT 1`,
      [STUDIO_SUBSTRAL_TENANT_ID]
    );
    governedProgram = program.rows[0] || null;
    if (!governedProgram) reasons.push('governed_outbound_program_missing');
  } catch {
    reasons.push('governed_outbound_schema_unavailable');
  }

  if (!governedOutboundEnabledForTenant(STUDIO_SUBSTRAL_TENANT_ID)) {
    reasons.push('governed_outbound_not_authorized');
  }

  const autosend = Boolean(client.autosend_enabled);
  if (autosend) reasons.push('autosend_enabled');

  const emmettAllowed = reasons.length === 0 && governedOutboundEnabledForTenant(STUDIO_SUBSTRAL_TENANT_ID);

  return {
    ready: emmettAllowed,
    reasons,
    sender,
    reply_mailbox: CANONICAL_SENDER,
    emmett_allowed: emmettAllowed,
    mailbox_integration_id: mailboxRow?.id || null,
    governed_program_id: governedProgram?.id || null,
    note: 'Outbound remains disabled until mailbox authentication, Emmett readiness, and explicit governed authorization are established.',
  };
}

function assertStudioSubstralOutboundNotImplicit(clientConfig) {
  if (!clientConfig) return;
  if (clientConfig.scoring_profile !== 'studio_substral') return;
  const sender = String(clientConfig.sender_email || '').toLowerCase();
  if (sender.includes('goanchorcleaning.com') || sender.includes('gopulseforge.com')) {
    throw new Error('Studio Substral outbound cannot reuse Anchor or Pulseforge sender identity.');
  }
}

module.exports = {
  CANONICAL_SENDER,
  evaluateStudioSubstralOutboundReadiness,
  assertStudioSubstralOutboundNotImplicit,
};
