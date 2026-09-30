'use strict';

/**
 * Emmett / outbound readiness for Studio Substral — isolated sender context.
 * No implicit fallback to Anchor or Pulseforge identities.
 */

const {
  STUDIO_SUBSTRAL_DOMAIN,
  findStudioSubstralClient,
} = require('./studioSubstralTenant');

const CANONICAL_SENDER = 'hello@studiosubstral.com';

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

  let mailboxBound = false;
  try {
    const mailbox = await db.query(
      `SELECT id, status FROM tenant_mailbox_integrations
        WHERE tenant_id = $1 AND lower(mailbox_address) = lower($2)
        LIMIT 1`,
      [String(client.id), CANONICAL_SENDER]
    );
    mailboxBound = mailbox.rows[0]?.status === 'ACTIVE';
    if (!mailboxBound) reasons.push('mailbox_not_authenticated');
  } catch {
    reasons.push('mailbox_schema_unavailable');
  }

  const autosend = Boolean(client.autosend_enabled);
  if (autosend) reasons.push('autosend_enabled');

  return {
    ready: reasons.length === 0,
    reasons,
    sender,
    reply_mailbox: CANONICAL_SENDER,
    emmett_allowed: false,
    note: 'Outbound remains disabled until mailbox authentication and governed authorization are established.',
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
