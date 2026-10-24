'use strict';

function buildDownstreamEffects({ prospect, expectations = [], aoName = null }) {
  const effects = {
    crm: [],
    briefing: [],
    outbound: [],
    follow_up: [],
    max: [],
  };
  if (!prospect) return effects;

  const awaiting = expectations.find(e =>
    e.prospect_id === prospect.id && ['OPEN', 'WAITING'].includes(e.status)
  );

  if (awaiting?.expectation_type === 'inbound_call') {
    effects.crm.push('Awaiting inbound call this week.');
    effects.briefing.push(
      `${prospect.company_name || 'Account'} — Existing relationship. Expecting inbound call this week. No action today.`
    );
    effects.outbound.push('Do not treat account as untouched cold inventory.');
    effects.follow_up.push('If expected window expires without evidence, prompt AO follow-up.');
    effects.max.push('Keep expectation unresolved until closing evidence arrives.');
  } else if (prospect.relationship_active || prospect.acquisition_metadata?.maxStateIngestion?.relationship_active) {
    effects.outbound.push('Preserve active-relationship suppression for cold outreach.');
    effects.briefing.push(`${prospect.company_name || 'Account'} — Active relationship in AO pipeline.`);
  }

  if (aoName) {
    effects.briefing.unshift(`AO context: ${aoName}`);
  }

  return effects;
}

module.exports = {
  buildDownstreamEffects,
};
