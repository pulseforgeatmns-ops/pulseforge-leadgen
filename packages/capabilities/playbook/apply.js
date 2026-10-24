'use strict';

/**
 * Apply Client Playbook strategy to downstream capability outputs (SPEC-028 / ADR-015).
 * Capabilities execute strategy — they do not invent channel/sequence defaults when a playbook is present.
 */

const { brandVoiceLabel, buildClientPlaybook } = require('./types');

const CHANNEL_LABELS = Object.freeze({
  direct_mail: 'Direct Mail',
  phone: 'Phone',
  email: 'Email',
  linkedin: 'LinkedIn',
  sms: 'SMS',
  letter: 'Letter',
});

/**
 * Resolve playbook from capability context (constraints / inputs / prior).
 * @param {object} context
 * @returns {object|null}
 */
function resolvePlaybookFromContext(context = {}) {
  const inputs = context.inputs || {};
  const constraints = context.constraints || {};
  const prior = inputs.priorOutputs || {};
  const raw =
    inputs.clientPlaybook ||
    prior.clientPlaybook ||
    constraints.clientPlaybook ||
    null;
  if (!raw || typeof raw !== 'object') return null;
  return buildClientPlaybook(raw);
}

/**
 * Build campaign strategy block from playbook.
 * @param {object} playbook
 * @returns {object}
 */
function campaignStrategyFromPlaybook(playbook) {
  if (!playbook) return null;
  const pb = buildClientPlaybook(playbook);
  return {
    playbookId: pb.id,
    playbookVersion: pb.version,
    playbookName: pb.name,
    brandVoice: pb.brandVoice,
    brandVoiceLabel: brandVoiceLabel(pb.brandVoice),
    preferredChannels: pb.preferredChannels.map(channelLabel),
    preferredChannelIds: [...pb.preferredChannels],
    outreachSequence: pb.outreachSequence.map((step) => ({
      day: step.day,
      channel: step.channel,
      channelLabel: channelLabel(step.channel),
      action: step.action,
      notes: step.notes || '',
    })),
    offers: [...pb.offers],
    constraints: pb.constraints.map((c) => ({
      type: c.type,
      rule: c.rule,
      detail: c.detail || '',
    })),
    valuePropositions: [...pb.valuePropositions],
    targetMarkets: [...pb.targetMarkets],
    successMetrics: [...pb.successMetrics],
    notes: pb.notes || '',
  };
}

/**
 * Filter / annotate prospects using playbook constraints (explainable, non-destructive when thin).
 * @param {object[]} prospects
 * @param {object} playbook
 * @returns {{ prospects: object[], excluded: object[], warnings: string[] }}
 */
function applyPlaybookConstraints(prospects, playbook) {
  const list = Array.isArray(prospects) ? prospects : [];
  if (!playbook) {
    return { prospects: list, excluded: [], warnings: [] };
  }
  const pb = buildClientPlaybook(playbook);
  const avoid = new Set(
    (pb.idealCustomer.industriesToAvoid || []).map((s) => s.toLowerCase())
  );
  const excludeIndustry = pb.constraints.some(
    (c) => c.type === 'exclude_industry' || /avoid restaurants/i.test(c.rule)
  );
  if (excludeIndustry) avoid.add('restaurant');
  if (excludeIndustry) avoid.add('restaurants');

  const excluded = [];
  const kept = [];
  const warnings = [];

  for (const p of list) {
    const industry = String(p.industry || p.vertical || '').toLowerCase();
    let blocked = null;
    for (const term of avoid) {
      if (term && industry.includes(term)) {
        blocked = {
          ...p,
          exclusionReason: `Playbook constraint: avoid ${term}`,
          playbookId: pb.id,
          playbookVersion: pb.version,
        };
        break;
      }
    }
    if (blocked) excluded.push(blocked);
    else kept.push(annotateProspectWithPlaybook(p, pb));
  }

  const callWindow = pb.constraints.find((c) => c.type === 'call_window');
  if (callWindow) {
    warnings.push(`Call window: ${callWindow.rule}`);
  }
  const focus = pb.constraints.find((c) => c.type === 'focus');
  if (focus) warnings.push(`Focus: ${focus.rule}`);
  const crm = pb.constraints.find((c) => c.type === 'exclude_crm');
  if (crm) warnings.push(`CRM: ${crm.rule}`);

  return { prospects: kept, excluded, warnings };
}

/**
 * @param {object} prospect
 * @param {object} playbook
 */
function annotateProspectWithPlaybook(prospect, playbook) {
  const pb = buildClientPlaybook(playbook);
  const primaryOffer = pb.offers[0] || null;
  const opener = buildOpener(prospect, pb);
  return {
    ...prospect,
    playbookId: pb.id,
    playbookVersion: pb.version,
    personalizationSentence: opener.sentence,
    openingHook: opener.hook,
    recommendedOffer: primaryOffer,
    recommendedChannel: pb.preferredChannels[0]
      ? channelLabel(pb.preferredChannels[0])
      : null,
  };
}

/**
 * @param {object} prospect
 * @param {object} playbook
 */
function buildOpener(prospect, playbook) {
  const pb = buildClientPlaybook(playbook);
  const name = prospect.companyName || 'your team';
  const voice = pb.brandVoice;
  const value = pb.valuePropositions[0] || 'reliable commercial service';
  const offer = pb.offers[0] || 'a brief conversation';
  const market =
    (prospect.industry && String(prospect.industry)) ||
    (pb.targetMarkets[0] && String(pb.targetMarkets[0])) ||
    'commercial facilities';

  let sentence;
  let hook;
  if (voice === 'direct') {
    sentence = `${name} — ${value}. We can schedule ${offer.toLowerCase()}.`;
    hook = `Worth 10 minutes on ${offer.toLowerCase()}?`;
  } else if (voice === 'friendly') {
    sentence = `Noticed ${name} serves ${market} clients — ${value} is how we support offices like yours.`;
    hook = `Open to ${offer.toLowerCase()} this month?`;
  } else if (voice === 'premium' || voice === 'professional') {
    sentence = `For ${name}, we emphasize ${value} for ${market} — aligned with how you already operate.`;
    hook = `Would ${offer.toLowerCase()} be useful?`;
  } else if (voice === 'technical') {
    sentence = `${name}: recurring facility coverage scoped to ${market}, measured against ${pb.successMetrics[0] || 'walkthroughs booked'}.`;
    hook = `Can we scope ${offer.toLowerCase()}?`;
  } else {
    // relationship_first (default consultative)
    sentence = `Reached out because ${name} looks like a strong fit for ${value.toLowerCase()} in ${market}.`;
    hook = `Happy to offer ${offer.toLowerCase()} with no obligation.`;
  }

  return { sentence, hook };
}

function channelLabel(channel) {
  const key = String(channel || '')
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
  if (CHANNEL_LABELS[key]) return CHANNEL_LABELS[key];
  if (!key) return '';
  return key
    .split('_')
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

/**
 * Proposal-facing excerpt from playbook (evidence-backed language).
 * @param {object} playbook
 * @returns {object|null}
 */
function proposalExcerptFromPlaybook(playbook) {
  if (!playbook) return null;
  const pb = buildClientPlaybook(playbook);
  return {
    playbookId: pb.id,
    playbookVersion: pb.version,
    playbookName: pb.name,
    brandVoice: pb.brandVoice,
    brandVoiceLabel: brandVoiceLabel(pb.brandVoice),
    valuePropositions: [...pb.valuePropositions],
    offers: [...pb.offers],
    idealCustomer: pb.idealCustomer,
    targetMarkets: [...pb.targetMarkets],
    successMetrics: [...pb.successMetrics],
    preferredChannels: pb.preferredChannels.map(channelLabel),
    notes: pb.notes || '',
  };
}

module.exports = {
  resolvePlaybookFromContext,
  campaignStrategyFromPlaybook,
  applyPlaybookConstraints,
  annotateProspectWithPlaybook,
  buildOpener,
  channelLabel,
  proposalExcerptFromPlaybook,
  CHANNEL_LABELS,
};
