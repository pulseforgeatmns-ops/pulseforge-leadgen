'use strict';

/**
 * Evidence helpers for Proposal Generator (SPEC-027B).
 * Every recommendation must trace to discovery / profile / ranking / campaign.
 */

/**
 * @param {string} kind
 * @param {string} field
 * @param {string} [detail]
 * @returns {object}
 */
function evidenceRef(kind, field, detail) {
  return {
    kind: String(kind),
    field: String(field),
    detail: detail != null ? String(detail) : null,
  };
}

/**
 * Collect available evidence keys from inputs for uncertainty reporting.
 * @param {object} summary
 * @param {object|null} profile
 * @param {object} strategy
 * @param {object|null} [playbook]
 * @returns {{ present: string[], missing: string[] }}
 */
function inventoryEvidence(summary, profile, strategy, playbook) {
  const present = [];
  const missing = [];

  const summaryFields = [
    'companyName',
    'contactName',
    'industry',
    'geography',
    'companyStage',
    'currentClients',
    'revenue',
    'currentMarketingChannels',
    'icp',
    'currentProcess',
    'challenges',
    'goals',
    'growthVision',
    'notes',
  ];

  for (const f of summaryFields) {
    const v = summary[f];
    const empty =
      v == null ||
      v === '' ||
      (Array.isArray(v) && v.length === 0);
    if (empty) missing.push(`discovery.${f}`);
    else present.push(`discovery.${f}`);
  }

  if (profile && profile.name) present.push('profile.name');
  else missing.push('profile.name');

  if (profile && Array.isArray(profile.industryTargets) && profile.industryTargets.length) {
    present.push('profile.industryTargets');
  } else if (summary.icp && summary.icp.length) {
    present.push('discovery.icp');
  } else {
    missing.push('profile.industryTargets');
  }

  if (playbook && playbook.id) {
    present.push('playbook.id');
    if (playbook.valuePropositions && playbook.valuePropositions.length) {
      present.push('playbook.valuePropositions');
    }
    if (playbook.offers && playbook.offers.length) present.push('playbook.offers');
    if (playbook.brandVoice) present.push('playbook.brandVoice');
    if (playbook.successMetrics && playbook.successMetrics.length) {
      present.push('playbook.successMetrics');
    }
    if (playbook.targetMarkets && playbook.targetMarkets.length) {
      present.push('playbook.targetMarkets');
    }
  } else {
    missing.push('playbook.id');
  }

  if (strategy && strategy.source) present.push(`strategy.${strategy.source}`);
  if (strategy && strategy.markets && strategy.markets.length) {
    present.push('strategy.markets');
  }

  return { present, missing };
}

/**
 * Resolve recommended markets without inventing them.
 * @param {object} summary
 * @param {object|null} profile
 * @param {object} strategy
 * @param {object|null} [playbook]
 * @returns {{ markets: string[], why: string, uncertain: boolean, refs: object[] }}
 */
function resolveMarkets(summary, profile, strategy, playbook) {
  const refs = [];
  const markets = [];

  if (strategy && Array.isArray(strategy.markets) && strategy.markets.length) {
    for (const m of strategy.markets) markets.push(String(m));
    refs.push(evidenceRef('strategy', 'markets', strategy.source || 'recommended_strategy'));
  }

  if (
    !markets.length &&
    playbook &&
    Array.isArray(playbook.targetMarkets) &&
    playbook.targetMarkets.length
  ) {
    for (const m of playbook.targetMarkets) markets.push(String(m));
    refs.push(
      evidenceRef(
        'client_playbook',
        'targetMarkets',
        playbook.name || playbook.id
      )
    );
  }

  if (
    !markets.length &&
    playbook &&
    playbook.idealCustomer &&
    Array.isArray(playbook.idealCustomer.primaryMarkets) &&
    playbook.idealCustomer.primaryMarkets.length
  ) {
    for (const m of playbook.idealCustomer.primaryMarkets) markets.push(String(m));
    refs.push(
      evidenceRef('client_playbook', 'idealCustomer.primaryMarkets', playbook.name)
    );
  }

  if (!markets.length && profile && Array.isArray(profile.industryTargets)) {
    for (const m of profile.industryTargets) markets.push(String(m));
    if (markets.length) {
      refs.push(
        evidenceRef('discovery_profile', 'industryTargets', profile.name || profile.id)
      );
    }
  }

  if (!markets.length && summary.icp && summary.icp.length) {
    for (const m of summary.icp) markets.push(String(m));
    refs.push(evidenceRef('discovery', 'icp', markets.join(', ')));
  }

  if (!markets.length) {
    return {
      markets: [],
      why:
        'Target markets were not confirmed in discovery notes, Client Playbook, or an attached Discovery Profile. ' +
        'We will lock markets with you at kickoff rather than assume them here.',
      uncertain: true,
      refs: [evidenceRef('uncertainty', 'markets', 'no playbook, profile, or icp evidence')],
    };
  }

  const geo =
    (playbook &&
      playbook.idealCustomer &&
      playbook.idealCustomer.geographicCoverage) ||
    (profile && profile.geography && (profile.geography.label || profile.geography.state)) ||
    summary.geography ||
    null;

  let why;
  if (playbook && playbook.name) {
    why = `These markets come from Client Playbook “${playbook.name}” (v${playbook.version})${geo ? ` for ${geo}` : ''} — how ${summary.companyName} wins customers.`;
    refs.push(evidenceRef('client_playbook', 'version', playbook.version));
  } else if (summary.growthVision) {
    why = `These markets were selected because they align with what ${summary.companyName} described as the growth vision: ${summary.growthVision}${geo ? ` — focused in ${geo}` : ''}.`;
    refs.push(evidenceRef('discovery', 'growthVision', summary.growthVision));
  } else if (profile && profile.name) {
    why = `These markets come from Discovery Profile “${profile.name}”${geo ? ` for ${geo}` : ''}, matched to this engagement.`;
  } else {
    why = `These markets reflect the ICP you shared during discovery${geo ? ` in ${geo}` : ''}.`;
  }

  if (geo) refs.push(evidenceRef('discovery', 'geography', String(geo)));

  return { markets, why, uncertain: false, refs };
}

/**
 * Pull strategy narrative from prior mission outputs when present.
 * @param {object} inputs
 * @returns {object}
 */
function resolveStrategyContext(inputs = {}) {
  const prior = inputs.priorOutputs || {};
  const campaign = inputs.campaign || prior.campaign || null;
  const ranked = inputs.prospects || prior.prospects || [];
  const profile =
    inputs.discoveryProfile ||
    prior.discoveryProfile ||
    (inputs.constraints && inputs.constraints.discoveryProfile) ||
    null;
  const playbook =
    inputs.clientPlaybook ||
    prior.clientPlaybook ||
    (inputs.constraints && inputs.constraints.clientPlaybook) ||
    null;

  const markets = [];
  if (inputs.recommendedStrategy && Array.isArray(inputs.recommendedStrategy.markets)) {
    markets.push(...inputs.recommendedStrategy.markets.map(String));
  }
  if (!markets.length && playbook && Array.isArray(playbook.targetMarkets)) {
    markets.push(...playbook.targetMarkets.map(String));
  }

  let source = null;
  if (campaign && campaign.name) source = 'campaign_builder';
  else if (playbook && playbook.id) source = 'client_playbook';
  else if (Array.isArray(ranked) && ranked.length) source = 'opportunity_ranking';
  else if (profile) source = 'discovery_profile';

  const topProspects = Array.isArray(ranked)
    ? ranked.slice(0, 5).map((p) => ({
        companyName: p.companyName,
        priority: p.priority || null,
        overallScore: p.overallScore != null ? p.overallScore : p.priorityScore,
        reason:
          (p.opportunityBrief && p.opportunityBrief.whyFit) ||
          (p.topReasons && p.topReasons[0]) ||
          p.reasonSelected ||
          null,
      }))
    : [];

  return {
    source,
    markets,
    campaignName: campaign && campaign.name ? String(campaign.name) : null,
    prospectCount: Array.isArray(ranked) ? ranked.length : 0,
    topProspects,
    profile,
    playbook,
    narrative:
      inputs.recommendedStrategy && inputs.recommendedStrategy.narrative
        ? String(inputs.recommendedStrategy.narrative)
        : null,
  };
}

module.exports = {
  evidenceRef,
  inventoryEvidence,
  resolveMarkets,
  resolveStrategyContext,
};
