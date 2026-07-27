'use strict';

/**
 * Opportunity Brief builder (SPEC-026).
 * Deterministic briefs from scored evidence — no LLM inventing angles.
 */

const { buildOpportunityBrief, PRIORITY } = require('./types');

/**
 * @param {object} prospect
 * @param {object} scored - result of scoreOpportunity
 * @param {object} [ctx]
 * @returns {object} OpportunityBrief
 */
function buildBrief(prospect, scored, ctx = {}) {
  const profile = ctx.profile || null;
  const company = prospect.companyName || 'This company';
  const industry = prospect.industry || inferIndustry(prospect) || 'professional services';
  const factors = Object.fromEntries(
    (scored.factorScores || []).map((f) => [f.factor, f])
  );

  const whyFit = buildWhyFit(company, industry, factors, profile, scored);
  const bestOutreachAngle = buildAngle(company, industry, factors, prospect);
  const talkingPoints = buildTalkingPoints(company, industry, factors, prospect, profile);
  const potentialObjections = buildObjections(factors, prospect);
  const suggestedFirstAction = buildFirstAction(scored, prospect);

  return buildOpportunityBrief({
    whyFit,
    bestOutreachAngle,
    talkingPoints,
    potentialObjections,
    suggestedFirstAction,
  });
}

/**
 * @param {object} scored
 * @param {object} prospect
 * @returns {string}
 */
function recommendNextAction(scored, prospect) {
  const priority = scored.priority || scored._priority;
  if (!prospect.email && !prospect.phone) {
    return 'Enrich decision-maker contact before outreach';
  }
  if (priority === PRIORITY.HIGH) {
    return 'Approve for Campaign Builder — prioritize in first wave';
  }
  if (priority === PRIORITY.MEDIUM) {
    return 'Review brief, then include in Campaign Builder second wave';
  }
  return 'Hold for review or exclude — evidence too thin for first wave';
}

function buildWhyFit(company, industry, factors, profile, scored) {
  const parts = [];
  if (factors.profile_match?.matched) {
    parts.push(
      profile
        ? `${company} matches Discovery Profile “${profile.name}” for ${industry}`
        : `${company} matched discovery ranking signals for ${industry}`
    );
  } else {
    parts.push(`${company} is a candidate in ${industry} with limited profile evidence`);
  }
  if (factors.geographic_fit?.matched) {
    parts.push('location fits the target geography');
  }
  if (factors.decision_maker_confidence?.matched) {
    parts.push('a reachable contact is evidenced');
  }
  if (factors.buying_signals?.matched) {
    parts.push('buying signals are present in enrichment/knowledge');
  }
  if (scored.topReasons?.length && parts.length < 2) {
    parts.push(scored.topReasons[0]);
  }
  return parts.join('; ') + '.';
}

function buildAngle(company, industry, factors, prospect) {
  if (factors.buying_signals?.matched) {
    return `Lead with the observed buying signal for ${company} — tie cleaning capacity to their current facility change.`;
  }
  if (factors.personalization_opportunities?.score >= 6 && prospect.website) {
    return `Reference ${company}'s ${industry} footprint (via website/address) and ask who owns facility cleaning today.`;
  }
  if (factors.decision_maker_confidence?.matched) {
    return `Short intro to the evidenced decision-maker: confirm they own vendor selection for ${industry} facilities.`;
  }
  return `Credibility-first intro: local commercial cleaning for ${industry} offices — ask permission to send a one-pager.`;
}

function buildTalkingPoints(company, industry, factors, prospect, profile) {
  const points = [];
  const geo =
    (profile && profile.geography && profile.geography.label) ||
    (prospect.address ? 'their area' : 'Greater Manchester');

  points.push(
    `${company} looks like a single-tenant ${industry} office — a strong commercial cleaning fit.`
  );
  if (factors.geographic_fit?.matched) {
    points.push(`Service coverage already includes ${geo}.`);
  } else {
    points.push(`Confirm serviceability for their address before quoting.`);
  }
  if (factors.decision_maker_confidence?.matched) {
    points.push('We have a path to a decision-maker (title and/or direct contact).');
  } else if (prospect.website) {
    points.push(`Use ${prospect.website} to identify the office manager / owner before dialing.`);
  } else {
    points.push('Ask who handles facilities or vendor approvals on the first touch.');
  }
  return points.slice(0, 3);
}

function buildObjections(factors, prospect) {
  const objections = [];
  if (!factors.buying_signals?.matched) {
    objections.push('“We already have a cleaner” — no timed buying trigger evidenced');
  }
  if (!factors.decision_maker_confidence?.matched) {
    objections.push('Gatekeeper / unknown buyer — may need enrichment before a real conversation');
  }
  if (!prospect.website) {
    objections.push('Thin web presence — harder to personalize and verify legitimacy');
  }
  if (factors.company_size?.score > 0 && factors.company_size.score < 4) {
    objections.push('Size may be too large / national — procurement could be centralized');
  }
  if (objections.length === 0) {
    objections.push('Timing — even strong fits may defer if contract is mid-term');
  }
  return objections.slice(0, 4);
}

function buildFirstAction(scored, prospect) {
  if (!prospect.email && !prospect.phone) {
    return 'Run contact enrichment for owner / office manager, then re-rank';
  }
  if ((scored.overallScore || 0) >= 70) {
    return 'Lock into Campaign Builder first wave and draft a personalized opener from the brief';
  }
  if ((scored.overallScore || 0) >= 45) {
    return 'Approve for second-wave Campaign Builder after a quick operator skim of risks';
  }
  return 'Exclude or park — gather more evidence before outreach';
}

function inferIndustry(prospect) {
  const signals = prospect.rankingSignals || [];
  const industrySignal = signals.find(
    (s) => s.signal === 'target_industry' && s.matched
  );
  if (industrySignal && industrySignal.detail) {
    const m = /—\s*(.+)$/.exec(industrySignal.detail);
    if (m) return m[1].trim();
  }
  return null;
}

module.exports = {
  buildBrief,
  recommendNextAction,
};
