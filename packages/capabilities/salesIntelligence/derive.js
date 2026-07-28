'use strict';

/**
 * Derive Sales Intelligence Profile (SPEC-048 / ADR-032).
 * Deterministic sell reasoning from facts + signals + brief + playbook.
 * Never invents company facts. Never writes channel prose.
 */

const {
  BUYER_TYPES,
  CONFIDENCE_LABEL,
  buildSalesIntelligenceProfile,
  buildBuyingSignal,
  buildPersonalizationClaim,
  buildMessagingStrategy,
  normalizeConfidenceLabel,
} = require('./types');
const { buildBusinessSignalsForProspect } = require('../signals');
const { buildClientPlaybook } = require('../playbook/types');

const TITLE_TO_BUYER = Object.freeze([
  { re: /\bproperty\s+manager\b/i, role: 'Property Manager' },
  { re: /\boperations?\s+manager\b/i, role: 'Operations Manager' },
  { re: /\boffice\s+manager\b/i, role: 'Office Manager' },
  { re: /\bpractice\s+manager\b/i, role: 'Practice Manager' },
  { re: /\bmanaging\s+partner\b/i, role: 'Managing Partner' },
  { re: /\bexecutive\s+director\b/i, role: 'Executive Director' },
  { re: /\b(owner|founder|president|ceo|principal)\b/i, role: 'Owner' },
]);

const INDUSTRY_DEFAULT_BUYER = Object.freeze({
  'property management': 'Property Manager',
  'commercial property management': 'Property Manager',
  'law firm': 'Office Manager',
  accounting: 'Office Manager',
  dental: 'Practice Manager',
  medical: 'Practice Manager',
  restaurant: 'Owner',
  cleaning: 'Owner',
});

/**
 * Derive one profile for a prospect.
 * @param {object} prospect
 * @param {object} [ctx]
 * @returns {object} SalesIntelligenceProfile
 */
function deriveSalesIntelligence(prospect, ctx = {}) {
  const playbook = ctx.playbook
    ? buildClientPlaybook(ctx.playbook)
    : null;
  const intel =
    prospect.companyIntelligence ||
    ctx.companyIntelligence ||
    null;
  const brief =
    prospect.opportunityBrief ||
    ctx.opportunityBrief ||
    null;

  const company = resolveCompany(prospect, intel);
  const industry = resolveIndustry(prospect, intel, playbook);
  const evidenceRefs = [];

  const signalPkg =
    ctx.signalPackage ||
    buildBusinessSignalsForProspect(prospect, {
      knowledge: ctx.knowledge || {},
      playbook,
      asOf: ctx.asOf,
    });

  const activeSignals = Array.isArray(signalPkg.activeSignals)
    ? signalPkg.activeSignals
    : [];
  const buyingSignals = (Array.isArray(signalPkg.buyingSignals)
    ? signalPkg.buyingSignals
    : activeSignals
  ).map((s) => {
    const ref = s.id || `signal:${s.type || s.title}`;
    evidenceRefs.push(ref);
    return buildBuyingSignal({
      signal: s.title || s.type || 'signal',
      confidence: s.confidence,
      confidenceScore: s.confidenceScore,
      evidence: s.description || s.title || '',
      source: Array.isArray(s.evidenceRefs) && s.evidenceRefs[0]
        ? String(s.evidenceRefs[0])
        : 'business_signals',
      evidenceRefs: s.evidenceRefs,
    });
  });

  const decision = inferDecisionMaker(prospect, intel, industry);
  if (decision.evidenceRef) evidenceRefs.push(decision.evidenceRef);

  const buyerType = inferBuyerType(industry, activeSignals, playbook);
  const pains = inferPains(industry, activeSignals, brief, playbook);
  const advantages = resolveAnchorAdvantages(playbook);
  const angle = resolveAngle(brief, activeSignals, signalPkg, pains);
  const cta = resolveCta(playbook, brief);
  const claims = buildClaims(prospect, {
    company,
    industry,
    intel,
    activeSignals,
    brief,
    playbook,
  });
  for (const c of claims) {
    if (c.evidenceRef) evidenceRefs.push(c.evidenceRef);
  }

  const messaging = buildMessagingStrategy({
    opening_focus: pains.primary_pain || angle || industry || 'operations',
    avoid: resolveAvoid(playbook),
    social_proof: advantages.slice(0, 3),
    cta,
    tone: resolveTone(playbook, buyerType),
    positioning:
      angle ||
      (advantages[0]
        ? `Reduce management burden through ${String(advantages[0]).toLowerCase()}.`
        : 'Dependable commercial cleaning that reduces vendor friction.'),
  });

  const confidenceScore = scoreProfileConfidence({
    industry,
    decision,
    claims,
    buyingSignals,
    brief,
    playbook,
  });

  return buildSalesIntelligenceProfile({
    prospectId: prospect.id != null ? String(prospect.id) : null,
    company,
    industry,
    decision_maker: decision.role,
    decision_maker_confidence: decision.confidence,
    buyer_type: buyerType,
    primary_pain: pains.primary_pain,
    secondary_pain: pains.secondary_pain,
    business_goal: pains.business_goal,
    risk_if_unchanged: pains.risk_if_unchanged,
    anchor_advantage: advantages,
    recommended_angle: angle,
    call_to_action: cta,
    buying_signals: buyingSignals,
    messaging_strategy: messaging,
    personalization_claims: claims,
    confidence: normalizeConfidenceLabel(confidenceScore),
    confidenceScore,
    evidenceRefs: [...new Set(evidenceRefs.filter(Boolean))],
    derivedAt: new Date().toISOString(),
  });
}

/**
 * Derive profiles for many prospects.
 * @param {object[]} prospects
 * @param {object} [ctx]
 * @returns {{ profiles: object[], byProspectId: Record<string, object> }}
 */
function deriveSalesIntelligenceStage(prospects, ctx = {}) {
  const list = Array.isArray(prospects) ? prospects : [];
  const profiles = list.map((p) => deriveSalesIntelligence(p, ctx));
  /** @type {Record<string, object>} */
  const byProspectId = {};
  for (const profile of profiles) {
    if (profile.prospectId) byProspectId[profile.prospectId] = profile;
    if (profile.company) {
      byProspectId[`company:${profile.company.toLowerCase()}`] = profile;
    }
  }
  return { profiles, byProspectId };
}

function resolveCompany(prospect, intel) {
  return String(
    (intel && (intel.companyName || intel.company || intel.name)) ||
      prospect.companyName ||
      prospect.company ||
      prospect.name ||
      ''
  ).trim();
}

function resolveIndustry(prospect, intel, playbook) {
  const fromIntel =
    intel &&
    (intel.industry ||
      (intel.firmographics && intel.firmographics.industry));
  if (fromIntel) return String(fromIntel).trim();
  if (prospect.industry) return String(prospect.industry).trim();
  if (playbook && playbook.targetMarkets && playbook.targetMarkets[0]) {
    return String(playbook.targetMarkets[0]).trim();
  }
  return '';
}

function inferDecisionMaker(prospect, intel, industry) {
  const title = String(
    prospect.jobTitle ||
      prospect.title ||
      prospect.contactTitle ||
      (intel && intel.decisionMakerTitle) ||
      ''
  ).trim();
  const contactName = String(
    prospect.contactName ||
      prospect.name ||
      (intel && intel.decisionMakerName) ||
      ''
  ).trim();

  for (const rule of TITLE_TO_BUYER) {
    if (title && rule.re.test(title)) {
      return {
        role: rule.role,
        confidence: CONFIDENCE_LABEL.HIGH,
        evidenceRef: `title:${title}`,
      };
    }
  }

  const key = String(industry || '').toLowerCase();
  for (const [ind, role] of Object.entries(INDUSTRY_DEFAULT_BUYER)) {
    if (key.includes(ind) || ind.includes(key)) {
      return {
        role,
        confidence: contactName
          ? CONFIDENCE_LABEL.MEDIUM
          : CONFIDENCE_LABEL.LOW,
        evidenceRef: `industry_default:${industry}`,
      };
    }
  }

  return {
    role: contactName ? 'Owner' : 'Decision Maker',
    confidence: CONFIDENCE_LABEL.LOW,
    evidenceRef: contactName ? `contact:${contactName}` : 'inference:default',
  };
}

function inferBuyerType(industry, activeSignals, playbook) {
  const posture =
    (activeSignals[0] && String(activeSignals[0].type || '')) || '';
  if (/hir|staff|headcount/i.test(posture)) return BUYER_TYPES.OPERATIONS_FOCUSED;
  if (/expans|growth|location/i.test(posture)) return BUYER_TYPES.GROWTH_ORIENTED;
  if (playbook && playbook.brandVoice === 'relationship_first') {
    return BUYER_TYPES.RELATIONSHIP_DRIVEN;
  }
  if (/property|management|real\s*estate/i.test(String(industry || ''))) {
    return BUYER_TYPES.RELATIONSHIP_DRIVEN;
  }
  return BUYER_TYPES.OPERATIONS_FOCUSED;
}

function inferPains(industry, activeSignals, brief, playbook) {
  const driving = activeSignals[0];
  let primary_pain = 'Reliable vendor execution';
  let secondary_pain = 'Tenant or staff complaints';
  let business_goal = 'Consistent facility presentation';
  let risk_if_unchanged = 'Increased management overhead';

  if (driving) {
    if (/hir|staff/i.test(driving.type || driving.title || '')) {
      primary_pain = 'Operational capacity under growth';
      secondary_pain = 'Inconsistent facility standards';
      business_goal = 'Stable day-to-day facility operations';
      risk_if_unchanged = 'Team distraction from core work';
    } else if (/expans|location|growth/i.test(driving.type || driving.title || '')) {
      primary_pain = 'Facility readiness across locations';
      secondary_pain = 'Vendor coordination overhead';
      business_goal = 'Consistent multi-site presentation';
      risk_if_unchanged = 'Uneven customer or tenant experience';
    } else if (/renovat|lease|acquisit/i.test(driving.type || driving.title || '')) {
      primary_pain = 'Transition / changeover cleaning quality';
      secondary_pain = 'Vendor reliability during change';
      business_goal = 'Smooth facility transition';
      risk_if_unchanged = 'Delayed readiness and rework';
    }
  }

  if (/property\s*management/i.test(String(industry || ''))) {
    primary_pain = 'Reliable vendor execution';
    secondary_pain = 'Tenant complaints';
    business_goal = 'Consistent building presentation';
    risk_if_unchanged = 'Increased management overhead';
  }

  if (brief && brief.bestOutreachAngle) {
    // Keep structured fields; angle used separately
  }
  if (playbook && playbook.idealCustomer && playbook.idealCustomer.buyingTriggers[0]) {
    secondary_pain =
      secondary_pain ||
      String(playbook.idealCustomer.buyingTriggers[0]);
  }

  return { primary_pain, secondary_pain, business_goal, risk_if_unchanged };
}

function resolveAnchorAdvantages(playbook) {
  if (!playbook) {
    return ['Owner-operated', 'Responsive communication', 'Consistent quality'];
  }
  const fromProps = (playbook.valuePropositions || []).slice(0, 3);
  if (fromProps.length) return fromProps;
  return ['Owner-operated', 'Responsive communication', 'Consistent quality'];
}

function resolveAngle(brief, activeSignals, signalPkg, pains) {
  if (brief && brief.bestOutreachAngle) {
    // Strip long prose into a short angle when possible
    const raw = String(brief.bestOutreachAngle);
    const lead = raw.split('—')[0].trim();
    if (lead.length && lead.length < 120) return lead.replace(/^Lead with\s+/i, '');
    return `Reduce vendor management burden around ${pains.primary_pain.toLowerCase()}.`;
  }
  if (signalPkg && signalPkg.messagingDescription) {
    return `Address ${pains.primary_pain.toLowerCase()} with dependable cleaning.`;
  }
  if (activeSignals.length) {
    return `Reduce vendor management burden.`;
  }
  return `Reduce management burden through dependable cleaning.`;
}

function resolveCta(playbook, brief) {
  if (brief && brief.suggestedFirstAction) {
    const a = String(brief.suggestedFirstAction);
    if (/walkthrough/i.test(a)) return 'Offer a walkthrough of one managed property.';
  }
  const offer =
    (playbook && playbook.offers && playbook.offers[0]) || 'a brief walkthrough';
  if (/walkthrough/i.test(String(offer))) {
    return `Offer ${String(offer).toLowerCase()}.`;
  }
  return `Offer a walkthrough of one managed property.`;
}

function resolveAvoid(playbook) {
  const avoid = ['Low price', 'Generic cleaning pitches'];
  if (playbook && playbook.idealCustomer) {
    for (const ind of playbook.idealCustomer.industriesToAvoid || []) {
      avoid.push(`Pitching as ${ind}`);
    }
  }
  return avoid;
}

function resolveTone(playbook, buyerType) {
  const voice = playbook && playbook.brandVoice;
  if (voice === 'direct') return ['Direct', 'Professional'];
  if (voice === 'friendly') return ['Friendly', 'Helpful'];
  if (voice === 'premium') return ['Professional', 'Premium'];
  if (buyerType === BUYER_TYPES.RELATIONSHIP_DRIVEN) {
    return ['Professional', 'Helpful', 'Consultative'];
  }
  return ['Professional', 'Consultative'];
}

function buildClaims(prospect, ctx) {
  const claims = [];
  if (ctx.industry && (prospect.industry || (ctx.intel && ctx.intel.industry))) {
    claims.push(
      buildPersonalizationClaim({
        claim: `Operates in ${ctx.industry}`,
        evidenceRef: prospect.industry
          ? `prospect.industry:${prospect.industry}`
          : `company_intelligence.industry:${ctx.industry}`,
        verified: true,
        source: prospect.industry ? 'prospect' : 'company_intelligence',
      })
    );
  }
  if (prospect.address || prospect.mailingAddress) {
    const addr = prospect.mailingAddress || prospect.address;
    claims.push(
      buildPersonalizationClaim({
        claim: `Located at ${addr}`,
        evidenceRef: `address:${addr}`,
        verified: true,
        source: 'prospect',
      })
    );
  }
  if (prospect.website) {
    claims.push(
      buildPersonalizationClaim({
        claim: `Maintains a public web presence at ${prospect.website}`,
        evidenceRef: `website:${prospect.website}`,
        verified: true,
        source: 'prospect',
      })
    );
  }
  for (const s of (ctx.activeSignals || []).slice(0, 2)) {
    claims.push(
      buildPersonalizationClaim({
        claim: s.description || s.title,
        evidenceRef: s.id || `signal:${s.type}`,
        verified: true,
        source: 'business_signals',
      })
    );
  }
  if (ctx.brief && ctx.brief.whyFit) {
    claims.push(
      buildPersonalizationClaim({
        claim: String(ctx.brief.whyFit).slice(0, 160),
        evidenceRef: 'opportunity_brief.whyFit',
        verified: true,
        source: 'opportunity_brief',
      })
    );
  }
  return claims.filter((c) => c.claim);
}

function scoreProfileConfidence(args) {
  let score = 0.25;
  if (args.industry) score += 0.15;
  if (
    args.decision &&
    args.decision.confidence === CONFIDENCE_LABEL.HIGH
  ) {
    score += 0.15;
  } else if (
    args.decision &&
    args.decision.confidence === CONFIDENCE_LABEL.MEDIUM
  ) {
    score += 0.08;
  }
  const verified = (args.claims || []).filter((c) => c.verified);
  if (verified.length >= 1) score += 0.12;
  if (verified.length >= 2) score += 0.08;
  if ((args.buyingSignals || []).length) score += 0.1;
  if (args.brief) score += 0.08;
  if (args.playbook) score += 0.07;
  return Math.max(0, Math.min(1, Number(score.toFixed(3))));
}

/**
 * Build a prospect-first opening sentence from a profile (language seed for generators).
 * @param {object} profile
 * @returns {string}
 */
function openingFromProfile(profile) {
  if (!profile) return '';
  const company = profile.company || 'This organization';
  const industry = profile.industry || 'their market';
  const focus =
    (profile.messaging_strategy && profile.messaging_strategy.opening_focus) ||
    profile.primary_pain ||
    'operations';
  const claim = (profile.personalization_claims || []).find((c) => c.verified);
  if (claim && claim.claim) {
    return `${company} — ${claim.claim.replace(/\.$/, '')}. Managing ${industry.toLowerCase()} work means ${String(focus).toLowerCase()} stays non-negotiable.`;
  }
  return `Managing ${industry.toLowerCase()} operations at ${company} means ${String(focus).toLowerCase()} stays front of mind.`;
}

module.exports = {
  deriveSalesIntelligence,
  deriveSalesIntelligenceStage,
  openingFromProfile,
  resolveCompany,
  resolveIndustry,
  inferDecisionMaker,
};
