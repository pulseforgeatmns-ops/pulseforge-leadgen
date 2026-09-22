'use strict';

const {
  PROSPECT_MOTIONS,
  ASSIGNMENT_CATEGORIES,
  ANCHOR_SERVICE_AREA,
  HYBRID_VERTICALS,
  RELATIONSHIP_VERTICALS,
  SEGMENT_PRIORITY,
  isGenericEmail,
  verticalLabel,
} = require('../utils/aoProspectRoutingConstants');
const { matchServiceAreaFromLocation } = require('../utils/serviceArea');
const { distributeDueDates, todayISOInZone } = require('../utils/aoAssignment');

const REQUIRED_LOG_FIELDS = Object.freeze([
  'decision_maker',
  'current_vendor_status',
  'pain_or_timing',
  'next_step',
  'follow_up_date',
]);

function normalizeText(value) {
  return String(value || '').trim();
}

function companyName(prospect, company) {
  return normalizeText(company?.name)
    || normalizeText(`${prospect?.first_name || ''} ${prospect?.last_name || ''}`)
    || normalizeText(prospect?.email)
    || 'Unknown account';
}

function locationText(prospect, company) {
  return normalizeText(prospect?.service_area_match)
    || normalizeText(company?.location)
    || 'Greater Manchester, NH';
}

function inServiceArea(prospect, company, serviceAreas = ANCHOR_SERVICE_AREA) {
  if (prospect?.service_area_match === false) return false;
  const hay = `${locationText(prospect, company)} ${company?.location || ''}`.trim();
  if (!hay) return true;
  return Boolean(matchServiceAreaFromLocation(hay, serviceAreas));
}

function hasValidPhone(prospect) {
  const phone = normalizeText(prospect?.phone);
  return phone.length >= 10 && !/^0+$/.test(phone.replace(/\D/g, ''));
}

function hasUsableEmail(prospect) {
  const email = normalizeText(prospect?.email);
  if (!email || !email.includes('@')) return false;
  return !isGenericEmail(email);
}

function hasAnyEmail(prospect) {
  return normalizeText(prospect?.email).includes('@');
}

function hasDecisionMakerName(prospect) {
  const name = normalizeText(`${prospect?.first_name || ''} ${prospect?.last_name || ''}`);
  return name.split(/\s+/).filter(Boolean).length >= 2;
}

function engagementScore(prospect, touchpoints = []) {
  let score = 0;
  const status = normalizeText(prospect?.status).toLowerCase();
  if (status === 'warm') score += 8;
  if (status === 'hot' || prospect?.is_hot) score += 12;
  const recent = touchpoints.filter(tp => {
    const action = normalizeText(tp?.action_type).toLowerCase();
    return ['open', 'click', 'reply', 'call'].includes(action);
  });
  score += Math.min(10, recent.length * 3);
  return score;
}

function computeAoFitScore({ prospect, company, touchpoints = [], serviceAreas = ANCHOR_SERVICE_AREA }) {
  const vertical = normalizeText(prospect?.vertical);
  const icp = Number(prospect?.icp_score || 0);
  const breakdown = [];
  let score = 0;

  const icpPoints = Math.round(Math.min(25, (icp / 100) * 25));
  score += icpPoints;
  breakdown.push({ factor: 'ICP fit', points: icpPoints, detail: `ICP score ${icp}` });

  const segmentPoints = SEGMENT_PRIORITY[vertical] || 0;
  score += segmentPoints;
  if (segmentPoints) breakdown.push({ factor: 'Segment priority', points: segmentPoints, detail: verticalLabel(vertical) });

  let localPoints = 0;
  if (inServiceArea(prospect, company, serviceAreas)) {
    const loc = locationText(prospect, company).toLowerCase();
    localPoints = loc.includes('manchester') ? 15 : 12;
  } else {
    localPoints = -40;
  }
  score += localPoints;
  breakdown.push({
    factor: 'Local accessibility',
    points: localPoints,
    detail: localPoints > 0 ? 'Inside Anchor service area' : 'Outside service area',
  });

  let contactPoints = 0;
  if (hasValidPhone(prospect) && hasDecisionMakerName(prospect)) contactPoints = 15;
  else if (hasValidPhone(prospect)) contactPoints = 12;
  else if (hasUsableEmail(prospect)) contactPoints = 6;
  else if (hasAnyEmail(prospect)) contactPoints = 2;
  else contactPoints = -20;
  score += contactPoints;
  breakdown.push({ factor: 'Decision-maker accessibility', points: contactPoints, detail: contactPoints >= 12 ? 'Reachable by phone' : 'Limited contact path' });

  let trustPoints = RELATIONSHIP_VERTICALS.includes(vertical) ? 10 : 0;
  if (HYBRID_VERTICALS.includes(vertical)) trustPoints = Math.max(trustPoints, 8);
  score += trustPoints;
  if (trustPoints) breakdown.push({ factor: 'Human-trust value', points: trustPoints, detail: 'Relationship-heavy segment' });

  const engagePoints = engagementScore(prospect, touchpoints);
  score += engagePoints;
  if (engagePoints) breakdown.push({ factor: 'Engagement signal', points: engagePoints, detail: 'Warmth or recent activity' });

  if (isGenericEmail(prospect?.email)) {
    score -= 10;
    breakdown.push({ factor: 'Weak contactability', points: -10, detail: 'Generic inbox email' });
  }
  if (icp > 0 && icp < 50) {
    score -= 10;
    breakdown.push({ factor: 'Low-value signal', points: -10, detail: 'Below ICP threshold' });
  }
  if (prospect?.do_not_contact) {
    score = 0;
    breakdown.push({ factor: 'Bad-fit signal', points: -100, detail: 'Do not contact' });
  }

  return {
    ao_fit_score: Math.max(0, Math.min(100, score)),
    score_breakdown: breakdown,
  };
}

function classifyMotion({ prospect, company, aoFitScore, touchpoints = [], serviceAreas = ANCHOR_SERVICE_AREA }) {
  const vertical = normalizeText(prospect?.vertical);
  const icp = Number(prospect?.icp_score || 0);

  if (prospect?.do_not_contact) {
    return { recommended_motion: 'SUPPRESS', motion_reason: 'Marked do not contact' };
  }
  if (!inServiceArea(prospect, company, serviceAreas)) {
    return { recommended_motion: 'SUPPRESS', motion_reason: 'Outside Anchor service area' };
  }
  if (icp > 0 && icp < 30) {
    return { recommended_motion: 'SUPPRESS', motion_reason: 'ICP score too low for outreach' };
  }

  const engaged = engagementScore(prospect, touchpoints) >= 8;
  const strategic = RELATIONSHIP_VERTICALS.includes(vertical) || aoFitScore >= 70;
  const hybridVertical = HYBRID_VERTICALS.includes(vertical);
  const usableEmail = hasUsableEmail(prospect);
  const weakEmail = hasAnyEmail(prospect) && !usableEmail;
  const phoneReachable = hasValidPhone(prospect);

  if (engaged && (strategic || hybridVertical || phoneReachable)) {
    return {
      recommended_motion: hybridVertical ? 'HYBRID' : 'AO_LED',
      motion_reason: 'Warm engagement signal — human follow-up warranted',
    };
  }
  if (hybridVertical && (weakEmail || strategic)) {
    return {
      recommended_motion: 'HYBRID',
      motion_reason: `${verticalLabel(vertical)} accounts benefit from email plus AO touch`,
    };
  }
  if (strategic && (weakEmail || !usableEmail) && phoneReachable) {
    return {
      recommended_motion: 'AO_LED',
      motion_reason: 'Strategic account with weak email data but reachable by phone or visit',
    };
  }
  if (aoFitScore >= 65 && phoneReachable) {
    return {
      recommended_motion: 'AO_LED',
      motion_reason: 'High AO fit with local accessibility',
    };
  }
  if (usableEmail && icp >= 50 && aoFitScore < 55) {
    return {
      recommended_motion: 'EMAIL_LED',
      motion_reason: 'Ordinary ICP with usable email — email-first until human time is justified',
    };
  }
  if (icp >= 40 || aoFitScore >= 40) {
    return {
      recommended_motion: 'NURTURE',
      motion_reason: 'Real account but timing is not active yet',
    };
  }
  return {
    recommended_motion: 'SUPPRESS',
    motion_reason: 'Not commercially relevant for AO or email outreach',
  };
}

function classifyAssignmentCategory({ prospect, company, aoFitScore, touchpoints = [], motion, existingAssignment }) {
  if (existingAssignment?.next_action === 'AO_FOLLOW_UP' || existingAssignment?.advisory_stage === 'debrief_pending') {
    return {
      assignment_category: 'FOLLOW_UP_REQUIRED',
      category_reason: 'Prior debrief requires AO follow-up',
    };
  }
  const engaged = engagementScore(prospect, touchpoints) >= 8;
  if (engaged) {
    return { assignment_category: 'WARM_SIGNAL', category_reason: 'Prospect shows warmth or engagement' };
  }
  const vertical = normalizeText(prospect?.vertical);
  const icp = Number(prospect?.icp_score || 0);
  if (RELATIONSHIP_VERTICALS.includes(vertical) && aoFitScore >= 60) {
    return {
      assignment_category: 'UNFAIR_ADVANTAGE',
      category_reason: 'Relationship-heavy segment with local advantage',
    };
  }
  if (icp >= 70) {
    return { assignment_category: 'HIGH_VALUE_ICP', category_reason: `ICP score ${icp}` };
  }
  const loc = locationText(prospect, company).toLowerCase();
  if (ANCHOR_SERVICE_AREA.some(city => loc.includes(city.toLowerCase()))) {
    return {
      assignment_category: 'ROUTE_CLUSTER',
      category_reason: 'Account clusters with other in-area route stops',
    };
  }
  if (motion === 'AO_LED' || motion === 'HYBRID') {
    return {
      assignment_category: 'WALK_IN_OPPORTUNITY',
      category_reason: 'Local visit or walk-in candidate',
    };
  }
  return { assignment_category: 'HIGH_VALUE_ICP', category_reason: 'Default ICP assignment' };
}

function buildWhyAccountMatters({ prospect, company }) {
  const vertical = normalizeText(prospect?.vertical);
  const location = locationText(prospect, company);
  const name = companyName(prospect, company);
  const templates = {
    property_manager: `${name} in ${location} likely has recurring vendor needs, turnover needs, and backup cleaner risk when primary vendors miss work.`,
    str_manager: `${name} manages short-term rentals with turnover cleaning, common-area upkeep, and backup coverage needs between guest stays.`,
    commercial_office: `${name} is a professional office where facilities decisions often sit with an office manager rather than a generic inbox.`,
    law_firm: `${name} is a law firm where a consistent office presentation matters and vendor decisions usually sit with office management.`,
    accounting: `${name} is a CPA or accounting office where client-facing space quality reflects professionalism.`,
    cleaning_company_overflow: `${name} may need overflow or backup labor when their regular crew is overloaded.`,
  };
  return templates[vertical]
    || `${name} in ${location} is in Anchor's service area and may need recurring or backup commercial cleaning support.`;
}

function buildRecommendedAngle({ prospect, motion }) {
  const vertical = normalizeText(prospect?.vertical);
  const angles = {
    property_manager: 'Backup and overflow cleaning resource.',
    str_manager: 'Turnover and backup cleaning coverage between guest stays.',
    commercial_office: 'Reliable local office cleaning with one accountable owner.',
    law_firm: 'Consistent office presentation without vendor churn.',
    accounting: 'Professional office cleaning with written scope and follow-through.',
    cleaning_company_overflow: 'Overflow labor when your crew is stretched.',
  };
  if (motion === 'EMAIL_LED') return 'Low-friction email intro to identify facilities contact.';
  return angles[vertical] || 'Local backup and recurring cleaning resource.';
}

function buildFirstAction({ prospect, motion }) {
  if (motion === 'EMAIL_LED') return 'Send intro email to identify who handles cleaning vendors.';
  if (motion === 'HYBRID') return 'Call the office. If no answer, send intro email. If nearby, walk in with a leave-behind.';
  if (motion === 'NURTURE') return 'Add to nurture queue — no active outreach until timing changes.';
  if (hasValidPhone(prospect)) return 'Call the office and ask who handles cleaning vendors.';
  return 'Visit in person or send intro email to identify the decision-maker.';
}

function buildDiscoveryObjective({ prospect }) {
  const vertical = normalizeText(prospect?.vertical);
  if (vertical === 'property_manager') {
    return 'Find who handles cleaning vendors and whether they ever need backup support during turnovers.';
  }
  return 'Identify the decision-maker, current cleaning setup, and whether backup or recurring support is relevant now or later.';
}

function buildSuggestedOpener({ prospect, aoName }) {
  const vertical = normalizeText(prospect?.vertical);
  const rep = aoName || 'local with Anchor Cleaning';
  if (vertical === 'property_manager') {
    return `Hey, I'm ${rep}. We help property teams when their usual cleaner is overloaded, misses a turnover, or they need backup coverage. Who usually handles cleaning vendors for your properties?`;
  }
  return `Hey, I'm ${rep}. We help local businesses keep their spaces consistently clean and can step in as backup when their usual vendor is stretched. Who usually handles cleaning or facilities vendors here?`;
}

function buildDesiredOutcome({ motion }) {
  if (motion === 'EMAIL_LED') return 'Identify the decision-maker or earn permission to continue by email.';
  if (motion === 'NURTURE') return 'Confirm account is real and note timing for future follow-up.';
  return 'Identify the decision-maker or earn permission to send backup vendor information.';
}

function buildAoFitReason({ prospect, company, aoFitScore, motion, scoreBreakdown }) {
  const location = locationText(prospect, company);
  const vertical = verticalLabel(prospect?.vertical);
  const emailNote = isGenericEmail(prospect?.email)
    ? 'Email data is generic, so AO-led outreach is preferred.'
    : hasUsableEmail(prospect?.email)
      ? 'Usable email is available.'
      : 'Contact path is thin, so human outreach is preferred.';
  const topFactors = scoreBreakdown
    .filter(item => item.points > 0)
    .slice(0, 3)
    .map(item => item.detail)
    .join('; ');
  return [
    `Why this account: This is a ${location.split(',')[0] || location} ${vertical.toLowerCase()} company in Anchor's service area.`,
    topFactors || 'Segment and geography fit Anchor immediate-cash targets.',
    emailNote,
    `Recommended motion: ${motion} (AO fit ${aoFitScore}/100).`,
  ].join(' ');
}

function pickRecommendedAo({ availableAos = [], assignmentCategory, prospect, existingAssignedId }) {
  if (existingAssignedId) {
    const existing = availableAos.find(ao => Number(ao.id) === Number(existingAssignedId));
    if (existing) return existing;
  }
  const active = availableAos.filter(ao => ao.active !== false);
  if (!active.length) return null;

  const territory = normalizeText(prospect?.service_area_match || '').toLowerCase();
  const territoryMatch = active.find(ao => normalizeText(ao.territory).toLowerCase()
    && territory.includes(normalizeText(ao.territory).toLowerCase()));
  if (territoryMatch) return territoryMatch;

  const sorted = [...active].sort((a, b) => {
    const loadA = Number(a.open_task_count || 0);
    const loadB = Number(b.open_task_count || 0);
    return loadA - loadB;
  });
  if (assignmentCategory === 'UNFAIR_ADVANTAGE') return sorted[0];
  return sorted[0];
}

function taskPriority({ prospect, assignmentCategory, aoFitScore }) {
  if (assignmentCategory === 'WARM_SIGNAL' || prospect?.is_hot || prospect?.status === 'hot') return 'warm';
  if (assignmentCategory === 'UNFAIR_ADVANTAGE' || aoFitScore >= 75) return 'high';
  return 'normal';
}

function routeProspect({
  prospect,
  company = null,
  touchpoints = [],
  availableAos = [],
  serviceAreas = ANCHOR_SERVICE_AREA,
  aoName = null,
  existingAssignment = null,
}) {
  const { ao_fit_score, score_breakdown } = computeAoFitScore({ prospect, company, touchpoints, serviceAreas });
  const { recommended_motion, motion_reason } = classifyMotion({
    prospect,
    company,
    aoFitScore: ao_fit_score,
    touchpoints,
    serviceAreas,
  });
  const { assignment_category, category_reason } = classifyAssignmentCategory({
    prospect,
    company,
    aoFitScore: ao_fit_score,
    touchpoints,
    motion: recommended_motion,
    existingAssignment,
  });

  const recommended_ao = pickRecommendedAo({
    availableAos,
    assignmentCategory: assignment_category,
    prospect,
    existingAssignedId: existingAssignment?.assigned_ao_id,
  });

  const recommended_angle = buildRecommendedAngle({ prospect, motion: recommended_motion });
  const recommended_first_action = buildFirstAction({ prospect, motion: recommended_motion });
  const ao_fit_reason = buildAoFitReason({
    prospect,
    company,
    aoFitScore: ao_fit_score,
    motion: recommended_motion,
    scoreBreakdown: score_breakdown,
  });

  return {
    recommended_motion,
    ao_fit_score,
    ao_fit_reason,
    assignment_category,
    recommended_ao_id: recommended_ao?.id || null,
    recommended_ao_name: recommended_ao?.name || null,
    recommended_angle,
    recommended_first_action,
    discovery_objective: buildDiscoveryObjective({ prospect }),
    suggested_opener: buildSuggestedOpener({ prospect, aoName: aoName || recommended_ao?.name }),
    desired_next_outcome: buildDesiredOutcome({ motion: recommended_motion }),
    required_log_fields: [...REQUIRED_LOG_FIELDS],
    reasoning: {
      motion_reason,
      category_reason,
      score_breakdown,
      why_account_matters: buildWhyAccountMatters({ prospect, company }),
      priority: taskPriority({ prospect, assignmentCategory: assignment_category, aoFitScore: ao_fit_score }),
      account: companyName(prospect, company),
      segment: verticalLabel(prospect?.vertical),
      location: locationText(prospect, company),
    },
  };
}

function buildWeeklyListMix(routedItems, { today } = {}) {
  const baseDate = today || todayISOInZone();
  const buckets = {
    UNFAIR_ADVANTAGE: [],
    HIGH_VALUE_ICP: [],
    ROUTE_CLUSTER: [],
    WALK_IN_OPPORTUNITY: [],
    WARM_SIGNAL: [],
    FOLLOW_UP_REQUIRED: [],
  };

  for (const item of routedItems) {
    const category = item.routing?.assignment_category || item.assignment_category;
    if (buckets[category]) buckets[category].push(item);
    else buckets.HIGH_VALUE_ICP.push(item);
  }

  const selected = [];
  const limits = {
    UNFAIR_ADVANTAGE: 5,
    HIGH_VALUE_ICP: 5,
    ROUTE_CLUSTER: 5,
    WALK_IN_OPPORTUNITY: 5,
  };

  for (const [category, limit] of Object.entries(limits)) {
    selected.push(...buckets[category].slice(0, limit));
  }
  selected.push(...buckets.WARM_SIGNAL);
  selected.push(...buckets.FOLLOW_UP_REQUIRED);

  const dueDates = distributeDueDates(selected.length, { today: baseDate });
  return selected.map((item, index) => {
    const routing = item.routing || item;
    return {
      prospectId: item.prospectId || null,
      ...routing,
      deadline: dueDates[index] || baseDate,
      required_debrief: routing.recommended_motion !== 'EMAIL_LED',
    };
  });
}

module.exports = {
  REQUIRED_LOG_FIELDS,
  computeAoFitScore,
  classifyMotion,
  classifyAssignmentCategory,
  routeProspect,
  buildWeeklyListMix,
  companyName,
  locationText,
  inServiceArea,
};
