'use strict';

/**
 * SPEC-107 — evidence-grounded recommendation composition.
 *
 * Consumes a SPEC-105 operating-evidence bundle (plus optional Blueprint
 * understanding and capability/policy state) and reasons to a bounded
 * advisory recommendation. Does not execute, persist, or enable agents.
 *
 * Retrieve → reason → recommend. Not a new recommendation engine.
 */

const CAPABILITY_STATUS = Object.freeze({
  AVAILABLE: 'available',
  DISABLED: 'disabled',
  BLOCKED: 'blocked',
  NOT_READY: 'not_ready',
  UNKNOWN: 'unknown',
});

const KNOWN_AGENTS = Object.freeze([
  'scout',
  'emmett',
  'paige',
  'riley',
  'sam',
  'cal',
  'faye',
  'link',
  'ivy',
  'vera',
]);

const EMAIL_AGENTS = Object.freeze(['emmett']);
const DISCOVERY_AGENTS = Object.freeze(['scout']);

const SUPPLY_THIN_QUALIFIED = 5;
const SUPPLY_HEALTHY_QUALIFIED = 15;
const SCOUT_HEALTHY_MATCHED = 10;

function present(text) {
  return String(text || '')
    .replace(/\s{2,}/g, ' ')
    .trim();
}

function asList(value) {
  if (Array.isArray(value)) return value.map((v) => String(v || '').toLowerCase()).filter(Boolean);
  return [];
}

function parseWhen(value) {
  if (!value) return null;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? null : d;
}

function formatDay(value) {
  const d = parseWhen(value);
  if (!d) return present(value);
  return d.toISOString().slice(0, 10);
}

function firstDefined(...values) {
  for (const value of values) {
    if (value !== undefined && value !== null) return value;
  }
  return undefined;
}

function statusForAgent(name, input = {}) {
  const enabled = input.enabledAgents;
  if (!Array.isArray(enabled)) return CAPABILITY_STATUS.UNKNOWN;
  if (!enabled.includes(name)) return CAPABILITY_STATUS.DISABLED;
  const readiness = input.readiness && typeof input.readiness === 'object' ? input.readiness : {};
  if (readiness[name] === false || readiness[name] === 'not_ready') {
    return CAPABILITY_STATUS.NOT_READY;
  }
  if (name === 'emmett' && input.autosendEnabled === false) {
    return CAPABILITY_STATUS.BLOCKED;
  }
  if (input.blockedAgents && input.blockedAgents.includes(name)) {
    return CAPABILITY_STATUS.BLOCKED;
  }
  return CAPABILITY_STATUS.AVAILABLE;
}

function normalizeCapabilityPolicy(raw = {}) {
  if (!raw || raw.available === false) {
    return {
      known: false,
      enabledAgents: null,
      autosendEnabled: null,
      revenueFlags: {},
      agents: Object.fromEntries(KNOWN_AGENTS.map((name) => [name, { name, status: CAPABILITY_STATUS.UNKNOWN }])),
    };
  }

  const enabledAgents = Array.isArray(raw.enabled_agents)
    ? asList(raw.enabled_agents)
    : Array.isArray(raw.enabledAgents)
      ? asList(raw.enabledAgents)
      : null;
  const autosendEnabled = firstDefined(raw.autosend_enabled, raw.autosendEnabled);
  const readiness = raw.readiness && typeof raw.readiness === 'object' ? raw.readiness : {};
  const blockedAgents = asList(raw.blocked_agents || raw.blockedAgents);
  const revenueFlags = raw.revenueFlags || raw.revenue_flags || {};

  const agents = {};
  for (const name of KNOWN_AGENTS) {
    agents[name] = {
      name,
      status: statusForAgent(name, {
        enabledAgents,
        autosendEnabled,
        readiness,
        blockedAgents,
      }),
    };
  }

  return {
    known: enabledAgents != null || autosendEnabled != null,
    enabledAgents,
    autosendEnabled: autosendEnabled == null ? null : Boolean(autosendEnabled),
    revenueFlags,
    agents,
    emailMotionActive: raw.emailMotionActive === true,
  };
}

function classifySupply(prospects = {}, scout = {}) {
  const qualified = Number(prospects.qualified || 0);
  const total = Number(prospects.total || 0);
  const matched = Number(scout.matched || 0);
  const scoutHealthy = matched >= SCOUT_HEALTHY_MATCHED || scout.healthy === true;
  const scoutThin = scout.available === false || (matched === 0 && scout.healthy !== true);

  if (qualified <= SUPPLY_THIN_QUALIFIED && (total <= SUPPLY_THIN_QUALIFIED || scoutThin)) {
    return 'thin';
  }
  if (qualified >= SUPPLY_HEALTHY_QUALIFIED || (qualified >= 10 && scoutHealthy)) {
    return 'healthy';
  }
  if (total >= SUPPLY_HEALTHY_QUALIFIED && qualified >= 8) return 'healthy';
  return 'uncertain';
}

function followUpTemporal(followUp = {}, now) {
  if (!followUp || (!followUp.expectedAt && !followUp.executed && !followUp.current)) {
    return { kind: 'none', expectedAt: null, executed: false };
  }
  if (followUp.executed) {
    return { kind: 'completed', expectedAt: followUp.expectedAt || null, executed: true };
  }
  if (!followUp.expectedAt) {
    return { kind: 'planned', expectedAt: null, executed: false };
  }
  const when = parseWhen(followUp.expectedAt);
  if (!when) {
    return { kind: 'planned', expectedAt: followUp.expectedAt, executed: false };
  }
  if (when.getTime() > now.getTime()) {
    return { kind: 'planned_future', expectedAt: followUp.expectedAt, executed: false };
  }
  return { kind: 'planned_due', expectedAt: followUp.expectedAt, executed: false };
}

const EMAIL_CURRENT_STATUS = new Set([
  'active',
  'in_progress',
  'sending',
  'executing',
  'current',
]);

function isEmailChannelRow(row) {
  if (!row || typeof row !== 'object') return false;
  const channel = String(row.channel || row.action_type || row.source || row.agent_name || '');
  return /email|emmett|brevo/i.test(channel);
}

function isCurrentEmailRow(row) {
  if (!row || typeof row !== 'object') return false;
  if (row.current === true || row.is_current === true || row.emailMotionCurrent === true) {
    return true;
  }
  const status = String(row.status || row.execution_status || row.operational_state || '').toLowerCase();
  return EMAIL_CURRENT_STATUS.has(status);
}

function emailMissions(bundle = {}) {
  return (Array.isArray(bundle.missions) ? bundle.missions : []).filter((mission) =>
    /email|emmett|outbound/i.test(String((mission && (mission.title || mission.objectiveText)) || ''))
  );
}

/**
 * Historical email activity is not current execution.
 * Missions are planned/intent, not active outbound.
 */
function assessEmailMotion(bundle = {}, capability = {}) {
  if (bundle.emailMotionActive === true || capability.emailMotionActive === true) {
    return {
      kind: 'active',
      epistemic: 'verified',
      current: true,
      historicalCount: 0,
      reason: 'current_execution_flag',
    };
  }
  const touchpoints = (bundle.activity && bundle.activity.touchpoints) || [];
  const activity = (bundle.activity && bundle.activity.activity) || [];
  const emailRows = [...touchpoints, ...activity].filter(isEmailChannelRow);
  const currentRows = emailRows.filter(isCurrentEmailRow);
  if (currentRows.length) {
    return {
      kind: 'active',
      epistemic: 'verified',
      current: true,
      historicalCount: emailRows.length - currentRows.length,
      reason: 'current_execution_rows',
    };
  }
  if (emailRows.length) {
    return {
      kind: 'historical',
      epistemic: 'verified',
      current: false,
      historicalCount: emailRows.length,
      reason: 'historical_touchpoints',
    };
  }
  const planned = emailMissions(bundle);
  if (planned.length) {
    return {
      kind: 'planned',
      epistemic: 'verified',
      current: false,
      historicalCount: 0,
      missions: planned,
      reason: 'mission_intent',
    };
  }
  const email = primaryEmailCapability(normalizeCapabilityPolicy(capability));
  if (email.status === CAPABILITY_STATUS.DISABLED) {
    return {
      kind: 'disabled',
      epistemic: 'verified',
      current: false,
      historicalCount: 0,
      reason: 'capability_disabled',
    };
  }
  return {
    kind: 'not_recorded',
    epistemic: 'not_recorded',
    current: false,
    historicalCount: 0,
    reason: 'no_current_execution',
  };
}

function hasEmailMotion(bundle = {}, capability = {}) {
  return assessEmailMotion(bundle, capability).current === true;
}

function primaryEmailCapability(capability) {
  for (const name of EMAIL_AGENTS) {
    if (capability.agents && capability.agents[name]) return capability.agents[name];
  }
  return { name: 'emmett', status: CAPABILITY_STATUS.UNKNOWN };
}

function primaryDiscoveryCapability(capability) {
  for (const name of DISCOVERY_AGENTS) {
    if (capability.agents && capability.agents[name]) return capability.agents[name];
  }
  return { name: 'scout', status: CAPABILITY_STATUS.UNKNOWN };
}

function assembleOperatingState(bundle = {}, extras = {}) {
  const now = extras.now instanceof Date ? extras.now : new Date();
  const campaign = bundle.campaign || {};
  const progress = campaign.progress || {};
  const prospects = (bundle.prospects && bundle.prospects.counts) || {};
  const scoutIntel = (bundle.scout && bundle.scout.intelligence) || {};
  const scoutMatched = Number(
    (scoutIntel.counts && (scoutIntel.counts.matched || scoutIntel.counts.considered)) ||
      (scoutIntel.companies || []).length ||
      (bundle.scout && bundle.scout.state && bundle.scout.state.opportunityCount) ||
      0
  );
  const capability = normalizeCapabilityPolicy(bundle.capability || extras.capability || {});
  const followUp = followUpTemporal(
    (bundle.operatorAttested && bundle.operatorAttested.followUp) || {},
    now
  );
  const mail = (bundle.operatorAttested && bundle.operatorAttested.mail) || {};
  const understanding = extras.businessUnderstanding || extras.understanding || null;
  const contract = (understanding && understanding.contract) || {};

  const aoLeads = Number(progress.seeded_in_ao || (campaign.leads || []).length || 0);
  const walkthroughs = Number(progress.walkthrough_requests || 0);
  const qualified = Number(prospects.qualified || 0);
  const totalProspects = Number(prospects.total || 0);
  const jobs = Number((bundle.outcomes && bundle.outcomes.jobs) || 0);
  const payments = Number((bundle.outcomes && bundle.outcomes.payments) || 0);

  const state = {
    now,
    campaignName: progress.campaign_name || campaign.campaignName || 'Campaign 001',
    aoLeads,
    walkthroughs,
    mailExecuted: campaign.mailExecuted === true,
    mailAttestedAt: mail.occurredAt || null,
    followUp,
    prospects: {
      total: totalProspects,
      qualified,
    },
    scout: {
      available: bundle.scout ? bundle.scout.available !== false : false,
      matched: scoutMatched,
      healthy: scoutMatched >= SCOUT_HEALTHY_MATCHED,
    },
    missions: Array.isArray(bundle.missions) ? bundle.missions : [],
    objectives: Array.isArray(bundle.objectives) ? bundle.objectives : [],
    outcomes: { jobs, payments, walkthroughs },
    emailMotion: assessEmailMotion(bundle, capability),
    emailMotionActive: hasEmailMotion(bundle, capability),
    capability,
    supply: classifySupply(
      { total: totalProspects, qualified },
      { matched: scoutMatched, available: bundle.scout ? bundle.scout.available !== false : false }
    ),
    businessName: contract.companyName || '',
    commercialPreference: Boolean(
      contract.commercialPreference ||
        (understanding && understanding.commercialPreference)
    ),
    items: bundle.items || [],
    hasAnyOperatingSignal: Boolean(
      aoLeads ||
        totalProspects ||
        scoutMatched ||
        mail.occurredAt ||
        followUp.kind !== 'none' ||
        (bundle.missions || []).length ||
        jobs ||
        payments
    ),
  };
  return applyWorkingModelCorrections(state, extras);
}

function applyWorkingModelCorrections(state, extras = {}) {
  const retracted = Array.isArray(extras.retractedPremises)
    ? extras.retractedPremises.map(String)
    : [];
  if (extras.operatorDeniedEmailActive === true || retracted.includes('email_motion')) {
    state.emailMotion = {
      kind: 'not_recorded',
      epistemic: 'operator_attested',
      current: false,
      historicalCount: (state.emailMotion && state.emailMotion.historicalCount) || 0,
      reason: 'operator_denied_current_email',
      denied: true,
    };
    state.emailMotionActive = false;
  }
  return state;
}

function factLines(state) {
  const lines = [];
  if (state.aoLeads > 0) {
    lines.push(
      `FACT: ${state.campaignName} has ${state.aoLeads} AO lead${state.aoLeads === 1 ? '' : 's'} attributed in PulseForge.`
    );
  }
  if (state.prospects.total > 0) {
    lines.push(
      `FACT: ${state.prospects.total} prospect${state.prospects.total === 1 ? '' : 's'} exist${state.prospects.total === 1 ? 's' : ''}${
        state.prospects.qualified
          ? `, ${state.prospects.qualified} at or above qualification threshold`
          : ''
      }.`
    );
  }
  if (state.scout.matched > 0) {
    lines.push(
      `FACT: Scout intelligence already has ${state.scout.matched} in-scope compan${state.scout.matched === 1 ? 'y' : 'ies'} on file.`
    );
  }
  if (state.walkthroughs > 0) {
    lines.push(`FACT: ${state.walkthroughs} walkthrough-request state${state.walkthroughs === 1 ? '' : 's'} are recorded.`);
  } else if (state.hasAnyOperatingSignal) {
    lines.push('FACT: No walkthrough-request states are recorded yet.');
  }
  if (!state.outcomes.jobs && !state.outcomes.payments && state.hasAnyOperatingSignal) {
    lines.push('FACT: No conversion, job, or payment outcomes are recorded yet.');
  }
  if (state.mailExecuted) {
    lines.push(`FACT: System-verified evidence records that ${state.campaignName} was mailed.`);
  }
  if (state.mailAttestedAt) {
    lines.push(
      `OPERATOR ATTESTED: ${state.campaignName} was reported as physically mailed on ${formatDay(state.mailAttestedAt)}.`
    );
  }
  if (state.followUp.kind === 'completed') {
    lines.push(`FACT: Follow-up on ${state.campaignName} has recorded execution evidence.`);
  } else if (state.followUp.expectedAt) {
    lines.push(
      `PLANNED: AO follow-up was expected ${formatDay(state.followUp.expectedAt)}. That is not recorded execution.`
    );
  }
  return lines;
}

function alreadyInMotionLines(state) {
  const lines = [];
  if (state.followUp.kind === 'planned_future') {
    lines.push(
      `${state.campaignName} AO follow-up is already scheduled for ${formatDay(state.followUp.expectedAt)} and has an owner. Do not treat that as a newly discovered initiative.`
    );
  } else if (state.followUp.kind === 'planned_due') {
    lines.push(
      `${state.campaignName} AO follow-up was planned/expected for ${formatDay(state.followUp.expectedAt)} and is waiting for confirmation. I do not have execution evidence that it occurred.`
    );
  } else if (state.followUp.kind === 'completed') {
    lines.push(`${state.campaignName} follow-up already has recorded execution.`);
  } else if (state.followUp.kind === 'planned') {
    lines.push(`${state.campaignName} follow-up is planned but not recorded as completed.`);
  }
  if (state.emailMotion && state.emailMotion.kind === 'active' && state.emailMotion.current === true) {
    lines.push('An outbound email motion is already active. That is current execution evidence, not historical sends.');
  } else if (state.emailMotion && state.emailMotion.kind === 'historical') {
    lines.push(
      `Historical email touchpoints exist (${state.emailMotion.historicalCount}), but they do not establish that email outbound is currently active.`
    );
  } else if (state.emailMotion && state.emailMotion.kind === 'planned') {
    lines.push(
      'An email-related mission is on file as planned/intent work. A mission is not recorded execution.'
    );
  } else if (state.emailMotion && state.emailMotion.kind === 'disabled') {
    lines.push('Emmett exists but is disabled for this tenant. Disabled is not active.');
  }
  if (state.missions.length) {
    const titles = state.missions
      .slice(0, 3)
      .map((m) => present(m.title || m.objectiveText || 'mission'))
      .filter(Boolean);
    lines.push(
      `${state.missions.length} mission${state.missions.length === 1 ? '' : 's'} on file as planned or tracked work${
        titles.length ? ` (${titles.join('; ')})` : ''
      } — not proof of external execution.`
    );
  }
  if (!lines.length) {
    lines.push('No separate execution stream is clearly already underway beyond the recorded inventory.');
  }
  return lines;
}

function capabilityConstraint(email, state) {
  const label = email.name === 'emmett' ? 'Emmett' : present(email.name) || 'Outbound email';
  if (email.status === CAPABILITY_STATUS.DISABLED) {
    return `${label} exists but is currently disabled for this tenant (not in enabled agents). Autosend is ${
      state.capability.autosendEnabled === false ? 'disabled' : state.capability.autosendEnabled === true ? 'enabled' : 'unconfirmed'
    }. Activation would require an explicit operator decision and a readiness gate before any send. This recommendation does not enable ${label} or start sending.`;
  }
  if (email.status === CAPABILITY_STATUS.BLOCKED) {
    return `${label} is enabled but policy currently blocks sending (autosend disabled or an equivalent send lock). Max cannot start sending from this recommendation.`;
  }
  if (email.status === CAPABILITY_STATUS.NOT_READY) {
    return `${label} is enabled but readiness conditions are not met. Treat this as a preparation/readiness decision, not authorization to send.`;
  }
  if (email.status === CAPABILITY_STATUS.AVAILABLE) {
    return `${label} is available, but this remains advisory. Max will not launch sequences or change policy from a recommendation.`;
  }
  return `I do not have enough capability evidence to treat outbound email as ready. I will not claim it can execute immediately.`;
}

function recommendEvaluateOutbound(state, email) {
  const label = email.name === 'emmett' ? 'Emmett' : present(email.name) || 'outbound email';
  return {
    bottleneck: 'outbound_capacity',
    focus: `Evaluate and prepare controlled ${label} activation as the next acquisition-capacity decision.`,
    inference:
      'Prospect supply is probably not the immediate bottleneck, and human AO follow-up is already in motion or scheduled. The missing motion is a controlled outbound email channel — not another discovery pass.',
    learn: 'Whether a bounded email motion can work the existing qualified inventory without duplicating AO follow-up, and what readiness remains before any send.',
    email,
  };
}

function recommendSupply(state) {
  const scout = primaryDiscoveryCapability(state.capability);
  const scoutNote =
    scout.status === CAPABILITY_STATUS.AVAILABLE
      ? 'Scout is available if you want additional accounts that are not already recorded.'
      : scout.status === CAPABILITY_STATUS.DISABLED
        ? 'Scout is currently disabled, so expanding supply would itself be an operator enablement decision.'
        : 'I will not invent a discovery motion I cannot place against capability state.';
  return {
    bottleneck: 'prospect_supply',
    focus: 'Relieve prospect-supply scarcity before adding another outreach channel.',
    inference:
      'Qualified inventory is thin and existing Scout intelligence is not a healthy surplus. Activating outbound email would not be the highest-leverage move from this state.',
    learn: 'Whether a small, in-scope discovery pass can produce enough qualified accounts to support a repeatable commercial loop.',
    scoutNote,
    email: primaryEmailCapability(state.capability),
  };
}

function recommendConfirmation(state) {
  return {
    bottleneck: 'confirmation',
    focus: `Confirm whether the planned ${state.campaignName} follow-up actually began, and capture dispositions.`,
    inference:
      'An expected date is not execution. Until confirmation exists, I will not treat follow-up as completed work.',
    learn: 'Whether the AO motion is producing conversations, stalls, or no contact — the first measurement point for the commercial loop.',
    email: primaryEmailCapability(state.capability),
  };
}

function recommendNextConstraint(state) {
  return {
    bottleneck: 'outcomes',
    focus: 'Measure conversion from work already in motion — walkthrough requests, dispositions, and closed-loop outcomes — rather than activating another channel.',
    inference:
      'Prospect supply looks adequate and a current outbound email motion is executing. Adding activation work would duplicate capacity that already exists.',
    learn: 'Which in-motion step is failing to produce walkthroughs or converted accounts.',
    email: primaryEmailCapability(state.capability),
  };
}

function recommendMeasureFollowUp(state) {
  return {
    bottleneck: 'measurement',
    focus: `Treat the scheduled ${state.campaignName} follow-up as the measurement point and make sure dispositions are captured.`,
    inference:
      'Follow-up already has an owner. The leverage is learning from that motion, not launching a parallel discovery initiative.',
    learn: 'Whether AO follow-up converts attributed leads into conversations and walkthrough requests.',
    email: primaryEmailCapability(state.capability),
  };
}

function recommendInspect(state) {
  return {
    bottleneck: 'unknown',
    focus: 'Make the current operating picture inspectable before choosing a new motion.',
    inference: 'Recorded operating evidence is too thin to name a highest-leverage next focus without inventing state.',
    learn: 'Which of supply, in-motion execution, or outbound capacity is actually missing.',
    email: primaryEmailCapability(state.capability),
    missing: true,
  };
}

function reasonOverOperatingState(state) {
  const email = primaryEmailCapability(state.capability);

  if (!state.hasAnyOperatingSignal) {
    return recommendInspect(state);
  }

  if (state.supply === 'thin') {
    return recommendSupply(state);
  }

  if (
    state.supply === 'healthy' &&
    !state.emailMotionActive &&
    (email.status === CAPABILITY_STATUS.DISABLED ||
      email.status === CAPABILITY_STATUS.BLOCKED ||
      email.status === CAPABILITY_STATUS.NOT_READY)
  ) {
    return recommendEvaluateOutbound(state, email);
  }

  if (state.emailMotionActive && (email.status === CAPABILITY_STATUS.AVAILABLE || email.status === CAPABILITY_STATUS.BLOCKED)) {
    return recommendNextConstraint(state);
  }

  if (state.followUp.kind === 'planned_due') {
    return recommendConfirmation(state);
  }

  if (state.followUp.kind === 'planned_future' && state.supply === 'healthy') {
    return recommendMeasureFollowUp(state);
  }

  if (state.supply === 'healthy' && !state.emailMotionActive && email.status === CAPABILITY_STATUS.AVAILABLE) {
    return {
      bottleneck: 'outbound_capacity',
      focus: 'Use the already-available outbound email capability on existing qualified inventory — as an operator-authorized, controlled motion, not an autonomous send.',
      inference:
        'Prospect discovery is not the obvious bottleneck. A channel that can work existing qualified accounts is available, but Max will not execute it from this recommendation.',
      learn: 'Whether a bounded email motion produces conversations from inventory that is not currently being worked.',
      email,
    };
  }

  if (state.followUp.kind === 'planned_due' || state.followUp.kind === 'planned') {
    return recommendConfirmation(state);
  }

  return recommendInspect(state);
}

function formatRecommendationProse(decision, state) {
  const why = [...factLines(state)];
  if (decision.inference) {
    why.push(`INFERENCE: ${decision.inference}`);
  }
  why.push(`RECOMMENDATION: ${decision.focus}`);

  const constraint = [];
  if (decision.bottleneck === 'confirmation' || state.followUp.kind === 'planned_due') {
    constraint.push(
      `Follow-up expected ${state.followUp.expectedAt ? formatDay(state.followUp.expectedAt) : 'on the reported date'} remains planned/expected until execution is confirmed. I will not say it occurred.`
    );
  }
  if (decision.scoutNote) constraint.push(decision.scoutNote);
  constraint.push(capabilityConstraint(decision.email || primaryEmailCapability(state.capability), state));
  constraint.push('This is advisory only. Max will not enable agents, change autosend, launch sequences, create campaigns, or mark planned work completed.');

  const parts = [
    'RECOMMENDATION',
    decision.focus,
    '',
    'WHY NOW',
    why.join('\n'),
    '',
    "WHAT'S ALREADY IN MOTION",
    alreadyInMotionLines(state).join('\n'),
    '',
    'CONSTRAINT / DECISION',
    constraint.join('\n'),
    '',
    "WHAT WE'LL LEARN",
    decision.learn,
  ];

  return parts.join('\n');
}

function composeEvidenceGroundedRecommendation(bundle, extras = {}) {
  const state = assembleOperatingState(bundle, extras);
  const decision = reasonOverOperatingState(state);
  const premises = collectPremises(state, decision);
  return {
    prose: formatRecommendationProse(decision, state),
    state,
    decision,
    premises,
    lastClaim: pickLastClaim(premises),
    executed: false,
    recommend: true,
  };
}

function collectPremises(state, decision) {
  const premises = [];
  const email = state.emailMotion || { kind: 'not_recorded', current: false };
  premises.push({
    id: 'email_motion',
    topic: 'email_motion',
    text: email.current
      ? 'An outbound email motion is already active.'
      : email.kind === 'historical'
        ? 'Historical email activity exists; current outbound email execution is not verified.'
        : email.kind === 'planned'
          ? 'An email campaign exists as planned or intended work, not current execution.'
          : email.kind === 'disabled'
            ? 'Emmett exists but is disabled for this tenant.'
            : 'A currently active outbound email motion is not verified.',
    kind: email.current ? 'observed' : email.kind === 'disabled' ? 'exists_disabled' : email.kind,
    support: email.current ? 'supported' : 'unknown',
    status: email.current ? 'supported' : 'unknown',
    epistemic: email.current ? 'observed' : email.kind === 'disabled' ? 'exists_disabled' : email.kind,
    evidence: email,
  });
  if (state.mailAttestedAt || state.mailExecuted) {
    premises.push({
      id: 'mail',
      topic: 'mail',
      text: state.mailAttestedAt
        ? `${state.campaignName} was operator-reported as mailed ${formatDay(state.mailAttestedAt)}.`
        : `${state.campaignName} was mailed.`,
      kind: state.mailAttestedAt ? 'operator_attested' : 'verified',
      support: 'supported',
      status: 'supported',
      epistemic: state.mailAttestedAt ? 'operator_attested' : 'verified',
      evidence: { occurredAt: state.mailAttestedAt, mailExecuted: state.mailExecuted },
    });
  }
  if (state.followUp && state.followUp.kind !== 'none') {
    premises.push({
      id: 'follow_up',
      topic: 'follow_up',
      text:
        state.followUp.kind === 'completed'
          ? `${state.campaignName} follow-up has recorded execution.`
          : `${state.campaignName} follow-up is planned and owned; completion is not yet recorded.`,
      kind: state.followUp.kind === 'completed' ? 'verified' : 'planned',
      support: 'supported',
      status: 'supported',
      epistemic: state.followUp.kind === 'completed' ? 'verified' : 'planned',
      evidence: state.followUp,
    });
  }
  if (decision) {
    premises.push({
      id: 'next_constraint',
      topic: 'next_constraint',
      text: decision.focus || decision.recommendation,
      kind: 'inferred',
      support: 'inferred',
      status: 'inferred',
      epistemic: 'inferred',
      evidence: [],
    });
  }
  return premises;
}

function pickLastClaim(premises) {
  const list = Array.isArray(premises) ? premises : [];
  const email = list.find((p) => p.topic === 'email_motion' && p.support === 'supported');
  if (email) return email;
  const mail = list.find((p) => p.topic === 'mail');
  if (mail) return mail;
  const follow = list.find((p) => p.topic === 'follow_up');
  if (follow) return follow;
  return list.find((p) => p.topic === 'email_motion') || list[0] || null;
}

module.exports = {
  CAPABILITY_STATUS,
  KNOWN_AGENTS,
  normalizeCapabilityPolicy,
  assembleOperatingState,
  reasonOverOperatingState,
  composeEvidenceGroundedRecommendation,
  classifySupply,
  followUpTemporal,
  assessEmailMotion,
  applyWorkingModelCorrections,
  collectPremises,
  pickLastClaim,
  hasEmailMotion,
};
