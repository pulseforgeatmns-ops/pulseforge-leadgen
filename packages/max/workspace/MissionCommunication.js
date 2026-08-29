'use strict';

/**
 * SPEC-121 — Mission-Oriented Communication.
 * External operator interface: Mission → Stage → Status → Next Action → Decision.
 * Internal reasoning (Known / Inference / Unknown / Evidence) stays available on demand.
 */

const {
  formatDiscoveryResultsLines,
} = require('../../acquisition-mission/DiscoveryPresentation');
const {
  formatInvestigationContinuationLines,
} = require('../../acquisition-mission/InvestigationContinuationPresentation');

const REASONING_MARKER = '▼ Show reasoning';
const REASONING_EXPANDED_MARKER = '▲ Hide reasoning';

/**
 * @param {string} question
 * @returns {boolean}
 */
function looksLikeReasoningRequest(question) {
  const q = String(question || '').trim().toLowerCase();
  return (
    /\bshow\s+(?:me\s+)?(?:your\s+)?reasoning\b/.test(q) ||
    /\bexplain\s+(?:your\s+)?(?:reasoning|thinking)\b/.test(q) ||
    /\bhow\s+did\s+you\s+(?:reason|decide|think)\b/.test(q) ||
    /^(?:why\??|why\s+(?:are|is|do|does|did|would|should)\b)/.test(q)
  );
}

/**
 * @param {object} [input]
 * @returns {{ known: string[], inference: string[], unknown: string[], evidenceNeeded: string[], confidence: number|null }}
 */
function buildReasoningEvidence(input = {}) {
  return {
    known: Array.isArray(input.known)
      ? input.known.filter(Boolean).map(String)
      : [],
    inference: Array.isArray(input.inference)
      ? input.inference.filter(Boolean).map(String)
      : [],
    unknown: Array.isArray(input.unknown)
      ? input.unknown.filter(Boolean).map(String)
      : [],
    evidenceNeeded: Array.isArray(input.evidenceNeeded)
      ? input.evidenceNeeded.filter(Boolean).map(String)
      : [],
    confidence:
      input.confidence != null && Number.isFinite(Number(input.confidence))
        ? Number(input.confidence)
        : null,
  };
}

/**
 * @param {ReturnType<typeof buildReasoningEvidence>} evidence
 * @returns {string}
 */
function formatReasoningDisclosure(evidence) {
  if (!evidence) return '';
  const sections = [];
  if (evidence.known.length) {
    sections.push('Known', '', ...evidence.known.map((k) => `• ${k}`), '');
  }
  if (evidence.inference.length) {
    sections.push('Inference', '', ...evidence.inference.map((i) => `• ${i}`), '');
  }
  if (evidence.unknown.length) {
    sections.push('Unknown', '', ...evidence.unknown.map((u) => `• ${u}`), '');
  }
  if (evidence.evidenceNeeded.length) {
    sections.push(
      'Evidence Needed',
      '',
      ...evidence.evidenceNeeded.map((e) => `• ${e}`),
      ''
    );
  }
  if (evidence.confidence != null) {
    sections.push(`Confidence: ${evidence.confidence.toFixed(2)}`);
  }
  return sections.join('\n').trim();
}

/**
 * @param {object} [input]
 */
function buildMissionCommunication(input = {}) {
  return {
    headline: input.headline || 'Mission Updated',
    mission: input.mission || null,
    objective: input.objective || null,
    status: input.status || null,
    stage: input.stage || null,
    progress: input.progress != null ? Number(input.progress) : null,
    health: input.health || null,
    waitingOn: input.waitingOn || null,
    confidence:
      input.confidence != null && Number.isFinite(Number(input.confidence))
        ? Number(input.confidence)
        : null,
    currentUnderstanding: Array.isArray(input.currentUnderstanding)
      ? input.currentUnderstanding
      : [],
    nextStep: input.nextStep || null,
    operatorDecision: input.operatorDecision || null,
    evidenceStatus: input.evidenceStatus || null,
    sources: Array.isArray(input.sources) ? input.sources : [],
    reasoningEvidence: input.reasoningEvidence || null,
    includeReasoningMarker: input.includeReasoningMarker !== false,
    discoveryResults: input.discoveryResults || null,
    investigationContinuationResults: input.investigationContinuationResults || null,
  };
}

/**
 * @param {ReturnType<typeof buildMissionCommunication>} comm
 * @param {object} [opts]
 * @param {boolean} [opts.includeReasoning]
 * @param {boolean} [opts.explicitReasoningRequest]
 */
function formatMissionProse(comm, opts = {}) {
  const includeReasoning =
    opts.includeReasoning === true || opts.explicitReasoningRequest === true;
  const lines = [];

  lines.push(comm.headline || 'Mission Updated');
  lines.push('');

  if (comm.mission) {
    lines.push(comm.mission);
    lines.push('');
  }

  if (comm.objective) {
    lines.push('Objective');
    lines.push('');
    lines.push(comm.objective);
    lines.push('');
  }

  if (comm.status) {
    lines.push('Status');
    lines.push('');
    lines.push(comm.status);
    lines.push('');
  }

  if (comm.stage) {
    lines.push('Stage');
    lines.push('');
    lines.push(comm.stage);
    lines.push('');
  }

  if (comm.progress != null && Number.isFinite(comm.progress)) {
    lines.push('Progress');
    lines.push('');
    lines.push(`${Math.round(comm.progress)}%`);
    lines.push('');
  }

  if (comm.confidence != null) {
    lines.push('Confidence');
    lines.push('');
    lines.push(comm.confidence.toFixed(2));
    lines.push('');
  }

  if (comm.health) {
    lines.push('Health');
    lines.push('');
    lines.push(comm.health);
    lines.push('');
  }

  if (comm.waitingOn) {
    lines.push('Waiting On');
    lines.push('');
    lines.push(comm.waitingOn);
    lines.push('');
  }

  if (comm.currentUnderstanding.length) {
    lines.push('Current Understanding');
    lines.push('');
    for (const item of comm.currentUnderstanding) {
      const label =
        typeof item === 'string' ? item : item.label || String(item);
      const done = typeof item === 'object' && item.done === false ? false : true;
      lines.push(`${done ? '✓' : '○'} ${label}`);
    }
    lines.push('');
  }

  if (comm.nextStep) {
    lines.push('Next Step');
    lines.push('');
    lines.push(comm.nextStep);
    lines.push('');
  }

  if (comm.investigationContinuationResults) {
    const continuationLines = formatInvestigationContinuationLines(
      comm.investigationContinuationResults
    );
    if (continuationLines.length) {
      lines.push(...continuationLines);
    }
  } else if (comm.discoveryResults) {
    const discoveryLines = formatDiscoveryResultsLines(comm.discoveryResults);
    if (discoveryLines.length) {
      lines.push(...discoveryLines);
    }
  }

  if (comm.operatorDecision) {
    lines.push('Operator Decision');
    lines.push('');
    lines.push(comm.operatorDecision);
    lines.push('');
  }

  if (comm.evidenceStatus) {
    lines.push('Evidence Status');
    lines.push('');
    lines.push(comm.evidenceStatus);
    lines.push('');
    if (comm.sources.length) {
      lines.push('Sources');
      lines.push('');
      lines.push(comm.sources.join(', '));
      lines.push('');
    }
  }

  if (comm.reasoningEvidence && comm.includeReasoningMarker && !includeReasoning) {
    lines.push(REASONING_MARKER);
  } else if (comm.reasoningEvidence && includeReasoning) {
    lines.push(REASONING_EXPANDED_MARKER);
    lines.push('');
    lines.push(formatReasoningDisclosure(comm.reasoningEvidence));
  }

  return lines.join('\n').trim();
}

/**
 * @param {object} structured
 * @param {ReturnType<typeof buildMissionCommunication>} comm
 * @param {object} [opts]
 */
function applyMissionCommunication(structured, comm, opts = {}) {
  const prose = formatMissionProse(comm, opts);
  return {
    ...structured,
    answer: prose,
    reasoning: [],
    metadata: {
      ...(structured.metadata || {}),
      missionCommunication: true,
      missionCommunicationPayload: comm,
      reasoningEvidence: comm.reasoningEvidence || null,
      strictOutputShape: true,
      showReasoningDisclosure: Boolean(comm.reasoningEvidence),
    },
  };
}

/**
 * Build mission-oriented communication for blueprint gap / unknowns turns.
 * @param {object} summary - normalized blueprint summary
 * @param {object} [opts]
 * @param {boolean} [opts.explicitReasoningRequest]
 */
function buildUnknownsMissionCommunication(summary, opts = {}) {
  const presentText =
    typeof opts.presentText === 'function'
      ? opts.presentText
      : (value) => String(value || '').trim();

  const icp = summary.idealCustomers || null;
  const market = summary.geography || summary.targetMarkets || null;
  const goals = summary.campaignGoals || null;
  const metrics = summary.successMetrics || null;

  const knownBits = [];
  if (summary.identity || summary.businessName) {
    knownBits.push(presentText(summary.identity || summary.businessName));
  }
  if (icp) knownBits.push(`ideal customers center on ${icp}`);
  if (market) knownBits.push(`geography is anchored to ${market}`);
  if (goals) knownBits.push(`near-term goal is ${goals}`);
  if (metrics) knownBits.push(`success is judged by ${metrics}`);

  const inferenceBits = [];
  if (icp) {
    inferenceBits.push(
      `starting with ${icp}${market ? ` in ${market}` : ''} is a reasonable first motion`
    );
  } else {
    inferenceBits.push(
      'a tighter commercial acquisition motion is the right shape of first experiment'
    );
  }

  const unknownBits = [];
  if (market) {
    unknownBits.push(
      `which part of the ${market} commercial market will respond best`
    );
  } else {
    unknownBits.push('which part of the commercial market will respond best');
  }
  if (icp && /property|facility/i.test(icp)) {
    unknownBits.push(
      'whether property managers will outperform facility managers'
    );
  } else {
    unknownBits.push('which decision-maker segment will outperform the others');
  }
  unknownBits.push(
    'which acquisition motion will produce the strongest recurring contracts'
  );
  for (const u of summary.unknowns || []) {
    if (u) unknownBits.push(presentText(u));
  }

  const evidenceNeededBits = [
    'expected walkthrough rate, close rate, and acquisition cost from live campaign evidence',
    'live company names, signals, or performance claims before the first experiment runs',
  ];

  const currentUnderstanding = [];
  if (icp) currentUnderstanding.push({ label: 'Target customer defined', done: true });
  if (market) currentUnderstanding.push({ label: 'Geography defined', done: true });
  if (goals || metrics) {
    currentUnderstanding.push({ label: 'Business objective understood', done: true });
  }
  if (!icp) {
    currentUnderstanding.push({ label: 'Ideal customer segment', done: false });
  }
  for (const u of (summary.unknowns || []).slice(0, 2)) {
    if (u) currentUnderstanding.push({ label: presentText(u), done: false });
  }

  const understandingCount = currentUnderstanding.filter((i) => i.done !== false).length;
  const progress =
    currentUnderstanding.length > 0
      ? Math.round((understandingCount / Math.max(currentUnderstanding.length, 4)) * 100)
      : 10;

  const objectiveParts = [];
  if (goals) objectiveParts.push(presentText(goals));
  else if (icp) objectiveParts.push(`acquire recurring clients among ${icp}`);
  else objectiveParts.push('establish a repeatable commercial acquisition motion');
  if (market) objectiveParts.push(`in ${market}`);

  const missionTitle =
    icp && /clean|commercial/i.test(String(icp))
      ? 'Commercial Acquisition Planning'
      : 'Acquisition Mission Planning';

  const reasoningEvidence = buildReasoningEvidence({
    known: knownBits.length
      ? [`From your approved Blueprint: ${knownBits.slice(0, 3).join('; ')}.`]
      : [],
    inference: [
      `${inferenceBits.join('; ')}. That is directional guidance from approved understanding — not observed performance.`,
    ],
    unknown: unknownBits.slice(0, 4),
    evidenceNeeded: [
      `${evidenceNeededBits[0]}. Those gaps are what the first acquisition experiment should help us learn — not reasons to wait.`,
      evidenceNeededBits[1],
    ],
    confidence: 0.84,
  });

  return buildMissionCommunication({
    headline: 'Mission Updated',
    mission: missionTitle,
    objective: objectiveParts.join(' '),
    status: 'Planning',
    stage: 'Discovery',
    progress: Math.max(10, Math.min(progress, 40)),
    health: 'Healthy',
    waitingOn: 'Operator direction',
    confidence: 0.84,
    currentUnderstanding,
    nextStep: icp
      ? 'Scout can identify high-probability operators matching this beachhead.'
      : 'Refine the ideal customer in your Blueprint, then run a focused discovery pass.',
    operatorDecision: icp
      ? 'Approve discovery?'
      : 'Confirm the target segment before discovery?',
    evidenceStatus: '✓ Grounded',
    sources: ['Blueprint'],
    reasoningEvidence,
    includeReasoningMarker: !opts.explicitReasoningRequest,
  });
}

/**
 * Build mission communication from SPEC-022 mission-engine state.
 * @param {object} mission
 * @param {object} [opts]
 */
function buildEngineMissionCommunication(mission, opts = {}) {
  const title = mission.title || 'Mission';
  const stage =
    (mission.progress && mission.progress.currentStage) || mission.status || 'Planning';
  const percent =
    mission.progress && mission.progress.percent != null
      ? mission.progress.percent
      : 0;

  let headline = 'Mission Updated';
  if (opts.created) headline = 'Mission Created';
  else if (mission.status === 'waiting') headline = 'Mission Waiting';
  else if (mission.status === 'failed') headline = 'Mission Blocked';
  else if (mission.status === 'completed' || mission.status === 'reviewed') {
    headline = 'Mission Complete';
  }

  const blockers = [
    ...(Array.isArray(mission.blockingIssues) ? mission.blockingIssues : []),
    ...(mission.stageReview && Array.isArray(mission.stageReview.blockingIssues)
      ? mission.stageReview.blockingIssues
      : []),
  ];

  let nextStep = 'Progress will appear in Operations on the Command Deck.';
  if (mission.status === 'review_required') {
    nextStep = 'Review results in Mission Workspace.';
  } else if (mission.status === 'waiting') {
    nextStep = 'Mission is paused for operator review.';
  } else if (opts.created) {
    nextStep = 'Open Mission Workspace to track progress and approve stages.';
  }

  let operatorDecision = null;
  if (mission.status === 'review_required') {
    operatorDecision = 'Review and approve results?';
  } else if (mission.status === 'waiting') {
    operatorDecision = 'Retry, import a prospect list, or cancel from Mission Workspace?';
  } else if (opts.created) {
    operatorDecision = 'Open Mission Workspace to continue?';
  }

  const currentUnderstanding = [];
  if (mission.discoveryProfile) {
    currentUnderstanding.push({
      label: `Discovery Profile: ${mission.discoveryProfile.name}`,
      done: true,
    });
  }
  if (mission.operatorProspectList && mission.operatorProspectList.injected) {
    currentUnderstanding.push({
      label: `Operator ProspectList imported (${mission.operatorProspectList.prospectCount} prospects). Discovery marked Satisfied (Operator Supplied).`,
      done: true,
    });
  }

  if (
    mission.operatorProspectList &&
    mission.operatorProspectList.promptImport &&
    !mission.operatorProspectList.injected
  ) {
    nextStep = `Detected a ProspectList in your prompt (${mission.operatorProspectList.prospectCount || 'partial'} rows). Open Mission Workspace to Import Prospect List.`;
    operatorDecision = 'Import detected ProspectList?';
  } else if (
    mission.operatorProspectList &&
    mission.operatorProspectList.injected
  ) {
    nextStep =
      'Operator ProspectList imported. Continuing at Business Intelligence.';
  }

  const comm = buildMissionCommunication({
    headline,
    mission: title,
    status: formatMissionStatus(mission.status),
    stage: String(stage),
    progress: percent,
    health: blockers.length ? 'Blocked' : 'Healthy',
    waitingOn:
      mission.status === 'waiting' || mission.status === 'review_required'
        ? 'Operator approval'
        : null,
    confidence:
      mission.confidence != null ? Number(mission.confidence) : 0.8,
    currentUnderstanding,
    nextStep,
    operatorDecision,
    evidenceStatus: '✓ Grounded',
    sources: ['Mission Engine'],
    reasoningEvidence: opts.includeReasoning
      ? buildReasoningEvidence({
          known: [`Mission type: ${mission.type || 'acquisition'}.`],
          inference: (opts.reasoningLines || []).filter(Boolean),
          unknown: blockers.length ? blockers.map(String) : [],
          evidenceNeeded: [],
          confidence:
            mission.confidence != null ? Number(mission.confidence) : 0.8,
        })
      : null,
    includeReasoningMarker: !opts.includeReasoning,
  });

  return comm;
}

/**
 * Build mission communication from SPEC-118 acquisition mission workspace.
 * @param {object} snapshot - inspect() result or answerOperator payload
 * @param {object} [opts]
 */
function buildAcquisitionMissionCommunication(snapshot, opts = {}) {
  const workspace = snapshot.workspace || snapshot.structured || snapshot;
  const mission = snapshot.mission || {};
  const blocker = snapshot.blocker || null;
  const health = snapshot.health || null;
  const inspection = snapshot.inspection || null;
  const missionContext = snapshot.missionContext || null;

  let headline = 'Mission Updated';
  if (opts.kind === 'explain') headline = 'Mission Reason';
  else if (opts.kind === 'inspection') headline = (inspection && inspection.headline) || 'Mission Inspection';
  else if (blocker) headline = 'Mission Blocked';
  else if (workspace.status === 'complete') headline = 'Mission Complete';

  const currentUnderstanding = (workspace.specialists || [])
    .filter((row) => row.state === 'complete' || row.state === 'approved')
    .map((row) => ({
      label: `${row.id}: ${row.label}`,
      done: true,
    }));

  let nextStep = 'Continue in mission workspace.';
  if (opts.kind === 'health' && health && health.summary) {
    nextStep = health.summary;
  } else if (opts.kind === 'blocker' && blocker) {
    nextStep = blocker.reason || blocker.label;
  } else if (workspace.scout && workspace.scout.state === 'ready') {
    nextStep = 'Scout discovery is ready.';
  }

  const comm = buildMissionCommunication({
    headline,
    mission: workspace.title || mission.title || 'Acquisition Mission',
    objective: mission.objective || null,
    status: workspace.status || mission.status || (missionContext && missionContext.stageLabel) || null,
    stage: workspace.stage || mission.stage || (missionContext && missionContext.stage) || null,
    progress:
      workspace.progressPercent != null
        ? workspace.progressPercent
        : (missionContext && missionContext.progress != null ? missionContext.progress : null),
    health: health && health.label ? health.label : (missionContext && missionContext.health) || (blocker ? 'Blocked' : 'Healthy'),
    waitingOn:
      (missionContext && missionContext.waitingOn) ||
      (blocker ? blocker.label || 'Operator approval' : null),
    confidence:
      (missionContext && missionContext.confidence != null ? missionContext.confidence : null) ||
      (mission.confidence != null ? Number(mission.confidence) : null),
    currentUnderstanding,
    nextStep,
    operatorDecision: blocker ? 'Resolve blocker to continue?' : null,
    evidenceStatus: '✓ Mission State',
    sources: ['Mission State'],
    reasoningEvidence: opts.includeReasoning && snapshot.why
      ? buildReasoningEvidence({
          known: (snapshot.why.grounded || []).map(String),
          inference: (snapshot.why.inferred || []).map(String),
          unknown: (snapshot.why.unknown || []).map(String),
          evidenceNeeded: (snapshot.why.evidenceNeeded || []).map(String),
          confidence: snapshot.why.confidence,
        })
      : null,
    includeReasoningMarker: !opts.includeReasoning,
  });

  return comm;
}

function formatMissionStatus(status) {
  const map = {
    requested: 'Requested',
    planning: 'Planning',
    executing: 'Executing',
    waiting: 'Waiting',
    review_required: 'Review Required',
    completed: 'Completed',
    reviewed: 'Reviewed',
    archived: 'Archived',
    failed: 'Failed',
  };
  return map[status] || status;
}

module.exports = {
  REASONING_MARKER,
  REASONING_EXPANDED_MARKER,
  looksLikeReasoningRequest,
  buildReasoningEvidence,
  formatReasoningDisclosure,
  buildMissionCommunication,
  formatMissionProse,
  applyMissionCommunication,
  buildUnknownsMissionCommunication,
  buildEngineMissionCommunication,
  buildAcquisitionMissionCommunication,
  formatMissionStatus,
};
