'use strict';

/**
 * One-time, idempotent correction for LA Endeavors' approved Growth Workspace.
 *
 * The generic Growth Conversation fallback produced recurring-service language
 * that is not applicable to private lending. This correction writes the
 * operator-approved prospecting-only focus directly to the existing approved
 * session while preserving the Blueprint, contacts, and every unrelated state
 * field. A durable marker makes subsequent startups read-only.
 */

const SESSION_ID = 'f4f30991-3dba-4b60-8c31-2cdb39c81ee9';
const CLIENT_ID = 21;
const CORRECTION_VERSION = 1;

function unique(values) {
  return [...new Set((values || []).filter(Boolean).map(String))];
}

function buildArtifacts(state, now) {
  const blueprintId = state.blueprintId || null;
  const blueprintVersion = state.blueprintVersion || null;
  const segmentKey = 'active_real_estate_operators_and_borrowers';
  const segmentDisplay = 'active real-estate operators and borrowers';

  const earlySignals = [
    'Verified evidence of an active or recent real-estate project',
    'Identifiable collateral with clear ownership',
    'A specific rehabilitation or equity-gap capital need',
    'A credible repayment or exit plan',
    'Verified direct contact information for the operator or decision-maker',
  ];

  const validationTarget = {
    kind: 'validation_target',
    title: 'Asset-Backed Borrower Validation Target',
    intent: 'define_validation_target',
    focusSegment: segmentDisplay,
    displaySegment: 'Active Real-Estate Operators and Borrowers',
    target_segment: segmentKey,
    best_fit_subtype:
      'Active real-estate operators nationwide seeking short-term, asset-backed capital for fix-and-flip rehabilitation costs or equity gaps.',
    property_size_or_context:
      'Prioritize fix-and-flip and rehabilitation projects. Consider buy-and-hold only when collateral, ownership, operator experience, capital need, and exit plan are clear.',
    likely_pain_point:
      'A time-sensitive rehabilitation or equity funding gap that flexible private capital can solve with fewer administrative hoops than institutional lenders.',
    credibility_proof_needed:
      'For each prospect: operator identity and role, verified direct contact information, project or recent-deal evidence, property and market, project type, estimated capital need when public, identifiable collateral, and evidence supporting the likely exit plan. Unknowns must be labeled, not invented.',
    early_signals: earlySignals,
    first_30_day_success_definition:
      'Produce 10 new decision-ready borrower prospect records. Success means Levi accepts at least 6 of the 10 as worth contacting. This phase is prospecting and qualification only; no outreach is authorized.',
    cautions:
      'Exclude private lenders and investors from this first lane, distressed or end-of-the-rope borrowers, inactive operators, stale contacts, unclear ownership or collateral, deals without a credible exit, and institutional processes with excessive administrative hoops.',
    sections: {
      bestFirstType: {
        label: 'Best first borrower type',
        body:
          'Active real-estate operators nationwide seeking short-term, asset-backed capital for fix-and-flip rehabilitation costs or equity gaps.',
      },
      propertySizeType: {
        label: 'Project type to pursue first',
        body:
          'Fix-and-flip and rehabilitation projects first. Buy-and-hold is secondary only when collateral, ownership, operator experience, capital need, and exit plan are clear.',
      },
      painPoint: {
        label: 'First pain point to test',
        body:
          'A time-sensitive rehabilitation or equity funding gap that flexible private capital can solve with fewer administrative hoops than institutional lenders.',
      },
      proof: {
        label: 'Decision-ready proof required',
        body:
          'Operator identity and role, verified direct contact information, project or recent-deal evidence, property and market, project type, estimated capital need when public, identifiable collateral, and evidence supporting the likely exit plan. Unknowns must be labeled, not invented.',
      },
      earlySignals: {
        label: 'Qualification signals worth continuing',
        bullets: earlySignals,
      },
      first30Days: {
        label: 'Successful first validation batch',
        body:
          'Produce 10 new decision-ready borrower prospect records, with at least 6 accepted by Levi as worth contacting. Prospecting and qualification only; no outreach.',
      },
      cautions: {
        label: 'Cautions',
        body:
          'Exclude private lenders and investors from this first lane, distressed borrowers, inactive operators, stale contacts, unclear ownership or collateral, deals without a credible exit, and excessive administrative hoops.',
      },
    },
    confidence: 'Directional, not market-validated.',
    confidenceLevel: 'directional',
    marketValidated: false,
    primaryArea: 'United States (nationwide)',
    businessName: 'LA Endeavors',
    blueprintId,
    blueprintVersion,
  };

  const firstSegmentDecision = {
    kind: 'first_segment_decision',
    title: 'First Segment Decision',
    primary_segment: segmentKey,
    secondary_segment: null,
    held_segments: ['individual_private_lenders_and_investors'],
    deprioritized_segments: ['institutional_lending_groups'],
    primarySegmentDisplay: segmentDisplay,
    secondarySegmentDisplay: null,
    rationale:
      'Active real-estate operators and borrowers are the first segment to validate. Individual private lenders and investors remain a separate second lane.',
    cautions: [
      'Directional, not market-validated.',
      'Prospecting and qualification only; no outreach is authorized.',
    ],
    next_step: 'define_validation_target',
    confidence_level: 'directional',
  };

  const firstGrowthPlanPreview = {
    kind: 'first_growth_plan_preview',
    title: 'First Growth Plan Preview',
    intent: 'summarize_first_growth_plan_preview',
    recommended_first_segment:
      'Active real-estate operators and borrowers nationwide seeking rehab or equity-gap capital',
    why_this_segment:
      'This segment directly matches LA Endeavors\' current private-lending and capital-matching focus and can be qualified from public evidence before Levi decides whether to make contact.',
    why_secondary:
      'Individual private lenders and investors remain a separate second lane so the first validation batch measures borrower quality without mixing both sides of the market.',
    first_subtype_to_test: validationTarget.best_fit_subtype,
    early_signals: earlySignals,
    successful_first_30_days: validationTarget.first_30_day_success_definition,
    next_step_before_campaign:
      'Run a prospecting-only validation batch of 10 qualified borrower records for Levi\'s review. Do not launch outreach or contact prospects.',
    credibility_proof_needed: validationTarget.credibility_proof_needed,
    primary_segment: segmentKey,
    secondary_segment: null,
    primarySegmentDisplay: segmentDisplay,
    secondarySegmentDisplay: null,
    primaryArea: 'United States (nationwide)',
    businessName: 'LA Endeavors',
    confidence: 'Directional, not market-validated.',
    confidenceLevel: 'directional',
    marketValidated: false,
    status: 'accepted',
    inputs: {
      hasSegmentRanking: false,
      hasValidationTarget: true,
      hasFirstSegmentDecision: true,
    },
    blueprintId,
    blueprintVersion,
  };

  const readinessReport = {
    kind: 'growth_infrastructure_readiness_report',
    overallStatus: 'ready_for_prospecting_review',
    summary:
      'LA Endeavors is ready for a prospecting-only validation batch. Outreach, campaigns, and automated contact remain disabled.',
    campaignsGenerated: false,
    prospectListsGenerated: false,
    outreachAuthorized: false,
    recommendedSetupSequence: [],
    areas: {
      prospect_capture: {
        id: 'prospect_capture',
        label: 'Prospect capture and review',
        items: {
          review_workspace: {
            id: 'review_workspace',
            status: 'ready',
            statusLabel: 'Ready',
            owner: 'operator',
            priority: 'high',
            evidence:
              'Pulseforge review-only prospect records are available for LA Endeavors.',
          },
        },
      },
      outreach: {
        id: 'outreach',
        label: 'Outreach',
        items: {
          automated_outreach: {
            id: 'automated_outreach',
            status: 'not_applicable',
            statusLabel: 'Not authorized',
            owner: 'client_required',
            priority: 'low',
            evidence: 'Current scope is prospecting and qualification only.',
          },
        },
      },
    },
    createdAt: now,
  };

  return {
    segmentKey,
    segmentDisplay,
    validationTarget,
    firstSegmentDecision,
    firstGrowthPlanPreview,
    readinessReport,
  };
}

function correctedState(input, now = new Date().toISOString()) {
  const state = input && typeof input === 'object' ? { ...input } : {};
  const marker = state.laEndeavorsWorkspaceCorrection;
  if (marker && Number(marker.version) >= CORRECTION_VERSION) {
    return { changed: false, state };
  }

  const artifacts = buildArtifacts(state, now);
  const priorGrowth =
    state.growthConversation && typeof state.growthConversation === 'object'
      ? state.growthConversation
      : {};
  const priorTurns = Array.isArray(priorGrowth.turns) ? priorGrowth.turns : [];
  const correctionMessage =
    'Growth focus corrected for LA Endeavors: prospect active real-estate operators nationwide seeking rehab or equity-gap capital. Initial validation is 10 decision-ready borrower records, with at least 6 accepted by Levi. Prospecting only; no outreach.';
  const turns = priorTurns.some((turn) => turn && turn.correctionVersion === CORRECTION_VERSION)
    ? priorTurns
    : [
        ...priorTurns,
        {
          speaker: 'assistant',
          message: correctionMessage,
          at: now,
          intent: 'manual_operator_correction',
          growth_step: 'complete_first_growth_plan_preview',
          correctionVersion: CORRECTION_VERSION,
        },
      ];

  const growthConversation = {
    ...priorGrowth,
    status: 'preview_ready',
    turns,
    selected_focus_area: 'segment_mix',
    primary_segment: artifacts.segmentKey,
    secondary_segment: null,
    held_segments: ['individual_private_lenders_and_investors'],
    deprioritized_segments: ['institutional_lending_groups'],
    current_growth_step: 'complete_first_growth_plan_preview',
    completed_steps: unique([
      ...(priorGrowth.completed_steps || []),
      'choose_focus_area',
      'select_primary_segment',
      'define_validation_target',
      'summarize_first_growth_plan_preview',
      'complete_first_growth_plan_preview',
    ]),
    validation_target: artifacts.validationTarget,
    validationTarget: artifacts.validationTarget,
    first_segment_decision: artifacts.firstSegmentDecision,
    firstSegmentDecision: artifacts.firstSegmentDecision,
    first_growth_plan_preview: artifacts.firstGrowthPlanPreview,
    firstGrowthPlanPreview: artifacts.firstGrowthPlanPreview,
    focus_accepted: true,
    confidence_level: 'directional',
  };

  const priorWork =
    state.growthWork && typeof state.growthWork === 'object'
      ? state.growthWork
      : {};
  const history = Array.isArray(priorWork.history) ? priorWork.history : [];
  const correctionHistory = history.some(
    (entry) => entry && entry.correctionVersion === CORRECTION_VERSION
  )
    ? history
    : [
        ...history,
        {
          taskId: 'milestone:growth_conversation',
          completedAt: now,
          source: 'operator_correction',
          note: 'Correct private-lending validation target injected for LA Endeavors.',
          correctionVersion: CORRECTION_VERSION,
        },
        {
          taskId: 'milestone:infrastructure_readiness',
          completedAt: now,
          source: 'operator_correction',
          note: 'Prospecting review is ready; outreach remains disabled.',
          correctionVersion: CORRECTION_VERSION,
        },
        {
          taskId: 'milestone:campaign_ready',
          completedAt: now,
          source: 'operator_correction',
          note: 'Next objective fixed to a prospecting-only 10-record borrower validation batch.',
          correctionVersion: CORRECTION_VERSION,
        },
      ];

  return {
    changed: true,
    state: {
      ...state,
      growthConversation,
      validationTarget: artifacts.validationTarget,
      firstSegmentDecision: artifacts.firstSegmentDecision,
      firstGrowthPlanPreview: artifacts.firstGrowthPlanPreview,
      growthInfrastructureReadinessReport: artifacts.readinessReport,
      infrastructureReadiness: {
        status: 'report_ready',
        report: artifacts.readinessReport,
        completedAt: now,
        source: 'operator_correction',
      },
      growthWork: {
        ...priorWork,
        completedTaskIds: unique([
          ...(priorWork.completedTaskIds || []),
          'milestone:growth_conversation',
          'milestone:infrastructure_readiness',
          'milestone:campaign_ready',
        ]),
        activeTaskId: null,
        history: correctionHistory,
        updatedAt: now,
      },
      laEndeavorsWorkspaceCorrection: {
        version: CORRECTION_VERSION,
        appliedAt: now,
        source: 'operator_approved_manual_injection',
        scope: 'prospecting_only_no_outreach',
      },
    },
  };
}

async function applyLaEndeavorsWorkspaceCorrection(pool) {
  if (!pool || typeof pool.connect !== 'function') return { changed: false };
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await client.query(
      `SELECT client_id, status, interview_state
         FROM cie_interview_sessions
        WHERE id = $1
        FOR UPDATE`,
      [SESSION_ID]
    );
    if (!result.rows.length) {
      await client.query('ROLLBACK');
      return { changed: false, reason: 'session_not_found' };
    }
    const row = result.rows[0];
    if (Number(row.client_id) !== CLIENT_ID || row.status !== 'APPROVED') {
      await client.query('ROLLBACK');
      return { changed: false, reason: 'session_scope_mismatch' };
    }

    const corrected = correctedState(row.interview_state || {});
    if (!corrected.changed) {
      await client.query('ROLLBACK');
      return { changed: false, reason: 'already_applied' };
    }

    await client.query(
      `UPDATE cie_interview_sessions
          SET interview_state = $2::jsonb,
              updated_at = NOW()
        WHERE id = $1`,
      [SESSION_ID, JSON.stringify(corrected.state)]
    );
    await client.query('COMMIT');
    return { changed: true, sessionId: SESSION_ID, clientId: CLIENT_ID };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

module.exports = {
  SESSION_ID,
  CLIENT_ID,
  CORRECTION_VERSION,
  correctedState,
  applyLaEndeavorsWorkspaceCorrection,
};

