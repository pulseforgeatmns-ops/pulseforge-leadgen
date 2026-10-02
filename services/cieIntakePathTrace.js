'use strict';

const { execSync } = require('child_process');

const PATH_ID = 'client_intelligence_postInterviewMessage_v3';

let cachedGitSha = null;

function getDeployedGitSha() {
  if (cachedGitSha) return cachedGitSha;
  const fromEnv =
    process.env.RAILWAY_GIT_COMMIT_SHA ||
    process.env.GIT_COMMIT ||
    process.env.VERCEL_GIT_COMMIT_SHA ||
    process.env.SOURCE_VERSION;
  if (fromEnv) {
    cachedGitSha = String(fromEnv).trim().slice(0, 12);
    return cachedGitSha;
  }
  try {
    cachedGitSha = execSync('git rev-parse --short HEAD', { encoding: 'utf8' }).trim();
  } catch {
    cachedGitSha = 'unknown';
  }
  return cachedGitSha;
}

function intakePathDebugVisible() {
  const flag = String(process.env.CIE_INTAKE_PATH_VISIBLE || process.env.CIE_INTAKE_PATH_DEBUG || '')
    .trim()
    .toLowerCase();
  return flag === '1' || flag === 'true' || flag === 'yes';
}

function intakePathShouldLog(trace) {
  if (intakePathDebugVisible()) return true;
  const flag = String(process.env.CIE_INTAKE_PATH_LOG || '').trim().toLowerCase();
  if (flag === '1' || flag === 'true' || flag === 'yes') return true;
  return trace && trace.activeQuestionKeyBeforeClassify === 'success_metrics';
}

/** Route-level dispatch logs — always on in production/Railway (matches cie-intake-routing). */
function intakeRouteLoggingEnabled(context = {}) {
  if (intakePathDebugVisible()) return true;
  const pathLog = String(process.env.CIE_INTAKE_PATH_LOG || '').trim().toLowerCase();
  if (pathLog === '1' || pathLog === 'true' || pathLog === 'yes') return true;
  if (context.activeQuestionKey === 'success_metrics') return true;
  const routingFlag = String(process.env.CIE_INTERVIEW_ROUTING_LOG || '').trim().toLowerCase();
  if (routingFlag === '0' || routingFlag === 'false' || routingFlag === 'no') return false;
  if (routingFlag === '1' || routingFlag === 'true' || routingFlag === 'yes') return true;
  return (
    process.env.NODE_ENV === 'production' ||
    Boolean(process.env.RAILWAY_ENVIRONMENT) ||
    Boolean(process.env.RAILWAY_PROJECT_ID)
  );
}

function emitIntakePathLog(payload) {
  try {
    console.log(
      `[cie-intake-path] ${JSON.stringify({
        at: new Date().toISOString(),
        gitSha: getDeployedGitSha(),
        ...payload,
      })}`
    );
  } catch {
    // ignore logging failures
  }
}

function logIntakeRouteDispatch(context = {}) {
  if (!intakeRouteLoggingEnabled(context)) return;
  emitIntakePathLog({
    event: 'route_dispatch',
    pathId: PATH_ID,
    ...context,
  });
}

function logIntakeRouteResult(context = {}) {
  if (!intakeRouteLoggingEnabled(context)) return;
  emitIntakePathLog({
    event: 'route_result',
    pathId: PATH_ID,
    ...context,
  });
}

function newIntakeTraceId() {
  return `intake_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

/**
 * Mutable trace bag for one postInterviewMessage turn.
 */
function createIntakePathTrace(base = {}) {
  return {
    intakeTraceId: newIntakeTraceId(),
    pathId: PATH_ID,
    gitSha: getDeployedGitSha(),
    routeHandler: base.routeHandler || 'POST /api/v1/interview/:id/message',
    responseFunction: base.responseFunction || 'postInterviewMessage_v3',
    tenantId: base.tenantId ?? null,
    sessionId: base.sessionId ?? null,
    activeQuestionKeyBeforeClassify: base.activeQuestionKeyBeforeClassify ?? null,
    incomingAnswerLength: base.incomingAnswerLength ?? 0,
    initialIntent: null,
    finalIntent: null,
    destinationSection: null,
    writingGuidanceBranchEntered: false,
    successMetricsHardGuardRan: false,
    recoveryGuardRan: false,
    normalizedSuccessMetricsBeforeSave: false,
    normalizedSuccessMetricsAfterSave: false,
    fieldMarkedComplete: false,
    nextQuestionKey: null,
    responseBranch: null,
    templateName: null,
    requestId: base.requestId || null,
  };
}

function patchIntakePathTrace(trace, patch = {}) {
  if (!trace) return trace;
  Object.assign(trace, patch);
  return trace;
}

function buildIntakePathDebugSuffix(trace) {
  if (!trace) return '';
  const intent = trace.finalIntent || trace.initialIntent || 'unknown';
  const active = trace.activeQuestionKeyBeforeClassify || 'none';
  const destination = trace.destinationSection || 'none';
  const completed = trace.fieldMarkedComplete ? 'true' : 'false';
  return (
    `debug_intake_path=${PATH_ID} commit=${trace.gitSha} intent=${intent} ` +
    `active=${active} destination=${destination} completed=${completed} ` +
    `branch=${trace.responseBranch || 'unknown'} template=${trace.templateName || 'unknown'} ` +
    `trace=${trace.intakeTraceId}`
  );
}

function logIntakePathTrace(trace) {
  if (!trace || !intakePathShouldLog(trace)) return;
  try {
    console.log(
      `[cie-intake-path] ${JSON.stringify({
        ...trace,
        at: new Date().toISOString(),
      })}`
    );
  } catch {
    // ignore logging failures
  }
}

/**
 * Apply trace logging + optional visible suffix; wrap withExperienceFields caller payload.
 */
function finalizeIntakePathPayload(trace, payload = {}) {
  if (!trace) return payload;
  logIntakePathTrace(trace);
  const out = { ...payload };
  if (intakePathDebugVisible()) {
    out.intakeTraceId = trace.intakeTraceId;
    out.intakePathDebug = {
      pathId: trace.pathId,
      gitSha: trace.gitSha,
      branch: trace.responseBranch,
      template: trace.templateName,
      initialIntent: trace.initialIntent,
      finalIntent: trace.finalIntent,
      activeQuestion: trace.activeQuestionKeyBeforeClassify,
      destinationSection: trace.destinationSection,
      guards: {
        writingGuidanceBranchEntered: trace.writingGuidanceBranchEntered,
        successMetricsHardGuardRan: trace.successMetricsHardGuardRan,
        recoveryGuardRan: trace.recoveryGuardRan,
      },
      normalizedSuccessMetricsBeforeSave: trace.normalizedSuccessMetricsBeforeSave,
      normalizedSuccessMetricsAfterSave: trace.normalizedSuccessMetricsAfterSave,
      fieldMarkedComplete: trace.fieldMarkedComplete,
      nextQuestionKey: trace.nextQuestionKey,
    };
    if (typeof out.message === 'string' && out.message.trim()) {
      out.message = `${out.message}\n\n${buildIntakePathDebugSuffix(trace)}`;
    }
  }
  return out;
}

/** Known assistant copy tied to refinement / writing-guidance routing (diagnostic map). */
const REFINEMENT_GUIDANCE_ACK_STRING =
  "Understood — I'll treat that as guidance for how I write and regenerate, not as business evidence.";

const INTAKE_RESPONSE_BRANCH_SOURCES = {
  conversationalAck_refinement_feedback: {
    file: 'services/clientIntelligenceInterview.js',
    fn: 'conversationalAck',
    messageType: 'refinement_feedback',
    exactString: REFINEMENT_GUIDANCE_ACK_STRING,
  },
  reasoningAck_refinement_feedback: {
    file: 'services/clientIntelligenceReasoning.js',
    fn: 'reasoningAck',
    messageClass: 'refinement_feedback',
    exactString: REFINEMENT_GUIDANCE_ACK_STRING,
  },
  refinement_pass_guidance_ack: {
    file: 'services/clientIntelligenceInterview.js',
    fn: 'postInterviewMessage.refinementPass',
    exactString: "Understood. I'll treat that as refinement guidance for Max, not as business evidence.",
  },
};

module.exports = {
  PATH_ID,
  REFINEMENT_GUIDANCE_ACK_STRING,
  INTAKE_RESPONSE_BRANCH_SOURCES,
  getDeployedGitSha,
  intakePathDebugVisible,
  intakePathShouldLog,
  intakeRouteLoggingEnabled,
  logIntakeRouteDispatch,
  logIntakeRouteResult,
  createIntakePathTrace,
  patchIntakePathTrace,
  buildIntakePathDebugSuffix,
  logIntakePathTrace,
  finalizeIntakePathPayload,
};
