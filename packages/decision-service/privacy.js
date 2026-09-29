'use strict';

function redactText(value, limit = 4000) {
  if (typeof value !== 'string' && typeof value !== 'number') return null;
  return String(value)
    .replace(/-----BEGIN [\s\S]*?(?:-----END [^-]+-----|$)/g, '[REDACTED KEY]')
    .replace(/\b(?:Bearer|Basic)\s+[^\s,;]+/gi, '[REDACTED AUTH]')
    .replace(/\b(?:sk[-_]|jv_live_|gh[pousr]_|github_pat_|xox[baprs]-)[a-zA-Z0-9_-]+/g, '[REDACTED TOKEN]')
    .replace(/((?:api[-_ ]?key|access[-_ ]?token|refresh[-_ ]?token|token|password|secret|authorization|cookie)\s*["']?\s*[:=]\s*)["']?[^\s,;"'}]+["']?/gi, '$1[REDACTED]')
    .replace(/\b(?:password|secret|api[-_ ]?key|access[-_ ]?token|refresh[-_ ]?token)\s+(?:is|was)\s+[^\r\n]+/gi, '[REDACTED CREDENTIAL]')
    .replace(/\b[a-z][a-z0-9+.-]*:\/\/[^\s]+/gi, '[REDACTED URL]')
    .replace(/\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b/gi, '[REDACTED EMAIL]')
    .replace(/\b(?:\+?\d[\d ().-]{7,}\d)\b/g, '[REDACTED PHONE]')
    .slice(0, limit);
}

function missionSnapshot(mission) {
  if (!mission || typeof mission !== 'object') return null;
  const pending = mission.pendingOperatorDecision;
  return {
    id: redactText(mission.id, 160), stage: redactText(mission.stage, 80),
    status: redactText(mission.status, 80),
    objective: redactText(mission.objective || mission.objectiveText, 2000),
    pending_decision: pending ? {
      kind: redactText(pending.kind, 100), stage: redactText(pending.stage, 80),
      prompt: redactText(pending.prompt, 1000),
    } : null,
  };
}

// Explicit field selection prevents whole CRM records, headers, credentials,
// customer lists, or session cookies being sent to the provider.
function snapshotInput(input = {}, session = null) {
  const context = input.context || input.rawContext || session?.context || {};
  const prior = session?.context || {};
  const question = String(input.question || '').trim().slice(0, 100000);
  const policy = session?.sessionState || context.sessionState || {};
  const contract = session?.conversationContract || context.conversationContract || prior.conversationContract || {};
  return {
    operator_message: redactText(question, 8000),
    message_truncated: question.length > 8000,
    context: {
      page: redactText(context.page, 80),
      mission_id: redactText(context.missionId || context.acquisitionMissionId || prior.missionId || prior.acquisitionMissionId, 160),
      execution_domain: redactText(session?.executionDomain || context.executionDomain, 80),
      execution_policy: redactText(policy.executionPolicy, 80),
      conversation_goal: redactText(contract.conversationGoal, 500),
      mission: missionSnapshot(context.mission || prior.mission),
    },
    recent_messages: (session?.messages || []).slice(-4).map(message => ({
      role: message.role === 'operator' ? 'operator' : 'max',
      text: redactText(message.text, 1500),
    })),
  };
}

module.exports = { redactText, missionSnapshot, snapshotInput };
