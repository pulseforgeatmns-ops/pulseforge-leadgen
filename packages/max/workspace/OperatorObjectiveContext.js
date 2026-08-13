'use strict';

/**
 * SPEC-095 — Max Workspace thin adapter for durable operator objectives.
 *
 * Runs before Paige gate / Intent Understanding / Mission routing:
 *   retrieve active objectives → resolve references → establish/update/status
 *
 * Context only — never executes Missions or mutates CRM/outreach state.
 */

const objectives = require('../../../services/operatorObjectives');
const { buildStructuredResponse } = require('./WorkspaceTypes');

function defaultObjectiveService() {
  return objectives;
}

/**
 * Attach active objectives + resolution onto session context (and return
 * a normalized attachment for callers).
 *
 * @param {object} input
 * @returns {Promise<object>}
 */
async function attachActiveObjectiveContext(input = {}) {
  const service = input.objectiveService || defaultObjectiveService();
  const session = input.session || null;
  const sessionCtx =
    session && session.context && typeof session.context === 'object'
      ? session.context
      : {};
  const envelope =
    input.context && typeof input.context === 'object' ? input.context : {};

  const tenantId = String(
    envelope.tenantId ||
      sessionCtx.tenantId ||
      (session && session.context && session.context.tenantId) ||
      ''
  ).trim();
  if (!tenantId) {
    return {
      activeObjectives: [],
      resolvedObjective: null,
      objectiveResolution: objectives.RESOLUTION.UNRESOLVED,
      attachment: objectives.buildObjectiveContextAttachment([], null, 'unresolved'),
    };
  }

  const clientId =
    envelope.clientId ??
    envelope.client_id ??
    sessionCtx.clientId ??
    sessionCtx.client_id ??
    (Number.isFinite(Number(tenantId)) ? Number(tenantId) : null);

  let activeObjectives = [];
  try {
    activeObjectives = await service.getActiveObjectives(
      {
        tenantId,
        clientId,
        limit: 25,
      },
      input.objectiveOpts || {}
    );
  } catch (_) {
    activeObjectives = [];
  }

  const question = String(input.question || '').trim();
  const resolution = service.resolveObjectiveReference({
    message: question,
    objectives: activeObjectives,
  });

  const attachment = service.buildObjectiveContextAttachment(
    activeObjectives,
    resolution.status === objectives.RESOLUTION.RESOLVED
      ? resolution.objective
      : null,
    resolution.status
  );

  // Attach onto session context for downstream gates (Paige, domain, compose)
  if (session && session.context && typeof session.context === 'object') {
    Object.assign(session.context, attachment);
    if (attachment.resolvedObjective) {
      session.context.objective = attachment.resolvedObjective.objectiveText;
      session.context.objectiveId = attachment.resolvedObjective.id;
      session.context.learningObjective =
        session.context.learningObjective || 'category_creation';
    }
  }

  return {
    activeObjectives,
    resolvedObjective: attachment.resolvedObjective,
    objectiveResolution: resolution.status,
    resolution,
    attachment,
    tenantId,
    clientId,
  };
}

function workspaceStructured(answer, reasoning, extras = {}) {
  return buildStructuredResponse({
    answer,
    reasoning,
    supportingEvidence: extras.supportingEvidence || [],
    contradictingEvidence: [],
    confidence: extras.confidence != null ? extras.confidence : 0.9,
    nextInvestigations: extras.nextInvestigations || [],
    recommendedActions: extras.recommendedActions || [
      {
        id: 'acknowledge',
        type: 'review',
        label: 'Continue',
      },
    ],
    confidenceContributors: ['operator_objectives', 'spec_095'],
    timelineReferences: [],
    relatedEntities: extras.relatedEntities || [],
    metadata: {
      sourcesUsed: {
        briefing: false,
        reasoning: true,
        memory: true,
        policy: false,
        knowledge: false,
      },
      evidenceCount: 0,
      asOf: new Date().toISOString(),
      unavailable: [],
      objectiveId: extras.objectiveId || null,
    },
  });
}

/**
 * Pre-routing objective handler.
 * Returns a workspace result payload when it fully handles the turn;
 * otherwise returns null so Max continues (with context attached).
 *
 * @param {object} input
 * @returns {Promise<object|null>}
 */
async function maybeHandleOperatorObjectiveTurn(input = {}) {
  const service = input.objectiveService || defaultObjectiveService();
  const question = String(input.question || '').trim();
  if (!question) return null;

  const pre = await attachActiveObjectiveContext(input);
  const opts = input.objectiveOpts || {};
  const { activeObjectives, resolution, tenantId } = pre;

  // 1) Ambiguous reference on status/content asks → fail closed
  if (
    resolution &&
    resolution.status === objectives.RESOLUTION.AMBIGUOUS &&
    (service.looksLikeObjectiveStatusRequest(question) ||
      service.looksLikeObjectiveContentRequest(question) ||
      /\b(the\s+)?(launch|expansion|rollout|objective)\b/i.test(question))
  ) {
    const prose = service.formatAmbiguousObjectiveResponse(resolution.matches);
    return {
      reason: 'operator_objective_ambiguous',
      handled: true,
      prose,
      structured: workspaceStructured(prose, [
        'Multiple active objectives matched the reference.',
        'Fail closed — operator must disambiguate (SPEC-095).',
      ]),
      resolvedObjective: null,
      activeObjectives,
      objectiveResolution: resolution.status,
    };
  }

  // 2) Explicit establishment
  const establish = service.detectObjectiveEstablishment(question);
  if (establish && establish.kind === 'establish') {
    // Fail closed if we somehow lack tenant
    if (!tenantId) return null;

    let clientId = null;
    if (establish.scope === 'client') {
      clientId =
        asClientIdSafe(input.session && input.session.context) ||
        asClientIdSafe(input.context) ||
        (Number.isFinite(Number(tenantId)) ? Number(tenantId) : null);
      if (clientId == null) {
        // Cannot create client-scoped without client — stay fail-closed
        return null;
      }
    }

    // Upsert by title+scope within tenant — do not duplicate standing objectives.
    const existingSame = (activeObjectives || []).find(
      (o) =>
        o &&
        o.scope === establish.scope &&
        String(o.title).toLowerCase() === String(establish.title).toLowerCase()
    );

    let created;
    if (existingSame) {
      created = await service.updateOperatorObjective(
        existingSame.id,
        {
          tenantId,
          objectiveText: establish.objectiveText,
          timeHorizon: establish.timeHorizon || existingSame.timeHorizon,
          currentPhase:
            establish.currentPhase ||
            existingSame.currentPhase ||
            'Thesis / problem exposure',
          aliases: establish.aliases,
          context: { ...(existingSame.context || {}), ...(establish.context || {}) },
          status: 'active',
        },
        opts
      );
    } else {
      created = await service.createOperatorObjective(
        {
          tenantId,
          scope: establish.scope,
          clientId,
          title: establish.title,
          objectiveText: establish.objectiveText,
          timeHorizon: establish.timeHorizon,
          currentPhase: establish.currentPhase || 'Thesis / problem exposure',
          aliases: establish.aliases,
          context: establish.context || {},
          createdBy:
            (input.session && input.session.operator) || 'workspace_operator',
        },
        opts
      );
    }

    // Refresh session context with the new active objective
    if (input.session && input.session.context) {
      const refreshed = await attachActiveObjectiveContext({
        ...input,
        question: created.title,
      });
      void refreshed;
      input.session.context.resolvedObjective = service.envelopeObjective(created);
      input.session.context.objective = created.objectiveText;
      input.session.context.objectiveId = created.id;
    }

    const prose = service.formatObjectiveCreatedResponse(created);
    return {
      reason: 'operator_objective_established',
      handled: true,
      prose,
      structured: workspaceStructured(
        prose,
        [
          'Operator explicitly established a durable strategic objective.',
          'Persisted outside SessionStore; no Mission or execution started.',
        ],
        {
          objectiveId: created.id,
          relatedEntities: [
            {
              id: created.id,
              type: 'operator_objective',
              name: created.title,
            },
          ],
        }
      ),
      resolvedObjective: created,
      activeObjectives: [...activeObjectives, created],
      objectiveResolution: objectives.RESOLUTION.RESOLVED,
      objective: created,
    };
  }

  // 3) Lifecycle change
  const lifecycle = service.detectObjectiveLifecycleChange(
    question,
    activeObjectives
  );
  if (lifecycle && lifecycle.kind === 'lifecycle' && lifecycle.objective) {
    const before = lifecycle.objective;
    const after = await service.updateOperatorObjective(
      before.id,
      { tenantId, status: lifecycle.status },
      opts
    );
    const prose = service.formatObjectiveUpdatedResponse(before, after);
    return {
      reason: 'operator_objective_lifecycle',
      handled: true,
      prose,
      structured: workspaceStructured(prose, [
        'Operator explicitly changed objective lifecycle status.',
        'No autonomous execution.',
      ], { objectiveId: after.id }),
      resolvedObjective: after,
      activeObjectives,
      objectiveResolution: objectives.RESOLUTION.RESOLVED,
      objective: after,
    };
  }

  // 4) Objective field update
  const update = service.detectObjectiveUpdate(question, activeObjectives);
  if (update && update.kind === 'update' && update.objective && update.patch) {
    const before = update.objective;
    const after = await service.updateOperatorObjective(
      before.id,
      { tenantId, ...update.patch },
      opts
    );
    const prose = service.formatObjectiveUpdatedResponse(before, after);
    return {
      reason: 'operator_objective_updated',
      handled: true,
      prose,
      structured: workspaceStructured(prose, [
        'Operator explicitly updated durable objective fields.',
      ], { objectiveId: after.id }),
      resolvedObjective: after,
      activeObjectives,
      objectiveResolution: objectives.RESOLUTION.RESOLVED,
      objective: after,
    };
  }

  // 5) Status / planning about resolved objective — do NOT create Mission
  const resolved =
    resolution && resolution.status === objectives.RESOLUTION.RESOLVED
      ? resolution.objective
      : null;

  if (
    resolved &&
    service.shouldSuppressMissionForResolvedObjective(question, resolved) &&
    service.looksLikeObjectiveStatusRequest(question)
  ) {
    const prose = service.formatObjectiveStatusResponse(resolved);
    return {
      reason: 'operator_objective_status',
      handled: true,
      prose,
      structured: workspaceStructured(
        prose,
        [
          'Resolved operator objective before intent routing.',
          'Status/planning request — Mission Engine not invoked.',
        ],
        {
          objectiveId: resolved.id,
          relatedEntities: [
            {
              id: resolved.id,
              type: 'operator_objective',
              name: resolved.title,
            },
          ],
        }
      ),
      resolvedObjective: resolved,
      activeObjectives,
      objectiveResolution: objectives.RESOLUTION.RESOLVED,
      suppressMission: true,
    };
  }

  // Context attached; continue to Paige / domain routing
  return {
    reason: 'operator_objective_context_attached',
    handled: false,
    resolvedObjective: resolved,
    activeObjectives,
    objectiveResolution: pre.objectiveResolution,
    suppressMission: Boolean(
      resolved &&
        service.shouldSuppressMissionForResolvedObjective(question, resolved)
    ),
    attachment: pre.attachment,
  };
}

function asClientIdSafe(ctx) {
  if (!ctx || typeof ctx !== 'object') return null;
  const v = ctx.clientId ?? ctx.client_id ?? null;
  if (v == null || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : null;
}

/**
 * Wrap Paige recommendation prose inside recovered objective context.
 *
 * @param {object} objective
 * @param {string} paigeProse
 * @returns {string}
 */
function synthesizeObjectivePaigeResponse(objective, paigeProse) {
  if (!objective) return paigeProse;
  const parts = [];
  parts.push(String(objective.title || 'ACTIVE OBJECTIVE').toUpperCase());
  parts.push('');
  if (objective.currentPhase) {
    parts.push(`Current phase: ${objective.currentPhase}`);
    parts.push('');
  }
  parts.push('Paige recommends:');
  parts.push(paigeProse);
  parts.push('');
  parts.push(
    'What this advances: standing strategic context for this objective — review-first, no autonomous publish.'
  );
  parts.push('Next: Approve, revise, request another experiment, or hold.');
  return parts.join('\n');
}

module.exports = {
  attachActiveObjectiveContext,
  maybeHandleOperatorObjectiveTurn,
  synthesizeObjectivePaigeResponse,
};
