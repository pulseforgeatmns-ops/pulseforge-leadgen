'use strict';

const { normalizeContext } = require('./ContextEnvelope');
const { buildOpeningState } = require('./OpeningStateBuilder');
const { buildSuggestions } = require('./SuggestionEngine');
const { SessionStore } = require('./SessionStore');
const { composeResponse } = require('./ResponseComposer');
const {
  composeMissionResponse,
  composeActiveMissionResponse,
} = require('./MissionResponse');
const { PresentationEngine } = require('./PresentationEngine');
const {
  selectExecutionDomain,
  attachDomainContext,
  isMissionDomain,
  toRouteDecision,
  EXECUTION_DOMAINS,
} = require('./ExecutionDomain');
const {
  ROUTE_KINDS,
  missionEnabled,
  activeMissionResolverEnabled,
} = require('../../mission-engine');

/**
 * WorkspaceEngine — SPEC-009 + SPEC-022 + SPEC-039 routing.
 *
 * Execution flow (operator intent owns the subsystem):
 *   Operator Input
 *   → Intent Understanding (via selectExecutionDomain)
 *   → Select Execution Domain
 *   → Select/Attach Context
 *   → Execute (Mission Engine | domain-owned intelligence)
 *
 * Active conversation never selects the execution domain.
 * Active Mission Resolver still runs inside the Mission domain (ADR-025).
 */
class WorkspaceEngine {
  /**
   * @param {object} [options]
   * @param {SessionStore} [options.sessions]
   * @param {PresentationEngine} [options.presentation]
   * @param {object} [options.anthropic]
   * @param {boolean} [options.disableLlm]
   * @param {object} [options.missionEngine] - SPEC-022 MissionEngine
   * @param {boolean} [options.missionsEnabled]
   * @param {boolean} [options.resolverEnabled] - SPEC-039
   */
  constructor(options = {}) {
    this._sessions = options.sessions || new SessionStore();
    this._presentation =
      options.presentation ||
      new PresentationEngine({
        anthropic: options.anthropic,
        disableLlm: options.disableLlm,
        model: options.model,
      });
    this._missionEngine = options.missionEngine || null;
    this._missionsEnabled =
      options.missionsEnabled != null
        ? options.missionsEnabled !== false
        : missionEnabled();
    this._resolverEnabled =
      options.resolverEnabled != null
        ? options.resolverEnabled !== false
        : activeMissionResolverEnabled();
  }

  /** @returns {SessionStore} */
  get sessions() {
    return this._sessions;
  }

  /**
   * Open a workspace session from an explicit context envelope.
   * @param {object} rawContext
   * @param {{ hour?: number, executionDomain?: string }} [options]
   */
  open(rawContext, options = {}) {
    const context = normalizeContext(rawContext);
    const session = this._sessions.create(context);
    if (options.executionDomain) {
      session.executionDomain = options.executionDomain;
    } else if (rawContext && rawContext.context === 'morning_brief') {
      session.executionDomain = EXECUTION_DOMAINS.MORNING_BRIEFING;
    } else if (context.page === 'market') {
      session.executionDomain = EXECUTION_DOMAINS.MARKET_INTELLIGENCE;
    } else {
      session.executionDomain = EXECUTION_DOMAINS.WORKSPACE;
    }
    context.executionDomain = session.executionDomain;
    context._answerCorpus =
      session.executionDomain === EXECUTION_DOMAINS.MORNING_BRIEFING
        ? 'briefing'
        : session.executionDomain === EXECUTION_DOMAINS.MARKET_INTELLIGENCE
          ? 'market'
          : 'workspace';
    session.context = context;

    const opening = buildOpeningState(context, { hour: options.hour });
    const suggestions = buildSuggestions(context);

    this._sessions.appendMessage(session.id, {
      role: 'max',
      text: opening.fullText,
    });

    return {
      sessionId: session.id,
      opening,
      suggestions,
      context,
      contextSwitch: null,
      executionDomain: session.executionDomain,
    };
  }

  /**
   * Continue or start a turn. Pass context to switch focus mid-session.
   * Operator intent (not the active conversation) selects the execution domain.
   *
   * @param {object} input
   * @param {string} [input.sessionId]
   * @param {string} input.question
   * @param {object} [input.context] - optional new MaxContext (evidence only)
   * @param {object} [input.rawContext] - alias
   */
  async ask(input) {
    if (!input || !String(input.question || '').trim()) {
      throw new Error('question is required');
    }
    const question = String(input.question).trim().slice(0, 100000);
    const rawContext = input.context || input.rawContext || null;

    let session = input.sessionId
      ? this._sessions.get(input.sessionId)
      : null;
    let envelopeSwitch = null;

    if (!session) {
      if (!rawContext) {
        throw new Error('sessionId or context is required');
      }
      const opened = this.open(rawContext);
      session = this._sessions.get(opened.sessionId);
    } else if (rawContext) {
      // Envelope updates supply evidence — they do not select the domain.
      const normalized = normalizeContext(rawContext);
      const switched = this._sessions.switchContext(session.id, normalized);
      envelopeSwitch = switched.contextSwitch;
      session = switched.session;
    }

    this._sessions.appendMessage(session.id, {
      role: 'operator',
      text: question,
    });

    // 1–2) Intent Understanding → Select Execution Domain (ignore active convo)
    const domainDecision = selectExecutionDomain(question, {
      previousDomain: session.executionDomain || null,
    });

    let structured;
    let mission = null;
    let route = toRouteDecision(domainDecision);
    let resolution = null;
    let domainAttach = null;

    const missionsAvailable =
      this._missionsEnabled && this._missionEngine && isMissionDomain(domainDecision.domain);

    if (missionsAvailable) {
      // 3–4) Attach mission domain context, then execute via Mission Engine
      if (this._resolverEnabled && this._missionEngine.activeMissionResolver) {
        const resolver = this._missionEngine.activeMissionResolver;
        resolver._enabled = this._resolverEnabled;

        resolution = await resolver.resolve({
          sessionId: session.id,
          message: question,
          tenantId: session.context.tenantId,
          clientId: session.context.tenantId,
          operatorId: (session && session.operator) || null,
        });
        mission = resolution.mission || null;
        route = {
          ...toRouteDecision(domainDecision),
          ...(resolution.route || {}),
          executionDomain: domainDecision.domain,
          // Prefer Intent Understanding domain reason when resolver created
          reason:
            resolution.action === 'created'
              ? domainDecision.reason
              : (resolution.route && resolution.route.reason) ||
                domainDecision.reason,
        };

        domainAttach = attachDomainContext({
          session,
          decision: domainDecision,
          incomingContext: rawContext,
          mission,
        });

        if (
          resolution.action === 'created' &&
          mission &&
          route.kind === ROUTE_KINDS.MISSION
        ) {
          structured = composeMissionResponse({
            mission,
            question,
            card: this._missionEngine.toCard(mission),
            executionDomain: domainDecision.domain,
          });
        } else if (
          mission &&
          (resolution.action === 'resumed' ||
            resolution.action === 'modified' ||
            resolution.action === 'diagnosed' ||
            resolution.action === 'clarified')
        ) {
          structured = composeActiveMissionResponse({
            resolution,
            question,
            card: this._missionEngine.toCard(mission),
            executionDomain: domainDecision.domain,
          });
          route = {
            kind: ROUTE_KINDS.MISSION,
            missionType: mission.type,
            reason: resolution.resolutionPath,
            missionIntent: domainDecision.missionIntent,
            executionDomain: domainDecision.domain,
          };
        } else if (domainDecision.routeKind === ROUTE_KINDS.MISSION) {
          // Domain says Mission but resolver returned intelligence — force Mission Engine
          mission = await this._missionEngine.createFromObjective({
            objective: question,
            tenantId: session.context.tenantId,
            clientId: session.context.tenantId,
            createdBy: (session && session.operator) || null,
            missionType: domainDecision.missionType,
            missionIntent: domainDecision.missionIntent || null,
          });
          if (
            this._missionEngine.activeMissionResolver &&
            mission &&
            typeof this._missionEngine.activeMissionResolver.bindSession ===
              'function'
          ) {
            await this._missionEngine.activeMissionResolver.bindSession({
              sessionId: session.id,
              mission,
              operatorId: (session && session.operator) || null,
            });
          }
          domainAttach = attachDomainContext({
            session,
            decision: domainDecision,
            incomingContext: rawContext,
            mission,
          });
          structured = composeMissionResponse({
            mission,
            question,
            card: this._missionEngine.toCard(mission),
            executionDomain: domainDecision.domain,
          });
          route = toRouteDecision(domainDecision);
          resolution = {
            action: 'created',
            classification: 'new_mission',
            resolutionPath: 'domain_forced',
            route,
            mission,
          };
        } else {
          domainAttach = attachDomainContext({
            session,
            decision: domainDecision,
            incomingContext: rawContext,
            mission: null,
          });
          structured = composeResponse({
            context: domainAttach.context,
            question,
            session,
          });
          route = resolution.route || toRouteDecision(domainDecision);
        }
      } else {
        // SPEC-022 fallback without resolver
        mission = await this._missionEngine.createFromObjective({
          objective: question,
          tenantId: session.context.tenantId,
          clientId: session.context.tenantId,
          createdBy: (session && session.operator) || null,
          missionType: domainDecision.missionType,
          missionIntent: domainDecision.missionIntent || null,
        });
        domainAttach = attachDomainContext({
          session,
          decision: domainDecision,
          incomingContext: rawContext,
          mission,
        });
        structured = composeMissionResponse({
          mission,
          question,
          card: this._missionEngine.toCard(mission),
          executionDomain: domainDecision.domain,
        });
        route = toRouteDecision(domainDecision);
      }
    } else {
      // Non-mission domain OR missions disabled — domain owns intelligence answer
      domainAttach = attachDomainContext({
        session,
        decision: domainDecision,
        incomingContext: rawContext,
        mission: null,
      });
      // When missions disabled but domain is mission, still label the route
      if (
        isMissionDomain(domainDecision.domain) &&
        !this._missionsEnabled
      ) {
        route = toRouteDecision(domainDecision);
      } else {
        route = toRouteDecision(domainDecision);
      }
      structured = composeResponse({
        context: domainAttach.context,
        question,
        session,
      });
    }

    const presented = await this._presentation.present(structured);

    let prose = presented.prose;
    const switchLines = [
      envelopeSwitch,
      domainAttach && domainAttach.domainSwitch,
    ].filter(Boolean);
    if (switchLines.length) {
      prose = `${switchLines.join('\n')}\n\n${prose}`;
    }

    this._sessions.appendMessage(session.id, {
      role: 'max',
      text: prose,
      structured,
    });

    const context = (domainAttach && domainAttach.context) || session.context;

    return {
      sessionId: session.id,
      prose,
      structured,
      metadata: presented.metadata,
      suggestions: structured.nextInvestigations,
      recommendedActions: structured.recommendedActions,
      contextSwitch: envelopeSwitch,
      domainSwitch: (domainAttach && domainAttach.domainSwitch) || null,
      context,
      presentation: presented.presentation,
      route: route.kind,
      mission,
      resolution,
      executionDomain: domainDecision.domain,
      domainDecision,
      executionContext:
        (domainAttach && domainAttach.executionContext) || null,
    };
  }

  /**
   * Explicit context switch without a question.
   * @param {string} sessionId
   * @param {object} rawContext
   */
  switchContext(sessionId, rawContext) {
    const context = normalizeContext(rawContext);
    const { session, contextSwitch } = this._sessions.switchContext(
      sessionId,
      context
    );
    const opening = buildOpeningState(context);
    const suggestions = buildSuggestions(context);
    if (contextSwitch) {
      this._sessions.appendMessage(session.id, {
        role: 'max',
        text: contextSwitch,
      });
    }
    return {
      sessionId: session.id,
      contextSwitch,
      opening,
      suggestions,
      context,
      executionDomain: session.executionDomain || null,
    };
  }
}

/**
 * @param {object} [options]
 */
function createWorkspaceEngine(options = {}) {
  return new WorkspaceEngine(options);
}

module.exports = {
  WorkspaceEngine,
  createWorkspaceEngine,
};
