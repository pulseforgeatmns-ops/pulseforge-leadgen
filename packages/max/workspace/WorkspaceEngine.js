'use strict';

const { normalizeContext } = require('./ContextEnvelope');
const { buildOpeningState } = require('./OpeningStateBuilder');
const {
  buildSuggestions,
  buildActiveWorkSuggestions,
  isActiveDeskWorkflow,
} = require('./SuggestionEngine');
const { SessionStore } = require('./SessionStore');
const { composeResponse } = require('./ResponseComposer');
const { buildStructuredResponse } = require('./WorkspaceTypes');
const {
  buildMarketIntelligenceContext,
} = require('./MarketIntelligenceContext');
const {
  maybeHandlePaigeCampaignContentDelegation,
} = require('./PaigeCampaignDelegationContext');
const {
  maybeHandleOperatorObjectiveTurn,
  synthesizeObjectivePaigeResponse,
} = require('./OperatorObjectiveContext');
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
  getActiveWorkContext,
  setActiveWorkContext,
  buildCanaryActiveWorkContext,
  entitiesToProspects,
  isActiveWorkFollowUpCue,
  isActiveWorkReuseProspectCue,
  isActiveWorkTransformCue,
  isPacketReviewRequest,
  isCallScriptReviewRequest,
  isCallScriptDecisionRecordRequest,
  isCanarySummaryJudgmentRequest,
  isFocusedCanaryWorkOrderRequest,
  wantsFocusedFutureMailingEligibilitySection,
  wantsFocusedFinalApprovalGateSection,
  extractPacketReviewProspectId,
  extractCallScriptReviewProspectId,
  isExplicitNewMissionRequest,
  isExplicitContextOverride,
  isExplicitExecutionRequest,
  isFillableTableRequest,
  isFillableTableUpdateRequest,
  isFillableTableReadinessReassessRequest,
  isFillableTableWholeTableReassessRequest,
  wantsStrictFillableTableOutputShape,
  wantsFillableTableHeading,
  activeContextHasFillableTable,
  knownActiveWorkProspectIds,
  parseFillableTableFieldUpdates,
  applyFillableTableFieldUpdates,
  extractReadinessReassessProspectIds,
  extractGateStatusUpdatedProspectIds,
  deriveOperatorNextActionFromGates,
  extractCampaignIdFromText,
  activeContextBlocksExecution,
  activeContextHasEntities,
  isCanaryDeskWorkflow,
  ingestPastedFillableVerificationTable,
  ingestPastedReadinessSummaryTable,
  looksLikeFillableVerificationTablePaste,
  looksLikeReadinessSummaryTablePaste,
  parseReadinessSummaryTableFromMessage,
  parseFillableVerificationTableFromMessage,
  normalizeReadinessSummaryRow,
  hasCanaryReadinessTableCues,
  diagnoseCanaryReadinessTableIngestion,
  emitCanaryReadinessIngestDiagnostics,
  shouldEmitCanaryReadinessDiagnostics,
  parseInlinePacketReviewKnownFacts,
  parseInlineCallScriptReviewKnownFacts,
  parseKnownCurrentStateBullets,
  isProceedWithPacketContentReviewRequest,
  isProceedWithCallScriptReviewRequest,
  entitiesFromFillableTableRows,
  CAMPAIGN_001_PREPARATION_ONLY_CANARY,
  LAST_OUTPUT_TYPES,
} = require('./ActiveWorkContext');
const {
  CANARY_WORKFLOW_TYPES,
  resolveCanaryWorkflowContract,
  resolveCanaryWorkflowType,
  isReadyForReviewValue,
  isDraftAllowedValue,
  formatBlockedOutboundVerbList,
  formatCanarySafetyLine,
} = require('./CanaryWorkflowContract');
const {
  ROUTE_KINDS,
  missionEnabled,
  activeMissionResolverEnabled,
  OperatorArtifactInjection,
} = require('../../mission-engine');

const { detectOperatorProspectListInMessage } = OperatorArtifactInjection;

/**
 * WorkspaceEngine — SPEC-009 + SPEC-022 + SPEC-039 routing.
 *
 * Execution flow (operator intent owns the subsystem):
 *   Operator Input
 *   → Resolve tenant/operator/client
 *   → Retrieve active objectives (SPEC-095)
 *   → Reference resolution
 *   → Intent Understanding in context (via selectExecutionDomain)
 *   → Select Execution Domain
 *   → Select/Attach Context
 *   → Execute (Mission Engine | domain-owned intelligence)
 *
 * Active conversation never selects the execution domain.
 * Active Mission Resolver still runs inside the Mission domain (ADR-025).
 * Durable objectives are Pulseforge state — not provider/LLM memory.
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
   * @param {object} [options.marketIntelligenceService] - SPEC-066 read-only adapter dependency
   * @param {object} [options.paigeCampaignDelegationService] - SPEC-094 Paige delegation
   * @param {object} [options.paigeLearningOpts] - SPEC-094 contentLearning store opts (tests)
   * @param {object} [options.operatorObjectiveService] - SPEC-095 objective service
   * @param {object} [options.operatorObjectiveOpts] - SPEC-095 store opts (tests)
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
    this._marketIntelligenceService = options.marketIntelligenceService || null;
    this._paigeCampaignDelegationService =
      options.paigeCampaignDelegationService || null;
    this._paigeLearningOpts = options.paigeLearningOpts || null;
    this._operatorObjectiveService = options.operatorObjectiveService || null;
    this._operatorObjectiveOpts = options.operatorObjectiveOpts || null;
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
      // When the operator is continuing active desk work, do not adopt stale
      // recommendation / suggestion-chip focus from the envelope.
      const envelopeForSwitch = preserveActiveWorkFocusOverStaleRecommendation({
        question,
        session,
        rawContext,
      });
      const normalized = normalizeContext(envelopeForSwitch);
      const switched = this._sessions.switchContext(session.id, normalized);
      envelopeSwitch = switched.contextSwitch;
      session = switched.session;
    }

    this._sessions.appendMessage(session.id, {
      role: 'operator',
      text: question,
    });

    // Early desk-context continuation — before domain routing, General
    // Conversation, policy fallback, mission resolver, or mission create/resume.
    const activeContinuation = await maybeHandleActiveWorkContinuation({
      question,
      session,
    });
    if (activeContinuation) {
      session.executionDomain = EXECUTION_DOMAINS.WORKSPACE;
      if (session.context && typeof session.context === 'object') {
        session.context.executionDomain = EXECUTION_DOMAINS.WORKSPACE;
        session.context._answerCorpus = 'workspace';
      }

      const structuredEarly = activeContinuation.structured;
      const routeEarly = {
        kind: ROUTE_KINDS.INTELLIGENCE,
        missionType: null,
        reason: activeContinuation.reason,
        missionIntent: null,
        executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      };
      const presentedEarly = await this._presentation.present(structuredEarly);
      // Latest typed operator message owns the turn — never surface a stale
      // recommendation/suggestion focus switch during active desk continuation.
      const proseEarly = presentedEarly.prose;
      envelopeSwitch = null;

      this._sessions.appendMessage(session.id, {
        role: 'max',
        text: proseEarly,
        structured: structuredEarly,
      });

      return {
        sessionId: session.id,
        prose: proseEarly,
        structured: structuredEarly,
        metadata: presentedEarly.metadata,
        suggestions: resolveResultSuggestions({
          structured: structuredEarly,
          session,
          question,
        }),
        recommendedActions: structuredEarly.recommendedActions,
        contextSwitch: envelopeSwitch,
        domainSwitch: null,
        context: session.context,
        presentation: presentedEarly.presentation,
        route: routeEarly.kind,
        mission: null,
        resolution: null,
        executionDomain: EXECUTION_DOMAINS.WORKSPACE,
        domainDecision: {
          domain: EXECUTION_DOMAINS.WORKSPACE,
          reason: activeContinuation.reason,
          missionType: null,
          missionIntent: null,
          confidence: 1,
          previousDomain: session.previousExecutionDomain || null,
          domainSwitched: false,
        },
        executionContext: {
          domain: EXECUTION_DOMAINS.WORKSPACE,
          routeKind: ROUTE_KINDS.INTELLIGENCE,
          reason: activeContinuation.reason,
          missionType: null,
          missionId: null,
        },
      };
    }

    // SPEC-095 — durable operator objectives before intent routing.
    // Persist → Retrieve → Resolve → Interpret → Route.
    // Never invents objectives; never starts Missions from status references.
    const objectiveTurn = await maybeHandleOperatorObjectiveTurn({
      question,
      session,
      context: rawContext || session.context,
      objectiveService: this._operatorObjectiveService || undefined,
      objectiveOpts: this._operatorObjectiveOpts || undefined,
    });
    if (objectiveTurn && objectiveTurn.handled) {
      session.executionDomain = EXECUTION_DOMAINS.WORKSPACE;
      if (session.context && typeof session.context === 'object') {
        session.context.executionDomain = EXECUTION_DOMAINS.WORKSPACE;
        session.context._answerCorpus = 'workspace';
        if (objectiveTurn.resolvedObjective) {
          session.context.resolvedObjective = objectiveTurn.resolvedObjective;
          session.context.objective =
            objectiveTurn.resolvedObjective.objectiveText ||
            session.context.objective;
          session.context.objectiveId = objectiveTurn.resolvedObjective.id;
        }
        if (objectiveTurn.activeObjectives) {
          session.context.activeObjectives = objectiveTurn.activeObjectives;
        }
        session.context.objectiveResolution =
          objectiveTurn.objectiveResolution || null;
      }

      const structuredObj = objectiveTurn.structured;
      const routeObj = {
        kind: ROUTE_KINDS.INTELLIGENCE,
        missionType: null,
        reason: objectiveTurn.reason,
        missionIntent: null,
        executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      };
      const presentedObj = await this._presentation.present(structuredObj);
      const proseObj = presentedObj.prose || objectiveTurn.prose;

      this._sessions.appendMessage(session.id, {
        role: 'max',
        text: proseObj,
        structured: structuredObj,
      });

      return {
        sessionId: session.id,
        prose: proseObj,
        structured: structuredObj,
        metadata: presentedObj.metadata,
        suggestions: resolveResultSuggestions({
          structured: structuredObj,
          session,
          question,
        }),
        recommendedActions: structuredObj.recommendedActions,
        contextSwitch: envelopeSwitch,
        domainSwitch: null,
        context: session.context,
        presentation: presentedObj.presentation,
        route: routeObj.kind,
        mission: null,
        resolution: null,
        executionDomain: EXECUTION_DOMAINS.WORKSPACE,
        resolvedObjective: objectiveTurn.resolvedObjective || null,
        activeObjectives: objectiveTurn.activeObjectives || [],
        domainDecision: {
          domain: EXECUTION_DOMAINS.WORKSPACE,
          reason: objectiveTurn.reason,
          missionType: null,
          missionIntent: null,
          confidence: 1,
          previousDomain: session.previousExecutionDomain || null,
          domainSwitched: false,
        },
        executionContext: {
          domain: EXECUTION_DOMAINS.WORKSPACE,
          routeKind: ROUTE_KINDS.INTELLIGENCE,
          reason: objectiveTurn.reason,
          missionType: null,
          missionId: null,
        },
      };
    }

    const suppressMissionForObjective = Boolean(
      objectiveTurn && objectiveTurn.suppressMission
    );

    // SPEC-094 — Max → Paige campaign content delegation (read-only advisory).
    // Runs before mission create so launch/content asks stay conversational.
    // SPEC-095: recovered objective context is already on session.context.
    const paigeDelegation = await maybeHandlePaigeCampaignContentDelegation({
      question,
      session,
      context: rawContext || session.context,
      delegationService: this._paigeCampaignDelegationService || undefined,
      learningOpts: this._paigeLearningOpts || undefined,
    });
    if (paigeDelegation) {
      session.executionDomain = EXECUTION_DOMAINS.WORKSPACE;
      if (session.context && typeof session.context === 'object') {
        session.context.executionDomain = EXECUTION_DOMAINS.WORKSPACE;
        session.context._answerCorpus = 'workspace';
        session.context.paigeRecommendation = paigeDelegation.recommendation;
      }

      const structuredPaige = paigeDelegation.structured;
      // SPEC-095 — present Paige inside recovered objective when available.
      const resolvedForSynth =
        (session.context && session.context.resolvedObjective) ||
        (objectiveTurn && objectiveTurn.resolvedObjective) ||
        null;
      if (resolvedForSynth && structuredPaige && structuredPaige.answer) {
        structuredPaige.answer = synthesizeObjectivePaigeResponse(
          resolvedForSynth,
          structuredPaige.answer
        );
      }
      const routePaige = {
        kind: ROUTE_KINDS.INTELLIGENCE,
        missionType: null,
        reason: paigeDelegation.reason,
        missionIntent: null,
        executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      };
      const presentedPaige = await this._presentation.present(structuredPaige);
      const prosePaige = presentedPaige.prose;

      this._sessions.appendMessage(session.id, {
        role: 'max',
        text: prosePaige,
        structured: structuredPaige,
      });

      return {
        sessionId: session.id,
        prose: prosePaige,
        structured: structuredPaige,
        metadata: presentedPaige.metadata,
        suggestions: resolveResultSuggestions({
          structured: structuredPaige,
          session,
          question,
        }),
        recommendedActions: structuredPaige.recommendedActions,
        contextSwitch: envelopeSwitch,
        domainSwitch: null,
        context: session.context,
        presentation: presentedPaige.presentation,
        route: routePaige.kind,
        mission: null,
        resolution: null,
        executionDomain: EXECUTION_DOMAINS.WORKSPACE,
        paigeRecommendation: paigeDelegation.recommendation,
        resolvedObjective: resolvedForSynth,
        domainDecision: {
          domain: EXECUTION_DOMAINS.WORKSPACE,
          reason: paigeDelegation.reason,
          missionType: null,
          missionIntent: null,
          confidence: 1,
          previousDomain: session.previousExecutionDomain || null,
          domainSwitched: false,
        },
        executionContext: {
          domain: EXECUTION_DOMAINS.WORKSPACE,
          routeKind: ROUTE_KINDS.INTELLIGENCE,
          reason: paigeDelegation.reason,
          missionType: null,
          missionId: null,
        },
      };
    }

    // 1–2) Intent Understanding → Select Execution Domain (ignore active convo)
    // SPEC-095: pass resolved objective so status references do not become Missions.
    const domainDecision = selectExecutionDomain(question, {
      previousDomain: session.executionDomain || null,
      resolvedObjective:
        (session.context && session.context.resolvedObjective) ||
        (objectiveTurn && objectiveTurn.resolvedObjective) ||
        null,
      suppressMissionForObjective,
    });

    let structured;
    let mission = null;
    let route = toRouteDecision(domainDecision);
    let resolution = null;
    let domainAttach = null;

    // Absolute canary hard stop — before resolver, mission create/resume,
    // campaign_creation fallback, and operator ProspectList mission injection.
    // Not gated on isMissionDomain / IntentUnderstanding.
    const canaryPrep = await maybeBuildCanaryPreparationResponse({
      question,
      tenantId: session.context.tenantId,
      missionEngine: this._missionEngine,
      domainDecision,
      session,
    });

    const missionsAvailable =
      this._missionsEnabled &&
      this._missionEngine &&
      isMissionDomain(domainDecision.domain);

    if (canaryPrep) {
      domainAttach = {
        context: session.context,
        contextSwitch: null,
        domainSwitch: null,
        executionContext: {
          domain: EXECUTION_DOMAINS.WORKSPACE,
          routeKind: ROUTE_KINDS.INTELLIGENCE,
          reason: canaryPrep.reason,
          missionType: null,
          missionId: null,
        },
      };
      structured = canaryPrep.structured;
      route = {
        kind: ROUTE_KINDS.INTELLIGENCE,
        missionType: null,
        reason: canaryPrep.reason,
        missionIntent: null,
        executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      };
    } else if (missionsAvailable) {
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
      if (domainDecision.domain === EXECUTION_DOMAINS.MARKET_INTELLIGENCE) {
        domainAttach.context.marketIntelligence = await buildMarketIntelligenceContext(
          question,
          { service: this._marketIntelligenceService || undefined }
        );
        session.context = domainAttach.context;
      }
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
      suggestions: resolveResultSuggestions({
        structured,
        session,
        question,
      }),
      recommendedActions: structured.recommendedActions,
      contextSwitch: envelopeSwitch,
      domainSwitch: (domainAttach && domainAttach.domainSwitch) || null,
      context,
      presentation: presented.presentation,
      route: route.kind,
      mission,
      resolution,
      executionDomain:
        (route && route.executionDomain) ||
        domainDecision.domain,
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

/**
 * When the latest operator message continues active canary/table desk work,
 * keep the prior session focus instead of adopting a stale recommendation or
 * suggestion-chip label from the context envelope (e.g. "What changed overnight?").
 * @param {object} input
 * @param {string} input.question
 * @param {object} input.session
 * @param {object} input.rawContext
 * @returns {object}
 */
function preserveActiveWorkFocusOverStaleRecommendation(input) {
  const question = String(input.question || '');
  const session = input.session;
  const rawContext = input.rawContext;
  if (!rawContext || typeof rawContext !== 'object' || !session) {
    return rawContext;
  }
  if (isExplicitNewMissionRequest(question)) return rawContext;

  const prior = getActiveWorkContext(session);
  if (!activeContextHasEntities(prior)) return rawContext;

  const continuingDesk =
    isFillableTableUpdateRequest(question, prior) ||
    isCanarySummaryJudgmentRequest(question) ||
    isPacketReviewRequest(question) ||
    isCallScriptReviewRequest(question) ||
    isCallScriptDecisionRecordRequest(question) ||
    isActiveWorkFollowUpCue(question) ||
    isActiveWorkTransformCue(question) ||
    isActiveWorkReuseProspectCue(question);
  if (!continuingDesk) return rawContext;

  const priorCtx = session.context || {};
  return {
    ...rawContext,
    // Preserve ambient briefing/deck evidence, but do not let stale
    // recommendation / chip focus override the active desk turn.
    page: priorCtx.page || rawContext.page,
    recommendationId:
      priorCtx.recommendationId != null
        ? priorCtx.recommendationId
        : null,
    companyId: priorCtx.companyId != null ? priorCtx.companyId : null,
    selectedEntity:
      priorCtx.selectedEntity && typeof priorCtx.selectedEntity === 'object'
        ? priorCtx.selectedEntity
        : null,
    activeWorkContext: prior,
  };
}

/**
 * Early guard: if desk context exists and the operator is continuing/transforming
 * prior work, handle it before domain routing can send the turn to General
 * Conversation / policy fallback / mission create.
 * @returns {Promise<{ structured: object, reason: string }|null>}
 */
async function maybeHandleActiveWorkContinuation(input) {
  const question = String(input.question || '');
  const session = input.session || null;
  if (!question.trim()) return null;

  // Explicit new campaign/mission work always goes through normal routing.
  if (isExplicitNewMissionRequest(question)) return null;

  // Pasted fillable verification table / readiness summary table → desk
  // context before packet review, table mutation, prospect extraction, or
  // mission routing.
  ingestPastedFillableVerificationTable({ question, session });
  ingestPastedReadinessSummaryTable({ question, session });

  const prior = getActiveWorkContext(session);
  const hasEntities = activeContextHasEntities(prior);
  const isFollowUp = isActiveWorkFollowUpCue(question);
  const isFillable = isFillableTableRequest(question);
  const isTransform = isActiveWorkTransformCue(question);
  const isExec = isExplicitExecutionRequest(question);
  const isTableUpdateRequest = isFillableTableUpdateRequest(question, prior);
  const isTableUpdate =
    hasEntities &&
    activeContextHasFillableTable(prior) &&
    isTableUpdateRequest;
  const isPacketReview = isPacketReviewRequest(question);
  const isCallScriptReview = isCallScriptReviewRequest(question);
  const isCallScriptDecisionRecord =
    isCallScriptDecisionRecordRequest(question);

  // Fillable table field mutation — before prospect extraction, artifact
  // injection, domain routing, or mission routing.
  if (isTableUpdate) {
    return handleFillableTableUpdateContinuation({
      question,
      session,
      prior,
    });
  }

  // Call-script decision-record from inline facts — before canary summary /
  // focused work-order selection (those cues often appear in Return sections).
  if (isCallScriptDecisionRecord) {
    const inlineResult = handleInlineKnownFactsCallScriptDecisionRecord({
      question,
    });
    if (inlineResult) return inlineResult;
    return {
      reason: 'active_work_context_missing_for_call_script_decision_record',
      structured: buildMissingCallScriptDecisionRecordResponse({ question }),
    };
  }

  // Proceed-with a named packet-content work order (or explicit packet review)
  // before focused work-order / canary summary selection.
  if (isPacketReview) {
    if (hasEntities && activeContextHasFillableTable(prior)) {
      return handlePacketReviewContinuation({
        question,
        session,
        prior,
      });
    }
    const inlineResult = handleInlineKnownFactsPacketReview({ question });
    if (inlineResult) return inlineResult;
    return {
      reason: 'active_work_context_missing_for_packet_review',
      structured: buildMissingPacketReviewResponse({ question }),
    };
  }

  // Proceed-with a named call-script work order (or explicit call-script review)
  // before focused work-order / canary summary selection.
  if (isCallScriptReview) {
    const inlineResult = handleInlineKnownFactsCallScriptReview({ question });
    // Prefer complete inline known facts when present (proceed-with always
    // carries them). Fall back to desk table when inline facts are absent.
    if (inlineResult) return inlineResult;
    if (hasEntities && activeContextHasFillableTable(prior)) {
      return handleCallScriptReviewContinuation({
        question,
        session,
        prior,
      });
    }
    return {
      reason: 'active_work_context_missing_for_call_script_review',
      structured: buildMissingCallScriptReviewResponse({ question }),
    };
  }

  // Cross-prospect canary status summary / judgment — before prospect
  // extraction / parse fallback. Input order: desk tableRows → pasted table
  // (already ingested) → known current-state bullets → ask for state.
  if (isCanarySummaryJudgmentRequest(question)) {
    return handleCanarySummaryJudgmentContinuation({
      question,
      session,
      prior,
    });
  }

  // Explicit table-update intent without desk table — ask for the current
  // table / prospects. Never fall through to General Conversation, briefing,
  // or market-intelligence fallback.
  if (
    isTableUpdateRequest &&
    (!hasEntities || !activeContextHasFillableTable(prior))
  ) {
    return {
      reason: 'active_work_context_missing_for_table_update',
      structured: buildMissingFillableTableUpdateResponse({ question }),
    };
  }

  // Mail/launch while desk constraints forbid execution — still early, so we
  // never infer launch from General Conversation routing.
  if (
    hasEntities &&
    isExec &&
    !isPreparationOnlyCanary(question) &&
    activeContextBlocksExecution(prior)
  ) {
    return {
      reason: 'active_work_context_execution_blocked',
      structured: buildCanaryExecutionBlockedResponse({
        activeWorkContext: prior,
        question,
      }),
    };
  }

  if (hasEntities && (isFollowUp || isFillable)) {
    // New paste overrides desk entities — fall through so canary/mission
    // paths can parse and replace. Reuse cues ("same 3 prospects already
    // listed") keep desk entities even when they mention a count.
    if (
      operatorAttemptedCanaryProspectSupply(question) &&
      !isActiveWorkReuseProspectCue(question) &&
      !isFillableTableUpdateRequest(question, prior) &&
      !isPacketReviewRequest(question) &&
      !isCallScriptReviewRequest(question) &&
      !isCallScriptDecisionRecordRequest(question) &&
      !isCanarySummaryJudgmentRequest(question) &&
      !looksLikeFillableVerificationTablePaste(question) &&
      !looksLikeReadinessSummaryTablePaste(question)
    ) {
      const detected = detectOperatorProspectListInMessage(question);
      const intendedCount = extractIntendedCanaryProspectCount(question);
      const completeProspects =
        detected.detected &&
        detected.prospectCount > 0 &&
        (!intendedCount || detected.prospectCount >= intendedCount);
      if (!completeProspects) {
        // Incomplete new paste — clarify, do not invent from old desk rows.
        return {
          reason: 'active_work_context_prospect_parse_clarification',
          structured: buildCanaryParseFailureResponse({
            intendedCount: intendedCount || 3,
          }),
        };
      }
      // Complete new paste: let normal canary path store/replace.
      return null;
    }

    const prospects = entitiesToProspects(prior.entities);
    const review = buildCanaryReviewPackageResponse({
      prospects,
      objectiveText: question,
      question,
      reusedFromActiveContext: true,
      tableRows: prior.tableRows,
    });
    rememberCanaryActiveWorkContext({
      session,
      prospects,
      question,
      lastOutputType: resolveCanaryLastOutputType(question, review.reason),
      prior,
      tableRows:
        review.tableRows ||
        (review.reason === 'canary_fillable_table'
          ? buildFillableTableRows(prospects)
          : prior.tableRows),
    });
    return {
      reason: review.reason || 'active_work_context_continuation',
      structured: review.structured || review,
    };
  }

  // Strong transform cue without desk context — ask for prospects instead of
  // falling through to General Conversation / policy unavailable.
  if (!hasEntities && isTransform) {
    if (
      isFillable ||
      /\bconvert\s+the\s+(?:verification\s+)?work\s+order\b/i.test(question)
    ) {
      return {
        reason: 'active_work_context_missing_for_transform',
        structured: buildStructuredResponse({
          answer: [
            'I do not have an active verification work order on the desk yet.',
            'Paste the prospects (or continue from a session that already has them) and I will build the fillable table.',
            'Send company name, decision maker if known, website, mailing address, and phone if you have it.',
            'Preparation-only. No mission created. No launch, execution, approval, print, or mail.',
          ].join(' '),
          reasoning: [
            'Operator asked to transform a verification work order, but activeWorkContext has no entities.',
            'Clarifying instead of falling through to generic routing or policy fallback.',
            'No mission create/resume.',
          ],
          supportingEvidence: [],
          contradictingEvidence: [],
          confidence: null,
          nextInvestigations: [
            'Paste prospects for the fillable verification table.',
          ],
          recommendedActions: [],
          metadata: {
            sourcesUsed: {},
            evidenceCount: 0,
            unavailable: ['active_work_context_entities'],
            surface: 'workspace',
            executionDomain: EXECUTION_DOMAINS.WORKSPACE,
            route: 'intelligence',
            canaryPreparationOnly: true,
            fillableTable: isFillable,
          },
        }),
      };
    }
  }

  return null;
}

/**
 * Mutate fields on the existing fillable verification table before any
 * prospect-list parsing can invent op_* rows from instruction labels.
 * @returns {{ reason: string, structured: object }}
 */
function handleFillableTableUpdateContinuation(input) {
  const question = String(input.question || '');
  const session = input.session || null;
  const prior = input.prior;
  const prospects = entitiesToProspects(prior.entities);
  const knownIds = knownActiveWorkProspectIds(prior);
  const baseRows =
    Array.isArray(prior.tableRows) && prior.tableRows.length > 0
      ? prior.tableRows.map((row) => ({ ...row }))
      : buildFillableTableRows(prospects);

  const parsed = parseFillableTableFieldUpdates(question);
  const shouldReassess = isFillableTableReadinessReassessRequest(question);
  const wholeTableReassess = isFillableTableWholeTableReassessRequest(question);
  const gateUpdatedIds = extractGateStatusUpdatedProspectIds(parsed.updates);
  const reassessIds = (() => {
    const ids = [];
    const seen = new Set();
    const pushAll = (list) => {
      for (const id of list || []) {
        const key = String(id || '')
          .trim()
          .toUpperCase();
        if (!key || seen.has(key)) continue;
        seen.add(key);
        ids.push(String(id).trim());
      }
    };

    if (shouldReassess) {
      const named = extractReadinessReassessProspectIds(question);
      if (named.length > 0) {
        pushAll(named);
      } else {
        // Field updates in the same turn imply which rows to reassess.
        const fromUpdates = (parsed.updates || [])
          .map((u) => u.prospectId)
          .filter(Boolean);
        if (fromUpdates.length > 0) {
          pushAll(fromUpdates);
        } else if (wholeTableReassess) {
          // "Reassess the Campaign 001 canary table" → all desk rows.
          pushAll(knownIds);
        } else if (knownIds.length === 1) {
          // "reassess readiness" with no id → only if a single desk row exists.
          pushAll(knownIds);
        }
      }
    }

    // Gate status mutations always recompute derived readiness / next-action
    // for the touched rows so contact_role_status=verified cannot leave
    // verification_status / operator_next_action stale.
    pushAll(gateUpdatedIds);
    return ids;
  })();

  const applied = applyFillableTableFieldUpdates(baseRows, parsed.updates, {
    reassessIds,
  });
  const unknownIds = [
    ...new Set([
      ...applied.unknownIds,
      ...parsed.referencedIds.filter((id) => {
        const key = String(id).toUpperCase();
        return !knownIds.some((known) => String(known).toUpperCase() === key);
      }),
    ]),
  ];

  // Unknown prospect ids → clarify; do not invent rows or drop known ones.
  if (unknownIds.length > 0) {
    const knownLabel = knownIds.length
      ? knownIds.join(', ')
      : '(none on desk)';
    return {
      reason: 'active_work_context_table_update_unknown_prospect',
      structured: buildStructuredResponse({
        answer: [
          'I could not apply that fillable table update.',
          `Unknown prospect_id: ${unknownIds.join(', ')}.`,
          `Known prospect ids on the desk: ${knownLabel}.`,
          'Tell me which known id to update, or paste a corrected field list.',
          'Preparation-only. No mission created. No launch, execution, approval, print, or mail.',
        ].join(' '),
        reasoning: [
          'Fillable table mutation referenced a prospect_id not present in activeWorkContext.',
          'Clarifying instead of inventing rows or parsing update instructions as new prospects.',
          'No mission create/resume.',
        ],
        supportingEvidence: [],
        contradictingEvidence: [],
        confidence: null,
        nextInvestigations: [
          `Update fields using a known prospect_id (${knownLabel}).`,
        ],
        recommendedActions: [],
        metadata: {
          sourcesUsed: { activeWorkContext: true },
          evidenceCount: baseRows.length,
          unavailable: unknownIds.map((id) => `prospect:${id}`),
          surface: 'workspace',
          executionDomain: EXECUTION_DOMAINS.WORKSPACE,
          route: 'intelligence',
          canaryPreparationOnly: true,
          fillableTable: true,
          activeWorkContextReused: true,
          tableUpdate: true,
        },
      }),
    };
  }

  // Reassess asked but no target resolved (multi-row desk, no id named).
  if (shouldReassess && reassessIds.length === 0 && applied.matchedIds.length === 0) {
    const knownLabel = knownIds.length
      ? knownIds.join(', ')
      : '(none on desk)';
    return {
      reason: 'active_work_context_table_reassess_needs_target',
      structured: buildStructuredResponse({
        answer: [
          'I can reassess readiness from the table gates, but I need a prospect_id.',
          `Known prospect ids on the desk: ${knownLabel}.`,
          'Example: reassess PM-001 readiness using the table gates.',
          'Preparation-only. No mission created. No launch, execution, approval, print, or mail.',
        ].join(' '),
        reasoning: [
          'Operator asked for table-gate readiness reassessment without a target prospect_id.',
          'Clarifying instead of mutating every desk row.',
          'No mission create/resume.',
        ],
        supportingEvidence: [],
        contradictingEvidence: [],
        confidence: null,
        nextInvestigations: [
          `Reassess readiness for a known prospect_id (${knownLabel}).`,
        ],
        recommendedActions: [],
        metadata: {
          sourcesUsed: { activeWorkContext: true },
          evidenceCount: baseRows.length,
          unavailable: ['readiness_reassess_target'],
          surface: 'workspace',
          executionDomain: EXECUTION_DOMAINS.WORKSPACE,
          route: 'intelligence',
          canaryPreparationOnly: true,
          fillableTable: true,
          activeWorkContextReused: true,
          tableUpdate: true,
        },
      }),
    };
  }

  const updatedRows = applied.rows;
  const structured = buildCanaryFillableTableResponse({
    prospects,
    tableRows: updatedRows,
    question,
    reusedFromActiveContext: true,
    tableUpdated: true,
    updatedProspectIds: applied.matchedIds,
    reassessedProspectIds: applied.reassessedIds,
  });

  rememberCanaryActiveWorkContext({
    session,
    prospects: syncProspectsFromTableRows(prospects, updatedRows),
    question,
    lastOutputType: LAST_OUTPUT_TYPES.FILLABLE_TABLE,
    prior,
    tableRows: updatedRows,
    nextAction: 'verify_mail_critical_fields',
  });

  return {
    reason: 'active_work_context_fillable_table_update',
    structured,
  };
}

/**
 * Explicit fillable-table update when session desk context is missing
 * (e.g. after refresh). Ask for the table or prospects — never brief /
 * market-intelligence / General Conversation fallback.
 * @param {{ question: string }} input
 */
function buildMissingFillableTableUpdateResponse(input) {
  const question = String(input.question || '');
  const parsed = parseFillableTableFieldUpdates(question);
  const forOnly = /\bfor\s+([A-Za-z0-9_-]+)\s+only\b/i.exec(question);
  const targetId =
    (parsed.updates[0] && parsed.updates[0].prospectId) ||
    (forOnly && forOnly[1]) ||
    null;
  const targetLabel = targetId ? ` the ${targetId} update` : ' that update';

  return buildStructuredResponse({
    answer: [
      'I can update that, but I don’t have the current fillable table in this session.',
      `Paste the table, or paste the 3 Campaign 001 prospects, and I’ll apply${targetLabel}.`,
      'Preparation-only: no mission created; no launch, approval, print, or mail.',
    ].join(' '),
    reasoning: [
      'Operator issued an explicit fillable verification table update, but activeWorkContext is missing.',
      'Clarifying for the current table or prospects instead of falling through to briefing / General Conversation.',
      'Preparation-only constraints preserved — no mission create/resume.',
    ],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: [
      'Paste the current fillable verification table (markdown rows with prospect_id).',
      'Or paste the 3 Campaign 001 prospects (company, contact, website, mailing address, phone).',
    ],
    recommendedActions: [],
    metadata: {
      sourcesUsed: {},
      evidenceCount: 0,
      unavailable: ['active_work_context_fillable_table'],
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      fillableTable: true,
      tableUpdate: true,
      missingActiveWorkContext: true,
      ...(targetId ? { requestedProspectId: targetId } : {}),
    },
  });
}

/**
 * Build a preparation-only packet review artifact from the active canary table.
 * Does not mutate tableRows. Never creates a mission or infers mail/launch.
 * @param {{ question: string, session: object, prior: object }} input
 */
function handlePacketReviewContinuation(input) {
  const question = String(input.question || '');
  const session = input.session;
  const prior = input.prior;
  const knownIds = knownActiveWorkProspectIds(prior);
  const knownLabel = knownIds.length ? knownIds.join(', ') : '(none on desk)';
  const requestedId = extractPacketReviewProspectId(question, knownIds);

  if (!requestedId) {
    return {
      reason: 'active_work_context_packet_review_needs_target',
      structured: buildStructuredResponse({
        answer: [
          'I can build a preparation-only packet review checklist from the current canary table.',
          `Which prospect_id should I use? Known ids on the desk: ${knownLabel}.`,
          'Example: Create a preparation-only packet review checklist for PM-001.',
          'Preparation-only. No mission created. No launch, execution, approval, print, or mail.',
        ].join(' '),
        reasoning: [
          'Operator asked for a packet review artifact without a target prospect_id.',
          'Clarifying instead of inventing a target or requiring a prospect re-paste.',
          'No mission create/resume.',
        ],
        supportingEvidence: [],
        contradictingEvidence: [],
        confidence: null,
        nextInvestigations: [
          `Create a preparation-only packet review checklist for a known prospect_id (${knownLabel}).`,
        ],
        recommendedActions: [],
        metadata: {
          sourcesUsed: { activeWorkContext: true },
          evidenceCount: Array.isArray(prior.tableRows) ? prior.tableRows.length : 0,
          unavailable: ['packet_review_target'],
          surface: 'workspace',
          executionDomain: EXECUTION_DOMAINS.WORKSPACE,
          route: 'intelligence',
          canaryPreparationOnly: true,
          packetReview: true,
          activeWorkContextReused: true,
          tableUpdate: false,
        },
      }),
    };
  }

  const rows = Array.isArray(prior.tableRows) ? prior.tableRows : [];
  const row = rows.find(
    (r) =>
      String((r && r.prospect_id) || '').trim().toUpperCase() ===
      String(requestedId).trim().toUpperCase()
  );

  if (!row) {
    return {
      reason: 'active_work_context_packet_review_unknown_prospect',
      structured: buildStructuredResponse({
        answer: [
          'I could not build that packet review checklist.',
          `Unknown prospect_id: ${requestedId}.`,
          `Known prospect ids on the desk: ${knownLabel}.`,
          'Tell me which known id to use. I will not invent a row or ask you to re-paste the whole list.',
          'Preparation-only. No mission created. No launch, execution, approval, print, or mail.',
        ].join(' '),
        reasoning: [
          'Packet review referenced a prospect_id not present in activeWorkContext.tableRows.',
          'Clarifying instead of inventing rows or falling through to prospect-parse fallback.',
          'No mission create/resume.',
        ],
        supportingEvidence: [],
        contradictingEvidence: [],
        confidence: null,
        nextInvestigations: [
          `Create a packet review checklist for a known prospect_id (${knownLabel}).`,
        ],
        recommendedActions: [],
        metadata: {
          sourcesUsed: { activeWorkContext: true },
          evidenceCount: rows.length,
          unavailable: [`prospect:${requestedId}`],
          surface: 'workspace',
          executionDomain: EXECUTION_DOMAINS.WORKSPACE,
          route: 'intelligence',
          canaryPreparationOnly: true,
          packetReview: true,
          activeWorkContextReused: true,
          tableUpdate: false,
          requestedProspectId: requestedId,
        },
      }),
    };
  }

  const entity = (Array.isArray(prior.entities) ? prior.entities : []).find(
    (e) =>
      String((e && e.id) || '').trim().toUpperCase() ===
      String(requestedId).trim().toUpperCase()
  );

  const structured = buildPacketReviewArtifactResponse({
    row,
    entity,
    question,
    campaignId:
      (prior.target && prior.target.campaignId) ||
      extractCampaignIdFromText(question) ||
      '001',
  });

  rememberCanaryActiveWorkContext({
    session,
    prospects: entitiesToProspects(prior.entities),
    question,
    lastOutputType: LAST_OUTPUT_TYPES.PACKET_REVIEW,
    prior,
    tableRows: prior.tableRows,
    nextAction: 'operator_packet_review_before_mail',
  });

  return {
    reason: 'active_work_context_packet_review',
    structured,
  };
}

/**
 * Resolve readiness rows for a canary summary / judgment request.
 * Order: latest-message compact readiness table → latest fillable paste →
 * activeWorkContext.tableRows → known current-state bullets → null.
 * @param {{ question: string, prior: object|null }} input
 * @returns {{ rows: object[], source: string, canarySummaryRows?: object[] }|null}
 */
function resolveCanarySummaryJudgmentRows(input = {}) {
  const question = String(input.question || '');
  const prior = input.prior || null;

  const toSummaryRows = (rows) =>
    (Array.isArray(rows) ? rows : [])
      .filter(Boolean)
      .map((row) =>
        normalizeReadinessSummaryRow({
          ...row,
          execution_readiness: 'blocked',
        })
      );

  // Latest-message compact readiness table owns canarySummaryRows — do not
  // require fillable columns or treat this paste as a ProspectList.
  const readiness = parseReadinessSummaryTableFromMessage(question);
  if (readiness && Array.isArray(readiness.rows) && readiness.rows.length > 0) {
    const rows = toSummaryRows(readiness.rows);
    return {
      rows,
      source: 'readiness_summary_table',
      canarySummaryRows: rows,
    };
  }

  // Full fillable paste in the same turn can also feed summary rows.
  const fillable = parseFillableVerificationTableFromMessage(question);
  if (fillable && Array.isArray(fillable.rows) && fillable.rows.length > 0) {
    const rows = toSummaryRows(fillable.rows);
    return {
      rows,
      source: 'fillable_verification_table',
      canarySummaryRows: rows,
    };
  }

  if (prior && activeContextHasFillableTable(prior)) {
    const rows = toSummaryRows(prior.tableRows);
    if (rows.length > 0) {
      return {
        rows,
        source: 'active_work_context',
        canarySummaryRows: rows,
      };
    }
  }

  const known = parseKnownCurrentStateBullets(question);
  if (known && known.hasKnownState && known.rows.length > 0) {
    const contract = resolveCanaryWorkflowContract({
      question,
      rows: known.rows,
      prior,
    });
    const rows = known.rows.map((row) => ({
      ...row,
      execution_readiness: 'blocked',
      operator_next_action:
        row.operator_next_action ||
        resolveCanarySummaryNextAction(row, contract),
    }));
    return {
      rows,
      source: 'known_current_state',
      canarySummaryRows: rows,
    };
  }

  return null;
}

/**
 * Cross-prospect preparation-only canary status summary / judgment.
 * Never creates a mission or falls through to prospect-parse clarification.
 * @param {{ question: string, session?: object|null, prior?: object|null }} input
 */
function handleCanarySummaryJudgmentContinuation(input = {}) {
  const question = String(input.question || '');
  const session = input.session || null;
  const prior = input.prior || getActiveWorkContext(session) || null;
  const focusedWorkOrder = isFocusedCanaryWorkOrderRequest(question);
  const diagnostics = emitCanaryReadinessIngestDiagnostics(question, {
    stage: focusedWorkOrder
      ? 'canary_focused_work_order'
      : 'canary_summary_judgment',
    summaryIntent: !focusedWorkOrder,
  });
  const resolved = resolveCanarySummaryJudgmentRows({ question, prior });

  if (!resolved || !resolved.rows.length) {
    const tableCuesPresent = hasCanaryReadinessTableCues(question);
    return {
      reason: tableCuesPresent
        ? focusedWorkOrder
          ? 'canary_focused_work_order_table_not_ingested'
          : 'canary_summary_table_not_ingested'
        : focusedWorkOrder
          ? 'canary_focused_work_order_missing_state'
          : 'canary_summary_missing_state',
      structured: buildMissingCanarySummaryJudgmentResponse({
        question,
        tableCuesPresent,
        diagnostics,
        focusedWorkOrder,
      }),
    };
  }

  const campaignId =
    (prior && prior.target && prior.target.campaignId) ||
    extractCampaignIdFromText(question) ||
    '001';

  if (focusedWorkOrder) {
    const structured = buildFocusedCanaryWorkOrderResponse({
      rows: resolved.rows,
      question,
      source: resolved.source,
      campaignId,
      prior,
    });
    rememberCanaryJudgmentActiveWorkContext({
      session,
      prior,
      question,
      campaignId,
      rows: resolved.rows,
      lastOutputType: LAST_OUTPUT_TYPES.FOCUSED_WORK_ORDER,
      prioritizedProspectId:
        structured.metadata && structured.metadata.prioritizedProspectId,
      canaryWorkflowType:
        structured.metadata && structured.metadata.canaryWorkflowType,
    });
    return {
      reason:
        resolved.source === 'active_work_context'
          ? 'active_work_context_focused_work_order'
          : resolved.source === 'readiness_summary_table'
            ? 'readiness_summary_table_focused_work_order'
            : resolved.source === 'fillable_verification_table'
              ? 'fillable_table_focused_work_order'
              : 'known_current_state_focused_work_order',
      structured,
    };
  }

  const structured = buildCanarySummaryJudgmentResponse({
    rows: resolved.rows,
    question,
    source: resolved.source,
    campaignId,
    prior,
  });
  rememberCanaryJudgmentActiveWorkContext({
    session,
    prior,
    question,
    campaignId,
    rows: resolved.rows,
    lastOutputType: LAST_OUTPUT_TYPES.CANARY_SUMMARY,
    prioritizedProspectId:
      structured.metadata && structured.metadata.prioritizedProspectId,
    canaryWorkflowType:
      structured.metadata && structured.metadata.canaryWorkflowType,
  });

  return {
    reason:
      resolved.source === 'active_work_context'
        ? 'active_work_context_canary_summary'
        : resolved.source === 'readiness_summary_table'
          ? 'readiness_summary_table_canary_summary'
          : resolved.source === 'fillable_verification_table'
            ? 'fillable_table_canary_summary'
            : 'known_current_state_canary_summary',
    structured,
  };
}

/**
 * Persist desk memory after canary summary / focused work-order artifacts so
 * suggestion chips follow the response subtype instead of the readiness-table
 * ingest placeholder (canary_readiness_table → review package).
 * @param {{ session?: object|null, prior?: object|null, question?: string, campaignId?: string, rows?: object[], lastOutputType: string, prioritizedProspectId?: string|null, canaryWorkflowType?: string|null }} input
 */
function rememberCanaryJudgmentActiveWorkContext(input = {}) {
  const session = input.session || null;
  if (!session) return null;

  const rows = Array.isArray(input.rows) ? input.rows : [];
  const prior = input.prior || getActiveWorkContext(session) || null;
  const canaryWorkflowType =
    input.canaryWorkflowType ||
    resolveCanaryWorkflowType(input.question || '', rows, prior);
  const tableRows =
    rows.length > 0
      ? rows.map((row) => ({ ...row, execution_readiness: 'blocked' }))
      : prior && Array.isArray(prior.tableRows)
        ? prior.tableRows
        : [];
  const entities = entitiesFromFillableTableRows(
    tableRows,
    prior && Array.isArray(prior.entities) ? prior.entities : []
  );
  const prospects = entitiesToProspects(entities);
  if (!prospects.length && !(prior && activeContextHasEntities(prior))) {
    return null;
  }

  return rememberCanaryActiveWorkContext({
    session,
    prospects: prospects.length
      ? prospects
      : entitiesToProspects(prior.entities),
    question: input.question,
    campaignId: input.campaignId,
    lastOutputType: input.lastOutputType,
    lastOutputKind: input.lastOutputType,
    prior,
    tableRows: tableRows.length
      ? tableRows
      : prior && prior.tableRows
        ? prior.tableRows
        : [],
    canaryWorkflowType,
    nextAction:
      input.lastOutputType === LAST_OUTPUT_TYPES.FOCUSED_WORK_ORDER
        ? input.prioritizedProspectId
          ? `operator_next_work_order_${String(input.prioritizedProspectId).toLowerCase()}`
          : 'operator_next_focused_work_order'
        : 'await_operator_packet_review_or_verification',
  });
}

/**
 * Ask for the current canary table / known state — never pipe-format prospects.
 * When summary intent is clear and readiness-table cues were present but rows
 * did not parse, return a targeted clarification (not a generic missing-state
 * or prospect-parse fallthrough).
 * @param {{ question?: string, tableCuesPresent?: boolean, diagnostics?: object|null, focusedWorkOrder?: boolean }} input
 */
function buildMissingCanarySummaryJudgmentResponse(input = {}) {
  const question = String(input.question || '');
  const tableCuesPresent =
    input.tableCuesPresent === true || hasCanaryReadinessTableCues(question);
  const focusedWorkOrder = input.focusedWorkOrder === true;
  const contract = resolveCanaryWorkflowContract({ question });
  const safety = formatCanarySafetyLine(contract);
  const readinessHint =
    contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? 'prospect_id, company, contact, phone_status, contact_role_status, call_readiness, script_readiness, execution_readiness'
      : 'prospect_id, company, contact, gate summary, mail_readiness, draft_readiness, execution_readiness';
  const diagnostics =
    input.diagnostics ||
    (shouldEmitCanaryReadinessDiagnostics()
      ? diagnoseCanaryReadinessTableIngestion(question)
      : null);

  const answer = focusedWorkOrder
    ? tableCuesPresent
      ? [
          'I can choose the next preparation-only work order, but the readiness table did not come through. Paste either the compact readiness table or one state line per prospect.',
          safety,
        ].join(' ')
      : [
          'I can create the next preparation-only work order for Campaign 001, but I don’t have the current table or known state for those prospects.',
          `Paste the fillable verification table, continue from a session with the active canary table, or provide known current-state bullets (${readinessHint}).`,
          safety,
        ].join(' ')
    : tableCuesPresent
      ? [
          'I can summarize this, but the readiness table did not come through. Paste either the compact readiness table or one state line per prospect.',
          safety,
        ].join(' ')
      : [
          `I can summarize Campaign 001 ${contract.statusLabel} status and judge next actions, but I don’t have the current table or known state for those prospects.`,
          `Paste the fillable verification table, continue from a session with the active canary table, or provide known current-state bullets (${readinessHint}).`,
          safety,
        ].join(' ');

  const reasoning = focusedWorkOrder
    ? tableCuesPresent
      ? [
          'Operator asked for the next focused preparation-only work order and the message included readiness-table cues, but canarySummaryRows did not parse.',
          'Clarifying for a compact readiness table or per-prospect state lines instead of falling through to prospect-parse fallback.',
          'No mission create/resume.',
        ]
      : [
          'Operator asked for the next focused preparation-only work order without desk tableRows or known current-state bullets.',
          'Clarifying for current state instead of falling through to prospect-parse fallback.',
          'No mission create/resume.',
        ]
    : tableCuesPresent
      ? [
          'Operator asked for a canary status summary / judgment and the message included readiness-table cues, but canarySummaryRows did not parse.',
          'Clarifying for a compact readiness table or per-prospect state lines instead of falling through to prospect-parse fallback.',
          'No mission create/resume.',
        ]
      : [
          'Operator asked for a canary status summary / judgment without desk tableRows or known current-state bullets.',
          'Clarifying for current state instead of falling through to prospect-parse fallback.',
          'No mission create/resume.',
        ];

  const outputKind = focusedWorkOrder
    ? LAST_OUTPUT_TYPES.FOCUSED_WORK_ORDER
    : LAST_OUTPUT_TYPES.CANARY_SUMMARY;

  return buildStructuredResponse({
    answer,
    reasoning,
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: [
      tableCuesPresent
        ? 'Paste the compact readiness table (prospect_id / company / contact / verification_summary / readiness columns) or one known-state line per prospect, then ask again.'
        : focusedWorkOrder
          ? 'Paste the Campaign 001 fillable verification table or known current-state bullets, then ask again for the next preparation-only work order.'
          : 'Paste the Campaign 001 fillable verification table or known current-state bullets, then ask again for the preparation-only canary status summary.',
    ],
    recommendedActions: [],
    metadata: {
      sourcesUsed: {},
      evidenceCount: 0,
      unavailable: tableCuesPresent
        ? ['canary_summary_table_rows']
        : ['canary_summary_state'],
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      canaryWorkflowType: contract.type,
      canarySummary: !focusedWorkOrder,
      focusedWorkOrder: focusedWorkOrder || undefined,
      missingActiveWorkContext: true,
      readinessTableNotIngested: tableCuesPresent || undefined,
      outputKind,
      lastOutputKind: outputKind,
      outputSubtype: focusedWorkOrder ? 'focused_work_order' : 'summary',
      strictOutputShape: wantsPacketReviewArtifactSuppression(question),
      ...(diagnostics
        ? { canaryReadinessIngestDiagnostics: diagnostics }
        : {}),
    },
  });
}

/**
 * Format a compact gate summary for the readiness table.
 * @param {object} row
 * @param {object} [contract]
 * @returns {string}
 */
function formatCanarySummaryGateSummary(row, contract = null) {
  if (row && row.gate_summary && String(row.gate_summary).trim()) {
    return String(row.gate_summary).trim();
  }
  if (row && row.verification_summary && String(row.verification_summary).trim()) {
    return String(row.verification_summary).trim();
  }
  const gates =
    (contract && Array.isArray(contract.verificationGates)
      ? contract.verificationGates
      : ['website_status', 'mailing_address_status', 'phone_status', 'contact_role_status']);
  const labels =
    (contract && contract.gateLabels) ||
    {
      website_status: 'website',
      mailing_address_status: 'address',
      phone_status: 'phone',
      contact_role_status: 'contact role',
    };
  const parts = gates.map((field) => {
    const label = labels[field] || field.replace(/_status$/, '').replace(/_/g, ' ');
    return `${label} ${row[field] || 'unknown'}`;
  });
  return parts.join('; ');
}

/**
 * True when call-script review is already complete for the preparation queue.
 * Reviewed script work must not be re-recommended; dial/call stays a separate
 * explicit operator ask.
 * @param {object|null|undefined} row
 * @returns {boolean}
 */
function isCallScriptReviewCompleteForPrepQueue(row) {
  const status = String((row && row.call_script_review_status) || '')
    .trim()
    .toLowerCase();
  return status === 'reviewed' || status === 'content_reviewed';
}

/**
 * True when approved_for_dial is falsey / not granted.
 * @param {unknown} value
 * @returns {boolean}
 */
function isApprovedForDialFalsey(value) {
  const raw = String(value == null ? 'false' : value)
    .trim()
    .toLowerCase();
  return (
    raw === '' ||
    raw === 'false' ||
    raw === '0' ||
    raw === 'no' ||
    raw === 'pending' ||
    raw === 'not_approved'
  );
}

/**
 * True when dial/call approval is still pending (not an outbound work order).
 * @param {object|null|undefined} row
 * @returns {boolean}
 */
function isAwaitingFutureDialCallApproval(row) {
  if (!isCallScriptReviewCompleteForPrepQueue(row)) return false;
  const dialStatus = String((row && row.dial_call_approval_status) || 'pending')
    .trim()
    .toLowerCase();
  return (
    isApprovedForDialFalsey(row && row.approved_for_dial) ||
    dialStatus === '' ||
    dialStatus === 'pending' ||
    dialStatus === 'not_approved'
  );
}

/**
 * Exact next operator action for a summary row, with judgment overrides.
 * @param {object} row
 * @param {object} contract
 * @returns {string}
 */
function resolveCanarySummaryNextAction(row, contract) {
  const primaryField =
    (contract && contract.primaryReadinessField) || 'mail_readiness';
  const primary = String((row && row[primaryField]) || 'blocked').toLowerCase();

  if (
    contract &&
    contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP &&
    isCallScriptReviewCompleteForPrepQueue(row)
  ) {
    return 'Await separate explicit dial/call approval (do not dial, call, text, or email yet)';
  }

  if (isReadyForReviewValue(primary)) {
    if (contract && contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP) {
      return 'Run preparation-only call-script review (execution remains blocked; do not dial, call, text, or email yet)';
    }
    return 'Run preparation-only packet review / operator packet-content approval (execution remains blocked; do not print or mail yet)';
  }

  // Workflow-aware verification actions outrank any mail-oriented
  // operator_next_action that may have been prefilled from known-state ingest.
  if (contract && contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP) {
    const phoneOk = isVerifiedGateStatus(row && row.phone_status);
    const roleOk =
      isVerifiedGateStatus(row && row.contact_role_status) &&
      !isNeedsVerificationGateStatus(row && row.contact_role_status);
    if (!phoneOk && !roleOk) {
      return 'verify phone and contact role';
    }
    if (!phoneOk) {
      return 'verify phone';
    }
    if (!roleOk) {
      return 'verify contact role';
    }
    return 'Complete phone/contact-role verification';
  }

  if (row && row.operator_next_action && String(row.operator_next_action).trim()) {
    return String(row.operator_next_action).trim();
  }
  return deriveOperatorNextActionFromGates(row) || 'Complete verification work';
}

/**
 * Enrich readiness rows and pick the single next work-order prospect.
 * Prefer open primary readiness=ready_for_review (not already-reviewed script
 * work); else first verification-blocked row. Reviewed call-script prospects
 * waiting on dial/call approval are not preparation next-work-orders.
 * @param {object[]} rows
 * @param {object} [contract]
 * @returns {{ enriched: object[], prioritize: object|null, reviewReady: object[], needsVerification: object[], draftAllowed: object[], awaitingOutboundApproval: object[] }}
 */
function selectNextCanaryWorkOrderProspect(rows, contract = null) {
  const resolvedContract =
    contract ||
    resolveCanaryWorkflowContract({ rows: Array.isArray(rows) ? rows : [] });
  const primaryField = resolvedContract.primaryReadinessField || 'mail_readiness';
  const draftField = resolvedContract.draftReadinessField || 'draft_readiness';
  const preferredId = `${resolvedContract.prospectIdPrefix || 'PM'}-001`;
  const isCallPrep =
    resolvedContract.type === CANARY_WORKFLOW_TYPES.CALL_PREP;

  const enriched = (Array.isArray(rows) ? rows : []).map((row) => {
    const draft = String(row[draftField] || 'blocked').toLowerCase();
    const primary = String(row[primaryField] || '').toLowerCase();
    const scriptReviewComplete =
      isCallPrep && isCallScriptReviewCompleteForPrepQueue(row);
    const awaitingOutboundApproval =
      isCallPrep && isAwaitingFutureDialCallApproval(row);
    const primaryReady = isReadyForReviewValue(primary) && !scriptReviewComplete;
    const nextAction = resolveCanarySummaryNextAction(row, resolvedContract);
    return {
      ...row,
      execution_readiness: 'blocked',
      operator_next_action: nextAction,
      draft_allowed: isDraftAllowedValue(draft),
      mail_ready_for_review: isReadyForReviewValue(primary),
      primary_ready_for_review: primaryReady,
      call_script_review_complete: scriptReviewComplete,
      awaiting_outbound_approval: awaitingOutboundApproval,
      primary_readiness: primary || 'blocked',
      draft_readiness_value: draft || 'blocked',
    };
  });

  const reviewReady = enriched.filter((r) => r.primary_ready_for_review);
  const awaitingOutboundApproval = enriched.filter(
    (r) => r.awaiting_outbound_approval
  );
  const needsVerification = enriched.filter(
    (r) => !r.primary_ready_for_review && !r.awaiting_outbound_approval
  );
  const draftAllowed = enriched.filter((r) => r.draft_allowed);

  let prioritize = reviewReady[0] || null;
  if (!prioritize) {
    prioritize = needsVerification[0] || null;
  }
  if (!prioritize && enriched.length) {
    const preferred = enriched.find(
      (r) =>
        String(r.prospect_id || '')
          .toUpperCase()
          .trim() === preferredId
    );
    if (preferred && !preferred.awaiting_outbound_approval) {
      prioritize = preferred;
    }
  }
  if (!prioritize) {
    prioritize =
      enriched.find((r) => !r.awaiting_outbound_approval) || null;
  }

  return {
    enriched,
    prioritize,
    reviewReady,
    needsVerification,
    draftAllowed,
    awaitingOutboundApproval,
  };
}

/**
 * True when gate status is verified.
 * Local helper so call-prep judgment does not depend on fillable-table exports.
 * @param {unknown} status
 */
function isVerifiedGateStatus(status) {
  return /^verified$/i.test(String(status || '').trim());
}

/**
 * @param {unknown} status
 */
function isNeedsVerificationGateStatus(status) {
  const value = String(status || '')
    .trim()
    .toLowerCase();
  return (
    value === 'needs_verification' ||
    value === 'needs verification' ||
    value === 'unknown' ||
    value === 'blocked' ||
    value === ''
  );
}

/**
 * Build markdown readiness table body for canary judgment artifacts.
 * @param {object[]} enriched
 * @param {object} contract
 * @returns {{ header: string, sep: string, body: string }}
 */
function buildCanaryReadinessMarkdownTable(enriched, contract) {
  const columns = (contract && contract.readinessTableColumns) || [
    'prospect_id',
    'company_name',
    'contact_name',
    'mail_readiness',
    'draft_readiness',
    'execution_readiness',
    'gate_summary',
  ];
  const header = `| ${columns.join(' | ')} |`;
  const sep = `|${columns.map(() => '---').join('|')}|`;
  const body = (Array.isArray(enriched) ? enriched : [])
    .map((row) => {
      const cells = columns.map((col) => {
        if (col === 'execution_readiness') return 'blocked';
        if (col === 'gate_summary') {
          return formatCanarySummaryGateSummary(row, contract);
        }
        if (
          col === 'prospect_id' ||
          col === 'company_name' ||
          col === 'contact_name'
        ) {
          return row[col] || '';
        }
        return row[col] || 'blocked';
      });
      return `| ${cells.join(' | ')} |`;
    })
    .join('\n');
  return { header, sep, body };
}

/**
 * One selected preparation-only work order — not the full canary summary.
 * @param {{ rows: object[], question?: string, source?: string, campaignId?: string, prior?: object|null }} input
 */
function buildFocusedCanaryWorkOrderResponse(input = {}) {
  const question = String(input.question || '');
  const campaignId = String(input.campaignId || '001');
  const fromKnownState = input.source === 'known_current_state';
  const fromReadinessTable =
    input.source === 'readiness_summary_table' ||
    input.source === 'fillable_verification_table';
  const fromPastedState = fromKnownState || fromReadinessTable;
  const suppressScaffolding = wantsPacketReviewArtifactSuppression(question);
  const debugOutput =
    !suppressScaffolding && wantsPacketReviewDebugOutput(question);
  const contract = resolveCanaryWorkflowContract({
    question,
    rows: input.rows,
    prior: input.prior,
  });
  const primaryField = contract.primaryReadinessField;
  const draftField = contract.draftReadinessField;
  const safety = formatCanarySafetyLine(contract);
  const blockedVerbs = formatBlockedOutboundVerbList(contract);

  const {
    enriched,
    prioritize,
    reviewReady,
    needsVerification,
    awaitingOutboundApproval,
  } = selectNextCanaryWorkOrderProspect(input.rows, contract);

  const prioritizeId = prioritize
    ? String(prioritize.prospect_id || '').trim()
    : null;
  const packetReviewWorkOrder =
    prioritize && prioritize.primary_ready_for_review === true;
  const includeReadinessTable =
    contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP;

  const deferred = needsVerification
    .map((r) => String(r.prospect_id || '').trim())
    .filter(Boolean);
  const deferredLabel =
    deferred.length === 0
      ? 'None'
      : deferred.length === 1
        ? deferred[0]
        : deferred.length === 2
          ? `${deferred[0]} and ${deferred[1]}`
          : `${deferred.slice(0, -1).join(', ')}, and ${deferred[deferred.length - 1]}`;

  const awaitingIds = (awaitingOutboundApproval || [])
    .map((r) => String(r.prospect_id || '').trim())
    .filter(Boolean);
  const awaitingLabel =
    awaitingIds.length === 0
      ? ''
      : awaitingIds.length === 1
        ? awaitingIds[0]
        : awaitingIds.length === 2
          ? `${awaitingIds[0]} and ${awaitingIds[1]}`
          : `${awaitingIds.slice(0, -1).join(', ')}, and ${awaitingIds[awaitingIds.length - 1]}`;

  const workOrderTitle = packetReviewWorkOrder
    ? `${prioritizeId} ${contract.reviewWorkOrderShort}.`
    : prioritizeId
      ? `${prioritizeId} ${contract.verificationWorkOrderLabel}.`
      : 'No work order available.';

  const whyFirst = !prioritize
    ? 'No prospects are available to prioritize.'
    : packetReviewWorkOrder
      ? reviewReady.length === 1
        ? `${prioritizeId} is the only prospect with ${primaryField}=ready_for_review while execution_readiness remains blocked. ${deferredLabel} still need ${contract.verificationDeferredLabel}.`
        : `${prioritizeId} has ${primaryField}=ready_for_review while execution_readiness remains blocked. Other ready_for_review prospects are deferred until this work order completes; ${deferredLabel === 'None' ? 'no other prospects still need verification.' : `${deferredLabel} still need ${contract.verificationDeferredLabel}.`}`
      : awaitingIds.length && contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? `${awaitingLabel} script review is already complete and outbound approval is pending; ${prioritizeId} is the next listed blocked prospect whose verification would move call_readiness forward.`
        : `${prioritizeId} still needs ${contract.verificationDeferredLabel} before ${contract.reviewWorkOrderLabel}; do not ${blockedVerbs}. Deferred prospects: ${deferred.filter((id) => id !== prioritizeId).join(', ') || 'none'}.`;

  const operatorSteps = packetReviewWorkOrder
    ? contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? [
          `1. Review ${prioritizeId} call-script contents.`,
          '2. Confirm provisional script is acceptable.',
          '3. Confirm phone and contact role remain verified.',
          '4. Record call-script review decision.',
          '5. Do not dial/call/text/email unless a separate future outbound approval is given.',
        ]
      : [
          `1. Review ${prioritizeId} packet contents.`,
          '2. Confirm customer-facing drafts are acceptable.',
          '3. Confirm print/sign/mail checklist is complete.',
          '4. Record packet-content review decision.',
          '5. Do not print/mail unless a separate future launch/mail approval is given.',
        ]
    : contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? [
          `1. Complete phone/contact-role verification for ${prioritizeId}.`,
          `2. Update ${primaryField} / ${draftField} / execution_readiness from verified gates.`,
          `3. Do not start call-script review until ${primaryField} is ready_for_review.`,
          '4. Do not dial/call/text/email unless a separate future outbound approval is given.',
        ]
      : [
          `1. Complete verification for ${prioritizeId} (website, mailing address, phone, contact role as needed).`,
          `2. Update ${primaryField} / ${draftField} / execution_readiness from verified gates.`,
          `3. Do not start packet review until ${primaryField} is ready_for_review.`,
          '4. Do not print/mail unless a separate future launch/mail approval is given.',
        ];

  const maxCanPrepare = packetReviewWorkOrder
    ? [
        `- ${prioritizeId} ${contract.reviewArtifactName} review checklist`,
        `- ${contract.draftArtifactLabel}`,
        '- tracking fields for review',
      ]
    : [
        `- ${prioritizeId} verification checklist`,
        '- gate status update fields',
        '- tracking fields for verification progress',
      ];

  const maxMustNot = [
    '- create a mission',
    `- ${contract.maxMustNotOutboundLine || blockedVerbs}`,
    '- mark execution ready',
  ];
  if (contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP && awaitingIds.length) {
    maxMustNot.push(
      `- dial or call ${awaitingLabel} without separate explicit dial/call approval`,
      `- re-run call-script review for ${awaitingLabel}`
    );
  }

  const deferredLines = needsVerification
    .filter((r) => {
      const id = String(r.prospect_id || '').trim();
      return id && id !== prioritizeId;
    })
    .map((row) => {
      const id = String(row.prospect_id || '').trim();
      const action =
        row.operator_next_action ||
        (contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
          ? 'Complete phone/contact-role verification'
          : 'Complete verification work');
      return `- ${id}: ${action}`;
    });

  const awaitingLines = (awaitingOutboundApproval || []).map((row) => {
    const id = String(row.prospect_id || '').trim();
    return `- ${id}: waiting on future explicit dial/call approval (call-script review complete; not a dial work order)`;
  });

  const eligibilityId =
    prioritizeId || `${contract.prospectIdPrefix || 'PM'}-001`;
  const includeFutureMailingEligibility =
    wantsFocusedFutureMailingEligibilitySection(question);
  const includeFinalApprovalGate =
    wantsFocusedFinalApprovalGateSection(question) ||
    contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP;

  const awaitingStatusClause =
    awaitingIds.length && contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? `; ${awaitingIds.length} waiting on future dial/call approval`
      : '';
  const overallStatus = reviewReady.length
    ? `Campaign ${campaignId} ${contract.statusLabel}: ${reviewReady.length} prospect(s) ready_for_review for ${contract.reviewWorkOrderLabel}; ${needsVerification.length} still need ${contract.verificationDeferredLabel}${awaitingStatusClause}; ${contract.overallSafetyClause || 'nothing launched or executed'}.`
    : needsVerification.length
      ? `Campaign ${campaignId} ${contract.statusLabel}: ${needsVerification.length} prospect(s) still need ${contract.verificationDeferredLabel} before ${contract.reviewWorkOrderLabel}${awaitingStatusClause}; ${contract.overallSafetyClause || 'nothing launched or executed'}.`
      : awaitingIds.length && contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? `Campaign ${campaignId} ${contract.statusLabel}: no open preparation work orders; ${awaitingIds.length} waiting on future dial/call approval; ${contract.overallSafetyClause || 'nothing launched or executed'}.`
        : `Campaign ${campaignId} ${contract.statusLabel}: all listed prospects still need ${contract.verificationDeferredLabel} before ${contract.reviewWorkOrderLabel}; ${contract.overallSafetyClause || 'nothing launched or executed'}.`;

  const readinessTable = buildCanaryReadinessMarkdownTable(enriched, contract);

  const remainsBlocked = [
    'All prospects: execution_readiness remains blocked.',
    contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? contract.remainsBlockedOutboundLine ||
        "Outbound remains blocked until the prospect's call readiness is ready_for_review, readiness remains current, and the operator gives explicit future dial/call approval."
      : `Printing and mailing stay blocked until ${primaryField} is ready_for_review AND the operator later gives explicit launch/mail approval.`,
    awaitingIds.length && contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? `${awaitingLabel}: call-script review complete; dial/call approval still pending.`
      : null,
    needsVerification.length
      ? `${needsVerification.map((r) => r.prospect_id).join(', ')} remain blocked pending ${contract.verificationDeferredLabel}.`
      : null,
  ]
    .filter(Boolean)
    .join(' ');

  const answerLines = [];

  if (includeReadinessTable) {
    answerLines.push(
      overallStatus,
      '',
      'Readiness table:',
      readinessTable.header,
      readinessTable.sep,
      readinessTable.body,
      ''
    );
  }

  answerLines.push(
    'Recommended next work order:',
    workOrderTitle,
    '',
    'Why this work order is first:',
    whyFirst,
    '',
    'Exact operator steps:',
    ...operatorSteps,
    '',
    'What Max can prepare next:',
    ...maxCanPrepare,
    '',
    'What Max must not do:',
    ...maxMustNot
  );

  if (includeFutureMailingEligibility) {
    answerLines.push(
      '',
      `${contract.futureEligibilityLabel} for ${eligibilityId}:`,
      ...(contract.futureEligibilityItems || []).map((item) => `- ${item}`)
    );
  }

  answerLines.push(
    '',
    'Deferred prospects:',
    ...(deferredLines.length ? deferredLines : ['- None'])
  );

  if (contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP) {
    answerLines.push(
      '',
      'Waiting on future explicit dial/call approval:',
      ...(awaitingLines.length ? awaitingLines : ['- None'])
    );
    answerLines.push('', 'What remains blocked:', remainsBlocked);
  }

  if (includeFinalApprovalGate) {
    answerLines.push(
      '',
      'Final approval gate before outbound action:',
      contract.finalApprovalGateLine
    );
  }

  answerLines.push('', safety);

  const answer = answerLines.join('\n');

  const reasoning = [
    fromKnownState
      ? 'Built focused next work order from known current-state bullets; not treated as a new prospect paste.'
      : fromReadinessTable
        ? 'Built focused next work order from pasted readiness summary table; not treated as a new prospect paste.'
        : 'Reused activeWorkContext.tableRows for focused next work order; no prospect re-paste required.',
    prioritize && prioritize.primary_ready_for_review
      ? `${prioritizeId} selected for ${contract.reviewWorkOrderLabel} because ${primaryField} is ready_for_review and execution remains blocked.`
      : awaitingIds.length && contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? `Skipped reviewed call-script work (${awaitingLabel}); selected ${prioritizeId} verification as next preparation-only work order.`
        : `No ${primaryField}=ready_for_review prospect — verification work is the selected next work order.`,
    'Returned one work order only; full canary summary suppressed unless readiness table was requested.',
    'No mission create/resume.',
  ];

  return buildStructuredResponse({
    answer,
    reasoning: debugOutput ? reasoning : [],
    supportingEvidence: debugOutput
      ? enriched.map((row) => ({
          id: `canary-prospect:${row.prospect_id}`,
          summary: `${row.company_name || row.prospect_id}: ${primaryField} ${row[primaryField]}, ${draftField} ${row[draftField]}, execution blocked`,
          sourceType: 'operator',
          confidence: null,
        }))
      : [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: debugOutput
      ? [
          prioritize && prioritize.primary_ready_for_review
            ? contract.suggestionReviewChip(prioritizeId)
            : 'Paste updated verification state or continue verification work for blocked prospects.',
        ]
      : [],
    recommendedActions: debugOutput
      ? [
          'Execute the selected preparation-only work order, then explicitly request review or verification work — never launch from this artifact.',
        ]
      : [],
    metadata: {
      sourcesUsed: fromKnownState
        ? { knownCurrentState: true }
        : fromReadinessTable
          ? { readinessSummaryTable: true }
          : { activeWorkContext: true },
      evidenceCount: debugOutput ? enriched.length : 0,
      unavailable: debugOutput ? [] : [],
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      canaryWorkflowType: contract.type,
      focusedWorkOrder: true,
      knownCurrentState: fromKnownState || undefined,
      readinessSummaryTable: fromReadinessTable || undefined,
      activeWorkContextReused: !fromPastedState,
      tableUpdate: false,
      campaignId,
      prospectCount: enriched.length,
      prioritizedProspectId: prioritizeId,
      mailReadiness:
        contract.type === CANARY_WORKFLOW_TYPES.DIRECT_MAIL
          ? (prioritize && prioritize.mail_readiness) || null
          : undefined,
      callReadiness:
        contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
          ? (prioritize && prioritize.call_readiness) || null
          : undefined,
      primaryReadiness: (prioritize && prioritize[primaryField]) || null,
      executionReadiness: 'blocked',
      outputKind: LAST_OUTPUT_TYPES.FOCUSED_WORK_ORDER,
      lastOutputKind: LAST_OUTPUT_TYPES.FOCUSED_WORK_ORDER,
      outputSubtype: 'focused_work_order',
      contextHints: {
        workflow: CAMPAIGN_001_PREPARATION_ONLY_CANARY,
        canaryWorkflowType: contract.type,
        lastOutputKind: LAST_OUTPUT_TYPES.FOCUSED_WORK_ORDER,
        lastOutputType: LAST_OUTPUT_TYPES.FOCUSED_WORK_ORDER,
        outputKind: LAST_OUTPUT_TYPES.FOCUSED_WORK_ORDER,
        preparationOnly: true,
        focusedWorkOrder: true,
        campaignId,
        prospectId: prioritizeId,
        mailReadiness:
          contract.type === CANARY_WORKFLOW_TYPES.DIRECT_MAIL
            ? (prioritize && prioritize.mail_readiness) || null
            : undefined,
        callReadiness:
          contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
            ? (prioritize && prioritize.call_readiness) || null
            : undefined,
        executionReadiness: 'blocked',
        knownCurrentState: fromKnownState || undefined,
        readinessSummaryTable: fromReadinessTable || undefined,
      },
      strictOutputShape: !debugOutput,
    },
  });
}

/**
 * Cross-prospect preparation-only canary summary / judgment artifact.
 * @param {{ rows: object[], question?: string, source?: string, campaignId?: string, prior?: object|null }} input
 */
function buildCanarySummaryJudgmentResponse(input = {}) {
  const rows = (Array.isArray(input.rows) ? input.rows : []).map((row) => ({
    ...row,
    execution_readiness: 'blocked',
  }));
  const question = String(input.question || '');
  const campaignId = String(input.campaignId || '001');
  const fromKnownState = input.source === 'known_current_state';
  const fromReadinessTable =
    input.source === 'readiness_summary_table' ||
    input.source === 'fillable_verification_table';
  const fromPastedState = fromKnownState || fromReadinessTable;
  const suppressScaffolding = wantsPacketReviewArtifactSuppression(question);
  const debugOutput =
    !suppressScaffolding && wantsPacketReviewDebugOutput(question);
  const contract = resolveCanaryWorkflowContract({
    question,
    rows,
    prior: input.prior,
  });
  const primaryField = contract.primaryReadinessField;
  const draftField = contract.draftReadinessField;
  const safety = formatCanarySafetyLine(contract);

  const {
    enriched,
    prioritize,
    reviewReady,
    needsVerification,
    draftAllowed,
    awaitingOutboundApproval,
  } = selectNextCanaryWorkOrderProspect(rows, contract);

  const prioritizeId = prioritize
    ? String(prioritize.prospect_id || '').trim()
    : null;
  const awaitingIds = (awaitingOutboundApproval || [])
    .map((r) => String(r.prospect_id || '').trim())
    .filter(Boolean);
  const awaitingLabel =
    awaitingIds.length === 0
      ? ''
      : awaitingIds.length === 1
        ? awaitingIds[0]
        : awaitingIds.length === 2
          ? `${awaitingIds[0]} and ${awaitingIds[1]}`
          : `${awaitingIds.slice(0, -1).join(', ')}, and ${awaitingIds[awaitingIds.length - 1]}`;
  const prioritizeWhy = prioritize
    ? prioritize.primary_ready_for_review
      ? contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? `${prioritizeId} has ${primaryField} ready_for_review while execution_readiness remains blocked — prioritize preparation-only ${contract.reviewWorkOrderLabel} before any outbound action.`
        : `${prioritizeId} has mail_readiness ready_for_review while execution_readiness remains blocked — prioritize preparation-only packet review / final packet review decision before any print or mail.`
      : awaitingIds.length && contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? `${awaitingLabel} script review is already complete and outbound approval is pending; ${prioritizeId} is the next listed blocked prospect whose verification would move call_readiness forward.`
        : contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
          ? `${prioritizeId} still needs ${contract.verificationDeferredLabel} before ${contract.reviewWorkOrderLabel}; do not ${formatBlockedOutboundVerbList(contract)}.`
          : `${prioritizeId} still needs verification work before packet review; do not print or mail.`
    : 'No prospects available to prioritize.';

  const readinessTable = buildCanaryReadinessMarkdownTable(enriched, contract);

  const perProspectActions = enriched.map((row) => {
    const id = row.prospect_id || 'unknown';
    if (row.awaiting_outbound_approval) {
      return `- ${id}: Call-script review complete — waiting on future explicit dial/call approval. Do not dial, call, text, email, launch, or execute.`;
    }
    if (row.primary_ready_for_review) {
      return contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? `- ${id}: Create a preparation-only call-script review checklist and complete operator call-script review. Do not dial, call, text, email, launch, or execute.`
        : `- ${id}: Create a preparation-only packet review checklist and complete operator packet-content review. Do not print, mail, launch, or execute.`;
    }
    return contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? `- ${id}: ${row.operator_next_action} — phone/contact-role verification only; not ready for dialing/calling.`
      : `- ${id}: ${row.operator_next_action} — verification work only; not ready for printing/mailing.`;
  });

  const safeToDraft = draftAllowed.length
    ? draftAllowed
        .map(
          (r) =>
            `${r.prospect_id} (${r.company_name || 'unknown'} / ${r.contact_name || 'unknown'})`
        )
        .join('; ')
    : `None — ${draftField} is not allowed for any listed prospect.`;

  const reviewReadyIds = reviewReady.map((r) => r.prospect_id).join(', ');
  const packetReviewedNowLine =
    reviewReady.length === 1
      ? contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? `${reviewReadyIds} may be call-script-reviewed now, but is still blocked from dial/call until explicit approval.`
        : `${reviewReadyIds} may be packet-reviewed now, but is still blocked from print/mail until explicit approval.`
      : reviewReady.length
        ? contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
          ? `${reviewReadyIds} may be call-script-reviewed now, but are still blocked from dial/call until explicit approval.`
          : `${reviewReadyIds} may be packet-reviewed now, but are still blocked from print/mail until explicit approval.`
        : awaitingIds.length && contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
          ? `${awaitingLabel} call-script review is complete and waiting on future explicit dial/call approval.`
          : `No prospect is ready_for_review for ${contract.reviewWorkOrderLabel} yet.`;

  const blockedFromPrintMail = [
    'All prospects: execution_readiness remains blocked.',
    contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? contract.remainsBlockedOutboundLine ||
        "Outbound remains blocked until the prospect's call readiness is ready_for_review, readiness remains current, and the operator gives explicit future dial/call approval."
      : `Printing and mailing stay blocked until ${primaryField} is ready_for_review AND the operator later gives explicit launch/mail approval.`,
    packetReviewedNowLine,
    awaitingIds.length && contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? `${awaitingLabel}: waiting on future explicit dial/call approval (not a dial work order).`
      : null,
    needsVerification.length
      ? `${needsVerification.map((r) => r.prospect_id).join(', ')} remain blocked pending ${contract.verificationDeferredLabel}.`
      : null,
  ]
    .filter(Boolean)
    .join(' ');

  const trackNext = [
    contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? `${primaryField} / ${draftField} / execution_readiness per prospect`
      : 'mail_readiness / draft_readiness / execution_readiness per prospect',
    contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? 'verification gate statuses (phone, contact role)'
      : 'verification gate statuses (website, mailing address, phone, contact role)',
    contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? `${contract.reviewArtifactName} review completion for ready_for_review prospects`
      : 'packet review completion for ready_for_review prospects',
    contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? 'explicit future dial/call approval (never implied by this summary)'
      : 'explicit future launch/mail approval (never implied by this summary)',
  ]
    .map((item) => `- ${item}`)
    .join('\n');

  const awaitingStatusClause =
    awaitingIds.length && contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? `; ${awaitingIds.length} waiting on future dial/call approval`
      : '';
  const overallStatus = reviewReady.length
    ? contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
      ? `Campaign ${campaignId} ${contract.statusLabel}: ${reviewReady.length} prospect(s) ready_for_review for ${contract.reviewWorkOrderLabel}; ${needsVerification.length} still need ${contract.verificationDeferredLabel}${awaitingStatusClause}; ${contract.overallSafetyClause}.`
      : `Campaign ${campaignId} preparation-only canary: ${reviewReady.length} prospect(s) ready_for_review for packet review; ${needsVerification.length} still need verification; nothing launched, approved, printed, or mailed.`
    : needsVerification.length
      ? contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? `Campaign ${campaignId} ${contract.statusLabel}: ${needsVerification.length} prospect(s) still need ${contract.verificationDeferredLabel} before ${contract.reviewWorkOrderLabel}${awaitingStatusClause}; ${contract.overallSafetyClause}.`
        : `Campaign ${campaignId} preparation-only canary: all listed prospects still need verification before packet review; nothing launched, approved, printed, or mailed.`
      : contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? awaitingIds.length
          ? `Campaign ${campaignId} ${contract.statusLabel}: no open preparation work orders; ${awaitingIds.length} waiting on future dial/call approval; ${contract.overallSafetyClause}.`
          : `Campaign ${campaignId} ${contract.statusLabel}: all listed prospects still need ${contract.verificationDeferredLabel} before ${contract.reviewWorkOrderLabel}; ${contract.overallSafetyClause}.`
        : `Campaign ${campaignId} preparation-only canary: all listed prospects still need verification before packet review; nothing launched, approved, printed, or mailed.`;

  const answer = [
    overallStatus,
    '',
    'Readiness table:',
    readinessTable.header,
    readinessTable.sep,
    readinessTable.body,
    '',
    'Work next:',
    prioritizeWhy,
    '',
    'Exact next operator action per prospect:',
    ...perProspectActions,
    '',
    'Safe to draft now:',
    draftAllowed.length
      ? contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? `${contract.draftAllowedPhrase.charAt(0).toUpperCase()}${contract.draftAllowedPhrase.slice(1)} is allowed for: ${safeToDraft}. Drafts stay preparation-only — not authorization to ${formatBlockedOutboundVerbList(contract)}.`
        : `Provisional drafting is allowed for: ${safeToDraft}. Drafts stay preparation-only — not authorization to print or mail.`
      : safeToDraft,
    '',
    `Blocked from ${contract.blockedFromLabel}:`,
    blockedFromPrintMail,
    '',
    'What PulseForge should track next:',
    trackNext,
    '',
    '--- Final operator decision required ---',
    prioritize && prioritize.primary_ready_for_review
      ? contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? `Decide whether to run preparation-only ${contract.reviewWorkOrderLabel} for ${prioritizeId} next. Explicit future outbound approval is still required. I will not ${formatBlockedOutboundVerbList(contract)} from this summary.`
        : `Decide whether to run preparation-only packet review for ${prioritizeId} next. Explicit future launch/mail approval is still required before any print or mail. I will not print, mail, launch, or execute from this summary.`
      : contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
        ? `Complete ${contract.verificationDeferredLabel} for blocked prospects before any ${contract.reviewWorkOrderLabel} or outbound action. I will not ${formatBlockedOutboundVerbList(contract)} from this summary.`
        : 'Complete verification for blocked prospects before any packet review or outbound action. I will not print, mail, launch, or execute from this summary.',
    '',
    safety,
  ].join('\n');

  const reasoning = [
    fromKnownState
      ? 'Built canary summary / judgment from known current-state bullets; not treated as a new prospect paste.'
      : fromReadinessTable
        ? 'Built canary summary / judgment from pasted readiness summary table; not treated as a new prospect paste.'
        : 'Reused activeWorkContext.tableRows for canary summary / judgment; no prospect re-paste required.',
    prioritize && prioritize.primary_ready_for_review
      ? `${prioritizeId} prioritized for final ${contract.reviewWorkOrderLabel} decision because ${primaryField} is ready_for_review and execution remains blocked.`
      : `No ${primaryField}=ready_for_review prospect — verification work is the priority.`,
    `Drafting may be allowed where ${draftField}=allowed; outbound actions remain blocked.`,
    'No mission create/resume. activeWorkContext not required to mutate for known-state path.',
  ];

  return buildStructuredResponse({
    answer,
    reasoning: debugOutput ? reasoning : [],
    supportingEvidence: debugOutput
      ? enriched.map((row) => ({
          id: `canary-prospect:${row.prospect_id}`,
          summary: `${row.company_name || row.prospect_id}: ${primaryField} ${row[primaryField]}, ${draftField} ${row[draftField]}, execution blocked`,
          sourceType: 'operator',
          confidence: null,
        }))
      : [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: debugOutput
      ? [
          prioritize && prioritize.primary_ready_for_review
            ? contract.suggestionReviewChip(prioritizeId)
            : 'Paste updated verification state or continue verification work for blocked prospects.',
        ]
      : [],
    recommendedActions: debugOutput
      ? [
          'Review the readiness judgment, then explicitly request review or verification work — never launch from this summary.',
        ]
      : [],
    metadata: {
      sourcesUsed: fromKnownState
        ? { knownCurrentState: true }
        : fromReadinessTable
          ? { readinessSummaryTable: true }
          : { activeWorkContext: true },
      evidenceCount: debugOutput ? enriched.length : 0,
      unavailable: debugOutput ? [] : [],
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      canaryWorkflowType: contract.type,
      canarySummary: true,
      knownCurrentState: fromKnownState || undefined,
      readinessSummaryTable: fromReadinessTable || undefined,
      activeWorkContextReused: !fromPastedState,
      tableUpdate: false,
      campaignId,
      prospectCount: enriched.length,
      prioritizedProspectId: prioritizeId,
      mailReadiness:
        contract.type === CANARY_WORKFLOW_TYPES.DIRECT_MAIL
          ? (prioritize && prioritize.mail_readiness) || null
          : undefined,
      callReadiness:
        contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
          ? (prioritize && prioritize.call_readiness) || null
          : undefined,
      primaryReadiness: (prioritize && prioritize[primaryField]) || null,
      executionReadiness: 'blocked',
      outputKind: 'canary_summary',
      lastOutputKind: LAST_OUTPUT_TYPES.CANARY_SUMMARY,
      outputSubtype: 'summary',
      contextHints: {
        workflow: CAMPAIGN_001_PREPARATION_ONLY_CANARY,
        canaryWorkflowType: contract.type,
        lastOutputKind: LAST_OUTPUT_TYPES.CANARY_SUMMARY,
        lastOutputType: LAST_OUTPUT_TYPES.CANARY_SUMMARY,
        outputKind: 'canary_summary',
        preparationOnly: true,
        canarySummary: true,
        campaignId,
        prospectId: prioritizeId,
        mailReadiness:
          contract.type === CANARY_WORKFLOW_TYPES.DIRECT_MAIL
            ? (prioritize && prioritize.mail_readiness) || null
            : undefined,
        callReadiness:
          contract.type === CANARY_WORKFLOW_TYPES.CALL_PREP
            ? (prioritize && prioritize.call_readiness) || null
            : undefined,
        executionReadiness: 'blocked',
        knownCurrentState: fromKnownState || undefined,
        readinessSummaryTable: fromReadinessTable || undefined,
      },
      strictOutputShape: !debugOutput,
    },
  });
}

/**
 * @param {{ question: string }} input
 */
function buildMissingPacketReviewResponse(input) {
  return buildStructuredResponse({
    answer: [
      'I can build a preparation-only packet review checklist, but I don’t have an active Campaign canary table in this session.',
      'Continue from a session with the fillable verification table, paste the table, or provide the known facts for one prospect (prospect_id, company_name, contact_name, mail_readiness, execution_readiness).',
      'Preparation-only: no mission created; no launch, approval, print, or mail.',
    ].join(' '),
    reasoning: [
      'Operator asked for a packet review artifact, but activeWorkContext has no fillable table and no sufficient inline known facts.',
      'Clarifying instead of falling through to prospect-parse fallback or mission routing.',
      'No mission create/resume.',
    ],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: [
      'Paste the Campaign 001 fillable verification table, or list known facts for one prospect_id, then ask for a packet review checklist.',
    ],
    recommendedActions: [],
    metadata: {
      sourcesUsed: {},
      evidenceCount: 0,
      unavailable: ['active_work_context_fillable_table'],
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      packetReview: true,
      missingActiveWorkContext: true,
    },
  });
}

/**
 * Preparation-only packet review from an inline known-facts block when the
 * session has no active canary table and no pasted markdown table.
 * Builds a temporary packetReviewContext only — does not mutate activeWorkContext.
 * @param {{ question: string }} input
 * @returns {{ reason: string, structured: object }|null}
 */
function handleInlineKnownFactsPacketReview(input = {}) {
  const question = String(input.question || '');
  const parsed = parseInlinePacketReviewKnownFacts(question);
  if (!parsed || !parsed.hasInlineFacts) return null;

  if (parsed.missingRequired.length > 0) {
    return {
      reason: 'inline_known_facts_packet_review_incomplete',
      structured: buildMissingInlineKnownFactsPacketReviewResponse({
        question,
        missingRequired: parsed.missingRequired,
        assignedFields: parsed.assignedFields,
      }),
    };
  }

  const row = { ...(parsed.row || {}) };
  // Safety: packet review never authorizes execution from inline facts.
  row.execution_readiness = 'blocked';

  const structured = buildPacketReviewArtifactResponse({
    row,
    entity: null,
    question,
    campaignId: extractCampaignIdFromText(question) || '001',
    source: 'inline_known_facts',
  });

  return {
    reason: 'inline_known_facts_packet_review',
    structured,
  };
}

/**
 * Preparation-only call-script review from an inline known-facts block when the
 * session has no active canary table. Temporary context only — does not mutate
 * activeWorkContext.
 * @param {{ question: string }} input
 * @returns {{ reason: string, structured: object }|null}
 */
function handleInlineKnownFactsCallScriptReview(input = {}) {
  const question = String(input.question || '');
  const parsed = parseInlineCallScriptReviewKnownFacts(question);
  if (!parsed || !parsed.hasInlineFacts) return null;

  if (parsed.missingRequired.length > 0) {
    return {
      reason: 'inline_known_facts_call_script_review_incomplete',
      structured: buildMissingInlineKnownFactsCallScriptReviewResponse({
        question,
        missingRequired: parsed.missingRequired,
        assignedFields: parsed.assignedFields,
      }),
    };
  }

  const row = { ...(parsed.row || {}) };
  row.execution_readiness = 'blocked';
  if (!row.work_order) row.work_order = 'call-script review';

  const structured = buildCallScriptReviewArtifactResponse({
    row,
    entity: null,
    question,
    campaignId: extractCampaignIdFromText(question) || '001',
    source: 'inline_known_facts',
  });

  return {
    reason: 'inline_known_facts_call_script_review',
    structured,
  };
}

/**
 * Preparation-only call-script review decision record from inline known facts.
 * Does not require a canary table. Never authorizes dial/call.
 * @param {{ question: string }} input
 * @returns {{ reason: string, structured: object }|null}
 */
function handleInlineKnownFactsCallScriptDecisionRecord(input = {}) {
  const question = String(input.question || '');
  const parsed = parseInlineCallScriptReviewKnownFacts(question);
  if (!parsed || !parsed.hasInlineFacts) return null;

  if (parsed.missingRequired.length > 0) {
    return {
      reason: 'inline_known_facts_call_script_decision_record_incomplete',
      structured: buildMissingInlineKnownFactsCallScriptDecisionRecordResponse({
        question,
        missingRequired: parsed.missingRequired,
        assignedFields: parsed.assignedFields,
      }),
    };
  }

  const row = { ...(parsed.row || {}) };
  row.execution_readiness = 'blocked';
  if (!row.work_order) row.work_order = 'call-script review decision record';

  const structured = buildCallScriptDecisionRecordResponse({
    row,
    question,
    campaignId: extractCampaignIdFromText(question) || '001',
    source: 'inline_known_facts',
  });

  return {
    reason: 'inline_known_facts_call_script_decision_record',
    structured,
  };
}

/**
 * Ask for required known facts only — never the full canary table.
 * @param {{ question?: string }} input
 */
function buildMissingCallScriptDecisionRecordResponse(input = {}) {
  void input;
  return buildStructuredResponse({
    answer: [
      'I can record a preparation-only call-script review decision once inline known facts are available.',
      'Provide known facts for one CP prospect (prospect_id, company_name, contact_name, call_readiness, script_readiness, execution_readiness).',
      'You do not need the full Campaign canary table.',
      'Preparation-only. No mission created. No launch, execution, approval, dial, call, text, or email.',
    ].join(' '),
    reasoning: [
      'Call-script decision-record requested without sufficient inline known facts or desk table.',
      'Asking for required fields only — not reconstructing the full table.',
      'No mission create/resume.',
    ],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: [
      'Provide inline known facts for CP-001 and ask again to record the call-script review decision.',
    ],
    recommendedActions: [],
    metadata: {
      sourcesUsed: {},
      evidenceCount: 0,
      unavailable: ['inline_known_facts'],
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      callScriptDecisionRecord: true,
      missingInlineKnownFacts: true,
    },
  });
}

/**
 * @param {{ question?: string, missingRequired: string[], assignedFields?: string[] }} input
 */
function buildMissingInlineKnownFactsCallScriptDecisionRecordResponse(
  input = {}
) {
  const missing = (Array.isArray(input.missingRequired)
    ? input.missingRequired
    : []
  ).filter(Boolean);
  const provided = (Array.isArray(input.assignedFields)
    ? input.assignedFields
    : []
  ).filter(Boolean);
  const missingLabel = missing.length ? missing.join(', ') : 'required fields';
  const providedLabel = provided.length
    ? `Received: ${provided.join(', ')}.`
    : '';

  return buildStructuredResponse({
    answer: [
      'I can record a preparation-only call-script review decision from the known facts you provided.',
      `Still need: ${missingLabel}.`,
      providedLabel,
      'You do not need to paste the full Campaign canary table — only the missing fields for this prospect.',
      'Preparation-only. No mission created. No launch, execution, approval, dial, call, text, or email.',
    ]
      .filter(Boolean)
      .join(' '),
    reasoning: [
      'Operator supplied inline known facts for a call-script decision record, but required fields are incomplete.',
      `Missing required fields: ${missingLabel}.`,
      'Asking only for missing fields — not reconstructing the full table.',
      'No mission create/resume. activeWorkContext not mutated.',
    ],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: [
      `Provide the missing fields (${missingLabel}) and ask again to record the call-script review decision.`,
    ],
    recommendedActions: [],
    metadata: {
      sourcesUsed: { inlineKnownFacts: true },
      evidenceCount: provided.length,
      unavailable: missing.map((f) => `inline_known_fact:${f}`),
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      callScriptDecisionRecord: true,
      inlineKnownFacts: true,
      missingRequiredFields: missing,
      tableUpdate: false,
    },
  });
}

/**
 * Resolve call_script_content_decision for a decision-record turn.
 * Pending/empty becomes approved_for_future_call_approval_review.
 * Never converts script approval into dial/call approval.
 * @param {unknown} value
 * @returns {string}
 */
function resolveCallScriptContentDecision(value) {
  const raw = String(value == null ? '' : value).trim();
  if (
    !raw ||
    /^\(pending\)$/i.test(raw) ||
    /^pending$/i.test(raw) ||
    /^unknown$/i.test(raw)
  ) {
    return 'approved_for_future_call_approval_review';
  }
  // Never treat dial/call approval language as a content decision.
  if (/^(?:approved_for_dial|dial|call|outbound)_?approved$/i.test(raw)) {
    return 'approved_for_future_call_approval_review';
  }
  return raw;
}

/**
 * Resolve call_script_review_status after recording a decision.
 * @param {unknown} value
 * @returns {string}
 */
function resolveCallScriptReviewStatus(value) {
  const raw = String(value == null ? '' : value).trim();
  if (/^content_reviewed$/i.test(raw)) return 'content_reviewed';
  if (/^reviewed$/i.test(raw)) return 'reviewed';
  return 'reviewed';
}

/**
 * Script readiness after a content decision — may stay allowed or become
 * reviewed; never implies dial approval.
 * @param {unknown} value
 * @returns {string}
 */
function resolveScriptReadinessAfterDecision(value) {
  const raw = String(value == null ? '' : value).trim();
  if (/^reviewed$/i.test(raw) || /^content_reviewed$/i.test(raw)) {
    return 'reviewed';
  }
  if (/^allowed$/i.test(raw)) return 'allowed';
  return raw || 'allowed';
}

/**
 * Preparation-only call-script decision record from inline known facts.
 * Script content approval is not dial/call approval.
 * @param {{ row: object, question?: string, campaignId?: string, source?: string }} input
 */
function buildCallScriptDecisionRecordResponse(input = {}) {
  const rawRow = input.row && typeof input.row === 'object' ? input.row : {};
  const row = normalizeReadinessSummaryRow({ ...rawRow });
  const campaignId = String(input.campaignId || '001');
  const fromInlineFacts = input.source === 'inline_known_facts';
  const question = String(input.question || '');
  const prospectId = String(row.prospect_id || 'unknown').trim();
  const company = blankTableValue(row.company_name) || 'unknown';
  const contact = blankTableValue(row.contact_name) || 'unknown';
  const phoneStatus = String(row.phone_status || 'unknown');
  const contactRoleStatus = String(row.contact_role_status || 'unknown');
  const phone = verifiedPacketFieldValue(phoneStatus, row.phone_value);
  const priorCallReadiness = String(
    rawRow.call_readiness || row.call_readiness || 'ready_for_review'
  );
  const priorScriptReadiness = String(
    rawRow.script_readiness || row.script_readiness || 'allowed'
  );
  const priorContentDecision = String(
    rawRow.call_script_content_decision || '(pending)'
  ).trim();
  const priorReviewStatus = String(
    rawRow.call_script_review_status || 'in_review'
  ).trim();
  const priorApprovedForDial = String(
    rawRow.approved_for_dial != null ? rawRow.approved_for_dial : 'false'
  ).trim();
  const priorDialApproval = String(
    rawRow.dial_call_approval_status || 'pending'
  ).trim();
  const notes = String(row.notes || '').trim();

  // Safety: this path records script content review only — never dial approval.
  const callReadiness = priorCallReadiness || 'ready_for_review';
  const scriptReadiness = resolveScriptReadinessAfterDecision(
    priorScriptReadiness
  );
  const executionReadiness = 'blocked';
  const contentDecision = resolveCallScriptContentDecision(
    priorContentDecision
  );
  const reviewStatus = resolveCallScriptReviewStatus(priorReviewStatus);
  const approvedForDial = 'false';
  const dialCallApprovalStatus = 'pending';

  const suppressScaffolding = wantsPacketReviewArtifactSuppression(question);
  const debugOutput =
    !suppressScaffolding && wantsPacketReviewDebugOutput(question);

  const whatChanged = [
    `- call_script_review_status: ${priorReviewStatus || 'in_review'} → ${reviewStatus}`,
    `- call_script_content_decision: ${priorContentDecision || '(pending)'} → ${contentDecision}`,
  ];
  if (
    String(priorScriptReadiness).toLowerCase() !==
    String(scriptReadiness).toLowerCase()
  ) {
    whatChanged.push(
      `- script_readiness: ${priorScriptReadiness} → ${scriptReadiness}`
    );
  }

  const answer = [
    `Preparation-only call-script review decision record — Campaign ${campaignId} / ${prospectId}`,
    '',
    'Decision record:',
    `- prospect_id: ${prospectId}`,
    `- company: ${company}`,
    `- contact: ${contact}`,
    `- call_script_review_status: ${reviewStatus}`,
    `- call_script_reviewed_by: (operator)`,
    `- call_script_reviewed_at: (recorded this turn)`,
    `- call_script_content_decision: ${contentDecision}`,
    `- approved_for_dial: ${approvedForDial}`,
    `- dial_call_approval_status: ${dialCallApprovalStatus}`,
    `- execution_readiness: ${executionReadiness}`,
    `- call_readiness: ${callReadiness}`,
    `- script_readiness: ${scriptReadiness}`,
    phone
      ? `- phone_value: ${phone} (review only — do not dial)`
      : `- phone_status: ${phoneStatus}`,
    `- contact_role_status: ${contactRoleStatus}`,
    notes ? `- notes: ${notes}` : null,
    '',
    'Updated readiness state:',
    `- call_readiness: ${callReadiness}`,
    `- script_readiness: ${scriptReadiness}`,
    `- execution_readiness: ${executionReadiness}`,
    `- approved_for_dial: ${approvedForDial}`,
    `- dial_call_approval_status: ${dialCallApprovalStatus}`,
    '',
    'What changed:',
    ...whatChanged,
    '',
    'What did not change:',
    `- approved_for_dial remains ${approvedForDial} (was ${priorApprovedForDial || 'false'})`,
    `- dial_call_approval_status remains ${dialCallApprovalStatus} (was ${priorDialApproval || 'pending'})`,
    `- execution_readiness remains ${executionReadiness}`,
    `- call_readiness remains ${callReadiness}`,
    '- Call-script content approval did not become dial/call approval',
    '',
    'What remains blocked:',
    '- Dial / call / text / email / launch / execute / approve outbound',
    '- Any outbound action without a separate explicit future dial/call approval',
    '- Invented industry, pain points, sources, portfolio size, or evidence',
    '',
    'Next recommended work order:',
    `Await separate explicit dial/call approval for ${prospectId} before any outbound action, or continue preparation-only work on other call-prep canary prospects. Do not dial, call, text, or email from this decision record.`,
    '',
    ...buildFutureCallApprovalGateLines(),
    '',
    'Preparation-only. No mission created. No launch, execution, approval, dial, call, text, or email.',
  ]
    .filter((line) => line != null)
    .join('\n');

  return buildStructuredResponse({
    answer,
    reasoning: debugOutput
      ? [
          'Operator asked to record a preparation-only call-script review decision.',
          `Prospect ${prospectId} — built from inline known facts; no full canary table required.`,
          'Call-script content approval is not dial/call approval; execution_readiness remains blocked.',
          'No mission create/resume.',
        ]
      : [],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: debugOutput
      ? [
          'Do not dial or call until a separate explicit approval step.',
          'Continue preparation-only work on remaining call-prep canary prospects if needed.',
        ]
      : [],
    recommendedActions: [],
    metadata: {
      sourcesUsed: fromInlineFacts
        ? { inlineKnownFacts: true }
        : { activeWorkContext: true },
      evidenceCount: debugOutput ? 1 : 0,
      unavailable: [],
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      callScriptDecisionRecord: true,
      callScriptReview: false,
      focusedWorkOrder: undefined,
      canarySummary: undefined,
      executionReadiness: 'blocked',
      callReadiness,
      scriptReadiness,
      approvedForDial: false,
      dialCallApprovalStatus: 'pending',
      callScriptContentDecision: contentDecision,
      callScriptReviewStatus: reviewStatus,
      prospectId,
      campaignId,
      inlineKnownFacts: fromInlineFacts || undefined,
      tableUpdate: false,
      lastOutputKind: LAST_OUTPUT_TYPES.CALL_SCRIPT_DECISION_RECORD,
      lastOutputType: LAST_OUTPUT_TYPES.CALL_SCRIPT_DECISION_RECORD,
      outputKind: LAST_OUTPUT_TYPES.CALL_SCRIPT_DECISION_RECORD,
      contextHints: {
        workflow: CAMPAIGN_001_PREPARATION_ONLY_CANARY,
        lastOutputKind: LAST_OUTPUT_TYPES.CALL_SCRIPT_DECISION_RECORD,
        lastOutputType: LAST_OUTPUT_TYPES.CALL_SCRIPT_DECISION_RECORD,
        outputKind: LAST_OUTPUT_TYPES.CALL_SCRIPT_DECISION_RECORD,
        preparationOnly: true,
        callScriptDecisionRecord: true,
        prospectId,
        campaignId,
        callReadiness,
        scriptReadiness,
        executionReadiness: 'blocked',
        approvedForDial: false,
        dialCallApprovalStatus: 'pending',
        inlineKnownFacts: fromInlineFacts || undefined,
      },
      strictOutputShape: !debugOutput,
    },
  });
}

/**
 * @param {{ question?: string, missingRequired: string[], assignedFields?: string[] }} input
 */
function buildMissingInlineKnownFactsCallScriptReviewResponse(input = {}) {
  const missing = (Array.isArray(input.missingRequired)
    ? input.missingRequired
    : []
  ).filter(Boolean);
  const provided = (Array.isArray(input.assignedFields)
    ? input.assignedFields
    : []
  ).filter(Boolean);
  const missingLabel = missing.length ? missing.join(', ') : 'required fields';
  const providedLabel = provided.length
    ? `Received: ${provided.join(', ')}.`
    : '';

  return buildStructuredResponse({
    answer: [
      'I can build a preparation-only call-script review from the known facts you provided.',
      `Still need: ${missingLabel}.`,
      providedLabel,
      'You do not need to paste the full Campaign canary table — only the missing fields for this prospect.',
      'Preparation-only. No mission created. No launch, execution, approval, dial, call, text, or email.',
    ]
      .filter(Boolean)
      .join(' '),
    reasoning: [
      'Operator supplied an inline known-facts block for call-script review, but required fields are incomplete.',
      `Missing required fields: ${missingLabel}.`,
      'Asking only for missing fields — not reconstructing the full table.',
      'No mission create/resume. activeWorkContext not mutated.',
    ],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: [
      `Provide the missing fields (${missingLabel}) and ask again for the preparation-only call-script review.`,
    ],
    recommendedActions: [],
    metadata: {
      sourcesUsed: { inlineKnownFacts: true },
      evidenceCount: provided.length,
      unavailable: missing.map((f) => `inline_known_fact:${f}`),
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      callScriptReview: true,
      inlineKnownFacts: true,
      missingRequiredFields: missing,
      tableUpdate: false,
    },
  });
}

/**
 * Ask for desk table / known facts when call-script review has no context.
 * @param {{ question?: string }} input
 */
function buildMissingCallScriptReviewResponse(input = {}) {
  void input;
  return buildStructuredResponse({
    answer: [
      'I can build a preparation-only call-script review checklist once a call-prep canary table or inline known facts are available.',
      'Provide the current desk table, or include known facts for one CP prospect (prospect_id, company_name, contact_name, call_readiness, script_readiness, execution_readiness).',
      'Preparation-only. No mission created. No launch, execution, approval, dial, call, text, or email.',
    ].join(' '),
    reasoning: [
      'Call-script review requested without an active fillable table or sufficient inline known facts.',
      'No mission create/resume.',
    ],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: [
      'Paste the call-prep canary table, or provide inline known facts for CP-001 call-script review.',
    ],
    recommendedActions: [],
    metadata: {
      sourcesUsed: {},
      evidenceCount: 0,
      unavailable: ['active_work_context_fillable_table'],
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      callScriptReview: true,
      missingActiveWorkContext: true,
    },
  });
}

/**
 * Build a preparation-only call-script review artifact from the active canary table.
 * Does not mutate tableRows. Never creates a mission or dials.
 * @param {{ question: string, session: object, prior: object }} input
 */
function handleCallScriptReviewContinuation(input) {
  const question = String(input.question || '');
  const session = input.session;
  const prior = input.prior;
  const knownIds = knownActiveWorkProspectIds(prior);
  const knownLabel = knownIds.length ? knownIds.join(', ') : '(none on desk)';
  const requestedId = extractCallScriptReviewProspectId(question, knownIds);

  if (!requestedId) {
    return {
      reason: 'active_work_context_call_script_review_needs_target',
      structured: buildStructuredResponse({
        answer: [
          'I can build a preparation-only call-script review checklist from the current canary table.',
          `Which prospect_id should I use? Known ids on the desk: ${knownLabel}.`,
          'Example: Create a preparation-only call-script review checklist for CP-001.',
          'Preparation-only. No mission created. No launch, execution, approval, dial, call, text, or email.',
        ].join(' '),
        reasoning: [
          'Operator asked for a call-script review artifact without a target prospect_id.',
          'Clarifying instead of inventing a target or requiring a prospect re-paste.',
          'No mission create/resume.',
        ],
        supportingEvidence: [],
        contradictingEvidence: [],
        confidence: null,
        nextInvestigations: [
          `Create a preparation-only call-script review checklist for a known prospect_id (${knownLabel}).`,
        ],
        recommendedActions: [],
        metadata: {
          sourcesUsed: { activeWorkContext: true },
          evidenceCount: Array.isArray(prior.tableRows) ? prior.tableRows.length : 0,
          unavailable: ['call_script_review_target'],
          surface: 'workspace',
          executionDomain: EXECUTION_DOMAINS.WORKSPACE,
          route: 'intelligence',
          canaryPreparationOnly: true,
          callScriptReview: true,
          activeWorkContextReused: true,
          tableUpdate: false,
        },
      }),
    };
  }

  const rows = Array.isArray(prior.tableRows) ? prior.tableRows : [];
  const row = rows.find(
    (r) =>
      String((r && r.prospect_id) || '').trim().toUpperCase() ===
      String(requestedId).trim().toUpperCase()
  );

  if (!row) {
    return {
      reason: 'active_work_context_call_script_review_unknown_prospect',
      structured: buildStructuredResponse({
        answer: [
          'I could not build that call-script review checklist.',
          `Unknown prospect_id: ${requestedId}.`,
          `Known prospect ids on the desk: ${knownLabel}.`,
          'Tell me which known id to use. I will not invent a row or ask you to re-paste the whole list.',
          'Preparation-only. No mission created. No launch, execution, approval, dial, call, text, or email.',
        ].join(' '),
        reasoning: [
          'Call-script review referenced a prospect_id not present in activeWorkContext.tableRows.',
          'Clarifying instead of inventing rows or falling through to prospect-parse fallback.',
          'No mission create/resume.',
        ],
        supportingEvidence: [],
        contradictingEvidence: [],
        confidence: null,
        nextInvestigations: [
          `Create a call-script review checklist for a known prospect_id (${knownLabel}).`,
        ],
        recommendedActions: [],
        metadata: {
          sourcesUsed: { activeWorkContext: true },
          evidenceCount: rows.length,
          unavailable: [`prospect:${requestedId}`],
          surface: 'workspace',
          executionDomain: EXECUTION_DOMAINS.WORKSPACE,
          route: 'intelligence',
          canaryPreparationOnly: true,
          callScriptReview: true,
          activeWorkContextReused: true,
          tableUpdate: false,
          requestedProspectId: requestedId,
        },
      }),
    };
  }

  const entity = (Array.isArray(prior.entities) ? prior.entities : []).find(
    (e) =>
      String((e && e.id) || '').trim().toUpperCase() ===
      String(requestedId).trim().toUpperCase()
  );

  const structured = buildCallScriptReviewArtifactResponse({
    row,
    entity,
    question,
    campaignId:
      (prior.target && prior.target.campaignId) ||
      extractCampaignIdFromText(question) ||
      '001',
  });

  rememberCanaryActiveWorkContext({
    session,
    prospects: entitiesToProspects(prior.entities),
    question,
    lastOutputType: LAST_OUTPUT_TYPES.CALL_SCRIPT_REVIEW,
    prior,
  });

  return {
    reason: 'active_work_context_call_script_review',
    structured,
  };
}

/**
 * True when the operator / row names this turn as call-script review.
 * @param {object} row
 * @param {string} question
 * @returns {boolean}
 */
function isCallScriptReviewWorkOrder(row, question) {
  const workOrder = String((row && row.work_order) || '').toLowerCase();
  if (/\bcall[-\s]*script\s+review\b/.test(workOrder)) return true;
  if (isProceedWithCallScriptReviewRequest(question)) return true;
  const q = String(question || '').toLowerCase();
  if (/\bcall[-\s]*script\s+review\b/.test(q) && /\bCP-\d{3}\b/i.test(question)) {
    return true;
  }
  return false;
}

/**
 * Exact call-script approval checklist — review only, not dial approval.
 * @returns {string[]}
 */
function buildCallScriptApprovalChecklistLines() {
  return [
    'Call-script approval checklist:',
    '- provisional call opener reviewed',
    '- discovery questions reviewed',
    '- objection-handling notes reviewed',
    '- voicemail draft reviewed',
    '- operator caveats accepted',
    '- known-facts caveats accepted (no invented industry, pain points, sources, or portfolio size)',
    '- call-script review checklist complete',
    '- review decision recorded',
    '- dial/call approval still pending',
  ];
}

/**
 * Future dial/call gate — call-script review is not call approval.
 * @returns {string[]}
 */
function buildFutureCallApprovalGateLines() {
  return [
    'Final approval gate before outbound action:',
    'No outbound action can happen until the operator explicitly approves dial/call in a future step. Call-script review is not call approval. Execution readiness remains blocked.',
  ];
}

/**
 * Call-script review artifact from a single desk / inline row — known facts only.
 * ready_for_review means the script can be reviewed, not dialed.
 * @param {{ row: object, entity?: object|null, question?: string, campaignId?: string, source?: string }} input
 */
function buildCallScriptReviewArtifactResponse(input = {}) {
  const rawRow = input.row && typeof input.row === 'object' ? input.row : {};
  const row = normalizeReadinessSummaryRow({ ...rawRow });
  const entity = input.entity && typeof input.entity === 'object' ? input.entity : {};
  const campaignId = String(input.campaignId || '001');
  const fromInlineFacts = input.source === 'inline_known_facts';
  const question = String(input.question || '');
  const prospectId = String(row.prospect_id || entity.id || 'unknown').trim();
  const company =
    blankTableValue(row.company_name) || blankToNull(entity.companyName) || null;
  const contact =
    blankTableValue(row.contact_name) || blankToNull(entity.contactName);
  const phoneStatus = String(row.phone_status || 'unknown');
  const contactRoleStatus = String(row.contact_role_status || 'unknown');
  const phone = verifiedPacketFieldValue(phoneStatus, row.phone_value);
  const callReadiness = String(
    rawRow.call_readiness || row.call_readiness || 'blocked'
  );
  const scriptReadiness = String(
    rawRow.script_readiness || row.script_readiness || 'blocked'
  );
  const executionReadiness = 'blocked';
  const notes = String(row.notes || '').trim();
  const callReadyForReview = /^ready(?:_for_review)?$/i.test(callReadiness);
  const scriptAllowed = /^allowed$/i.test(scriptReadiness);
  const callScriptWorkOrder = isCallScriptReviewWorkOrder(row, question);

  const canDraft = Boolean(company && contact);
  const contactFirstName = contact
    ? String(contact).trim().split(/\s+/)[0] || contact
    : null;
  const draftLabel =
    canDraft && callReadyForReview && scriptAllowed
      ? 'provisional'
      : canDraft
        ? 'provisional / held pending readiness'
        : 'held';

  // Customer-facing scripts stay generic and natural — no preparation-only /
  // execution-blocked / review-only / gate language. Safety stays in caveats.
  const opener = canDraft
    ? `Hi ${contactFirstName}, this is [operator] with PulseForge. I'm calling about ${company}. Do you have a minute for a quick question?`
    : '(opener held — company_name and contact_name are required)';

  const discoveryQuestions = canDraft
    ? [
        `1. Is ${contact} still the right person to speak with about day-to-day operations at ${company}?`,
        '2. What does a typical week look like for your team right now? (use only their words — do not invent pain points)',
        '3. Are you currently evaluating any tools or process changes, or is this a discovery-only conversation?',
        '4. What would make a follow-up conversation worth your time?',
      ]
    : ['(discovery questions held — company_name and contact_name are required)'];

  const objectionNotes = [
    '- If not interested: thank them, confirm no further outreach this turn, leave the door open for operator-initiated follow-up only.',
    '- If wrong person: ask who owns the relevant work; do not invent a role or contact.',
    '- If asks for proof/portfolio: stay within verified desk facts only; do not invent sources, portfolio size, or evidence.',
    '- If asks to be called back: capture preferred time in tracking fields; do not dial from this checklist.',
  ];

  const voicemail = canDraft
    ? `Hi ${contactFirstName}, this is [operator] with PulseForge calling about ${company}. When convenient, you can reach me at [operator number].`
    : '(voicemail held — company_name and contact_name are required)';

  const phoneReviewField = formatPacketReviewConfirmField(
    'phone (review field only — do not dial)',
    phoneStatus,
    phone,
    blankTableValue(row.phone_value)
  );

  const suppressScaffolding = wantsPacketReviewArtifactSuppression(question);
  const debugOutput =
    !suppressScaffolding && wantsPacketReviewDebugOutput(question);

  const answer = [
    `Preparation-only call-script review — Campaign ${campaignId} / ${prospectId}`,
    '',
    `Company: ${company || 'unknown'}`,
    `Contact: ${contact || 'unknown'}`,
    `Call readiness: ${callReadiness}${
      callReadyForReview
        ? ' (script may be reviewed — not authorization to dial/call)'
        : ' (not ready to dial or call)'
    }`,
    `Script readiness: ${scriptReadiness}`,
    `Execution readiness: ${executionReadiness} (remains blocked)`,
    `Contact role status: ${contactRoleStatus}`,
    notes ? `Notes (from known facts): ${notes}` : null,
    callScriptWorkOrder ? 'Work order: call-script review' : null,
    '',
    `${prospectId} call-script review checklist:`,
    '- Confirm company and contact from known facts only',
    '- Review provisional call opener',
    '- Review discovery questions',
    '- Review objection-handling notes',
    '- Review voicemail draft',
    '- Accept operator caveats',
    '- Record call-script review decision',
    '- Do not dial, call, text, or email from this checklist',
    '',
    `Provisional call opener (${draftLabel}):`,
    opener,
    '',
    'Discovery questions:',
    ...discoveryQuestions,
    '',
    'Objection-handling notes:',
    ...objectionNotes,
    '',
    `Voicemail draft (${draftLabel}):`,
    voicemail,
    '',
    '--- Operator caveats ---',
    '- Preparation-only. Call-script review is not call approval.',
    '- Use verified phone as a review field only; do not call it from this turn.',
    '- Do not invent industry, pain points, sources, portfolio size, or evidence.',
    '- Execution readiness remains blocked until a separate explicit dial/call approval.',
    '',
    'Fields to confirm before any future dial/call:',
    phoneReviewField,
    `contact role (${contactRoleStatus})`,
    `call_readiness (${callReadiness})`,
    `script_readiness (${scriptReadiness})`,
    '',
    ...buildCallScriptApprovalChecklistLines(),
    '',
    ...buildFutureCallApprovalGateLines(),
    '',
    'PulseForge call-review tracking fields:',
    `- prospect_id: ${prospectId}`,
    `- company: ${company || 'unknown'}`,
    `- contact: ${contact || 'unknown'}`,
    '- call_script_review_status: in_review',
    '- call_script_reviewed_by: (operator)',
    '- call_script_reviewed_at: (set when reviewed)',
    '- call_script_content_decision: (pending)',
    '- approved_for_dial: false',
    '- dial_call_approval_status: pending',
    `- execution_readiness: ${executionReadiness}`,
    `- call_readiness_at_review: ${callReadiness}`,
    `- script_readiness_at_review: ${scriptReadiness}`,
    phone
      ? `- phone_value_at_review: ${phone} (review only — do not dial)`
      : `- phone_status_at_review: ${phoneStatus}`,
    '',
    'What remains blocked:',
    '- Dial / call / text / email / launch / execute / approve',
    '- Any outbound action without a separate explicit future dial/call approval',
    '- Invented industry, pain points, sources, portfolio size, or evidence',
    '',
    '--- Final operator decision required ---',
    'Complete call-script review only. Call-script review is not call approval. Explicit future dial/call approval is still required before any outbound action. I will not dial, call, text, email, launch, or execute from this checklist.',
    '',
    'Preparation-only. No mission created. No launch, execution, approval, dial, call, text, or email.',
  ]
    .filter((line) => line != null)
    .join('\n');

  return buildStructuredResponse({
    answer,
    reasoning: debugOutput
      ? [
          'Operator proceeded with a preparation-only call-script review work order.',
          `Prospect ${prospectId} — known facts only; no invented evidence.`,
          'Call-script review is not call approval; execution_readiness remains blocked.',
          fromInlineFacts
            ? 'Built from inline known facts (temporary context only).'
            : 'Built from active canary desk row.',
          'No mission create/resume.',
        ]
      : [],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: debugOutput
      ? [
          'Record the call-script review decision on the desk.',
          'Do not dial or call until a separate explicit approval step.',
        ]
      : [],
    recommendedActions: [],
    metadata: {
      sourcesUsed: fromInlineFacts
        ? { inlineKnownFacts: true }
        : { activeWorkContext: true },
      evidenceCount: debugOutput ? 1 : 0,
      unavailable: [],
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      callScriptReview: true,
      callScriptContentReview: true,
      focusedWorkOrder: undefined,
      canarySummary: undefined,
      executionReadiness: 'blocked',
      callReadiness,
      scriptReadiness,
      prospectId,
      campaignId,
      inlineKnownFacts: fromInlineFacts || undefined,
      tableUpdate: false,
      lastOutputKind: LAST_OUTPUT_TYPES.CALL_SCRIPT_REVIEW,
      lastOutputType: LAST_OUTPUT_TYPES.CALL_SCRIPT_REVIEW,
      outputKind: LAST_OUTPUT_TYPES.CALL_SCRIPT_REVIEW,
      contextHints: {
        workflow: CAMPAIGN_001_PREPARATION_ONLY_CANARY,
        lastOutputKind: LAST_OUTPUT_TYPES.CALL_SCRIPT_REVIEW,
        lastOutputType: LAST_OUTPUT_TYPES.CALL_SCRIPT_REVIEW,
        outputKind: LAST_OUTPUT_TYPES.CALL_SCRIPT_REVIEW,
        preparationOnly: true,
        callScriptReview: true,
        callScriptContentReview: true,
        prospectId,
        campaignId,
        callReadiness,
        scriptReadiness,
        executionReadiness: 'blocked',
        inlineKnownFacts: fromInlineFacts || undefined,
      },
      // Artifact mode: suppress Reasoning / Unavailable / Next unless debug.
      strictOutputShape: !debugOutput,
    },
  });
}

/**
 * Ask only for missing required known-fact fields — never the full table.
 * @param {{ question?: string, missingRequired: string[], assignedFields?: string[] }} input
 */
function buildMissingInlineKnownFactsPacketReviewResponse(input = {}) {
  const missing = (Array.isArray(input.missingRequired)
    ? input.missingRequired
    : []
  ).filter(Boolean);
  const provided = (Array.isArray(input.assignedFields)
    ? input.assignedFields
    : []
  ).filter(Boolean);
  const missingLabel = missing.length ? missing.join(', ') : 'required fields';
  const providedLabel = provided.length
    ? `Received: ${provided.join(', ')}.`
    : '';

  return buildStructuredResponse({
    answer: [
      'I can build a preparation-only packet review from the known facts you provided.',
      `Still need: ${missingLabel}.`,
      providedLabel,
      'You do not need to paste the full Campaign canary table — only the missing fields for this prospect.',
      'Preparation-only. No mission created. No launch, execution, approval, print, or mail.',
    ]
      .filter(Boolean)
      .join(' '),
    reasoning: [
      'Operator supplied an inline known-facts block for packet review, but required fields are incomplete.',
      `Missing required fields: ${missingLabel}.`,
      'Asking only for missing fields — not reconstructing the full table.',
      'No mission create/resume. activeWorkContext not mutated.',
    ],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: [
      `Provide the missing fields (${missingLabel}) and ask again for the preparation-only packet review.`,
    ],
    recommendedActions: [],
    metadata: {
      sourcesUsed: { inlineKnownFacts: true },
      evidenceCount: provided.length,
      unavailable: missing.map((f) => `inline_known_fact:${f}`),
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      packetReview: true,
      inlineKnownFacts: true,
      missingRequiredFields: missing,
      tableUpdate: false,
    },
  });
}

/**
 * Format a confirm-before-printing field without inventing "verified: unknown"
 * when a status exists but no actual value was supplied.
 * @param {string} label
 * @param {string} status
 * @param {string|null} verifiedValue
 * @param {string|null} rawValue
 * @returns {string}
 */
function formatPacketReviewConfirmField(label, status, verifiedValue, rawValue) {
  const statusText = String(status || '').trim();
  if (/^verified$/i.test(statusText)) {
    if (verifiedValue) return `${label} (verified: ${verifiedValue})`;
    // Gate verified, value absent — never say "verified: unknown" / "(unknown)".
    return `${label} (verified; value not included in this prompt)`;
  }
  const displayStatus = statusText || 'unknown';
  if (rawValue) {
    return `${label} (${displayStatus}: ${rawValue} — not verified — do not treat as confirmed)`;
  }
  // Status-only: never append ": unknown" for a missing value.
  return `${label} (${displayStatus})`;
}

/**
 * Packet-content review confirm lines: gate status separate from value presence.
 * @param {{
 *   websiteStatus: string,
 *   mailingStatus: string,
 *   phoneStatus: string,
 *   contactRoleStatus: string,
 *   websiteValue?: string|null,
 *   mailingValue?: string|null,
 *   phoneValue?: string|null,
 * }} input
 * @returns {string[]}
 */
function buildPacketContentReviewConfirmLines(input = {}) {
  const fields = [
    {
      label: 'Website',
      status: input.websiteStatus,
      value: input.websiteValue || null,
    },
    {
      label: 'Mailing address',
      status: input.mailingStatus,
      value: input.mailingValue || null,
    },
    {
      label: 'Phone',
      status: input.phoneStatus,
      value: input.phoneValue || null,
    },
    {
      label: 'Contact role',
      status: input.contactRoleStatus,
      value: null,
    },
  ];

  /** @type {string[]} */
  const lines = ['Confirm before any future print step:'];
  let anyVerifiedMissingValue = false;
  let allCoreVerified = true;

  for (const field of fields) {
    const statusText = String(field.status || '').trim();
    const verified = /^verified$/i.test(statusText);
    if (!verified) allCoreVerified = false;
    if (verified && !field.value) anyVerifiedMissingValue = true;

    if (verified && field.value) {
      lines.push(`- ${field.label} gate: verified; value: ${field.value}`);
    } else if (verified) {
      lines.push(
        `- ${field.label} gate: verified; value not included in this prompt`
      );
    } else {
      const display = statusText || 'unknown';
      lines.push(
        field.value
          ? `- ${field.label} gate: ${display}; value: ${field.value}`
          : `- ${field.label} gate: ${display}`
      );
    }
  }

  if (allCoreVerified && anyVerifiedMissingValue) {
    lines.push(
      'Gate statuses are verified, but the specific website/address/phone values were not included in this prompt. Confirm the values are present in the source system and packet metadata before any future print step.'
    );
  } else if (anyVerifiedMissingValue || allCoreVerified) {
    lines.push(
      '- Confirm actual values are present in the source system / packet metadata before future print/mail approval.'
    );
  } else {
    lines.push(
      '- Confirm fields against a trusted source before any future print step.'
    );
  }
  lines.push(
    '- Print / sign / mail only after a separate explicit launch/mail approval (not this turn).'
  );
  return lines;
}

/**
 * True when the operator / row names this turn as packet-content review.
 * @param {object} row
 * @param {string} question
 * @returns {boolean}
 */
function isPacketContentReviewWorkOrder(row, question) {
  const workOrder = String((row && row.work_order) || '').toLowerCase();
  if (/\bpacket[-\s]*content\s+review\b/.test(workOrder)) return true;
  if (isProceedWithPacketContentReviewRequest(question)) return true;
  const q = String(question || '').toLowerCase();
  if (/\bpacket[-\s]*content\s+review\b/.test(q) && /\bPM-\d{3}\b/i.test(question)) {
    return true;
  }
  return false;
}

/**
 * Packet-content approval checklist for ready_for_review reviews.
 * @returns {string[]}
 */
function buildPacketContentApprovalChecklistLines() {
  return [
    'Packet-content approval checklist:',
    '- letter draft reviewed',
    '- handwritten note reviewed',
    '- scorecard cover reviewed',
    '- customer-facing copy contains no unsupported claims',
    '- known-facts caveats accepted',
    '- packet contents checklist complete',
    '- review decision recorded',
    '- launch/mail approval still pending',
  ];
}

/**
 * Future launch/mail gate — packet-content approval is not mail approval.
 * @returns {string[]}
 */
function buildFutureMailApprovalGateLines() {
  return [
    'Future mail approval gate:',
    'No outbound action can happen until the operator explicitly approves launch/mail in a future step. Packet-content approval is not mail approval. Execution readiness remains blocked.',
  ];
}

/**
 * Packet review artifact from a single desk table row — known facts only.
 * ready_for_review means packet can be reviewed, not mailed.
 * @param {{ row: object, entity?: object|null, question?: string, campaignId?: string, source?: string }} input
 */
function buildPacketReviewArtifactResponse(input = {}) {
  const rawRow = input.row && typeof input.row === 'object' ? input.row : {};
  // Apply verification_summary → gate statuses when individual statuses are
  // blank/unknown. Never invent field values; never downgrade explicit statuses.
  const row = normalizeReadinessSummaryRow({ ...rawRow });
  const entity = input.entity && typeof input.entity === 'object' ? input.entity : {};
  const campaignId = String(input.campaignId || '001');
  const fromInlineFacts = input.source === 'inline_known_facts';
  const question = String(input.question || '');
  const prospectId = String(row.prospect_id || entity.id || 'unknown').trim();
  const company =
    blankTableValue(row.company_name) || blankToNull(entity.companyName) || null;
  const contact =
    blankTableValue(row.contact_name) || blankToNull(entity.contactName);
  const industry =
    blankToNull(entity.industry) ||
    blankToNull(entity.vertical) ||
    blankToNull(row.industry) ||
    blankToNull(row.vertical) ||
    null;
  const websiteStatus = String(row.website_status || 'unknown');
  const mailingStatus = String(row.mailing_address_status || 'unknown');
  const phoneStatus = String(row.phone_status || 'unknown');
  const contactRoleStatus = String(row.contact_role_status || 'unknown');
  const website = verifiedPacketFieldValue(websiteStatus, row.website_value);
  const mailing = verifiedPacketFieldValue(
    mailingStatus,
    row.mailing_address_value
  );
  const phone = verifiedPacketFieldValue(phoneStatus, row.phone_value);
  // Prefer operator-supplied mail_readiness; normalizeReadinessSummaryRow may
  // default blanks but must not downgrade ready_for_review.
  const mailReadiness = String(
    rawRow.mail_readiness || row.mail_readiness || 'blocked'
  );
  const draftReadiness = String(
    rawRow.draft_readiness || row.draft_readiness || 'blocked'
  );
  const executionReadiness = 'blocked';
  const notes = String(row.notes || '').trim();
  const operatorNext = String(row.operator_next_action || '').trim();

  const mailReadyForReview = /^ready(?:_for_review)?$/i.test(mailReadiness);
  const packetContentWorkOrder = isPacketContentReviewWorkOrder(row, question);
  // Explicit packet-content review work order + ready_for_review centers the
  // artifact on content approval (not verification / post-mail tracking).
  const packetContentReviewMode =
    mailReadyForReview && packetContentWorkOrder;

  const personalizationGaps = [];
  if (!company) personalizationGaps.push('company name');
  if (!contact) personalizationGaps.push('contact name');
  if (!industry) personalizationGaps.push('industry');

  const canDraft = Boolean(company && contact);
  const draftsHeld = !canDraft;
  const draftsProvisional = canDraft && personalizationGaps.length > 0;
  const draftConfidence = !canDraft
    ? 'blocked'
    : !industry
      ? 'low'
      : 'medium';

  const drafts = canDraft
    ? buildPacketReviewDrafts({
        companyName: company,
        contactName: contact,
        industry,
        mailingAddress: mailing,
        website,
        phone,
        notes,
        mailReadyForReview,
        contactRoleStatus,
      })
    : {
        letter:
          '(draft held — company_name and contact_name are required before drafting)',
        handwrittenNote:
          '(draft held — company_name and contact_name are required before drafting)',
        scorecardCover:
          '(draft held — company_name and contact_name are required before drafting)',
        followUpNotes:
          'Confirm company and decision-maker contact before drafting a packet or placing a follow-up call. Stay within known table facts only.',
        preMailVerificationPlan:
          'Verify company and decision-maker contact from a trusted source or CRM before any print/mail step. Do not invent mailing address, website, or phone.',
      };

  const whyBlockedForMailing = mailReadyForReview
    ? []
    : buildWhyBlockedForMailingLines({
        mailingStatus,
        websiteStatus,
        phoneStatus,
        contactRoleStatus,
        mailing,
        website,
        phone,
        row,
      });

  // ready_for_review means verification is complete enough for packet review —
  // do not reopen a pre-mail verification plan solely because raw values are
  // absent from this turn's known facts.
  const needsPreMailVerification = !mailReadyForReview;
  const draftLabel = draftsHeld
    ? 'held'
    : mailReadyForReview
      ? draftsProvisional
        ? 'provisional / low evidence'
        : 'provisional'
      : draftsProvisional
        ? 'provisional / operator-only — do not print/mail until fields are verified'
        : 'provisional / operator-only — do not print/mail until fields are verified';

  const confirmBeforePrinting = [
    formatPacketReviewConfirmField(
      'mailing address',
      mailingStatus,
      mailing,
      blankTableValue(row.mailing_address_value)
    ),
    formatPacketReviewConfirmField(
      'website',
      websiteStatus,
      website,
      blankTableValue(row.website_value)
    ),
    formatPacketReviewConfirmField(
      'phone',
      phoneStatus,
      phone,
      blankTableValue(row.phone_value)
    ),
    `contact role (${contactRoleStatus})`,
    !industry
      ? 'industry / persona context (missing from table — needed for stronger personalization; drafts remain provisional)'
      : null,
  ]
    .filter(Boolean)
    .join('; ');

  const suppressScaffolding = wantsPacketReviewArtifactSuppression(question);
  const debugOutput =
    !suppressScaffolding && wantsPacketReviewDebugOutput(question);

  const printMailChecklist = packetContentReviewMode
    ? buildPacketContentReviewConfirmLines({
        websiteStatus,
        mailingStatus,
        phoneStatus,
        contactRoleStatus,
        websiteValue: website || blankTableValue(row.website_value),
        mailingValue: mailing || blankTableValue(row.mailing_address_value),
        phoneValue: phone || blankTableValue(row.phone_value),
      })
    : mailReadyForReview
      ? [
          'Print / sign / mail checklist:',
          '- Confirm fields below against a trusted source',
          '- Print packet only after operator sign-off',
          '- Sign letter / note only after operator review',
          '- Mail only after explicit future launch approval (not this turn)',
          '',
          'Fields to confirm before printing:',
          confirmBeforePrinting,
        ]
      : [
          'Print / sign / mail checklist: not available until verification is complete.',
          'Future print / sign / mail checklist (after verification):',
          '- Confirm fields below against a trusted source',
          '- Print packet only after operator sign-off',
          '- Sign letter / note only after operator review',
          '- Mail only after explicit future launch approval (not this turn)',
          '',
          'Fields that must be verified before any future print/mail step:',
          confirmBeforePrinting,
        ];

  const operatorActionSection = packetContentReviewMode
    ? [
        ...buildPacketContentApprovalChecklistLines(),
        '',
        ...buildFutureMailApprovalGateLines(),
      ]
    : needsPreMailVerification
      ? [
          'Pre-mail verification plan (operator only):',
          drafts.preMailVerificationPlan ||
            buildPreMailVerificationPlan({
              contactName: contact,
              mailing,
              website,
              phone,
              mailingStatus,
              websiteStatus,
              phoneStatus,
              contactRoleStatus,
              industry,
            }),
        ]
      : [
          'First follow-up call notes (operator only):',
          drafts.followUpNotes,
        ];

  const trackingFieldsSection = packetContentReviewMode
    ? [
        'PulseForge packet-review tracking fields:',
        `- prospect_id: ${prospectId}`,
        `- company: ${company || 'unknown'}`,
        `- contact: ${contact || 'unknown'}`,
        '- packet_review_status: in_review',
        '- packet_reviewed_by: (operator)',
        '- packet_reviewed_at: (set when reviewed)',
        '- packet_content_decision: (pending)',
        '- approved_for_print: false',
        '- launch_approval_status: pending',
        `- execution_readiness: ${executionReadiness}`,
        `- mail_readiness_at_review: ${mailReadiness}`,
      ]
    : [
        mailReadyForReview
          ? 'PulseForge tracking fields to log after mailing:'
          : 'PulseForge tracking fields (current state + future mail log):',
        `- prospect_id: ${prospectId}`,
        `- company: ${company || 'unknown'}`,
        `- contact: ${contact || 'unknown'}`,
        `- industry: ${industry || 'not provided'}`,
        `- current_mail_readiness: ${mailReadiness}`,
        `- mail_date: (set when mailed)`,
        '- packet_contents: letter / handwritten note / scorecard / business card',
        '- mail_readiness_at_send: (set when mailed)',
        needsPreMailVerification
          ? '- first_follow_up_call_outcome: (set after call — only after a verified phone exists and packet is mailed)'
          : '- first_follow_up_call_outcome: (set after call)',
      ];

  const finalDecisionSection = packetContentReviewMode
    ? [
        '--- Final operator decision required ---',
        'Complete packet-content review only. Packet-content approval is not mail approval. Explicit future launch/mail approval is still required before any print or mail. I will not print, mail, launch, or execute from this checklist.',
      ]
    : [
        '--- Final operator decision required ---',
        mailReadyForReview
          ? 'Packet is ready_for_review only. Explicitly approve a future launch/mail step after readiness remains complete — I will not print, mail, launch, or execute from this checklist.'
          : 'Mail readiness is blocked. Complete pre-mail verification first; then request an explicit launch/mail approval. I will not print, mail, launch, or execute from this checklist.',
      ];

  const answer = [
    packetContentReviewMode
      ? `Preparation-only packet-content review — Campaign ${campaignId} / ${prospectId}`
      : `Preparation-only packet review — Campaign ${campaignId} / ${prospectId}`,
    '',
    `Company: ${company || 'unknown'}`,
    `Contact: ${contact || 'unknown'}`,
    industry
      ? `Industry: ${industry}`
      : 'Industry/persona evidence: not provided.',
    `Mail readiness: ${mailReadiness}${
      mailReadyForReview
        ? ' (packet may be reviewed — not authorization to mail)'
        : ' (not ready to print or mail)'
    }`,
    `Draft readiness: ${draftReadiness}`,
    `Execution readiness: ${executionReadiness} (remains blocked)`,
    `Draft confidence: ${draftConfidence}`,
    notes ? `Notes (from table): ${notes}` : null,
    operatorNext ? `Operator next action (from table): ${operatorNext}` : null,
    packetContentWorkOrder ? 'Work order: packet-content review' : null,
    '',
    whyBlockedForMailing.length
      ? ['Why blocked for mailing:', ...whyBlockedForMailing, ''].join('\n')
      : null,
    'Packet contents checklist:',
    '- Personalized letter',
    '- Handwritten note',
    '- Scorecard cover',
    '- Business card (operator-supplied; not invented here)',
    '',
    ...printMailChecklist,
    '',
    '--- Customer-facing drafts ---',
    mailReadyForReview
      ? null
      : 'Operator-only provisional drafts — do not print/mail until mailing fields are verified.',
    `Personalized letter draft (${draftLabel}):`,
    drafts.letter,
    '',
    `Handwritten note draft (${draftLabel}):`,
    drafts.handwrittenNote,
    '',
    `Scorecard cover text draft (${draftLabel}):`,
    drafts.scorecardCover,
    '',
    '--- Operator caveats ---',
    `Draft confidence: ${draftConfidence}${
      draftConfidence === 'low'
        ? mailReadyForReview
          ? ' — industry/persona evidence missing; drafts stay generic and provisional'
          : ' — industry/persona evidence missing; drafts are operator-only provisional and must not be printed/mailed yet'
        : draftConfidence === 'medium'
          ? mailReadyForReview
            ? ' — industry known; still preparation-only / provisional'
            : ' — industry known; still preparation-only — do not print/mail while mail readiness is blocked'
          : ' — company and contact required before drafting'
    }`,
    '',
    'Missing personalization evidence:',
    personalizationGaps.length
      ? `Missing: ${personalizationGaps.join(', ')}. Listed for confirmation — does not block provisional drafting when company and contact are present. No invented portfolio size, pain points, persona, or industry.`
      : 'No personalization gaps on company, contact, or industry. Drafts still use only verified table contact fields and known desk facts.',
    '',
    ...operatorActionSection,
    '',
    ...trackingFieldsSection,
    '',
    ...finalDecisionSection,
    '',
    'Preparation-only. No mission created. No launch, execution, approval, print, or mail.',
  ]
    .filter((line) => line != null)
    .join('\n');

  const packetReasoning = [
    fromInlineFacts
      ? 'Built packet review from inline known facts; no active canary table or markdown table paste required.'
      : 'Reused activeWorkContext.tableRows for packet review; no prospect re-paste required.',
    fromInlineFacts
      ? `Selected prospect_id ${prospectId} from operator-supplied known facts for Campaign ${campaignId}.`
      : `Selected prospect_id ${prospectId} from the current Campaign ${campaignId} canary table.`,
    packetContentReviewMode
      ? 'Packet-content review mode: approval checklist + future mail gate; verification not reopened.'
      : 'ready_for_review means packet review is allowed; execution_readiness stays blocked absent explicit launch.',
    canDraft
      ? 'Provisional drafts generated from known company/contact facts; missing industry does not block drafting.'
      : 'Drafts held because company_name and contact_name are both required.',
    fromInlineFacts
      ? 'No mission create/resume. activeWorkContext not mutated (temporary packetReviewContext only).'
      : 'No mission create/resume. Table not mutated.',
  ];

  return buildStructuredResponse({
    answer,
    reasoning: debugOutput ? packetReasoning : [],
    supportingEvidence: debugOutput
      ? [
          {
            id: `canary-prospect:${prospectId}`,
            summary: `${company || prospectId}: packet review — mail ${mailReadiness}, execution blocked`,
            sourceType: 'operator',
            confidence: null,
          },
        ]
      : [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: debugOutput
      ? canaryWorkflowSuggestions({
          lastOutputType: LAST_OUTPUT_TYPES.PACKET_REVIEW,
          prospects: [
            {
              id: prospectId,
              companyName: company,
              contactName: contact,
              industry,
              website,
              mailingAddress: mailing,
              phone,
            },
          ],
          tableRows: [row],
          question,
        })
      : [],
    recommendedActions: debugOutput
      ? [
          packetContentReviewMode
            ? 'Complete the packet-content approval checklist, then explicitly request any later launch/mail approval.'
            : 'Review the provisional packet drafts, confirm print fields, then explicitly request any later launch/mail approval.',
        ]
      : [],
    metadata: {
      sourcesUsed: fromInlineFacts
        ? { inlineKnownFacts: true }
        : { activeWorkContext: true },
      evidenceCount: debugOutput ? 1 : 0,
      unavailable: debugOutput
        ? personalizationGaps.map((g) => `personalization:${g}`)
        : [],
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      packetReview: true,
      packetContentReview: packetContentReviewMode || undefined,
      fillableTable: !fromInlineFacts,
      inlineKnownFacts: fromInlineFacts || undefined,
      provisionalDrafts: draftsProvisional || draftsHeld || undefined,
      draftConfidence,
      activeWorkContextReused: !fromInlineFacts,
      tableUpdate: false,
      prospectId,
      campaignId,
      mailReadiness,
      executionReadiness,
      // Response-level chip routing without requiring persisted activeWorkContext
      // (inline known-facts path keeps a temporary packetReviewContext only).
      outputKind: 'packet_review_artifact',
      lastOutputKind: LAST_OUTPUT_TYPES.PACKET_REVIEW,
      contextHints: {
        workflow: CAMPAIGN_001_PREPARATION_ONLY_CANARY,
        lastOutputKind: LAST_OUTPUT_TYPES.PACKET_REVIEW,
        lastOutputType: LAST_OUTPUT_TYPES.PACKET_REVIEW,
        outputKind: 'packet_review_artifact',
        preparationOnly: true,
        packetReview: true,
        packetContentReview: packetContentReviewMode || undefined,
        prospectId,
        campaignId,
        mailReadiness,
        executionReadiness,
        inlineKnownFacts: fromInlineFacts || undefined,
      },
      // Artifact mode: suppress Reasoning / Unavailable / Next unless debug.
      strictOutputShape: !debugOutput,
    },
  });
}

/**
 * Operator asked to suppress packet-review debug scaffolding
 * (Reasoning / Unavailable / Next) — artifact mode only.
 * @param {string} text
 * @returns {boolean}
 */
function wantsPacketReviewArtifactSuppression(text) {
  const lower = String(text || '').toLowerCase();
  if (!lower.trim()) return false;

  const noReasoning =
    /\bdo\s+not\s+include\s+reasoning\b/.test(lower) ||
    /\bdon'?t\s+include\s+reasoning\b/.test(lower) ||
    /\bwithout\s+reasoning\b/.test(lower) ||
    /\bno\s+reasoning\b/.test(lower);

  const noUnavailable =
    /\bdo\s+not\s+include\s+unavailable\b/.test(lower) ||
    /\bdon'?t\s+include\s+unavailable\b/.test(lower) ||
    /\bwithout\s+unavailable\b/.test(lower) ||
    /\bno\s+unavailable\b/.test(lower);

  const noNext =
    /\bdo\s+not\s+include\s+next\b/.test(lower) ||
    /\bdon'?t\s+include\s+next\b/.test(lower) ||
    /\bwithout\s+next(?:\s+sections?)?\b/.test(lower) ||
    /\bno\s+next(?:\s+sections?|\s+steps?|\s+investigations?)?\b/.test(lower);

  const returnOnly =
    /\breturn\s+only\b/.test(lower) ||
    /\bartifact\s+only\b/.test(lower) ||
    /\bonly\s+(?:the\s+)?(?:packet\s+review\s+)?artifact\b/.test(lower);

  return noReasoning || noUnavailable || noNext || returnOnly;
}

/**
 * Operator asked for packet-review debug sections (Reasoning / Unavailable / Next).
 * Negated phrasing ("do not include reasoning") is not treated as a debug ask.
 * @param {string} text
 * @returns {boolean}
 */
function wantsPacketReviewDebugOutput(text) {
  const lower = String(text || '').toLowerCase();
  if (!lower.trim()) return false;
  if (wantsPacketReviewArtifactSuppression(lower)) return false;

  return (
    /\bdebug\s+mode\b/.test(lower) ||
    /\bwith\s+debug\b/.test(lower) ||
    /(?<!\bdo\s+not\s)(?<!\bdon'?t\s)(?<!\bwithout\s)\binclude\s+reasoning\b/.test(
      lower
    ) ||
    /(?<!\bdo\s+not\s)(?<!\bdon'?t\s)(?<!\bwithout\s)\bshow\s+reasoning\b/.test(
      lower
    ) ||
    /(?<!\bdo\s+not\s)(?<!\bdon'?t\s)(?<!\bwithout\s)\binclude\s+unavailable\b/.test(
      lower
    ) ||
    /(?<!\bdo\s+not\s)(?<!\bdon'?t\s)(?<!\bwithout\s)\binclude\s+next\s+(?:steps?|investigations?|sections?)\b/.test(
      lower
    )
  );
}

/**
 * Verified table field value only — unverified values are not treated as facts for drafts.
 * @param {unknown} status
 * @param {unknown} value
 * @returns {string|null}
 */
function verifiedPacketFieldValue(status, value) {
  if (!/^verified$/i.test(String(status || '').trim())) return null;
  return blankTableValue(value);
}

/**
 * @param {unknown} status
 * @returns {boolean}
 */
function isPacketFieldUnresolved(status) {
  const s = String(status || '')
    .trim()
    .toLowerCase();
  if (!s) return true;
  if (/^verified$/i.test(s)) return false;
  return /^(unknown|blocked)$/i.test(s) || /needs\s*verification/i.test(s);
}

/**
 * Explicit blockers for a preparation-only packet when mail_readiness is blocked.
 * @param {object} input
 * @returns {string[]}
 */
function buildWhyBlockedForMailingLines(input = {}) {
  const lines = [];
  const mailingStatus = String(input.mailingStatus || 'unknown');
  const websiteStatus = String(input.websiteStatus || 'unknown');
  const phoneStatus = String(input.phoneStatus || 'unknown');
  const contactRoleStatus = String(input.contactRoleStatus || 'unknown');
  const row = input.row && typeof input.row === 'object' ? input.row : {};

  if (!input.mailing || isPacketFieldUnresolved(mailingStatus)) {
    const raw = blankTableValue(row.mailing_address_value);
    const detail =
      /^blocked$/i.test(mailingStatus) && (!raw || /^unknown$/i.test(raw))
        ? 'unknown/blocked'
        : /^blocked$/i.test(mailingStatus)
          ? `blocked${raw ? ` (${raw})` : ''}`
          : !raw || /^unknown$/i.test(String(raw))
            ? 'unknown/blocked'
            : `${mailingStatus}${raw ? ` (${raw})` : ''}`;
    lines.push(`- mailing address ${detail}`);
  }
  if (!input.website || isPacketFieldUnresolved(websiteStatus)) {
    const raw = blankTableValue(row.website_value);
    const detail =
      /^blocked$/i.test(websiteStatus) && (!raw || /^unknown$/i.test(raw))
        ? 'unknown/blocked'
        : /^blocked$/i.test(websiteStatus)
          ? `blocked${raw ? ` (${raw})` : ''}`
          : !raw || /^unknown$/i.test(String(raw))
            ? 'unknown/blocked'
            : `${websiteStatus}${raw ? ` (${raw})` : ''}`;
    lines.push(`- website ${detail}`);
  }
  if (!input.phone || isPacketFieldUnresolved(phoneStatus)) {
    const raw = blankTableValue(row.phone_value);
    const detail =
      /^blocked$/i.test(phoneStatus) && (!raw || /^unknown$/i.test(raw))
        ? 'unknown/blocked'
        : /^blocked$/i.test(phoneStatus)
          ? `blocked${raw ? ` (${raw})` : ''}`
          : !raw || /^unknown$/i.test(String(raw))
            ? 'unknown/blocked'
            : `${phoneStatus}${raw ? ` (${raw})` : ''}`;
    lines.push(`- phone ${detail}`);
  }
  if (isPacketFieldUnresolved(contactRoleStatus)) {
    lines.push(
      `- contact role ${
        /needs\s*verification/i.test(contactRoleStatus)
          ? 'needs verification'
          : contactRoleStatus || 'unknown'
      }`
    );
  }
  return lines;
}

/**
 * Pre-mail verification plan when address/phone are not verified.
 * First action is trusted-source / CRM verification — not calling.
 * @param {object} input
 * @returns {string}
 */
function buildPreMailVerificationPlan(input = {}) {
  const firstName = input.contactName
    ? String(input.contactName).split(/\s+/)[0]
    : 'the contact';
  const steps = [];
  if (!input.mailing) {
    steps.push(
      'Verify mailing address from a trusted source or CRM before any print/mail step.'
    );
  }
  if (!input.website) {
    steps.push('Verify website from a trusted source or CRM.');
  }
  if (!input.phone) {
    steps.push(
      'Verify phone from a trusted source or CRM — do not call until a verified number exists.'
    );
  }
  if (isPacketFieldUnresolved(input.contactRoleStatus)) {
    steps.push(
      `Confirm ${firstName} is the right contact / decision-maker before outreach.`
    );
  } else if (input.contactName) {
    steps.push(`Confirm ${firstName} is the right contact.`);
  }
  steps.push(
    'Do not print, mail, or reference a mailed packet until verification is complete.'
  );
  if (!blankToNull(input.industry)) {
    steps.push('Do not claim industry-specific context.');
  }
  return steps.join(' ');
}

/**
 * Packet-review drafts from known desk facts only.
 * Customer-facing letter / note / scorecard stay sendable and omit operator
 * readiness, confidence, and evidence caveats (those live outside this copy).
 * Never invents pain/value claims (vendor coordination, operational gaps,
 * portfolio size, likely needs) unless those facts are present as evidence.
 * Generates conservative provisional copy when company + contact are present,
 * even if industry/persona evidence is missing (never invents industry).
 * @param {object} input
 */
function buildPacketReviewDrafts(input = {}) {
  const company = input.companyName;
  const contact = input.contactName;
  const firstName = String(contact).split(/\s+/)[0] || contact;
  const industry = blankToNull(input.industry);
  const industryLower = industry ? String(industry).toLowerCase() : null;
  const mailing = input.mailingAddress || null;
  const website = input.website || null;
  const phone = input.phone || null;
  const mailReadyForReview = input.mailReadyForReview === true;

  // Customer-facing letter: known facts only — no implied pain/value claims.
  const letterBody = industryLower
    ? [
        `I’m reaching out because ${company} appears to be in ${industryLower}.`,
        '',
        `I included a short scorecard packet for ${company} as a quick review item.`,
        '',
        'If useful, I’d be glad to walk through it after you’ve had a chance to review.',
      ]
    : [
        `I included a short scorecard packet for ${company} as a quick review item.`,
        '',
        'If useful, I’d be glad to walk through it after you’ve had a chance to review.',
      ];

  const letter = [`${firstName},`, '', ...letterBody, '', 'Best,', '[Sender]'].join(
    '\n'
  );

  // Customer-facing handwritten note: generic, sendable, no industry-unknown talk.
  const handwrittenNote = industryLower
    ? `${firstName} — included a short scorecard for ${company}. Thought it may be useful as a quick review for ${industryLower} teams. — [Sender]`
    : `${firstName} — included a short scorecard for ${company}. Thought it may be useful as a quick review item. — [Sender]`;

  // Customer-facing scorecard cover: known contact facts only — no operator status.
  const scorecardCover = [
    `Operational scorecard — ${company}`,
    `Prepared for: ${contact}`,
    industry ? `Context: ${industry}` : null,
    mailing ? `Mailing address: ${mailing}` : null,
    website ? `Website: ${website}` : null,
    phone ? `Phone: ${phone}` : null,
  ]
    .filter(Boolean)
    .join('\n');

  const preMailVerificationPlan = buildPreMailVerificationPlan({
    contactName: contact,
    mailing,
    website,
    phone,
    contactRoleStatus: input.contactRoleStatus,
    industry,
  });

  // Operator-only call notes — only when mail-ready with a verified reach path.
  const followUpNotes = [
    phone
      ? `Use verified phone ${phone}.`
      : 'No verified phone on file — do not invent a reach number.',
    `Confirm ${firstName} is the right contact.`,
    mailReadyForReview
      ? 'Reference the packet only after it is actually mailed.'
      : 'Do not reference a mailed packet — mail readiness is blocked.',
    industryLower ? null : 'Do not claim industry-specific context.',
    'Log outcome.',
  ]
    .filter(Boolean)
    .join(' ');

  return {
    letter,
    handwrittenNote,
    scorecardCover,
    followUpNotes,
    preMailVerificationPlan,
  };
}

function blankToNull(value) {
  if (value == null) return null;
  const s = String(value).trim();
  if (!s || /^unknown$/i.test(s) || /^n\/?a$/i.test(s)) return null;
  return s;
}

/**
 * Preparation-only canary: either ask for prospects, or return a review
 * package conversationally — never create/resume Campaign or Direct Mail Execution.
 * A parser miss must never fall through to campaign_creation /
 * mail_package_generation / direct_mail_execution.
 *
 * Also reuses session.context.activeWorkContext for follow-ups that transform
 * prior canary work (fillable table, continue, revise) without re-pasting.
 * @returns {Promise<{ structured: object, reason: string }|null>}
 */
async function maybeBuildCanaryPreparationResponse(input) {
  const question = String(input.question || '');
  const session = input.session || null;

  // Pasted fillable verification table / readiness summary table before
  // canary prospect sniffing / packet-review missing-table clarify / mission
  // routing.
  ingestPastedFillableVerificationTable({ question, session });
  ingestPastedReadinessSummaryTable({ question, session });

  const prior = getActiveWorkContext(session);

  // Table mutations are owned by early active-work continuation.
  if (
    activeContextHasEntities(prior) &&
    activeContextHasFillableTable(prior) &&
    isFillableTableUpdateRequest(question, prior)
  ) {
    return handleFillableTableUpdateContinuation({
      question,
      session,
      prior,
    });
  }

  // Proceed-with / packet review before canary summary — mirrors early
  // continuation so secondary canary routing never re-selects a work order.
  if (isCallScriptDecisionRecordRequest(question)) {
    const inlineResult = handleInlineKnownFactsCallScriptDecisionRecord({
      question,
    });
    if (inlineResult) return inlineResult;
    return {
      reason: 'canary_call_script_decision_record_missing_facts',
      structured: buildMissingCallScriptDecisionRecordResponse({ question }),
    };
  }

  if (isPacketReviewRequest(question)) {
    if (activeContextHasEntities(prior) && activeContextHasFillableTable(prior)) {
      return handlePacketReviewContinuation({
        question,
        session,
        prior,
      });
    }
    const inlineResult = handleInlineKnownFactsPacketReview({ question });
    if (inlineResult) return inlineResult;
    return {
      reason: 'canary_packet_review_missing_table',
      structured: buildMissingPacketReviewResponse({ question }),
    };
  }

  // Proceed-with / call-script review before canary summary.
  if (isCallScriptReviewRequest(question)) {
    const inlineResult = handleInlineKnownFactsCallScriptReview({ question });
    if (inlineResult) return inlineResult;
    if (activeContextHasEntities(prior) && activeContextHasFillableTable(prior)) {
      return handleCallScriptReviewContinuation({
        question,
        session,
        prior,
      });
    }
    return {
      reason: 'canary_call_script_review_missing_table',
      structured: buildMissingCallScriptReviewResponse({ question }),
    };
  }

  // Cross-prospect canary summary / judgment. Secondary hard stop before
  // prospect sniff / parse clarification.
  if (isCanarySummaryJudgmentRequest(question)) {
    return handleCanarySummaryJudgmentContinuation({
      question,
      session,
      prior,
    });
  }

  const isCanary = isPreparationOnlyCanary(question);
  const isFollowUp = isActiveWorkFollowUpCue(question);
  const isExec = isExplicitExecutionRequest(question);
  const hasPriorCanary =
    isCanaryDeskWorkflow(prior) && activeContextHasEntities(prior);
  const overrideWithNewProspects =
    hasPriorCanary &&
    (isExplicitContextOverride(question) ||
      operatorAttemptedCanaryProspectSupply(question)) &&
    !isFillableTableUpdateRequest(question, prior) &&
    !isPacketReviewRequest(question) &&
    !isCallScriptReviewRequest(question) &&
    !isCallScriptDecisionRecordRequest(question) &&
    !isCanarySummaryJudgmentRequest(question) &&
    !looksLikeFillableVerificationTablePaste(question) &&
    !looksLikeReadinessSummaryTablePaste(question);

  // Execution / mail while preparation-only canary context is active:
  // never infer launch from desk context — block and ask for readiness.
  if (
    !isCanary &&
    isExec &&
    activeContextBlocksExecution(prior) &&
    hasPriorCanary
  ) {
    return {
      reason: 'canary_active_context_execution_blocked',
      structured: buildCanaryExecutionBlockedResponse({
        activeWorkContext: prior,
        question,
        domainDecision: input.domainDecision,
      }),
    };
  }

  const shouldHandle =
    isCanary ||
    (hasPriorCanary && isFollowUp) ||
    (hasPriorCanary && isFillableTableRequest(question)) ||
    overrideWithNewProspects;

  if (!shouldHandle) return null;

  const detected = detectOperatorProspectListInMessage(question);
  const intendedCount = extractIntendedCanaryProspectCount(question);
  const completeProspects =
    detected.detected &&
    detected.prospectCount > 0 &&
    (!intendedCount || detected.prospectCount >= intendedCount);

  if (completeProspects) {
    const review = buildCanaryReviewPackageResponse({
      prospects: detected.prospects,
      objectiveText: detected.objectiveText || question,
      question,
      domainDecision: input.domainDecision,
      reusedFromActiveContext: false,
    });
    rememberCanaryActiveWorkContext({
      session,
      prospects: detected.prospects,
      question,
      lastOutputType: resolveCanaryLastOutputType(question, review.reason),
      prior,
      tableRows: review.tableRows,
    });
    return {
      reason: review.reason || 'canary_preparation_review_package',
      structured: review.structured || review,
    };
  }

  // Parser miss / incomplete paste — never create a Campaign mission.
  // Do not silently fall back to prior entities when the operator attempted a new paste.
  // Reuse cues ("same 3 prospects already listed") with desk entities are not a paste miss.
  if (
    operatorAttemptedCanaryProspectSupply(question) &&
    !isFillableTableUpdateRequest(question, prior) &&
    !isPacketReviewRequest(question) &&
    !isCanarySummaryJudgmentRequest(question) &&
    !looksLikeFillableVerificationTablePaste(question) &&
    !looksLikeReadinessSummaryTablePaste(question) &&
    !(hasPriorCanary && isActiveWorkReuseProspectCue(question))
  ) {
    const count = intendedCount || 3;
    return {
      reason: 'canary_prospect_parse_clarification',
      structured: buildCanaryParseFailureResponse({
        domainDecision: input.domainDecision,
        intendedCount: count,
      }),
    };
  }

  // Reuse desk context when follow-up/canary continue has no new paste.
  if (
    hasPriorCanary &&
    (isFollowUp ||
      isCanary ||
      isFillableTableRequest(question) ||
      isActiveWorkReuseProspectCue(question))
  ) {
    const prospects = entitiesToProspects(prior.entities);
    const review = buildCanaryReviewPackageResponse({
      prospects,
      objectiveText: question,
      question,
      domainDecision: input.domainDecision,
      reusedFromActiveContext: true,
      tableRows: prior.tableRows,
    });
    rememberCanaryActiveWorkContext({
      session,
      prospects,
      question,
      lastOutputType: resolveCanaryLastOutputType(question, review.reason),
      prior,
      tableRows:
        review.tableRows ||
        (review.reason === 'canary_fillable_table'
          ? buildFillableTableRows(prospects)
          : prior.tableRows),
    });
    return {
      reason: review.reason || 'canary_active_work_context_reuse',
      structured: review.structured || review,
    };
  }

  // No prospects supplied — ask for them. Never fall through to MissionEngine.
  if (isVerificationWorkOrderRequest(question)) {
    return {
      reason: 'canary_verification_work_order_missing_prospects',
      structured: buildStructuredResponse({
        answer: [
          'Got it. I will treat this as a preparation-only canary verification work order, not a launch or execution run.',
          'I need 3 prospects before I can build the verification work order.',
          'Send them as company name, decision maker if known, website, mailing address, and phone if you have it.',
          'Preparation-only. No mission created. No launch, execution, approval, print, or mail.',
        ].join(' '),
        reasoning: [
          'Absolute canary hard stop: verification work order stays preparation-only and never creates or resumes a mission.',
          'No usable canary prospect rows were found in the operator message.',
          'Hard stop: no campaign_creation / mail_package_generation / direct_mail_execution mission was started.',
        ],
        supportingEvidence: [],
        contradictingEvidence: [],
        confidence: null,
        nextInvestigations: ['Paste 3 prospects for the verification work order.'],
        recommendedActions: [],
        metadata: {
          sourcesUsed: {},
          evidenceCount: 0,
          unavailable: ['campaign_001_prospect_artifact'],
          surface: 'workspace',
          executionDomain: EXECUTION_DOMAINS.WORKSPACE,
          route: 'intelligence',
          canaryPreparationOnly: true,
          verificationWorkOrder: true,
        },
      }),
    };
  }

  return {
    reason: 'canary_missing_prospects_clarification',
    structured: buildStructuredResponse({
      answer: [
        'Got it. I will treat this as a preparation-only canary, not a launch or execution run.',
        'I cannot see three usable Campaign 001 prospects in the current workspace context, so send me 3 prospect names before I create any package mission.',
        'Send them as company name, decision maker if known, website, mailing address, and phone if you have it.',
      ].join(' '),
      reasoning: [
        'Absolute canary hard stop: preparation-only / prep-only / review-only canary never creates or resumes a mission.',
        'No usable canary prospect rows were found in the operator message.',
        'Hard stop: no campaign_creation / mail_package_generation / direct_mail_execution mission was started.',
      ],
      supportingEvidence: [],
      contradictingEvidence: [],
      confidence: null,
      nextInvestigations: ['Paste 3 prospects for the canary package.'],
      recommendedActions: [],
      metadata: {
        sourcesUsed: {},
        evidenceCount: 0,
        unavailable: ['campaign_001_prospect_artifact'],
        surface: 'workspace',
        executionDomain: EXECUTION_DOMAINS.WORKSPACE,
        route: 'intelligence',
        canaryPreparationOnly: true,
      },
    }),
  };
}

function rememberCanaryActiveWorkContext(input = {}) {
  if (!input.session) return null;
  const lastOutputType =
    input.lastOutputType || LAST_OUTPUT_TYPES.CANARY_REVIEW_PACKAGE;
  const nextAction =
    input.nextAction !== undefined
      ? input.nextAction
      : lastOutputType === LAST_OUTPUT_TYPES.VERIFICATION_WORK_ORDER
        ? 'convert_to_fillable_table'
        : lastOutputType === LAST_OUTPUT_TYPES.FILLABLE_TABLE
          ? 'verify_mail_critical_fields'
          : 'await_operator_transform_or_verification';

  const tableRows =
    input.tableRows !== undefined
      ? input.tableRows
      : lastOutputType === LAST_OUTPUT_TYPES.FILLABLE_TABLE
        ? buildFillableTableRows(input.prospects)
        : input.prior && Array.isArray(input.prior.tableRows)
          ? input.prior.tableRows
          : [];

  return setActiveWorkContext(
    input.session,
    buildCanaryActiveWorkContext({
      prospects: input.prospects,
      campaignId:
        extractCampaignIdFromText(input.question) ||
        (input.prior && input.prior.target && input.prior.target.campaignId) ||
        '001',
      workflow:
        input.workflow ||
        (input.prior && input.prior.workflow) ||
        undefined,
      canaryWorkflowType:
        input.canaryWorkflowType ||
        resolveCanaryWorkflowType(
          input.question || '',
          Array.isArray(input.tableRows) ? input.tableRows : [],
          input.prior || null
        ),
      lastOutputType,
      lastOutputKind: input.lastOutputKind,
      nextAction,
      prior: input.prior,
      tableRows,
    })
  );
}

/**
 * Workflow-aware nextInvestigations for canary desk responses.
 * @param {object} input
 * @returns {string[]}
 */
function canaryWorkflowSuggestions(input = {}) {
  const awc = buildCanaryActiveWorkContext({
    prospects: input.prospects || [],
    campaignId: input.campaignId || '001',
    lastOutputType:
      input.lastOutputType || LAST_OUTPUT_TYPES.CANARY_REVIEW_PACKAGE,
    tableRows: input.tableRows || [],
  });
  return buildActiveWorkSuggestions(awc, {
    latestQuestion: input.question,
  }).slice(0, 5);
}

/**
 * UI suggestion chips for an ask result. Prefer structured nextInvestigations
 * when present; otherwise rebuild from activeWorkContext and/or response-level
 * packet-review metadata (outputKind / contextHints) so artifact mode can
 * suppress "Next:" prose without falling back to briefing chips.
 * @param {{ structured?: object, session?: object, question?: string }} input
 * @returns {string[]}
 */
function resolveResultSuggestions(input = {}) {
  const structured = input.structured && typeof input.structured === 'object'
    ? input.structured
    : {};
  const next = Array.isArray(structured.nextInvestigations)
    ? structured.nextInvestigations.map(String).filter(Boolean)
    : [];
  if (next.length) return next;

  const metadata =
    structured.metadata && typeof structured.metadata === 'object'
      ? structured.metadata
      : {};
  const session = input.session || null;
  const awc = getActiveWorkContext(session);
  const page =
    (session && session.context && session.context.page) || 'command-deck';

  const wantsWorkflowChips =
    (awc && isActiveDeskWorkflow(awc)) ||
    metadata.packetReview === true ||
    metadata.callScriptReview === true ||
    metadata.callScriptDecisionRecord === true ||
    metadata.canaryPreparationOnly === true ||
    Boolean(metadata.contextHints) ||
    /packet|call.?script/.test(
      String(metadata.outputKind || metadata.lastOutputKind || '')
    );

  if (!wantsWorkflowChips) return [];

  return buildSuggestions({
    page,
    ...((session && session.context && typeof session.context === 'object'
      ? session.context
      : {})),
    activeWorkContext: awc || undefined,
    metadata,
    outputKind: metadata.outputKind,
    lastOutputKind: metadata.lastOutputKind || metadata.outputKind,
    contextHints: metadata.contextHints,
    packetReviewContext: metadata.packetReviewContext,
    latestQuestion: input.question,
  });
}

function resolveCanaryLastOutputType(question, reason) {
  if (isPacketReviewRequest(question) || reason === 'active_work_context_packet_review') {
    return LAST_OUTPUT_TYPES.PACKET_REVIEW;
  }
  if (
    isCallScriptDecisionRecordRequest(question) ||
    reason === 'inline_known_facts_call_script_decision_record' ||
    reason === 'active_work_context_missing_for_call_script_decision_record' ||
    reason === 'canary_call_script_decision_record_missing_facts'
  ) {
    return LAST_OUTPUT_TYPES.CALL_SCRIPT_DECISION_RECORD;
  }
  if (
    isCallScriptReviewRequest(question) ||
    reason === 'active_work_context_call_script_review' ||
    reason === 'inline_known_facts_call_script_review'
  ) {
    return LAST_OUTPUT_TYPES.CALL_SCRIPT_REVIEW;
  }
  if (isFillableTableRequest(question) || reason === 'canary_fillable_table') {
    return LAST_OUTPUT_TYPES.FILLABLE_TABLE;
  }
  if (
    isVerificationWorkOrderRequest(question) ||
    reason === 'canary_verification_work_order'
  ) {
    return LAST_OUTPUT_TYPES.VERIFICATION_WORK_ORDER;
  }
  if (isProvisionalDraftRequest(question)) {
    return LAST_OUTPUT_TYPES.PROVISIONAL_DRAFTS;
  }
  return LAST_OUTPUT_TYPES.CANARY_REVIEW_PACKAGE;
}

/**
 * @param {string} question
 * @returns {number|null}
 */
function extractIntendedCanaryProspectCount(question) {
  const text = String(question || '');
  const match =
    /\buse\s+(?:these|the\s+same)\s+(\d+)\s+prospects?\b/i.exec(text) ||
    /\b(\d+)\s+canary\s+prospects?\b/i.exec(text) ||
    /\bthese\s+(\d+)\s+prospects?\b/i.exec(text);
  if (!match) return null;
  const n = Number(match[1]);
  return Number.isFinite(n) && n > 0 ? n : null;
}

function buildCanaryParseFailureResponse(input = {}) {
  const count = Number(input.intendedCount) > 0 ? Number(input.intendedCount) : 3;
  const example =
    'PM-001 | Gamache Properties | Ben Gamache | Property Management | website unknown | mailing address unknown | phone unknown';
  return buildStructuredResponse({
    answer: [
      'I’m keeping this preparation-only and will not create a Campaign mission.',
      '',
      `I see you intended to provide ${count} canary prospects, but I could not parse them cleanly.`,
      'Please paste them one per line in this format:',
      '',
      example,
    ].join('\n'),
    reasoning: [
      'Preparation-only canary with no-launch / no-execute constraints.',
      'Prospect extraction failed or was incomplete — hard stop prevents Campaign Creation fallback.',
      'No campaign_creation, mail_package_generation, or direct_mail_execution mission was started.',
    ],
    supportingEvidence: [],
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: [
      `Paste ${count} canary prospects one per line in the pipe-delimited format.`,
    ],
    recommendedActions: [],
    metadata: {
      sourcesUsed: {},
      evidenceCount: 0,
      unavailable: ['canary_prospect_rows'],
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
    },
  });
}

/**
 * True when the operator tried to supply canary prospects inline (even if
 * parsing later fails).
 * @param {string} question
 */
function operatorAttemptedCanaryProspectSupply(question) {
  const text = String(question || '');
  const lower = text.toLowerCase();
  // Field-mutation instructions on an existing fillable table are not a
  // new prospect paste — even when they mention PM-001 / set: / leave unchanged.
  if (isFillableTableUpdateRequest(text)) return false;
  // Packet-review artifact generation references a desk prospect_id; it is not
  // a new prospect paste.
  if (isPacketReviewRequest(text)) return false;
  if (isCallScriptReviewRequest(text)) return false;
  if (isCallScriptDecisionRecordRequest(text)) return false;
  // Cross-prospect canary status summary / judgment mentions PM-00x ids and
  // known-state bullets / readiness tables — never treat as a prospect paste.
  if (isCanarySummaryJudgmentRequest(text)) return false;
  if (looksLikeReadinessSummaryTablePaste(text)) return false;
  if (looksLikeFillableVerificationTablePaste(text)) return false;

  const hasRowSignals =
    hasInlineProspectList(text) ||
    /\bPM-\d{3}\b/i.test(text) ||
    /\d+[\.)]\s+[A-Za-z]{1,12}[-_]?\d{1,6}\b/.test(text) ||
    (/\bprospects?\b/.test(lower) &&
      (/[—–]/.test(text) || /\s\|\s/.test(text)) &&
      /\d+[\.)]\s+/.test(text));

  // "Use the same 3 prospects already listed" reuses desk context — only treat
  // as a new supply attempt when actual prospect rows are also present.
  if (isActiveWorkReuseProspectCue(text)) {
    return hasRowSignals;
  }

  if (/\buse\s+these\s+\d+\s+prospects?\b/i.test(text)) return true;
  if (/\b\d+\s+prospects?\s*:/i.test(text)) return true;
  if (hasRowSignals) return true;
  return false;
}

function buildCanaryReviewPackageResponse(input) {
  const prospects = Array.isArray(input.prospects) ? input.prospects : [];
  const questionText = input.question || input.objectiveText || '';
  const reused = input.reusedFromActiveContext === true;

  if (isFillableTableRequest(questionText)) {
    const tableRows =
      Array.isArray(input.tableRows) && input.tableRows.length > 0
        ? input.tableRows
        : buildFillableTableRows(prospects);
    return {
      reason: 'canary_fillable_table',
      tableRows,
      structured: buildCanaryFillableTableResponse({
        prospects,
        tableRows,
        question: questionText,
        reusedFromActiveContext: reused,
      }),
    };
  }

  const verificationWorkOrder = isVerificationWorkOrderRequest(questionText);
  if (verificationWorkOrder) {
    return {
      reason: 'canary_verification_work_order',
      structured: buildCanaryVerificationWorkOrderResponse({
        prospects,
        question: questionText,
        reusedFromActiveContext: reused,
      }),
    };
  }

  const provisional = isProvisionalDraftRequest(questionText);
  const reviews = prospects.map((p) =>
    assessCanaryProspectReadiness(p, { provisionalDrafts: provisional })
  );
  const count = reviews.length;
  const mailBlocked = reviews.filter((r) => r.mailReadiness !== 'Ready');
  const missingKinds = collectMissingFieldKinds(reviews);
  const safety = [
    'This is preparation-only.',
    'No launch, execution, approval, or mailing has occurred.',
    'Do not print/mail until missing fields are verified.',
  ].join(' ');
  const reuseNote = reused
    ? 'Reusing the prospects already on the desk from active work context.'
    : null;

  if (provisional) {
    const intro = [
      `I found the ${count} canary prospect${count === 1 ? '' : 's'}.`,
      reuseNote,
      'We can draft now from known facts; we cannot mail yet.',
      mailBlocked.length
        ? `Mail readiness is Blocked until ${formatMissingKinds(missingKinds)} are verified.`
        : 'Mailing fields look present; execution stays Blocked until you explicitly approve a launch.',
      safety,
    ]
      .filter(Boolean)
      .join(' ');

    const perProspect = reviews.map(formatProvisionalProspectBlock).join('\n\n');

    const closing =
      'Send or verify website, mailing address, and phone before any print/mail step. I will not launch, execute, approve, or mail anything from this prep request.';

    return buildStructuredResponse({
      answer: [intro, '', perProspect, '', closing].join('\n'),
      reasoning: [
        reused
          ? 'Reused activeWorkContext entities for this follow-up; no re-paste required.'
          : 'Operator supplied canary prospects with preparation-only / no-execution constraints.',
        'Operator asked for provisional review drafts using only known facts, allowing drafts while mailing readiness is Blocked.',
        'Draft readiness is independent of mail readiness; missing address/website/phone block mail only.',
        'No campaign or mail execution mission was started.',
        'Execution readiness stays Blocked absent an explicit launch/execute/approve/mail request.',
      ],
      supportingEvidence: reviews.map((r, i) => ({
        id: `canary-prospect:${r.id || i}`,
        summary: `${r.companyName}: mail ${r.mailReadiness}, draft ${r.draftReadiness}, execution ${r.executionReadiness}`,
        sourceType: 'operator',
        confidence: null,
      })),
      contradictingEvidence: [],
      confidence: null,
      nextInvestigations: canaryWorkflowSuggestions({
        lastOutputType: LAST_OUTPUT_TYPES.PROVISIONAL_DRAFTS,
        prospects,
        question: questionText,
      }),
      recommendedActions: [],
      metadata: {
        sourcesUsed: {
          operatorProspectList: !reused,
          activeWorkContext: reused,
        },
        evidenceCount: reviews.length,
        unavailable: missingKinds,
        surface: 'workspace',
        executionDomain: EXECUTION_DOMAINS.WORKSPACE,
        route: 'intelligence',
        canaryPreparationOnly: true,
        provisionalDrafts: true,
        prospectCount: count,
        activeWorkContextReused: reused,
      },
    });
  }

  const notReady = reviews.filter((r) => r.readiness !== 'Ready');
  const intro = [
    `I found the ${count} canary prospect${count === 1 ? '' : 's'}. I’m keeping this preparation-only.`,
    reuseNote,
    notReady.length
      ? `These are missing ${formatMissingKinds(missingKinds)}, so I can’t mark them ready to mail yet.`
      : 'Mailing fields look present; still no launch, execute, approve, or mail.',
    safety,
  ]
    .filter(Boolean)
    .join(' ');

  const perProspect = reviews
    .map((r) => {
      const missing =
        r.missingFields.length > 0
          ? r.missingFields.join(', ')
          : 'none flagged';
      return [
        `${r.companyName}${r.contactName ? ` (${r.contactName})` : ''}:`,
        `- readiness status: ${r.readiness}`,
        `- Mail readiness: ${r.mailReadiness}`,
        `- Draft readiness: ${r.draftReadiness}`,
        `- Execution readiness: ${r.executionReadiness}`,
        `- missing or unverified fields: ${missing}`,
        `- packet checklist: letter, handwritten note, scorecard cover, business card`,
        `- personalized letter: draft held — needs a verified mailing address first`,
        `- handwritten note: draft held pending address/contact verification`,
        `- scorecard cover text: held for review package once mailing fields are complete`,
        `- first follow-up call notes: confirm decision maker + best reach number before dial`,
        `- next action: ${r.nextAction}`,
        `- tracking fields: prospect id ${r.id}, company, contact, industry, mail readiness`,
      ].join('\n');
    })
    .join('\n\n');

  const closing = notReady.length
    ? 'Send website, mailing address, and phone for each prospect (or confirm which are already verified). I will not launch, execute, approve, or mail anything from this prep request.'
    : 'Review the package above. Say when you want Ready-to-Print prep — I still will not mail or execute unless you explicitly approve a launch.';

  return buildStructuredResponse({
    answer: [intro, '', perProspect, '', closing].join('\n'),
    reasoning: [
      reused
        ? 'Reused activeWorkContext entities for this follow-up; no re-paste required.'
        : 'Operator supplied canary prospects with preparation-only / no-execution constraints.',
      'Prospect rows were extracted; surrounding instruction lines stayed as constraints.',
      'No campaign or mail execution mission was started.',
      notReady.length
        ? 'Missing mailing address, website, and/or phone — mail readiness stays Blocked / Needs verification.'
        : 'Mailing fields present; package stays review-only until operator approves launch.',
    ],
    supportingEvidence: reviews.map((r, i) => ({
      id: `canary-prospect:${r.id || i}`,
      summary: `${r.companyName}: ${r.readiness} — missing ${
        r.missingFields.length ? r.missingFields.join(', ') : 'none'
      }`,
      sourceType: 'operator',
      confidence: null,
    })),
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: canaryWorkflowSuggestions({
      lastOutputType: LAST_OUTPUT_TYPES.CANARY_REVIEW_PACKAGE,
      prospects,
      question: questionText,
    }),
    recommendedActions: [],
    metadata: {
      sourcesUsed: {
        operatorProspectList: !reused,
        activeWorkContext: reused,
      },
      evidenceCount: reviews.length,
      unavailable: missingKinds,
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      provisionalDrafts: false,
      prospectCount: count,
      activeWorkContextReused: reused,
    },
  });
}

function isProvisionalDraftRequest(text) {
  const lower = String(text || '').toLowerCase();
  return (
    /\bprovisional\s+review\s+drafts?\b/.test(lower) ||
    /\bprovisional\s+drafts?\b/.test(lower) ||
    /\busing\s+only\s+known\s+facts\b/.test(lower) ||
    /\bit\s+is\s+okay\s+if\s+mailing\s+readiness\s+is\s+blocked\b/.test(
      lower
    ) ||
    /\bdraft\s+confidence\b/.test(lower)
  );
}

/**
 * Operator wants a field-by-field verification work order instead of
 * provisional drafts or the generic canary readiness package.
 * @param {string} text
 */
function isVerificationWorkOrderRequest(text) {
  const lower = String(text || '').toLowerCase();
  if (/\bverification\s+work\s+order\b/.test(lower)) return true;

  const cues = [
    /\bfields?\s+to\s+verify\b/.test(lower),
    /\bsource\s+type\b/.test(lower),
    /\bready\s+vs\s+(?:still\s+)?blocked\b/.test(lower),
    /\bwhat\s+should\s+be\s+logged\b/.test(lower),
    /\bwhat\s+i\s+should\s+do\s+first\b/.test(lower),
    /\bverify\b/.test(lower),
  ];
  // Require several of the listed work-order cues so generic "verify"
  // language alone does not steal the provisional/readiness paths.
  return cues.filter(Boolean).length >= 4;
}

const CANARY_VERIFICATION_LOGGING_FIELDS = [
  'prospect_id',
  'company_name',
  'contact_name',
  'industry',
  'verification_status',
  'verified_website',
  'verified_mailing_address',
  'verified_phone',
  'verification_source',
  'source_confidence',
  'verified_by',
  'verified_at',
  'mail_readiness',
  'draft_readiness',
  'execution_readiness',
  'follow_up_due_date',
  'notes',
];

function buildCanaryVerificationWorkOrderResponse(input) {
  const prospects = Array.isArray(input.prospects) ? input.prospects : [];
  const reviews = prospects.map((p) => assessCanaryProspectReadiness(p));
  const count = reviews.length;
  const reused = input.reusedFromActiveContext === true;
  const safety =
    'Preparation-only. No mission created. No launch, execution, approval, print, or mail.';

  const intro = [
    'Verification work order',
    '',
    reused
      ? `Reusing the ${count} canary prospect${count === 1 ? '' : 's'} already on the desk.`
      : `I found the ${count} canary prospect${count === 1 ? '' : 's'}.`,
    'Goal: verify mail-critical fields before print/mail.',
    safety,
  ].join('\n');

  const perProspect = reviews
    .map(formatVerificationWorkOrderProspectBlock)
    .join('\n\n');

  const firstAction =
    count === 3
      ? 'First action: verify mailing address for all 3 prospects, because address is the hard blocker for printing/mailing. Then verify website and phone before final packet review.'
      : `First action: verify mailing address for all ${count} prospects, because address is the hard blocker for printing/mailing. Then verify website and phone before final packet review.`;

  return buildStructuredResponse({
    answer: [intro, '', perProspect, '', firstAction, '', safety].join('\n'),
    reasoning: [
      reused
        ? 'Reused activeWorkContext entities for verification work order transformation.'
        : 'Operator supplied canary prospects and requested a verification work order.',
      'Response stays inside the preparation-only canary hard stop — no mission create/resume.',
      'Field checklist uses known prospect identity only; no invented websites, phones, addresses, or evidence.',
      'Mail readiness stays Blocked until operator-verified mailing fields are supplied.',
    ],
    supportingEvidence: reviews.map((r, i) => ({
      id: `canary-prospect:${r.id || i}`,
      summary: `${r.companyName}: verification work order — mail ${r.mailReadiness}`,
      sourceType: 'operator',
      confidence: null,
    })),
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: canaryWorkflowSuggestions({
      lastOutputType: LAST_OUTPUT_TYPES.VERIFICATION_WORK_ORDER,
      prospects,
      question: input.question || input.objectiveText || '',
    }),
    recommendedActions: [firstAction],
    metadata: {
      sourcesUsed: {
        operatorProspectList: !reused,
        activeWorkContext: reused,
      },
      evidenceCount: reviews.length,
      unavailable: collectMissingFieldKinds(reviews),
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      verificationWorkOrder: true,
      provisionalDrafts: false,
      prospectCount: count,
      activeWorkContextReused: reused,
    },
  });
}

const CANARY_FILLABLE_TABLE_COLUMNS = [
  'prospect_id',
  'company_name',
  'contact_name',
  'contact_role_status',
  'website_status',
  'website_value',
  'mailing_address_status',
  'mailing_address_value',
  'phone_status',
  'phone_value',
  'source_to_check_first',
  'verification_status',
  'mail_readiness',
  'draft_readiness',
  'execution_readiness',
  'operator_next_action',
  'notes',
];

function fieldStatusAndValue(value) {
  const raw = value == null ? '' : String(value).trim();
  if (!raw || /^unknown$/i.test(raw) || /^n\/?a$/i.test(raw)) {
    return { status: 'blocked', value: 'unknown' };
  }
  return { status: 'needs verification', value: raw };
}

/**
 * Build fillable verification table rows from canary prospects.
 * Does not invent websites, phones, addresses, or readiness beyond known facts.
 * @param {object[]} prospects
 * @returns {object[]}
 */
function buildFillableTableRows(prospects) {
  const list = Array.isArray(prospects) ? prospects : [];
  const reviews = list.map((p) => assessCanaryProspectReadiness(p));
  return reviews.map((r, i) => {
    const source = list[i] || {};
    const website = fieldStatusAndValue(source.website);
    const mailing = fieldStatusAndValue(
      source.mailingAddress || source.address
    );
    const phone = fieldStatusAndValue(source.phone);
    const contactRoleStatus = r.contactName
      ? 'needs verification'
      : 'blocked';
    const operatorNext =
      mailing.status === 'blocked'
        ? 'verify mailing address first'
        : website.status === 'blocked'
          ? 'verify website next'
          : phone.status === 'blocked'
            ? 'verify phone next'
            : 'confirm all fields then request explicit approval before mail';

    return {
      prospect_id: r.id || 'unknown',
      company_name: r.companyName || 'unknown',
      contact_name: r.contactName || 'unknown',
      contact_role_status: contactRoleStatus,
      website_status: website.status,
      website_value: website.value,
      mailing_address_status: mailing.status,
      mailing_address_value: mailing.value,
      phone_status: phone.status,
      phone_value: phone.value,
      source_to_check_first:
        'official company website / contact page, then business registry or trusted CRM',
      verification_status: 'needs verification',
      mail_readiness:
        String(r.mailReadiness || 'Blocked').toLowerCase() === 'ready'
          ? 'ready'
          : String(r.mailReadiness || 'blocked').toLowerCase(),
      draft_readiness:
        String(r.draftReadiness || 'Blocked').toLowerCase() === 'allowed'
          ? 'allowed'
          : 'blocked',
      execution_readiness: 'blocked',
      operator_next_action: operatorNext,
      notes: '',
    };
  });
}

/**
 * Keep entity identity fields in sync with operator-updated table values
 * without inventing evidence from "unknown" placeholders.
 * @param {object[]} prospects
 * @param {object[]} rows
 */
function syncProspectsFromTableRows(prospects, rows) {
  const byId = new Map();
  for (const row of rows || []) {
    const id = String(row.prospect_id || '').trim();
    if (id) byId.set(id.toUpperCase(), row);
  }
  return (Array.isArray(prospects) ? prospects : []).map((p) => {
    const row = byId.get(String(p.id || '').toUpperCase());
    if (!row) return { ...p };
    const website = blankTableValue(row.website_value);
    const mailing = blankTableValue(row.mailing_address_value);
    const phone = blankTableValue(row.phone_value);
    return {
      ...p,
      id: row.prospect_id || p.id,
      companyName:
        row.company_name && row.company_name !== 'unknown'
          ? row.company_name
          : p.companyName,
      contactName:
        row.contact_name && row.contact_name !== 'unknown'
          ? row.contact_name
          : p.contactName,
      // Table schema usually omits industry — preserve prior desk entity industry.
      industry: p.industry || blankToNull(row.industry) || blankToNull(row.vertical) || null,
      website: website != null ? website : p.website,
      mailingAddress: mailing != null ? mailing : p.mailingAddress,
      address: mailing != null ? mailing : p.address,
      phone: phone != null ? phone : p.phone,
    };
  });
}

function blankTableValue(value) {
  if (value == null) return null;
  const s = String(value).trim();
  if (!s || /^unknown$/i.test(s) || /^n\/?a$/i.test(s)) return null;
  return s;
}

function formatFillableTableMarkdown(rows) {
  const list = Array.isArray(rows) ? rows : [];
  const header = `| ${CANARY_FILLABLE_TABLE_COLUMNS.join(' | ')} |`;
  const separator = `| ${CANARY_FILLABLE_TABLE_COLUMNS.map(() => '---').join(' | ')} |`;
  const body = list.map((row) => {
    const cells = CANARY_FILLABLE_TABLE_COLUMNS.map((col) => {
      const value = row[col];
      return value == null || value === '' ? '' : String(value);
    });
    return `| ${cells.join(' | ')} |`;
  });
  return [header, separator, ...body].join('\n');
}

function buildCanaryFillableTableResponse(input) {
  const prospects = Array.isArray(input.prospects) ? input.prospects : [];
  const rows =
    Array.isArray(input.tableRows) && input.tableRows.length > 0
      ? input.tableRows
      : buildFillableTableRows(prospects);
  const count = rows.length;
  const reused = input.reusedFromActiveContext === true;
  const tableUpdated = input.tableUpdated === true;
  const updatedIds = Array.isArray(input.updatedProspectIds)
    ? input.updatedProspectIds
    : [];
  const reassessedIds = Array.isArray(input.reassessedProspectIds)
    ? input.reassessedProspectIds
    : [];
  const question = String(input.question || '');
  const strictShape = wantsStrictFillableTableOutputShape(question);
  const includeHeading = !strictShape || wantsFillableTableHeading(question);
  const tableMarkdown = formatFillableTableMarkdown(rows);
  const safety =
    strictShape
      ? 'Preparation-only: no mission created; no launch, approval, print, or mail.'
      : 'Preparation-only. No mission created. No launch, execution, approval, print, or mail.';

  let answer;
  if (strictShape) {
    const parts = [];
    if (includeHeading) {
      parts.push('Fillable verification table', '');
    }
    parts.push(tableMarkdown, '', safety);
    answer = parts.join('\n');
  } else {
    const intro = [
      'Fillable verification table',
      '',
      tableUpdated
        ? `Updated the fillable verification table${
            updatedIds.length ? ` for ${updatedIds.join(', ')}` : ''
          }. Other rows are unchanged.`
        : reused
          ? `Converted the verification work order into a fillable table for the same ${count} prospect${count === 1 ? '' : 's'} already on the desk.`
          : `Fillable table for ${count} canary prospect${count === 1 ? '' : 's'}.`,
      tableUpdated
        ? reassessedIds.length
          ? 'Only operator-requested field changes were applied, then readiness was reassessed from table gates. No websites, phones, addresses, sources, or readiness values were invented.'
          : 'Only operator-requested field changes were applied. No websites, phones, addresses, sources, or readiness values were invented.'
        : 'Known identity fields are filled from active work context. Missing mail-critical values stay unknown — I will not invent websites, phones, or addresses.',
      safety,
    ].join('\n');

    const closing =
      'Fill website_value, mailing_address_value, and phone_value from trusted sources before any print/mail step. Constraints stay no-launch / no-mail until you explicitly approve after readiness is complete.';

    answer = [intro, '', tableMarkdown, '', closing].join('\n');
  }

  return buildStructuredResponse({
    answer,
    reasoning: strictShape
      ? []
      : [
          tableUpdated
            ? 'Applied fillable verification table field mutations from activeWorkContext before prospect extraction or mission routing.'
            : reused
              ? 'Early activeWorkContext continuation reused desk entities before domain routing.'
              : 'Operator requested a fillable verification table for canary prospects.',
          tableUpdated
            ? 'Preserved existing table shape/columns and left non-targeted rows unchanged.'
            : 'Table preserves known identity fields only; mail-critical fields left unknown for verification.',
          reassessedIds.length
            ? 'Reassessed mail/draft/execution readiness from verified table gates without inferring launch.'
            : null,
          'No mission create/resume. No launch, mail, print, or approval inferred from desk context.',
          tableUpdated
            ? 'Handled as a table mutation before domain routing.'
            : 'Handled via early active-work continuation before domain routing.',
        ].filter(Boolean),
    supportingEvidence: strictShape
      ? []
      : rows.map((row, i) => ({
          id: `canary-prospect:${row.prospect_id || i}`,
          summary: `${row.company_name}: fillable table row — mail ${row.mail_readiness}`,
          sourceType: 'operator',
          confidence: null,
        })),
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: strictShape
      ? []
      : canaryWorkflowSuggestions({
          lastOutputType: LAST_OUTPUT_TYPES.FILLABLE_TABLE,
          prospects,
          tableRows: rows,
          question,
        }),
    recommendedActions: strictShape
      ? []
      : [
          'Verify mailing address first for every row, then website and phone.',
        ],
    metadata: {
      sourcesUsed: {
        operatorProspectList: !reused && !tableUpdated,
        activeWorkContext: reused || tableUpdated,
      },
      evidenceCount: strictShape ? 0 : rows.length,
      unavailable: strictShape
        ? []
        : collectMissingFieldKinds(
            prospects.map((p) => assessCanaryProspectReadiness(p))
          ),
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      fillableTable: true,
      verificationWorkOrder: false,
      provisionalDrafts: false,
      prospectCount: count,
      activeWorkContextReused: reused || tableUpdated,
      tableUpdate: tableUpdated || undefined,
      updatedProspectIds: tableUpdated ? updatedIds : undefined,
      reassessedProspectIds: tableUpdated && reassessedIds.length
        ? reassessedIds
        : undefined,
      strictOutputShape: strictShape || undefined,
    },
  });
}

function buildCanaryExecutionBlockedResponse(input = {}) {
  const ctx = input.activeWorkContext || {};
  const prospects = entitiesToProspects(ctx.entities);
  const reviews = prospects.map((p) => assessCanaryProspectReadiness(p));
  const missingKinds = collectMissingFieldKinds(reviews);
  const names = reviews.map((r) => r.companyName).filter(Boolean);
  const pending =
    Array.isArray(ctx.pendingFields) && ctx.pendingFields.length
      ? ctx.pendingFields.join(', ')
      : formatMissingKinds(missingKinds);

  const answer = [
    'I am not mailing or launching anything from this request.',
    'Active work context still has preparation-only / no-mail / no-execution constraints.',
    names.length
      ? `Prospects still on the desk: ${names.join('; ')}.`
      : null,
    pending
      ? `Required readiness still missing or unverified: ${pending}.`
      : 'Mail-critical fields still need explicit operator verification.',
    'To proceed later, verify the missing fields first, then give an explicit approval to launch or mail after readiness is complete. I will not infer execution from prior desk context.',
  ]
    .filter(Boolean)
    .join(' ');

  return buildStructuredResponse({
    answer,
    reasoning: [
      'Operator asked to mail/launch/execute while activeWorkContext constraints forbid execution.',
      'Active work context never overrides missing-field readiness or invents approval.',
      'No mission created. No print, mail, launch, or approval occurred.',
    ],
    supportingEvidence: reviews.map((r, i) => ({
      id: `canary-prospect:${r.id || i}`,
      summary: `${r.companyName}: mail ${r.mailReadiness}, execution Blocked`,
      sourceType: 'operator',
      confidence: null,
    })),
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: canaryWorkflowSuggestions({
      lastOutputType:
        (ctx && ctx.lastOutputType) || LAST_OUTPUT_TYPES.FILLABLE_TABLE,
      prospects,
      tableRows: Array.isArray(ctx.tableRows) ? ctx.tableRows : [],
      question: input.question,
    }),
    recommendedActions: [
      'Do not mail yet — complete verification first, then request explicit approval.',
    ],
    metadata: {
      sourcesUsed: { activeWorkContext: true },
      evidenceCount: reviews.length,
      unavailable: missingKinds,
      surface: 'workspace',
      executionDomain: EXECUTION_DOMAINS.WORKSPACE,
      route: 'intelligence',
      canaryPreparationOnly: true,
      activeWorkContextReused: true,
      prospectCount: reviews.length,
    },
  });
}

function formatVerificationWorkOrderProspectBlock(r) {
  const contact = r.contactName || 'contact unknown';
  const loggingLines = CANARY_VERIFICATION_LOGGING_FIELDS.map(
    (field) => `- ${field}`
  ).join('\n');

  return [
    `${r.id} — ${r.companyName} (${contact})`,
    '',
    'Status: Blocked for mailing',
    'Goal: verify mail-critical fields before print/mail',
    '',
    'Fields to verify:',
    '1. Mailing address',
    '   - Suggested source type: official company website, property-management contact page, business registry, trusted CRM record, or direct confirmation',
    '   - Why it matters: required for packet delivery and print approval',
    '   - Ready value: complete deliverable mailing address tied to company/contact',
    '   - Still Blocked if: missing, stale, residential-only, ambiguous office/location, or unverified',
    '',
    '2. Website',
    '   - Suggested source type: official website, verified business profile, CRM, or direct confirmation',
    '   - Why it matters: evidence anchor for company identity and personalization review',
    '   - Ready value: official/verified website',
    '   - Still Blocked if: unknown, placeholder, directory-only with low confidence, or conflicting sources',
    '',
    '3. Phone',
    '   - Suggested source type: official website, business listing, CRM, or direct confirmation',
    '   - Why it matters: required for follow-up call workflow',
    '   - Ready value: working business/contact phone',
    '   - Still Blocked if: unknown, disconnected, personal/unverified, or conflicting sources',
    '',
    '4. Contact name / role',
    '   - Suggested source type: company site, LinkedIn, CRM, direct confirmation',
    '   - Why it matters: avoids misaddressed package and call notes',
    '   - Ready value: contact confirmed as relevant decision maker or influencer',
    '   - Still Blocked if: role unverified or contact mismatch',
    '',
    'PulseForge logging fields:',
    loggingLines,
  ].join('\n');
}

function formatProvisionalProspectBlock(r) {
  const missing =
    r.missingFields.length > 0
      ? r.missingFields.join(', ')
      : 'none flagged';
  const drafts = r.drafts || {};
  const statusLabel =
    r.mailReadiness === 'Ready'
      ? 'Ready for mailing review'
      : 'Blocked for mailing';

  return [
    `${r.companyName}${r.contactName ? ` (${r.contactName})` : ''}:`,
    `Status: ${statusLabel}`,
    `Draft confidence: ${r.draftConfidence}`,
    `Mail readiness: ${r.mailReadiness}`,
    `Draft readiness: ${r.draftReadiness}`,
    `Execution readiness: ${r.executionReadiness}`,
    '',
    'Provisional personalized letter:',
    drafts.letter || '(draft held — company, contact, and industry required)',
    '',
    'Handwritten note:',
    drafts.handwrittenNote ||
      '(draft held — company, contact, and industry required)',
    '',
    'Scorecard cover text:',
    drafts.scorecardCover ||
      '(draft held — company, contact, and industry required)',
    '',
    'First follow-up call notes:',
    drafts.followUpNotes ||
      'Confirm decision maker and best reach number before dial.',
    '',
    `Missing fields blocking mail readiness: ${missing}`,
    '',
    'Verify before printing:',
    r.verifyBeforePrinting,
    '',
    'Track once mailed:',
    r.trackOnceMailed,
  ].join('\n');
}

function assessCanaryProspectReadiness(prospect = {}, options = {}) {
  const companyName = String(prospect.companyName || '').trim() || 'Unknown company';
  const contactName = prospect.contactName
    ? String(prospect.contactName).trim()
    : null;
  const industry = prospect.industry
    ? String(prospect.industry).trim()
    : prospect.vertical
      ? String(prospect.vertical).trim()
      : null;

  const missingFields = [];
  if (!String(prospect.address || prospect.mailingAddress || '').trim()) {
    missingFields.push('mailing address');
  }
  if (!String(prospect.website || '').trim()) {
    missingFields.push('website');
  }
  if (!String(prospect.phone || '').trim()) {
    missingFields.push('phone');
  }

  const mailReadiness =
    missingFields.length === 0
      ? 'Ready'
      : missingFields.includes('mailing address')
        ? 'Blocked'
        : 'Needs verification';

  // Draft readiness is independent of mailing fields.
  const draftReadiness =
    companyName &&
    companyName !== 'Unknown company' &&
    contactName &&
    industry
      ? 'Allowed'
      : 'Blocked';

  // Execution always blocked unless the operator explicitly launches later.
  const executionReadiness = 'Blocked';

  const draftConfidence =
    draftReadiness !== 'Allowed'
      ? 'Low'
      : mailReadiness === 'Ready'
        ? 'High'
        : 'Medium';

  const readiness =
    mailReadiness === 'Ready'
      ? 'Ready'
      : mailReadiness === 'Blocked'
        ? 'Blocked'
        : 'Needs verification';

  const drafts =
    options.provisionalDrafts && draftReadiness === 'Allowed'
      ? buildConservativeCanaryDrafts({
          companyName,
          contactName,
          industry,
        })
      : null;

  return {
    id: prospect.id || 'unknown',
    companyName,
    contactName,
    industry,
    readiness,
    mailReadiness,
    draftReadiness,
    executionReadiness,
    draftConfidence,
    missingFields,
    drafts,
    verifyBeforePrinting:
      missingFields.length > 0
        ? `Verify ${missingFields.join(', ')} against a trusted source before any print or mail step.`
        : 'Confirm mailing address, website, and phone still match the decision maker before printing.',
    trackOnceMailed:
      'Prospect id, company, contact, industry, mail date, packet contents (letter / note / scorecard), and first follow-up call outcome.',
    nextAction:
      mailReadiness === 'Ready'
        ? 'Operator review of letter / note / scorecard before any approve-to-mail'
        : `Supply ${missingFields.join(', ')} before marking ready to mail`,
  };
}

/**
 * Conservative provisional drafts — known facts only (name, company, industry).
 * No invented property counts, cities, vendors, pain points, or prior conversations.
 */
function buildConservativeCanaryDrafts(input) {
  const company = input.companyName;
  const contact = input.contactName;
  const firstName = String(contact).split(/\s+/)[0] || contact;
  const industry = input.industry;
  const industryLower = industry.toLowerCase();

  const framing = industryFraming(industryLower);

  const letter = [
    `${firstName},`,
    '',
    `I’m reaching out because ${company} appears to be in ${industryLower}, where ${framing} can directly affect owner confidence.`,
    '',
    `PulseForge is preparing a short operational scorecard for ${industryLower} teams to identify where follow-up, vendor coordination, and growth opportunities may be slipping through.`,
    '',
    'I’d like to send you the scorecard for review once we verify the correct mailing address.',
    '',
    'Best,',
    '[Sender]',
  ].join('\n');

  const handwrittenNote = [
    `${firstName} — preparing a short operational scorecard for ${industryLower} teams.`,
    'Will send once we confirm the correct mailing address.',
    '— [Sender]',
  ].join(' ');

  const scorecardCover = [
    `Operational scorecard — ${company}`,
    `Prepared for: ${contact}`,
    `Context: ${industry}`,
    'Status: Provisional review draft — mailing fields not yet verified',
    'Evidence note: uses only company name, contact name, and industry from the operator list; no other claims.',
  ].join('\n');

  const followUpNotes = [
    `Confirm decision-maker (${contact}) and best reach number before dialing.`,
    'Do not reference an unverified mailing address, website, or phone.',
    `Ask whether a short operational scorecard for ${industryLower} teams would be useful to review.`,
    'Stay within known facts only — company, contact, and industry.',
  ].join(' ');

  return {
    letter,
    handwrittenNote,
    scorecardCover,
    followUpNotes,
  };
}

function industryFraming(industryLower) {
  if (/property\s*management|prop(?:erty)?\s*mgmt/.test(industryLower)) {
    return 'tenant experience, vendor reliability, and response time';
  }
  if (/law|legal|attorney|accountant|accounting|cpa/.test(industryLower)) {
    return 'client response time, vendor reliability, and office follow-through';
  }
  return 'follow-up reliability, vendor coordination, and response time';
}

function collectMissingFieldKinds(reviews) {
  const set = new Set();
  reviews.forEach((r) => {
    (r.missingFields || []).forEach((f) => set.add(f));
  });
  return [...set];
}

function formatMissingKinds(kinds) {
  if (!kinds.length) return 'required mailing fields';
  const plural = (k) =>
    k === 'mailing address'
      ? 'mailing addresses'
      : k === 'website'
        ? 'websites'
        : k === 'phone'
          ? 'phones'
          : `${k}s`;
  const parts = kinds.map(plural);
  if (parts.length === 1) return parts[0];
  if (parts.length === 2) return `${parts[0]} and/or ${parts[1]}`;
  return `${parts.slice(0, -1).join(', ')}, and/or ${parts[parts.length - 1]}`;
}

/**
 * Absolute preparation-only canary matcher.
 * No separate no-execution phrase is required — any of these cues hard-stops
 * MissionEngine create/resume, including when IntentUnderstanding returns
 * campaign_creation or Unknown.
 * @param {string} text
 */
function isPreparationOnlyCanary(text) {
  const lower = String(text || '').toLowerCase();

  const canary =
    /\bcanary\b/.test(lower) &&
    (/\bpreparation[-\s]*only\b/.test(lower) ||
      /\bprep[-\s]*only\b/.test(lower) ||
      /\breview[-\s]*only\b/.test(lower) ||
      /\bcanary package\b/.test(lower) ||
      /\bcanary package for review\b/.test(lower) ||
      /\bdraft[-\s]*only\b/.test(lower) ||
      /\bprepare\s+the\s+review\s+package\s+only\b/.test(lower) ||
      /\breview\s+package\s+only\b/.test(lower));

  return canary;
}

function hasInlineProspectList(question) {
  if (looksLikeFillableVerificationTablePaste(question)) return false;

  const detected = detectOperatorProspectListInMessage(question);
  if (detected.detected && detected.prospectCount > 0) return true;

  const lines = String(question || '')
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  if (
    lines.some(
      (line) => /company\s+name/i.test(line) && /website|address|phone/i.test(line)
    )
  ) {
    return true;
  }
  return lines.filter(
    (line) => /https?:\/\//i.test(line) || line.split(',').length >= 3
  ).length >= 2;
}

module.exports = {
  WorkspaceEngine,
  createWorkspaceEngine,
};
