'use strict';

const { normalizeContext } = require('./ContextEnvelope');
const { buildOpeningState } = require('./OpeningStateBuilder');
const { buildSuggestions } = require('./SuggestionEngine');
const { SessionStore } = require('./SessionStore');
const { composeResponse } = require('./ResponseComposer');
const { buildStructuredResponse } = require('./WorkspaceTypes');
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
  isExplicitNewMissionRequest,
  isExplicitContextOverride,
  isExplicitExecutionRequest,
  isFillableTableRequest,
  isFillableTableUpdateRequest,
  activeContextHasFillableTable,
  knownActiveWorkProspectIds,
  parseFillableTableFieldUpdates,
  applyFillableTableFieldUpdates,
  extractCampaignIdFromText,
  activeContextBlocksExecution,
  activeContextHasEntities,
  LAST_OUTPUT_TYPES,
} = require('./ActiveWorkContext');
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
      let proseEarly = presentedEarly.prose;
      if (envelopeSwitch) {
        proseEarly = `${envelopeSwitch}\n\n${proseEarly}`;
      }

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
        suggestions: structuredEarly.nextInvestigations,
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

    // 1–2) Intent Understanding → Select Execution Domain (ignore active convo)
    const domainDecision = selectExecutionDomain(question, {
      previousDomain: session.executionDomain || null,
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

  const prior = getActiveWorkContext(session);
  const hasEntities = activeContextHasEntities(prior);
  const isFollowUp = isActiveWorkFollowUpCue(question);
  const isFillable = isFillableTableRequest(question);
  const isTransform = isActiveWorkTransformCue(question);
  const isExec = isExplicitExecutionRequest(question);
  const isTableUpdate =
    hasEntities &&
    activeContextHasFillableTable(prior) &&
    isFillableTableUpdateRequest(question, prior);

  // Fillable table field mutation — before prospect extraction, artifact
  // injection, domain routing, or mission routing.
  if (isTableUpdate) {
    return handleFillableTableUpdateContinuation({
      question,
      session,
      prior,
    });
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
      !isFillableTableUpdateRequest(question, prior)
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
    if (isFillable || /\bconvert\s+the\s+(?:verification\s+)?work\s+order\b/i.test(question)) {
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
  const applied = applyFillableTableFieldUpdates(baseRows, parsed.updates);
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

  const updatedRows = applied.rows;
  const structured = buildCanaryFillableTableResponse({
    prospects,
    tableRows: updatedRows,
    question,
    reusedFromActiveContext: true,
    tableUpdated: true,
    updatedProspectIds: applied.matchedIds,
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

  const isCanary = isPreparationOnlyCanary(question);
  const isFollowUp = isActiveWorkFollowUpCue(question);
  const isExec = isExplicitExecutionRequest(question);
  const hasPriorCanary =
    prior &&
    prior.workflow === 'campaign_canary' &&
    activeContextHasEntities(prior);
  const overrideWithNewProspects =
    hasPriorCanary &&
    (isExplicitContextOverride(question) ||
      operatorAttemptedCanaryProspectSupply(question)) &&
    !isFillableTableUpdateRequest(question, prior);

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
      lastOutputType,
      nextAction,
      prior: input.prior,
      tableRows,
    })
  );
}

function resolveCanaryLastOutputType(question, reason) {
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
      nextInvestigations: mailBlocked.length
        ? [
            'Provide mailing address, website, and phone for each canary prospect.',
          ]
        : ['Confirm review package, then explicitly approve any later launch.'],
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
    nextInvestigations: notReady.length
      ? [
          'Provide mailing address, website, and phone for each canary prospect.',
        ]
      : ['Confirm review package, then explicitly approve any later launch.'],
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
    nextInvestigations: [
      'Verify mailing address for each canary prospect, then website and phone.',
    ],
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
  const safety =
    'Preparation-only. No mission created. No launch, execution, approval, print, or mail.';

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
      ? 'Only operator-requested field changes were applied. No websites, phones, addresses, sources, or readiness values were invented.'
      : 'Known identity fields are filled from active work context. Missing mail-critical values stay unknown — I will not invent websites, phones, or addresses.',
    safety,
  ].join('\n');

  const closing =
    'Fill website_value, mailing_address_value, and phone_value from trusted sources before any print/mail step. Constraints stay no-launch / no-mail until you explicitly approve after readiness is complete.';

  return buildStructuredResponse({
    answer: [intro, '', formatFillableTableMarkdown(rows), '', closing].join(
      '\n'
    ),
    reasoning: [
      tableUpdated
        ? 'Applied fillable verification table field mutations from activeWorkContext before prospect extraction or mission routing.'
        : reused
          ? 'Early activeWorkContext continuation reused desk entities before domain routing.'
          : 'Operator requested a fillable verification table for canary prospects.',
      tableUpdated
        ? 'Preserved existing table shape/columns and left non-targeted rows unchanged.'
        : 'Table preserves known identity fields only; mail-critical fields left unknown for verification.',
      'No mission create/resume. No launch, mail, print, or approval inferred from desk context.',
      tableUpdated
        ? 'Handled as a table mutation before domain routing.'
        : 'Handled via early active-work continuation before domain routing.',
    ],
    supportingEvidence: rows.map((row, i) => ({
      id: `canary-prospect:${row.prospect_id || i}`,
      summary: `${row.company_name}: fillable table row — mail ${row.mail_readiness}`,
      sourceType: 'operator',
      confidence: null,
    })),
    contradictingEvidence: [],
    confidence: null,
    nextInvestigations: [
      'Fill website_value, mailing_address_value, and phone_value for each row from a trusted source.',
    ],
    recommendedActions: [
      'Verify mailing address first for every row, then website and phone.',
    ],
    metadata: {
      sourcesUsed: {
        operatorProspectList: !reused && !tableUpdated,
        activeWorkContext: reused || tableUpdated,
      },
      evidenceCount: rows.length,
      unavailable: collectMissingFieldKinds(
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
    nextInvestigations: [
      'Verify mailing address, website, and phone, then explicitly approve any launch.',
    ],
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
