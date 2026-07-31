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

    const canaryPrep = missionsAvailable
      ? await maybeBuildCanaryPreparationResponse({
          question,
          tenantId: session.context.tenantId,
          missionEngine: this._missionEngine,
          domainDecision,
        })
      : null;

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
        missionIntent: domainDecision.missionIntent || null,
        executionDomain: domainDecision.domain,
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

/**
 * Preparation-only canary: either ask for prospects, or return a review
 * package conversationally — never create/resume Campaign or Direct Mail Execution.
 * A parser miss must never fall through to campaign_creation /
 * mail_package_generation / direct_mail_execution.
 * @returns {Promise<{ structured: object, reason: string }|null>}
 */
async function maybeBuildCanaryPreparationResponse(input) {
  const question = String(input.question || '');
  if (!isPreparationOnlyCanary(question)) return null;

  const detected = detectOperatorProspectListInMessage(question);
  const intendedCount = extractIntendedCanaryProspectCount(question);
  const completeProspects =
    detected.detected &&
    detected.prospectCount > 0 &&
    (!intendedCount || detected.prospectCount >= intendedCount);

  if (completeProspects) {
    return {
      reason: 'canary_preparation_review_package',
      structured: buildCanaryReviewPackageResponse({
        prospects: detected.prospects,
        objectiveText: detected.objectiveText || question,
        question,
        domainDecision: input.domainDecision,
      }),
    };
  }

  // Soft clarification when the operator invited "ask me for names" OR
  // referenced existing campaign prospects without pasting an inline list.
  if (
    !operatorAttemptedCanaryProspectSupply(question) &&
    (isCanaryAwaitingProspects(question) ||
      referencesExistingCampaignProspects(question))
  ) {
    return {
      reason: 'canary_missing_prospects_clarification',
      structured: buildStructuredResponse({
        answer: [
          'Got it. I will treat this as a preparation-only canary, not a launch or execution run.',
          'I cannot see three usable Campaign 001 prospects in the current workspace context, so send me 3 prospect names before I create any package mission.',
          'Send them as company name, decision maker if known, website, mailing address, and phone if you have it.',
        ].join(' '),
        reasoning: [
          'The operator explicitly said not to launch or execute direct mail.',
          'The operator asked Max to request 3 prospect names instead of creating a mission when existing Campaign 001 prospects are not accessible.',
          'No usable Campaign 001 prospect artifact was found in the current mission workspace context.',
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
          executionDomain: input.domainDecision && input.domainDecision.domain,
          route: 'intelligence',
          canaryPreparationOnly: true,
        },
      }),
    };
  }

  // Hard stop on parser miss / incomplete supply — never create a Campaign mission.
  const count = intendedCount || 3;
  return {
    reason: 'canary_prospect_parse_clarification',
    structured: buildCanaryParseFailureResponse({
      domainDecision: input.domainDecision,
      intendedCount: count,
    }),
  };
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
      executionDomain: input.domainDecision && input.domainDecision.domain,
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
  if (/\buse\s+these\s+\d+\s+prospects?\b/i.test(text)) return true;
  if (/\buse\s+the\s+same\s+\d+\s+prospects?\b/i.test(text)) return true;
  if (/\b\d+\s+prospects?\s*:/i.test(text)) return true;
  if (/\bPM-\d{3}\b/i.test(text)) return true;
  if (/\d+[\.)]\s+[A-Za-z]{1,12}[-_]?\d{1,6}\b/.test(text)) return true;
  if (hasInlineProspectList(text)) return true;
  if (
    /\bprospects?\b/.test(lower) &&
    (/[—–]/.test(text) || /\s\|\s/.test(text)) &&
    /\d+[\.)]\s+/.test(text)
  ) {
    return true;
  }
  return false;
}

function referencesExistingCampaignProspects(question) {
  const lower = String(question || '').toLowerCase();
  return (
    /\bfrom\s+(?:the\s+)?existing\b/.test(lower) ||
    /\bif\s+available\b/.test(lower) ||
    /\bif\s+you\s+cannot\s+access\b/.test(lower)
  );
}

function buildCanaryReviewPackageResponse(input) {
  const prospects = Array.isArray(input.prospects) ? input.prospects : [];
  const provisional = isProvisionalDraftRequest(
    input.question || input.objectiveText || ''
  );
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

  if (provisional) {
    const intro = [
      `I found the ${count} canary prospect${count === 1 ? '' : 's'}.`,
      'We can draft now from known facts; we cannot mail yet.',
      mailBlocked.length
        ? `Mail readiness is Blocked until ${formatMissingKinds(missingKinds)} are verified.`
        : 'Mailing fields look present; execution stays Blocked until you explicitly approve a launch.',
      safety,
    ].join(' ');

    const perProspect = reviews.map(formatProvisionalProspectBlock).join('\n\n');

    const closing =
      'Send or verify website, mailing address, and phone before any print/mail step. I will not launch, execute, approve, or mail anything from this prep request.';

    return buildStructuredResponse({
      answer: [intro, '', perProspect, '', closing].join('\n'),
      reasoning: [
        'Operator supplied canary prospects with preparation-only / no-execution constraints.',
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
        sourcesUsed: { operatorProspectList: true },
        evidenceCount: reviews.length,
        unavailable: missingKinds,
        surface: 'workspace',
        executionDomain: input.domainDecision && input.domainDecision.domain,
        route: 'intelligence',
        canaryPreparationOnly: true,
        provisionalDrafts: true,
        prospectCount: count,
      },
    });
  }

  const notReady = reviews.filter((r) => r.readiness !== 'Ready');
  const intro = [
    `I found the ${count} canary prospect${count === 1 ? '' : 's'}. I’m keeping this preparation-only.`,
    notReady.length
      ? `These are missing ${formatMissingKinds(missingKinds)}, so I can’t mark them ready to mail yet.`
      : 'Mailing fields look present; still no launch, execute, approve, or mail.',
    safety,
  ].join(' ');

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
      'Operator supplied canary prospects with preparation-only / no-execution constraints.',
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
      sourcesUsed: { operatorProspectList: true },
      evidenceCount: reviews.length,
      unavailable: missingKinds,
      surface: 'workspace',
      executionDomain: input.domainDecision && input.domainDecision.domain,
      route: 'intelligence',
      canaryPreparationOnly: true,
      provisionalDrafts: false,
      prospectCount: count,
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

function isPreparationOnlyCanary(question) {
  const lower = String(question || '').toLowerCase();
  const canaryCue =
    /\bcanary\b/.test(lower) &&
    (/\b(preparation|prep|review|draft)[-\s]*only\b/.test(lower) ||
      /\bprepare\s+the\s+review\s+package\s+only\b/.test(lower) ||
      /\breview\s+package\s+only\b/.test(lower));
  const noExec =
    /\bnot\s+(launching|executing|mailing)\b/.test(lower) ||
    /\b(still\s+)?do\s+not\s+(run|execute|launch|mail|resume|approve|print|create)\b/.test(
      lower
    ) ||
    /\bdo\s+not\s+launch,\s*execute/.test(lower) ||
    /\bdo\s+not\s+create\s+a\s+mission\b/.test(lower) ||
    /\bno[- ]?(launch|execute|mail|approve|print)\b/.test(lower);
  return canaryCue && noExec;
}

function isCanaryAwaitingProspects(question) {
  const lower = String(question || '').toLowerCase();
  return (
    /\bask\s+me\s+for\s+(3|three)\s+prospect/.test(lower) ||
    /\binstead\s+of\s+creating\b/.test(lower) ||
    /\bif\s+you\s+cannot\s+access\b/.test(lower)
  );
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
