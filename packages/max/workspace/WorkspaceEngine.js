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
 * @returns {Promise<{ structured: object, reason: string }|null>}
 */
async function maybeBuildCanaryPreparationResponse(input) {
  const question = String(input.question || '');
  if (!isPreparationOnlyCanary(question)) return null;

  const detected = detectOperatorProspectListInMessage(question);
  if (detected.detected && detected.prospectCount > 0) {
    return {
      reason: 'canary_preparation_review_package',
      structured: buildCanaryReviewPackageResponse({
        prospects: detected.prospects,
        objectiveText: detected.objectiveText || question,
        domainDecision: input.domainDecision,
      }),
    };
  }

  if (hasInlineProspectList(question)) return null;

  const existing = await hasExistingCampaignProspects({
    missionEngine: input.missionEngine,
    tenantId: input.tenantId,
    question,
  });
  if (existing) return null;

  // Only ask for prospects when the operator invited that clarification path
  // or clearly has no list yet.
  if (!isCanaryAwaitingProspects(question)) return null;

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
      },
    }),
  };
}

function buildCanaryReviewPackageResponse(input) {
  const prospects = Array.isArray(input.prospects) ? input.prospects : [];
  const reviews = prospects.map((p) => assessCanaryProspectReadiness(p));
  const count = reviews.length;
  const notReady = reviews.filter((r) => r.readiness !== 'Ready');
  const missingKinds = collectMissingFieldKinds(reviews);

  const intro = [
    `I found the ${count} canary prospect${count === 1 ? '' : 's'}. I’m keeping this preparation-only.`,
    notReady.length
      ? `These are missing ${formatMissingKinds(missingKinds)}, so I can’t mark them ready to mail yet.`
      : 'Mailing fields look present; still no launch, execute, approve, or mail.',
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
        ? 'Missing mailing address, website, and/or phone — readiness stays Blocked / Needs verification.'
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
      prospectCount: count,
    },
  });
}

function assessCanaryProspectReadiness(prospect = {}) {
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
  const readiness =
    missingFields.length === 0
      ? 'Ready'
      : missingFields.includes('mailing address')
        ? 'Blocked'
        : 'Needs verification';
  return {
    id: prospect.id || 'unknown',
    companyName: String(prospect.companyName || '').trim() || 'Unknown company',
    contactName: prospect.contactName
      ? String(prospect.contactName).trim()
      : null,
    industry: prospect.industry ? String(prospect.industry).trim() : null,
    readiness,
    missingFields,
    nextAction:
      readiness === 'Ready'
        ? 'Operator review of letter / note / scorecard before any approve-to-mail'
        : `Supply ${missingFields.join(', ')} before marking ready to mail`,
  };
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
    /\b(still\s+)?do\s+not\s+(run|execute|launch|mail|resume|approve)\b/.test(
      lower
    ) ||
    /\bdo\s+not\s+launch,\s*execute/.test(lower);
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

async function hasExistingCampaignProspects(input) {
  const engine = input.missionEngine;
  if (!engine || typeof engine.list !== 'function') return false;
  const campaign = extractCampaignId(input.question);
  try {
    const missions = await engine.list({ tenantId: input.tenantId });
    return missions.some((mission) => {
      const text = `${mission.title || ''} ${mission.objectiveText || ''}`;
      if (campaign && !new RegExp(`campaign\\s*${campaign}\\b`, 'i').test(text)) {
        return false;
      }
      const deliverables = mission.deliverables || {};
      const campaignArtifact = deliverables.campaign || {};
      return (
        Array.isArray(deliverables.prospects) &&
        deliverables.prospects.length > 0
      ) || (
        Array.isArray(campaignArtifact.prospects) &&
        campaignArtifact.prospects.length > 0
      );
    });
  } catch {
    return false;
  }
}

function extractCampaignId(question) {
  const match = /\bcampaign\s+(\d+)\b/i.exec(String(question || ''));
  return match ? match[1] : null;
}

module.exports = {
  WorkspaceEngine,
  createWorkspaceEngine,
};
