'use strict';

const { normalizeContext } = require('./ContextEnvelope');
const { buildOpeningState } = require('./OpeningStateBuilder');
const { buildSuggestions } = require('./SuggestionEngine');
const { SessionStore } = require('./SessionStore');
const { composeResponse } = require('./ResponseComposer');
const { composeMissionResponse } = require('./MissionResponse');
const { PresentationEngine } = require('./PresentationEngine');
const {
  routeIntent,
  ROUTE_KINDS,
  missionEnabled,
} = require('../../mission-engine');

/**
 * WorkspaceEngine — SPEC-009 + SPEC-022 routing.
 * open() / ask() over explicit MaxContext. Never invents intelligence.
 * Business objectives route to Mission Engine before Market Intelligence.
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
  }

  /** @returns {SessionStore} */
  get sessions() {
    return this._sessions;
  }

  /**
   * Open a workspace session from an explicit context envelope.
   * @param {object} rawContext
   * @param {{ hour?: number }} [options]
   */
  open(rawContext, options = {}) {
    const context = normalizeContext(rawContext);
    const session = this._sessions.create(context);
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
    };
  }

  /**
   * Continue or start a turn. Pass context to switch focus mid-session.
   * @param {object} input
   * @param {string} [input.sessionId]
   * @param {string} input.question
   * @param {object} [input.context] - optional new MaxContext
   * @param {object} [input.rawContext] - alias
   */
  async ask(input) {
    if (!input || !String(input.question || '').trim()) {
      throw new Error('question is required');
    }
    const question = String(input.question).trim().slice(0, 2000);
    const rawContext = input.context || input.rawContext || null;

    let session = input.sessionId
      ? this._sessions.get(input.sessionId)
      : null;
    let contextSwitch = null;

    if (!session) {
      if (!rawContext) {
        throw new Error('sessionId or context is required');
      }
      const opened = this.open(rawContext);
      session = this._sessions.get(opened.sessionId);
    } else if (rawContext) {
      const normalized = normalizeContext(rawContext);
      const switched = this._sessions.switchContext(session.id, normalized);
      contextSwitch = switched.contextSwitch;
      session = switched.session;
    }

    const context = session.context;
    this._sessions.appendMessage(session.id, {
      role: 'operator',
      text: question,
    });

    // SPEC-022: Mission Engine is the first routing layer for business objectives.
    // Market Intelligence handles intelligence-specific requests only.
    const route = routeIntent(question);
    let structured;
    let mission = null;

    if (
      this._missionsEnabled &&
      this._missionEngine &&
      route.kind === ROUTE_KINDS.MISSION
    ) {
      mission = await this._missionEngine.createFromObjective({
        objective: question,
        tenantId: context.tenantId,
        clientId: context.tenantId,
        createdBy: (session && session.operator) || null,
        missionType: route.missionType,
      });
      structured = composeMissionResponse({
        mission,
        question,
        card: this._missionEngine.toCard(mission),
      });
    } else {
      structured = composeResponse({
        context,
        question,
        session,
      });
    }

    const presented = await this._presentation.present(structured);

    let prose = presented.prose;
    if (contextSwitch) {
      prose = `${contextSwitch}\n\n${prose}`;
    }

    this._sessions.appendMessage(session.id, {
      role: 'max',
      text: prose,
      structured,
    });

    return {
      sessionId: session.id,
      prose,
      structured,
      metadata: presented.metadata,
      suggestions: structured.nextInvestigations,
      recommendedActions: structured.recommendedActions,
      contextSwitch,
      context,
      presentation: presented.presentation,
      route: route.kind,
      mission,
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
