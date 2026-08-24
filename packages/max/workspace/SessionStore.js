'use strict';

const crypto = require('crypto');
const {
  contextFingerprint,
  contextFocusLabel,
} = require('./ContextEnvelope');

/**
 * In-process session memory for Max Intelligence Workspace.
 * Non-durable across process restarts (SPEC-009 v1).
 */
class SessionStore {
  constructor() {
    /** @type {Map<string, object>} */
    this._sessions = new Map();
  }

  /**
   * @param {object} context - normalized MaxContext
   * @returns {object} session
   */
  create(context) {
    const sessionId = crypto.randomUUID
      ? crypto.randomUUID()
      : crypto.randomBytes(16).toString('hex');
    const session = {
      id: sessionId,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString(),
      context,
      fingerprint: contextFingerprint(context),
      messages: [],
      discussedEntities: new Set(),
      conversationMemory: null,
      activeWorkContext:
        context && context.activeWorkContext
          ? context.activeWorkContext
          : null,
    };
    rememberEntity(session, context);
    this._sessions.set(sessionId, session);
    return session;
  }

  /**
   * @param {string} sessionId
   * @returns {object|null}
   */
  get(sessionId) {
    if (!sessionId) return null;
    return this._sessions.get(String(sessionId)) || null;
  }

  /**
   * Apply a new context; return switch acknowledgement when focus changes.
   * @param {string} sessionId
   * @param {object} context - normalized
   * @returns {{ session: object, contextSwitch: string|null }}
   */
  switchContext(sessionId, context) {
    const session = this.get(sessionId);
    if (!session) {
      throw new Error('Unknown workspace session');
    }
    const nextFp = contextFingerprint(context);
    let contextSwitch = null;
    if (nextFp !== session.fingerprint) {
      const label = contextFocusLabel(context);
      contextSwitch = `We're now looking at ${label}. I'll use that as the current context.`;
    }
    const priorActive =
      session.activeWorkContext ||
      (session.context && session.context.activeWorkContext) ||
      null;
    const nextActive =
      (context && context.activeWorkContext) || priorActive || null;
    const preservedConversationalState =
      session.conversationalState ||
      (session.context && session.context.conversationalState) ||
      null;
    const preservedSessionState =
      session.sessionState ||
      (session.context && session.context.sessionState) ||
      null;
    session.context = context;
    if (preservedConversationalState) {
      session.conversationalState = preservedConversationalState;
      session.context.conversationalState = preservedConversationalState;
    }
    if (preservedSessionState) {
      session.sessionState = preservedSessionState;
      session.context.sessionState = preservedSessionState;
    }
    if (nextActive) {
      session.activeWorkContext = nextActive;
      session.context.activeWorkContext = nextActive;
    } else {
      session.activeWorkContext = null;
    }
    session.fingerprint = nextFp;
    session.updatedAt = new Date().toISOString();
    rememberEntity(session, context);
    return { session, contextSwitch };
  }

  /**
   * @param {string} sessionId
   * @param {{ role: string, text: string, structured?: object }} message
   */
  appendMessage(sessionId, message) {
    const session = this.get(sessionId);
    if (!session) throw new Error('Unknown workspace session');
    session.messages.push({
      role: String(message.role),
      text: String(message.text || ''),
      structured: message.structured || null,
      at: new Date().toISOString(),
    });
    session.updatedAt = new Date().toISOString();
    if (session.messages.length > 40) {
      session.messages = session.messages.slice(-40);
    }
    return session;
  }

  /** Test helper */
  clear() {
    this._sessions.clear();
  }

  get size() {
    return this._sessions.size;
  }
}

function rememberEntity(session, context) {
  if (context.selectedEntity && context.selectedEntity.name) {
    session.discussedEntities.add(context.selectedEntity.name);
  }
  if (context.companyId) session.discussedEntities.add(context.companyId);
  if (context.recommendationId) {
    session.discussedEntities.add(context.recommendationId);
  }
}

module.exports = {
  SessionStore,
};
