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
  constructor({now = Date.now, ttlMs = 2 * 60 * 60 * 1000, maxSessions = 1024} = {}) {
    this._now = now;
    this._ttlMs = ttlMs;
    this._maxSessions = maxSessions;
    this._lastAccess = new Map();
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
    this._prune();
    if (this._sessions.size >= this._maxSessions) {
      const oldest = [...this._lastAccess].sort((a,b) => a[1]-b[1])[0]?.[0];
      this._sessions.delete(oldest); this._lastAccess.delete(oldest);
    }
    this._sessions.set(sessionId, session);
    this._lastAccess.set(sessionId, this._now());
    return session;
  }

  /** Restore a durable referent-only session under its original opaque id. */
  restore(sessionId, context) {
    const id = String(sessionId || '');
    if (!id || id.length > 160 || !/^[a-zA-Z0-9_.:-]+$/.test(id)) throw new Error('Invalid workspace session');
    const existing = this.get(id);
    if (existing) {
      if (String(existing.context?.tenantId) !== String(context?.tenantId)) throw new Error('Workspace session tenant mismatch');
      return existing;
    }
    const createdAt = new Date().toISOString();
    const session = {
      id, createdAt, updatedAt: createdAt, context,
      fingerprint: contextFingerprint(context), messages: [], discussedEntities: new Set(),
      conversationMemory: null, activeWorkContext: context?.activeWorkContext || null,
    };
    rememberEntity(session, context);
    this._prune();
    if (this._sessions.size >= this._maxSessions) {
      const oldest = [...this._lastAccess].sort((a,b) => a[1]-b[1])[0]?.[0];
      this._sessions.delete(oldest); this._lastAccess.delete(oldest);
    }
    this._sessions.set(id, session); this._lastAccess.set(id, this._now());
    return session;
  }

  /**
   * @param {string} sessionId
   * @returns {object|null}
   */
  get(sessionId) {
    if (!sessionId) return null;
    const id = String(sessionId);
    if (this._now() - (this._lastAccess.get(id) ?? -Infinity) >= this._ttlMs) {
      this._sessions.delete(id); this._lastAccess.delete(id); return null;
    }
    const session = this._sessions.get(id) || null;
    if (session) this._lastAccess.set(id, this._now());
    return session;
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
    const priorCtx =
      session.context && typeof session.context === 'object' ? session.context : {};
    const nextCtx = { ...context };
    for (const key of ['missionId', 'acquisitionMissionId', 'acquisitionOwner']) {
      if (nextCtx[key] == null && priorCtx[key] != null) {
        nextCtx[key] = priorCtx[key];
      }
    }
    session.context = nextCtx;
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

  _prune() {
    for (const [id, at] of this._lastAccess) if (this._now()-at >= this._ttlMs) {
      this._sessions.delete(id); this._lastAccess.delete(id);
    }
  }

  /** Test helper */
  clear() {
    this._sessions.clear();
    this._lastAccess.clear();
  }

  get size() {
    this._prune();
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
