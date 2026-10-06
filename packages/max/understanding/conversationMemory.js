'use strict';

const { accountMatchesReference } = require('./accountResolution');
const { normalizeText } = require('../stateIngestion/claimParser');
const { SEMANTIC_TYPE } = require('./conversationMemoryTypes');

/**
 * Bounded conversational context for pronoun / entity reference resolution.
 * Not long-term memory — active thread only (plus optional durable hydration).
 */

class ConversationMemory {
  constructor({ conversationId = null, maxTurns = 12, durableLoadFailed = false } = {}) {
    this.conversationId = conversationId;
    this.maxTurns = maxTurns;
    this.turns = [];
    this.entitiesByKey = new Map();
    this.openQuestions = [];
    this.activeContacts = [];
    this.activeAccounts = [];
    this.durableLoadFailed = durableLoadFailed;
    this.durableReferenceResolved = false;
    this.pendingSpreadsheetWorkbook = null;
  }

  static fromSeed(seed = {}) {
    const mem = new ConversationMemory({ conversationId: seed.conversationId });
    for (const turn of seed.turns || []) {
      mem.recordTurn(turn);
    }
    if (seed.pendingSpreadsheetWorkbook) {
      mem.pendingSpreadsheetWorkbook = seed.pendingSpreadsheetWorkbook;
    }
    return mem;
  }

  static fromDurableRecords({ conversationId, records = [] }) {
    const mem = new ConversationMemory({ conversationId });
    const threadRecords = records
      .filter(r => r.semanticType === SEMANTIC_TYPE.RECENT_THREAD)
      .sort((a, b) => String(a.createdAt).localeCompare(String(b.createdAt)));
    for (const rec of threadRecords) {
      const turn = rec.payload?.turn;
      if (turn?.inputId) {
        mem.recordTurn({
          inputId: turn.inputId,
          text: turn.rawText || null,
          situationModel: turn,
        });
      }
    }

    mem.activeContacts = records
      .filter(r => r.semanticType === SEMANTIC_TYPE.ACTIVE_CONTACT && !r.supersededAt)
      .map(r => r.payload?.contact)
      .filter(Boolean);

    mem.activeAccounts = records
      .filter(r => r.semanticType === SEMANTIC_TYPE.ACTIVE_ENTITY && !r.supersededAt)
      .map(r => r.payload?.name)
      .filter(Boolean);

    mem.openQuestions = records
      .filter(r => r.semanticType === SEMANTIC_TYPE.OPEN_QUESTION && !r.supersededAt)
      .filter(r => !r.payload?.resolved)
      .map(r => ({
        id: r.id,
        recordFingerprint: r.recordFingerprint,
        ...r.payload,
      }));

    for (const contact of mem.activeContacts) {
      if (contact.name) mem.entitiesByKey.set(contact.name.toLowerCase(), contact);
    }
    return mem;
  }

  activeOpenQuestion() {
    return this.openQuestions[this.openQuestions.length - 1] || null;
  }

  resolveClarificationAnswer(text) {
    const pending = this.activeOpenQuestion();
    if (!pending?.ambiguity?.candidates?.length) return null;
    const raw = normalizeText(text).replace(/[.!?]+$/, '').trim();
    if (!raw || raw.split(/\s+/).length > 4) return null;

    for (const candidate of pending.ambiguity.candidates) {
      if (accountMatchesReference(raw, candidate)) {
        this.durableReferenceResolved = true;
        pending.resolved = true;
        return {
          account: candidate,
          openQuestion: pending,
          deferred: pending.deferred || null,
        };
      }
      if (String(candidate).toLowerCase().includes(raw.toLowerCase()) && raw.length >= 3) {
        this.durableReferenceResolved = true;
        pending.resolved = true;
        return {
          account: candidate,
          openQuestion: pending,
          deferred: pending.deferred || null,
        };
      }
    }
    return null;
  }

  knownAccountNames() {
    const names = new Set(this.activeAccounts || []);
    for (const turn of this.turns) {
      const model = turn.situationModel;
      for (const thread of model?.threads || [{ accountName: null, entities: model?.entities || [] }]) {
        if (thread.accountName) names.add(thread.accountName);
        for (const entity of thread.entities || []) {
          if (entity.kind === 'account' && entity.name) names.add(entity.name);
        }
      }
    }
    return [...names];
  }

  turnsSinceContactMention(contactName) {
    const key = String(contactName || '').toLowerCase();
    if (!key) return Infinity;
    for (let i = this.turns.length - 1; i >= 0; i -= 1) {
      const model = this.turns[i].situationModel;
      const pool = [
        ...(model?.entities || []),
        ...(model?.threads || []).flatMap(t => t.entities || []),
      ];
      if (pool.some(e => e.kind === 'contact' && e.name?.toLowerCase() === key)) {
        return this.turns.length - 1 - i;
      }
    }
    return Infinity;
  }

  unrelatedAccountTurnsSinceContact(contactName) {
    const key = String(contactName || '').toLowerCase();
    let foundContactTurn = false;
    let unrelated = 0;
    for (let i = this.turns.length - 1; i >= 0; i -= 1) {
      const model = this.turns[i].situationModel;
      const threads = model?.threads?.length ? model.threads : [{ entities: model?.entities || [], accountName: null }];
      const mentionsContact = threads.some(t =>
        (t.entities || []).some(e => e.kind === 'contact' && e.name?.toLowerCase() === key)
      );
      if (mentionsContact) {
        foundContactTurn = true;
        break;
      }
      const primaryAccount = threads.map(t => t.accountName || (t.entities || []).find(e => e.kind === 'account')?.name)
        .find(Boolean);
      if (primaryAccount) unrelated += 1;
    }
    return foundContactTurn ? unrelated : Infinity;
  }

  getPendingSpreadsheetWorkbook() {
    return this.pendingSpreadsheetWorkbook || null;
  }

  recordPendingSpreadsheetWorkbook(workbook = {}) {
    this.pendingSpreadsheetWorkbook = {
      ...workbook,
      recordedAt: workbook.recordedAt || new Date().toISOString(),
    };
  }

  clearPendingSpreadsheetWorkbook() {
    this.pendingSpreadsheetWorkbook = null;
  }

  recordTurn({ inputId, text, situationModel }) {
    this.turns.push({ inputId, text, situationModel, at: new Date().toISOString() });
    while (this.turns.length > this.maxTurns) this.turns.shift();

    for (const entity of situationModel?.entities || []) {
      if (entity.name) {
        this.entitiesByKey.set(entity.name.toLowerCase(), entity);
      }
      if (entity.id) this.entitiesByKey.set(entity.id, entity);
    }
    for (const thread of situationModel?.threads || []) {
      for (const entity of thread.entities || []) {
        if (entity.name) this.entitiesByKey.set(entity.name.toLowerCase(), entity);
      }
    }
  }

  recentContacts({ genderHint = null, accountName = null } = {}) {
    const contacts = [];
    for (const c of this.activeContacts || []) {
      if (accountName && c.accountName && c.accountName.toLowerCase() !== accountName.toLowerCase()) {
        continue;
      }
      contacts.push(c);
    }
    for (let i = this.turns.length - 1; i >= 0; i -= 1) {
      const model = this.turns[i].situationModel;
      const pool = [
        ...(model?.entities || []),
        ...(model?.threads || []).flatMap(t => t.entities || []),
      ].filter(e => e.kind === 'contact');
      for (const c of pool) {
        if (accountName && c.accountName && c.accountName.toLowerCase() !== accountName.toLowerCase()) {
          continue;
        }
        contacts.push(c);
      }
    }
    if (genderHint === 'male') {
      return contacts.filter(c => c.gender === 'male' || c.gender == null);
    }
    if (genderHint === 'female') {
      return contacts.filter(c => c.gender === 'female' || c.gender == null);
    }
    return contacts;
  }

  lastMentionedAccount() {
    for (let i = this.turns.length - 1; i >= 0; i -= 1) {
      const model = this.turns[i].situationModel;
      const threads = model?.threads?.length ? model.threads : [{ entities: model?.entities || [] }];
      for (const thread of threads) {
        const account = (thread.entities || []).find(e => e.kind === 'account');
        if (account) return account;
      }
    }
    return null;
  }

  toJSON() {
    return {
      conversationId: this.conversationId,
      maxTurns: this.maxTurns,
      turns: this.turns,
      pendingSpreadsheetWorkbook: this.pendingSpreadsheetWorkbook,
    };
  }

  lastPrimaryContactForAccount(accountName) {
    const key = String(accountName || '').toLowerCase();
    for (let i = this.turns.length - 1; i >= 0; i -= 1) {
      const model = this.turns[i].situationModel;
      for (const thread of model?.threads || [{ entities: model?.entities || [] }]) {
        const account = (thread.entities || []).find(e => e.kind === 'account' && e.name?.toLowerCase() === key);
        if (!account) continue;
        const contact = (thread.entities || []).find(e => e.kind === 'contact');
        if (contact) return contact;
      }
    }
    return null;
  }
}

module.exports = {
  ConversationMemory,
};
