'use strict';

/**
 * Bounded conversational context for pronoun / entity reference resolution.
 * Not long-term memory — active thread only.
 */

class ConversationMemory {
  constructor({ conversationId = null, maxTurns = 12 } = {}) {
    this.conversationId = conversationId;
    this.maxTurns = maxTurns;
    this.turns = [];
    this.entitiesByKey = new Map();
  }

  static fromSeed(seed = {}) {
    const mem = new ConversationMemory({ conversationId: seed.conversationId });
    for (const turn of seed.turns || []) {
      mem.recordTurn(turn);
    }
    return mem;
  }

  knownAccountNames() {
    const names = new Set();
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
