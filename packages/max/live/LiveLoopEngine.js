'use strict';

const { deepFreeze } = require('../reasoning/ReasoningTypes');
const { EventStore } = require('./EventStore');
const { LifecycleTracker } = require('./LifecycleTracker');
const { diffCommandDeck } = require('./DeckDiff');
const { toNotifications, isMaterialEvent } = require('./MaterialFilter');
const { buildAwareness } = require('./AwarenessBuilder');
const {
  EVENT_TYPES,
  ENTITY_KINDS,
  SEVERITY,
  LIFECYCLE,
  encodeCursor,
  decodeCursor,
  mapChangeTypeToEventType,
  lifecycleForEventType,
  DEFAULT_CONFIDENCE_THRESHOLD,
} = require('./LiveTypes');

/**
 * LiveLoopEngine — SPEC-011 / ADR-006.
 * Observes deck + memory transitions; never scores or invents.
 */
class LiveLoopEngine {
  /**
   * @param {object} [options]
   * @param {EventStore} [options.store]
   * @param {LifecycleTracker} [options.lifecycle]
   * @param {number} [options.confidenceThreshold]
   */
  constructor(options = {}) {
    this._store = options.store || new EventStore();
    this._lifecycle = options.lifecycle || new LifecycleTracker();
    this._confidenceThreshold =
      options.confidenceThreshold != null
        ? Number(options.confidenceThreshold)
        : DEFAULT_CONFIDENCE_THRESHOLD;
    /** @type {Map<string, object>} last observed deck per tenant */
    this._lastDeck = new Map();
    /** @type {Map<string, object[]>} briefing evolution entries per tenant */
    this._briefingEvolution = new Map();
  }

  /** @returns {EventStore} */
  get store() {
    return this._store;
  }

  /** @returns {LifecycleTracker} */
  get lifecycle() {
    return this._lifecycle;
  }

  /**
   * Observe a freshly composed CommandDeckModel.
   * @param {object} input
   * @param {string} input.tenantId
   * @param {object} input.model
   * @param {boolean} [input.forceInitial=false]
   * @returns {{ events: object[], notifications: object[], evolution: object[], cursor: string, model: object }}
   */
  observeDeck(input) {
    if (!input || !input.tenantId) {
      throw new Error('observeDeck requires tenantId');
    }
    if (!input.model) {
      throw new Error('observeDeck requires model');
    }
    const tenantId = String(input.tenantId);
    const previous = input.forceInitial
      ? null
      : this._lastDeck.get(tenantId) || null;
    const partials = diffCommandDeck(previous, input.model, {
      tenantId,
      confidenceThreshold: this._confidenceThreshold,
    });

    const events = [];
    for (const partial of partials) {
      const event = this._store.append(partial);
      this._lifecycle.apply(event);
      events.push(event);
      if (event.type === EVENT_TYPES.BRIEFING_EVOLVED) {
        this._pushEvolution(tenantId, {
          at: event.timestamp,
          summary: event.summary,
          eventId: event.id,
          type: event.type,
        });
      }
    }

    this._lastDeck.set(tenantId, cloneShallowDeck(input.model));

    const notifications = toNotifications(events);
    const evolution = (this._briefingEvolution.get(tenantId) || []).slice();
    const cursor = this._store.cursor();

    const live = deepFreeze({
      cursor,
      evolution,
      notifications,
      eventCount: events.length,
      materialCount: notifications.length,
    });

    // Attach live envelope without mutating frozen model if already frozen —
    // return a new wrapper for callers that need it.
    return {
      events,
      notifications,
      evolution,
      cursor,
      live,
      model: input.model,
    };
  }

  /**
   * Ingest SPEC-003 change events for an entity.
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.entityId
   * @param {string} [input.entityKind]
   * @param {string} [input.entityLabel]
   * @param {object[]} input.changes - ChangeDetector output
   * @param {string} [input.timestamp]
   */
  observeChanges(input) {
    if (!input || !input.tenantId || !input.entityId) {
      throw new Error('observeChanges requires tenantId and entityId');
    }
    const tenantId = String(input.tenantId);
    const entityId = String(input.entityId);
    const timestamp = input.timestamp || new Date().toISOString();
    const events = [];

    for (const change of input.changes || []) {
      const type = mapChangeTypeToEventType(change.type);
      const lifecycle = lifecycleForEventType(type, null);
      const evidenceIds = [];
      if (change.claimId) evidenceIds.push(String(change.claimId));
      if (change.evidenceId) evidenceIds.push(String(change.evidenceId));
      if (change.details && change.details.evidenceId) {
        evidenceIds.push(String(change.details.evidenceId));
      }

      let material = undefined;
      let severity = SEVERITY.INFO;
      let summary = humanChangeSummary(type, change, input.entityLabel);

      if (type === EVENT_TYPES.EVIDENCE_CONTRADICTED) {
        material = true;
        severity = SEVERITY.HIGH;
      }
      if (
        type === EVENT_TYPES.CONFIDENCE_INCREASED &&
        change.confidenceAfter != null &&
        change.confidenceBefore != null &&
        Number(change.confidenceBefore) < this._confidenceThreshold &&
        Number(change.confidenceAfter) >= this._confidenceThreshold
      ) {
        // Promote to threshold-crossed material event
        const event = this._store.append({
          type: EVENT_TYPES.CONFIDENCE_THRESHOLD_CROSSED,
          entity: {
            kind: input.entityKind || ENTITY_KINDS.RECOMMENDATION,
            id: entityId,
            label: input.entityLabel || entityId,
          },
          severity: SEVERITY.HIGH,
          timestamp,
          summary: `Confidence crossed ${Math.round(
            this._confidenceThreshold * 100
          )}% · ${input.entityLabel || entityId}`,
          tenantId,
          lifecycle: LIFECYCLE.STRENGTHENED,
          material: true,
          relatedEvidence: evidenceIds,
          payload: {
            confidenceBefore: change.confidenceBefore,
            confidenceAfter: change.confidenceAfter,
          },
        });
        this._lifecycle.apply(event);
        events.push(event);
        continue;
      }

      const event = this._store.append({
        type,
        entity: {
          kind: input.entityKind || ENTITY_KINDS.RECOMMENDATION,
          id: entityId,
          label: input.entityLabel || entityId,
        },
        severity,
        timestamp,
        summary,
        tenantId,
        lifecycle,
        material,
        relatedEvidence: evidenceIds,
        payload: change.details || null,
      });
      this._lifecycle.apply(event);
      events.push(event);
    }

    return {
      events,
      notifications: toNotifications(events),
      cursor: this._store.cursor(),
    };
  }

  /**
   * Soft-poll payload since cursor.
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string|number} [input.since]
   * @param {boolean} [input.includeDeck=false]
   * @param {boolean} [input.materialOnly=false]
   */
  liveSince(input) {
    if (!input || !input.tenantId) {
      throw new Error('liveSince requires tenantId');
    }
    const tenantId = String(input.tenantId);
    const after = decodeCursor(input.since);
    const events = this._store.since(tenantId, after, {
      materialOnly: input.materialOnly === true,
      limit: 200,
    });
    const notifications = toNotifications(events);
    const evolution = (this._briefingEvolution.get(tenantId) || []).slice();
    const affectedEntityIds = [
      ...new Set(events.map((e) => String(e.entity.id))),
    ];

    return deepFreeze({
      tenantId,
      since: encodeCursor(after),
      cursor: this._store.cursor(),
      events,
      notifications,
      evolution,
      affectedEntityIds,
      hasUpdates: events.length > 0,
      material: notifications.length > 0,
      deck: input.includeDeck ? this._lastDeck.get(tenantId) || null : null,
    });
  }

  /**
   * Entity timeline mixing events + lifecycle history.
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} input.entityId
   * @param {string} [input.kind]
   * @param {number} [input.limit]
   */
  timeline(input) {
    if (!input || !input.tenantId || !input.entityId) {
      throw new Error('timeline requires tenantId and entityId');
    }
    const events = this._store.timeline(
      String(input.tenantId),
      String(input.entityId),
      { kind: input.kind, limit: input.limit || 50 }
    );
    const lifecycle = this._lifecycle.history(
      String(input.tenantId),
      String(input.entityId)
    );
    return deepFreeze({
      entityId: String(input.entityId),
      tenantId: String(input.tenantId),
      lifecycle: this._lifecycle.get(
        String(input.tenantId),
        String(input.entityId)
      ),
      events,
      transitions: lifecycle,
    });
  }

  /**
   * Max awareness for an active session.
   * @param {object} input
   * @param {string} input.tenantId
   * @param {string} [input.since] - cursor or ISO time; prefer session createdAt via sinceSeq
   * @param {number} [input.sinceSeq]
   * @param {string} [input.entityId]
   * @param {string} [input.entityLabel]
   * @param {string} [input.openedAt]
   */
  awareness(input) {
    if (!input || !input.tenantId) {
      throw new Error('awareness requires tenantId');
    }
    const tenantId = String(input.tenantId);
    let after = 0;
    if (input.sinceSeq != null) after = Number(input.sinceSeq) || 0;
    else if (input.since) after = decodeCursor(input.since);

    let events = this._store.since(tenantId, after, { limit: 100 });
    if (input.entityId) {
      const eid = String(input.entityId);
      events = events.filter((e) => String(e.entity.id) === eid);
    }
    // Also filter by openedAt wall clock when provided
    if (input.openedAt) {
      const opened = Date.parse(input.openedAt);
      if (Number.isFinite(opened)) {
        events = events.filter((e) => Date.parse(e.timestamp) >= opened);
      }
    }

    const awareness = buildAwareness({
      events,
      openedAt: input.openedAt,
      entityLabel: input.entityLabel,
    });

    return deepFreeze({
      ...awareness,
      events,
      material: events.filter(isMaterialEvent),
      cursor: this._store.cursor(),
    });
  }

  /**
   * Attach live envelope onto a compose result (non-mutating for frozen models).
   * @param {object} model
   * @param {object} liveResult - observeDeck return
   */
  withLive(model, liveResult) {
    return {
      ...model,
      live: liveResult.live,
    };
  }

  /** Test helper */
  clear() {
    this._store.clear();
    this._lifecycle.clear();
    this._lastDeck.clear();
    this._briefingEvolution.clear();
  }

  _pushEvolution(tenantId, entry) {
    const list = this._briefingEvolution.get(tenantId) || [];
    list.push(entry);
    if (list.length > 40) list.splice(0, list.length - 40);
    this._briefingEvolution.set(tenantId, list);
  }
}

function cloneShallowDeck(model) {
  return {
    morningBrief: model.morningBrief
      ? { ...model.morningBrief }
      : null,
    highestLeverageAction: model.highestLeverageAction
      ? { ...model.highestLeverageAction }
      : null,
    watchAlerts: (model.watchAlerts || []).map((w) => ({ ...w })),
    marketTrends: (model.marketTrends || []).map((t) => ({ ...t })),
    priorityQueue: (model.priorityQueue || []).map((p) => ({ ...p })),
    meta: model.meta ? { ...model.meta } : null,
  };
}

function humanChangeSummary(type, change, label) {
  const name = label || 'Recommendation';
  switch (type) {
    case EVENT_TYPES.NEW_HIRING_SIGNAL:
      return `New hiring signal · ${name}`;
    case EVENT_TYPES.NEW_EVIDENCE:
      return `Supporting evidence added · ${name}`;
    case EVENT_TYPES.EVIDENCE_CONTRADICTED:
      return `Evidence contradicted · ${name}`;
    case EVENT_TYPES.CONFIDENCE_INCREASED:
      return `Confidence +${change.magnitude != null ? change.magnitude : ''} · ${name}`.replace(
        /\s+/g,
        ' '
      );
    case EVENT_TYPES.CONFIDENCE_DECREASED:
      return `Confidence decreased · ${name}`;
    case EVENT_TYPES.SCORE_INCREASED:
      return `Score increased · ${name}`;
    case EVENT_TYPES.SCORE_DECREASED:
      return `Score decreased · ${name}`;
    default:
      return `${type} · ${name}`;
  }
}

/**
 * @param {object} [options]
 */
function createLiveLoopEngine(options = {}) {
  return new LiveLoopEngine(options);
}

module.exports = {
  LiveLoopEngine,
  createLiveLoopEngine,
};
