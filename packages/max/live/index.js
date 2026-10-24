'use strict';

const {
  LIFECYCLE,
  LIFECYCLE_ORDER,
  SEVERITY,
  EVENT_TYPES,
  MATERIAL_EVENT_TYPES,
  ENTITY_KINDS,
  DEFAULT_CONFIDENCE_THRESHOLD,
  buildIntelligenceEvent,
  mapChangeTypeToEventType,
  lifecycleForEventType,
  encodeCursor,
  decodeCursor,
} = require('./LiveTypes');
const { EventStore } = require('./EventStore');
const { LifecycleTracker, isForwardLifecycle } = require('./LifecycleTracker');
const { diffCommandDeck } = require('./DeckDiff');
const { isMaterialEvent, toNotifications } = require('./MaterialFilter');
const { buildAwareness } = require('./AwarenessBuilder');
const {
  LiveLoopEngine,
  createLiveLoopEngine,
} = require('./LiveLoopEngine');

module.exports = {
  LIFECYCLE,
  LIFECYCLE_ORDER,
  SEVERITY,
  EVENT_TYPES,
  MATERIAL_EVENT_TYPES,
  ENTITY_KINDS,
  DEFAULT_CONFIDENCE_THRESHOLD,
  buildIntelligenceEvent,
  mapChangeTypeToEventType,
  lifecycleForEventType,
  encodeCursor,
  decodeCursor,
  EventStore,
  LifecycleTracker,
  isForwardLifecycle,
  diffCommandDeck,
  isMaterialEvent,
  toNotifications,
  buildAwareness,
  LiveLoopEngine,
  createLiveLoopEngine,
};
