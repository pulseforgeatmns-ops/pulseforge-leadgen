'use strict';

const { KnowledgeWriter } = require('./KnowledgeWriter');
const {
  OPERATIONAL_EVENTS,
  FLIGHT_STAGES,
  FLIGHT_STAGE_ORDER,
  normalizeKnowledgeEvent,
} = require('./operationalEvents');
const {
  envelopeForCompany,
  envelopeForProspect,
  envelopeForTouchpoint,
  envelopeForOperationalEvent,
  operationalEventFromTouchpoint,
} = require('./envelopes');
const {
  ensureDualWriteSchema,
  recordFlightStage,
  getFlightJourney,
  listRecentFlights,
  stageLabel,
} = require('./flightRecorder');

module.exports = {
  KnowledgeWriter,
  OPERATIONAL_EVENTS,
  FLIGHT_STAGES,
  FLIGHT_STAGE_ORDER,
  normalizeKnowledgeEvent,
  envelopeForCompany,
  envelopeForProspect,
  envelopeForTouchpoint,
  envelopeForOperationalEvent,
  operationalEventFromTouchpoint,
  ensureDualWriteSchema,
  recordFlightStage,
  getFlightJourney,
  listRecentFlights,
  stageLabel,
};
