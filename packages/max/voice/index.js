'use strict';

const types = require('./types');
const transcriptionAdapter = require('./transcriptionAdapter');
const recordingStore = require('./recordingStore');
const voiceIngestion = require('./voiceIngestion');
const telemetry = require('./telemetry');

module.exports = {
  ...types,
  ...transcriptionAdapter,
  ...recordingStore,
  ...voiceIngestion,
  ...telemetry,
};
