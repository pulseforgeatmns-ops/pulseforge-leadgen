'use strict';

const types = require('./types');
const limits = require('./limits');
const telemetry = require('./telemetry');
const adapters = require('./adapters');
const submitTurn = require('./submitTurn');
const attachmentStore = require('./attachmentStore');
const preview = require('./preview');
const attachmentIntent = require('./attachmentIntent');
const spreadsheetTurn = require('./spreadsheetTurn');

module.exports = {
  ...types,
  ...limits,
  ...telemetry,
  ...adapters,
  ...submitTurn,
  ...attachmentStore,
  ...preview,
  ...attachmentIntent,
  ...spreadsheetTurn,
};
