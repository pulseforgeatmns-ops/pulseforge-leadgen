'use strict';

const { createOperatorJsonFeedCollector } = require('./operatorJsonFeedCollector');

function createProductionCollectors(options = {}) {
  const collectors = [createOperatorJsonFeedCollector(options)];
  return collectors;
}

module.exports = {
  createProductionCollectors,
};
