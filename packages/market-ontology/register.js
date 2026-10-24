'use strict';

const { registerDomainOntology } = require('@pulseforge/knowledge/ontology');
const { createMarketOntology } = require('./MarketOntology');

/**
 * Register the Market domain ontology with the global registry.
 *
 * @param {import('@pulseforge/knowledge/ontology').OntologyRegistry} [registry]
 */
function registerMarketOntology(registry) {
  const ontology = createMarketOntology();
  if (registry) {
    return registry.register(ontology);
  }
  return registerDomainOntology(ontology);
}

module.exports = {
  registerMarketOntology,
  createMarketOntology,
};
