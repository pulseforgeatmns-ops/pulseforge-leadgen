'use strict';

const { COG_101 } = require('./COG-101-identity');
const { COG_102 } = require('./COG-102-conversation-continuity');
const { COG_103 } = require('./COG-103-assumption-extraction');
const { COG_104 } = require('./COG-104-counterfactual-reasoning');
const { COG_105 } = require('./COG-105-self-revision');
const { COG_106 } = require('./COG-106-competing-evidence');
const { COG_107 } = require('./COG-107-abstraction');
const { COG_108 } = require('./COG-108-unknowns-confidence');
const { COG_109 } = require('./COG-109-long-conversation');
const { COG_110 } = require('./COG-110-reasoning-graph');

const BUILTIN_DOMAINS = Object.freeze({
  'COG-101': COG_101,
  'COG-102': COG_102,
  'COG-103': COG_103,
  'COG-104': COG_104,
  'COG-105': COG_105,
  'COG-106': COG_106,
  'COG-107': COG_107,
  'COG-108': COG_108,
  'COG-109': COG_109,
  'COG-110': COG_110,
});

/** @type {Record<string, import('../types').CognitiveDomain>} */
const DOMAIN_MAP = { ...BUILTIN_DOMAINS };

function listDomains() {
  return Object.values(DOMAIN_MAP).map(d => ({ ...d }));
}

function getDomain(domainId) {
  const found = DOMAIN_MAP[domainId];
  return found ? { ...found } : null;
}

function registerDomain(domain) {
  if (!domain?.id) throw new Error('Domain requires id');
  if (DOMAIN_MAP[domain.id]) {
    throw new Error(`Domain ${domain.id} already registered`);
  }
  DOMAIN_MAP[domain.id] = Object.freeze(domain);
  return { ...domain };
}

function listDomainIds() {
  return Object.keys(DOMAIN_MAP);
}

module.exports = {
  BUILTIN_DOMAINS,
  DOMAIN_MAP,
  listDomains,
  getDomain,
  registerDomain,
  listDomainIds,
};
