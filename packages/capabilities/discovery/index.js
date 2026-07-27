'use strict';

/**
 * Prospect Discovery + Discovery Profiles (SPEC-024).
 */

const types = require('./types');
const { seedDiscoveryProfiles, MANCHESTER_GEO } = require('./seedProfiles');
const {
  DiscoveryProfileStore,
  createDiscoveryProfileStore,
  bumpVersion,
} = require('./DiscoveryProfileStore');
const {
  ProfileSelector,
  createProfileSelector,
  inferClientId,
  scoreProfileMatch,
  synthesizeTemporaryProfile,
} = require('./ProfileSelector');
const { rankAgainstProfile } = require('./ranking');
const { verifyCandidate } = require('./verification');
const {
  dedupeCandidates,
  flagCrmDuplicates,
  normalizeName,
  normalizeWebsite,
} = require('./dedupe');
const { createPlacesProvider } = require('./providers/PlacesProvider');
const {
  createFixtureProvider,
  manchesterFixtureCandidates,
} = require('./providers/FixtureProvider');
const { createProspectDiscoveryCapability } = require('./ProspectDiscovery');
const {
  PostgresDiscoveryProfileStore,
  createPostgresDiscoveryProfileStore,
  ensureDiscoveryProfileSchema,
} = require('./PostgresDiscoveryProfileStore');

module.exports = {
  ...types,
  seedDiscoveryProfiles,
  MANCHESTER_GEO,
  DiscoveryProfileStore,
  createDiscoveryProfileStore,
  bumpVersion,
  ProfileSelector,
  createProfileSelector,
  inferClientId,
  scoreProfileMatch,
  synthesizeTemporaryProfile,
  rankAgainstProfile,
  verifyCandidate,
  dedupeCandidates,
  flagCrmDuplicates,
  normalizeName,
  normalizeWebsite,
  createPlacesProvider,
  createFixtureProvider,
  manchesterFixtureCandidates,
  createProspectDiscoveryCapability,
  PostgresDiscoveryProfileStore,
  createPostgresDiscoveryProfileStore,
  ensureDiscoveryProfileSchema,
};
