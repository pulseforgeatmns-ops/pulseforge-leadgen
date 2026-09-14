'use strict';

/**
 * SPEC-252 — Penny paid acquisition platform evidence package.
 */

const types = require('./types');
const accountResolution = require('./accountResolution');
const collector = require('./PaidPlatformEvidenceCollector');
const googleAds = require('./adapters/googleAds');
const metaAds = require('./adapters/metaAds');
const stubPlatform = require('./adapters/stubPlatform');

module.exports = {
  ...types,
  ...accountResolution,
  ...collector,
  googleAds,
  metaAds,
  stubPlatform,
  readGoogleAdsEvidence: googleAds.readGoogleAdsEvidence,
  readMetaAdsEvidence: metaAds.readMetaAdsEvidence,
};
