'use strict';

/**
 * SPEC-252 / SPEC-253 — Penny paid acquisition platform evidence package.
 * SPEC-255 — First-party paid attribution evidence retrieval.
 */

const types = require('./types');
const accountResolution = require('./accountResolution');
const collector = require('./PaidPlatformEvidenceCollector');
const firstPartyAttribution = require('./FirstPartyAttributionEvidence');
const googleAds = require('./adapters/googleAds');
const metaAds = require('./adapters/metaAds');
const chatgptAds = require('./adapters/chatgptAds');
const stubPlatform = require('./adapters/stubPlatform');

module.exports = {
  ...types,
  ...accountResolution,
  ...collector,
  ...firstPartyAttribution,
  googleAds,
  metaAds,
  chatgptAds,
  stubPlatform,
  readGoogleAdsEvidence: googleAds.readGoogleAdsEvidence,
  readMetaAdsEvidence: metaAds.readMetaAdsEvidence,
  readChatGptAdsEvidence: chatgptAds.readChatGptAdsEvidence,
  assessChatGptAdsProductionReadiness: chatgptAds.assessChatGptAdsProductionReadiness,
};
