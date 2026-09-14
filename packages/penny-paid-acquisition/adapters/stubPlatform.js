'use strict';

/**
 * SPEC-252 — Explicit unavailable adapters for unsupported live reads.
 */

const { PLATFORM, unavailableEvidence, UNAVAILABLE_REASON } = require('../types');

function readChatGptAdsEvidence() {
  return unavailableEvidence(
    PLATFORM.CHATGPT_ADS,
    UNAVAILABLE_REASON.PLATFORM_ADAPTER_NOT_IMPLEMENTED
  );
}

function readYelpAdsEvidence() {
  return unavailableEvidence(
    PLATFORM.YELP,
    UNAVAILABLE_REASON.PLATFORM_ADAPTER_NOT_IMPLEMENTED
  );
}

module.exports = {
  readChatGptAdsEvidence,
  readYelpAdsEvidence,
};
