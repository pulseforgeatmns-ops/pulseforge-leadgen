'use strict';

/** Paige-owned social post channels that flow through canonical artifacts. */
const PAIGE_SOCIAL_PUBLISH_CHANNELS = Object.freeze([
  'blog',
  'google_business',
  'facebook_page',
  'linkedin_page',
  'linkedin_personal',
]);

function isPaigeSocialPublishChannel(channel) {
  return PAIGE_SOCIAL_PUBLISH_CHANNELS.includes(String(channel || '').trim());
}

module.exports = {
  PAIGE_SOCIAL_PUBLISH_CHANNELS,
  isPaigeSocialPublishChannel,
};
