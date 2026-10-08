'use strict';

/**
 * SPEC-PAIGE-ANCHOR-SOCIAL-002 — reusable organic social planning types.
 * Anchor doctrine/config sits on these canonical capabilities.
 */

const ANCHOR_CLIENT_ID = 10;

const CONTENT_CATEGORIES = Object.freeze([
  'work_and_results',
  'useful_expertise',
  'operator_perspective',
  'social_proof',
  'anchor_progress',
  'local_relevance',
]);

const CONTENT_FORMATS = Object.freeze([
  'text_only',
  'single_photo',
  'multi_photo',
  'before_after',
  'carousel',
  'short_video',
  'customer_story',
  'designed_graphic',
]);

const BACKLOG_APPROVAL_STATES = Object.freeze({
  DRAFT: 'draft',
  PENDING_ARTIFACT: 'pending_artifact',
  PENDING_APPROVAL: 'pending_approval',
  APPROVED: 'approved',
  REJECTED: 'rejected',
  PUBLISHED: 'published',
});

const PLATFORM_CHANNELS = Object.freeze({
  facebook_page: 'facebook_page',
  linkedin_page: 'linkedin_page',
  google_business: 'google_business',
});

const DEFAULT_WEEKLY_TARGET = 3;
const DEFAULT_BACKLOG_MIN = 6;
const EXPLORATION_RATE = 0.18;

/** Reasonable priors when Anchor history is thin — not permanent defaults. */
const SCHEDULING_PRIORS = Object.freeze({
  facebook_page: [
    { dow: 2, hour: 8, minute: 30, weight: 1.0 },
    { dow: 4, hour: 12, minute: 0, weight: 0.9 },
    { dow: 1, hour: 17, minute: 15, weight: 0.85 },
  ],
  linkedin_page: [
    { dow: 2, hour: 8, minute: 15, weight: 1.0 },
    { dow: 3, hour: 7, minute: 45, weight: 0.95 },
    { dow: 1, hour: 11, minute: 30, weight: 0.8 },
  ],
  google_business: [
    { dow: 1, hour: 9, minute: 45, weight: 0.9 },
    { dow: 3, hour: 10, minute: 30, weight: 0.85 },
    { dow: 5, hour: 8, minute: 0, weight: 0.8 },
  ],
});

function mapPlatformToOutcomeChannel(platform) {
  return {
    facebook_page: 'facebook',
    linkedin_page: 'linkedin',
    google_business: 'gbp',
  }[platform] || 'other';
}

function categoryToLegacyContentType(category) {
  const map = {
    work_and_results: 'behind-the-scenes',
    useful_expertise: 'educational',
    operator_perspective: 'behind-the-scenes',
    social_proof: 'community',
    anchor_progress: 'promotional',
    local_relevance: 'community',
  };
  return map[category] || 'educational';
}

module.exports = {
  ANCHOR_CLIENT_ID,
  CONTENT_CATEGORIES,
  CONTENT_FORMATS,
  BACKLOG_APPROVAL_STATES,
  PLATFORM_CHANNELS,
  DEFAULT_WEEKLY_TARGET,
  DEFAULT_BACKLOG_MIN,
  EXPLORATION_RATE,
  SCHEDULING_PRIORS,
  mapPlatformToOutcomeChannel,
  categoryToLegacyContentType,
};
