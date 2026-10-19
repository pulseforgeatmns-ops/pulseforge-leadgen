'use strict';

// Navigation to existing drafts is read-only. An active mission must not own it.
function isContentReviewRequest(question) {
  const q = String(question || '').replace(/[’]/g, "'").replace(/\s+/g, ' ').trim();
  if (!q || /\b(?:write|generate|create|draft|publish|schedule|send)\s+(?:me\s+|a\s+|the\s+|new\s+|these\s+|those\s+|all\s+)*(?:content|posts?|drafts?|campaign|it|them)\b/i.test(q)) return false;
  if (/^(?:approve|approved|reject|proceed|continue|execute)\b/i.test(q)) return false;
  const content = /\b(?:content|posts?|drafts?|social|paige)\b/i.test(q);
  const queue = /\b(?:pending|awaiting|waiting|approval|to approve|ready for review|review queue)\b/i.test(q);
  const read = /\b(?:show|open|review|see|check|preview|what|which|does|do|any|is|are)\b/i.test(q);
  return content && read && (queue || /^(?:(?:please|let me)\s+)?(?:open|show(?: me)?|review|preview)\s+(?:the\s+|my\s+|our\s+)?(?:content(?: review)?|social(?: posts?)?|drafts?|posts?)[.!?]?$/i.test(q));
}

function contentReviewNavigation() {
  return {
    type: 'open_content_review',
    label: 'Review content',
    href: '/command-deck#content-review',
  };
}

module.exports = { isContentReviewRequest, contentReviewNavigation };
