'use strict';
const { parseReviewNavigation } = require('./ReviewNavigation');
function isContentReviewRequest(question) { return parseReviewNavigation(question)?.kind === 'content_review'; }
function contentReviewNavigation() { return { type: 'open_content_review', label: 'Review content', href: '/command-deck#content-review' }; }
module.exports = { isContentReviewRequest, contentReviewNavigation };
