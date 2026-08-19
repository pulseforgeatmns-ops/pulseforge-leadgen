'use strict';

/**
 * SPEC-124 — Acquisition objective detection (shared, dependency-free).
 */

const ACQUISITION_OBJECTIVE_RE =
  /\b(?:(?:i|we)\s+(?:want|need|would like|are trying|are looking)\s+to\s+(?:acquire|get|win|land|sign|close|add)|(?:our|my)\s+(?:goal|objective|target|aim)\s+(?:is|are)\s+to\s+(?:acquire|get|win|land|sign|close|add)|help\s+(?:me|us)\s+(?:acquire|get|win|land|sign|close)|(?:acquire|get|win|land|sign|close)\s+(?:one|a|an|\d+)\s+(?:new\s+)?(?:recurring\s+)?(?:commercial\s+)?(?:cleaning\s+)?(?:client|customer|account)s?)\b/i;

const ACQUISITION_OBJECTIVE_LEAD_RE =
  /^acquire\s+(?:one|a|an|\d+|\w+)/i;

const ADVISORY_NOT_OBJECTIVE_RE =
  /\b(?:what should we|where should we|recommend|why are we|why is|how is|who are our ideal|what do you think|explain|describe)\b/i;

const SCOUT_MARKET_RE =
  /\b(?:find(?:\s+\w+){0,6}\s+(?:opportunit|worth pursuing|like)|where should we (?:be )?look|what(?:'s| has) changed in (?:our )?(?:target )?market)\b/i;

const EXECUTION_RE =
  /\b(launch|execute|send|publish|mail|start)\b.{0,40}\b(campaign|emails?|post|outreach|sequence)\b/;

function normalizeObjectiveText(text) {
  return String(text || '')
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * @param {string} question
 * @returns {boolean}
 */
function detectAcquisitionObjective(question) {
  const q = normalizeObjectiveText(question);
  if (!q) return false;
  if (EXECUTION_RE.test(q)) return false;
  if (SCOUT_MARKET_RE.test(q)) return false;
  if (ADVISORY_NOT_OBJECTIVE_RE.test(q)) return false;
  if (ACQUISITION_OBJECTIVE_LEAD_RE.test(q)) return true;
  return ACQUISITION_OBJECTIVE_RE.test(q);
}

module.exports = {
  detectAcquisitionObjective,
  normalizeObjectiveText,
};
