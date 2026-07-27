'use strict';

const { BRIEFING_SECTIONS } = require('../BriefingTypes');

/**
 * Briefing template — always the same structured section order.
 * Future UI renders however it wants; this is domain shape only.
 */
function applyBriefingTemplate(sections) {
  const briefing = {
    summary: sections.summary,
    priorities: sections.priorities,
    changes: sections.changes,
    watchAlerts: sections.watchAlerts,
    risks: sections.risks,
    recommendations: sections.recommendations,
    metrics: sections.metrics,
  };

  // Ensure stable key order for deterministic serialization
  const ordered = {};
  for (const key of BRIEFING_SECTIONS) {
    ordered[key] = briefing[key];
  }
  ordered.meta = sections.meta || null;
  return ordered;
}

module.exports = {
  applyBriefingTemplate,
  BRIEFING_SECTIONS,
};
