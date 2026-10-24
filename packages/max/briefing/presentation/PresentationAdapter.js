'use strict';

/**
 * Presentation Adapter — extension point between Briefing Engine and UI.
 *
 * Briefing Engine produces domain objects only.
 * Adapters may render those objects for a surface (CLI, dashboard, assistant)
 * without changing briefing assembly logic.
 *
 *   Briefing Engine → Presentation Adapter → Operator surface
 */

class PresentationAdapter {
  /**
   * @param {object} briefing - structured Briefing domain object
   * @returns {object}
   */
  present(briefing) {
    return {
      format: 'structured',
      briefing,
    };
  }
}

/**
 * Identity / structured adapter — default. No formatting.
 */
class StructuredPresentationAdapter extends PresentationAdapter {
  present(briefing) {
    return {
      format: 'structured',
      briefing,
    };
  }
}

/**
 * Lightweight markdown adapter for CLI / review — still evidence-backed fields only.
 * Does not invent narrative; prints structured fields as markdown lists.
 */
class MarkdownPresentationAdapter extends PresentationAdapter {
  present(briefing) {
    const lines = [];
    const s = briefing.summary || {};
    lines.push('# Max Briefing');
    lines.push('');
    lines.push('## Summary');
    lines.push(`- Period: ${s.period}`);
    lines.push(`- As of: ${s.asOf}`);
    lines.push(`- Companies monitored: ${s.companiesMonitored}`);
    lines.push(`- Priority opportunities: ${s.priorityOpportunities}`);
    lines.push(`- New decision makers: ${s.newDecisionMakers}`);
    lines.push(`- Watch alerts: ${s.watchAlertsTriggered}`);
    lines.push('');

    lines.push('## Priority Queue');
    for (const p of briefing.priorities || []) {
      lines.push(
        `- [#${p.rank}] ${p.companyName || p.companyId} score=${p.score} confidence=${p.confidence} trend=${p.trend} action=${p.recommendedAction}`
      );
    }
    lines.push('');

    lines.push('## Changes');
    lines.push(`- Total: ${(briefing.changes && briefing.changes.total) || 0}`);
    for (const h of (briefing.changes && briefing.changes.highlights) || []) {
      lines.push(`- ${h.companyName || h.companyId}: ${h.summary}`);
    }
    lines.push('');

    lines.push('## Watch Alerts');
    for (const a of (briefing.watchAlerts && briefing.watchAlerts.items) || []) {
      lines.push(`- ${a.message}`);
    }
    lines.push('');

    lines.push('## Risks');
    for (const r of (briefing.risks && briefing.risks.items) || []) {
      lines.push(
        `- ${r.companyName || r.companyId}: ${r.kind} severity=${r.severity}`
      );
    }
    lines.push('');

    lines.push('## Recommendations');
    for (const r of (briefing.recommendations && briefing.recommendations.items) || []) {
      lines.push(
        `- [#${r.rank}] ${r.companyName || r.companyId}: ${r.recommendedAction} (${r.type})`
      );
    }
    lines.push('');

    lines.push('## Metrics');
    const m = briefing.metrics || {};
    lines.push(`- Build time ms: ${m.buildTimeMs}`);
    lines.push(`- Query count: ${m.queryCount}`);
    lines.push(`- Recommendation count: ${m.recommendationCount}`);
    lines.push(`- Memory lookups: ${m.memoryLookups}`);
    lines.push(`- Strategy count: ${m.strategyCount}`);

    return {
      format: 'markdown',
      body: lines.join('\n'),
      briefing,
    };
  }
}

/**
 * @param {string} [format='structured']
 * @returns {PresentationAdapter}
 */
function createPresentationAdapter(format = 'structured') {
  const f = String(format || 'structured').toLowerCase();
  if (f === 'markdown' || f === 'md') return new MarkdownPresentationAdapter();
  return new StructuredPresentationAdapter();
}

module.exports = {
  PresentationAdapter,
  StructuredPresentationAdapter,
  MarkdownPresentationAdapter,
  createPresentationAdapter,
};
