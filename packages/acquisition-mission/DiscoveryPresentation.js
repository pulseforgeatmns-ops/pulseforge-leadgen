'use strict';

/**
 * SPEC-133 — Discovery Artifact Presentation.
 * Render directly from executionResult.discovery.payload — never reconstruct.
 */

/**
 * @param {unknown} item
 * @returns {string}
 */
function formatDiscoveryItem(item) {
  if (item == null) return '';
  if (typeof item === 'string') return item.trim();
  if (typeof item === 'number' || typeof item === 'boolean') return String(item);
  if (typeof item === 'object') {
    const row = /** @type {Record<string, unknown>} */ (item);
    if (row.name) return String(row.name);
    if (row.label) return String(row.label);
    if (row.signal) return String(row.signal);
    if (row.text) return String(row.text);
    if (row.value) return String(row.value);
    if (row.source) return String(row.source);
  }
  return String(item);
}

/**
 * @param {unknown[] | null | undefined} items
 * @returns {string[]}
 */
function normalizeDiscoveryItems(items) {
  if (!Array.isArray(items)) return [];
  return items.map(formatDiscoveryItem).filter(Boolean);
}

/**
 * Build ranked prospect rows from discovery payload without discarding fields.
 * @param {object} payload - executionResult.discovery.payload
 * @returns {object}
 */
function presentationFromDiscoveryPayload(payload = {}) {
  const companies = Array.isArray(payload.companies) ? payload.companies : [];
  const prospects = Array.isArray(payload.prospects) ? payload.prospects : [];
  const buyingSignals = normalizeDiscoveryItems(payload.buyingSignals);
  const evidence = normalizeDiscoveryItems(payload.evidence);
  const decisionMakers = normalizeDiscoveryItems(payload.decisionMakers);
  const confidence =
    payload.confidence != null && Number.isFinite(Number(payload.confidence))
      ? Number(payload.confidence)
      : null;
  const qualifiedCount =
    payload.qualifiedCount != null
      ? Number(payload.qualifiedCount)
      : companies.length || prospects.length;
  const summary =
    payload.summary != null && String(payload.summary).trim()
      ? String(payload.summary).trim()
      : null;

  const rankedProspects = [];
  for (let i = 0; i < companies.length; i += 1) {
    const company = companies[i];
    rankedProspects.push({
      rank: i + 1,
      name: formatDiscoveryItem(company) || `Prospect ${i + 1}`,
      id:
        company && typeof company === 'object' && company.id != null
          ? company.id
          : null,
      kind: 'company',
    });
  }
  if (!rankedProspects.length && prospects.length) {
    for (let i = 0; i < prospects.length; i += 1) {
      const person = prospects[i];
      rankedProspects.push({
        rank: i + 1,
        name: formatDiscoveryItem(person) || `Prospect ${i + 1}`,
        id:
          person && typeof person === 'object' && person.id != null
            ? person.id
            : null,
        title:
          person && typeof person === 'object' && person.title
            ? String(person.title)
            : null,
        kind: 'prospect',
      });
    }
  }

  return {
    companies,
    prospects,
    rankedProspects,
    buyingSignals,
    evidence,
    decisionMakers,
    confidence,
    qualifiedCount,
    summary,
    outcome: payload.outcome || null,
    blocked: Boolean(payload.blocked),
  };
}

/**
 * @param {ReturnType<typeof presentationFromDiscoveryPayload>} presentation
 * @returns {string[]}
 */
function formatDiscoveryResultsLines(presentation) {
  if (!presentation) return [];
  const lines = ['Scout Discovery', ''];
  const count = presentation.qualifiedCount || presentation.rankedProspects.length;
  lines.push(`Found ${count} prospect${count === 1 ? '' : 's'}`);
  lines.push('');

  if (presentation.rankedProspects.length) {
    for (const row of presentation.rankedProspects) {
      lines.push(`${row.rank}.`);
      lines.push(row.name);
      if (row.title) lines.push(`   Role: ${row.title}`);
      lines.push('');
    }
  } else if (count > 0) {
    lines.push('Prospects were qualified but no ranked list was returned.');
    lines.push('');
  }

  if (presentation.confidence != null) {
    lines.push('Confidence');
    lines.push('');
    lines.push(presentation.confidence.toFixed(2));
    lines.push('');
  }

  if (presentation.buyingSignals.length) {
    lines.push('Buying Signals');
    lines.push('');
    for (const signal of presentation.buyingSignals) {
      lines.push(`• ${signal}`);
    }
    lines.push('');
  }

  if (presentation.evidence.length) {
    lines.push('Supporting Evidence');
    lines.push('');
    for (const item of presentation.evidence) {
      lines.push(`• ${item}`);
    }
    lines.push('');
  }

  if (presentation.decisionMakers.length) {
    lines.push('Decision Makers');
    lines.push('');
    for (const maker of presentation.decisionMakers) {
      lines.push(`• ${maker}`);
    }
    lines.push('');
  }

  if (presentation.summary) {
    lines.push('Discovery Summary');
    lines.push('');
    lines.push(presentation.summary);
    lines.push('');
  }

  return lines;
}

/**
 * @param {object} payload - executionResult.discovery.payload
 * @returns {string}
 */
function formatDiscoveryResultsProse(payload) {
  return formatDiscoveryResultsLines(presentationFromDiscoveryPayload(payload))
    .join('\n')
    .trim();
}

/**
 * Find the latest Scout discovery contribution on a mission snapshot.
 * @param {object[]} contributions
 * @returns {object|null}
 */
function findLatestDiscoveryContribution(contributions = []) {
  return [...contributions]
    .reverse()
    .find((row) => row.specialist === 'scout' && row.kind === 'discovery') || null;
}

module.exports = {
  formatDiscoveryItem,
  presentationFromDiscoveryPayload,
  formatDiscoveryResultsLines,
  formatDiscoveryResultsProse,
  findLatestDiscoveryContribution,
};
