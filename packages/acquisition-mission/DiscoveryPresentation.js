'use strict';

/**
 * SPEC-133 — Discovery Artifact Presentation.
 * Render directly from executionResult.discovery.payload — never reconstruct.
 */

const { formatSignalLabel, sourceLabel } = require('./DiscoveryPayload');

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

function formatBuyingSignalLine(signal) {
  if (signal == null) return '';
  if (typeof signal === 'string') return signal.trim();
  if (typeof signal !== 'object') return String(signal);
  const parts = [];
  if (signal.label) parts.push(String(signal.label));
  else if (signal.type) parts.push(formatSignalLabel(signal));
  if (signal.company) parts.push(`@ ${signal.company}`);
  if (signal.source) parts.push(`(${signal.source})`);
  return parts.join(' ').trim();
}

function formatEvidenceLine(item) {
  if (item == null) return '';
  if (typeof item === 'string') return item.trim();
  if (typeof item !== 'object') return String(item);
  const parts = [];
  if (item.label) parts.push(String(item.label));
  if (item.source) parts.push(`— ${item.source}`);
  if (item.company) parts.push(`(${item.company})`);
  return parts.join(' ').trim();
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
  const buyingSignalsRaw = Array.isArray(payload.buyingSignals) ? payload.buyingSignals : [];
  const buyingSignals = buyingSignalsRaw.map(formatBuyingSignalLine).filter(Boolean);
  const evidenceRaw = Array.isArray(payload.evidence) ? payload.evidence : [];
  const evidence = evidenceRaw.map(formatEvidenceLine).filter(Boolean);
  const decisionMakers = normalizeDiscoveryItems(payload.decisionMakers);
  const confidence =
    payload.confidence != null && Number.isFinite(Number(payload.confidence))
      ? Number(payload.confidence)
      : null;
  const confidenceBreakdown =
    payload.confidenceBreakdown && typeof payload.confidenceBreakdown === 'object'
      ? payload.confidenceBreakdown
      : null;
  const qualifiedCount =
    payload.qualifiedCount != null
      ? Number(payload.qualifiedCount)
      : companies.length || prospects.length;
  const summary =
    payload.summary != null && String(payload.summary).trim()
      ? String(payload.summary).trim()
      : null;
  const missionObjective =
    payload.missionObjective != null && String(payload.missionObjective).trim()
      ? String(payload.missionObjective).trim()
      : null;
  const coverage =
    payload.coverage && typeof payload.coverage === 'object' ? payload.coverage : null;
  const discoveryReport =
    payload.discoveryReport && typeof payload.discoveryReport === 'object'
      ? payload.discoveryReport
      : null;
  const discoveryStatus =
    payload.discoveryStatus != null ? String(payload.discoveryStatus) : null;
  const candidateUniverseCount =
    payload.candidateUniverseCount != null
      ? Number(payload.candidateUniverseCount)
      : discoveryReport && discoveryReport.candidateUniverse != null
        ? Number(discoveryReport.candidateUniverse)
        : null;

  let rankedProspects = Array.isArray(payload.rankedProspects)
    ? payload.rankedProspects.map((row) => ({ ...row }))
    : [];

  if (!rankedProspects.length) {
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
    buyingSignalsRaw,
    evidence,
    evidenceRaw,
    decisionMakers,
    confidence,
    confidenceBreakdown,
    qualifiedCount,
    summary,
    missionObjective,
    outcome: payload.outcome || null,
    blocked: Boolean(payload.blocked),
    coverage,
    discoveryReport,
    discoveryStatus,
    candidateUniverseCount,
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

  if (presentation.missionObjective) {
    lines.push('Mission Objective');
    lines.push('');
    lines.push(presentation.missionObjective);
    lines.push('');
  }

  if (presentation.summary) {
    lines.push('Discovery Summary');
    lines.push('');
    lines.push(presentation.summary);
    lines.push('');
  }

  lines.push(`Found ${count} prospect${count === 1 ? '' : 's'}`);
  lines.push('');

  if (presentation.rankedProspects.length) {
    lines.push('Ranked Prospects');
    lines.push('');
    for (const row of presentation.rankedProspects) {
      lines.push(`${row.rank}. ${row.name}`);
      if (row.rationale) lines.push(`   Why: ${row.rationale}`);
      if (row.intelligenceBrief && row.intelligenceBrief.whyRankedHere) {
        lines.push(`   Because: ${row.intelligenceBrief.whyRankedHere}`);
      }
      if (row.confidenceExplanation) {
        lines.push(`   Confidence: ${row.confidenceExplanation.score}`);
        if (row.confidenceExplanation.basedOn && row.confidenceExplanation.basedOn.length) {
          lines.push(
            `   Based on: ${row.confidenceExplanation.basedOn.map((b) => b.label).join(', ')}`
          );
        }
        if (row.confidenceExplanation.missing && row.confidenceExplanation.missing.length) {
          lines.push(
            `   Missing: ${row.confidenceExplanation.missing
              .slice(0, 3)
              .map((m) => m.label)
              .join(', ')}`
          );
        }
        if (row.confidenceExplanation.contradictionNote) {
          lines.push(`   ${row.confidenceExplanation.contradictionNote}`);
        }
      } else if (row.confidence != null) {
        lines.push(`   Confidence: ${Number(row.confidence).toFixed(2)}`);
      }
      if (row.trust) {
        lines.push(`   Trust: ${row.trust.label} — ${row.trust.reason}`);
      }
      if (row.fit != null) lines.push(`   Fit: ${Number(row.fit).toFixed(2)}`);
      if (row.timing != null) lines.push(`   Timing: ${Number(row.timing).toFixed(2)}`);
      if (row.title) lines.push(`   Role: ${row.title}`);
      if (row.intelligenceBrief && row.intelligenceBrief.competingHypotheses?.length) {
        lines.push('   Competing hypotheses:');
        for (const hyp of row.intelligenceBrief.competingHypotheses.slice(0, 3)) {
          lines.push(`     • ${hyp.text} (${hyp.confidence})`);
        }
      }
      if (row.highestRemainingUnknowns && row.highestRemainingUnknowns.length) {
        const top = row.highestRemainingUnknowns[0];
        lines.push(`   Highest unknown: ${top.unknown} (${top.impact} impact)`);
        lines.push(`   Verify via: ${top.howToVerify}`);
      } else if (row.unknowns && row.unknowns.length) {
        lines.push(`   Unknowns: ${row.unknowns.slice(0, 2).join('; ')}`);
      }
      if (row.recommendedNextInvestigation && row.recommendedNextInvestigation.action) {
        lines.push(`   Next verification: ${row.recommendedNextInvestigation.action}`);
      }
      lines.push('');
    }
  } else if (count > 0) {
    lines.push('Prospects were qualified but no ranked list was returned.');
    lines.push('');
  }

  if (presentation.confidenceBreakdown) {
    const cb = presentation.confidenceBreakdown;
    lines.push('Confidence');
    lines.push('');
    lines.push(`Overall: ${cb.overall != null ? cb.overall.toFixed(2) : '—'}`);
    lines.push(`  Discovery: ${cb.discovery != null ? cb.discovery.toFixed(2) : '—'}`);
    lines.push(`  Evidence: ${cb.evidence != null ? cb.evidence.toFixed(2) : '—'}`);
    lines.push(`  Market: ${cb.market != null ? cb.market.toFixed(2) : '—'}`);
    lines.push(`  Fit: ${cb.fit != null ? cb.fit.toFixed(2) : '—'}`);
    lines.push(`  Completeness: ${cb.completeness != null ? cb.completeness.toFixed(2) : '—'}`);
    if (cb.missingEvidence && cb.missingEvidence.length) {
      lines.push('');
      lines.push('Missing Evidence');
      for (const item of cb.missingEvidence) lines.push(`• ${item}`);
    }
    if (cb.unknowns && cb.unknowns.length) {
      lines.push('');
      lines.push('Unknowns');
      for (const item of cb.unknowns.slice(0, 3)) lines.push(`• ${item}`);
    }
    lines.push('');
  } else if (presentation.confidence != null) {
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

  if (presentation.coverage || presentation.discoveryReport) {
    lines.push('Coverage');
    lines.push('');
    const report = presentation.discoveryReport || {};
    const cov = presentation.coverage || {};
    const citiesLine =
      report.coverage && report.coverage.cities
        ? report.coverage.cities
        : cov.cities
          ? `${cov.cities.searched}/${cov.cities.planned}`
          : '—';
    const conceptsLine =
      report.coverage && report.coverage.concepts
        ? report.coverage.concepts
        : cov.concepts
          ? `${cov.concepts.searched}/${cov.concepts.planned}`
          : '—';
    const sourcesLine =
      report.coverage && report.coverage.sources
        ? report.coverage.sources
        : cov.sources
          ? `${cov.sources.searched}/${cov.sources.planned}`
          : '—';
    lines.push(`Cities searched: ${citiesLine}`);
    lines.push(`Concepts: ${conceptsLine}`);
    lines.push(`Sources: ${sourcesLine}`);
    if (presentation.candidateUniverseCount != null) {
      lines.push(`Candidate Universe: ${presentation.candidateUniverseCount}`);
    }
    lines.push(`Qualified: ${presentation.qualifiedCount || 0}`);
    if (presentation.confidence != null) {
      lines.push(`Confidence: ${Number(presentation.confidence).toFixed(2)}`);
    }
    if (presentation.discoveryStatus === 'incomplete') {
      lines.push('');
      lines.push('Coverage Warning');
      const warnings = (report.warnings || cov.warnings || []).slice(0, 3);
      for (const warning of warnings) lines.push(`• ${warning}`);
    }
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
  formatBuyingSignalLine,
  formatEvidenceLine,
};
