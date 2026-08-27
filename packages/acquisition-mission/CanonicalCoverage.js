'use strict';

/**
 * SPEC-192 — Canonical Coverage Contract.
 *
 * Coverage is engine-agnostic. Every discovery engine normalizes into the same
 * Coverage shape; presentation renders via renderCoverage() only.
 * Unavailable metrics are null — never undefined.
 */

function emptySearchedMetric() {
  return { searched: null, planned: null, ratio: null };
}

function emptySatisfiedMetric() {
  return { satisfied: null, planned: null, ratio: null };
}

function emptyExecutedMetric() {
  return { executed: null, planned: null, ratio: null };
}

function parseReportFraction(value) {
  if (typeof value !== 'string' || !value.includes('/')) return null;
  const [leftRaw, rightRaw] = value.split('/');
  const searched = Number(String(leftRaw || '').trim());
  const planned = Number(String(rightRaw || '').trim());
  if (!Number.isFinite(searched) || !Number.isFinite(planned)) return null;
  return {
    searched,
    planned,
    ratio: planned > 0 ? searched / planned : null,
  };
}

function normalizeSearchedMetric(raw, reportValue) {
  if (raw && typeof raw === 'object') {
    const searched =
      raw.searched != null
        ? Number(raw.searched)
        : raw.addressed != null
          ? Number(raw.addressed)
          : null;
    const planned = raw.planned != null ? Number(raw.planned) : null;
    const ratio =
      raw.ratio != null
        ? Number(raw.ratio)
        : searched != null && planned != null && planned > 0
          ? searched / planned
          : null;
    return {
      searched: Number.isFinite(searched) ? searched : null,
      planned: Number.isFinite(planned) ? planned : null,
      ratio: Number.isFinite(ratio) ? ratio : null,
    };
  }
  const parsed = parseReportFraction(reportValue);
  return parsed || emptySearchedMetric();
}

function normalizeSatisfiedMetric(raw) {
  if (raw && typeof raw === 'object') {
    const satisfied = raw.satisfied != null ? Number(raw.satisfied) : null;
    const planned = raw.planned != null ? Number(raw.planned) : null;
    const ratio =
      raw.ratio != null
        ? Number(raw.ratio)
        : satisfied != null && planned != null && planned > 0
          ? satisfied / planned
          : null;
    return {
      satisfied: Number.isFinite(satisfied) ? satisfied : null,
      planned: Number.isFinite(planned) ? planned : null,
      ratio: Number.isFinite(ratio) ? ratio : null,
    };
  }
  return emptySatisfiedMetric();
}

function normalizeExecutedMetric(raw) {
  if (raw && typeof raw === 'object') {
    const executed = raw.executed != null ? Number(raw.executed) : null;
    const planned = raw.planned != null ? Number(raw.planned) : null;
    const ratio =
      raw.ratio != null
        ? Number(raw.ratio)
        : executed != null && planned != null && planned > 0
          ? executed / planned
          : null;
    return {
      executed: Number.isFinite(executed) ? executed : null,
      planned: Number.isFinite(planned) ? planned : null,
      ratio: Number.isFinite(ratio) ? ratio : null,
    };
  }
  return emptyExecutedMetric();
}

function normalizeNullableNumber(value) {
  if (value == null) return null;
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

/**
 * Normalize any discovery engine coverage payload into the canonical contract.
 * @param {object} input
 * @returns {object}
 */
function normalizeCoverage(input = {}) {
  const coverage = input.coverage && typeof input.coverage === 'object' ? input.coverage : input;
  const report =
    input.discoveryReport && typeof input.discoveryReport === 'object'
      ? input.discoveryReport
      : {};
  const reportCoverage =
    report.coverage && typeof report.coverage === 'object' ? report.coverage : {};

  const warnings = Array.isArray(coverage.warnings)
    ? coverage.warnings.map((row) => String(row)).filter(Boolean)
    : Array.isArray(report.warnings)
      ? report.warnings.map((row) => String(row)).filter(Boolean)
      : [];

  const complete =
    coverage.complete != null
      ? Boolean(coverage.complete)
      : report.status != null
        ? report.status === 'complete'
        : null;

  return {
    cities: normalizeSearchedMetric(coverage.cities, reportCoverage.cities),
    concepts: normalizeSearchedMetric(coverage.concepts, reportCoverage.concepts),
    sources: normalizeSearchedMetric(coverage.sources, reportCoverage.sources),
    evidenceRequirements: normalizeSatisfiedMetric(coverage.evidenceRequirements),
    tasks: normalizeExecutedMetric(coverage.tasks),
    candidateUniverse:
      normalizeNullableNumber(input.candidateUniverseCount) ??
      normalizeNullableNumber(report.candidateUniverse) ??
      normalizeNullableNumber(coverage.candidateUniverse),
    qualified:
      normalizeNullableNumber(input.qualifiedCount) ??
      normalizeNullableNumber(report.qualified) ??
      normalizeNullableNumber(coverage.qualified),
    confidence:
      normalizeNullableNumber(input.confidence) ??
      normalizeNullableNumber(report.confidence) ??
      normalizeNullableNumber(coverage.confidence),
    warnings,
    complete,
  };
}

function formatFractionLine(label, left, planned) {
  if (left == null && planned == null) return null;
  if (left != null && planned != null) return `${label}: ${left}/${planned}`;
  if (left != null) return `${label}: ${left}`;
  return null;
}

/**
 * Render canonical coverage metrics for presentation surfaces.
 * @param {object} coverage - output of normalizeCoverage()
 * @param {object} [opts]
 * @returns {string[]}
 */
function renderCoverage(coverage, opts = {}) {
  if (!coverage || typeof coverage !== 'object') return [];

  const lines = [];
  const citiesLine = formatFractionLine(
    'Cities searched',
    coverage.cities && coverage.cities.searched,
    coverage.cities && coverage.cities.planned
  );
  if (citiesLine) lines.push(citiesLine);

  const conceptsLine = formatFractionLine(
    'Concepts',
    coverage.concepts && coverage.concepts.searched,
    coverage.concepts && coverage.concepts.planned
  );
  if (conceptsLine) lines.push(conceptsLine);

  const sourcesLine = formatFractionLine(
    'Sources',
    coverage.sources && coverage.sources.searched,
    coverage.sources && coverage.sources.planned
  );
  if (sourcesLine) lines.push(sourcesLine);

  const evidenceLine = formatFractionLine(
    'Evidence requirements',
    coverage.evidenceRequirements && coverage.evidenceRequirements.satisfied,
    coverage.evidenceRequirements && coverage.evidenceRequirements.planned
  );
  if (evidenceLine) lines.push(evidenceLine);

  const tasksLine = formatFractionLine(
    'Investigation tasks',
    coverage.tasks && coverage.tasks.executed,
    coverage.tasks && coverage.tasks.planned
  );
  if (tasksLine) lines.push(tasksLine);

  if (coverage.candidateUniverse != null) {
    lines.push(`Candidate Universe: ${coverage.candidateUniverse}`);
  }

  if (coverage.qualified != null) {
    lines.push(`Qualified: ${coverage.qualified}`);
  } else if (opts.qualifiedCount != null) {
    lines.push(`Qualified: ${opts.qualifiedCount}`);
  }

  if (coverage.confidence != null) {
    lines.push(`Confidence: ${Number(coverage.confidence).toFixed(2)}`);
  }

  if (coverage.complete === false || opts.discoveryStatus === 'incomplete') {
    lines.push('');
    lines.push('Coverage Warning');
    const warnings = Array.isArray(coverage.warnings) ? coverage.warnings.slice(0, 3) : [];
    for (const warning of warnings) lines.push(`• ${warning}`);
  }

  return lines;
}

module.exports = {
  normalizeCoverage,
  renderCoverage,
  emptySearchedMetric,
  emptySatisfiedMetric,
  emptyExecutedMetric,
};
