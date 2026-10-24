'use strict';

const fs = require('fs');
const path = require('path');

const { STAGES, STAGE_SYMBOLS } = require('./CompetencyLifecycle');
const { listCompetencies, CATEGORIES } = require('./CompetencyRegistry');

const RECORD_PATH = path.join(__dirname, 'training-record.json');

function loadRecordOverrides() {
  try {
    const raw = fs.readFileSync(RECORD_PATH, 'utf8');
    return JSON.parse(raw);
  } catch (error) {
    if (error.code === 'ENOENT') return { competencies: {}, exercises: [] };
    throw error;
  }
}

function mergeCompetency(base, override = {}) {
  return {
    ...base,
    stage: override.stage || base.stage,
    graduatedAt: override.graduatedAt || base.graduatedAt || null,
    regressionAt: override.regressionAt || null,
    notes: override.notes || null,
  };
}

function buildTrainingRecord(options = {}) {
  const overrides = options.overrides || loadRecordOverrides();
  const competencyOverrides = overrides.competencies || {};
  const merged = listCompetencies().map(c => mergeCompetency(c, competencyOverrides[c.id]));

  const byCategory = {};
  for (const [key, label] of Object.entries({
    [CATEGORIES.CORE_MANAGEMENT]: 'Core Management',
    [CATEGORIES.ARBITRATION]: 'Arbitration',
    [CATEGORIES.PLANNING]: 'Planning',
    [CATEGORIES.ECONOMICS]: 'Economics',
  })) {
    byCategory[key] = {
      label,
      competencies: merged.filter(c => c.category === key).map(formatCompetencySummary),
    };
  }

  const graduated = merged
    .filter(c => c.stage === STAGES.GRADUATED)
    .sort((a, b) => String(b.graduatedAt || '').localeCompare(String(a.graduatedAt || '')))
    .map(c => ({
      id: c.id,
      label: c.label,
      stage: c.stage,
      graduatedAt: c.graduatedAt,
      specRefs: c.specRefs,
    }));

  return {
    spec: 'SPEC-102F',
    generatedAt: new Date().toISOString(),
    summary: {
      total: merged.length,
      graduated: merged.filter(c => c.stage === STAGES.GRADUATED).length,
      training: merged.filter(c => c.stage === STAGES.TRAINING).length,
      practicing: merged.filter(c => c.stage === STAGES.PRACTICING).length,
      notStarted: merged.filter(c => c.stage === STAGES.NOT_STARTED).length,
      regression: merged.filter(c => c.stage === STAGES.REGRESSION).length,
    },
    categories: byCategory,
    trainingRecord: graduated,
    competencies: merged,
    exerciseLog: overrides.exercises || [],
  };
}

function formatCompetencySummary(competency) {
  return {
    symbol: STAGE_SYMBOLS[competency.stage] || '?',
    id: competency.id,
    label: competency.label,
    stage: competency.stage,
    graduatedAt: competency.graduatedAt || null,
    specRefs: competency.specRefs,
  };
}

function formatTrainingRecordText(record = buildTrainingRecord()) {
  const lines = ['Training Record', ''];
  for (const entry of record.trainingRecord) {
    lines.push(entry.label);
    lines.push(`Graduated`);
    if (entry.graduatedAt) lines.push(entry.graduatedAt);
    lines.push('');
  }
  const training = record.competencies.filter(c => c.stage === STAGES.TRAINING);
  for (const entry of training) {
    lines.push(entry.label);
    lines.push('Training');
    lines.push('');
  }
  const notStarted = record.competencies.filter(c => c.stage === STAGES.NOT_STARTED);
  for (const entry of notStarted) {
    lines.push(entry.label);
    lines.push('Not Started');
    lines.push('');
  }
  return lines.join('\n').trimEnd();
}

module.exports = {
  RECORD_PATH,
  loadRecordOverrides,
  buildTrainingRecord,
  formatTrainingRecordText,
  formatCompetencySummary,
};
