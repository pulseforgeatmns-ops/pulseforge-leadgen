'use strict';

/**
 * SPEC-158 — Terminology Learning.
 * Future missions remember which market language performed better.
 */

const { asText } = require('../../max/scoutAcquisition/Types');

function normalizeGeography(value) {
  return asText(value).toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
}

function buildLearningKey(geography, terminology) {
  return `${normalizeGeography(geography)}::${asText(terminology).toLowerCase()}`;
}

/**
 * Record terminology performance from a completed investigation.
 * @param {Map|object} store
 * @param {object} entry
 */
function recordTerminologyPerformance(store, entry = {}) {
  const geography = asText(entry.geography);
  const terminology = asText(entry.terminology);
  if (!geography || !terminology) return store;

  const key = buildLearningKey(geography, terminology);
  const map = store instanceof Map ? store : new Map(Object.entries(store || {}));
  const existing = map.get(key) || {
    geography,
    terminology,
    missions: 0,
    totalResults: 0,
    performance: 0,
  };

  const resultCount = Number(entry.resultCount) || 0;
  existing.missions += 1;
  existing.totalResults += resultCount;
  existing.performance = Number((existing.totalResults / existing.missions).toFixed(2));
  existing.lastUsedAt = new Date().toISOString();
  if (entry.confidence != null) {
    existing.lastConfidence = Number(entry.confidence);
  }
  existing.reason =
    entry.reason ||
    `"${terminology}" in ${geography}: avg ${existing.performance} results over ${existing.missions} missions`;

  map.set(key, existing);
  return map;
}

/**
 * Rank terminology by historical performance for a geography.
 * @param {Map|object} store
 * @param {string} geography
 * @param {string[]} [candidates]
 * @returns {object[]}
 */
function rankTerminologyForGeography(store, geography, candidates = []) {
  const map = store instanceof Map ? store : new Map(Object.entries(store || {}));
  const geoNorm = normalizeGeography(geography);
  const rows = [];

  for (const row of map.values()) {
    if (normalizeGeography(row.geography) !== geoNorm) continue;
    if (candidates.length && !candidates.some((c) => c.toLowerCase() === row.terminology.toLowerCase())) {
      continue;
    }
    rows.push(row);
  }

  return rows.sort((a, b) => (b.performance || 0) - (a.performance || 0));
}

/**
 * Export learning store to plain object for persistence.
 * @param {Map} store
 * @returns {object}
 */
function exportTerminologyLearning(store) {
  if (!(store instanceof Map)) return store || {};
  return Object.fromEntries(store.entries());
}

/**
 * Import learning store from persisted object.
 * @param {object} snapshot
 * @returns {Map}
 */
function importTerminologyLearning(snapshot = {}) {
  return new Map(Object.entries(snapshot));
}

/**
 * Apply terminology learning to reorder market definition terminology.
 * @param {object} marketDefinition
 * @param {Map|object} store
 * @returns {object}
 */
function applyTerminologyLearning(marketDefinition = {}, store = {}) {
  const ranked = rankTerminologyForGeography(store, marketDefinition.geography);
  if (!ranked.length) return marketDefinition;

  const learnedFirst = ranked.map((r) => r.terminology);
  const remainder = (marketDefinition.terminology || []).filter(
    (t) => !learnedFirst.some((l) => l.toLowerCase() === t.toLowerCase())
  );

  return {
    ...marketDefinition,
    terminology: [...learnedFirst, ...remainder],
    terminologyLearningApplied: true,
    terminologyLearning: ranked.slice(0, 5),
  };
}

module.exports = {
  recordTerminologyPerformance,
  rankTerminologyForGeography,
  exportTerminologyLearning,
  importTerminologyLearning,
  applyTerminologyLearning,
  buildLearningKey,
};
