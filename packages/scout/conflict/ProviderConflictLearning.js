'use strict';

/**
 * SPEC-146 — Provider conflict learning.
 * Each provider accumulates freshness, authority, conflict rate, and resolution rate.
 */

const { evidenceWeight } = require('../credibility/EvidenceWeights');
const { evidenceAgeDays } = require('../credibility/EvidenceFreshness');

function normalizeKey(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
}

function createProviderConflictLearningStore(seed = {}) {
  const profiles = JSON.parse(JSON.stringify(seed));
  const history = [];

  function ensureProfile(providerId) {
    const key = normalizeKey(providerId);
    if (!profiles[key]) {
      profiles[key] = {
        providerId: key,
        authority: evidenceWeight(key),
        freshnessScore: 0.5,
        conflictRate: 0,
        resolutionRate: 0,
        correctionHistory: [],
        conflictCount: 0,
        resolutionCount: 0,
        totalObservations: 0,
      };
    }
    return profiles[key];
  }

  function recordConflictOutcome(providerId, outcome = {}) {
    const profile = ensureProfile(providerId);
    profile.conflictCount += 1;
    profile.totalObservations += 1;
    profile.conflictRate = Number(
      (profile.conflictCount / Math.max(1, profile.totalObservations)).toFixed(3)
    );

    if (outcome.resolved) {
      profile.resolutionCount += 1;
      profile.resolutionRate = Number(
        (profile.resolutionCount / Math.max(1, profile.conflictCount)).toFixed(3)
      );
    }

    if (outcome.observedAt) {
      const age = evidenceAgeDays(outcome.observedAt);
      if (age != null) {
        const freshnessSignal = age <= 30 ? 1 : age <= 90 ? 0.8 : age <= 365 ? 0.6 : 0.4;
        profile.freshnessScore = Number(
          (profile.freshnessScore * 0.85 + freshnessSignal * 0.15).toFixed(3)
        );
      }
    }

    if (outcome.wasCorrected) {
      profile.correctionHistory.push({
        at: outcome.at || new Date().toISOString(),
        subject: outcome.subject,
        previousValue: outcome.previousValue,
        correctedValue: outcome.correctedValue,
      });
      profile.correctionHistory = profile.correctionHistory.slice(-20);
    }

    history.push({
      providerId: normalizeKey(providerId),
      outcome,
      at: new Date().toISOString(),
    });

    return profile;
  }

  function recordResolution(conflict, winningProvider) {
    for (const claim of conflict.conflictingClaims || []) {
      recordConflictOutcome(claim.source, {
        resolved: conflict.resolution?.resolved,
        subject: conflict.subject,
        observedAt: claim.observedAt,
        wasCorrected: winningProvider && normalizeKey(claim.source) !== normalizeKey(winningProvider),
        previousValue: claim.value,
        correctedValue: winningProvider ? claim.value : null,
      });
    }
  }

  function getProviderProfile(providerId) {
    return ensureProfile(providerId);
  }

  function adjustAuthorityWeight(providerId, subject) {
    const profile = ensureProfile(providerId);
    const base = profile.authority;
    const resolutionBoost = profile.resolutionRate * 0.05;
    const conflictPenalty = profile.conflictRate * 0.08;
    const freshnessBoost = (profile.freshnessScore - 0.5) * 0.1;
    return Number(Math.min(1, Math.max(0.2, base + resolutionBoost - conflictPenalty + freshnessBoost)).toFixed(3));
  }

  function summarize() {
    return {
      profiles: { ...profiles },
      historyCount: history.length,
      topReliable: Object.values(profiles)
        .sort((a, b) => b.resolutionRate - a.resolutionRate)
        .slice(0, 5)
        .map((p) => ({
          provider: p.providerId,
          resolutionRate: p.resolutionRate,
          freshnessScore: p.freshnessScore,
        })),
      staleProviders: Object.values(profiles)
        .filter((p) => p.freshnessScore < 0.5 && p.conflictCount >= 2)
        .map((p) => ({
          provider: p.providerId,
          freshnessScore: p.freshnessScore,
          note: 'Historically stale relative to peers — future investigations weight fresher sources higher.',
        })),
    };
  }

  function exportForMemory() {
    return profiles;
  }

  return {
    profiles,
    recordConflictOutcome,
    recordResolution,
    getProviderProfile,
    adjustAuthorityWeight,
    summarize,
    exportForMemory,
  };
}

function loadConflictLearningFromMemory(memory = {}) {
  const investigation = memory.investigation || {};
  return createProviderConflictLearningStore(investigation.providerConflictLearning || {});
}

function exportConflictLearningForMemory(store) {
  return store.exportForMemory();
}

module.exports = {
  createProviderConflictLearningStore,
  loadConflictLearningFromMemory,
  exportConflictLearningForMemory,
};
