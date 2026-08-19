'use strict';

/**
 * SPEC-100 — Max ↔ Scout acquisition intelligence (service facade).
 * SPEC-118 — Scout discovery attaches to the acquisition mission when missionId is present.
 */

const scoutAcquisition = require('../packages/max/scoutAcquisition');
const { attachScoutDiscovery } = require('./acquisitionMission');

async function runAcquisitionIntelligenceLoop(input = {}, opts = {}) {
  const result = await scoutAcquisition.runAcquisitionIntelligenceLoop(input, opts);
  if (result && (input.missionId || opts.missionId)) {
    await attachScoutDiscovery(input, result.result || result, opts);
  }
  return result;
}

module.exports = {
  ...scoutAcquisition,
  runAcquisitionIntelligenceLoop,
};
