'use strict';

/**
 * SPEC-112 — Acquisition Intelligence Model service facade.
 */

const {
  createMemoryAimStore,
  buildFedirAim,
  qualifyProspect,
  briefPaige,
  assessPilot,
  qualifyAndBrief,
  FEDIR_CLIENT_KEY,
} = require('../packages/aim');

let memoryStore = null;

function getAimStore(opts = {}) {
  if (opts.store) return opts.store;
  if (!memoryStore) memoryStore = createMemoryAimStore();
  return memoryStore;
}

function getAim(clientKey, opts = {}) {
  return getAimStore(opts).getAim(clientKey || FEDIR_CLIENT_KEY);
}

function listAims(opts = {}) {
  return getAimStore(opts).listAims();
}

function qualify(clientKey, prospect, opts = {}) {
  const store = getAimStore(opts);
  const aim = store.getAim(clientKey);
  if (!aim) {
    const err = new Error(`AIM not found for clientKey "${clientKey}"`);
    err.code = 'aim_not_found';
    throw err;
  }
  const { qualification, briefing } = qualifyAndBrief(aim, prospect, opts);
  store.putQualification(qualification);
  return { aim, qualification, briefing };
}

function pilotStatus(clientKey, opts = {}) {
  const store = getAimStore(opts);
  const aim = store.getAim(clientKey);
  if (!aim) {
    const err = new Error(`AIM not found for clientKey "${clientKey}"`);
    err.code = 'aim_not_found';
    throw err;
  }
  return assessPilot({
    aim,
    qualifications: store.listQualifications(clientKey),
    outcomes: opts.outcomes || {},
  });
}

module.exports = {
  FEDIR_CLIENT_KEY,
  getAimStore,
  getAim,
  listAims,
  qualify,
  pilotStatus,
  buildFedirAim,
  qualifyProspect,
  briefPaige,
  assessPilot,
};
