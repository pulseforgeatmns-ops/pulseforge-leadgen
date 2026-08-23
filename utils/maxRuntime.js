'use strict';

/**
 * Shared Max runtime for HTTP surfaces (Command Deck + Max Workspace).
 * SPEC-014: boots persistent Knowledge + dual-write when enabled.
 * SPEC-022: attaches Mission Engine for business-objective routing.
 */

const { getKnowledgeBoot } = require('./knowledgeRuntime');
const { getMissionEngine, missionEnabled } = require('./missionRuntime');
const {
  bootAcquisitionMissionRuntime,
  assertSingleRuntime,
} = require('../services/acquisitionMissionRuntime');

let runtimePromise = null;

/**
 * @param {object} [options]
 * @param {boolean} [options.reset=false] - force a fresh runtime (tests)
 * @param {boolean} [options.disableLlm] - force deterministic presentation
 * @param {boolean} [options.inMemory] - force in-memory knowledge (tests)
 */
function getMaxRuntime(options = {}) {
  if (options.reset) {
    runtimePromise = null;
  }
  if (!runtimePromise) {
    runtimePromise = (async () => {
      let missionEngine = options.missionEngine || null;
      if (!missionEngine && missionEnabled() && options.missionsEnabled !== false) {
        try {
          missionEngine = await getMissionEngine({
            reset: options.reset,
            inMemory: options.inMemory,
            pool: options.pool,
          });
        } catch (err) {
          console.error('[maxRuntime] mission engine boot failed:', err.message);
        }
      }

      const boot = await getKnowledgeBoot({
        reset: options.reset,
        disableLlm: options.disableLlm,
        inMemory: options.inMemory,
        tenantPolicies: options.tenantPolicies,
        pool: options.pool,
        missionEngine,
        missionsEnabled: options.missionsEnabled,
      });

      const max = boot.max;
      if (missionEngine) {
        max.missionEngine = missionEngine;
        // Re-bind workspace if boot path did not receive engine (memory fallback)
        if (max.workspace && !max.workspace._missionEngine) {
          max.workspace._missionEngine = missionEngine;
          max.workspace._missionsEnabled = missionEnabled();
        }
      }

      if (max.workspace) {
        max.workspace._loadOperatorContext = true;
        const pool = options.pool || (() => {
          try {
            return require('../db');
          } catch (_) {
            return null;
          }
        })();

        if (!max.workspace._operatorContextOpts) {
          max.workspace._operatorContextOpts = {
            pool,
            missionEngine: missionEngine || null,
          };
        }

        const amoRuntime = bootAcquisitionMissionRuntime({
          pool,
          persist: options.inMemory !== true,
        });
        assertSingleRuntime();
        max.workspace._runtimeProvider = () => amoRuntime;
        max.acquisitionMissionRuntime = amoRuntime;
      }
      return max;
    })();
  }
  return runtimePromise;
}

module.exports = {
  getMaxRuntime,
};
