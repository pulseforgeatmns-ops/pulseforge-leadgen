'use strict';

/**
 * TEST ONLY — must never feed production prospective cohort.
 */
function createProceduralTestCollector(options = {}) {
  const id = 'procedural-test';
  let queue = [...(options.observations || [])];

  async function health() {
    return { available: true, testOnly: true };
  }

  async function poll() {
    const batch = queue;
    queue = [];
    return batch.map(row => ({
      ...row,
      provenance: {
        ...(row.provenance || {}),
        dataClass: 'PROCEDURAL',
        collectorId: id,
        testOnly: true,
      },
    }));
  }

  function enqueue(observations) {
    queue.push(...observations);
  }

  return { id, poll, health, enqueue };
}

module.exports = {
  createProceduralTestCollector,
};
