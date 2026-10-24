const PROSPEO_THROTTLE_MS = 2100;

let lastProspeoCallAt = 0;
let prospeoSlotQueue = Promise.resolve();

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function reserveProspeoSlot() {
  const now = Date.now();
  const waitMs = Math.max(0, PROSPEO_THROTTLE_MS - (now - lastProspeoCallAt));
  if (waitMs > 0) {
    await sleep(waitMs);
  }
  lastProspeoCallAt = Date.now();
}

function awaitProspeoSlot() {
  const slot = prospeoSlotQueue.then(reserveProspeoSlot, reserveProspeoSlot);
  prospeoSlotQueue = slot.catch(() => {});
  return slot;
}

module.exports = {
  PROSPEO_THROTTLE_MS,
  awaitProspeoSlot,
};
