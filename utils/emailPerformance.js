// Deprecated: email_events is the source of truth for email reporting.
// Keep these exports as no-ops so old imports do not recreate or mutate the
// corrupted email_performance counters.
async function ensureEmailPerformanceTable() {
  return null;
}

async function recordSend() {
  return null;
}

async function recordEvent() {
  return null;
}

module.exports = { ensureEmailPerformanceTable, recordSend, recordEvent };
