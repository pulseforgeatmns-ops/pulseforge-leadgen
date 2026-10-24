'use strict';

const PROVENANCE = Object.freeze([
  'live',
  'historical_backfill',
  'manual_recalculation',
  'daily_decay',
  'manual_override',
  'synthetic_smoke',
]);
const PROVENANCE_SET = new Set(PROVENANCE);

function signalProvenance(signal = {}) {
  const explicit = signal.metadata?.provenance;
  if (explicit != null) {
    const value = String(explicit);
    if (!PROVENANCE_SET.has(value)) throw new Error(`Unsupported Max event provenance: ${value}`);
    return value;
  }
  if (signal.metadata?.historical_backfill === true) return 'historical_backfill';
  if (signal.source === 'max_shadow_smoke' || signal.metadata?.synthetic === true) return 'synthetic_smoke';
  return 'live';
}

function withSignalProvenance(signal = {}) {
  const provenance = signalProvenance(signal);
  return {
    ...signal,
    metadata: { ...(signal.metadata || {}), provenance },
  };
}

module.exports = { PROVENANCE, PROVENANCE_SET, signalProvenance, withSignalProvenance };
