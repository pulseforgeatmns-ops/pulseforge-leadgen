'use strict';
function metric(value, evidence = {}, now = Date.now()) {
  const observed = Date.parse(evidence.observedAt), source = Date.parse(evidence.providerTimestamp);
  let state = value === null || value === undefined ? 'UNKNOWN' : evidence.partial || !Number.isFinite(source) ? 'PARTIAL'
    : source > now || observed > now || !Number.isFinite(observed) ? 'UNKNOWN'
      : now - Math.min(source, observed) > (evidence.maxAgeMs || 20000) ? 'DELAYED' : 'LIVE';
  if (value != null && Number.isFinite(observed) && now-observed > (evidence.maxAgeMs || 20000)) state = 'DELAYED';
  return {value:value ?? null, state, source:evidence.provider || null, observedAt:evidence.observedAt || null,
    providerTimestamp:evidence.providerTimestamp || null, ageSeconds:Number.isFinite(observed) ? Math.max(0,Math.floor((now-observed)/1000)) : null,
    timestampBasis:evidence.timestampBasis || (Number.isFinite(source) ? 'provider_time' : 'response_received'), reason:evidence.reason || null};
}
function knownBy(row, at) {
  return Number.isFinite(Date.parse(row.knownAt)) && Number.isFinite(Date.parse(row.occurredAt || row.observedAt || row.blockTime))
    && Date.parse(row.knownAt) <= Date.parse(at) && Date.parse(row.occurredAt || row.observedAt || row.blockTime) <= Date.parse(at);
}
module.exports = {metric, knownBy};
