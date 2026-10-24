'use strict';

// Provenance only. Possessing a ref does not authorize processing: the durable
// ticket store separately validates membership and a live fenced claim.
const ID_FIELDS = ['tenantId', 'fingerprint', 'planVersion', 'ticketVersion',
  'queryKey', 'payloadHash', 'memberKey', 'candidateKey'];

function mergeTicketRefs(...records) {
  const result = new Map();
  for (const record of records) {
    const refs = Array.isArray(record) ? record : record?._scoutTicketRefs || [];
    if (!Array.isArray(refs)) throw Error('Invalid Scout ticket memberships');
    for (const ref of refs) {
      if (!ref || ID_FIELDS.some(key => typeof ref[key] !== 'string' || !ref[key])) throw Error('Invalid Scout ticket membership');
      const key = JSON.stringify(ID_FIELDS.map(field => ref[field]));
      const prior = result.get(key);
      if (prior?.claimToken && ref.claimToken && (prior.claimToken !== ref.claimToken
        || prior.membershipRevision !== ref.membershipRevision)) throw Error('Conflicting Scout ticket claims');
      if (!prior || ref.claimToken || !prior.claimToken) result.set(key, structuredClone(ref));
    }
  }
  return [...result.values()];
}

function ticketFields(...records) {
  const refs = mergeTicketRefs(...records);
  return refs.length ? { _scoutTicketRefs: refs } : {};
}

module.exports = { ID_FIELDS, mergeTicketRefs, ticketFields };
