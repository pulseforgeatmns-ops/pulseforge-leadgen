'use strict';

function sameId(left, right) {
  return left != null && right != null && String(left) === String(right);
}

// A snapshot is a proposed target, never proof that a persisted mutation succeeded.
function expectationScope(clientId, snapshot) {
  const exp = snapshot?.expectation;
  const account = snapshot?.account;
  if (!exp?.id || !sameId(exp.client_id, clientId)
    || !sameId(account?.client_id, clientId)
    || !sameId(exp.prospect_id, account?.id)
    || !sameId(exp.ao_id, account?.owner_id)) return null;
  return { clientId, prospectId: exp.prospect_id, aoId: exp.ao_id };
}

function matchesExpectation(row, id, scope) {
  return Boolean(scope && row && sameId(row.id, id)
    && sameId(row.client_id, scope.clientId)
    && sameId(row.prospect_id, scope.prospectId)
    && sameId(row.ao_id, scope.aoId));
}

module.exports = { expectationScope, matchesExpectation };
