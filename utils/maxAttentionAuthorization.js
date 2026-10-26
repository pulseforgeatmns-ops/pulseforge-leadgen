'use strict';

const { resolveMaxOperationalScope } = require('./maxOperationalAuthorization');

async function resolveAttentionScope(req, db) {
  return (await resolveMaxOperationalScope(req, db, { accessDeniedCode: 'attention_access_denied' })).clientId;
}

module.exports = { resolveAttentionScope };
