'use strict';

const { interpretConversationalInput } = require('../understanding');
const { resolveClaim } = require('../stateIngestion/entityResolver');
const { CLAIM_TYPES, RESOLUTION } = require('../stateIngestion/types');
const { synthesizeRowUnderstandingText } = require('./rowText');

function classifyRowPreview({ interpreted, store }) {
  const context = store.snapshotContext();
  const accountClaim = interpreted.situationModel?.threads?.[0]?.ingestionClaims?.find(c => c.claim_type === CLAIM_TYPES.ACCOUNT)
    || interpreted.situationModel?.threads?.[0]?.entities?.find(e => e.kind === 'account');

  let accountName = null;
  if (accountClaim?.payload?.name) accountName = accountClaim.payload.name;
  else if (accountClaim?.name) accountName = accountClaim.name;
  else {
    accountName = interpreted.situationModel?.threads?.[0]?.accountName
      || interpreted.situationModel?.entities?.find(e => e.kind === 'account')?.name;
  }

  if (!accountName) {
    return { status: 'needs_clarification', reason: 'missing_account' };
  }

  const resolution = resolveClaim(
    { claim_type: CLAIM_TYPES.ACCOUNT, payload: { name: accountName } },
    context,
    {}
  );
  if (resolution.status === RESOLUTION.AMBIGUOUS) {
    return { status: 'needs_clarification', reason: 'ambiguous_account', accountName, resolution };
  }
  if (resolution.status === RESOLUTION.RESOLVED || resolution.status === RESOLUTION.PROVISIONALLY_RESOLVED) {
    return { status: 'matched', accountName, resolution };
  }
  return { status: 'new_account', accountName, resolution };
}

function buildSpreadsheetPreview({
  rows = [],
  instruction = null,
  store,
  memory,
  conversationId,
  filename,
  sheetName,
}) {
  const examples = [];
  let matched = 0;
  let needsClarification = 0;
  let newAccount = 0;
  let rejected = 0;
  let ready = 0;

  for (const row of rows) {
    const text = synthesizeRowUnderstandingText({
      instruction,
      rowValues: row.values,
      sheetName: row.sheet || sheetName,
      rowNumber: row.rowNumber,
      filename,
    });
    const interpreted = interpretConversationalInput({
      text,
      conversationId,
      memory,
    });
    const preview = classifyRowPreview({ interpreted, store });
    if (preview.status === 'matched') {
      matched += 1;
      ready += 1;
      if (examples.length < 4) {
        examples.push(`${preview.accountName} — matched existing account`);
      }
    } else if (preview.status === 'new_account') {
      newAccount += 1;
      ready += 1;
      if (examples.length < 4) {
        examples.push(`${preview.accountName} — appears to be a new account`);
      }
    } else if (preview.status === 'needs_clarification') {
      needsClarification += 1;
      if (examples.length < 4) {
        examples.push(`${preview.accountName || 'Row ' + row.rowNumber} — needs clarification`);
      }
    } else {
      rejected += 1;
    }
  }

  const total = rows.length;
  const summaryLines = [
    `I found ${total} prospect update${total === 1 ? '' : 's'}.`,
    `${matched} matched existing accounts.`,
    newAccount ? `${newAccount} appear${newAccount === 1 ? 's' : ''} to be new account${newAccount === 1 ? '' : 's'}.` : null,
    needsClarification ? `${needsClarification} need${needsClarification === 1 ? 's' : ''} clarification.` : null,
    rejected ? `${rejected} rejected.` : null,
  ].filter(Boolean);

  if (examples.length) {
    summaryLines.push('');
    summaryLines.push('Examples:');
    for (const ex of examples) summaryLines.push(`- ${ex}`);
  }

  return {
    total,
    ready,
    needs_clarification: needsClarification,
    rejected,
    matched,
    new_account: newAccount,
    summary: summaryLines.join('\n'),
    counts: { total, ready, needs_clarification: needsClarification, rejected },
  };
}

module.exports = {
  buildSpreadsheetPreview,
  classifyRowPreview,
};
