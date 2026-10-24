'use strict';

const { EPISTEMIC_CATEGORY } = require('./types');

function evidenceRef({
  inputId,
  textSpan = null,
  epistemic = EPISTEMIC_CATEGORY.CONFIRMED,
  sourceActor = null,
  reportedBy = null,
  confidence = null,
  claimId = null,
}) {
  return {
    input_id: inputId,
    text_span: textSpan,
    epistemic,
    source_actor: sourceActor,
    reported_by: reportedBy,
    confidence,
    claim_id: claimId,
  };
}

function spanFromMatch(text, match) {
  if (!match || match.index == null) return null;
  return {
    start: match.index,
    end: match.index + match[0].length,
    text: match[0],
  };
}

module.exports = {
  evidenceRef,
  spanFromMatch,
};
