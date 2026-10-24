'use strict';

const spreadsheet = require('./spreadsheet');
const document = require('./document');
const image = require('./image');
const voice = require('./voice');

const ADAPTERS = [spreadsheet, document, image, voice];

function adapterFor(attachment) {
  return ADAPTERS.find(a => a.supports(attachment)) || null;
}

async function extractAttachment(attachment, ctx = {}) {
  const adapter = adapterFor(attachment);
  if (!adapter) {
    return {
      extractionStatus: 'failed',
      extractionEvidence: { error: 'no_adapter', filename: attachment.filename },
    };
  }
  return adapter.extract(attachment, ctx);
}

module.exports = {
  ADAPTERS,
  adapterFor,
  extractAttachment,
  spreadsheet,
  document,
  image,
  voice,
};
