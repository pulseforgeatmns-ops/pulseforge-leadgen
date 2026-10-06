'use strict';

function supports(attachment = {}) {
  const mime = String(attachment.mimeType || '').toLowerCase();
  const name = String(attachment.filename || '').toLowerCase();
  if (/^image\//.test(mime)) return true;
  return /\.(png|jpe?g|webp|gif)$/i.test(name);
}

async function extract(_attachment) {
  return {
    extractionStatus: 'pending',
    extractionEvidence: {
      adapter: 'image',
      message: 'Image semantic extraction is not production-ready; attachment is stored for a future adapter.',
    },
  };
}

module.exports = {
  supports,
  extract,
};
