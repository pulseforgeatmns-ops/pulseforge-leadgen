'use strict';

const DOCUMENT_MIMES = new Set([
  'text/plain',
  'text/markdown',
  'application/pdf',
  'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
]);

function supports(attachment = {}) {
  const mime = String(attachment.mimeType || '').toLowerCase();
  const name = String(attachment.filename || '').toLowerCase();
  if (DOCUMENT_MIMES.has(mime)) return true;
  return /\.(txt|md|pdf|docx)$/i.test(name);
}

async function extract(attachment, { buffer } = {}) {
  if (!buffer) {
    return { extractionStatus: 'failed', extractionEvidence: { error: 'missing_buffer' } };
  }
  const name = String(attachment.filename || '').toLowerCase();
  if (name.endsWith('.txt') || name.endsWith('.md') || attachment.mimeType === 'text/plain' || attachment.mimeType === 'text/markdown') {
    const text = buffer.toString('utf8');
    return {
      extractedText: text,
      provenance: { adapter: 'document', format: 'text' },
      extractionStatus: 'ready',
    };
  }
  if (name.endsWith('.pdf') || attachment.mimeType === 'application/pdf') {
    return {
      extractionStatus: 'failed',
      extractionEvidence: {
        error: 'pdf_extraction_not_configured',
        message: 'PDF text extraction adapter is not wired in this environment.',
      },
    };
  }
  if (name.endsWith('.docx')) {
    return {
      extractionStatus: 'failed',
      extractionEvidence: {
        error: 'docx_extraction_not_configured',
        message: 'DOCX extraction adapter is not wired in this environment.',
      },
    };
  }
  return {
    extractionStatus: 'failed',
    extractionEvidence: { error: 'unsupported_document_type' },
  };
}

module.exports = {
  supports,
  extract,
};
