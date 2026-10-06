'use strict';

function supports(attachment = {}) {
  const mime = String(attachment.mimeType || '').toLowerCase();
  const name = String(attachment.filename || '').toLowerCase();
  if (mime.startsWith('audio/')) return true;
  return /\.(webm|mp3|m4a|wav|ogg)$/i.test(name);
}

async function extract(attachment, { transcription } = {}) {
  const fromAttachment = attachment.transcription || transcription;
  if (fromAttachment && String(fromAttachment).trim()) {
    return {
      transcription: String(fromAttachment).trim(),
      extractedText: String(fromAttachment).trim(),
      provenance: { adapter: 'voice', source: 'transcription' },
      extractionStatus: 'ready',
    };
  }
  return {
    extractionStatus: 'pending',
    extractionEvidence: {
      adapter: 'voice',
      message: 'Voice recording stored; transcription pending (MAX-VOICE-001).',
    },
  };
}

module.exports = {
  supports,
  extract,
};
