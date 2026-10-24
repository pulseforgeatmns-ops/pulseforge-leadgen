'use strict';

const { isSupportedAudioMime, normalizeMimeType } = require('../../voice/types');

function supports(attachment = {}) {
  if (attachment.type === 'voice') return true;
  const mime = String(attachment.mimeType || '').toLowerCase();
  const name = String(attachment.filename || '').toLowerCase();
  if (mime.startsWith('audio/')) return true;
  return /\.(webm|mp3|m4a|wav|ogg)$/i.test(name);
}

async function extract(attachment, ctx = {}) {
  const fromAttachment = attachment.transcription || ctx.transcription;
  if (fromAttachment && String(fromAttachment).trim()) {
    const text = String(fromAttachment).trim();
    return {
      transcription: text,
      extractedText: text,
      provenance: {
        adapter: 'voice',
        source: 'transcription',
        attachmentId: attachment.id,
        segments: ctx.segments || attachment.extractionEvidence?.segments || [],
        confidence: ctx.confidence ?? attachment.extractionEvidence?.confidence,
      },
      extractionStatus: 'ready',
      extractionEvidence: {
        adapter: 'voice',
        source: 'transcription',
        segments: ctx.segments || attachment.extractionEvidence?.segments || [],
        confidence: ctx.confidence ?? attachment.extractionEvidence?.confidence,
        rawTranscript: text,
      },
    };
  }

  const mime = normalizeMimeType(attachment.mimeType || ctx.mimeType);
  if (ctx.buffer && !isSupportedAudioMime(mime)) {
    return {
      extractionStatus: 'failed',
      extractionEvidence: {
        adapter: 'voice',
        error: 'unsupported_audio_type',
        mimeType: mime,
      },
    };
  }

  if (ctx.buffer && typeof ctx.transcribeVoice === 'function') {
    try {
      const result = await ctx.transcribeVoice({
        attachment,
        buffer: ctx.buffer,
        mimeType: mime,
        durationMs: ctx.durationMs,
      });
      const text = String(result.text || '').trim();
      if (!text) {
        return {
          extractionStatus: 'failed',
          extractionEvidence: {
            adapter: 'voice',
            error: 'empty_transcript',
            recordingId: result.recordingId,
          },
        };
      }
      return {
        transcription: text,
        extractedText: text,
        provenance: {
          adapter: 'voice',
          source: 'transcription',
          attachmentId: attachment.id,
          recordingId: result.recordingId,
          segments: result.segments || [],
          confidence: result.confidence,
        },
        extractionStatus: 'ready',
        extractionEvidence: {
          adapter: 'voice',
          source: 'transcription',
          recordingId: result.recordingId,
          segments: result.segments || [],
          confidence: result.confidence,
          rawTranscript: text,
          providerMetadata: result.providerMetadata,
        },
      };
    } catch (err) {
      return {
        extractionStatus: 'failed',
        extractionEvidence: {
          adapter: 'voice',
          error: err.code || 'transcription_failed',
          message: err.message,
          recordingId: err.recordingId,
        },
      };
    }
  }

  return {
    extractionStatus: 'pending',
    extractionEvidence: {
      adapter: 'voice',
      message: 'Voice recording stored; transcription pending.',
    },
  };
}

module.exports = {
  supports,
  extract,
};
