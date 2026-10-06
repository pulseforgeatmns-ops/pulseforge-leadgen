'use strict';

const { normalizeMimeType } = require('./types');

/**
 * Speech-to-text only — no CRM or semantic interpretation.
 * @typedef {object} TranscriptSegment
 * @property {number} startMs
 * @property {number} endMs
 * @property {string} text
 * @property {number} [confidence]
 */

/**
 * @typedef {object} TranscriptionResult
 * @property {string} text
 * @property {number} [confidence]
 * @property {TranscriptSegment[]} [segments]
 * @property {object} [providerMetadata]
 */

function createStubTranscriptionAdapter({ fixedText = null } = {}) {
  return {
    name: 'stub',
    async transcribe(input) {
      const text = fixedText != null
        ? String(fixedText)
        : (input.priorTranscript || '');
      if (!String(text).trim()) {
        return { text: '', confidence: null, segments: [], providerMetadata: { stub: true } };
      }
      return {
        text: String(text).trim(),
        confidence: input.confidenceHint ?? 0.95,
        segments: input.segments || [{
          startMs: 0,
          endMs: input.durationMs || 0,
          text: String(text).trim(),
        }],
        providerMetadata: { stub: true },
      };
    },
  };
}

function createWhisperTranscriptionAdapter({ apiKey, fetchImpl = fetch, FormDataImpl = FormData, BlobImpl = Blob }) {
  if (!apiKey) {
    throw new Error('OPENAI_API_KEY required for Whisper transcription adapter');
  }
  return {
    name: 'whisper-1',
    async transcribe(input) {
      const mime = normalizeMimeType(input.mimeType || 'audio/webm');
      const ext = mime.includes('mp4') || mime.includes('m4a') ? 'm4a' : mime.includes('wav') ? 'wav' : 'webm';
      const form = new FormDataImpl();
      const blob = new BlobImpl([input.buffer], { type: mime });
      form.append('model', 'whisper-1');
      form.append('response_format', 'verbose_json');
      form.append('file', blob, `max-voice.${ext}`);

      const response = await fetchImpl('https://api.openai.com/v1/audio/transcriptions', {
        method: 'POST',
        headers: { Authorization: `Bearer ${apiKey}` },
        body: form,
      });
      const bodyText = await response.text();
      let body;
      try {
        body = bodyText ? JSON.parse(bodyText) : {};
      } catch {
        body = { raw: bodyText };
      }
      if (!response.ok) {
        const message = body?.error?.message || body?.raw || `Whisper HTTP ${response.status}`;
        const err = new Error(message);
        err.code = 'transcription_provider_error';
        throw err;
      }

      const text = typeof body.text === 'string' ? body.text.trim() : '';
      const segments = Array.isArray(body.segments)
        ? body.segments.map(seg => ({
          startMs: Math.round((seg.start || 0) * 1000),
          endMs: Math.round((seg.end || 0) * 1000),
          text: String(seg.text || '').trim(),
          confidence: seg.avg_logprob != null ? Math.min(1, Math.max(0, 1 + seg.avg_logprob)) : undefined,
        })).filter(s => s.text)
        : [];

      let confidence = null;
      if (segments.length) {
        const scored = segments.filter(s => s.confidence != null);
        if (scored.length) {
          confidence = scored.reduce((sum, s) => sum + s.confidence, 0) / scored.length;
        }
      }

      return {
        text,
        confidence,
        segments,
        providerMetadata: {
          provider: 'openai',
          model: body.model || 'whisper-1',
          duration: body.duration,
          language: body.language,
        },
      };
    },
  };
}

function createVoiceTranscriptionAdapter(options = {}) {
  if (options.adapter) return options.adapter;
  if (options.stub) return createStubTranscriptionAdapter(options.stubOptions || {});
  const apiKey = options.apiKey || process.env.OPENAI_API_KEY;
  if (apiKey) return createWhisperTranscriptionAdapter({ apiKey, ...options });
  return createStubTranscriptionAdapter();
}

module.exports = {
  createVoiceTranscriptionAdapter,
  createStubTranscriptionAdapter,
  createWhisperTranscriptionAdapter,
};
