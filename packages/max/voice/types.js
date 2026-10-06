'use strict';

const ALLOWED_AUDIO_MIME = Object.freeze([
  'audio/webm',
  'audio/mp4',
  'audio/m4a',
  'audio/wav',
  'audio/x-wav',
  'audio/mpeg',
  'audio/ogg',
]);

function normalizeMimeType(mime) {
  const m = String(mime || '').toLowerCase().split(';')[0].trim();
  if (m === 'audio/x-m4a') return 'audio/m4a';
  return m;
}

function isSupportedAudioMime(mime) {
  const n = normalizeMimeType(mime);
  if (ALLOWED_AUDIO_MIME.includes(n)) return true;
  return n.startsWith('audio/');
}

module.exports = {
  ALLOWED_AUDIO_MIME,
  normalizeMimeType,
  isSupportedAudioMime,
};
