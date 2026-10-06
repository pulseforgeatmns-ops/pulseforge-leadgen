'use strict';

/**
 * Browser voice capture for Max composer (MAX-VOICE-001).
 * Record-then-upload via MediaRecorder; no streaming transcription.
 */
(function (global) {
  const DEFAULT_MAX_MS = 5 * 60 * 1000;
  const WARN_MS = 4 * 60 * 1000;

  function pickMimeType() {
    const candidates = [
      'audio/webm;codecs=opus',
      'audio/webm',
      'audio/mp4',
      'audio/ogg;codecs=opus',
    ];
    if (typeof MediaRecorder === 'undefined') return '';
    for (const c of candidates) {
      if (MediaRecorder.isTypeSupported(c)) return c;
    }
    return '';
  }

  function formatDuration(ms) {
    const totalSec = Math.floor(ms / 1000);
    const m = String(Math.floor(totalSec / 60)).padStart(2, '0');
    const s = String(totalSec % 60).padStart(2, '0');
    return `${m}:${s}`;
  }

  function createMaxVoiceRecorder(options = {}) {
    const maxDurationMs = options.maxDurationMs || DEFAULT_MAX_MS;
    const warnDurationMs = options.warnDurationMs || WARN_MS;
    let mediaStream = null;
    let recorder = null;
    let chunks = [];
    let startedAt = 0;
    let timer = null;
    let state = 'idle';
    let lastBlob = null;
    let lastMime = pickMimeType() || 'audio/webm';

    function emit(event, detail) {
      if (typeof options.onEvent === 'function') options.onEvent(event, detail);
    }

    async function start() {
      if (state === 'recording') return;
      if (!navigator.mediaDevices?.getUserMedia) {
        const err = new Error('Recording is not supported in this browser.');
        err.code = 'unsupported';
        throw err;
      }
      try {
        mediaStream = await navigator.mediaDevices.getUserMedia({ audio: true });
      } catch (err) {
        const wrapped = new Error('Microphone permission denied or unavailable.');
        wrapped.code = 'permission_denied';
        wrapped.cause = err;
        throw wrapped;
      }
      chunks = [];
      lastBlob = null;
      const mime = pickMimeType();
      lastMime = mime || 'audio/webm';
      try {
        recorder = mime ? new MediaRecorder(mediaStream, { mimeType: mime }) : new MediaRecorder(mediaStream);
      } catch (err) {
        stopTracks();
        const wrapped = new Error('Could not start recording.');
        wrapped.code = 'recording_failed';
        wrapped.cause = err;
        throw wrapped;
      }
      recorder.ondataavailable = (e) => {
        if (e.data?.size) chunks.push(e.data);
      };
      recorder.onerror = () => {
        emit('error', { code: 'recording_failed' });
      };
      recorder.onstop = () => {
        if (chunks.length) {
          lastBlob = new Blob(chunks, { type: recorder.mimeType || lastMime });
        }
        stopTracks();
        state = 'stopped';
        clearInterval(timer);
        emit('stopped', { blob: lastBlob, durationMs: Date.now() - startedAt });
      };
      recorder.start();
      startedAt = Date.now();
      state = 'recording';
      emit('started', { mimeType: recorder.mimeType || lastMime });
      timer = setInterval(() => {
        const elapsed = Date.now() - startedAt;
        emit('tick', { elapsedMs: elapsed, label: formatDuration(elapsed) });
        if (elapsed >= warnDurationMs) emit('warn', { elapsedMs: elapsed });
        if (elapsed >= maxDurationMs) stop();
      }, 250);
      bumpStarted();
    }

    function bumpStarted() {
      /* placeholder for client-side metrics hook */
    }

    function stopTracks() {
      if (mediaStream) {
        mediaStream.getTracks().forEach(t => t.stop());
        mediaStream = null;
      }
    }

    function stop() {
      if (state !== 'recording' || !recorder) return;
      state = 'stopping';
      try {
        recorder.stop();
      } catch {
        emit('error', { code: 'recording_failed' });
      }
    }

    function cancel() {
      chunks = [];
      lastBlob = null;
      if (recorder && state === 'recording') {
        recorder.onstop = () => {
          stopTracks();
          state = 'idle';
          clearInterval(timer);
          emit('cancelled');
        };
        try {
          recorder.stop();
        } catch {
          stopTracks();
          state = 'idle';
        }
      } else {
        stopTracks();
        state = 'idle';
        clearInterval(timer);
        emit('cancelled');
      }
    }

    function toFile(filenameBase) {
      if (!lastBlob) return null;
      const ext = (lastBlob.type || '').includes('mp4') ? 'm4a' : 'webm';
      const name = `${filenameBase || 'voice-note'}.${ext}`;
      return new File([lastBlob], name, { type: lastBlob.type || lastMime });
    }

    return {
      start,
      stop,
      cancel,
      toFile,
      getState: () => state,
      getBlob: () => lastBlob,
    };
  }

  global.MaxVoiceRecorder = {
    create: createMaxVoiceRecorder,
    formatDuration,
    pickMimeType,
  };
})(typeof window !== 'undefined' ? window : globalThis);
