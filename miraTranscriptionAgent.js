require('dotenv').config();

const axios = require('axios');
const pool = require('./db');

const AGENT_NAME = 'mira_transcription';
const DEFAULT_LIMIT = 5;
const WORKER_INTERVAL_MS = 15_000;
const ADVISORY_LOCK_KEY = 91720260601;

let intervalHandle = null;
let intervalRunning = false;

function truncateError(value, max = 500) {
  const text = value === undefined || value === null ? '' : String(value);
  return text.length > max ? text.slice(0, max) : text;
}

async function downloadVoiceFile(voiceUrl) {
  const response = await axios.get(voiceUrl, {
    responseType: 'arraybuffer',
    timeout: 20_000,
  });

  return {
    buffer: Buffer.from(response.data),
    contentType: response.headers['content-type'] || 'audio/ogg',
  };
}

async function transcribeWithWhisper({ buffer, contentType, captureId }) {
  if (!process.env.OPENAI_API_KEY) {
    throw new Error('OPENAI_API_KEY not set');
  }

  const form = new FormData();
  const blob = new Blob([buffer], { type: contentType });
  form.append('model', 'whisper-1');
  form.append('file', blob, `mira-capture-${captureId}.ogg`);

  const response = await fetch('https://api.openai.com/v1/audio/transcriptions', {
    method: 'POST',
    headers: {
      Authorization: `Bearer ${process.env.OPENAI_API_KEY}`,
    },
    body: form,
  });

  const bodyText = await response.text();
  let body;
  try {
    body = bodyText ? JSON.parse(bodyText) : {};
  } catch (_) {
    body = { raw: bodyText };
  }

  if (!response.ok) {
    const message = body?.error?.message || body?.raw || `OpenAI transcription failed with HTTP ${response.status}`;
    throw new Error(message);
  }

  const transcript = typeof body.text === 'string' ? body.text.trim() : '';
  if (!transcript) {
    throw new Error('OpenAI transcription response did not include text');
  }

  return transcript;
}

async function getPendingVoiceCaptures(limit = DEFAULT_LIMIT) {
  const { rows } = await pool.query(`
    SELECT id, voice_url
    FROM capture_inbox
    WHERE content_type = 'voice'
      AND transcript IS NULL
      AND status = 'new'
      AND voice_url IS NOT NULL
    ORDER BY received_at ASC
    LIMIT $1
  `, [limit]);

  return rows;
}

async function transcribeCapture(row) {
  try {
    const audio = await downloadVoiceFile(row.voice_url);
    const transcript = await transcribeWithWhisper({
      buffer: audio.buffer,
      contentType: audio.contentType,
      captureId: row.id,
    });

    await pool.query(`
      UPDATE capture_inbox
      SET transcript = $1,
          status = 'transcribed',
          processed_at = NOW()
      WHERE id = $2
    `, [transcript, row.id]);

    console.log(`[mira_transcription] capture_id=${row.id} transcribed`);
    return { id: row.id, status: 'transcribed' };
  } catch (err) {
    console.error(`[mira_transcription] capture_id=${row.id} failed:`, err.message);
    await pool.query(`
      UPDATE capture_inbox
      SET status = 'failed',
          processed_at = NOW()
      WHERE id = $1
    `, [row.id]);

    return { id: row.id, status: 'failed', error: truncateError(err.message) };
  }
}

async function withWorkerLock(fn) {
  const lock = await pool.query('SELECT pg_try_advisory_lock($1) AS locked', [ADVISORY_LOCK_KEY]);
  if (!lock.rows[0]?.locked) {
    return { skipped: true, reason: 'worker_already_running' };
  }

  try {
    return await fn();
  } finally {
    await pool.query('SELECT pg_advisory_unlock($1)', [ADVISORY_LOCK_KEY]).catch(err => {
      console.error('[mira_transcription] advisory unlock failed:', err.message);
    });
  }
}

async function run(params = {}) {
  const limit = Math.max(1, Number(params.limit || DEFAULT_LIMIT));

  return withWorkerLock(async () => {
    const rows = await getPendingVoiceCaptures(limit);
    if (!rows.length) {
      return { scanned: 0, transcribed: 0, failed: 0 };
    }

    let transcribed = 0;
    let failed = 0;
    const results = [];

    for (const row of rows) {
      const result = await transcribeCapture(row);
      results.push(result);
      if (result.status === 'transcribed') transcribed++;
      if (result.status === 'failed') failed++;
    }

    return { scanned: rows.length, transcribed, failed, results };
  });
}

function startMiraTranscriptionWorker(options = {}) {
  if (intervalHandle) return intervalHandle;
  const intervalMs = Math.max(5_000, Number(options.intervalMs || process.env.MIRA_TRANSCRIPTION_INTERVAL_MS || WORKER_INTERVAL_MS));

  intervalHandle = setInterval(() => {
    if (intervalRunning) return;
    intervalRunning = true;
    run()
      .catch(err => console.error('[mira_transcription] worker error:', err.message))
      .finally(() => {
        intervalRunning = false;
      });
  }, intervalMs);

  intervalHandle.unref?.();
  console.log(`[mira_transcription] worker started interval=${intervalMs}ms`);
  return intervalHandle;
}

module.exports = {
  run,
  startMiraTranscriptionWorker,
};

if (require.main === module) {
  run()
    .then(result => {
      console.log(JSON.stringify(result, null, 2));
      return pool.end();
    })
    .catch(async err => {
      console.error('[mira_transcription] fatal:', err.message);
      await pool.end().catch(() => {});
      process.exit(1);
    });
}
