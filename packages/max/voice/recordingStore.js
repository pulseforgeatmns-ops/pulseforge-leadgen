'use strict';

const { newComposerId } = require('../composer/id');

class MemoryVoiceRecordingStore {
  constructor() {
    this.records = new Map();
  }

  async create(record) {
    const row = {
      ...record,
      created_at: record.created_at || new Date().toISOString(),
      updated_at: record.updated_at || new Date().toISOString(),
    };
    this.records.set(row.id, row);
    return row;
  }

  async get(id, clientId) {
    const row = this.records.get(id);
    if (!row || Number(row.client_id) !== Number(clientId)) return null;
    return row;
  }

  async update(id, clientId, patch) {
    const row = await this.get(id, clientId);
    if (!row) return null;
    Object.assign(row, patch, { updated_at: new Date().toISOString() });
    return row;
  }
}

class PostgresVoiceRecordingStore {
  constructor(db) {
    this.db = db;
  }

  async create(record) {
    await this.db.query(`
      INSERT INTO max_voice_recordings (
        id, client_id, conversation_id, actor_id, mime_type, duration_ms, storage_ref,
        audio_bytea, transcription_status, transcript_text, transcript_segments,
        transcription_confidence, transcription_provider, transcription_metadata,
        envelope_id, attachment_id
      ) VALUES (
        $1, $2, $3, $4, $5, $6, $7,
        $8, $9, $10, $11::jsonb,
        $12, $13, $14::jsonb,
        $15, $16
      )
    `, [
      record.id,
      record.client_id,
      record.conversation_id || null,
      record.actor_id || null,
      record.mime_type,
      record.duration_ms ?? null,
      record.storage_ref,
      record.audio_bytea || null,
      record.transcription_status || 'pending',
      record.transcript_text || null,
      JSON.stringify(record.transcript_segments || []),
      record.transcription_confidence ?? null,
      record.transcription_provider || null,
      JSON.stringify(record.transcription_metadata || {}),
      record.envelope_id || null,
      record.attachment_id || null,
    ]);
    return this.get(record.id, record.client_id);
  }

  async get(id, clientId) {
    const { rows } = await this.db.query(`
      SELECT * FROM max_voice_recordings WHERE id = $1 AND client_id = $2 LIMIT 1
    `, [id, clientId]);
    return rows[0] || null;
  }

  async update(id, clientId, patch) {
    const existing = await this.get(id, clientId);
    if (!existing) return null;
    const next = {
      transcription_status: patch.transcription_status ?? existing.transcription_status,
      transcript_text: patch.transcript_text ?? existing.transcript_text,
      transcript_segments: patch.transcript_segments ?? existing.transcript_segments,
      transcription_confidence: patch.transcription_confidence ?? existing.transcription_confidence,
      transcription_provider: patch.transcription_provider ?? existing.transcription_provider,
      transcription_metadata: patch.transcription_metadata ?? existing.transcription_metadata,
      duration_ms: patch.duration_ms ?? existing.duration_ms,
      envelope_id: patch.envelope_id ?? existing.envelope_id,
    };
    await this.db.query(`
      UPDATE max_voice_recordings SET
        transcription_status = $3,
        transcript_text = $4,
        transcript_segments = $5::jsonb,
        transcription_confidence = $6,
        transcription_provider = $7,
        transcription_metadata = $8::jsonb,
        duration_ms = COALESCE($9, duration_ms),
        envelope_id = COALESCE($10, envelope_id),
        updated_at = NOW()
      WHERE id = $1 AND client_id = $2
    `, [
      id,
      clientId,
      next.transcription_status,
      next.transcript_text,
      JSON.stringify(next.transcript_segments || []),
      next.transcription_confidence,
      next.transcription_provider,
      JSON.stringify(next.transcription_metadata || {}),
      patch.duration_ms ?? null,
      patch.envelope_id ?? null,
    ]);
    return this.get(id, clientId);
  }
}

function newVoiceRecordingId() {
  return newComposerId('voice');
}

module.exports = {
  MemoryVoiceRecordingStore,
  PostgresVoiceRecordingStore,
  newVoiceRecordingId,
};
