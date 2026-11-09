'use strict';
const { randomUUID } = require('node:crypto');
const TTL_MS = 2 * 60 * 60 * 1000;
const MAX_BYTES = 16384;
function identifier(value) {
  return typeof value === 'string' && value.length > 0 && value.length <= 160 && /^[a-zA-Z0-9_.:-]+$/.test(value) ? value : null;
}
function scope(tenantId, sessionId) {
  if (!identifier(tenantId) || !identifier(sessionId)) throw new Error('Invalid conversation scope');
}
// Explicit projection: never serialize a live SessionStore object. No transcript,
// customer records, execution policy, package presentation, or approvals survive.
function project(session, tenantId) {
  if (String(session?.context?.tenantId) !== tenantId) throw new Error('Conversation tenant mismatch');
  const currentFocus = require('../workspace/OperationalQuestionFrame').currentFocus(session,tenantId);
  const focus = identifier(currentFocus?.missionIds?.[0]);
  const mission = identifier(session.context.acquisitionMissionId || session.context.missionId);
  const candidates = [...new Set([focus, mission].filter(Boolean))].slice(0, 16);
  const evidence = session.operationalEvidence;
  const evidenceRefs = evidence && String(evidence.tenantId) === tenantId
    ? [{ kind: 'operational_inspection', observedAt: evidence.recordedAt || evidence.asOf || null }].filter(r => Number.isFinite(Date.parse(r.observedAt))) : [];
  return { version: 1, candidates, focusId: focus, evidenceRefs, requiresEvidenceRefresh: true };
}
function validate(value) {
  if (!value || value.version !== 1 || !Array.isArray(value.candidates) || value.candidates.length > 16 ||
      value.candidates.some(x => !identifier(x)) || (value.focusId != null && !identifier(value.focusId)) ||
      (value.focusId && !value.candidates.includes(value.focusId)) ||
      !Array.isArray(value.evidenceRefs) || value.evidenceRefs.length > 16 ||
      value.evidenceRefs.some(r => r.kind !== 'operational_inspection' || typeof r.observedAt !== 'string' || !Number.isFinite(Date.parse(r.observedAt)) || Object.keys(r).some(k => !['kind','observedAt'].includes(k))) ||
      value.requiresEvidenceRefresh !== true || Buffer.byteLength(JSON.stringify(value)) > MAX_BYTES ||
      Object.keys(value).some(k => !['version','candidates','focusId','evidenceRefs','requiresEvidenceRefresh'].includes(k))) throw new Error('Invalid conversation context');
  return JSON.parse(JSON.stringify(value));
}
class ConversationContextStore {
  constructor({db, now = Date.now} = {}) {
    if (!db?.query) throw new Error('ConversationContextStore requires a database');
    this.db = db;
    this.now = now;
  }
  async load(tenantId, sessionId) {
    scope(tenantId, sessionId);
    const {rows} = await this.db.query(`SELECT revision, context FROM max_conversation_context
      WHERE tenant_id=$1 AND session_id=$2 AND expires_at > $3`, [tenantId,sessionId,new Date(this.now())]);
    if (!rows.length) return null;
    return {revision:rows[0].revision,context:validate(rows[0].context)};
  }
  async save(tenantId, sessionId, context, expectedRevision = null) {
    scope(tenantId, sessionId);
    const projected = validate(context);
    if (expectedRevision !== null && !identifier(expectedRevision)) throw new Error('Invalid conversation revision');
    const revision = randomUUID();
    const values = [tenantId,sessionId,revision,JSON.stringify(projected),new Date(this.now()+TTL_MS),expectedRevision,new Date(this.now())];
    // Compare-and-swap prevents silent lost updates across replicas. Expired
    // context cannot be revived with a stale revision; restart with a null one.
    if (expectedRevision !== null) {
      const updated = await this.db.query(`UPDATE max_conversation_context SET revision=$3,context=$4::jsonb,expires_at=$5
        WHERE tenant_id=$1 AND session_id=$2 AND revision=$6 AND expires_at > $7 RETURNING revision`, values);
      if (updated.rows.length) return {saved:true,revision};
      return {saved:false,conflict:true};
    }
    const {rows} = await this.db.query(`INSERT INTO max_conversation_context (tenant_id,session_id,revision,context,expires_at)
      SELECT $1,$2,$3,$4::jsonb,$5 WHERE $6::text IS NULL
      ON CONFLICT (tenant_id,session_id) DO UPDATE SET revision=$3,context=$4::jsonb,expires_at=$5
      WHERE (max_conversation_context.revision=$6 AND max_conversation_context.expires_at > $7)
         OR ($6::text IS NULL AND max_conversation_context.expires_at <= $7)
      RETURNING revision`, values);
    return rows.length ? {saved:true,revision} : {saved:false,conflict:true};
  }
  async remove(tenantId, sessionId) {
    scope(tenantId,sessionId);
    await this.db.query('DELETE FROM max_conversation_context WHERE tenant_id=$1 AND session_id=$2',[tenantId,sessionId]);
  }
  async purgeExpired() {
    await this.db.query('DELETE FROM max_conversation_context WHERE expires_at <= $1',[new Date(this.now())]);
  }
}
module.exports = {ConversationContextStore,project,validate,TTL_MS,MAX_BYTES};
