'use strict';
// Idempotent bridge into the existing manual outcome capture + Paige/Max learning path.
// A retry after an outcome-write failure never creates another provider post.
async function syncPaigeSocialOutcome(pool, artifact) {
  const channel = { linkedin_page: 'linkedin', linkedin_personal: 'linkedin', facebook_page: 'facebook', google_business: 'gbp' }[artifact.platform];
  const conn = await pool.connect();
  try {
    await conn.query('BEGIN');
    await conn.query(`INSERT INTO content_publications
      (id, client_id, tenant_id, content_artifact_id, channel, external_post_id, external_url, published_at, objective, campaign_id, title, format)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT (id) DO NOTHING`,
    [artifact.id, artifact.clientId, artifact.tenantId, artifact.id, channel, artifact.publication.providerPostId,
      artifact.publishedUrl, artifact.publishedAt, artifact.contentObjective, artifact.meta?.campaignId || artifact.missionId,
      artifact.label, artifact.meta?.format || artifact.contentType]);
    const row = (await conn.query('SELECT client_id, content_artifact_id, external_post_id FROM content_publications WHERE id=$1', [artifact.id])).rows[0];
    if (row?.client_id !== artifact.clientId || row.content_artifact_id !== artifact.id || row.external_post_id !== artifact.publication.providerPostId) throw new Error('outcome_binding_conflict');
    await conn.query("UPDATE pending_comments SET status='posted', posted_at=$3 WHERE id=$1 AND client_id=$2", [artifact.pendingCommentId, artifact.clientId, artifact.publishedAt]);
    await conn.query('COMMIT');
    return artifact.id;
  } catch (err) { await conn.query('ROLLBACK'); throw err; }
  finally { conn.release(); }
}
module.exports = { syncPaigeSocialOutcome };
