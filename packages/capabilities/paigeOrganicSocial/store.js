'use strict';

const crypto = require('crypto');
const {
  BACKLOG_APPROVAL_STATES,
} = require('./types');

function newId() {
  return crypto.randomUUID();
}

function clone(row) {
  return JSON.parse(JSON.stringify(row));
}

function normalizeAsset(row) {
  return {
    id: row.id,
    clientId: Number(row.client_id ?? row.clientId),
    driveFileId: row.drive_file_id ?? row.driveFileId,
    driveFolderId: row.drive_folder_id ?? row.driveFolderId ?? null,
    filename: row.filename,
    mimeType: row.mime_type ?? row.mimeType ?? null,
    mediaKind: row.media_kind ?? row.mediaKind ?? 'other',
    byteSize: row.byte_size ?? row.byteSize ?? null,
    createdTime: row.created_time ?? row.createdTime ?? null,
    modifiedTime: row.modified_time ?? row.modifiedTime ?? null,
    thumbnailLink: row.thumbnail_link ?? row.thumbnailLink ?? null,
    webViewLink: row.web_view_link ?? row.webViewLink ?? null,
    jobGroupKey: row.job_group_key ?? row.jobGroupKey ?? null,
    visualHints: row.visual_hints ?? row.visualHints ?? {},
    usageCount: Number(row.usage_count ?? row.usageCount ?? 0),
    lastUsedAt: row.last_used_at ?? row.lastUsedAt ?? null,
    discoveredAt: row.discovered_at ?? row.discoveredAt ?? null,
    metadata: row.metadata ?? {},
  };
}

function normalizeBacklog(row) {
  return {
    id: row.id,
    clientId: Number(row.client_id ?? row.clientId),
    storyConcept: row.story_concept ?? row.storyConcept,
    contentCategory: row.content_category ?? row.contentCategory,
    targetPlatform: row.target_platform ?? row.targetPlatform,
    proposedFormat: row.proposed_format ?? row.proposedFormat,
    assetIds: row.asset_ids ?? row.assetIds ?? [],
    bodyDraft: row.body_draft ?? row.bodyDraft ?? null,
    platformVariants: row.platform_variants ?? row.platformVariants ?? {},
    proposedPublishAt: row.proposed_publish_at ?? row.proposedPublishAt ?? null,
    schedulingRationale: row.scheduling_rationale ?? row.schedulingRationale ?? null,
    planningRationale: row.planning_rationale ?? row.planningRationale ?? null,
    approvalState: row.approval_state ?? row.approvalState ?? BACKLOG_APPROVAL_STATES.DRAFT,
    artifactId: row.artifact_id ?? row.artifactId ?? null,
    exploration: Boolean(row.exploration),
    meta: row.meta ?? {},
    createdAt: row.created_at ?? row.createdAt,
    updatedAt: row.updated_at ?? row.updatedAt,
  };
}

function normalizeSignal(row) {
  return {
    clientId: Number(row.client_id ?? row.clientId),
    platform: row.platform,
    contentCategory: row.content_category ?? row.contentCategory ?? null,
    format: row.format ?? null,
    dow: Number(row.dow),
    hourLocal: Number(row.hour_local ?? row.hourLocal),
    sampleCount: Number(row.sample_count ?? row.sampleCount ?? 0),
    impressionsSum: Number(row.impressions_sum ?? row.impressionsSum ?? 0),
    engagementSum: Number(row.engagement_sum ?? row.engagementSum ?? 0),
    engagementRateAvg: row.engagement_rate_avg ?? row.engagementRateAvg ?? null,
    lastObservedAt: row.last_observed_at ?? row.lastObservedAt ?? null,
  };
}

async function ensureOrganicSocialSchema(db) {
  await db.query('CREATE EXTENSION IF NOT EXISTS pgcrypto');
  await db.query(`
    CREATE TABLE IF NOT EXISTS paige_media_assets (
      id UUID PRIMARY KEY,
      client_id INTEGER NOT NULL,
      drive_file_id TEXT NOT NULL,
      drive_folder_id TEXT,
      filename TEXT NOT NULL,
      mime_type TEXT,
      media_kind TEXT NOT NULL DEFAULT 'other',
      byte_size BIGINT,
      created_time TIMESTAMPTZ,
      modified_time TIMESTAMPTZ,
      thumbnail_link TEXT,
      web_view_link TEXT,
      job_group_key TEXT,
      visual_hints JSONB NOT NULL DEFAULT '{}'::jsonb,
      usage_count INTEGER NOT NULL DEFAULT 0,
      last_used_at TIMESTAMPTZ,
      discovered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
      UNIQUE (client_id, drive_file_id)
    );
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS paige_content_backlog (
      id UUID PRIMARY KEY,
      client_id INTEGER NOT NULL,
      story_concept TEXT NOT NULL,
      content_category TEXT NOT NULL,
      target_platform TEXT NOT NULL,
      proposed_format TEXT NOT NULL,
      asset_ids UUID[] NOT NULL DEFAULT '{}'::uuid[],
      body_draft TEXT,
      platform_variants JSONB NOT NULL DEFAULT '{}'::jsonb,
      proposed_publish_at TIMESTAMPTZ,
      scheduling_rationale TEXT,
      planning_rationale TEXT,
      approval_state TEXT NOT NULL DEFAULT 'draft',
      artifact_id UUID,
      exploration BOOLEAN NOT NULL DEFAULT false,
      meta JSONB NOT NULL DEFAULT '{}'::jsonb,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
  `);
  await db.query(`
    CREATE INDEX IF NOT EXISTS paige_content_backlog_client_state_idx
      ON paige_content_backlog (client_id, approval_state, proposed_publish_at);
  `);
  await db.query(`
    CREATE TABLE IF NOT EXISTS paige_social_platform_history (
      client_id INTEGER NOT NULL,
      platform TEXT NOT NULL,
      content_category TEXT NOT NULL DEFAULT '',
      format TEXT NOT NULL DEFAULT '',
      dow SMALLINT NOT NULL,
      hour_local SMALLINT NOT NULL,
      sample_count INTEGER NOT NULL DEFAULT 0,
      impressions_sum BIGINT NOT NULL DEFAULT 0,
      engagement_sum BIGINT NOT NULL DEFAULT 0,
      engagement_rate_avg NUMERIC,
      last_observed_at TIMESTAMPTZ,
      PRIMARY KEY (client_id, platform, content_category, format, dow, hour_local)
    );
  `);
}

function createMemoryOrganicSocialStore() {
  const assets = new Map();
  const backlog = new Map();
  const signals = new Map();

  function signalKey(row) {
    return [row.clientId, row.platform, row.contentCategory || '', row.format || '', row.dow, row.hourLocal].join(':');
  }

  return {
    kind: 'memory',
    async ensureSchema() {},
    async upsertMediaAsset(input) {
      const existing = [...assets.values()].find(
        (a) => a.clientId === input.clientId && a.driveFileId === input.driveFileId
      );
      const id = existing?.id || input.id || newId();
      const row = normalizeAsset({
        ...input,
        id,
        client_id: input.clientId,
        drive_file_id: input.driveFileId,
        discovered_at: existing?.discoveredAt || input.discoveredAt || new Date().toISOString(),
      });
      assets.set(id, row);
      return clone(row);
    },
    async listMediaAssets(clientId, filter = {}) {
      let rows = [...assets.values()].filter((a) => a.clientId === Number(clientId));
      if (filter.unusedOnly) rows = rows.filter((a) => a.usageCount === 0);
      if (filter.jobGroupKey) rows = rows.filter((a) => a.jobGroupKey === filter.jobGroupKey);
      rows.sort((a, b) => String(b.modifiedTime || b.discoveredAt).localeCompare(String(a.modifiedTime || a.discoveredAt)));
      return rows.map(clone);
    },
    async getMediaAsset(id, clientId) {
      const row = assets.get(id);
      if (!row || row.clientId !== Number(clientId)) return null;
      return clone(row);
    },
    async recordAssetUsage(assetIds, clientId) {
      const now = new Date().toISOString();
      for (const id of assetIds || []) {
        const row = assets.get(id);
        if (!row || row.clientId !== Number(clientId)) continue;
        row.usageCount += 1;
        row.lastUsedAt = now;
      }
    },
    async insertBacklogItem(input) {
      const id = input.id || newId();
      const now = new Date().toISOString();
      const row = normalizeBacklog({
        ...input,
        id,
        client_id: input.clientId,
        created_at: now,
        updated_at: now,
      });
      backlog.set(id, row);
      return clone(row);
    },
    async updateBacklogItem(id, clientId, patch) {
      const row = backlog.get(id);
      if (!row || row.clientId !== Number(clientId)) throw new Error('backlog_not_found');
      Object.assign(row, normalizeBacklog({ ...row, ...patch, updated_at: new Date().toISOString() }));
      backlog.set(id, row);
      return clone(row);
    },
    async listBacklog(clientId, filter = {}) {
      let rows = [...backlog.values()].filter((b) => b.clientId === Number(clientId));
      if (filter.approvalState) rows = rows.filter((b) => b.approvalState === filter.approvalState);
      if (filter.platform) rows = rows.filter((b) => b.targetPlatform === filter.platform);
      rows.sort((a, b) => String(a.proposedPublishAt || a.createdAt).localeCompare(String(b.proposedPublishAt || b.createdAt)));
      return rows.map(clone);
    },
    async getBacklogItem(id, clientId) {
      const row = backlog.get(id);
      if (!row || row.clientId !== Number(clientId)) return null;
      return clone(row);
    },
    async upsertPlatformSignal(input) {
      const row = normalizeSignal(input);
      const key = signalKey(row);
      const existing = signals.get(key);
      if (!existing) {
        signals.set(key, row);
        return clone(row);
      }
      existing.sampleCount += row.sampleCount;
      existing.impressionsSum += row.impressionsSum;
      existing.engagementSum += row.engagementSum;
      existing.engagementRateAvg = existing.impressionsSum > 0
        ? existing.engagementSum / existing.impressionsSum
        : existing.engagementRateAvg;
      existing.lastObservedAt = row.lastObservedAt || existing.lastObservedAt;
      return clone(existing);
    },
    async listPlatformSignals(clientId, platform) {
      return [...signals.values()]
        .filter((s) => s.clientId === Number(clientId) && (!platform || s.platform === platform))
        .map(clone);
    },
  };
}

function createPostgresOrganicSocialStore(pool) {
  const db = pool;
  return {
    kind: 'postgres',
    ensureSchema: () => ensureOrganicSocialSchema(db),
    async upsertMediaAsset(input) {
      const result = await db.query(
        `INSERT INTO paige_media_assets (
          id, client_id, drive_file_id, drive_folder_id, filename, mime_type, media_kind,
          byte_size, created_time, modified_time, thumbnail_link, web_view_link,
          job_group_key, visual_hints, metadata
        ) VALUES (
          COALESCE($1, gen_random_uuid()), $2, $3, $4, $5, $6, $7,
          $8, $9, $10, $11, $12, $13, $14::jsonb, $15::jsonb
        )
        ON CONFLICT (client_id, drive_file_id) DO UPDATE SET
          drive_folder_id = EXCLUDED.drive_folder_id,
          filename = EXCLUDED.filename,
          mime_type = EXCLUDED.mime_type,
          media_kind = EXCLUDED.media_kind,
          byte_size = EXCLUDED.byte_size,
          created_time = EXCLUDED.created_time,
          modified_time = EXCLUDED.modified_time,
          thumbnail_link = EXCLUDED.thumbnail_link,
          web_view_link = EXCLUDED.web_view_link,
          job_group_key = COALESCE(EXCLUDED.job_group_key, paige_media_assets.job_group_key),
          visual_hints = EXCLUDED.visual_hints,
          metadata = paige_media_assets.metadata || EXCLUDED.metadata
        RETURNING *`,
        [
          input.id || null,
          input.clientId,
          input.driveFileId,
          input.driveFolderId || null,
          input.filename,
          input.mimeType || null,
          input.mediaKind || 'other',
          input.byteSize || null,
          input.createdTime || null,
          input.modifiedTime || null,
          input.thumbnailLink || null,
          input.webViewLink || null,
          input.jobGroupKey || null,
          JSON.stringify(input.visualHints || {}),
          JSON.stringify(input.metadata || {}),
        ]
      );
      return normalizeAsset(result.rows[0]);
    },
    async listMediaAssets(clientId, filter = {}) {
      const clauses = ['client_id = $1'];
      const params = [Number(clientId)];
      if (filter.unusedOnly) clauses.push('usage_count = 0');
      if (filter.jobGroupKey) {
        params.push(filter.jobGroupKey);
        clauses.push(`job_group_key = $${params.length}`);
      }
      const result = await db.query(
        `SELECT * FROM paige_media_assets WHERE ${clauses.join(' AND ')}
         ORDER BY COALESCE(modified_time, discovered_at) DESC`,
        params
      );
      return result.rows.map(normalizeAsset);
    },
    async getMediaAsset(id, clientId) {
      const result = await db.query(
        'SELECT * FROM paige_media_assets WHERE id = $1 AND client_id = $2',
        [id, Number(clientId)]
      );
      return result.rows[0] ? normalizeAsset(result.rows[0]) : null;
    },
    async recordAssetUsage(assetIds, clientId) {
      if (!assetIds?.length) return;
      await db.query(
        `UPDATE paige_media_assets
            SET usage_count = usage_count + 1,
                last_used_at = NOW()
          WHERE client_id = $1 AND id = ANY($2::uuid[])`,
        [Number(clientId), assetIds]
      );
    },
    async insertBacklogItem(input) {
      const result = await db.query(
        `INSERT INTO paige_content_backlog (
          id, client_id, story_concept, content_category, target_platform, proposed_format,
          asset_ids, body_draft, platform_variants, proposed_publish_at, scheduling_rationale,
          planning_rationale, approval_state, artifact_id, exploration, meta
        ) VALUES (
          COALESCE($1, gen_random_uuid()), $2, $3, $4, $5, $6,
          $7::uuid[], $8, $9::jsonb, $10, $11, $12, $13, $14, $15, $16::jsonb
        ) RETURNING *`,
        [
          input.id || null,
          input.clientId,
          input.storyConcept,
          input.contentCategory,
          input.targetPlatform,
          input.proposedFormat,
          input.assetIds || [],
          input.bodyDraft || null,
          JSON.stringify(input.platformVariants || {}),
          input.proposedPublishAt || null,
          input.schedulingRationale || null,
          input.planningRationale || null,
          input.approvalState || BACKLOG_APPROVAL_STATES.DRAFT,
          input.artifactId || null,
          Boolean(input.exploration),
          JSON.stringify(input.meta || {}),
        ]
      );
      return normalizeBacklog(result.rows[0]);
    },
    async updateBacklogItem(id, clientId, patch) {
      const fields = [];
      const params = [id, Number(clientId)];
      const map = {
        storyConcept: 'story_concept',
        contentCategory: 'content_category',
        targetPlatform: 'target_platform',
        proposedFormat: 'proposed_format',
        assetIds: 'asset_ids',
        bodyDraft: 'body_draft',
        platformVariants: 'platform_variants',
        proposedPublishAt: 'proposed_publish_at',
        schedulingRationale: 'scheduling_rationale',
        planningRationale: 'planning_rationale',
        approvalState: 'approval_state',
        artifactId: 'artifact_id',
        exploration: 'exploration',
        meta: 'meta',
      };
      for (const [key, col] of Object.entries(map)) {
        if (patch[key] === undefined) continue;
        params.push(key === 'assetIds' ? patch[key] : key === 'platformVariants' || key === 'meta' ? JSON.stringify(patch[key]) : patch[key]);
        const idx = params.length;
        if (key === 'assetIds') fields.push(`${col} = $${idx}::uuid[]`);
        else if (key === 'platformVariants' || key === 'meta') fields.push(`${col} = $${idx}::jsonb`);
        else fields.push(`${col} = $${idx}`);
      }
      if (!fields.length) throw new Error('backlog_patch_empty');
      fields.push('updated_at = NOW()');
      const result = await db.query(
        `UPDATE paige_content_backlog SET ${fields.join(', ')}
          WHERE id = $1 AND client_id = $2 RETURNING *`,
        params
      );
      if (!result.rows[0]) throw new Error('backlog_not_found');
      return normalizeBacklog(result.rows[0]);
    },
    async listBacklog(clientId, filter = {}) {
      const clauses = ['client_id = $1'];
      const params = [Number(clientId)];
      if (filter.approvalState) {
        params.push(filter.approvalState);
        clauses.push(`approval_state = $${params.length}`);
      }
      if (filter.platform) {
        params.push(filter.platform);
        clauses.push(`target_platform = $${params.length}`);
      }
      const result = await db.query(
        `SELECT * FROM paige_content_backlog WHERE ${clauses.join(' AND ')}
         ORDER BY COALESCE(proposed_publish_at, created_at) ASC`,
        params
      );
      return result.rows.map(normalizeBacklog);
    },
    async getBacklogItem(id, clientId) {
      const result = await db.query(
        'SELECT * FROM paige_content_backlog WHERE id = $1 AND client_id = $2',
        [id, Number(clientId)]
      );
      return result.rows[0] ? normalizeBacklog(result.rows[0]) : null;
    },
    async upsertPlatformSignal(input) {
      await db.query(
        `INSERT INTO paige_social_platform_history (
          client_id, platform, content_category, format, dow, hour_local,
          sample_count, impressions_sum, engagement_sum, engagement_rate_avg, last_observed_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
        ON CONFLICT (client_id, platform, content_category, format, dow, hour_local) DO UPDATE SET
          sample_count = paige_social_platform_history.sample_count + EXCLUDED.sample_count,
          impressions_sum = paige_social_platform_history.impressions_sum + EXCLUDED.impressions_sum,
          engagement_sum = paige_social_platform_history.engagement_sum + EXCLUDED.engagement_sum,
          engagement_rate_avg = CASE
            WHEN paige_social_platform_history.impressions_sum + EXCLUDED.impressions_sum > 0
            THEN (paige_social_platform_history.engagement_sum + EXCLUDED.engagement_sum)::numeric
                 / (paige_social_platform_history.impressions_sum + EXCLUDED.impressions_sum)::numeric
            ELSE paige_social_platform_history.engagement_rate_avg
          END,
          last_observed_at = EXCLUDED.last_observed_at`,
        [
          input.clientId,
          input.platform,
          input.contentCategory || '',
          input.format || '',
          input.dow,
          input.hourLocal,
          input.sampleCount || 1,
          input.impressionsSum || 0,
          input.engagementSum || 0,
          input.engagementRateAvg ?? null,
          input.lastObservedAt || new Date().toISOString(),
        ]
      );
    },
    async listPlatformSignals(clientId, platform) {
      const params = [Number(clientId)];
      let sql = 'SELECT * FROM paige_social_platform_history WHERE client_id = $1';
      if (platform) {
        params.push(platform);
        sql += ` AND platform = $${params.length}`;
      }
      const result = await db.query(sql, params);
      return result.rows.map(normalizeSignal);
    },
  };
}

module.exports = {
  ensureOrganicSocialSchema,
  createMemoryOrganicSocialStore,
  createPostgresOrganicSocialStore,
  normalizeAsset,
  normalizeBacklog,
};
