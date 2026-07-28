'use strict';

/**
 * Mission Artifact Bus — immutable, versioned business state (SPEC-042 / ADR-028).
 */

const { newId } = require('./types');
const {
  ARTIFACT_VALIDATION_STATUS,
} = require('./PipelineGate');
const {
  ARTIFACT_TYPES,
  SCHEMA_VERSION,
  resolveArtifactType,
  lookupArtifactType,
  draftsFromCapabilityOutputs,
  flattenArtifactsToOutputs,
  summarizeArtifact,
  TYPE_TO_ALIAS,
} = require('./ArtifactRegistry');

const ARTIFACT_EVENTS = Object.freeze({
  PUBLISHED: 'ArtifactPublished',
  VALIDATED: 'ArtifactValidated',
  QUARANTINED: 'ArtifactQuarantined',
  SUPERSEDED: 'ArtifactSuperseded',
  CONSUMED: 'ArtifactConsumed',
});

const CONSUMABLE_STATUSES = Object.freeze([
  ARTIFACT_VALIDATION_STATUS.VALID,
  ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS,
]);

function isConsumable(status) {
  return CONSUMABLE_STATUSES.includes(status);
}

class ArtifactBus {
  /**
   * @param {object} [opts]
   * @param {object} [opts.snapshot] - restored deliverables.artifactBus
   */
  constructor(opts = {}) {
    /** @type {Map<string, object>} */
    this._byId = new Map();
    /** @type {Map<string, Map<string, string[]>>} missionId → type → ordered ids */
    this._index = new Map();
    /** @type {object[]} */
    this._events = [];
    if (opts.snapshot) this._hydrate(opts.snapshot);
  }

  /**
   * Validate a draft artifact (registry minimum schema).
   * Does not publish.
   * @param {object} draft
   * @returns {{ status: string, ok: boolean, warnings: string[], errors: string[], def: object|null }}
   */
  validateArtifact(draft = {}) {
    const artifactType = resolveArtifactType(draft.artifactType || draft.type);
    const def = lookupArtifactType(artifactType);
    if (!artifactType || !def) {
      return {
        status: ARTIFACT_VALIDATION_STATUS.INVALID,
        ok: false,
        warnings: [],
        errors: [`Unknown artifact type: ${draft.artifactType || draft.type}`],
        def: null,
      };
    }
    const result = def.validate(draft.payload);
    if (!result.ok) {
      return {
        status: ARTIFACT_VALIDATION_STATUS.INVALID,
        ok: false,
        warnings: result.warnings || [],
        errors: result.errors || [],
        def,
      };
    }
    if ((result.warnings || []).length) {
      return {
        status: ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS,
        ok: true,
        warnings: result.warnings,
        errors: [],
        def,
      };
    }
    return {
      status: ARTIFACT_VALIDATION_STATUS.VALID,
      ok: true,
      warnings: [],
      errors: [],
      def,
    };
  }

  /**
   * Publish an immutable artifact revision.
   * @param {object} input
   * @param {string} input.missionId
   * @param {string} [input.stageId]
   * @param {string} input.artifactType - registry name or alias
   * @param {string} [input.producer]
   * @param {object} input.payload
   * @param {Array<{artifactType?: string, artifactId?: string, revision?: number}>} [input.dependencies]
   * @param {object} [input.metadata]
   * @param {string} [input.validationStatus] - override (e.g. quarantined from gate)
   * @param {boolean} [input.skipRegistryValidation] - when gate already validated
   * @returns {object} published artifact
   */
  publishArtifact(input = {}) {
    const missionId = input.missionId;
    if (!missionId) throw new Error('publishArtifact requires missionId');

    const artifactType = resolveArtifactType(
      input.artifactType || input.type
    );
    if (!artifactType) {
      throw new Error(
        `Unknown artifact type: ${input.artifactType || input.type}`
      );
    }

    let validationStatus = input.validationStatus || null;
    let validation = null;

    if (validationStatus === ARTIFACT_VALIDATION_STATUS.QUARANTINED) {
      validation = {
        status: ARTIFACT_VALIDATION_STATUS.QUARANTINED,
        ok: false,
        warnings: [],
        errors: input.validationErrors || ['Quarantined by Pipeline Gate'],
        def: lookupArtifactType(artifactType),
      };
    } else if (!input.skipRegistryValidation) {
      validation = this.validateArtifact({
        artifactType,
        payload: input.payload,
      });
      if (!validation.ok && !validationStatus) {
        validationStatus = ARTIFACT_VALIDATION_STATUS.QUARANTINED;
      } else if (!validationStatus) {
        validationStatus = validation.status;
      }
    } else {
      validationStatus =
        validationStatus || ARTIFACT_VALIDATION_STATUS.VALID;
      validation = {
        status: validationStatus,
        ok: isConsumable(validationStatus),
        warnings: input.warnings || [],
        errors: input.validationErrors || [],
        def: lookupArtifactType(artifactType),
      };
    }

    // Enforce quarantine when invalid
    if (
      validation &&
      !validation.ok &&
      validationStatus !== ARTIFACT_VALIDATION_STATUS.QUARANTINED
    ) {
      validationStatus = ARTIFACT_VALIDATION_STATUS.QUARANTINED;
    }

    const history = this.getArtifactHistory(missionId, artifactType);
    const revision = history.length + 1;
    const previous = history[history.length - 1] || null;

    const artifact = Object.freeze({
      id: input.id || newId('art'),
      missionId: String(missionId),
      stageId: input.stageId || null,
      revision,
      artifactType,
      alias: TYPE_TO_ALIAS[artifactType] || null,
      schemaVersion:
        (validation && validation.def && validation.def.schemaVersion) ||
        SCHEMA_VERSION,
      producer: input.producer || null,
      createdAt: input.createdAt || new Date().toISOString(),
      validationStatus,
      dependencies: Object.freeze(
        (input.dependencies || []).map((d) => Object.freeze({ ...d }))
      ),
      metadata: Object.freeze({ ...(input.metadata || {}) }),
      payload: deepFreezeClone(input.payload),
      summary: summarizeArtifact({
        artifactType,
        payload: input.payload,
      }),
      supersededBy: null,
    });

    this._byId.set(artifact.id, artifact);
    this._indexRevision(missionId, artifactType, artifact.id);

    if (previous && isConsumable(previous.validationStatus)) {
      const superseded = Object.freeze({
        ...previous,
        supersededBy: artifact.id,
      });
      this._byId.set(previous.id, superseded);
      this._emit({
        type: ARTIFACT_EVENTS.SUPERSEDED,
        missionId,
        artifactId: previous.id,
        supersededBy: artifact.id,
        artifactType,
        revision: previous.revision,
      });
    }

    this._emit({
      type: ARTIFACT_EVENTS.PUBLISHED,
      missionId,
      artifactId: artifact.id,
      artifactType,
      revision,
      validationStatus,
    });

    if (validationStatus === ARTIFACT_VALIDATION_STATUS.QUARANTINED) {
      this._emit({
        type: ARTIFACT_EVENTS.QUARANTINED,
        missionId,
        artifactId: artifact.id,
        artifactType,
        revision,
        errors: (validation && validation.errors) || [],
      });
    } else {
      this._emit({
        type: ARTIFACT_EVENTS.VALIDATED,
        missionId,
        artifactId: artifact.id,
        artifactType,
        revision,
        validationStatus,
      });
    }

    return artifact;
  }

  /**
   * @param {string} artifactId
   * @returns {object|null}
   */
  getArtifact(artifactId) {
    return this._byId.get(artifactId) || null;
  }

  /**
   * Newest validated revision. Quarantined artifacts are invisible.
   * @param {string} missionId
   * @param {string} artifactTypeOrAlias
   * @param {object} [opts]
   * @param {boolean} [opts.includeQuarantined=false]
   * @returns {object|null}
   */
  getLatestArtifact(missionId, artifactTypeOrAlias, opts = {}) {
    const history = this.getArtifactHistory(missionId, artifactTypeOrAlias);
    if (!history.length) return null;
    if (opts.includeQuarantined) return history[history.length - 1];
    for (let i = history.length - 1; i >= 0; i -= 1) {
      if (isConsumable(history[i].validationStatus)) return history[i];
    }
    return null;
  }

  /**
   * Full revision history (including quarantined), oldest → newest.
   * @param {string} missionId
   * @param {string} artifactTypeOrAlias
   * @returns {object[]}
   */
  getArtifactHistory(missionId, artifactTypeOrAlias) {
    const artifactType = resolveArtifactType(artifactTypeOrAlias);
    if (!artifactType || !missionId) return [];
    const byType = this._index.get(String(missionId));
    if (!byType) return [];
    const ids = byType.get(artifactType) || [];
    return ids.map((id) => this._byId.get(id)).filter(Boolean);
  }

  /**
   * Resolve + mark consumed for audit.
   * @param {string} missionId
   * @param {string} artifactTypeOrAlias
   * @param {object} [opts]
   * @param {string} [opts.consumer]
   * @param {string} [opts.stageId]
   * @returns {object|null}
   */
  consumeArtifact(missionId, artifactTypeOrAlias, opts = {}) {
    const artifact = this.getLatestArtifact(missionId, artifactTypeOrAlias);
    if (!artifact) return null;
    this._emit({
      type: ARTIFACT_EVENTS.CONSUMED,
      missionId,
      artifactId: artifact.id,
      artifactType: artifact.artifactType,
      revision: artifact.revision,
      consumer: opts.consumer || null,
      stageId: opts.stageId || null,
    });
    return artifact;
  }

  /**
   * Compare two artifact revisions (same or different types).
   * @param {string} idA
   * @param {string} idB
   */
  compareArtifacts(idA, idB) {
    const a = this.getArtifact(idA);
    const b = this.getArtifact(idB);
    if (!a || !b) {
      return {
        ok: false,
        error: 'One or both artifacts not found',
        a: a || null,
        b: b || null,
      };
    }
    const payloadA = a.payload || {};
    const payloadB = b.payload || {};
    const changedKeys = diffKeys(payloadA, payloadB);
    return {
      ok: true,
      a: {
        id: a.id,
        artifactType: a.artifactType,
        revision: a.revision,
        validationStatus: a.validationStatus,
        summary: a.summary,
      },
      b: {
        id: b.id,
        artifactType: b.artifactType,
        revision: b.revision,
        validationStatus: b.validationStatus,
        summary: b.summary,
      },
      sameType: a.artifactType === b.artifactType,
      changedKeys,
      highlights: buildCompareHighlights(a, b, changedKeys),
    };
  }

  /**
   * Build a replay plan starting from a given artifact revision.
   * Upstream dependency producers are skipped; the producing stage re-runs.
   * @param {string} missionId
   * @param {string} artifactId
   * @param {object} [opts]
   * @param {string[]} [opts.planStageIds] - ordered stage ids from mission plan
   * @returns {object}
   */
  replayFromArtifact(missionId, artifactId, opts = {}) {
    const artifact = this.getArtifact(artifactId);
    if (!artifact || String(artifact.missionId) !== String(missionId)) {
      return {
        ok: false,
        error: 'Artifact not found for mission',
        startStageId: null,
        reuseArtifactIds: [],
        skipStageIds: [],
      };
    }

    const planStageIds = Array.isArray(opts.planStageIds)
      ? opts.planStageIds
      : [];
    const startStageId = artifact.stageId;
    const startIdx = startStageId
      ? planStageIds.indexOf(startStageId)
      : -1;

    const skipStageIds =
      startIdx > 0 ? planStageIds.slice(0, startIdx) : [];

    // Reuse latest validated artifacts produced by skipped stages
    const reuseArtifactIds = [];
    const seenTypes = new Set();
    for (const dep of artifact.dependencies || []) {
      if (dep.artifactId) {
        reuseArtifactIds.push(dep.artifactId);
        seenTypes.add(dep.artifactType);
      } else if (dep.artifactType) {
        const latest = this.getLatestArtifact(missionId, dep.artifactType);
        if (latest) {
          reuseArtifactIds.push(latest.id);
          seenTypes.add(latest.artifactType);
        }
      }
    }

    // Also reuse all latest consumable artifacts whose producer stage is skipped
    for (const type of Object.values(ARTIFACT_TYPES)) {
      if (seenTypes.has(type)) continue;
      const latest = this.getLatestArtifact(missionId, type);
      if (!latest) continue;
      if (latest.stageId && skipStageIds.includes(latest.stageId)) {
        reuseArtifactIds.push(latest.id);
      }
    }

    return {
      ok: true,
      startStageId,
      startRevision: artifact.revision,
      artifactType: artifact.artifactType,
      artifactId: artifact.id,
      reuseArtifactIds: [...new Set(reuseArtifactIds)],
      skipStageIds,
      note:
        'Replay reuses upstream validated artifacts and re-runs from the producing stage',
    };
  }

  /**
   * Latest consumable artifact per type for a mission (workspace).
   * @param {string} missionId
   * @returns {object[]}
   */
  listMissionArtifacts(missionId) {
    const byType = this._index.get(String(missionId));
    if (!byType) return [];
    const list = [];
    for (const artifactType of byType.keys()) {
      const consumable = this.getLatestArtifact(missionId, artifactType);
      const latest =
        consumable ||
        this.getLatestArtifact(missionId, artifactType, {
          includeQuarantined: true,
        });
      if (!latest) continue;
      const history = this.getArtifactHistory(missionId, artifactType);
      list.push({
        ...publicArtifactView(latest),
        revisionCount: history.length,
        consumable: isConsumable(latest.validationStatus),
        history: history.map(publicArtifactView),
      });
    }
    return list.sort((a, b) =>
      String(a.artifactType).localeCompare(String(b.artifactType))
    );
  }

  /**
   * Dependency graph edges from artifact dependencies.
   * @param {string} missionId
   */
  getArtifactGraph(missionId) {
    const nodes = [];
    const edges = [];
    const byType = this._index.get(String(missionId));
    if (!byType) return { nodes, edges };

    for (const artifactType of byType.keys()) {
      const latest = this.getLatestArtifact(missionId, artifactType, {
        includeQuarantined: true,
      });
      if (!latest) continue;
      nodes.push({
        id: latest.id,
        artifactType: latest.artifactType,
        revision: latest.revision,
        validationStatus: latest.validationStatus,
        stageId: latest.stageId,
        summary: latest.summary,
      });
      for (const dep of latest.dependencies || []) {
        const depId =
          dep.artifactId ||
          (dep.artifactType
            ? (this.getLatestArtifact(missionId, dep.artifactType) || {}).id
            : null);
        if (depId) {
          edges.push({
            from: depId,
            to: latest.id,
            artifactType: dep.artifactType || null,
          });
        }
      }
    }
    return { nodes, edges };
  }

  /**
   * Publish drafts derived from a gated capability result.
   * @param {object} input
   */
  publishFromGate(input = {}) {
    const {
      missionId,
      stageId,
      producer,
      produces,
      outputs,
      gate,
      priorArtifacts,
    } = input;
    const publish = gate && gate.publishOutputs;
    const quarantined = !publish;
    const drafts = draftsFromCapabilityOutputs(produces || [], outputs || {});
    const published = [];
    const deps = (priorArtifacts || [])
      .filter(Boolean)
      .map((a) => ({
        artifactType: a.artifactType,
        artifactId: a.id,
        revision: a.revision,
      }));

    const statusFromGate = (draftType) => {
      const stamped = [
        ...((gate && gate.publishedArtifacts) || []),
        ...((gate && gate.quarantinedArtifacts) || []),
      ];
      const match = stamped.find(
        (a) =>
          resolveArtifactType(a.type) === draftType || a.type === draftType
      );
      if (match && match.validationStatus) return match.validationStatus;
      if (quarantined) return ARTIFACT_VALIDATION_STATUS.QUARANTINED;
      if (gate && gate.outcome === 'completed_with_warnings') {
        return ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS;
      }
      return ARTIFACT_VALIDATION_STATUS.VALID;
    };

    for (const draft of drafts) {
      const validationStatus = statusFromGate(draft.artifactType);
      const art = this.publishArtifact({
        missionId,
        stageId,
        artifactType: draft.artifactType,
        producer,
        payload: draft.payload,
        dependencies: deps,
        metadata: {
          gateOutcome: gate && gate.outcome,
          alias: draft.alias,
        },
        validationStatus,
        skipRegistryValidation: true,
        warnings: (gate && gate.warnings) || [],
        validationErrors: (gate && gate.blockingIssues) || [],
      });
      published.push(art);
    }
    return published;
  }

  /**
   * Resolve stage consumes → capability priorOutputs adapter.
   * @param {string} missionId
   * @param {string[]} consumes
   * @param {object} [opts]
   */
  resolveInputs(missionId, consumes, opts = {}) {
    const consumed = [];
    for (const alias of consumes || []) {
      const type = resolveArtifactType(alias);
      if (!type) continue;
      const art = this.consumeArtifact(missionId, type, {
        consumer: opts.consumer,
        stageId: opts.stageId,
      });
      if (art) consumed.push(art);
    }
    // Also include any other latest consumables for flatten completeness
    const byType = this._index.get(String(missionId));
    if (byType) {
      for (const type of byType.keys()) {
        if (consumed.some((a) => a.artifactType === type)) continue;
        const art = this.getLatestArtifact(missionId, type);
        if (art) consumed.push(art);
      }
    }
    return {
      artifacts: consumed,
      priorOutputs: flattenArtifactsToOutputs(consumed),
    };
  }

  /**
   * Persistable snapshot.
   */
  toJSON() {
    return {
      version: 1,
      artifacts: [...this._byId.values()].map((a) => ({
        ...a,
        dependencies: [...(a.dependencies || [])],
        metadata: { ...(a.metadata || {}) },
        payload: a.payload,
      })),
      events: [...this._events],
    };
  }

  /**
   * @param {object} snapshot
   */
  static fromJSON(snapshot) {
    return new ArtifactBus({ snapshot });
  }

  /** Test / workspace helper */
  events(missionId) {
    if (!missionId) return [...this._events];
    return this._events.filter((e) => String(e.missionId) === String(missionId));
  }

  _hydrate(snapshot) {
    if (!snapshot || !Array.isArray(snapshot.artifacts)) return;
    for (const raw of snapshot.artifacts) {
      const artifact = Object.freeze({
        ...raw,
        dependencies: Object.freeze([...(raw.dependencies || [])]),
        metadata: Object.freeze({ ...(raw.metadata || {}) }),
        payload: deepFreezeClone(raw.payload),
      });
      this._byId.set(artifact.id, artifact);
      this._indexRevision(artifact.missionId, artifact.artifactType, artifact.id);
    }
    this._events = Array.isArray(snapshot.events) ? [...snapshot.events] : [];
  }

  _indexRevision(missionId, artifactType, id) {
    const mid = String(missionId);
    if (!this._index.has(mid)) this._index.set(mid, new Map());
    const byType = this._index.get(mid);
    if (!byType.has(artifactType)) byType.set(artifactType, []);
    const list = byType.get(artifactType);
    if (!list.includes(id)) list.push(id);
  }

  _emit(event) {
    this._events.push({
      ...event,
      at: event.at || new Date().toISOString(),
    });
  }
}

function publicArtifactView(artifact) {
  if (!artifact) return null;
  return {
    id: artifact.id,
    missionId: artifact.missionId,
    stageId: artifact.stageId,
    revision: artifact.revision,
    artifactType: artifact.artifactType,
    alias: artifact.alias,
    schemaVersion: artifact.schemaVersion,
    producer: artifact.producer,
    createdAt: artifact.createdAt,
    validationStatus: artifact.validationStatus,
    dependencies: [...(artifact.dependencies || [])],
    metadata: { ...(artifact.metadata || {}) },
    summary: artifact.summary,
    supersededBy: artifact.supersededBy || null,
    payload: artifact.payload,
  };
}

function deepFreezeClone(value) {
  if (value === null || value === undefined) return value;
  if (typeof value !== 'object') return value;
  const clone = Array.isArray(value)
    ? value.map((v) => deepFreezeClone(v))
    : Object.fromEntries(
        Object.entries(value).map(([k, v]) => [k, deepFreezeClone(v)])
      );
  return Object.freeze(clone);
}

function diffKeys(a, b) {
  const keys = new Set([...Object.keys(a || {}), ...Object.keys(b || {})]);
  const changed = [];
  for (const k of keys) {
    try {
      if (JSON.stringify(a[k]) !== JSON.stringify(b[k])) changed.push(k);
    } catch {
      if (a[k] !== b[k]) changed.push(k);
    }
  }
  return changed;
}

function buildCompareHighlights(a, b, changedKeys) {
  const highlights = {
    changedProspects: false,
    changedScores: false,
    changedPersonalization: false,
    changedLetters: false,
    changedKeys,
  };
  if (changedKeys.includes('prospects') || changedKeys.includes('campaign')) {
    highlights.changedProspects = true;
  }
  const payloadKeys = changedKeys.join(' ');
  if (/score|rank/i.test(payloadKeys)) highlights.changedScores = true;
  if (/personalization|openingHook/i.test(payloadKeys)) {
    highlights.changedPersonalization = true;
  }
  if (/letter|mail|body|package/i.test(payloadKeys)) {
    highlights.changedLetters = true;
  }
  // Deep peek for campaign mail merge
  try {
    const mailA = (a.payload && a.payload.campaign && a.payload.campaign.mailMerge) || [];
    const mailB = (b.payload && b.payload.campaign && b.payload.campaign.mailMerge) || [];
    if (JSON.stringify(mailA) !== JSON.stringify(mailB)) {
      highlights.changedPersonalization = true;
      highlights.changedLetters = true;
    }
  } catch {
    // ignore
  }
  return highlights;
}

function createArtifactBus(opts) {
  return new ArtifactBus(opts);
}

module.exports = {
  ArtifactBus,
  createArtifactBus,
  ARTIFACT_EVENTS,
  CONSUMABLE_STATUSES,
  isConsumable,
  publicArtifactView,
};
