'use strict';

/**
 * Artifact Registry — typed business artifacts for the Mission Artifact Bus
 * (SPEC-042 / ADR-028).
 */

const ARTIFACT_TYPES = Object.freeze({
  DISCOVERY_PROFILE: 'DiscoveryProfile',
  PROSPECT_LIST: 'ProspectList',
  COMPANY_INTELLIGENCE: 'CompanyIntelligence',
  OPPORTUNITY_RANKING: 'OpportunityRanking',
  SALES_INTELLIGENCE_PROFILE: 'SalesIntelligenceProfile',
  CAMPAIGN: 'Campaign',
  MAIL_PACKAGE: 'MailPackage',
  REVIEW_DECISION: 'ReviewDecision',
  EXECUTION_PACKAGE: 'ExecutionPackage',
  DELIVERY_RESULTS: 'DeliveryResults',
  OUTCOME_SUMMARY: 'OutcomeSummary',
});

/** Stage Library / PipelineGate snake_case → registry type */
const ALIAS_TO_TYPE = Object.freeze({
  discovery_profile: ARTIFACT_TYPES.DISCOVERY_PROFILE,
  prospect_list: ARTIFACT_TYPES.PROSPECT_LIST,
  enriched_list: ARTIFACT_TYPES.COMPANY_INTELLIGENCE,
  company_intelligence: ARTIFACT_TYPES.COMPANY_INTELLIGENCE,
  ranked_prospects: ARTIFACT_TYPES.OPPORTUNITY_RANKING,
  sales_intelligence_profile: ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE,
  sales_intelligence: ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE,
  campaign: ARTIFACT_TYPES.CAMPAIGN,
  campaign_draft: ARTIFACT_TYPES.CAMPAIGN,
  mail_package: ARTIFACT_TYPES.MAIL_PACKAGE,
  mail_packages: ARTIFACT_TYPES.MAIL_PACKAGE,
  review_decision: ARTIFACT_TYPES.REVIEW_DECISION,
  review_package: ARTIFACT_TYPES.REVIEW_DECISION,
  approved_campaign: ARTIFACT_TYPES.REVIEW_DECISION,
  ready_to_print_package: ARTIFACT_TYPES.EXECUTION_PACKAGE,
  execution_package: ARTIFACT_TYPES.EXECUTION_PACKAGE,
  delivery_results: ARTIFACT_TYPES.DELIVERY_RESULTS,
  outcome_summary: ARTIFACT_TYPES.OUTCOME_SUMMARY,
  knowledge: null, // not a bus business artifact in v1
});

const TYPE_TO_ALIAS = Object.freeze({
  [ARTIFACT_TYPES.DISCOVERY_PROFILE]: 'discovery_profile',
  [ARTIFACT_TYPES.PROSPECT_LIST]: 'prospect_list',
  [ARTIFACT_TYPES.COMPANY_INTELLIGENCE]: 'company_intelligence',
  [ARTIFACT_TYPES.OPPORTUNITY_RANKING]: 'ranked_prospects',
  [ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE]: 'sales_intelligence_profile',
  [ARTIFACT_TYPES.CAMPAIGN]: 'campaign',
  [ARTIFACT_TYPES.MAIL_PACKAGE]: 'mail_package',
  [ARTIFACT_TYPES.REVIEW_DECISION]: 'review_decision',
  [ARTIFACT_TYPES.EXECUTION_PACKAGE]: 'execution_package',
  [ARTIFACT_TYPES.DELIVERY_RESULTS]: 'delivery_results',
  [ARTIFACT_TYPES.OUTCOME_SUMMARY]: 'outcome_summary',
});

const SCHEMA_VERSION = '1.0.0';

/**
 * @typedef {object} ArtifactTypeDef
 * @property {string} name
 * @property {string} alias
 * @property {string} schemaVersion
 * @property {string[]} producers
 * @property {string[]} consumers
 * @property {(payload: object) => { ok: boolean, warnings: string[], errors: string[] }} validate
 */

/** @type {Record<string, ArtifactTypeDef>} */
const REGISTRY = Object.freeze({
  [ARTIFACT_TYPES.DISCOVERY_PROFILE]: Object.freeze({
    name: ARTIFACT_TYPES.DISCOVERY_PROFILE,
    alias: 'discovery_profile',
    schemaVersion: SCHEMA_VERSION,
    producers: ['mission_planner', 'prospect_discovery'],
    consumers: ['prospect_discovery'],
    validate: (payload) => {
      const errors = [];
      const warnings = [];
      if (!payload || typeof payload !== 'object') {
        errors.push('DiscoveryProfile payload required');
      } else if (!payload.id) {
        errors.push('DiscoveryProfile.id required');
      }
      return { ok: errors.length === 0, warnings, errors };
    },
  }),
  [ARTIFACT_TYPES.PROSPECT_LIST]: Object.freeze({
    name: ARTIFACT_TYPES.PROSPECT_LIST,
    alias: 'prospect_list',
    schemaVersion: SCHEMA_VERSION,
    // Discovery is one producer — operator ingress is first-class (SPEC-043 / ADR-029)
    producers: [
      'prospect_discovery',
      'operator_manual',
      'operator_import',
    ],
    consumers: ['company_enrichment', 'opportunity_ranking'],
    validate: (payload) => {
      const errors = [];
      const warnings = [];
      const prospects = Array.isArray(payload && payload.prospects)
        ? payload.prospects
        : [];
      const count =
        payload && payload.prospectCount != null
          ? Number(payload.prospectCount)
          : prospects.length;
      if (count <= 0) errors.push('ProspectList requires prospectCount > 0');
      if (payload && payload.targetCount != null && count < Number(payload.targetCount)) {
        warnings.push(
          `Requested ${payload.targetCount} prospects; found ${count}`
        );
      }
      // Shared business fields (Discovery + operator): Company Name required
      prospects.forEach((p, i) => {
        const name = prospectCompanyName(p);
        if (!name) {
          errors.push(`Prospect ${i + 1}: Company Name is required`);
          return;
        }
        if (!prospectField(p, ['website', 'url', 'domain'])) {
          warnings.push(`Prospect ${i + 1} (${name}): Website recommended`);
        }
        if (!prospectField(p, ['address', 'street', 'location'])) {
          warnings.push(`Prospect ${i + 1} (${name}): Address recommended`);
        }
      });
      return { ok: errors.length === 0, warnings, errors };
    },
  }),
  [ARTIFACT_TYPES.COMPANY_INTELLIGENCE]: Object.freeze({
    name: ARTIFACT_TYPES.COMPANY_INTELLIGENCE,
    alias: 'company_intelligence',
    schemaVersion: SCHEMA_VERSION,
    producers: ['company_enrichment'],
    consumers: ['opportunity_ranking'],
    validate: (payload) => {
      const errors = [];
      const warnings = [];
      const prospects = Array.isArray(payload && payload.prospects)
        ? payload.prospects
        : [];
      const count =
        payload && payload.enrichedCount != null
          ? Number(payload.enrichedCount)
          : prospects.length;
      if (count <= 0) errors.push('CompanyIntelligence requires enrichedCount > 0');
      return { ok: errors.length === 0, warnings, errors };
    },
  }),
  [ARTIFACT_TYPES.OPPORTUNITY_RANKING]: Object.freeze({
    name: ARTIFACT_TYPES.OPPORTUNITY_RANKING,
    alias: 'ranked_prospects',
    schemaVersion: SCHEMA_VERSION,
    producers: ['opportunity_ranking'],
    consumers: ['sales_intelligence', 'campaign_builder'],
    validate: (payload) => {
      const errors = [];
      const warnings = [];
      const prospects = Array.isArray(payload && payload.prospects)
        ? payload.prospects
        : [];
      const count =
        payload && payload.rankedCount != null
          ? Number(payload.rankedCount)
          : prospects.length;
      if (count <= 0) errors.push('OpportunityRanking requires rankedCount > 0');
      return { ok: errors.length === 0, warnings, errors };
    },
  }),
  [ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE]: Object.freeze({
    name: ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE,
    alias: 'sales_intelligence_profile',
    schemaVersion: SCHEMA_VERSION,
    producers: ['sales_intelligence'],
    consumers: ['campaign_builder', 'mail_package_generator', 'campaign_review'],
    validate: (payload) => {
      const errors = [];
      const warnings = [];
      const profiles = Array.isArray(payload && payload.profiles)
        ? payload.profiles
        : [];
      const count =
        payload && payload.profileCount != null
          ? Number(payload.profileCount)
          : profiles.length;
      if (count <= 0) {
        errors.push('SalesIntelligenceProfile requires profileCount > 0');
      }
      profiles.forEach((p, i) => {
        if (!p || !p.company) {
          errors.push(`Profile ${i + 1}: company is required`);
        }
        const claims = Array.isArray(p && p.personalization_claims)
          ? p.personalization_claims
          : [];
        const unverified = claims.filter((c) => c && c.verified && !c.evidenceRef);
        if (unverified.length) {
          warnings.push(
            `Profile ${i + 1}: verified claim missing evidenceRef`
          );
        }
      });
      return { ok: errors.length === 0, warnings, errors };
    },
  }),
  [ARTIFACT_TYPES.CAMPAIGN]: Object.freeze({
    name: ARTIFACT_TYPES.CAMPAIGN,
    alias: 'campaign',
    schemaVersion: SCHEMA_VERSION,
    producers: ['campaign_builder'],
    consumers: ['mail_package_generator', 'campaign_review'],
    validate: (payload) => {
      const errors = [];
      const warnings = [];
      const campaign = payload && (payload.campaign || payload);
      if (!campaign || typeof campaign !== 'object') {
        errors.push('Campaign payload required');
      } else {
        const count =
          campaign.prospectCount != null
            ? Number(campaign.prospectCount)
            : Array.isArray(campaign.prospects)
              ? campaign.prospects.length
              : 0;
        if (count <= 0) errors.push('Campaign requires prospectCount > 0');
      }
      return { ok: errors.length === 0, warnings, errors };
    },
  }),
  [ARTIFACT_TYPES.MAIL_PACKAGE]: Object.freeze({
    name: ARTIFACT_TYPES.MAIL_PACKAGE,
    alias: 'mail_package',
    schemaVersion: SCHEMA_VERSION,
    producers: ['mail_package_generator'],
    consumers: ['campaign_review', 'direct_mail_execution'],
    validate: (payload) => {
      const errors = [];
      if (!payload || typeof payload !== 'object') {
        errors.push('MailPackage payload required');
      }
      return { ok: errors.length === 0, warnings: [], errors };
    },
  }),
  [ARTIFACT_TYPES.REVIEW_DECISION]: Object.freeze({
    name: ARTIFACT_TYPES.REVIEW_DECISION,
    alias: 'review_decision',
    schemaVersion: SCHEMA_VERSION,
    producers: ['campaign_review'],
    consumers: ['direct_mail_execution'],
    validate: (payload) => {
      const errors = [];
      if (!payload || typeof payload !== 'object') {
        errors.push('ReviewDecision payload required');
      }
      return { ok: errors.length === 0, warnings: [], errors };
    },
  }),
  [ARTIFACT_TYPES.EXECUTION_PACKAGE]: Object.freeze({
    name: ARTIFACT_TYPES.EXECUTION_PACKAGE,
    alias: 'execution_package',
    schemaVersion: SCHEMA_VERSION,
    producers: ['direct_mail_execution'],
    consumers: [],
    validate: (payload) => {
      const errors = [];
      if (!payload || typeof payload !== 'object') {
        errors.push('ExecutionPackage payload required');
      }
      return { ok: errors.length === 0, warnings: [], errors };
    },
  }),
  [ARTIFACT_TYPES.DELIVERY_RESULTS]: Object.freeze({
    name: ARTIFACT_TYPES.DELIVERY_RESULTS,
    alias: 'delivery_results',
    schemaVersion: SCHEMA_VERSION,
    producers: ['direct_mail_execution'],
    consumers: ['outcome_intelligence'],
    validate: (payload) => {
      const errors = [];
      if (!payload || typeof payload !== 'object') {
        errors.push('DeliveryResults payload required');
      }
      return { ok: errors.length === 0, warnings: [], errors };
    },
  }),
  [ARTIFACT_TYPES.OUTCOME_SUMMARY]: Object.freeze({
    name: ARTIFACT_TYPES.OUTCOME_SUMMARY,
    alias: 'outcome_summary',
    schemaVersion: SCHEMA_VERSION,
    producers: ['outcome_intelligence'],
    consumers: ['operator_inbox'],
    validate: (payload) => {
      const errors = [];
      if (!payload || typeof payload !== 'object') {
        errors.push('OutcomeSummary payload required');
      }
      return { ok: errors.length === 0, warnings: [], errors };
    },
  }),
});

/**
 * Normalize Stage Library alias or registry name → ArtifactType or null.
 * @param {string} nameOrAlias
 * @returns {string|null}
 */
function resolveArtifactType(nameOrAlias) {
  if (!nameOrAlias) return null;
  const raw = String(nameOrAlias).trim();
  if (REGISTRY[raw]) return raw;
  if (Object.prototype.hasOwnProperty.call(ALIAS_TO_TYPE, raw)) {
    return ALIAS_TO_TYPE[raw];
  }
  // camelCase / PascalCase fuzzy: prospectList → ProspectList
  const pascal = raw
    .replace(/(^|_|-)([a-z])/g, (_, __, c) => c.toUpperCase())
    .replace(/_/g, '');
  if (REGISTRY[pascal]) return pascal;
  return null;
}

/**
 * @param {string} nameOrAlias
 * @returns {ArtifactTypeDef|null}
 */
function getArtifactTypeDef(nameOrAlias) {
  const type = resolveArtifactType(nameOrAlias);
  return type ? REGISTRY[type] : null;
}

/**
 * @returns {ArtifactTypeDef[]}
 */
function listArtifactTypes() {
  return Object.values(REGISTRY);
}

/**
 * Register or replace a type definition (extensibility).
 * @param {ArtifactTypeDef} def
 */
function registerArtifactType(def) {
  if (!def || !def.name) throw new Error('Artifact type name required');
  // Mutable extension bag kept separate so freeze of REGISTRY stays intact
  if (!globalThis.__pulseforgeArtifactExtensions) {
    globalThis.__pulseforgeArtifactExtensions = Object.create(null);
  }
  globalThis.__pulseforgeArtifactExtensions[def.name] = Object.freeze({ ...def });
  return globalThis.__pulseforgeArtifactExtensions[def.name];
}

/**
 * Lookup including runtime extensions.
 * @param {string} nameOrAlias
 */
function lookupArtifactType(nameOrAlias) {
  const base = getArtifactTypeDef(nameOrAlias);
  if (base) return base;
  const type = resolveArtifactType(nameOrAlias) || nameOrAlias;
  const ext =
    globalThis.__pulseforgeArtifactExtensions &&
    globalThis.__pulseforgeArtifactExtensions[type];
  return ext || null;
}

/**
 * Map capability outputs → typed artifact payload drafts for produced aliases.
 * @param {string[]} produces - Stage Library produces list
 * @param {object} outputs - CapabilityResult.outputs
 * @returns {{ artifactType: string, alias: string, payload: object }[]}
 */
function draftsFromCapabilityOutputs(produces, outputs) {
  const out = outputs || {};
  const list = Array.isArray(produces) ? produces : [];
  const drafts = [];
  const seen = new Set();

  for (const alias of list) {
    const artifactType = resolveArtifactType(alias);
    if (!artifactType || seen.has(artifactType)) continue;
    seen.add(artifactType);
    const payload = extractPayload(artifactType, out);
    if (payload == null) continue;
    drafts.push({ artifactType, alias: TYPE_TO_ALIAS[artifactType] || alias, payload });
  }

  return drafts.filter((d) => d.payload != null);
}

/**
 * @param {string} artifactType
 * @param {object} outputs
 */
function extractPayload(artifactType, outputs) {
  const out = outputs || {};
  switch (artifactType) {
    case ARTIFACT_TYPES.DISCOVERY_PROFILE:
      return out.discoveryProfile || null;
    case ARTIFACT_TYPES.PROSPECT_LIST:
      return {
        prospects: Array.isArray(out.prospects) ? out.prospects : [],
        prospectCount:
          out.prospectCount != null
            ? Number(out.prospectCount)
            : Array.isArray(out.prospects)
              ? out.prospects.length
              : 0,
        targetCount: out.targetCount != null ? Number(out.targetCount) : null,
        summary: out.summary || null,
        rejected: out.rejected || null,
        discoveryProfile: out.discoveryProfile || null,
      };
    case ARTIFACT_TYPES.COMPANY_INTELLIGENCE:
      return {
        prospects: Array.isArray(out.prospects) ? out.prospects : [],
        enrichedCount:
          out.enrichedCount != null
            ? Number(out.enrichedCount)
            : Array.isArray(out.prospects)
              ? out.prospects.length
              : 0,
      };
    case ARTIFACT_TYPES.OPPORTUNITY_RANKING:
      return {
        prospects: Array.isArray(out.prospects) ? out.prospects : [],
        rankedCount:
          out.rankedCount != null
            ? Number(out.rankedCount)
            : Array.isArray(out.prospects)
              ? out.prospects.length
              : 0,
      };
    case ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE:
      return {
        profiles: Array.isArray(out.profiles)
          ? out.profiles
          : Array.isArray(out.salesIntelligenceProfiles)
            ? out.salesIntelligenceProfiles
            : [],
        profileCount:
          out.profileCount != null
            ? Number(out.profileCount)
            : Array.isArray(out.profiles)
              ? out.profiles.length
              : Array.isArray(out.salesIntelligenceProfiles)
                ? out.salesIntelligenceProfiles.length
                : 0,
        sendableCount:
          out.sendableCount != null ? Number(out.sendableCount) : null,
        byProspectId: out.byProspectId || null,
      };
    case ARTIFACT_TYPES.CAMPAIGN:
      return {
        campaign: out.campaign,
        clientPlaybook: out.clientPlaybook || null,
        clientPlaybookId: out.clientPlaybookId || null,
        clientPlaybookVersion: out.clientPlaybookVersion || null,
      };
    case ARTIFACT_TYPES.MAIL_PACKAGE:
      return out.mailPackage || out.packages || out;
    case ARTIFACT_TYPES.REVIEW_DECISION:
      return out.reviewDecision || out.reviewPackage || out;
    case ARTIFACT_TYPES.EXECUTION_PACKAGE:
      return out.executionPackage || out;
    case ARTIFACT_TYPES.DELIVERY_RESULTS:
      return out.deliveryResults || out;
    case ARTIFACT_TYPES.OUTCOME_SUMMARY:
      return out.outcomeSummary || out.summary || out;
    default:
      return out;
  }
}

/**
 * Flatten latest artifacts into legacy priorOutputs shape for capability inputs.
 * Returns a deep clone so capabilities may mutate without touching the bus.
 * @param {Array<object>} artifacts - latest validated artifacts
 * @returns {object}
 */
function flattenArtifactsToOutputs(artifacts) {
  /** @type {object} */
  const prior = {};
  for (const art of artifacts || []) {
    if (!art || !art.payload) continue;
    const p = cloneJson(art.payload);
    switch (art.artifactType) {
      case ARTIFACT_TYPES.DISCOVERY_PROFILE:
        prior.discoveryProfile = p;
        break;
      case ARTIFACT_TYPES.PROSPECT_LIST:
        prior.prospects = p.prospects;
        prior.prospectCount = p.prospectCount;
        prior.targetCount = p.targetCount;
        prior.summary = p.summary;
        prior.rejected = p.rejected;
        if (p.discoveryProfile) prior.discoveryProfile = p.discoveryProfile;
        break;
      case ARTIFACT_TYPES.COMPANY_INTELLIGENCE:
        prior.prospects = p.prospects;
        prior.enrichedCount = p.enrichedCount;
        break;
      case ARTIFACT_TYPES.OPPORTUNITY_RANKING:
        prior.prospects = p.prospects;
        prior.rankedCount = p.rankedCount;
        break;
      case ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE:
        prior.salesIntelligenceProfiles = p.profiles;
        prior.profiles = p.profiles;
        prior.profileCount = p.profileCount;
        prior.sendableCount = p.sendableCount;
        if (p.byProspectId) prior.salesIntelligenceByProspectId = p.byProspectId;
        break;
      case ARTIFACT_TYPES.CAMPAIGN:
        prior.campaign = p.campaign;
        if (p.clientPlaybook) prior.clientPlaybook = p.clientPlaybook;
        if (p.clientPlaybookId) prior.clientPlaybookId = p.clientPlaybookId;
        if (p.clientPlaybookVersion) {
          prior.clientPlaybookVersion = p.clientPlaybookVersion;
        }
        break;
      case ARTIFACT_TYPES.MAIL_PACKAGE:
        prior.mailPackage = p;
        break;
      case ARTIFACT_TYPES.REVIEW_DECISION:
        prior.reviewDecision = p;
        prior.reviewPackage = p;
        break;
      case ARTIFACT_TYPES.EXECUTION_PACKAGE:
        prior.executionPackage = p;
        break;
      case ARTIFACT_TYPES.DELIVERY_RESULTS:
        prior.deliveryResults = p;
        break;
      case ARTIFACT_TYPES.OUTCOME_SUMMARY:
        prior.outcomeSummary = p;
        break;
      default:
        break;
    }
  }
  return prior;
}

function cloneJson(value) {
  if (value === null || value === undefined) return value;
  try {
    return JSON.parse(JSON.stringify(value));
  } catch {
    return value;
  }
}

function prospectCompanyName(p) {
  if (!p || typeof p !== 'object') return '';
  return String(
    p.companyName || p.company || p.name || p.businessName || ''
  ).trim();
}

function prospectField(p, keys) {
  if (!p || typeof p !== 'object') return '';
  for (const k of keys) {
    if (p[k] != null && String(p[k]).trim()) return String(p[k]).trim();
  }
  return '';
}

/**
 * Human summary for workspace cards.
 * @param {object} artifact
 */
function summarizeArtifact(artifact) {
  if (!artifact) return '';
  const p = artifact.payload || {};
  switch (artifact.artifactType) {
    case ARTIFACT_TYPES.PROSPECT_LIST:
      return `${p.prospectCount != null ? p.prospectCount : (p.prospects || []).length} Prospects`;
    case ARTIFACT_TYPES.COMPANY_INTELLIGENCE:
      return `${p.enrichedCount != null ? p.enrichedCount : (p.prospects || []).length} Enriched`;
    case ARTIFACT_TYPES.OPPORTUNITY_RANKING:
      return `${p.rankedCount != null ? p.rankedCount : (p.prospects || []).length} Ranked`;
    case ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE:
      return `${p.profileCount != null ? p.profileCount : (p.profiles || []).length} Sales Profiles`;
    case ARTIFACT_TYPES.CAMPAIGN: {
      const c = p.campaign || p;
      const n =
        c.prospectCount != null
          ? c.prospectCount
          : Array.isArray(c.prospects)
            ? c.prospects.length
            : 0;
      return c.name ? `${c.name} (${n})` : `${n} Campaign Prospects`;
    }
    case ARTIFACT_TYPES.DISCOVERY_PROFILE:
      return p.name || p.id || 'Discovery Profile';
    default:
      return artifact.artifactType;
  }
}

module.exports = {
  ARTIFACT_TYPES,
  ALIAS_TO_TYPE,
  TYPE_TO_ALIAS,
  SCHEMA_VERSION,
  REGISTRY,
  resolveArtifactType,
  getArtifactTypeDef,
  lookupArtifactType,
  listArtifactTypes,
  registerArtifactType,
  draftsFromCapabilityOutputs,
  extractPayload,
  flattenArtifactsToOutputs,
  summarizeArtifact,
};
