'use strict';

/**
 * Typed Artifact Validator — boundary validation before Artifact Bus
 * (SPEC-052 / ADR-036).
 *
 * Pipeline: Identify Type → Schema → Semantic → Compatibility → PASS|FAIL
 * Natural language is not an artifact.
 */

const {
  ARTIFACT_TYPES,
  SCHEMA_VERSION,
  resolveArtifactType,
  lookupArtifactType,
} = require('./ArtifactRegistry');
const {
  ARTIFACT_VALIDATION_STATUS,
} = require('./PipelineGate');

const VALIDATION_STAGES = Object.freeze({
  IDENTIFY: 'identify',
  SCHEMA: 'schema',
  SEMANTIC: 'semantic',
  COMPATIBILITY: 'compatibility',
});

/** Mission / instruction language — not a company name. */
const NL_INSTRUCTION_PATTERN =
  /\b(build|create|launch|prepare|reuse|execute|run|complete|monitor|summarize|review|using|existing|pipeline|campaign|mission|import|prospect\s*list|mail\s*package|sales\s*intelligence|human\s*test|ready\s*to\s*print|dry\s*run)\b/i;

const COMPANY_ENTITY_SUFFIX =
  /\b(llc|inc\.?|corp\.?|ltd\.?|co\.?|company|law|llp|cpa|pc|pllc|group|partners|associates|clinic|salon|gym|studio|firm|office|dental|plumbing|hvac|roofing|services)\b/i;

const NL_LEADING_IMPERATIVE =
  /^(reuse|use|import|please|make\s+sure|do\s+not|don't|execute|build|create|launch|prepare|review|monitor|summarize|complete|run)\b/i;

/**
 * True when a string looks like free-form prose / mission language,
 * not a structured company (or similar) field.
 * @param {string} value
 * @returns {boolean}
 */
function looksLikeNaturalLanguage(value) {
  const s = String(value || '').trim();
  if (!s) return true;
  if (/[.!?]/.test(s)) return true;
  if (NL_LEADING_IMPERATIVE.test(s)) return true;
  if (NL_INSTRUCTION_PATTERN.test(s) && !COMPANY_ENTITY_SUFFIX.test(s)) {
    return true;
  }
  const words = s.split(/\s+/).filter(Boolean);
  if (words.length >= 6 && !COMPANY_ENTITY_SUFFIX.test(s)) return true;
  // Numbered list placeholders ("Prospect 1") are not companies.
  if (/^prospect\s*\d+$/i.test(s)) return true;
  return false;
}

/**
 * Company name suitable for ProspectList (not NL).
 * @param {string} value
 * @returns {boolean}
 */
function isViableCompanyName(value) {
  const s = String(value || '').trim();
  if (!s) return false;
  if (looksLikeNaturalLanguage(s)) return false;
  return true;
}

/**
 * Extract a display company name from a prospect-like row.
 * @param {object|string} p
 * @returns {string}
 */
function companyNameFromRow(p) {
  if (p == null) return '';
  if (typeof p === 'string') return String(p).trim();
  return String(
    p.companyName ||
      p.company_name ||
      p.name ||
      (p.company && (p.company.name || p.company)) ||
      ''
  ).trim();
}

/**
 * Semantic validation for ProspectList payloads.
 * @param {object} payload
 * @returns {{ ok: boolean, errors: string[], warnings: string[], validCount: number }}
 */
function validateProspectListSemantics(payload) {
  const errors = [];
  const warnings = [];
  const prospects = Array.isArray(payload && payload.prospects)
    ? payload.prospects
    : [];

  if (!prospects.length) {
    errors.push('No valid prospect rows detected.');
    return { ok: false, errors, warnings, validCount: 0 };
  }

  let nlCount = 0;
  let validCount = 0;
  prospects.forEach((p, i) => {
    const name = companyNameFromRow(p);
    if (!name) {
      errors.push(`Prospect ${i + 1}: Company Name is required`);
      return;
    }
    if (looksLikeNaturalLanguage(name)) {
      nlCount += 1;
      errors.push(
        `Prospect ${i + 1}: Input is natural language ("${truncate(name, 60)}").`
      );
      return;
    }
    validCount += 1;
  });

  if (nlCount > 0 && validCount === 0) {
    if (!errors.some((e) => /natural language/i.test(e))) {
      errors.unshift('Input is natural language.');
    }
    if (!errors.some((e) => /No valid prospect/i.test(e))) {
      errors.push('No valid prospect rows detected.');
    }
  } else if (nlCount > 0 && validCount > 0) {
    warnings.push(
      `${nlCount} row(s) look like natural language and must not be treated as companies`
    );
  }

  if (validCount === 0) {
    errors.push('ProspectList requires at least one valid company.');
  }

  return {
    ok: errors.length === 0 && validCount > 0,
    errors,
    warnings,
    validCount,
  };
}

/**
 * Semantic validation for Campaign.
 * @param {object} payload
 */
function validateCampaignSemantics(payload) {
  const errors = [];
  const warnings = [];
  const campaign = payload && (payload.campaign || payload);
  if (!campaign || typeof campaign !== 'object' || Array.isArray(campaign)) {
    errors.push('Campaign requires a valid Campaign object.');
    return { ok: false, errors, warnings };
  }
  const count =
    campaign.prospectCount != null
      ? Number(campaign.prospectCount)
      : Array.isArray(campaign.prospects)
        ? campaign.prospects.length
        : 0;
  if (!(count > 0)) {
    errors.push('Campaign requires prospectCount > 0.');
  }
  return { ok: errors.length === 0, errors, warnings };
}

/**
 * Semantic validation for Sales Intelligence profiles.
 * @param {object} payload
 */
function validateSalesIntelligenceSemantics(payload) {
  const errors = [];
  const warnings = [];
  const profiles = Array.isArray(payload && payload.profiles)
    ? payload.profiles
    : [];
  if (!profiles.length) {
    errors.push('SalesIntelligenceProfile requires at least one profile.');
    return { ok: false, errors, warnings };
  }
  profiles.forEach((p, i) => {
    if (!p || !p.company) {
      errors.push(`Profile ${i + 1}: company is required`);
    }
    const hasReasoning =
      (p && p.messaging_strategy) ||
      (p && p.messagingStrategy) ||
      (p && p.opening) ||
      (p && p.reasoning) ||
      (Array.isArray(p && p.personalization_claims) &&
        p.personalization_claims.length > 0) ||
      (Array.isArray(p && p.personalizationClaims) &&
        p.personalizationClaims.length > 0);
    if (!hasReasoning) {
      errors.push(
        `Profile ${i + 1}: required reasoning fields missing (messaging strategy, opening, or personalization claims)`
      );
    }
  });
  return { ok: errors.length === 0, errors, warnings };
}

/**
 * Semantic validation for Mail Package — renderable outputs required.
 * @param {object} payload
 */
function validateMailPackageSemantics(payload) {
  const errors = [];
  const warnings = [];
  if (!payload || typeof payload !== 'object') {
    errors.push('MailPackage payload required');
    return { ok: false, errors, warnings };
  }
  const packages = Array.isArray(payload.packages)
    ? payload.packages
    : Array.isArray(payload.mailPackages)
      ? payload.mailPackages
      : null;
  const single =
    payload.letter ||
    payload.content ||
    payload.body ||
    payload.html ||
    payload.renderable;

  if (packages) {
    if (!packages.length) {
      errors.push('MailPackage requires at least one renderable package.');
    } else {
      const renderable = packages.filter(
        (pkg) =>
          pkg &&
          (pkg.letter ||
            pkg.content ||
            pkg.body ||
            pkg.html ||
            pkg.renderable ||
            pkg.preview)
      );
      if (!renderable.length) {
        errors.push(
          'MailPackage packages must include renderable letter/content outputs.'
        );
      }
    }
  } else if (!single) {
    // Stubs may publish a thin envelope — warn, do not hard-fail if object has id/type markers
    if (payload.id || payload.packageId || payload.campaignId) {
      warnings.push('MailPackage missing explicit renderable letter/content');
    } else {
      errors.push('MailPackage requires renderable outputs.');
    }
  }
  return { ok: errors.length === 0, errors, warnings };
}

/**
 * Run type-specific semantic validation.
 * @param {string} artifactType
 * @param {object} payload
 */
function runSemanticValidation(artifactType, payload) {
  switch (artifactType) {
    case ARTIFACT_TYPES.PROSPECT_LIST:
      return validateProspectListSemantics(payload);
    case ARTIFACT_TYPES.CAMPAIGN:
      return validateCampaignSemantics(payload);
    case ARTIFACT_TYPES.SALES_INTELLIGENCE_PROFILE:
      return validateSalesIntelligenceSemantics(payload);
    case ARTIFACT_TYPES.MAIL_PACKAGE:
      return validateMailPackageSemantics(payload);
    default:
      return { ok: true, errors: [], warnings: [] };
  }
}

/**
 * Compatibility: schema major version must match platform SCHEMA_VERSION.
 * @param {string|number|null} declared
 * @param {string} expected
 */
function checkCompatibility(declared, expected = SCHEMA_VERSION) {
  const errors = [];
  const warnings = [];
  if (declared == null || declared === '') {
    return { ok: true, errors, warnings };
  }
  const declaredMajor = String(declared).split('.')[0];
  const expectedMajor = String(expected).split('.')[0];
  if (declaredMajor !== expectedMajor) {
    errors.push(
      `Incompatible schema version: declared ${declared}, expected major ${expectedMajor} (platform ${expected})`
    );
  } else if (String(declared) !== String(expected)) {
    warnings.push(
      `Schema patch/minor differs: declared ${declared}, platform ${expected}`
    );
  }
  return { ok: errors.length === 0, errors, warnings };
}

/**
 * Validate an artifact candidate at the system boundary.
 *
 * @param {object} candidate
 * @param {string} [candidate.type]
 * @param {string} [candidate.artifactType]
 * @param {object} [candidate.payload]
 * @param {string} [candidate.schemaVersion]
 * @param {string} [candidate.version]
 * @returns {object} validation result
 */
function validateArtifactCandidate(candidate = {}) {
  const stages = {
    [VALIDATION_STAGES.IDENTIFY]: null,
    [VALIDATION_STAGES.SCHEMA]: null,
    [VALIDATION_STAGES.SEMANTIC]: null,
    [VALIDATION_STAGES.COMPATIBILITY]: null,
  };
  const errors = [];
  const warnings = [];

  const rawType = candidate.artifactType || candidate.type;
  const artifactType = resolveArtifactType(rawType);
  const def = artifactType ? lookupArtifactType(artifactType) : null;

  if (!artifactType || !def) {
    stages[VALIDATION_STAGES.IDENTIFY] = {
      ok: false,
      errors: [`Unknown artifact type: ${rawType || '(missing)'}`],
    };
    errors.push(`Unknown artifact type: ${rawType || '(missing)'}`);
    return failResult({
      artifactType: rawType || null,
      stages,
      errors,
      warnings,
      reason: 'Unknown artifact type.',
      remainsPlainText: true,
    });
  }

  stages[VALIDATION_STAGES.IDENTIFY] = { ok: true, artifactType };

  const payload = candidate.payload;
  if (payload === undefined) {
    stages[VALIDATION_STAGES.SCHEMA] = {
      ok: false,
      errors: ['Artifact payload required'],
    };
    errors.push('Artifact payload required');
    return failResult({
      artifactType,
      stages,
      errors,
      warnings,
      reason: 'Corrupt or missing payload.',
      remainsPlainText: true,
      def,
    });
  }

  // Reject raw string / non-object payloads as corrupt for structured types
  if (payload !== null && typeof payload !== 'object') {
    stages[VALIDATION_STAGES.SCHEMA] = {
      ok: false,
      errors: ['Artifact payload must be a structured object (not plain text)'],
    };
    errors.push(
      'Artifact payload must be a structured object (not plain text)'
    );
    return failResult({
      artifactType,
      stages,
      errors,
      warnings,
      reason: 'Input is natural language or untyped text.',
      remainsPlainText: true,
      def,
    });
  }

  const schemaResult = def.validate(payload);
  stages[VALIDATION_STAGES.SCHEMA] = {
    ok: schemaResult.ok,
    errors: schemaResult.errors || [],
    warnings: schemaResult.warnings || [],
  };
  if (!schemaResult.ok) {
    errors.push(...(schemaResult.errors || []));
  }
  warnings.push(...(schemaResult.warnings || []));

  const semantic = runSemanticValidation(artifactType, payload);
  stages[VALIDATION_STAGES.SEMANTIC] = {
    ok: semantic.ok,
    errors: semantic.errors || [],
    warnings: semantic.warnings || [],
  };
  if (!semantic.ok) {
    errors.push(...(semantic.errors || []));
  }
  warnings.push(...(semantic.warnings || []));

  const declaredVersion =
    candidate.schemaVersion != null
      ? candidate.schemaVersion
      : candidate.version != null
        ? candidate.version
        : null;
  const compat = checkCompatibility(
    declaredVersion,
    def.schemaVersion || SCHEMA_VERSION
  );
  stages[VALIDATION_STAGES.COMPATIBILITY] = {
    ok: compat.ok,
    errors: compat.errors || [],
    warnings: compat.warnings || [],
  };
  if (!compat.ok) {
    errors.push(...(compat.errors || []));
  }
  warnings.push(...(compat.warnings || []));

  const uniqueErrors = uniqueStrings(errors);
  const uniqueWarnings = uniqueStrings(warnings);

  if (uniqueErrors.length) {
    const remainsPlainText =
      artifactType === ARTIFACT_TYPES.PROSPECT_LIST &&
      uniqueErrors.some((e) => /natural language/i.test(e));
    return failResult({
      artifactType,
      stages,
      errors: uniqueErrors,
      warnings: uniqueWarnings,
      reason: uniqueErrors[0] || 'Validation failed.',
      remainsPlainText,
      def,
    });
  }

  const status = uniqueWarnings.length
    ? ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS
    : ARTIFACT_VALIDATION_STATUS.VALID;

  return {
    ok: true,
    status,
    artifactType,
    schemaVersion: def.schemaVersion || SCHEMA_VERSION,
    stages,
    errors: [],
    warnings: uniqueWarnings,
    reason: null,
    remainsPlainText: false,
    def,
    review: null,
  };
}

/**
 * Build a Review Workspace failure record.
 * @param {object} result - from validateArtifactCandidate
 */
function toReviewFailure(result) {
  if (!result || result.ok) return null;
  return {
    title: 'Artifact Validation',
    artifactType: result.artifactType || 'Unknown',
    status: 'FAILED',
    reasons: result.errors || [result.reason || 'Validation failed.'],
    remainsPlainText: Boolean(result.remainsPlainText),
    createdAt: new Date().toISOString(),
  };
}

function failResult({
  artifactType,
  stages,
  errors,
  warnings,
  reason,
  remainsPlainText,
  def,
}) {
  const uniqueErrors = uniqueStrings(errors);
  const uniqueWarnings = uniqueStrings(warnings);
  const result = {
    ok: false,
    status: ARTIFACT_VALIDATION_STATUS.INVALID,
    artifactType,
    schemaVersion: (def && def.schemaVersion) || SCHEMA_VERSION,
    stages,
    errors: uniqueErrors,
    warnings: uniqueWarnings,
    reason: reason || uniqueErrors[0] || 'Validation failed.',
    remainsPlainText: Boolean(remainsPlainText),
    def: def || null,
  };
  result.review = toReviewFailure(result);
  return result;
}

function uniqueStrings(list) {
  const seen = new Set();
  const out = [];
  for (const item of list || []) {
    const s = String(item || '').trim();
    if (!s || seen.has(s)) continue;
    seen.add(s);
    out.push(s);
  }
  return out;
}

function truncate(s, n) {
  const t = String(s || '');
  return t.length > n ? `${t.slice(0, n - 1)}…` : t;
}

module.exports = {
  VALIDATION_STAGES,
  SCHEMA_VERSION,
  looksLikeNaturalLanguage,
  isViableCompanyName,
  companyNameFromRow,
  validateProspectListSemantics,
  validateCampaignSemantics,
  validateSalesIntelligenceSemantics,
  validateMailPackageSemantics,
  runSemanticValidation,
  checkCompatibility,
  validateArtifactCandidate,
  toReviewFailure,
};
