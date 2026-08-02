'use strict';

/**
 * Operator Artifact Injection — normalize / validate / publish ProspectList
 * onto the Mission Artifact Bus (SPEC-043 / ADR-029).
 *
 * Consumers resolve by type + validation status + revision only.
 * Producer identity is provenance for audit / Workspace.
 */

const {
  ARTIFACT_VALIDATION_STATUS,
} = require('./PipelineGate');
const {
  ARTIFACT_TYPES,
  extractPayload,
} = require('./ArtifactRegistry');
const {
  looksLikeNaturalLanguage,
  isViableCompanyName,
  validateArtifactCandidate,
  toReviewFailure,
} = require('./ArtifactValidator');

const OPERATOR_PRODUCERS = Object.freeze({
  MANUAL: 'operator_manual',
  IMPORT: 'operator_import',
});

const OPERATOR_SOURCES = Object.freeze({
  MANUAL_ENTRY: 'manual_entry',
  CSV_IMPORT: 'csv_import',
  SPREADSHEET_PASTE: 'spreadsheet_paste',
  CRM_EXPORT: 'crm_export',
  API: 'api',
});

const FIELD_ALIASES = Object.freeze({
  companyName: [
    'companyname',
    'company',
    'company_name',
    'name',
    'business',
    'businessname',
    'business_name',
  ],
  website: ['website', 'url', 'web', 'domain', 'site'],
  address: ['address', 'street', 'location', 'fulladdress', 'full_address'],
  phone: ['phone', 'telephone', 'tel', 'phonenumber', 'phone_number'],
  contactName: [
    'contactname',
    'contact',
    'contact_name',
    'person',
    'owner',
    'decisionmaker',
  ],
  notes: ['notes', 'note', 'comments', 'comment'],
});

/**
 * Normalize a single raw row into a ProspectList row.
 * @param {object|string} raw
 * @param {number} index
 * @returns {object}
 */
function normalizeProspectRow(raw, index = 0) {
  if (raw == null) {
    return emptyProspect(index);
  }
  if (typeof raw === 'string') {
    const companyName = String(raw).trim();
    return {
      id: `op_${index + 1}`,
      companyName,
      website: null,
      address: null,
      phone: null,
      contactName: null,
      notes: null,
      source: 'operator',
      status: 'operator_supplied',
    };
  }

  const obj = typeof raw === 'object' ? raw : {};
  const map = lowerKeyMap(obj);
  const companyName = pickField(map, FIELD_ALIASES.companyName);
  return {
    id: String(obj.id || obj.prospectId || `op_${index + 1}`),
    companyName: companyName || '',
    website: emptyToNull(pickField(map, FIELD_ALIASES.website)),
    address: emptyToNull(pickField(map, FIELD_ALIASES.address)),
    phone: emptyToNull(pickField(map, FIELD_ALIASES.phone)),
    contactName: emptyToNull(pickField(map, FIELD_ALIASES.contactName)),
    notes: emptyToNull(pickField(map, FIELD_ALIASES.notes)),
    industry:
      emptyToNull(map.industry || map.vertical || map.sector) || null,
    source: 'operator',
    status: 'operator_supplied',
  };
}

/**
 * Parse CSV or TSV text into prospect rows.
 * Also accepts numbered / em-dash prospect rows (canary paste format).
 * @param {string} text
 * @returns {object[]}
 */
function parseDelimitedProspects(text) {
  const raw = normalizeNewlines(String(text || '').replace(/^\uFEFF/, '')).trim();
  if (!raw) return [];

  const lines = raw.split('\n').filter((line) => String(line).trim());
  if (!lines.length) return [];

  // Prefer numbered / dash-separated prospect rows when present
  const numbered = [];
  let numberedHits = 0;
  for (const line of lines) {
    const parsed = parseNumberedProspectRow(line, numbered.length);
    if (parsed) {
      numberedHits += 1;
      numbered.push(parsed);
    } else if (isInstructionOrChecklistLine(line)) {
      // Skip trailing instruction / bullet lines inside a mixed paste
      continue;
    } else if (numberedHits > 0) {
      // Contiguous numbered block ended
      break;
    }
  }
  if (numberedHits >= 1 && numbered.length === numberedHits) {
    // All non-instruction lines were numbered prospect rows
    const nonInstruction = lines.filter((l) => !isInstructionOrChecklistLine(l));
    if (
      numberedHits >= 2 ||
      (numberedHits === 1 && nonInstruction.length === 1)
    ) {
      if (numberedHits === nonInstruction.length) {
        return numbered;
      }
    }
  }
  if (numberedHits >= 2 && numbered.length >= 2) {
    return numbered;
  }

  const delimiter = detectDelimiter(lines[0]);
  const firstCells = splitDelimitedLine(lines[0], delimiter).map((c) =>
    String(c).trim()
  );
  const headerish = firstCells.some((c) =>
    FIELD_ALIASES.companyName.includes(normalizeHeaderKey(c))
  );

  if (headerish) {
    const headers = firstCells.map(normalizeHeaderKey);
    const rows = [];
    for (let i = 1; i < lines.length; i += 1) {
      const cells = splitDelimitedLine(lines[i], delimiter);
      if (cells.every((c) => !String(c || '').trim())) continue;
      if (isInstructionOrChecklistLine(lines[i])) continue;
      const obj = {};
      headers.forEach((h, idx) => {
        obj[h] = cells[idx] != null ? String(cells[idx]).trim() : '';
      });
      if (obj.companyname && !obj.companyName) obj.companyName = obj.companyname;
      if (obj.company_name && !obj.companyName) {
        obj.companyName = obj.company_name;
      }
      rows.push(normalizeProspectRow(obj, rows.length));
    }
    return rows;
  }

  // Headerless: one company per line, OR a single line of company names.
  const expanded = [];
  lines.forEach((line) => {
    if (isInstructionOrChecklistLine(line)) return;
    const numberedRow = parseNumberedProspectRow(line, expanded.length);
    if (numberedRow) {
      expanded.push(numberedRow);
      return;
    }
    const cells = splitDelimitedLine(line, delimiter).map((c) =>
      String(c).trim()
    );
    if (!cells.length || cells.every((c) => !c)) return;
    if (cells.length === 1) {
      expanded.push(normalizeProspectRow(cells[0], expanded.length));
      return;
    }
    if (looksLikeCompanyNameList(cells)) {
      cells.filter(Boolean).forEach((name) => {
        expanded.push(normalizeProspectRow(name, expanded.length));
      });
      return;
    }
    // id + company + contact + industry (tab / comma) without header
    if (
      cells.length >= 3 &&
      looksLikeProspectId(cells[0]) &&
      isViableCompanyName(cells[1])
    ) {
      expanded.push(
        normalizeProspectRow(
          {
            id: cells[0],
            companyName: cells[1],
            contactName: cells[2],
            industry: cells[3] || null,
          },
          expanded.length
        )
      );
      return;
    }
    expanded.push(
      normalizeProspectRow(
        {
          companyName: cells[0],
          website: cells[1],
          address: cells[2],
          phone: cells[3],
          contactName: cells[4],
          notes: cells[5],
        },
        expanded.length
      )
    );
  });
  return expanded;
}

/**
 * True when every cell looks like a company name (not URL / phone / address).
 * Handles "Acme Law, Beta CPA, Gamma LLC" pastes without a header row.
 * @param {string[]} cells
 */
function looksLikeCompanyNameList(cells) {
  const list = (Array.isArray(cells) ? cells : []).filter((c) =>
    String(c || '').trim()
  );
  if (list.length < 2) return false;
  return list.every((c) => looksLikeCompanyNameOnly(c));
}

function looksLikeCompanyNameOnly(value) {
  const s = String(value || '').trim();
  if (!s) return false;
  if (looksLikeWebsite(s) || looksLikePhone(s)) return false;
  // Street-like addresses usually start with a digit.
  if (/^\d+\s+\S/.test(s)) return false;
  // Fillable-table mutation fragments ("For PM-001 only", "set:") are not companies.
  if (/^for\s+\S+\s+only\b/i.test(s)) return false;
  if (/^set\s*:?\s*$/i.test(s)) return false;
  if (FILLABLE_TABLE_FIELD_ASSIGNMENT.test(s)) return false;
  if (/_(?:status|value|readiness)$/i.test(s)) return false;
  // SPEC-052: mission prose / instructions are not company names
  if (looksLikeNaturalLanguage(s)) return false;
  return true;
}

function looksLikeWebsite(value) {
  const s = String(value || '').trim();
  if (!s) return false;
  return /^(https?:\/\/|www\.)/i.test(s) || /\.[a-z]{2,}(\/|$)/i.test(s);
}

function looksLikePhone(value) {
  const s = String(value || '').trim();
  if (!s) return false;
  const digits = s.replace(/\D/g, '');
  return digits.length >= 7 && /^[\d\s()+./-]+$/.test(s);
}

/**
 * Normalize CR/CRLF/Unicode line separators so Excel / Mac pastes keep one row
 * per prospect instead of collapsing into a single line.
 * @param {string} text
 */
function normalizeNewlines(text) {
  return String(text || '')
    .replace(/\r\n/g, '\n')
    .replace(/\r/g, '\n')
    .replace(/\u2028|\u2029/g, '\n');
}

/**
 * Field-level validation for an operator ProspectList.
 * Required: companyName. Recommended: website, address → warnings.
 * SPEC-052: natural language / mission objectives never become companies.
 * @param {object[]} prospects
 */
function validateOperatorProspectRows(prospects) {
  const errors = [];
  const warnings = [];
  const list = Array.isArray(prospects) ? prospects : [];
  const valid = [];
  let naturalLanguageRows = 0;

  list.forEach((p, i) => {
    const row = normalizeProspectRow(p, i);
    const label = `Row ${i + 1}`;
    const name = String(row.companyName || '').trim();
    if (!name) {
      errors.push(`${label}: Company Name is required`);
      return;
    }
    if (!isViableCompanyName(name)) {
      naturalLanguageRows += 1;
      errors.push(
        `${label}: Input is natural language ("${name.slice(0, 60)}").`
      );
      return;
    }
    if (!row.website) {
      warnings.push(`${label} (${row.companyName}): Website recommended`);
    }
    if (!row.address) {
      warnings.push(`${label} (${row.companyName}): Address recommended`);
    }
    valid.push(row);
  });

  if (!valid.length) {
    if (naturalLanguageRows > 0) {
      errors.unshift('Input is natural language.');
      errors.push('No valid prospect rows detected.');
    } else {
      errors.push('ProspectList requires at least one prospect with Company Name');
    }
  }

  return {
    ok: errors.length === 0 && valid.length > 0,
    prospects: valid,
    errors,
    warnings,
    prospectCount: valid.length,
    naturalLanguageRows,
  };
}

/**
 * Build a ProspectList payload suitable for ArtifactBus.publishArtifact.
 * @param {object} input
 */
function buildOperatorProspectListPayload(input = {}) {
  let rows = [];
  let source = OPERATOR_SOURCES.MANUAL_ENTRY;

  if (typeof input.csv === 'string' && input.csv.trim()) {
    rows = parseDelimitedProspects(input.csv);
    source = OPERATOR_SOURCES.CSV_IMPORT;
  } else if (typeof input.paste === 'string' && input.paste.trim()) {
    rows = parseDelimitedProspects(input.paste);
    source = OPERATOR_SOURCES.SPREADSHEET_PASTE;
  } else if (Array.isArray(input.prospects)) {
    rows = input.prospects.map((p, i) => normalizeProspectRow(p, i));
    source = input.source || OPERATOR_SOURCES.MANUAL_ENTRY;
  }

  if (typeof input.source === 'string' && input.source.trim()) {
    const key = String(input.source).trim();
    const upper = key.toUpperCase();
    if (OPERATOR_SOURCES[upper]) source = OPERATOR_SOURCES[upper];
    else source = key;
  }

  const fieldValidation = validateOperatorProspectRows(rows);
  const payload = extractPayload(ARTIFACT_TYPES.PROSPECT_LIST, {
    prospects: fieldValidation.prospects,
    prospectCount: fieldValidation.prospectCount,
    targetCount:
      input.targetCount != null
        ? Number(input.targetCount)
        : fieldValidation.prospectCount,
    summary: {
      discovered: fieldValidation.prospectCount,
      verified: fieldValidation.prospectCount,
      rejected: Math.max(0, rows.length - fieldValidation.prospectCount),
      targetCount:
        input.targetCount != null
          ? Number(input.targetCount)
          : fieldValidation.prospectCount,
      operatorSupplied: true,
    },
  });

  return {
    source,
    payload,
    fieldValidation,
  };
}

/**
 * Resolve producer id from source (provenance only — ADR-029).
 * @param {string} source
 */
function producerForSource(source) {
  if (
    source === OPERATOR_SOURCES.MANUAL_ENTRY ||
    source === 'manual' ||
    source === 'operator_manual'
  ) {
    return OPERATOR_PRODUCERS.MANUAL;
  }
  return OPERATOR_PRODUCERS.IMPORT;
}

/**
 * Publish a validated operator ProspectList onto an ArtifactBus instance.
 * @param {object} input
 */
function publishOperatorProspectList(input = {}) {
  const bus = input.bus;
  if (!bus) throw new Error('publishOperatorProspectList requires bus');
  if (!input.missionId) {
    throw new Error('publishOperatorProspectList requires missionId');
  }

  const built = buildOperatorProspectListPayload(input);
  if (!built.fieldValidation.ok) {
    const typed = validateArtifactCandidate({
      artifactType: ARTIFACT_TYPES.PROSPECT_LIST,
      payload: built.payload,
    });
    return {
      ok: false,
      artifact: null,
      errors: built.fieldValidation.errors,
      warnings: built.fieldValidation.warnings,
      source: built.source,
      payload: built.payload,
      validationFailure:
        typed.review ||
        toReviewFailure({
          ok: false,
          artifactType: ARTIFACT_TYPES.PROSPECT_LIST,
          errors: built.fieldValidation.errors,
          remainsPlainText: Boolean(
            built.fieldValidation.naturalLanguageRows
          ),
        }),
    };
  }

  const registry = bus.validateArtifact({
    artifactType: ARTIFACT_TYPES.PROSPECT_LIST,
    payload: built.payload,
  });
  if (!registry.ok) {
    return {
      ok: false,
      artifact: null,
      errors: [
        ...built.fieldValidation.errors,
        ...(registry.errors || []),
      ],
      warnings: [
        ...built.fieldValidation.warnings,
        ...(registry.warnings || []),
      ],
      source: built.source,
      payload: built.payload,
      validationFailure:
        registry.review ||
        toReviewFailure({
          ok: false,
          artifactType: ARTIFACT_TYPES.PROSPECT_LIST,
          errors: registry.errors || [],
          remainsPlainText: false,
        }),
    };
  }

  const producer = producerForSource(built.source);
  const warnings = [
    ...built.fieldValidation.warnings,
    ...(registry.warnings || []),
  ];
  const validationStatus = warnings.length
    ? ARTIFACT_VALIDATION_STATUS.VALID_WITH_WARNINGS
    : ARTIFACT_VALIDATION_STATUS.VALID;

  const artifact = bus.publishArtifact({
    missionId: input.missionId,
    stageId: input.stageId || 'prospect_discovery',
    artifactType: ARTIFACT_TYPES.PROSPECT_LIST,
    producer,
    payload: built.payload,
    dependencies: [],
    metadata: {
      provenance: {
        producer,
        source: built.source,
        validated: true,
        createdBy: input.createdBy || 'operator',
      },
      operatorSupplied: true,
      warnings,
    },
    validationStatus,
    skipRegistryValidation: true,
    warnings,
  });

  return {
    ok: true,
    artifact,
    errors: [],
    warnings,
    source: built.source,
    payload: built.payload,
    producer,
  };
}

function emptyProspect(index) {
  return {
    id: `op_${index + 1}`,
    companyName: '',
    website: null,
    address: null,
    phone: null,
    contactName: null,
    notes: null,
    source: 'operator',
    status: 'operator_supplied',
  };
}

function lowerKeyMap(obj) {
  const map = Object.create(null);
  for (const [k, v] of Object.entries(obj || {})) {
    map[normalizeHeaderKey(k)] = v;
  }
  return map;
}

function normalizeHeaderKey(key) {
  return String(key || '')
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
}

function pickField(map, aliases) {
  for (const alias of aliases) {
    if (map[alias] != null && String(map[alias]).trim()) {
      return String(map[alias]).trim();
    }
  }
  return '';
}

function emptyToNull(value) {
  if (value == null) return null;
  const s = String(value).trim();
  return s ? s : null;
}

function detectDelimiter(line) {
  const commas = (String(line).match(/,/g) || []).length;
  const tabs = (String(line).match(/\t/g) || []).length;
  const semis = (String(line).match(/;/g) || []).length;
  if (tabs > commas && tabs >= semis) return '\t';
  if (semis > commas) return ';';
  return ',';
}

function splitDelimitedLine(line, delimiter) {
  const out = [];
  let cur = '';
  let inQuotes = false;
  const text = String(line || '');
  for (let i = 0; i < text.length; i += 1) {
    const ch = text[i];
    if (ch === '"') {
      if (inQuotes && text[i + 1] === '"') {
        cur += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }
    if (ch === delimiter && !inQuotes) {
      out.push(cur);
      cur = '';
      continue;
    }
    cur += ch;
  }
  out.push(cur);
  return out;
}

const EXPLICIT_IMPORT_CUE =
  /\b(import|use these|here(?:'s| are)|prospect\s+list|company\s+list|from\s+(?:this\s+|my\s+)?list|paste[sd]?|attached\s+list|operator[- ]supplied)\b/i;

const OBJECTIVE_ONLY_LINE =
  /\b(build|create|launch|prepare|new)\s+(a\s+)?(campaign|mission)\b|\bmonitor\b|\bsummarize\b|\breview\s+campaign\b/i;

/** Operator constraint / request prose that must not become ProspectList rows. */
const INSTRUCTION_LINE =
  /\b(still\s+do\s+not|do\s+not\s+(launch|execute|approve|mail|run|resume|print|create)|prepare\s+the\s+review|for\s+each\s+prospect|return:|readiness\s+status|missing\s+or\s+unverified|packet\s+checklist|personalized\s+letter|handwritten\s+note|scorecard|follow-?up\s+call|next\s+action|tracking\s+fields)\b/i;

/** Mid-string instruction prose that ends a flattened prospect chunk. */
const INLINE_INSTRUCTION_START =
  /\s+(?=(?:still\s+)?do\s+not\b|\bprepare\s+the\s+review\b|\bfor\s+each\s+prospect\b|\bcreate\s+provisional\b)/i;

const CHECKLIST_BULLET = /^\s*[-*•]\s+\S/;

/** Fillable verification table columns — field assignment lines are not companies. */
const FILLABLE_TABLE_FIELD_NAMES = Object.freeze([
  'prospect_id',
  'company_name',
  'contact_name',
  'contact_role_status',
  'website_status',
  'website_value',
  'mailing_address_status',
  'mailing_address_value',
  'phone_status',
  'phone_value',
  'source_to_check_first',
  'verification_status',
  'mail_readiness',
  'draft_readiness',
  'execution_readiness',
  'operator_next_action',
  'notes',
]);

const FILLABLE_TABLE_FIELD_ASSIGNMENT = new RegExp(
  `^(?:[-*•]\\s*)?(?:${FILLABLE_TABLE_FIELD_NAMES.join('|')})\\s*[=:]\\s*\\S`,
  'i'
);

/**
 * True for fillable-table mutation / readiness reassessment messages that must
 * never be sniffed as ProspectList pastes.
 * @param {string} text
 */
function looksLikeFillableTableMutationMessage(text) {
  const raw = String(text || '');
  if (!raw.trim()) return false;
  const lower = raw.toLowerCase();
  const updateCue =
    /\bupdate\s+(?:the\s+)?(?:fillable\s+)?(?:verification\s+)?table\b/.test(
      lower
    ) ||
    /\bedit\s+(?:the\s+)?(?:fillable\s+)?(?:verification\s+)?table\b/.test(
      lower
    );
  const fieldAssignment = FILLABLE_TABLE_FIELD_NAMES.some((field) =>
    new RegExp(`\\b${field}\\s*[=:]\\s*\\S`, 'i').test(raw)
  );
  const forOnlySet = /\bfor\s+[A-Za-z0-9_-]+\s+only\b/i.test(raw);
  const reassess =
    /\breassess\b[\s\S]{0,120}\breadiness\b/i.test(raw) ||
    /\busing\s+(?:the\s+)?table\s+gates\b/i.test(raw);
  if (updateCue && (fieldAssignment || forOnlySet || reassess)) return true;
  if (fieldAssignment && forOnlySet) return true;
  if (reassess && (forOnlySet || fieldAssignment || updateCue)) return true;
  return false;
}

/**
 * True when a line is a fillable-table field assignment / mutation header.
 * @param {string} line
 */
function isFillableTableFieldAssignmentLine(line) {
  const text = String(line || '').trim();
  if (!text) return false;
  if (FILLABLE_TABLE_FIELD_ASSIGNMENT.test(text)) return true;
  if (/^for\s+[A-Za-z0-9_-]+\s+only\b/i.test(text)) return true;
  if (/^set\s*:?\s*$/i.test(text)) return true;
  if (/^leave\b[\s\S]{0,80}\bunchanged\b/i.test(text)) return true;
  if (/^update\s+(?:the\s+)?(?:fillable\s+)?(?:verification\s+)?table\b/i.test(text)) {
    return true;
  }
  if (/^reassess\b/i.test(text) && /\breadiness\b/i.test(text)) return true;
  if (/^return\s+only\b/i.test(text)) return true;
  if (/^keep\s+this\s+preparation/i.test(text)) return true;
  return false;
}

/**
 * Strip leading list markers: `1.`, `1)`, `- `, `* `
 * @param {string} line
 */
function stripListPrefix(line) {
  return String(line || '')
    .trim()
    .replace(/^\d+[\.)]\s+/, '')
    .replace(/^[-*•]\s+/, '')
    .trim();
}

function looksLikeProspectId(value) {
  const s = String(value || '').trim();
  if (!s) return false;
  // PM-001, LEAD_12, A1, etc.
  return /^[A-Za-z]{1,12}[-_]?\d{1,6}$/.test(s) || /^[A-Za-z]+\d+$/.test(s);
}

function hasProspectRowSeparators(text) {
  const s = String(text || '');
  return (
    /[—–]/.test(s) ||
    /\t/.test(s) ||
    /\s+-\s+/.test(s) ||
    /\s*\|\s*/.test(s)
  );
}

/**
 * True for operator instruction / checklist lines that must not be prospects.
 * Does not call numbered-row parsers (avoids recursion).
 * @param {string} line
 */
function isInstructionOrChecklistLine(line) {
  const text = String(line || '').trim();
  if (!text) return false;
  if (isFillableTableFieldAssignmentLine(text)) return true;
  if (INSTRUCTION_LINE.test(text)) return true;
  if (OBJECTIVE_ONLY_LINE.test(text) && !hasProspectRowSeparators(text)) {
    return true;
  }
  // Bullets that are field requests ("- readiness status"), not company rows
  if (CHECKLIST_BULLET.test(text) && !hasProspectRowSeparators(text)) {
    const body = stripListPrefix(text);
    if (!body) return true;
    if (looksLikeNaturalLanguage(body)) return true;
    if (!isViableCompanyName(body)) return true;
  }
  return false;
}

/**
 * Split a stripped prospect line into id/company/contact/industry parts.
 * @param {string} stripped
 * @returns {string[]}
 */
function splitProspectParts(stripped) {
  let parts = String(stripped || '')
    .split(/\s*[—–]\s*/)
    .map((p) => p.trim())
    .filter(Boolean);
  if (parts.length < 3) {
    const tabParts = String(stripped || '')
      .split(/\t+/)
      .map((p) => p.trim())
      .filter(Boolean);
    if (tabParts.length >= 3) parts = tabParts;
  }
  if (parts.length < 3) {
    parts = String(stripped || '')
      .split(/\s+-\s+/)
      .map((p) => p.trim())
      .filter(Boolean);
  }
  if (parts.length < 3) {
    const pipeParts = String(stripped || '')
      .split(/\s*\|\s*/)
      .map((p) => p.trim())
      .filter(Boolean);
    if (pipeParts.length >= 3) parts = pipeParts;
  }
  return parts;
}

/**
 * Map optional trailing field parts (website / address / phone), treating
 * "unknown" placeholders as null.
 * @param {string[]} parts
 * @param {number} startIdx
 */
function mapOptionalProspectFields(parts, startIdx) {
  let website = null;
  let address = null;
  let phone = null;
  const list = Array.isArray(parts) ? parts : [];
  for (let i = startIdx; i < list.length; i += 1) {
    const raw = String(list[i] || '').trim();
    if (!raw) continue;
    const lower = raw.toLowerCase();
    const value = fieldValueOrNull(raw);
    if (/^website\b/i.test(lower) || looksLikeWebsite(raw)) {
      website = value;
    } else if (/^(mailing\s+)?address\b/i.test(lower)) {
      address = value;
    } else if (/^phone\b/i.test(lower) || looksLikePhone(raw)) {
      phone = value;
    }
  }
  return { website, address, phone };
}

function fieldValueOrNull(raw) {
  const s = String(raw || '').trim();
  if (!s) return null;
  const lower = s.toLowerCase();
  if (
    /^(website|mailing\s+address|address|phone)\s+unknown$/i.test(lower) ||
    lower === 'unknown' ||
    lower === 'n/a' ||
    lower === 'none'
  ) {
    return null;
  }
  // "website https://..." / "phone 555-..." → strip label
  const labeled = /^(website|mailing\s+address|address|phone)\s*[:\-]?\s*(.+)$/i.exec(
    s
  );
  if (labeled) {
    const rest = String(labeled[2] || '').trim();
    if (!rest || /^unknown$/i.test(rest)) return null;
    return rest;
  }
  return s;
}

/**
 * Parse `1. PM-001 — Company — Contact — Industry` (em dash / hyphen / tab / pipe).
 * @param {string} line
 * @param {number} index
 * @returns {object|null}
 */
function parseNumberedProspectRow(line, index = 0) {
  const text = String(line || '').trim();
  if (!text) return null;
  if (INSTRUCTION_LINE.test(text)) return null;
  if (!hasProspectRowSeparators(text) && !/^\d+[\.)]\s+/.test(text)) {
    // Require separators or a numeric list prefix
    if (!/\t/.test(text)) return null;
  }

  const stripped = stripListPrefix(text);
  if (!stripped || INSTRUCTION_LINE.test(stripped)) return null;

  const parts = splitProspectParts(stripped);
  if (parts.length < 3) return null;

  let id = null;
  let companyName;
  let contactName;
  let industry = null;
  let fieldStart = 3;

  if (looksLikeProspectId(parts[0])) {
    id = parts[0];
    companyName = parts[1];
    contactName = parts[2];
    industry = parts[3] || null;
    fieldStart = industry ? 4 : 3;
  } else if (isViableCompanyName(parts[0])) {
    companyName = parts[0];
    contactName = parts[1];
    industry = parts[2] || null;
    fieldStart = industry ? 3 : 2;
  } else {
    return null;
  }

  if (!companyName || !isViableCompanyName(companyName)) return null;
  if (looksLikeNaturalLanguage(companyName)) return null;

  // Industry slot may itself be a labeled optional field
  if (industry && /^(website|mailing\s+address|address|phone)\b/i.test(industry)) {
    fieldStart = looksLikeProspectId(parts[0]) ? 3 : 2;
    industry = null;
  }

  const optional = mapOptionalProspectFields(parts, fieldStart);

  return normalizeProspectRow(
    {
      id: id || `op_${index + 1}`,
      companyName,
      contactName: contactName || null,
      industry: industry || null,
      website: optional.website,
      address: optional.address,
      phone: optional.phone,
    },
    index
  );
}

function looksLikeNumberedProspectRow(line) {
  const text = String(line || '').trim();
  if (!text) return false;
  if (INSTRUCTION_LINE.test(text)) return false;
  const hasListPrefix = /^\d+[\.)]\s+/.test(text) || /^[-*•]\s+/.test(text);
  if (!hasProspectRowSeparators(text) && !hasListPrefix) return false;
  return parseNumberedProspectRow(text, 0) != null;
}

/**
 * CSV/TSV data row (not instruction prose that happens to contain commas).
 * @param {string} line
 */
function looksLikeDelimitedDataRow(line) {
  const text = String(line || '').trim();
  if (!text) return false;
  if (INSTRUCTION_LINE.test(text)) return false;
  if (isInstructionOrChecklistLine(text)) return false;
  if (looksLikeNumberedProspectRow(text)) return true;

  const tabs = (text.match(/\t/g) || []).length;
  if (tabs >= 1) {
    const cells = splitDelimitedLine(text, '\t').map((c) => String(c).trim());
    if (cells.length >= 2 && cells.some((c) => isViableCompanyName(c))) {
      return true;
    }
  }

  const commas = (text.match(/,/g) || []).length;
  if (commas >= 1) {
    // Reject NL sentences that use commas as separators ("Still do not launch, execute…")
    if (looksLikeNaturalLanguage(stripListPrefix(text))) return false;
    const cells = splitDelimitedLine(text, ',').map((c) => String(c).trim());
    if (cells.length >= 2 && cells.some((c) => isViableCompanyName(c))) {
      return true;
    }
  }
  return false;
}

/**
 * Expand single-paragraph numbered prospect rows into one line per prospect.
 * Stops before trailing instruction prose on the final chunk.
 *
 * Example:
 *   "Use these 3 prospects: 1. PM-001 — A — B — C 2. PM-002 — D — E — F Do not launch..."
 * → preamble + two prospect lines + trailing instructions
 *
 * @param {string} text
 * @returns {string}
 */
function expandFlattenedNumberedProspectText(text) {
  const raw = normalizeNewlines(String(text || ''));
  if (!raw.trim()) return raw;

  const lines = raw.split('\n');
  const out = [];

  for (const line of lines) {
    const expanded = splitFlattenedNumberedProspectLine(line);
    if (!expanded) {
      out.push(line);
      continue;
    }
    if (expanded.preamble) out.push(expanded.preamble);
    expanded.rows.forEach((row) => out.push(row));
    if (expanded.trailing) out.push(expanded.trailing);
  }

  return out.join('\n');
}

/**
 * @param {string} line
 * @returns {{ preamble: string|null, rows: string[], trailing: string|null }|null}
 */
function splitFlattenedNumberedProspectLine(line) {
  const text = String(line || '');
  if (!text.trim()) return null;
  if (!hasProspectRowSeparators(text)) return null;

  // Numbered markers: "1. " / "2) " ahead of an id or company-like token
  const markerRe = /\d+[\.)]\s+(?=[A-Za-z0-9])/g;
  const markers = [...text.matchAll(markerRe)];
  if (markers.length < 2) return null;

  const preamble = text.slice(0, markers[0].index).trim() || null;
  const rows = [];
  let trailing = null;

  for (let i = 0; i < markers.length; i += 1) {
    const start = markers[i].index;
    const end = i + 1 < markers.length ? markers[i + 1].index : text.length;
    let chunk = text.slice(start, end).trim();
    if (!chunk) continue;

    if (i === markers.length - 1) {
      const trimmed = trimInstructionProseFromProspectChunk(chunk);
      chunk = trimmed.row;
      trailing = trimmed.trailing;
    }

    if (!chunk) continue;
    if (parseNumberedProspectRow(chunk, rows.length)) {
      rows.push(chunk);
    } else if (i === markers.length - 1) {
      trailing = [chunk, trailing].filter(Boolean).join(' ').trim() || trailing;
    }
  }

  if (rows.length < 2) return null;
  return { preamble, rows, trailing };
}

/**
 * Peel trailing instruction prose off the last flattened prospect chunk.
 * @param {string} chunk
 * @returns {{ row: string, trailing: string|null }}
 */
function trimInstructionProseFromProspectChunk(chunk) {
  const text = String(chunk || '').trim();
  if (!text) return { row: '', trailing: null };

  const instr = INLINE_INSTRUCTION_START.exec(text);
  if (instr && instr.index > 0) {
    const row = text.slice(0, instr.index).trim();
    const trailing = text.slice(instr.index).trim();
    if (parseNumberedProspectRow(row, 0)) {
      return { row, trailing: trailing || null };
    }
  }

  // Fallback: keep only id/company/contact/industry + optional labeled fields
  const stripped = stripListPrefix(text);
  const prefix = text.slice(0, text.length - stripped.length);
  const parts = splitProspectParts(stripped);
  if (parts.length >= 3 && looksLikeProspectId(parts[0])) {
    let keep = 4; // id, company, contact, industry
    for (let i = 4; i < parts.length; i += 1) {
      if (/^(website|mailing\s+address|address|phone)\b/i.test(parts[i])) {
        keep = i + 1;
      } else {
        break;
      }
    }
    if (keep < parts.length) {
      const rowParts = parts.slice(0, keep);
      const sep = /[—–]/.test(text) ? ' — ' : /\s\|\s/.test(text) ? ' | ' : ' - ';
      const row = `${prefix}${rowParts.join(sep)}`.trim();
      const trailing = parts.slice(keep).join(' ').trim();
      if (parseNumberedProspectRow(row, 0)) {
        return { row, trailing: trailing || null };
      }
    }
  }

  return { row: text, trailing: null };
}

/**
 * Extract a contiguous prospect-list block from a Mission chat prompt.
 * Looks for numbered dash rows, CSV/TSV headers, delimited rows, or name lists.
 * Stops before trailing operator instructions / checklist bullets.
 * @param {string} text
 * @returns {{ block: string, startLine: number, endLine: number, hasHeader: boolean }|null}
 */
function extractProspectBlock(text) {
  const raw = expandFlattenedNumberedProspectText(
    normalizeNewlines(String(text || '').replace(/^\uFEFF/, ''))
  );
  if (!raw.trim()) return null;

  const lines = raw.split('\n');
  let start = -1;
  let hasHeader = false;

  for (let i = 0; i < lines.length; i += 1) {
    const line = String(lines[i] || '').trim();
    if (!line) continue;
    if (isInstructionOrChecklistLine(line) && !looksLikeNumberedProspectRow(line)) {
      continue;
    }
    if (OBJECTIVE_ONLY_LINE.test(line) && !looksLikeDelimitedDataRow(line)) {
      continue;
    }
    const delimiter = detectDelimiter(line);
    const cells = splitDelimitedLine(line, delimiter).map((c) =>
      String(c).trim()
    );
    const headerish = cells.some((c) =>
      FIELD_ALIASES.companyName.includes(normalizeHeaderKey(c))
    );
    if (headerish) {
      start = i;
      hasHeader = true;
      break;
    }
    if (looksLikeNumberedProspectRow(line) || looksLikeDelimitedDataRow(line)) {
      start = i;
      break;
    }
  }

  // Fallback: after first blank line, collect 2+ non-instruction name lines
  if (start < 0) {
    const blankIdx = lines.findIndex((l, idx) => idx > 0 && !String(l).trim());
    if (blankIdx >= 0) {
      const candidates = [];
      for (let i = blankIdx + 1; i < lines.length; i += 1) {
        const line = String(lines[i] || '').trim();
        if (!line) {
          if (candidates.length) break;
          continue;
        }
        if (isInstructionOrChecklistLine(line)) {
          if (candidates.length) break;
          continue;
        }
        if (OBJECTIVE_ONLY_LINE.test(line)) continue;
        candidates.push(i);
      }
      if (candidates.length >= 2) {
        start = candidates[0];
      }
    }
  }

  if (start < 0) return null;

  let end = start;
  for (let i = start; i < lines.length; i += 1) {
    const line = String(lines[i] || '').trim();
    if (!line) {
      // Stop at blank that precedes instructions / ends the contiguous block
      const next = String(lines[i + 1] || '').trim();
      if (
        i > start &&
        (!next ||
          isInstructionOrChecklistLine(next) ||
          (OBJECTIVE_ONLY_LINE.test(next) && !looksLikeDelimitedDataRow(next)))
      ) {
        break;
      }
      continue;
    }
    if (
      i > start &&
      (isInstructionOrChecklistLine(line) ||
        (OBJECTIVE_ONLY_LINE.test(line) && !looksLikeDelimitedDataRow(line)))
    ) {
      break;
    }
    // Contiguous numbered/data block only — stop when row shape breaks
    if (
      i > start &&
      !looksLikeNumberedProspectRow(line) &&
      !looksLikeDelimitedDataRow(line) &&
      !hasHeader
    ) {
      // Allow headerless name-only continuations if first row was names
      const first = String(lines[start] || '').trim();
      if (looksLikeNumberedProspectRow(first) || looksLikeDelimitedDataRow(first)) {
        break;
      }
    }
    end = i;
  }

  const blockLines = lines.slice(start, end + 1).filter((l, idx, arr) => {
    const t = String(l || '').trim();
    if (!t) return idx > 0 && idx < arr.length - 1;
    return true;
  });
  const block = blockLines.join('\n').trim();
  if (!block) return null;

  if (!hasHeader) {
    const firstLine = block.split('\n')[0];
    const firstCells = splitDelimitedLine(
      firstLine,
      detectDelimiter(firstLine)
    ).map((c) => String(c).trim());
    hasHeader = firstCells.some((c) =>
      FIELD_ALIASES.companyName.includes(normalizeHeaderKey(c))
    );
  }

  return { block, startLine: start, endLine: end, hasHeader };
}

function looksLikeDelimitedRow(line) {
  return looksLikeDelimitedDataRow(line) || looksLikeNumberedProspectRow(line);
}

/**
 * Detect operator-supplied ProspectList data embedded in a Mission prompt.
 * High confidence → autoInject; medium → prompt operator to import.
 *
 * @param {string} text
 * @returns {object}
 */
function detectOperatorProspectListInMessage(text) {
  const raw = expandFlattenedNumberedProspectText(
    normalizeNewlines(String(text || ''))
  );
  const empty = {
    detected: false,
    confidence: 'none',
    autoInject: false,
    promptImport: false,
    paste: null,
    prospects: [],
    validation: null,
    source: OPERATOR_SOURCES.SPREADSHEET_PASTE,
    hasHeader: false,
    explicitCue: false,
    objectiveText: raw.trim(),
    prospectCount: 0,
  };

  if (!raw.trim()) return empty;

  // Fillable verification table mutations must never become ProspectList pastes.
  if (looksLikeFillableTableMutationMessage(raw)) {
    return {
      ...empty,
      rejectedAsNaturalLanguage: true,
      remainsPlainText: true,
      suppressedFillableTableUpdate: true,
      objectiveText: raw.trim(),
    };
  }

  const extracted = extractProspectBlock(raw);
  if (!extracted) return empty;

  const rows = parseDelimitedProspects(extracted.block);
  const validation = validateOperatorProspectRows(rows);
  const explicitCue = EXPLICIT_IMPORT_CUE.test(raw);
  const prospectCount = validation.prospectCount || 0;

  // SPEC-052: typed validation against raw candidate rows (not filtered empties)
  const typed = validateArtifactCandidate({
    artifactType: ARTIFACT_TYPES.PROSPECT_LIST,
    payload: {
      prospects: rows,
      prospectCount: rows.length,
    },
  });

  const allNaturalLanguage =
    rows.length > 0 &&
    (validation.naturalLanguageRows || 0) >= rows.length &&
    prospectCount === 0;

  if (allNaturalLanguage || (typed.remainsPlainText && prospectCount === 0)) {
    const failure =
      typed.remainsPlainText && typed.review
        ? typed.review
        : toReviewFailure({
            ok: false,
            artifactType: ARTIFACT_TYPES.PROSPECT_LIST,
            errors: [
              'Input is natural language.',
              'No valid prospect rows detected.',
            ],
            remainsPlainText: true,
          });
    return {
      ...empty,
      detected: false,
      rejectedAsNaturalLanguage: true,
      remainsPlainText: true,
      validationFailure: failure,
      validation,
      objectiveText: raw.trim(),
    };
  }

  let confidence = 'none';
  let autoInject = false;
  let promptImport = false;

  if (validation.ok && typed.ok && prospectCount >= 2) {
    confidence = 'high';
    autoInject = true;
  } else if (
    validation.ok &&
    typed.ok &&
    prospectCount >= 1 &&
    (extracted.hasHeader || explicitCue)
  ) {
    confidence = 'high';
    autoInject = true;
  } else if (
    (rows.length >= 2 || extracted.hasHeader || explicitCue) &&
    !allNaturalLanguage
  ) {
    confidence = 'medium';
    promptImport = true;
  }

  if (confidence === 'none') {
    if (!typed.ok && rows.length > 0) {
      return {
        ...empty,
        rejectedAsNaturalLanguage: Boolean(typed.remainsPlainText),
        remainsPlainText: Boolean(typed.remainsPlainText),
        validationFailure: typed.review,
        validation,
        objectiveText: raw.trim(),
      };
    }
    return empty;
  }

  const objectiveText = stripProspectBlock(raw, extracted).trim() || raw.trim();

  return {
    detected: true,
    confidence,
    autoInject,
    promptImport,
    paste: extracted.block,
    prospects: validation.prospects || [],
    validation,
    typedValidation: typed,
    validationFailure: typed.ok ? null : typed.review,
    source: OPERATOR_SOURCES.SPREADSHEET_PASTE,
    hasHeader: extracted.hasHeader,
    explicitCue,
    objectiveText,
    prospectCount,
  };
}

/**
 * Remove the prospect block from the prompt, leaving objective prose for the planner.
 * @param {string} text
 * @param {{ startLine: number, endLine: number }} extracted
 */
function stripProspectBlock(text, extracted) {
  const lines = normalizeNewlines(String(text || '')).split('\n');
  if (!extracted || extracted.startLine == null) return String(text || '');
  const kept = lines.filter(
    (_line, i) => i < extracted.startLine || i > extracted.endLine
  );
  return kept.join('\n').replace(/\n{3,}/g, '\n\n').trim();
}

module.exports = {
  OPERATOR_PRODUCERS,
  OPERATOR_SOURCES,
  normalizeProspectRow,
  parseDelimitedProspects,
  validateOperatorProspectRows,
  buildOperatorProspectListPayload,
  producerForSource,
  publishOperatorProspectList,
  extractProspectBlock,
  detectOperatorProspectListInMessage,
  stripProspectBlock,
  parseNumberedProspectRow,
  looksLikeNumberedProspectRow,
  isInstructionOrChecklistLine,
  looksLikeNaturalLanguage,
  isViableCompanyName,
  expandFlattenedNumberedProspectText,
  looksLikeFillableTableMutationMessage,
  isFillableTableFieldAssignmentLine,
};
