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
  /\b(still\s+do\s+not|do\s+not\s+(launch|execute|approve|mail|run|resume)|prepare\s+the\s+review|for\s+each\s+prospect|return:|readiness\s+status|missing\s+or\s+unverified|packet\s+checklist|personalized\s+letter|handwritten\s+note|scorecard|follow-?up\s+call|next\s+action|tracking\s+fields)\b/i;

const CHECKLIST_BULLET = /^\s*[-*•]\s+\S/;

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
  return /[—–]/.test(s) || /\t/.test(s) || /\s+-\s+/.test(s);
}

/**
 * True for operator instruction / checklist lines that must not be prospects.
 * Does not call numbered-row parsers (avoids recursion).
 * @param {string} line
 */
function isInstructionOrChecklistLine(line) {
  const text = String(line || '').trim();
  if (!text) return false;
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
  return parts;
}

/**
 * Parse `1. PM-001 — Company — Contact — Industry` (em dash / hyphen / tab).
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

  if (looksLikeProspectId(parts[0])) {
    id = parts[0];
    companyName = parts[1];
    contactName = parts[2];
    industry = parts[3] || null;
  } else if (isViableCompanyName(parts[0])) {
    companyName = parts[0];
    contactName = parts[1];
    industry = parts[2] || null;
  } else {
    return null;
  }

  if (!companyName || !isViableCompanyName(companyName)) return null;
  if (looksLikeNaturalLanguage(companyName)) return null;

  return normalizeProspectRow(
    {
      id: id || `op_${index + 1}`,
      companyName,
      contactName: contactName || null,
      industry: industry || null,
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
 * Extract a contiguous prospect-list block from a Mission chat prompt.
 * Looks for numbered dash rows, CSV/TSV headers, delimited rows, or name lists.
 * Stops before trailing operator instructions / checklist bullets.
 * @param {string} text
 * @returns {{ block: string, startLine: number, endLine: number, hasHeader: boolean }|null}
 */
function extractProspectBlock(text) {
  const raw = normalizeNewlines(String(text || '').replace(/^\uFEFF/, ''));
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
  const raw = String(text || '');
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
};
