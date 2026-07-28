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
 * @param {string} text
 * @returns {object[]}
 */
function parseDelimitedProspects(text) {
  const raw = String(text || '').replace(/^\uFEFF/, '').trim();
  if (!raw) return [];

  const lines = raw.split(/\r?\n/).filter((line) => String(line).trim());
  if (!lines.length) return [];

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

  return lines.map((line, i) => {
    const cells = splitDelimitedLine(line, delimiter).map((c) =>
      String(c).trim()
    );
    if (cells.length === 1) return normalizeProspectRow(cells[0], i);
    return normalizeProspectRow(
      {
        companyName: cells[0],
        website: cells[1],
        address: cells[2],
        phone: cells[3],
        contactName: cells[4],
        notes: cells[5],
      },
      i
    );
  });
}

/**
 * Field-level validation for an operator ProspectList.
 * Required: companyName. Recommended: website, address → warnings.
 * @param {object[]} prospects
 */
function validateOperatorProspectRows(prospects) {
  const errors = [];
  const warnings = [];
  const list = Array.isArray(prospects) ? prospects : [];
  const valid = [];

  list.forEach((p, i) => {
    const row = normalizeProspectRow(p, i);
    const label = `Row ${i + 1}`;
    if (!String(row.companyName || '').trim()) {
      errors.push(`${label}: Company Name is required`);
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
    errors.push('ProspectList requires at least one prospect with Company Name');
  }

  return {
    ok: errors.length === 0 && valid.length > 0,
    prospects: valid,
    errors,
    warnings,
    prospectCount: valid.length,
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
    return {
      ok: false,
      artifact: null,
      errors: built.fieldValidation.errors,
      warnings: built.fieldValidation.warnings,
      source: built.source,
      payload: built.payload,
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

/**
 * Extract a contiguous prospect-list block from a Mission chat prompt.
 * Looks for CSV/TSV headers, delimited rows, or a blank-line-separated name list.
 * @param {string} text
 * @returns {{ block: string, startLine: number, endLine: number, hasHeader: boolean }|null}
 */
function extractProspectBlock(text) {
  const raw = String(text || '').replace(/^\uFEFF/, '');
  if (!raw.trim()) return null;

  const lines = raw.split(/\r?\n/);
  let start = -1;
  let hasHeader = false;

  for (let i = 0; i < lines.length; i += 1) {
    const line = String(lines[i] || '').trim();
    if (!line) continue;
    if (OBJECTIVE_ONLY_LINE.test(line) && !looksLikeDelimitedRow(line)) {
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
    if (looksLikeDelimitedRow(line)) {
      start = i;
      break;
    }
  }

  // Fallback: after first blank line, collect 2+ non-objective name lines
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
      // allow a single blank inside the block; stop on trailing blanks
      if (i > start && !String(lines[i + 1] || '').trim()) break;
      continue;
    }
    if (i > start && OBJECTIVE_ONLY_LINE.test(line) && !looksLikeDelimitedRow(line)) {
      break;
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
    const firstCells = splitDelimitedLine(
      block.split(/\r?\n/)[0],
      detectDelimiter(block.split(/\r?\n/)[0])
    ).map((c) => String(c).trim());
    hasHeader = firstCells.some((c) =>
      FIELD_ALIASES.companyName.includes(normalizeHeaderKey(c))
    );
  }

  return { block, startLine: start, endLine: end, hasHeader };
}

function looksLikeDelimitedRow(line) {
  const text = String(line || '');
  if (!text.trim()) return false;
  const commas = (text.match(/,/g) || []).length;
  const tabs = (text.match(/\t/g) || []).length;
  return tabs >= 1 || commas >= 1;
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

  let confidence = 'none';
  let autoInject = false;
  let promptImport = false;

  if (validation.ok && prospectCount >= 2) {
    confidence = 'high';
    autoInject = true;
  } else if (
    validation.ok &&
    prospectCount >= 1 &&
    (extracted.hasHeader || explicitCue)
  ) {
    confidence = 'high';
    autoInject = true;
  } else if (rows.length >= 2 || extracted.hasHeader || explicitCue) {
    confidence = 'medium';
    promptImport = true;
  }

  if (confidence === 'none') return empty;

  const objectiveText = stripProspectBlock(raw, extracted).trim() || raw.trim();

  return {
    detected: true,
    confidence,
    autoInject,
    promptImport,
    paste: extracted.block,
    prospects: validation.prospects || [],
    validation,
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
  const lines = String(text || '').split(/\r?\n/);
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
};
