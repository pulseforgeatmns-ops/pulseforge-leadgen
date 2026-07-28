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

module.exports = {
  OPERATOR_PRODUCERS,
  OPERATOR_SOURCES,
  normalizeProspectRow,
  parseDelimitedProspects,
  validateOperatorProspectRows,
  buildOperatorProspectListPayload,
  producerForSource,
  publishOperatorProspectList,
};
