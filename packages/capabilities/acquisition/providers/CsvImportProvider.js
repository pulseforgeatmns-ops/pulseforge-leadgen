'use strict';

/**
 * CSV Import acquisition provider (SPEC-060).
 * CSV → CandidateSet. Never publishes ProspectList.
 */

const {
  ACQUISITION_SOURCES,
  IMPORT_METHODS,
  buildCandidate,
  buildAcquisitionEvidence,
} = require('../types');

function loadOperatorNormalize() {
  try {
    return require('../../../mission-engine/OperatorArtifactInjection');
  } catch {
    return null;
  }
}

/**
 * Minimal CSV parser when OperatorArtifactInjection is unavailable.
 * @param {string} text
 * @returns {object[]}
 */
function parseCsvFallback(text) {
  const raw = String(text || '')
    .replace(/^\uFEFF/, '')
    .trim();
  if (!raw) return [];
  const lines = raw.split(/\r?\n/).filter((l) => l.trim());
  if (!lines.length) return [];
  const delim = lines[0].includes('\t') ? '\t' : ',';
  const headers = lines[0].split(delim).map((h) =>
    String(h)
      .trim()
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, '')
  );
  const hasCompany = headers.some((h) =>
    ['companyname', 'company', 'name', 'business', 'businessname'].includes(h)
  );
  if (!hasCompany) {
    return lines.map((line) => ({ companyName: line.split(delim)[0].trim() }));
  }
  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cells = lines[i].split(delim);
    const obj = {};
    headers.forEach((h, idx) => {
      obj[h] = cells[idx] != null ? String(cells[idx]).trim() : '';
    });
    const companyName =
      obj.companyname || obj.company || obj.name || obj.businessname || '';
    const known = {
      companyName,
      website: obj.website || obj.url || obj.domain || null,
      address: obj.address || obj.street || obj.location || null,
      phone: obj.phone || obj.telephone || null,
      email: obj.email || null,
      contactName: obj.contactname || obj.contact || null,
      notes: obj.notes || null,
      naics: obj.naics || null,
    };
    const metadata = {};
    headers.forEach((h) => {
      if (
        ![
          'companyname',
          'company',
          'name',
          'business',
          'businessname',
          'website',
          'url',
          'domain',
          'address',
          'street',
          'location',
          'phone',
          'telephone',
          'email',
          'contactname',
          'contact',
          'notes',
          'naics',
        ].includes(h) &&
        obj[h]
      ) {
        metadata[h] = obj[h];
      }
    });
    if (companyName) rows.push({ ...known, metadata });
  }
  return rows;
}

/**
 * @param {object} [deps]
 */
function createCsvImportProvider(deps = {}) {
  const id = 'csv_import';

  return {
    id,
    available() {
      return true;
    },
    async acquire(request = {}) {
      const warnings = [];
      const csv =
        (typeof request.csv === 'string' && request.csv) ||
        (typeof request.paste === 'string' && request.paste) ||
        (typeof request.text === 'string' && request.text) ||
        '';

      if (!String(csv).trim()) {
        return {
          candidates: [],
          evidence: [],
          warnings: ['CSV text is empty'],
        };
      }

      const op = deps.operatorInjection || loadOperatorNormalize();
      let rows = [];
      if (op && typeof op.parseDelimitedProspects === 'function') {
        rows = op.parseDelimitedProspects(csv);
      } else {
        rows = parseCsvFallback(csv);
      }

      const provenance = {
        acquisitionSource: ACQUISITION_SOURCES.CSV_IMPORT,
        provider: id,
        importMethod: IMPORT_METHODS.CSV,
        missionId: request.missionId || null,
        operator: request.operator || null,
      };

      const candidates = rows.map((raw, i) =>
        buildCandidate(raw, { index: i, provenance })
      );

      if (!candidates.length) {
        warnings.push('No valid CSV rows with Company Name');
      }

      const preview = candidates
        .slice(0, Number(request.previewLimit) || 10)
        .map((c) => ({
          companyName: c.companyName,
          website: c.website,
          address: c.address,
          phone: c.phone,
          email: c.email,
        }));

      return {
        candidates,
        preview,
        evidence: [
          buildAcquisitionEvidence({
            provider: id,
            acquisitionSource: ACQUISITION_SOURCES.CSV_IMPORT,
            candidateCount: candidates.length,
            importMethod: IMPORT_METHODS.CSV,
            operator: provenance.operator,
            missionId: provenance.missionId,
            summary: `CSV import acquired ${candidates.length} candidates`,
            details: { previewCount: preview.length },
          }),
        ],
        warnings,
      };
    },
    metadata() {
      return {
        id,
        label: 'CSV Import',
        category: 'operator',
        acquisitionSource: ACQUISITION_SOURCES.CSV_IMPORT,
        status: 'new',
        supports: ['csv', 'preview'],
        publishes: 'candidates',
        formats: ['csv'],
        futureFormats: ['excel'],
      };
    },
    health() {
      return {
        ok: true,
        status: 'healthy',
        provider: id,
        checkedAt: new Date().toISOString(),
        details: { alwaysAvailable: true },
      };
    },
  };
}

module.exports = {
  createCsvImportProvider,
  parseCsvFallback,
};
