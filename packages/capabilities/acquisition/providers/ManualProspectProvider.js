'use strict';

/**
 * Manual Prospect List acquisition provider (SPEC-060).
 * Operator paste / typed companies → Candidates (never ProspectList).
 */

const {
  ACQUISITION_SOURCES,
  IMPORT_METHODS,
  buildCandidate,
  buildAcquisitionEvidence,
} = require('../types');

/**
 * Reuse operator normalization when available (SPEC-043).
 * Lazy require avoids circular deps with mission-engine.
 */
function loadOperatorNormalize() {
  try {
    return require('../../../mission-engine/OperatorArtifactInjection');
  } catch {
    return null;
  }
}

/**
 * @param {object} [deps]
 */
function createManualProspectProvider(deps = {}) {
  const id = 'manual_prospect_list';

  return {
    id,
    available() {
      return true;
    },
    async acquire(request = {}) {
      const warnings = [];
      const provenanceBase = {
        acquisitionSource: ACQUISITION_SOURCES.MANUAL,
        provider: id,
        importMethod: IMPORT_METHODS.MANUAL_ENTRY,
        missionId: request.missionId || null,
        operator: request.operator || null,
      };

      let rows = [];
      if (Array.isArray(request.prospects) && request.prospects.length) {
        rows = request.prospects;
      } else if (typeof request.paste === 'string' && request.paste.trim()) {
        const op = deps.operatorInjection || loadOperatorNormalize();
        if (op && typeof op.parseDelimitedProspects === 'function') {
          rows = op.parseDelimitedProspects(request.paste);
          provenanceBase.importMethod = IMPORT_METHODS.SPREADSHEET_PASTE;
          provenanceBase.acquisitionSource =
            ACQUISITION_SOURCES.SPREADSHEET_PASTE;
        } else {
          rows = String(request.paste)
            .split(/\n/)
            .map((line) => line.trim())
            .filter(Boolean);
        }
      } else if (typeof request.text === 'string' && request.text.trim()) {
        rows = String(request.text)
          .split(/\n/)
          .map((line) => line.trim())
          .filter(Boolean);
      }

      const candidates = rows.map((raw, i) =>
        buildCandidate(raw, { index: i, provenance: provenanceBase })
      );

      if (!candidates.length) {
        warnings.push('No manual prospects provided');
      }

      return {
        candidates,
        evidence: [
          buildAcquisitionEvidence({
            provider: id,
            acquisitionSource: provenanceBase.acquisitionSource,
            candidateCount: candidates.length,
            importMethod: provenanceBase.importMethod,
            operator: provenanceBase.operator,
            missionId: provenanceBase.missionId,
            summary: `Manual prospect list acquired (${candidates.length})`,
          }),
        ],
        warnings,
      };
    },
    metadata() {
      return {
        id,
        label: 'Manual Prospect List',
        category: 'operator',
        acquisitionSource: ACQUISITION_SOURCES.MANUAL,
        status: 'new',
        supports: ['manual_entry', 'paste'],
        publishes: 'candidates',
        requiredFields: ['companyName'],
        optionalFields: [
          'address',
          'website',
          'phone',
          'email',
          'contactName',
          'notes',
          'naics',
          'source',
        ],
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
  createManualProspectProvider,
};
