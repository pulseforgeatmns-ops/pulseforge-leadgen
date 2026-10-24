'use strict';

/**
 * Existing Prospect Repository provider (SPEC-060).
 * Reuses a previously verified / saved ProspectList as Candidates.
 * Optional verification refresh happens downstream — never rediscovery.
 */

const {
  ACQUISITION_SOURCES,
  IMPORT_METHODS,
  buildCandidate,
  buildAcquisitionEvidence,
} = require('../types');

/**
 * @param {object} [deps]
 */
function createExistingProspectRepositoryProvider(deps = {}) {
  const id = 'existing_prospect_repository';

  return {
    id,
    available() {
      return true;
    },
    async acquire(request = {}) {
      const warnings = [];
      const list =
        request.prospectList ||
        request.existingList ||
        request.payload ||
        null;

      let prospects = [];
      if (Array.isArray(request.prospects)) {
        prospects = request.prospects;
      } else if (list && Array.isArray(list.prospects)) {
        prospects = list.prospects;
      } else if (typeof deps.loadProspectList === 'function') {
        const loaded = await deps.loadProspectList(request);
        prospects = Array.isArray(loaded && loaded.prospects)
          ? loaded.prospects
          : [];
      }

      if (!prospects.length) {
        warnings.push(
          'No existing ProspectList selected — supply prospectList or prospects'
        );
      }

      const provenance = {
        acquisitionSource: ACQUISITION_SOURCES.EXISTING_REPOSITORY,
        provider: id,
        importMethod: IMPORT_METHODS.EXISTING_LIST,
        missionId: request.missionId || null,
        operator: request.operator || null,
      };

      const listId =
        (list && (list.id || list.artifactId || list.name)) ||
        request.listId ||
        request.listName ||
        null;

      const candidates = prospects.map((raw, i) =>
        buildCandidate(raw, {
          index: i,
          provenance: {
            ...provenance,
            originalData: {
              listId,
              sourceProspectId: raw && (raw.id || raw.prospectId) || null,
              ...(raw && typeof raw === 'object' ? { row: raw } : {}),
            },
          },
        })
      );

      return {
        candidates,
        evidence: [
          buildAcquisitionEvidence({
            provider: id,
            acquisitionSource: ACQUISITION_SOURCES.EXISTING_REPOSITORY,
            candidateCount: candidates.length,
            importMethod: IMPORT_METHODS.EXISTING_LIST,
            operator: provenance.operator,
            missionId: provenance.missionId,
            summary: listId
              ? `Reused existing ProspectList "${listId}" (${candidates.length})`
              : `Reused existing ProspectList (${candidates.length})`,
            details: {
              listId,
              refreshVerification: Boolean(request.refreshVerification),
            },
          }),
        ],
        warnings,
      };
    },
    metadata() {
      return {
        id,
        label: 'Existing Prospect Repository',
        category: 'repository',
        acquisitionSource: ACQUISITION_SOURCES.EXISTING_REPOSITORY,
        status: 'new',
        supports: ['reuse', 'optional_verification_refresh'],
        publishes: 'candidates',
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
  createExistingProspectRepositoryProvider,
};
