'use strict';

/**
 * Prospect Acquisition orchestrator (SPEC-060 / ADR-044).
 * Selects a provider, acquires Candidates, optionally verifies → ProspectList.
 * No campaign logic.
 */

const {
  ACQUISITION_STRATEGIES,
  ACQUISITION_SOURCES,
  buildCandidateSet,
  buildAcquisitionEvidence,
  isValidAcquireResult,
} = require('./types');
const { assertProviderContract } = require('./providerContract');
const {
  createDefaultAcquisitionRegistry,
} = require('./ProviderRegistry');
const { verifyCandidateSet } = require('./ProspectVerification');

const STRATEGY_TO_PROVIDER = Object.freeze({
  [ACQUISITION_STRATEGIES.DISCOVERY]: 'google_places',
  [ACQUISITION_STRATEGIES.MANUAL]: 'manual_prospect_list',
  [ACQUISITION_STRATEGIES.CSV]: 'csv_import',
  [ACQUISITION_STRATEGIES.EXISTING]: 'existing_prospect_repository',
});

/**
 * Infer acquisition strategy from a free-text objective / request.
 * @param {string} text
 * @param {object} [hints]
 * @returns {string}
 */
function selectAcquisitionStrategy(text, hints = {}) {
  if (hints.acquisitionStrategy) {
    const s = String(hints.acquisitionStrategy).toLowerCase();
    if (Object.values(ACQUISITION_STRATEGIES).includes(s)) return s;
  }
  if (hints.csv || hints.csvText) return ACQUISITION_STRATEGIES.CSV;
  if (hints.prospects || hints.paste || hints.manual) {
    return ACQUISITION_STRATEGIES.MANUAL;
  }
  if (
    hints.prospectList ||
    hints.existingList ||
    hints.listId ||
    hints.prospectList === 'current'
  ) {
    return ACQUISITION_STRATEGIES.EXISTING;
  }

  const lower = String(text || '').toLowerCase();
  if (
    /\b(import\s+(this\s+)?csv|csv\s+import|upload\s+csv)\b/.test(lower)
  ) {
    return ACQUISITION_STRATEGIES.CSV;
  }
  if (
    /\b(i('ll| will)?\s+type|manual(ly)?|paste\s+(the\s+)?(companies|list)|i already have)\b/.test(
      lower
    ) ||
    /\btype\s+the\s+companies\b/.test(lower)
  ) {
    return ACQUISITION_STRATEGIES.MANUAL;
  }
  if (
    /\b(use|reuse|using)\b[\s\S]{0,40}\b(campaign\s+\d+|existing|previous|current|saved)\b[\s\S]{0,40}\b(list|prospects)\b/.test(
      lower
    ) ||
    /\b(manchester\s+commercial\s+cleaning\s+list|existing\s+prospect\s+list)\b/.test(
      lower
    ) ||
    /\busing\s+the\s+current\s+prospectlist\b/.test(lower) ||
    /\busing\s+my\b[\s\S]{0,30}\blist\b/.test(lower)
  ) {
    return ACQUISITION_STRATEGIES.EXISTING;
  }
  if (
    /\b(find|discover|search|scrape)\b[\s\S]{0,40}\b(\d+\s+)?(prospects?|companies|cleaners?)\b/.test(
      lower
    )
  ) {
    return ACQUISITION_STRATEGIES.DISCOVERY;
  }
  return ACQUISITION_STRATEGIES.DISCOVERY;
}

/**
 * @param {object} [deps]
 */
function createProspectAcquisition(deps = {}) {
  const registry =
    deps.registry || createDefaultAcquisitionRegistry(deps);

  return {
    registry,

    /**
     * Acquire a CandidateSet from the selected provider.
     * @param {object} request
     */
    async acquire(request = {}) {
      const strategy = selectAcquisitionStrategy(
        request.objective || request.text || '',
        request
      );
      const providerId =
        request.providerId ||
        STRATEGY_TO_PROVIDER[strategy] ||
        'manual_prospect_list';
      const provider = registry.get(providerId);

      if (!provider) {
        return {
          ok: false,
          strategy,
          providerId,
          candidateSet: buildCandidateSet({ candidates: [] }),
          errors: [`Unknown acquisition provider: ${providerId}`],
          warnings: [],
          evidence: [],
        };
      }

      if (!provider.available()) {
        const alternatives = registry
          .available()
          .map((p) => p.metadata().id)
          .filter((id) => id !== providerId);
        return {
          ok: false,
          strategy,
          providerId,
          candidateSet: buildCandidateSet({ candidates: [] }),
          errors: [`Provider ${providerId} is not available`],
          warnings: alternatives.length
            ? [
                `Recommended alternatives: ${alternatives.join(', ')}`,
              ]
            : ['Manual acquisition is always available'],
          evidence: [
            buildAcquisitionEvidence({
              provider: providerId,
              acquisitionSource: strategy,
              candidateCount: 0,
              summary: `Provider ${providerId} unavailable`,
            }),
          ],
          recommendedStrategies: alternatives.includes('manual_prospect_list')
            ? [ACQUISITION_STRATEGIES.MANUAL, ACQUISITION_STRATEGIES.CSV]
            : alternatives,
        };
      }

      let raw;
      try {
        raw = await provider.acquire({
          ...request,
          strategy,
        });
      } catch (err) {
        return {
          ok: false,
          strategy,
          providerId,
          candidateSet: buildCandidateSet({ candidates: [] }),
          errors: [String(err && err.message ? err.message : err)],
          warnings: [
            'Acquisition failed — try Manual or CSV import',
          ],
          evidence: [],
          recommendedStrategies: [
            ACQUISITION_STRATEGIES.MANUAL,
            ACQUISITION_STRATEGIES.CSV,
          ],
        };
      }

      if (!isValidAcquireResult(raw)) {
        return {
          ok: false,
          strategy,
          providerId,
          candidateSet: buildCandidateSet({ candidates: [] }),
          errors: ['Provider returned invalid acquire() result'],
          warnings: [],
          evidence: [],
        };
      }

      // Guard: providers must not publish ProspectLists
      if (raw.prospectList || raw.type === 'ProspectList') {
        return {
          ok: false,
          strategy,
          providerId,
          candidateSet: buildCandidateSet({ candidates: [] }),
          errors: [
            'Provider published ProspectList — contract violation (Candidates only)',
          ],
          warnings: [],
          evidence: [],
        };
      }

      const meta = provider.metadata();
      const candidateSet = buildCandidateSet({
        candidates: raw.candidates,
        provider: providerId,
        acquisitionSource:
          meta.acquisitionSource ||
          ACQUISITION_SOURCES[String(strategy).toUpperCase()] ||
          strategy,
        missionId: request.missionId,
        operator: request.operator,
        importMethod: request.importMethod,
        evidence: raw.evidence || [],
        warnings: raw.warnings || [],
        summary: {
          strategy,
          preview: raw.preview || null,
        },
      });

      return {
        ok: candidateSet.candidateCount > 0,
        strategy,
        providerId,
        candidateSet,
        preview: raw.preview || null,
        errors: [],
        warnings: raw.warnings || [],
        evidence: [
          ...(raw.evidence || []),
          buildAcquisitionEvidence({
            provider: providerId,
            acquisitionSource: candidateSet.acquisitionSource,
            candidateCount: candidateSet.candidateCount,
            missionId: request.missionId,
            operator: request.operator,
            summary: `Acquired ${candidateSet.candidateCount} candidates via ${meta.label || providerId}`,
          }),
        ],
      };
    },

    /**
     * Acquire + verify → ProspectList (shared verification pipeline).
     * @param {object} request
     */
    async acquireAndVerify(request = {}) {
      const acquired = await this.acquire(request);
      if (!acquired.ok && !(request.verifyEmpty === true)) {
        return {
          ...acquired,
          prospectList: null,
          rejectedProspects: [],
          verificationReport: null,
        };
      }

      const verified = verifyCandidateSet({
        candidateSet: acquired.candidateSet,
        profile: request.profile,
        targetCount: request.targetCount,
        options: {
          operatorSupplied:
            acquired.strategy !== ACQUISITION_STRATEGIES.DISCOVERY,
          acceptSoftFailures: Boolean(request.acceptSoftFailures),
        },
        missionId: request.missionId,
        operator: request.operator,
      });

      return {
        ...acquired,
        ok: verified.ok,
        prospectList: verified.prospectList,
        rejectedProspects: verified.rejectedProspects,
        verificationReport: verified.verificationReport,
        evidence: [...(acquired.evidence || []), ...(verified.evidence || [])],
        warnings: [
          ...(acquired.warnings || []),
          ...(verified.warnings || []),
        ],
      };
    },

    listProviders() {
      return registry.listMetadata();
    },

    health() {
      return registry.healthReport();
    },

    selectStrategy: selectAcquisitionStrategy,
  };
}

/**
 * Workspace summary for Prospect Acquisition panel.
 * @param {object} state
 */
function buildAcquisitionWorkspaceView(state = {}) {
  const acquisition = state.acquisition || state.lastAcquisition || null;
  const verification = state.verificationReport || null;
  const prospectList = state.prospectList || null;

  return {
    section: 'Prospect Acquisition',
    provider:
      (acquisition && (acquisition.providerId || acquisition.provider)) ||
      null,
    status: !acquisition
      ? 'idle'
      : acquisition.ok
        ? verification || prospectList
          ? 'prospect_list_generated'
          : 'candidates_acquired'
        : 'failed',
    candidateCount:
      (acquisition &&
        acquisition.candidateSet &&
        acquisition.candidateSet.candidateCount) ||
      0,
    importSummary:
      (acquisition &&
        acquisition.evidence &&
        acquisition.evidence[0] &&
        acquisition.evidence[0].summary) ||
      null,
    verificationStatus: verification
      ? `accepted ${verification.acceptedCount}, rejected ${verification.rejectedCount}`
      : prospectList
        ? 'complete'
        : 'pending',
    source:
      (acquisition &&
        acquisition.candidateSet &&
        acquisition.candidateSet.acquisitionSource) ||
      null,
    operator: state.operator || null,
    strategy: (acquisition && acquisition.strategy) || null,
    executionEvidence: (acquisition && acquisition.evidence) || [],
    importedCandidates:
      acquisition &&
      acquisition.candidateSet &&
      Array.isArray(acquisition.candidateSet.candidates)
        ? acquisition.candidateSet.candidates
        : [],
    prospectListGenerated: Boolean(
      prospectList && (prospectList.prospectCount || 0) > 0
    ),
    failures: (acquisition && acquisition.errors) || [],
  };
}

module.exports = {
  STRATEGY_TO_PROVIDER,
  selectAcquisitionStrategy,
  createProspectAcquisition,
  buildAcquisitionWorkspaceView,
  assertProviderContract,
};
