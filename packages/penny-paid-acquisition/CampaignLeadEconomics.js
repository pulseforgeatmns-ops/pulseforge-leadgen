'use strict';

/**
 * SPEC-256 — Deterministic campaign alignment between PLATFORM_API evidence
 * and FIRST_PARTY_ATTRIBUTION walkthrough leads.
 *
 * Pure/read-only derivation. No DB access.
 */

const { AVAILABILITY, PLATFORM } = require('./types');
const { resolveObservationWindow } = require('./FirstPartyAttributionEvidence');

const SPEC = 'SPEC-256';

const WINDOW_ALIGNMENT = Object.freeze({
  ALIGNED: 'ALIGNED',
  MISMATCHED: 'MISMATCHED',
});

const ECONOMICS_STATUS = Object.freeze({
  OK: 'OK',
  NO_FIRST_PARTY_LEADS: 'NO_FIRST_PARTY_LEADS',
  SPEND_UNAVAILABLE_OR_ZERO_WITH_ATTRIBUTED_LEADS: 'SPEND_UNAVAILABLE_OR_ZERO_WITH_ATTRIBUTED_LEADS',
  PLATFORM_UNAVAILABLE: 'PLATFORM_UNAVAILABLE',
  FIRST_PARTY_UNAVAILABLE: 'FIRST_PARTY_UNAVAILABLE',
  WINDOW_MISMATCHED: 'WINDOW_MISMATCHED',
});

const LEAD_SOURCE_TO_PLATFORM = Object.freeze({
  chatgpt_ads: PLATFORM.CHATGPT_ADS,
  google_ads: PLATFORM.GOOGLE_ADS,
  yelp: PLATFORM.YELP,
  meta_ads: PLATFORM.META_ADS,
  meta: PLATFORM.META_ADS,
});

function asText(value) {
  return value == null ? '' : String(value).trim();
}

function normalizeCampaignId(id) {
  const text = asText(id);
  return text || null;
}

function resolveSharedObservationWindow(opts = {}) {
  if (opts.observationWindow && opts.observationWindow.start && opts.observationWindow.end) {
    return resolveObservationWindow(opts.observationWindow, opts.observationWindowDays);
  }
  return resolveObservationWindow(null, opts.observationWindowDays);
}

function windowsMatch(a, b) {
  if (!a || !b) return false;
  return asText(a.start) === asText(b.start) && asText(a.end) === asText(b.end);
}

function resolveWindowAlignment(observationWindow, platformWindow, retrievalWindow) {
  const platformAligned = !platformWindow || windowsMatch(observationWindow, platformWindow);
  const retrievalAligned = !retrievalWindow || windowsMatch(observationWindow, retrievalWindow);
  return platformAligned && retrievalAligned
    ? WINDOW_ALIGNMENT.ALIGNED
    : WINDOW_ALIGNMENT.MISMATCHED;
}

function leadSourcePlatform(leadSource) {
  return LEAD_SOURCE_TO_PLATFORM[asText(leadSource).toLowerCase()] || null;
}

function isProviderCompatible(platform, leadSource) {
  const expected = leadSourcePlatform(leadSource);
  if (!expected || !platform) return true;
  return asText(platform).toLowerCase() === expected;
}

function buildFirstPartyAttributionRetrieval(result = {}) {
  const availability = result.availability || AVAILABILITY.UNAVAILABLE;
  let observedCount = null;
  if (availability === AVAILABILITY.AVAILABLE) {
    observedCount = Number.isInteger(result.observedCount)
      ? result.observedCount
      : (Array.isArray(result.evidence) ? result.evidence.length : 0);
  }
  return {
    availability,
    observedCount,
    reason: result.reason || null,
    observationWindow: result.observationWindow || null,
  };
}

function extractFirstPartyAttributionEvidence(evidence = []) {
  return (evidence || []).filter((row) => row?.kind === 'first_party_attributed_lead');
}

function roundMoney(value) {
  return Math.round(value * 100) / 100;
}

function computeCostPerFirstPartyLead(spend, deterministicLeadCount, windowAlignment) {
  if (windowAlignment !== WINDOW_ALIGNMENT.ALIGNED) return null;
  if (deterministicLeadCount <= 0) return null;
  if (spend == null || !Number.isFinite(spend) || spend <= 0) return null;
  return roundMoney(spend / deterministicLeadCount);
}

function resolveEconomicsStatus({
  platformAvailable,
  firstPartyAvailable,
  windowAlignment,
  spend,
  deterministicLeadCount,
  inferredLeadCount,
}) {
  if (!firstPartyAvailable) return ECONOMICS_STATUS.FIRST_PARTY_UNAVAILABLE;
  if (!platformAvailable) return ECONOMICS_STATUS.PLATFORM_UNAVAILABLE;
  if (windowAlignment !== WINDOW_ALIGNMENT.ALIGNED) return ECONOMICS_STATUS.WINDOW_MISMATCHED;
  if ((spend == null || spend <= 0) && (deterministicLeadCount + inferredLeadCount) > 0) {
    return ECONOMICS_STATUS.SPEND_UNAVAILABLE_OR_ZERO_WITH_ATTRIBUTED_LEADS;
  }
  if (deterministicLeadCount === 0 && spend > 0) return ECONOMICS_STATUS.NO_FIRST_PARTY_LEADS;
  return ECONOMICS_STATUS.OK;
}

/**
 * Derive inspectable campaign-level lead economics from retrieved evidence.
 *
 * @param {object} input
 * @param {object[]} input.platformEvidence
 * @param {object[]} input.firstPartyAttributionEvidence
 * @param {object} input.firstPartyAttributionRetrieval
 * @param {object} input.observationWindow
 * @returns {object}
 */
function deriveCampaignLeadEconomics(input = {}) {
  const platformEvidence = Array.isArray(input.platformEvidence) ? input.platformEvidence : [];
  const firstPartyAttributionEvidence = Array.isArray(input.firstPartyAttributionEvidence)
    ? input.firstPartyAttributionEvidence
    : [];
  const firstPartyAttributionRetrieval = input.firstPartyAttributionRetrieval || {};
  const observationWindow = input.observationWindow || null;

  const unmatchedFirstPartyAttribution = {
    missingCampaignIdCount: 0,
    unmatchedCampaignIdCount: 0,
    providerConflictCount: 0,
    inferredCount: 0,
    unattributedCount: 0,
  };

  const firstPartyAvailable = firstPartyAttributionRetrieval.availability === AVAILABILITY.AVAILABLE;
  const anyPlatformAvailable = platformEvidence.some((row) => row.availability === AVAILABILITY.AVAILABLE);

  if (!firstPartyAvailable) {
    return {
      spec: SPEC,
      campaignLeadEconomics: [],
      unmatchedFirstPartyAttribution,
      platformConversionsAreSeparateFromFirstPartyLeads: true,
    };
  }

  if (!anyPlatformAvailable) {
    summarizeUnmatchedFirstParty(
      firstPartyAttributionEvidence,
      new Set(),
      unmatchedFirstPartyAttribution
    );
    return {
      spec: SPEC,
      campaignLeadEconomics: [],
      unmatchedFirstPartyAttribution,
      platformConversionsAreSeparateFromFirstPartyLeads: true,
    };
  }

  const platformCampaigns = [];
  for (const platformRow of platformEvidence) {
    if (platformRow.availability !== AVAILABILITY.AVAILABLE) continue;
    const windowAlignment = resolveWindowAlignment(
      observationWindow,
      platformRow.observationWindow,
      firstPartyAttributionRetrieval.observationWindow
    );
    for (const campaign of platformRow.campaigns || []) {
      platformCampaigns.push({
        platform: platformRow.platform,
        channel: platformRow.channel,
        externalCampaignId: normalizeCampaignId(campaign.externalCampaignId),
        campaignName: campaign.name || null,
        observedPlatformSpend: campaign.spend,
        platformConversions: campaign.platformConversions ?? 0,
        observationWindow: observationWindow || platformRow.observationWindow || null,
        windowAlignment,
      });
    }
  }

  const leadsByCampaignId = groupFirstPartyLeads(
    firstPartyAttributionEvidence,
    unmatchedFirstPartyAttribution
  );
  const knownPlatformCampaignIds = new Set(
    platformCampaigns.map((row) => row.externalCampaignId).filter(Boolean)
  );
  const joinedCampaignIds = new Set();
  const campaignLeadEconomics = [];

  for (const platformCampaign of platformCampaigns) {
    const campaignId = platformCampaign.externalCampaignId;
    if (!campaignId) continue;

    const leads = leadsByCampaignId.get(campaignId) || [];
    const counts = countJoinedLeads(leads, platformCampaign.platform, unmatchedFirstPartyAttribution);
    if (counts.deterministicLeadCount > 0 || counts.inferredLeadCount > 0) {
      joinedCampaignIds.add(campaignId);
    }

    const firstPartyAttributedLeadCount = counts.deterministicLeadCount;
    const costPerFirstPartyLead = computeCostPerFirstPartyLead(
      platformCampaign.observedPlatformSpend,
      firstPartyAttributedLeadCount,
      platformCampaign.windowAlignment
    );
    const economicsStatus = resolveEconomicsStatus({
      platformAvailable: true,
      firstPartyAvailable: true,
      windowAlignment: platformCampaign.windowAlignment,
      spend: platformCampaign.observedPlatformSpend,
      deterministicLeadCount: counts.deterministicLeadCount,
      inferredLeadCount: counts.inferredLeadCount,
    });

    campaignLeadEconomics.push({
      spec: SPEC,
      platform: platformCampaign.platform,
      externalCampaignId: campaignId,
      campaignName: platformCampaign.campaignName,
      observationWindow: platformCampaign.observationWindow,
      windowAlignment: platformCampaign.windowAlignment,
      observedPlatformSpend: platformCampaign.observedPlatformSpend,
      firstPartyAttributedLeadCount,
      costPerFirstPartyLead,
      platformConversions: platformCampaign.platformConversions,
      cashExposure: null,
      cashExposureStatus: 'UNKNOWN',
      attribution: {
        deterministicLeadCount: counts.deterministicLeadCount,
        inferredLeadCount: counts.inferredLeadCount,
        unmatchedLeadCount: 0,
      },
      economicsStatus,
      evidenceOnly: true,
      platformConversionsAreSeparateFromFirstPartyLeads: true,
    });
  }

  for (const [campaignId, leads] of leadsByCampaignId.entries()) {
    if (joinedCampaignIds.has(campaignId)) continue;
    if (!knownPlatformCampaignIds.has(campaignId)) {
      unmatchedFirstPartyAttribution.unmatchedCampaignIdCount += leads.length;
    }
  }

  return {
    spec: SPEC,
    campaignLeadEconomics,
    unmatchedFirstPartyAttribution,
    platformConversionsAreSeparateFromFirstPartyLeads: true,
  };
}

function groupFirstPartyLeads(firstPartyAttributionEvidence, unmatched) {
  const byCampaignId = new Map();
  const seenEvidenceIds = new Set();

  for (const row of firstPartyAttributionEvidence) {
    if (row?.kind !== 'first_party_attributed_lead') continue;

    const evidenceId = row.evidenceId != null ? String(row.evidenceId) : null;
    if (evidenceId) {
      if (seenEvidenceIds.has(evidenceId)) continue;
      seenEvidenceIds.add(evidenceId);
    }

    const status = asText(row.attributionStatus).toLowerCase();
    if (status === 'unattributed') {
      unmatched.unattributedCount += 1;
      continue;
    }

    const campaignId = normalizeCampaignId(row.campaignId);
    if (!campaignId) {
      unmatched.missingCampaignIdCount += 1;
      continue;
    }

    if (status === 'inferred') {
      unmatched.inferredCount += 1;
    }

    if (!byCampaignId.has(campaignId)) byCampaignId.set(campaignId, []);
    byCampaignId.get(campaignId).push(row);
  }

  return byCampaignId;
}

function countJoinedLeads(leads, platform, unmatched) {
  const countedIds = new Set();
  let deterministicLeadCount = 0;
  let inferredLeadCount = 0;

  for (const lead of leads) {
    if (!isProviderCompatible(platform, lead.leadSource)) {
      unmatched.providerConflictCount += 1;
      continue;
    }

    const evidenceId = lead.evidenceId != null ? String(lead.evidenceId) : null;
    if (evidenceId && countedIds.has(evidenceId)) continue;
    if (evidenceId) countedIds.add(evidenceId);

    const status = asText(lead.attributionStatus).toLowerCase();
    if (status === 'deterministic') deterministicLeadCount += 1;
    else if (status === 'inferred') inferredLeadCount += 1;
  }

  return { deterministicLeadCount, inferredLeadCount };
}

function summarizeUnmatchedFirstParty(firstPartyAttributionEvidence, joinedCampaignIds, unmatched) {
  const byCampaignId = groupFirstPartyLeads(firstPartyAttributionEvidence, unmatched);
  for (const [campaignId, leads] of byCampaignId.entries()) {
    if (joinedCampaignIds.has(campaignId)) continue;
    unmatched.unmatchedCampaignIdCount += leads.length;
  }
}

module.exports = {
  SPEC,
  WINDOW_ALIGNMENT,
  ECONOMICS_STATUS,
  resolveSharedObservationWindow,
  buildFirstPartyAttributionRetrieval,
  extractFirstPartyAttributionEvidence,
  deriveCampaignLeadEconomics,
  normalizeCampaignId,
  isProviderCompatible,
  windowsMatch,
};
