// Canonical vertical-tier resolution shared by Scout, dynamic scoring, and warm routing.
// Tier configuration lives on clients.vertical_tiers; never infer Tier A from a label.

const TIER_POLICY = Object.freeze({
  A: { vertical_points: 25, score_ceiling: 100, autonomous_sourcing: true, warm_eligible: true },
  B: { vertical_points: 15, score_ceiling: 100, autonomous_sourcing: false, warm_eligible: true },
  C: { vertical_points: 0, score_ceiling: 30, autonomous_sourcing: false, warm_eligible: false },
  W: { vertical_points: 0, score_ceiling: 30, autonomous_sourcing: false, warm_eligible: false },
  unknown: { vertical_points: 0, score_ceiling: 60, autonomous_sourcing: false, warm_eligible: false },
});

function normalizeVertical(value) {
  return String(value == null ? '' : value)
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_+|_+$/g, '')
    .replace(/_+/g, '_');
}

function normalizeTierMap(verticalTiers) {
  if (!verticalTiers || typeof verticalTiers !== 'object' || Array.isArray(verticalTiers)) return {};
  return Object.entries(verticalTiers).reduce((map, [rawVertical, rawTier]) => {
    const vertical = normalizeVertical(rawVertical);
    const tier = String(rawTier || '').trim().toUpperCase();
    if (vertical && Object.hasOwn(TIER_POLICY, tier)) map[vertical] = tier;
    return map;
  }, {});
}

function resolveVerticalTier(rawVertical, clientConfig = {}) {
  const vertical = normalizeVertical(rawVertical);
  const tiers = normalizeTierMap(clientConfig.vertical_tiers);
  const tier = vertical && tiers[vertical] ? tiers[vertical] : 'unknown';
  return {
    vertical,
    tier,
    matched: tier !== 'unknown',
    ...TIER_POLICY[tier],
  };
}

function targetVerticalEntries(clientConfig = {}) {
  if (!Array.isArray(clientConfig.target_verticals)) return [];
  return clientConfig.target_verticals
    .filter(entry => entry && typeof entry === 'object' && !Array.isArray(entry))
    .map(entry => ({
      ...entry,
      vertical: normalizeVertical(entry.vertical),
      tier: String(entry.tier || '').trim().toUpperCase(),
      autonomous_sourcing: entry.autonomous_sourcing === true,
      seed_terms: Array.isArray(entry.seed_terms)
        ? entry.seed_terms.map(term => String(term || '').trim()).filter(Boolean)
        : [],
    }))
    .filter(entry => entry.vertical);
}

function autonomousTargetVerticals(clientConfig = {}) {
  return targetVerticalEntries(clientConfig)
    .filter(entry => entry.autonomous_sourcing && entry.tier === 'A')
    .map(entry => entry.vertical);
}

module.exports = {
  TIER_POLICY,
  normalizeVertical,
  normalizeTierMap,
  resolveVerticalTier,
  targetVerticalEntries,
  autonomousTargetVerticals,
};
