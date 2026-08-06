'use strict';

/**
 * SPEC-083 — Map approved Business Blueprint → pending_review Client Playbook.
 *
 * Understanding only. Never invents preferredChannels, offers, or outreachSequence.
 * Invariant: The Playbook may only contain strategy directly supported by the
 * approved Business Blueprint or explicitly added by an operator.
 */

const {
  buildClientPlaybook,
  PLAYBOOK_STATUS,
} = require('../packages/capabilities/playbook/types');
const {
  ClientPlaybookStore,
} = require('../packages/capabilities/playbook/ClientPlaybookStore');
const {
  createPostgresClientPlaybookStore,
} = require('../packages/capabilities/playbook/PostgresClientPlaybookStore');

const defaultPool = require('../db');

/**
 * Provenance map: playbook field → blueprint section(s).
 * Every generated playbook section must be traceable to ≥1 blueprint section.
 */
const SECTION_PROVENANCE = Object.freeze({
  name: ['identity'],
  valuePropositions: ['services'],
  idealCustomer: ['idealCustomers', 'avoidCustomers', 'targetMarkets'],
  targetMarkets: ['targetMarkets'],
  brandVoice: ['brandVoice'],
  successMetrics: ['successMetrics'],
  notes: [
    'identity',
    'competitiveAdvantages',
    'campaignGoals',
    'services',
    'idealCustomers',
  ],
  // Strategy fields intentionally empty — not mapped from CIE
  preferredChannels: [],
  outreachSequence: [],
  offers: [],
  constraints: [],
});

function sectionSummary(sections, key) {
  const s = sections && sections[key];
  return s && s.summary ? String(s.summary).trim() : '';
}

function splitList(summary) {
  if (!summary) return [];
  return summary
    .split(/[,;\n]|•|\u2022/)
    .map((p) => p.trim())
    .filter(Boolean);
}

function normalizeVoice(summary) {
  const raw = String(summary || '')
    .trim()
    .toLowerCase()
    .replace(/[\s-]+/g, '_');
  const allowed = [
    'professional',
    'friendly',
    'relationship_first',
    'technical',
    'premium',
    'direct',
  ];
  for (const v of allowed) {
    if (raw.includes(v)) return v;
  }
  if (raw.includes('relationship')) return 'relationship_first';
  return 'professional';
}

function buildNotes(blueprint) {
  const sections = blueprint.sections || {};
  const lines = [
    `CIE handoff from Business Blueprint ${blueprint.id}@${blueprint.version}`,
    `generatedBy=${blueprint.generated_by || 'CIE-v1'}`,
    '',
    'Understanding (not strategy):',
  ];
  const identity = sectionSummary(sections, 'identity');
  const advantages = sectionSummary(sections, 'competitiveAdvantages');
  const goals = sectionSummary(sections, 'campaignGoals');
  if (identity) lines.push(`Identity: ${identity}`);
  if (advantages) lines.push(`Competitive advantages: ${advantages}`);
  if (goals) lines.push(`Campaign goals: ${goals}`);
  lines.push('');
  lines.push(
    'Strategy fields (preferredChannels, offers, outreachSequence) left empty for operator.'
  );
  lines.push('');
  lines.push('sectionProvenance=' + JSON.stringify(SECTION_PROVENANCE));
  return lines.join('\n');
}

/**
 * @param {object} blueprint — normalized cie blueprint row
 * @param {{ playbookStore?: object, pool?: object }} [opts]
 */
async function createPlaybookFromApprovedBlueprint(blueprint, opts = {}) {
  if (!blueprint || !blueprint.id) {
    throw new Error('blueprint is required for playbook handoff');
  }

  const clientId =
    blueprint.client_id != null ? blueprint.client_id : blueprint.clientId;
  const generatedBy = blueprint.generated_by || blueprint.generatedBy || 'CIE-v1';
  const sections = blueprint.sections || {};
  const identity = sectionSummary(sections, 'identity');
  const services = sectionSummary(sections, 'services');
  const ideal = sectionSummary(sections, 'idealCustomers');
  const avoid = sectionSummary(sections, 'avoidCustomers');
  const markets = sectionSummary(sections, 'targetMarkets');
  const voice = sectionSummary(sections, 'brandVoice');
  const metrics = sectionSummary(sections, 'successMetrics');

  const name =
    identity.split(/[.!\n]/)[0].trim().slice(0, 120) ||
    `Client ${clientId} Playbook`;

  const playbookId = `cie-client-${clientId}`;
  const payload = buildClientPlaybook({
    id: playbookId,
    clientId,
    name,
    version: '1.0',
    status: PLAYBOOK_STATUS.PENDING_REVIEW,
    targetMarkets: splitList(markets),
    valuePropositions: splitList(services).length
      ? splitList(services)
      : services
        ? [services]
        : [],
    idealCustomer: {
      primaryMarkets: splitList(ideal).length
        ? splitList(ideal)
        : ideal
          ? [ideal]
          : [],
      secondaryMarkets: [],
      geographicCoverage: markets || '',
      minimumCompanySize: null,
      industriesToAvoid: splitList(avoid).length
        ? splitList(avoid)
        : avoid
          ? [avoid]
          : [],
      buyingTriggers: [],
    },
    brandVoice: normalizeVoice(voice),
    preferredChannels: [],
    outreachSequence: [],
    offers: [],
    constraints: [],
    successMetrics: splitList(metrics).length
      ? splitList(metrics)
      : metrics
        ? [metrics]
        : [],
    notes: buildNotes({
      id: blueprint.id,
      version: blueprint.version,
      generated_by: generatedBy,
      sections,
    }),
  });

  // Assert invariant: no invented strategy
  if (
    payload.preferredChannels.length ||
    payload.outreachSequence.length ||
    payload.offers.length
  ) {
    throw new Error(
      'CIE handoff must not invent preferredChannels, outreachSequence, or offers'
    );
  }

  // Assert provenance for every non-empty understanding field
  const provenance = { ...SECTION_PROVENANCE };
  for (const [field, sources] of Object.entries(provenance)) {
    if (['preferredChannels', 'outreachSequence', 'offers', 'constraints'].includes(field)) {
      continue;
    }
    if (!sources.length) {
      throw new Error(`Playbook field ${field} lacks blueprint provenance`);
    }
  }

  let store = opts.playbookStore;
  if (!store) {
    if (opts.pool) {
      store = createPostgresClientPlaybookStore(opts.pool);
    } else if (opts.useMemoryPlaybookStore) {
      store = new ClientPlaybookStore({ seed: false });
    } else {
      try {
        store = createPostgresClientPlaybookStore(defaultPool);
      } catch (_) {
        store = new ClientPlaybookStore({ seed: false });
      }
    }
  }

  let playbook;
  const existing = await store.get(playbookId);
  if (existing) {
    playbook = await store.createVersion(playbookId, {
      ...payload,
      id: playbookId,
    }, { autoActivate: false });
  } else {
    playbook = await store.create(payload);
  }

  if (playbook.status !== PLAYBOOK_STATUS.PENDING_REVIEW) {
    throw new Error(
      `CIE handoff must create pending_review playbook (got ${playbook.status})`
    );
  }

  return {
    playbook: {
      id: playbook.id,
      version: playbook.version,
      status: playbook.status,
      clientId: playbook.clientId,
      name: playbook.name,
      preferredChannels: playbook.preferredChannels,
      outreachSequence: playbook.outreachSequence,
      offers: playbook.offers,
      valuePropositions: playbook.valuePropositions,
      idealCustomer: playbook.idealCustomer,
      targetMarkets: playbook.targetMarkets,
      brandVoice: playbook.brandVoice,
      successMetrics: playbook.successMetrics,
      notes: playbook.notes,
    },
    sectionProvenance: provenance,
  };
}

module.exports = {
  SECTION_PROVENANCE,
  createPlaybookFromApprovedBlueprint,
  buildNotes,
};
