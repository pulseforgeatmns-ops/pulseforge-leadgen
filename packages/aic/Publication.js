'use strict';

/**
 * SPEC-113 Stage 5 — Publication.
 * Only published AIMs become runtime knowledge.
 * Compiler never executes outreach.
 */

const {
  WORKSPACE_STATUS,
  CONCEPT_TYPES,
  CONCEPT_STATUS,
  RELATIONS,
  asText,
  asList,
  nowIso,
  slugify,
} = require('./types');
const { buildAim, AIM_STATUS, PAIN_CATEGORIES } = require('../aim');
const { publishableConcepts } = require('./Review');
const { neighbors } = require('./Ontology');

function assertApproved(workspace) {
  if (!workspace || workspace.status !== WORKSPACE_STATUS.APPROVED) {
    const err = new Error('Publish requires operator approval. Nothing publishes automatically.');
    err.code = 'aic_not_approved';
    throw err;
  }
}

function active(concepts, type) {
  return concepts.filter((c) => c.type === type);
}

function firstStatement(concepts, type) {
  const row = active(concepts, type)[0];
  return row ? asText(row.statement) : '';
}

function parseTransformation(concepts) {
  const row = active(concepts, CONCEPT_TYPES.TRANSFORMATION)[0];
  if (!row) return { currentState: '', futureState: '' };
  if (row.meta && (row.meta.currentState || row.meta.futureState)) {
    return {
      currentState: asText(row.meta.currentState),
      futureState: asText(row.meta.futureState),
    };
  }
  const current = asText(row.statement).match(/current:\s*(.+?)(?:future:|$)/i);
  const future = asText(row.statement).match(/future:\s*(.+)$/i);
  return {
    currentState: current ? asText(current[1]) : '',
    futureState: future ? asText(future[1]) : '',
  };
}

function expandSignal(label) {
  const s = String(label || '').toLowerCase().trim();
  if (!s) return [];
  const extra = [];
  if (/^job posts?$/.test(s)) extra.push('job postings', 'job posting');
  return [s, ...extra];
}

function signalLabels(workspace, pain) {
  const ontology = { concepts: workspace.concepts, edges: workspace.edges };
  const observed = neighbors(ontology, pain.id, RELATIONS.OBSERVED_THROUGH).map((c) => c.label);
  const buying = neighbors(ontology, pain.id, RELATIONS.MAPS_TO)
    .filter((c) => c.type === CONCEPT_TYPES.BUYING_TRIGGER)
    .map((c) => c.label);
  const language = neighbors(ontology, pain.id, RELATIONS.BELONGS_TO)
    .filter((c) => c.type === CONCEPT_TYPES.LANGUAGE)
    .map((c) => c.statement)
    .filter((s) => s && s.length <= 80);
  const extras = asList(pain.meta && pain.meta.signals);
  return [...new Set([...observed, ...buying, ...language, ...extras].flatMap(expandSignal).filter(Boolean))];
}

function buildPainOntologyFromWorkspace(workspace, concepts) {
  const pains = active(concepts, CONCEPT_TYPES.PAIN);
  const problems = pains.map((pain) => ({
    id: slugify(pain.label),
    label: pain.label,
    definition: pain.statement,
    signals: signalLabels(workspace, pain).map((s) => String(s).toLowerCase()),
  }));
  return [
    {
      id: PAIN_CATEGORIES.PEOPLE_MANAGEMENT,
      label: 'People Management',
      problems: problems.length
        ? problems
        : [],
    },
  ];
}

function buildKnowledge(workspace, concepts) {
  const pains = active(concepts, CONCEPT_TYPES.PAIN);
  const ontology = { concepts: workspace.concepts, edges: workspace.edges };
  return pains.map((pain) => {
    const related = (kind) =>
      neighbors(ontology, pain.id, RELATIONS.BELONGS_TO)
        .concat(neighbors(ontology, pain.id, RELATIONS.SUPPORTED_BY))
        .filter((c) => c.type === kind)
        .map((c) => c.statement);
    return {
      painId: slugify(pain.label),
      label: pain.label,
      definition: pain.statement,
      observableEvidence: [
        ...signalLabels(workspace, pain),
        ...related(CONCEPT_TYPES.EVIDENCE),
      ],
      commonObjections: related(CONCEPT_TYPES.OBJECTION).concat(
        active(concepts, CONCEPT_TYPES.OBJECTION).map((c) => c.statement)
      ),
      typicalLanguage: related(CONCEPT_TYPES.LANGUAGE).concat(
        active(concepts, CONCEPT_TYPES.LANGUAGE).map((c) => c.statement)
      ),
      recommendedMessaging: active(concepts, CONCEPT_TYPES.MESSAGING).map((c) => c.statement),
      discoveryQuestions: [],
      caseStudies: [],
      successStories: [],
    };
  });
}

function toPublishedAim(workspace, opts = {}) {
  assertApproved(workspace);
  const concepts = publishableConcepts(workspace);
  const mission = firstStatement(concepts, CONCEPT_TYPES.MISSION);
  if (!mission) {
    const err = new Error('Cannot publish an AIM without an accepted mission. Missing knowledge stays unknown — it is not invented.');
    err.code = 'aic_mission_required';
    throw err;
  }
  const transformation = parseTransformation(concepts);
  const icpRow = active(concepts, CONCEPT_TYPES.ICP)[0];
  const disqualifiers = active(concepts, CONCEPT_TYPES.DISQUALIFIER);
  const icpSignals = asList(icpRow && icpRow.meta && icpRow.meta.signals);
  const version = Number(opts.version || workspace.version || 1);
  const aim = buildAim({
    id: `aim-${workspace.clientKey}-v${version}`,
    clientKey: workspace.clientKey,
    clientName: workspace.clientName,
    status: AIM_STATUS.PUBLISHED,
    version,
    mission: { transformation: mission },
    icp: {
      company: {
        reasoning: icpRow ? icpRow.statement : '',
        signals: icpSignals,
      },
      founder: {
        reasoning: active(concepts, CONCEPT_TYPES.LANGUAGE)
          .map((c) => c.statement)
          .find((s) => /i do everything myself|founder/i.test(s)) || '',
        signals: active(concepts, CONCEPT_TYPES.LANGUAGE)
          .map((c) => c.statement.toLowerCase())
          .filter((s) => /i do everything myself|founder|owner-operated|too many hats/i.test(s)),
      },
      size: { reasoning: '', known: false },
      geography: { reasoning: '', known: false },
      exclusions: {
        reasoning: disqualifiers.map((d) => d.label || d.statement).join('; '),
        signals: disqualifiers.map((d) => String(d.label || d.statement).toLowerCase()),
      },
    },
    transformation,
    painOntology: buildPainOntologyFromWorkspace(workspace, concepts),
    knowledge: buildKnowledge(workspace, concepts),
  });
  aim.spec = 'SPEC-113';
  aim.sourceSpec = ['SPEC-113', 'SPEC-112'];
  aim.isOperatingFact = false;
  aim.compiler = {
    workspaceId: workspace.id,
    publishedAt: nowIso(),
    approvedBy: workspace.approvedBy,
    documentIds: (workspace.documents || []).map((d) => d.id),
    documentTitles: (workspace.documents || []).map((d) => d.title),
  };
  aim.buyingSignals = active(concepts, CONCEPT_TYPES.BUYING_TRIGGER).map((c) => c.label);
  aim.disqualifiers = disqualifiers.map((c) => c.label || c.statement);
  aim.confidenceRules = active(concepts, CONCEPT_TYPES.CONFIDENCE_RULE).map((c) => c.statement);
  aim.unknowns = active(concepts, CONCEPT_TYPES.UNKNOWN)
    .concat((workspace.unknowns || []).map((u) => ({ statement: u })))
    .map((c) => c.statement || c)
    .filter(Boolean);
  if (!aim.unknowns.length) {
    const unknownRow = (workspace.concepts || []).find((c) => c.type === CONCEPT_TYPES.UNKNOWN);
    if (unknownRow) aim.unknowns.push(unknownRow.statement);
  }
  if (!transformation.currentState || !transformation.futureState) {
    aim.unknowns.push('Transformation current/future state is incomplete.');
  }
  if (!icpRow) aim.unknowns.push('ICP reasoning was not supplied in source documents.');
  aim.provenance = (workspace.concepts || [])
    .filter((c) => c.status !== CONCEPT_STATUS.REMOVED && c.status !== CONCEPT_STATUS.MERGED)
    .map((c) => ({
      conceptId: c.id,
      type: c.type,
      label: c.label,
      document: c.provenance && c.provenance.documentTitle,
      section: c.provenance && c.provenance.section,
      excerpt: c.evidenceExcerpt,
      operatorApproval: c.operatorApproval,
    }));
  return aim;
}

function publishWorkspace(workspace, opts = {}) {
  assertApproved(workspace);
  const aim = toPublishedAim(workspace, opts);
  workspace.status = WORKSPACE_STATUS.PUBLISHED;
  workspace.publishedAt = aim.compiler.publishedAt;
  workspace.aimId = aim.id;
  workspace.publishedAim = aim;
  workspace.version = aim.version;
  workspace.updatedAt = nowIso();
  if (opts.aimStore && typeof opts.aimStore.putAim === 'function') {
    const previous = opts.aimStore.getAim && opts.aimStore.getAim(workspace.clientKey);
    if (previous && previous.id !== aim.id && previous.status !== AIM_STATUS.SUPERSEDED) {
      previous.status = AIM_STATUS.SUPERSEDED;
      opts.aimStore.putAim(previous);
    }
    opts.aimStore.putAim(aim);
  }
  return { workspace, aim };
}

module.exports = {
  toPublishedAim,
  publishWorkspace,
  parseTransformation,
};
