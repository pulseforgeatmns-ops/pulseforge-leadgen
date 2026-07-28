'use strict';

/**
 * Execution Graph — objective-driven plan construction (SPEC-041 / ADR-027).
 * Planner composes stages; never executes capabilities.
 */

const {
  PLANNER_VERSION,
  getStage,
  listStages,
  seedStagesForType,
  matchOutcomeStages,
  COMPOSITION_EDGES,
  stageLabel,
} = require('./StageLibrary');

/**
 * Build an execution graph from mission inputs.
 * Stage keywords augment the seed — they never replace it.
 *
 * @param {object} missionOrInput
 * @param {string} [missionOrInput.objective]
 * @param {string} [missionOrInput.objectiveText]
 * @param {string} [missionOrInput.type]
 * @param {string} [missionOrInput.missionType]
 * @param {object} [missionOrInput.constraints]
 * @param {object} [missionOrInput.plan] - existing plan (for replan)
 * @param {string[]} [missionOrInput.extraStages]
 * @param {string[]} [missionOrInput.removeStages]
 * @returns {object} execution graph
 */
function createExecutionGraph(missionOrInput = {}) {
  const objective = String(
    missionOrInput.objective ||
      missionOrInput.objectiveText ||
      ''
  ).trim();
  const missionType =
    missionOrInput.missionType ||
    missionOrInput.type ||
    null;

  /** @type {Map<string, string>} stageId → selection reason */
  const selected = new Map();
  /** @type {Map<string, string>} stageId → skip reason */
  const skipped = new Map();

  // 1. Seed from mission type (baseline pipeline)
  const seeds = missionType ? seedStagesForType(missionType) : [];
  for (const id of seeds) {
    selected.set(id, `Seeded from mission type ${missionType}`);
  }

  // 2. Augment from objective outcome keywords (compose — never replace)
  const keywordHits = matchOutcomeStages(objective);
  for (const hit of keywordHits) {
    if (!selected.has(hit.stageId)) {
      selected.set(hit.stageId, hit.reason);
    } else {
      // Strengthen reason when keyword also matches a seeded stage
      const prior = selected.get(hit.stageId);
      if (prior && !prior.includes('keyword')) {
        selected.set(
          hit.stageId,
          `${prior}; also matched objective keyword`
        );
      }
    }
  }

  // 3. Explicit extras (operator / feature)
  for (const id of missionOrInput.extraStages || []) {
    if (getStage(id) && !selected.has(id)) {
      selected.set(id, 'Explicitly inserted by operator / feature flag');
    }
  }

  // 4. Explicit removals
  for (const id of missionOrInput.removeStages || []) {
    if (selected.has(id)) {
      selected.delete(id);
      skipped.set(id, 'Explicitly removed by operator');
    }
  }

  // 5. Auto-insert review gates when downstream stages need them
  if (
    selected.has('ready_to_print') &&
    !selected.has('campaign_review')
  ) {
    selected.set(
      'campaign_review',
      'Review gate required for Ready To Print'
    );
  }
  if (
    selected.has('direct_mail_execution') &&
    !selected.has('campaign_review') &&
    selected.has('campaign_builder')
  ) {
    selected.set(
      'campaign_review',
      'Review gate required before Direct Mail Execution'
    );
  }

  // 6. Close transitive dependencies for selected stages
  closeDependencies(selected);

  // 7. Record library stages not selected (for explainPlan)
  for (const stage of listStages()) {
    if (!selected.has(stage.id) && !skipped.has(stage.id)) {
      skipped.set(
        stage.id,
        `Not required for objective${missionType ? ` / type ${missionType}` : ''}`
      );
    }
  }

  const nodes = [...selected.keys()].map((id) => {
    const def = getStage(id);
    return {
      id,
      name: def.name,
      capabilityId: def.capabilityId,
      consumes: [...def.consumes],
      produces: [...def.produces],
      dependencies: resolveRuntimeDeps(id, selected),
      reviewRequired: def.reviewRequired,
      priority: def.priority,
      reason: selected.get(id),
    };
  });

  const edges = buildEdges(nodes);
  const ordered = topologicalSort(nodes, edges);
  const validation = validateGraph({ nodes, edges, ordered });

  const executable = ordered.filter((n) => n.capabilityId);
  // Dedupe capability ids while preserving order (ready_to_print has no cap)
  const seenCap = new Set();
  const uniqueExecutable = [];
  for (const n of executable) {
    if (seenCap.has(n.capabilityId)) continue;
    seenCap.add(n.capabilityId);
    uniqueExecutable.push(n);
  }

  const reasoning = buildReasoning({
    objective,
    missionType,
    selected,
    skipped,
    keywordHits,
    seeds,
    ordered,
    validation,
  });

  return {
    plannerVersion: PLANNER_VERSION,
    objective,
    missionType,
    nodes,
    edges,
    orderedStageIds: ordered.map((n) => n.id),
    selectedStages: [...selected.keys()],
    skippedStages: Object.fromEntries(skipped),
    selectionReasons: Object.fromEntries(selected),
    reviewGates: ordered
      .filter((n) => n.reviewRequired)
      .map((n) => n.id),
    executableStages: uniqueExecutable.map((n) => ({
      stageId: n.id,
      capabilityId: n.capabilityId,
      name: n.name,
      reason: n.reason,
    })),
    validation,
    reasoning,
    produceReadyToPrint: selected.has('ready_to_print'),
    createdAt: new Date().toISOString(),
  };
}

/**
 * Replan from an existing mission + modifications.
 * Preserves completed valid stages; recomputes affected downstream segments.
 *
 * @param {object} mission
 * @param {object} [mods]
 * @param {object} [mods.constraints]
 * @param {string} [mods.objective]
 * @param {string[]} [mods.insertStages]
 * @param {string[]} [mods.removeStages]
 * @param {string} [mods.replaceFrom]
 * @param {string} [mods.replaceTo]
 * @returns {object} { graph, steps, preservedStageIds, invalidatedStageIds }
 */
function replanGraph(mission, mods = {}) {
  const objective =
    mods.objective ||
    mission.objectiveText ||
    mission.objective ||
    '';
  const extraStages = [...(mods.insertStages || [])];
  const removeStages = [...(mods.removeStages || [])];

  if (mods.replaceFrom && mods.replaceTo) {
    removeStages.push(mods.replaceFrom);
    extraStages.push(mods.replaceTo);
  }

  // Carry forward stages already on the plan (so replan doesn't drop them)
  const existingIds = (
    (mission.plan &&
      mission.plan.executionGraph &&
      mission.plan.executionGraph.selectedStages) ||
    (mission.plan && mission.plan.steps
      ? mission.plan.steps.map((s) => s.stageId || s.capabilityId)
      : [])
  ).filter(Boolean);

  for (const id of existingIds) {
    if (getStage(id) && !extraStages.includes(id) && !removeStages.includes(id)) {
      extraStages.push(id);
    }
  }

  const graph = createExecutionGraph({
    objective,
    missionType: mission.type || mission.missionType,
    constraints: { ...(mission.constraints || {}), ...(mods.constraints || {}) },
    extraStages,
    removeStages,
  });

  if (mods.replaceFrom && mods.replaceTo) {
    // ensure replace semantics even if replaceFrom wasn't in seed
    insertStage(graph, mods.replaceTo, {
      after: null,
      reason: `Replaced ${mods.replaceFrom}`,
    });
  }

  const priorSteps = (mission.plan && mission.plan.steps) || [];
  const completedValid = new Set(
    priorSteps
      .filter((s) => s.status === 'completed')
      .map((s) => s.stageId || s.capabilityId)
  );

  const preservedStageIds = [];
  const invalidatedStageIds = [];
  for (const stageId of graph.orderedStageIds) {
    const node = graph.nodes.find((n) => n.id === stageId);
    const capId = node && node.capabilityId;
    const key = stageId;
    const wasCompleted =
      completedValid.has(key) || (capId && completedValid.has(capId));
    if (wasCompleted && !removeStages.includes(stageId)) {
      preservedStageIds.push(stageId);
    } else if (graph.selectedStages.includes(stageId)) {
      // Downstream of any non-preserved ancestor is invalidated
      const deps = node ? node.dependencies : [];
      const depInvalid = deps.some(
        (d) =>
          invalidatedStageIds.includes(d) || !preservedStageIds.includes(d)
      );
      if (wasCompleted && depInvalid) {
        invalidatedStageIds.push(stageId);
      } else if (!wasCompleted) {
        invalidatedStageIds.push(stageId);
      }
    }
  }

  return {
    graph,
    preservedStageIds,
    invalidatedStageIds,
    produceReadyToPrint: graph.produceReadyToPrint,
  };
}

/**
 * Validate dependency graph: cycles, missing stages, duplicates, required artifacts.
 * @param {object} graph
 * @returns {{ ok: boolean, errors: string[], warnings: string[] }}
 */
function validateGraph(graph) {
  const errors = [];
  const warnings = [];
  const nodes = graph.nodes || [];
  const edges = graph.edges || [];
  const ids = nodes.map((n) => n.id);

  // Duplicates
  const seen = new Set();
  for (const id of ids) {
    if (seen.has(id)) errors.push(`Duplicate stage: ${id}`);
    seen.add(id);
  }

  // Missing dependency references
  const idSet = new Set(ids);
  for (const n of nodes) {
    for (const dep of n.dependencies || []) {
      if (!idSet.has(dep)) {
        errors.push(
          `Stage ${n.id} depends on missing stage ${dep}`
        );
      }
      if (!getStage(dep)) {
        errors.push(`Unknown dependency stage: ${dep}`);
      }
    }
    if (!getStage(n.id)) {
      errors.push(`Unknown stage in graph: ${n.id}`);
    }
  }

  // Cycles
  const cycle = detectCycle(nodes, edges);
  if (cycle) {
    errors.push(`Dependency cycle detected: ${cycle.join(' → ')}`);
  }

  // Empty graph
  if (!nodes.length) {
    errors.push('Execution graph has no stages');
  }

  // Executable coverage — at least one capability unless only gates (should not happen)
  const executable = nodes.filter((n) => n.capabilityId);
  if (nodes.length && !executable.length) {
    errors.push('Execution graph has no executable capabilities');
  }

  // Soft: ready_to_print without review
  if (idSet.has('ready_to_print') && !idSet.has('campaign_review')) {
    errors.push('Ready To Print requires Campaign Review gate');
  }

  return {
    ok: errors.length === 0,
    errors,
    warnings,
  };
}

/**
 * Human-readable explanation of why stages were selected / skipped.
 * @param {object} graph
 * @returns {object}
 */
function explainPlan(graph) {
  const reasons = graph.selectionReasons || {};
  const skipped = graph.skippedStages || {};
  const selected = (graph.selectedStages || []).map((id) => ({
    stageId: id,
    name: stageLabel(id),
    reason: reasons[id] || 'Selected',
    reviewGate: (graph.reviewGates || []).includes(id),
  }));
  const notSelected = Object.entries(skipped).map(([id, reason]) => ({
    stageId: id,
    name: stageLabel(id),
    reason,
  }));

  return {
    plannerVersion: graph.plannerVersion || PLANNER_VERSION,
    summary:
      graph.reasoning && graph.reasoning.summary
        ? graph.reasoning.summary
        : `Selected ${selected.length} stage(s)`,
    pipeline: (graph.orderedStageIds || []).map(stageLabel).join(' → '),
    selected,
    skipped: notSelected,
    reviewGates: (graph.reviewGates || []).map((id) => ({
      stageId: id,
      name: stageLabel(id),
      reason: reasons[id] || 'Review gate',
    })),
    dependencies: (graph.edges || []).map((e) => ({
      from: stageLabel(e.from),
      to: stageLabel(e.to),
      reason: e.reason || null,
    })),
    validation: graph.validation || null,
    answers: {
      whyMailPackage: findWhy(reasons, skipped, 'mail_package_generator'),
      whyNotDirectMail: findWhy(reasons, skipped, 'direct_mail_execution'),
      whyReviewRequired: findWhy(reasons, skipped, 'campaign_review'),
      whyReadyToPrint: findWhy(reasons, skipped, 'ready_to_print'),
    },
  };
}

/**
 * Insert a stage into an existing graph (mutates copy).
 * @param {object} graph
 * @param {string} stageId
 * @param {object} [opts]
 * @param {string|null} [opts.after]
 * @param {string} [opts.reason]
 * @returns {object} new graph
 */
function insertStage(graph, stageId, opts = {}) {
  const def = getStage(stageId);
  if (!def) throw new Error(`Unknown stage: ${stageId}`);

  const selected = new Set(graph.selectedStages || []);
  selected.add(stageId);
  const reasons = { ...(graph.selectionReasons || {}) };
  reasons[stageId] =
    opts.reason || `Inserted into plan${opts.after ? ` after ${opts.after}` : ''}`;

  return createExecutionGraph({
    objective: graph.objective,
    missionType: graph.missionType,
    extraStages: [...selected],
    removeStages: [],
  });
}

/**
 * Remove a stage and recompute.
 * @param {object} graph
 * @param {string} stageId
 * @param {string} [reason]
 * @returns {object}
 */
function removeStage(graph, stageId, reason) {
  return createExecutionGraph({
    objective: graph.objective,
    missionType: graph.missionType,
    extraStages: (graph.selectedStages || []).filter((id) => id !== stageId),
    removeStages: [stageId],
  });
}

/**
 * Replace one stage with another.
 * @param {object} graph
 * @param {string} fromId
 * @param {string} toId
 * @returns {object}
 */
function replaceStage(graph, fromId, toId) {
  if (!getStage(toId)) throw new Error(`Unknown stage: ${toId}`);
  const extras = (graph.selectedStages || [])
    .filter((id) => id !== fromId)
    .concat([toId]);
  return createExecutionGraph({
    objective: graph.objective,
    missionType: graph.missionType,
    extraStages: extras,
    removeStages: [fromId],
  });
}

// ── internals ──────────────────────────────────────────────────────────

function closeDependencies(selected) {
  let changed = true;
  while (changed) {
    changed = false;
    for (const id of [...selected.keys()]) {
      const def = getStage(id);
      if (!def) continue;
      for (const dep of def.dependencies) {
        if (!selected.has(dep) && getStage(dep)) {
          // Only auto-add hard deps when the selected stage needs upstream
          // for campaign-style pipelines — skip for focused single-stage missions
          // that intentionally run alone (proposal, inbox, mail-only, review-only).
          if (shouldAutoCloseDependency(selected, id, dep)) {
            selected.set(
              dep,
              `Required dependency of ${def.name}`
            );
            changed = true;
          }
        }
      }
    }
  }
}

/**
 * Auto-close deps when building a multi-stage campaign pipeline.
 * Focused single-capability missions keep their seed only.
 */
function shouldAutoCloseDependency(selected, stageId, depId) {
  // Always close ready_to_print → campaign_review (handled earlier)
  // For mail_package with campaign_builder already selected, close campaign chain
  if (
    stageId === 'mail_package_generator' &&
    selected.has('campaign_builder')
  ) {
    return true;
  }
  if (
    stageId === 'campaign_review' &&
    (selected.has('campaign_builder') || selected.has('mail_package_generator'))
  ) {
    return true;
  }
  if (stageId === 'ready_to_print') return true;
  if (stageId === 'direct_mail_execution' && selected.has('campaign_builder')) {
    return true;
  }
  // Ranking needs discovery when both in a discovery-based pipeline
  if (
    stageId === 'opportunity_ranking' &&
    (selected.has('prospect_discovery') || selected.has('campaign_builder'))
  ) {
    return depId === 'prospect_discovery';
  }
  if (
    stageId === 'campaign_builder' &&
    selected.has('prospect_discovery')
  ) {
    return true;
  }
  if (
    stageId === 'company_enrichment' &&
    selected.has('prospect_discovery')
  ) {
    return depId === 'prospect_discovery';
  }
  return false;
}

function resolveRuntimeDeps(stageId, selected) {
  const def = getStage(stageId);
  const deps = new Set((def && def.dependencies) || []);
  for (const edge of COMPOSITION_EDGES) {
    if (edge.to === stageId && selected.has(edge.from)) {
      deps.add(edge.from);
    }
  }
  // Only keep deps that are actually selected
  return [...deps].filter((d) => selected.has(d));
}

function buildEdges(nodes) {
  /** @type {{ from: string, to: string, reason: string|null }[]} */
  const edges = [];
  const idSet = new Set(nodes.map((n) => n.id));
  for (const n of nodes) {
    for (const dep of n.dependencies || []) {
      if (idSet.has(dep)) {
        edges.push({
          from: dep,
          to: n.id,
          reason: `${stageLabel(n.id)} depends on ${stageLabel(dep)}`,
        });
      }
    }
  }
  for (const edge of COMPOSITION_EDGES) {
    if (idSet.has(edge.from) && idSet.has(edge.to)) {
      const exists = edges.some(
        (e) => e.from === edge.from && e.to === edge.to
      );
      if (!exists) {
        edges.push({ from: edge.from, to: edge.to, reason: edge.reason });
        // also reflect on node.dependencies
        const node = nodes.find((n) => n.id === edge.to);
        if (node && !node.dependencies.includes(edge.from)) {
          node.dependencies.push(edge.from);
        }
      }
    }
  }
  return edges;
}

function topologicalSort(nodes, edges) {
  const byId = new Map(nodes.map((n) => [n.id, n]));
  const indegree = new Map(nodes.map((n) => [n.id, 0]));
  const adj = new Map(nodes.map((n) => [n.id, []]));
  for (const e of edges) {
    if (!indegree.has(e.to) || !indegree.has(e.from)) continue;
    indegree.set(e.to, indegree.get(e.to) + 1);
    adj.get(e.from).push(e.to);
  }

  /** @type {string[]} */
  const queue = nodes
    .filter((n) => indegree.get(n.id) === 0)
    .sort((a, b) => a.priority - b.priority)
    .map((n) => n.id);

  /** @type {object[]} */
  const ordered = [];
  while (queue.length) {
    // stable: always pick lowest priority among indegree 0
    queue.sort((a, b) => byId.get(a).priority - byId.get(b).priority);
    const id = queue.shift();
    ordered.push(byId.get(id));
    for (const next of adj.get(id) || []) {
      indegree.set(next, indegree.get(next) - 1);
      if (indegree.get(next) === 0) queue.push(next);
    }
  }

  if (ordered.length !== nodes.length) {
    // Cycle — return original priority order as fallback; validateGraph catches it
    return [...nodes].sort((a, b) => a.priority - b.priority);
  }
  return ordered;
}

function detectCycle(nodes, edges) {
  const adj = new Map(nodes.map((n) => [n.id, []]));
  for (const e of edges) {
    if (adj.has(e.from)) adj.get(e.from).push(e.to);
  }
  const WHITE = 0;
  const GRAY = 1;
  const BLACK = 2;
  const color = new Map(nodes.map((n) => [n.id, WHITE]));
  /** @type {string[]} */
  const stack = [];

  function dfs(u) {
    color.set(u, GRAY);
    stack.push(u);
    for (const v of adj.get(u) || []) {
      if (color.get(v) === GRAY) {
        const idx = stack.indexOf(v);
        return stack.slice(idx).concat([v]);
      }
      if (color.get(v) === WHITE) {
        const c = dfs(v);
        if (c) return c;
      }
    }
    stack.pop();
    color.set(u, BLACK);
    return null;
  }

  for (const n of nodes) {
    if (color.get(n.id) === WHITE) {
      const c = dfs(n.id);
      if (c) return c;
    }
  }
  return null;
}

function buildReasoning(ctx) {
  const pipeline = ctx.ordered.map((n) => n.name).join(' → ');
  const keywordStageIds = new Set(ctx.keywordHits.map((h) => h.stageId));
  const augmented = [...keywordStageIds].filter(
    (id) => !ctx.seeds.includes(id) && ctx.selected.has(id)
  );
  return {
    summary: ctx.missionType
      ? `Composed execution graph from ${ctx.missionType} seed` +
        (augmented.length
          ? ` augmented with: ${augmented.map(stageLabel).join(', ')}`
          : '')
      : `Composed execution graph from objective keywords`,
    pipeline,
    seedStages: ctx.seeds,
    keywordAugmentations: ctx.keywordHits,
    selectedCount: ctx.selected.size,
    validationOk: ctx.validation.ok,
  };
}

function findWhy(reasons, skipped, stageId) {
  if (reasons[stageId]) {
    return { included: true, reason: reasons[stageId] };
  }
  if (skipped[stageId]) {
    return { included: false, reason: skipped[stageId] };
  }
  return { included: false, reason: 'Not evaluated' };
}

module.exports = {
  createExecutionGraph,
  replanGraph,
  validateGraph,
  explainPlan,
  insertStage,
  removeStage,
  replaceStage,
};
