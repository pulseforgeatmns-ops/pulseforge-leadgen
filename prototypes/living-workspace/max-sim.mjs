// Simulated, deterministic Max for the fixture workspace. No model, network or
// production orchestration: a small phrase interpreter turns an utterance plus
// the current workspace context into registered dispatcher commands, and the
// reply is composed afterwards from the *resulting* state, never from intent.
import { tools } from './store.mjs';

const DOCK_WORD = { left: 'on the left', right: 'on the right', top: 'across the top', bottom: 'across the bottom', floating: 'floating' };
const toolTitle = id => tools[id]?.title || id;

export function panelSteps(state, toolId, recordId, dock) {
  const target = `${toolId}:${recordId}`;
  const existing = state.panels.find(p => p.id === target);
  if (existing) return [...(existing.minimized ? [{ type: 'restore_panel', panelId: target }] : []), { type: 'focus_panel', panelId: target }];
  const candidates = state.panels.filter(p => p.toolId === toolId);
  const reuse = candidates.find(p => p.id === state.focused) || candidates.filter(p => p.dock !== 'floating').at(-1) || candidates.at(-1);
  if (reuse) return [{ type: 'retarget_panel', panelId: reuse.id, recordId }];
  return [{ type: 'open_tool', toolId, recordId }, { type: 'dock_panel', panelId: target, dock }];
}

// What Max "sees": derived from shared state only. Used for the context strip and every reply.
export function describeContext(state, fixture) {
  const record = id => fixture.records.find(r => r.id === id);
  const domain = fixture.overview.domains.find(d => d.id === state.fieldFocus) || null;
  const selected = record(state.selected) || null;
  const panels = state.panels.map(p => ({ id: p.id, toolId: p.toolId, title: toolTitle(p.toolId), dock: p.dock, minimized: p.minimized, maximized: p.maximized, pinned: p.follow === false, record: record(p.recordId) }));
  const path = [fixture.name, domain?.label, selected?.kind === 'object' ? selected.name : null].filter(Boolean);
  const visible = panels.filter(p => !p.minimized);
  const panelText = visible.length ? visible.map(p => `${p.title} (${p.dock}) — ${p.record.name}`).join('; ') : 'no panels open';
  const field = state.fieldMode === 'docked' ? `docked ${state.fieldDock} at ${Math.round(state.fieldRatio * 100)}%` : state.fieldMode;
  return { tenant: fixture.name, domain, selected, panels, visible, path, field,
    summary: `${path.join(' › ')} · ${panelText} · field ${field}` };
}

// Match a business object in the *current* tenant only: full name, or its distinctive first word.
function findObject(text, fixture) {
  const words = new Set(text.split(/[^a-z0-9-]+/));
  return fixture.records.find(r => {
    if (r.kind !== 'object') return false;
    const name = r.name.toLowerCase(), first = name.split(/\s+/)[0];
    return text.includes(name) || (first.length >= 4 && words.has(first));
  });
}
const findDomain = (text, fixture) => fixture.overview.domains.find(d => new RegExp(`\\b${d.id}\\b`).test(text));
const domainRecord = (fixture, id) => fixture.records.find(r => r.kind === 'domain' && r.domain === id);

function label(step, fixture) {
  const rec = id => fixture.records.find(r => r.id === id)?.name || id;
  const panel = id => { const [tool, recordId] = String(id).split(':'); return `${toolTitle(tool)} (${rec(recordId)})`; };
  switch (step.type) {
    case 'focus_domain': return step.domainId ? `focus_domain → ${step.domainId}` : 'focus_domain → overview';
    case 'select_entity': return `select_entity → ${rec(step.recordId)}`;
    case 'open_tool': return `open_tool → ${toolTitle(step.toolId)} for ${rec(step.recordId)}`;
    case 'retarget_panel': return `retarget_panel → ${panel(step.panelId)} now shows ${rec(step.recordId)}`;
    case 'dock_panel': return `dock_panel → ${panel(step.panelId)} ${step.dock}`;
    case 'set_field_mode': return `set_field_mode → ${step.mode}`;
    case 'set_field_dock': return `set_field_dock → ${step.dock} ${Math.round(step.ratio * 100)}%`;
    case 'checkpoint_layout': return 'checkpoint_layout → remembered your current layout';
    case 'restore_checkpoint': return 'restore_checkpoint → applied the remembered layout';
    case 'arrange_layout': return `arrange_layout → ${step.preset}`;
    default: return `${step.type}${step.panelId ? ` → ${panel(step.panelId)}` : ''}${step.name ? ` → ${step.name}` : ''}`;
  }
}

const failed = 'I couldn’t do that: the workspace rejected the change, so nothing moved.';
function panelFor(state, toolId, recordId) { return state.panels.find(p => p.toolId === toolId && p.recordId === recordId); }
function where(p) { return p.minimized ? 'minimized in the dock' : p.maximized ? 'filling the workspace' : DOCK_WORD[p.dock]; }

/**
 * Plan a Max turn. Returns { kind, steps, report(after, ok) → { text, receipt } }.
 * `steps` are registered dispatcher commands without scope/source; the caller
 * dispatches them as one batch from Max. `env.maxFieldRatio` caps field growth
 * so the panels underneath keep their usable minimum height.
 */
export function planMaxTurn(utterance, state, fixture, env = {}) {
  const text = String(utterance).toLowerCase().replace(/^\s*(hey\s+)?max[,:]?\s*/, '').replace(/[.!?]+$/, '').trim();
  const ctx = describeContext(state, fixture);
  const sel = ctx.selected;
  const plan = (kind, steps, verify) => ({ kind, steps, report(after, ok) {
    const receipt = ok ? steps.map(s => label(s, fixture)) : [];
    if (steps.length && !ok) return { text: after.layoutLocked
      ? 'Your layout is locked, so I left it unchanged. You can keep the lock, create a temporary copy, or explicitly unlock this tenant composition.'
      : failed, receipt };
    return { text: verify(after, describeContext(after, fixture)), receipt };
  } });
  const say = (kind, message) => plan(kind, [], () => message);
  const layoutChange = steps => [{ type: 'checkpoint_layout' }, ...steps];

  if (!text) return say('empty', 'Ask me to show a business, open the investigation, or rearrange the workspace.');

  if (/\b(what|where)\b.*\b(looking at|am i|see|context|selected|in view)\b|^(status|context)$/.test(text)) {
    return say('context', `You’re in ${ctx.path.join(' › ')}. ${sel ? `Selected: ${sel.name}${sel.kind === 'object' ? ` (${sel.type}, ${sel.stage})` : ''}.` : 'Nothing is selected.'} Open: ${ctx.visible.length ? ctx.visible.map(p => `${p.title} ${DOCK_WORD[p.dock]} showing ${p.record.name}`).join('; ') : 'no panels'}. The spatial field is ${ctx.field}.`);
  }

  if (/\b(outstanding|open questions?|unanswered|still (open|unknown|missing))\b/.test(text)) {
    if (!sel) return say('questions', 'Select a business first and I’ll list its open questions.');
    const own = sel.kind === 'object' ? sel.questions.map(q => q) : fixture.records.filter(r => r.kind === 'object' && r.domain === sel.domain).flatMap(o => o.questions.map(q => `${o.name}: ${q}`));
    return say('questions', own.length ? `Open questions on ${sel.name}: ${own.map((q, i) => `(${i + 1}) ${q}`).join(' ')} These come from fixture research notes; nothing has been asked of anyone.` : `There are no open questions recorded for ${sel.name}.`);
  }
  if (/\b(restore|revert|bring back|go back to|return to)\b.*\b(previous|last|earlier|prior|old|original)\b.*\blayout\b|\bundo your (layout|change)/.test(text)) {
    if (!state.layoutCheckpoint) return say('restore', `There’s no earlier layout of yours waiting to be restored: I keep the one from before my most recent layout change, and it has already been restored or I haven’t changed anything.${state.canUndo ? ' Undo steps back through layout changes one at a time.' : ''}`);
    const before = describeContext(state, fixture);
    return plan('restore', [{ type: 'restore_checkpoint' }], (after, a) => {
      const notes = [];
      if (after.fieldRatio !== state.fieldRatio || after.fieldMode !== state.fieldMode) notes.push(`the field is ${a.field} again (was ${before.field})`);
      const moved = a.visible.filter(p => { const prior = before.panels.find(q => q.id === p.id); return prior && prior.dock !== p.dock; });
      if (moved.length) notes.push(moved.map(p => `${p.title} is ${DOCK_WORD[p.dock]}`).join(', '));
      return `Restored the layout you had before my last change${notes.length ? `: ${notes.join('; ')}` : ' (it already matched)'}. Selection is unchanged${a.selected ? `: ${a.selected.name}` : ''}. Changes you made after mine were replaced too; Undo brings them back.`;
    });
  }
  if (/^(undo|redo)$/.test(text)) return { kind: text, steps: [], undo: `${text}_layout`, report: (after, ok) => ({ text: ok ? `Layout ${text} complete. Your selection is unchanged.` : `There’s no layout change to ${text}.`, receipt: ok ? [`${text}_layout`] : [] }) };
  const saveLoad = text.match(/^(save|load) layout (.+)$/);
  if (saveLoad) {
    const name = utterance.trim().replace(/[.!?]+$/, '').split(/\s+/).slice(2).join(' ');
    return plan(saveLoad[1], [{ type: `${saveLoad[1]}_layout`, name }], () => saveLoad[1] === 'save' ? `Saved this layout as “${name}” for ${ctx.tenant}.` : `Restored “${name}”. Your records and selection are unchanged.`);
  }

  if (/\b(field|map|orbit|orbital|spatial|solar)\b/.test(text)) {
    const mode = /\b(expand|maximi[sz]e|enlarge|full ?screen|fill)\b/.test(text) ? 'expanded' : /\b(minimi[sz]e|collapse|hide|shrink away)\b/.test(text) ? 'minimized' : /\b(dock|restore|normal|reset)\b/.test(text) ? 'docked' : null;
    if (mode) return plan('field', layoutChange([{ type: 'set_field_mode', mode }]), after => after.fieldMode === mode
      ? `The spatial field is now ${mode}.${mode !== 'docked' ? ' Your panels are kept in the dock with their records.' : ''}`
      : `I asked for a ${mode} field, but it is ${after.fieldMode}.`);
    const grow = /\b(bigger|taller|larger|more (room|space)|grow)\b/.test(text) ? 1 : /\b(smaller|shorter|less (room|space))\b/.test(text) ? -1 : 0;
    if (grow) {
      const ratio = Math.max(0.3, Math.min(env.maxFieldRatio ?? 0.65, 0.65, state.fieldRatio + grow * 0.1));
      if (Math.abs(ratio - state.fieldRatio) < 0.005) return say('field', `The field is already as ${grow > 0 ? 'large as it can be while keeping the panels usable' : 'small as it goes'} (${Math.round(state.fieldRatio * 100)}%).`);
      return plan('field', layoutChange([{ type: 'set_field_dock', dock: state.fieldDock, ratio }]), after => `The field now takes ${Math.round(after.fieldRatio * 100)}% (was ${Math.round(state.fieldRatio * 100)}%).`);
    }
  }

  const evidence = /\b(evidence|investigation)\b/.test(text);
  const arrangement = /\b(beside|next to|alongside|side by side|by the side|right of|left of)\b/.test(text) ? 'side' : /\b(below|under|beneath|underneath)\b/.test(text) ? 'below' : /\b(above|over)\b/.test(text) ? 'above' : null;
  if (evidence && arrangement && /\b(details|prospect|profile|record)\b/.test(text)) {
    if (!sel) return say('arrange', 'Select a business first. Then I can put its evidence beside its details.');
    const [detailsDock, evidenceDock] = { side: ['left', 'right'], below: ['top', 'bottom'], above: ['bottom', 'top'] }[arrangement];
    // Stacking needs room for two usable rows: make the field smaller if that is enough.
    const fit = arrangement !== 'side' && state.fieldMode === 'docked' && ['top', 'bottom'].includes(state.fieldDock) && env.fitRatioForRows ? env.fitRatioForRows(2) : null;
    const resize = fit !== null && fit >= 0.3 && fit < state.fieldRatio ? [{ type: 'set_field_dock', dock: state.fieldDock, ratio: Math.round(fit * 1000) / 1000 }] : [];
    const steps = layoutChange([...resize,
      ...panelSteps(state, 'domain', sel.id, detailsDock), ...panelSteps(state, 'investigation', sel.id, evidenceDock),
      { type: 'dock_panel', panelId: `domain:${sel.id}`, dock: detailsDock }, { type: 'dock_panel', panelId: `investigation:${sel.id}`, dock: evidenceDock },
      ...(state.fieldMode === 'docked' ? [] : [{ type: 'set_field_mode', mode: 'docked' }]),
    ]);
    return plan('arrange', steps, after => {
      const d = panelFor(after, 'domain', sel.id), e = panelFor(after, 'investigation', sel.id);
      if (!d || !e || d.dock !== detailsDock || e.dock !== evidenceDock) return 'I tried to arrange the panels, but they didn’t end up where I intended. Check the layout, or use Undo.';
      // Report what is on screen, not just the dock setting: minimum sizes can reflow panels.
      const seen = env.relation ? env.relation(after, d.id, e.id) : null;
      const wanted = { side: 'beside', below: 'below', above: 'above' }[arrangement];
      const resized = resize.length ? ` I made the field smaller (${Math.round(after.fieldRatio * 100)}%) to make room.` : '';
      if (seen && seen !== wanted) {
        const actual = seen === 'beside' ? 'side by side' : seen === 'stacked' ? 'with the evidence stacked in the dock' : seen;
        return `I docked Details ${DOCK_WORD[d.dock]} and the evidence ${DOCK_WORD[e.dock]}, but there isn’t room to show both ${wanted} at a usable size here, so they’re ${actual} for now. They’ll ${wanted === 'beside' ? 'sit side by side' : 'stack'} when there’s space. Both show ${sel.name}.`;
      }
      return `Done. Details is ${DOCK_WORD[d.dock]} and the evidence is ${DOCK_WORD[e.dock]}, both for ${sel.name}.${resized} Say “restore my previous layout” to go back.`;
    });
  }
  if (evidence && /\b(close|hide|dismiss)\b/.test(text)) {
    const open = state.panels.filter(p => p.toolId === 'investigation');
    if (!open.length) return say('close', 'No investigation is open.');
    return plan('close', layoutChange(open.map(p => ({ type: 'close_panel', panelId: p.id }))), after => after.panels.some(p => p.toolId === 'investigation') ? 'The investigation is still open; the close didn’t apply.' : `Closed the investigation. ${after.selected ? `${ctx.selected.name} is still selected.` : ''}`.trim());
  }
  if (evidence && /\b(open|show|see|bring up|pull up)\b/.test(text) && !findObject(text, fixture)) {
    if (!sel) return say('investigate', 'Nothing is selected yet. Tell me which business to show, for example “show me Northgate Bakery”.');
    const wasOpen = panelFor(state, 'investigation', sel.id);
    return plan('investigate', [...(wasOpen ? [] : [{ type: 'checkpoint_layout' }]), ...panelSteps(state, 'investigation', sel.id, 'right'), ...(state.fieldMode === 'expanded' ? [{ type: 'set_field_mode', mode: 'docked' }] : [])], after => {
      const p = panelFor(after, 'investigation', sel.id);
      if (!p) return `I couldn’t confirm an investigation panel for ${sel.name}.`;
      return `${wasOpen ? 'The investigation was already open; I brought it forward.' : 'Opened the investigation.'} It shows ${sel.name}’s evidence, ${where(p)}.`;
    });
  }

  if (/\b(overview|zoom out|all domains|whole system)\b|\bback to (the )?(sun|start|top)\b/.test(text)) {
    return plan('overview', [{ type: 'focus_domain', domainId: null }], after => after.fieldFocus === null
      ? `Back at the ${ctx.tenant} overview: four domains.${after.selected ? ` ${describeContext(after, fixture).selected.name} stays selected.` : ''}` : 'I couldn’t return to the overview.');
  }

  const object = findObject(text, fixture);
  if (object) {
    const steps = [{ type: 'focus_domain', domainId: object.domain }, { type: 'select_entity', recordId: object.id },
      ...panelSteps(state, 'domain', object.id, 'left'), ...panelSteps(state, 'investigation', object.id, 'right'),
      ...(state.fieldMode === 'minimized' ? [{ type: 'set_field_mode', mode: 'docked' }] : [])];
    return plan('select', steps, (after, a) => {
      const d = panelFor(after, 'domain', object.id), e = panelFor(after, 'investigation', object.id);
      if (after.selected !== object.id || after.fieldFocus !== object.domain) return `I tried to select ${object.name}, but the selection is ${a.selected?.name || 'empty'}.`;
      const domain = fixture.overview.domains.find(dd => dd.id === object.domain);
      const placement = after.fieldMode === 'expanded' ? '; the field is expanded, so Details and Investigation are ready in the dock (say “dock the field” to show them)' : d && e ? `; Details is ${where(d)} and the evidence is ${where(e)}` : '';
      const qs = object.questions.length ? ` ${object.questions.length} open question${object.questions.length > 1 ? 's' : ''} recorded.` : '';
      return `Here’s ${object.name}, a ${object.type.toLowerCase()} in ${domain.label} (${object.stage}). It’s highlighted in the field${placement}.${qs}`;
    });
  }

  const domain = findDomain(text, fixture);
  if (domain) {
    const rec = domainRecord(fixture, domain.id);
    const investigate = /\binvestigat/.test(text);
    const steps = [{ type: 'focus_domain', domainId: domain.id }, { type: 'select_entity', recordId: rec.id }, ...panelSteps(state, 'domain', rec.id, 'left')];
    if (investigate) steps.push({ type: 'dock_panel', panelId: `domain:${rec.id}`, dock: 'left' }, ...panelSteps(state, 'investigation', rec.id, 'right'), { type: 'dock_panel', panelId: `investigation:${rec.id}`, dock: 'right' }, { type: 'set_field_mode', mode: 'docked' });
    return plan('domain', steps, after => {
      if (after.fieldFocus !== domain.id) return `I couldn’t bring ${domain.label} into focus.`;
      const count = fixture.records.filter(r => r.kind === 'object' && r.domain === domain.id).length;
      const showing = after.panels.filter(p => p.recordId === rec.id).map(p => toolTitle(p.toolId));
      const pinned = after.panels.filter(p => p.follow === false && p.recordId !== rec.id).map(p => `${toolTitle(p.toolId)} stays pinned to ${fixture.records.find(r => r.id === p.recordId).name}`);
      return `${/\bback\b/.test(text) ? 'Back in' : 'Now in'} ${domain.label}: ${count} ${domain.objectPlural}. ${showing.length ? `${showing.join(' and ')} ${showing.length > 1 ? 'now show' : 'now shows'} the domain.` : 'No panel shows the domain yet.'}${pinned.length ? ` ${pinned.join('; ')}.` : ''}`;
    });
  }

  if (/\barrange\b/.test(text)) {
    const preset = /spatial|default|top/.test(text) ? 'spatial-first' : /review/.test(text) ? 'review' : 'research';
    return plan('preset', layoutChange([{ type: 'arrange_layout', preset }]), after => `Applied the ${preset} layout (field ${describeContext(after, fixture).field}). Your selection is unchanged; “restore my previous layout” undoes it.`);
  }
  if (/^minimi[sz]e (all )?panels$/.test(text)) {
    return plan('minimize', layoutChange(state.panels.filter(p => !p.minimized).map(p => ({ type: 'minimize_panel', panelId: p.id }))), after => `Minimized ${after.panels.filter(p => p.minimized).length} panel(s). Restore them from the workspace dock.`);
  }
  if (/\bclose (all )?panels\b/.test(text)) {
    if (!state.panels.length) return say('close', 'There are no panels open.');
    return plan('close', layoutChange(state.panels.map(p => ({ type: 'close_panel', panelId: p.id }))), after => `Closed ${state.panels.length - after.panels.length} panel(s). Your selection is still available.`);
  }
  return say('unknown', `I didn’t find that in ${ctx.tenant}. Right now: ${ctx.summary}. Try “show me ${fixture.records.find(r => r.kind === 'object' && r.domain === 'acquisition')?.name || 'a prospect'}”, “open the investigation”, or “expand the spatial field”.`);
}

// Context-aware suggestions for the conversation (touch-friendly shortcuts that submit the same text).
export function suggestions(state, fixture) {
  const ctx = describeContext(state, fixture);
  const first = fixture.records.find(r => r.kind === 'object' && r.domain === 'acquisition');
  if (ctx.selected?.kind === 'object') {
    const out = [];
    if (!panelFor(state, 'investigation', ctx.selected.id)) out.push('Open the investigation');
    out.push('Put the evidence beside the prospect details');
    if (state.layoutCheckpoint) out.push('Restore my previous layout');
    out.push(`Take me back to ${fixture.overview.domains.find(d => d.id === ctx.selected.domain).label}`);
    return out.slice(0, 3);
  }
  return [first ? `Show me ${first.name}` : 'What am I looking at?', state.fieldMode === 'expanded' ? 'Dock the spatial field' : 'Expand the spatial field', 'What am I looking at?'];
}
