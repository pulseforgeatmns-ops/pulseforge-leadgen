// Pure responsive geometry. Dock names are real regions of the working surface.
// Docked panels never shrink below a usable minimum: when another panel would
// push a region under it, regions reflow (rows fold into columns, columns
// merge), and panels that still do not fit are stacked into the dock
// (`stacked`) — the focused panel, then the most recently opened, stay visible.
export const MIN_PANEL = Object.freeze({ width: 300, height: 220 });
export function workspaceGeometry(state, width, height) {
  const gap = 8;
  let field = { x: 0, y: 0, width, height }, work = { ...field };
  if (state.fieldMode === 'expanded') return { field, work, panels: {}, stacked: [] };
  if (state.fieldMode === 'minimized') {
    field.height = 34; work.y = 42; work.height = Math.max(0, height - 42);
  } else {
    const horizontal = ['left', 'right'].includes(state.fieldDock);
    const size = (horizontal ? width : height) * state.fieldRatio;
    if (horizontal) {
      field.width = size; work.width = width - size - gap;
      if (state.fieldDock === 'left') work.x = size + gap; else field.x = work.width + gap;
    } else {
      field.height = size; work.height = height - size - gap;
      if (state.fieldDock === 'top') work.y = size + gap; else field.y = work.height + gap;
    }
  }
  const panels = {}, stacked = [];
  const live = state.panels.filter(p => !p.minimized && !p.maximized && p.dock !== 'floating');
  // Visibility priority: focused first, then most recently opened.
  const rank = p => (p.id === state.focused ? -1 : live.length - live.indexOf(p));
  const groups = Object.fromEntries(['top', 'bottom', 'left', 'right'].map(d => [d, live.filter(p => p.dock === d)]));
  const sideCount = () => groups.left.length + groups.right.length;
  const rowsUsed = () => ['top', 'bottom'].filter(d => groups[d].length);
  const rowHeight = () => {
    const rows = rowsUsed().length, bands = rows + (sideCount() ? 1 : 0);
    return bands ? (work.height - gap * (bands - 1)) / bands : work.height;
  };
  // Rows that would be shorter than the minimum fold into the side columns.
  while (rowsUsed().length && rowHeight() < MIN_PANEL.height && (rowsUsed().length > 1 || sideCount())) {
    const d = groups.bottom.length ? 'bottom' : 'top';
    groups[d === 'bottom' ? 'right' : 'left'].push(...groups[d]); groups[d] = [];
  }
  // Two columns narrower than the minimum merge into one.
  if (groups.left.length && groups.right.length && (work.width - gap) / 2 < MIN_PANEL.width) { groups.left.push(...groups.right); groups.right = []; }
  function fit(group, capacity) {
    if (group.length <= capacity) return group;
    const keep = new Set([...group].sort((a, b) => rank(a) - rank(b)).slice(0, Math.max(1, capacity)));
    group.filter(p => !keep.has(p)).forEach(p => stacked.push(p.id));
    return group.filter(p => keep.has(p));
  }
  function tile(group, rect, axis) {
    const along = axis === 'x' ? 'width' : 'height';
    const min = axis === 'x' ? MIN_PANEL.width : MIN_PANEL.height;
    const shown = fit(group, Math.floor((rect[along] + gap) / (min + gap)));
    const size = Math.max(0, (rect[along] - gap * (shown.length - 1)) / shown.length);
    shown.forEach((p, i) => { panels[p.id] = { ...rect, [axis]: rect[axis] + i * (size + gap), [along]: size }; });
  }
  const remaining = { ...work }, rh = rowHeight();
  for (const d of ['top', 'bottom']) if (groups[d].length) {
    const y = d === 'top' ? remaining.y : remaining.y + remaining.height - rh;
    tile(groups[d], { x: work.x, y, width: work.width, height: rh }, 'x');
    remaining.height -= rh + gap;
    if (d === 'top') remaining.y += rh + gap;
  }
  const both = groups.left.length && groups.right.length;
  const sideWidth = both ? (remaining.width - gap) / 2 : remaining.width;
  tile(groups.left, { ...remaining, width: sideWidth }, 'y');
  tile(groups.right, { ...remaining, x: remaining.x + (both ? sideWidth + gap : 0), width: sideWidth }, 'y');
  for (const p of state.panels) {
    if (p.maximized) panels[p.id] = { x: 0, y: 0, width, height };
    else if (p.dock === 'floating') {
      const w = Math.min(p.width, width), h = Math.min(p.height, height);
      panels[p.id] = { x: Math.max(0, Math.min(p.x, width - w)), y: Math.max(0, Math.min(p.y, height - h)), width: w, height: h };
    }
  }
  return { field, work, panels, stacked };
}
