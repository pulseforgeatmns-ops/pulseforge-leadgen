// Pure responsive geometry. Dock names are real regions of the working surface.
export function workspaceGeometry(state, width, height) {
  const gap = 8;
  let field = { x: 0, y: 0, width, height }, work = { ...field };
  if (state.fieldMode === 'expanded') return { field, work, panels: {} };
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
  const panels = {}, remaining = { ...work };
  const live = state.panels.filter(p => !p.minimized && !p.maximized && p.dock !== 'floating');
  const groups = Object.fromEntries(['top', 'bottom', 'left', 'right'].map(d => [d, live.filter(p => p.dock === d)]));
  function tile(group, rect) {
    group.forEach((p, i) => {
      const h = Math.max(0, (rect.height - gap * (group.length - 1)) / group.length);
      panels[p.id] = { ...rect, y: rect.y + i * (h + gap), height: h };
    });
  }
  const sideCount = groups.left.length + groups.right.length;
  const rowCount = Number(!!groups.top.length) + Number(!!groups.bottom.length);
  const rowHeight = sideCount ? (work.height - gap * rowCount) / (rowCount + 1) : (work.height - gap * Math.max(0, rowCount - 1)) / Math.max(1, rowCount);
  for (const d of ['top', 'bottom']) if (groups[d].length) {
    const y = d === 'top' ? remaining.y : remaining.y + remaining.height - rowHeight;
    tile(groups[d], { x: work.x, y, width: work.width, height: rowHeight });
    remaining.height -= rowHeight + gap;
    if (d === 'top') remaining.y += rowHeight + gap;
  }
  const both = groups.left.length && groups.right.length;
  const sideWidth = both ? (remaining.width - gap) / 2 : remaining.width;
  tile(groups.left, { ...remaining, width: sideWidth });
  tile(groups.right, { ...remaining, x: remaining.x + (both ? sideWidth + gap : 0), width: sideWidth });
  for (const p of state.panels) {
    if (p.maximized) panels[p.id] = { x: 0, y: 0, width, height };
    else if (p.dock === 'floating') {
      const w = Math.min(p.width, width), h = Math.min(p.height, height);
      panels[p.id] = { x: Math.max(0, Math.min(p.x, width - w)), y: Math.max(0, Math.min(p.y, height - h)), width: w, height: h };
    }
  }
  return { field, work, panels };
}
