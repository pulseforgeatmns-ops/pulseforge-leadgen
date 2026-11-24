import test from 'node:test';
import assert from 'node:assert/strict';
import { workspaceGeometry, MIN_PANEL } from './layout.mjs';
const overlap = (a,b) => a.x < b.x+b.width-.1 && a.x+a.width > b.x+.1 && a.y < b.y+b.height-.1 && a.y+a.height > b.y+.1;
test('all field edges and panel regions tile without overlap at tablet dimensions', () => {
  for (const fieldDock of ['top','bottom','left','right']) for (const docks of [['left','right'], ['top','bottom'], ['top','left','right'], ['left','left','bottom'], ['top','bottom','left','right']]) {
    const panels = docks.map((dock,i) => ({ id:String(i), dock, minimized:false, maximized:false }));
    const g=workspaceGeometry({ fieldMode:'docked',fieldDock,fieldRatio:.48,panels },1024,592);
    const boxes=[g.field,...Object.values(g.panels)];
    for (const r of boxes) { assert.ok(r.x>=0 && r.y>=0 && r.x+r.width<=1024.01 && r.y+r.height<=592.01); assert.ok(r.width>0 && r.height>0); }
    for (let i=0;i<boxes.length;i++) for (let j=i+1;j<boxes.length;j++) assert.equal(overlap(boxes[i],boxes[j]),false);
  }
});
test('floating geometry stays on screen and minimized panels reserve no space',()=>{
  const state={fieldMode:'minimized',fieldDock:'top',fieldRatio:.48,panels:[{id:'a',dock:'floating',x:9999,y:9999,width:1500,height:1200},{id:'b',dock:'left',minimized:true}]};
  const g=workspaceGeometry(state,1024,592);
  assert.deepEqual(g.panels.a,{x:0,y:0,width:1024,height:592}); assert.equal(g.panels.b,undefined);
  assert.equal(workspaceGeometry({...state,fieldMode:'expanded'},1024,592).field.height,592);
});
test('additional docked panels keep a usable minimum size and stack the overflow', () => {
  const panels = ['a', 'b', 'c', 'd', 'e'].map(id => ({ id, dock: 'left', minimized: false, maximized: false }));
  const g = workspaceGeometry({ fieldMode: 'docked', fieldDock: 'top', fieldRatio: .48, focused: 'a', panels }, 1180, 700);
  const shown = Object.entries(g.panels);
  assert.ok(shown.length >= 1 && shown.length < 5);
  for (const [, r] of shown) assert.ok(r.width >= MIN_PANEL.width && r.height >= MIN_PANEL.height);
  assert.ok(g.panels.a, 'focused panel stays visible');
  assert.deepEqual([...g.stacked, ...shown.map(([id]) => id)].sort(), ['a', 'b', 'c', 'd', 'e']);
  // Rows too short to be usable fold into columns instead of shrinking.
  const rows = workspaceGeometry({ fieldMode: 'docked', fieldDock: 'top', fieldRatio: .48, panels: ['top', 'bottom', 'left', 'right'].map((dock, i) => ({ id: String(i), dock, minimized: false, maximized: false })) }, 1440, 900);
  for (const r of Object.values(rows.panels)) assert.ok(r.width >= MIN_PANEL.width && r.height >= MIN_PANEL.height);
  // Two side-by-side panels underneath the field remain the default composition.
  const pair = workspaceGeometry({ fieldMode: 'docked', fieldDock: 'top', fieldRatio: .48, panels: [{ id: 'l', dock: 'left' }, { id: 'r', dock: 'right' }] }, 1024, 680);
  assert.equal(pair.stacked.length, 0); assert.ok(pair.panels.l.x + pair.panels.l.width <= pair.panels.r.x);
});
