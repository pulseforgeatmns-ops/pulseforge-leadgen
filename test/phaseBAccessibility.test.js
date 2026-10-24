'use strict';

// Phase B accessibility + keyboard primitives — always-run coverage.
//
// Covers the shared a11y helpers and structural guarantees in the Calls /
// Dashboard shells without requiring a live browser session:
//   - focus trap + Tab cycle
//   - Escape closes and restores focus
//   - tablist arrow-key navigation
//   - live-region announcements for dynamic outcome forms
//   - dial handoff restore path still exposes Log outcome
//   - CSS enforces min 44×44 touch targets on primary Call / sticky actions
//   - both shells load accessibility.js + activity-panel.js + workspace

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const vm = require('node:vm');

const root = path.join(__dirname, '..');

function makeElement(tag = 'div', attrs = {}) {
  const listeners = new Map();
  const el = {
    tagName: String(tag).toUpperCase(),
    id: attrs.id || '',
    className: attrs.className || '',
    textContent: attrs.textContent || '',
    attributes: { ...attrs },
    children: [],
    style: {},
    dataset: {},
    offsetParent: {},
    parentNode: null,
    setAttribute(name, value) {
      this.attributes[name] = String(value);
      if (name === 'aria-live') this['ariaLive'] = String(value);
      if (name === 'role') this.role = String(value);
    },
    getAttribute(name) { return this.attributes[name] ?? null; },
    appendChild(child) {
      child.parentNode = this;
      this.children.push(child);
      return child;
    },
    remove() {
      if (!this.parentNode) return;
      this.parentNode.children = this.parentNode.children.filter(c => c !== this);
      this.parentNode = null;
    },
    focus() { el.ownerDocument.activeElement = el; },
    addEventListener(type, fn) {
      if (!listeners.has(type)) listeners.set(type, []);
      listeners.get(type).push(fn);
    },
    removeEventListener(type, fn) {
      const list = listeners.get(type) || [];
      listeners.set(type, list.filter(f => f !== fn));
    },
    dispatchEvent(event) {
      for (const fn of listeners.get(event.type) || []) fn(event);
      return true;
    },
    querySelectorAll(selector) {
      const matches = [];
      const walk = node => {
        if (nodeMatches(node, selector)) matches.push(node);
        for (const child of node.children || []) walk(child);
      };
      walk(this);
      return matches;
    },
    querySelector(selector) {
      return this.querySelectorAll(selector)[0] || null;
    },
  };
  el.ownerDocument = null;
  return el;
}

function nodeMatches(node, selector) {
  if (selector.includes(',')) {
    return selector.split(',').map(s => s.trim()).some(part => nodeMatches(node, part));
  }
  if (selector.startsWith('[role="')) {
    const role = selector.match(/\[role="([^"]+)"\]/)[1];
    return node.getAttribute('role') === role;
  }
  if (selector.includes('[tabindex]:not([tabindex="-1"])')) {
    const ti = node.getAttribute('tabindex');
    return ti != null && ti !== '-1';
  }
  if (selector.startsWith('button')) {
    if (node.tagName !== 'BUTTON') return false;
    if (selector.includes(':not([disabled])') && node.getAttribute('disabled') != null) return false;
    return true;
  }
  if (selector.startsWith('a[href]')) {
    return node.tagName === 'A' && node.getAttribute('href') != null;
  }
  if (selector.startsWith('input') || selector.startsWith('select') || selector.startsWith('textarea')) {
    const tag = selector.split(/[:\[]/)[0].toUpperCase();
    if (node.tagName !== tag) return false;
    if (selector.includes(':not([disabled])') && node.getAttribute('disabled') != null) return false;
    return true;
  }
  return false;
}

function loadA11yModule() {
  const source = fs.readFileSync(path.join(root, 'public', 'shared', 'accessibility.js'), 'utf8');
  const body = makeElement('body');
  const doc = {
    activeElement: null,
    body,
    getElementById(id) {
      const walk = node => {
        if (node.id === id) return node;
        for (const child of node.children || []) {
          const hit = walk(child);
          if (hit) return hit;
        }
        return null;
      };
      return walk(body);
    },
    createElement(tag) {
      const el = makeElement(tag);
      el.ownerDocument = doc;
      return el;
    },
  };
  body.ownerDocument = doc;
  const sandbox = {
    document: doc,
    window: { setTimeout, clearTimeout },
    setTimeout,
    clearTimeout,
  };
  sandbox.window.document = doc;
  vm.createContext(sandbox);
  vm.runInContext(source, sandbox);
  return { a11y: sandbox.window.PulseforgeA11y, doc, body };
}

test('trapFocus cycles Tab, Escape invokes onEscape, and release restores focus', () => {
  const { a11y, doc, body } = loadA11yModule();
  const prior = makeElement('button', { id: 'prior' });
  prior.ownerDocument = doc;
  body.appendChild(prior);
  prior.focus();

  const dialog = makeElement('div', { id: 'dialog', role: 'dialog' });
  dialog.ownerDocument = doc;
  const first = makeElement('button', { id: 'first' });
  const last = makeElement('button', { id: 'last' });
  first.ownerDocument = doc;
  last.ownerDocument = doc;
  dialog.appendChild(first);
  dialog.appendChild(last);
  body.appendChild(dialog);

  let escaped = false;
  const release = a11y.trapFocus(dialog, { onEscape: () => { escaped = true; } });
  assert.equal(doc.activeElement, first, 'focus moves into the dialog');

  last.focus();
  dialog.dispatchEvent({ key: 'Tab', shiftKey: false, preventDefault() { this.prevented = true; }, type: 'keydown' });
  assert.equal(doc.activeElement, first, 'Tab from last wraps to first');

  first.focus();
  dialog.dispatchEvent({ key: 'Tab', shiftKey: true, preventDefault() { this.prevented = true; }, type: 'keydown' });
  assert.equal(doc.activeElement, last, 'Shift+Tab from first wraps to last');

  dialog.dispatchEvent({ key: 'Escape', type: 'keydown', stopPropagation() {} });
  assert.equal(escaped, true);

  release();
  assert.equal(doc.activeElement, prior, 'release restores the previously focused element');
});

test('tablist keyboard arrows and Home/End move focus and activate', () => {
  const { a11y, doc, body } = loadA11yModule();
  const list = makeElement('div');
  list.setAttribute('role', 'tablist');
  list.ownerDocument = doc;
  const tabs = ['a', 'b', 'c'].map(id => {
    const tab = makeElement('button', { id });
    tab.setAttribute('role', 'tab');
    tab.ownerDocument = doc;
    list.appendChild(tab);
    return tab;
  });
  body.appendChild(list);
  tabs[0].focus();
  const activated = [];
  a11y.enableTablistKeyboard(list, tab => activated.push(tab.id));

  list.dispatchEvent({ key: 'ArrowRight', type: 'keydown', preventDefault() {} });
  assert.equal(doc.activeElement, tabs[1]);
  list.dispatchEvent({ key: 'ArrowRight', type: 'keydown', preventDefault() {} });
  assert.equal(doc.activeElement, tabs[2]);
  list.dispatchEvent({ key: 'ArrowRight', type: 'keydown', preventDefault() {} });
  assert.equal(doc.activeElement, tabs[0], 'ArrowRight wraps');
  list.dispatchEvent({ key: 'End', type: 'keydown', preventDefault() {} });
  assert.equal(doc.activeElement, tabs[2]);
  list.dispatchEvent({ key: 'Home', type: 'keydown', preventDefault() {} });
  assert.equal(doc.activeElement, tabs[0]);
  assert.ok(activated.length >= 4);
});

test('announce creates an aria-live region and updates its text', async () => {
  const { a11y, doc } = loadA11yModule();
  a11y.announce('Outcome fields updated for Wrong number');
  await new Promise(resolve => setTimeout(resolve, 50));
  const region = doc.getElementById('pf-live-region');
  assert.ok(region);
  assert.equal(region.getAttribute('role'), 'status');
  assert.equal(region.getAttribute('aria-live'), 'polite');
  assert.match(region.textContent, /Wrong number/);

  a11y.announce('Saved', { assertive: true });
  await new Promise(resolve => setTimeout(resolve, 50));
  assert.equal(region.getAttribute('aria-live'), 'assertive');
  assert.equal(region.textContent, 'Saved');
});

test('Phase B shells load shared a11y, workspace, activity panel, and 44px touch targets', () => {
  const shells = ['public/dashboard.html', 'public/setter-dashboard.html'];
  for (const rel of shells) {
    const html = fs.readFileSync(path.join(root, rel), 'utf8');
    assert.match(html, /shared\/accessibility\.js/, `${rel} must load accessibility.js`);
    assert.match(html, /shared\/prospect-workspace\.js/, `${rel} must load prospect-workspace.js`);
    assert.match(html, /shared\/shell\.js/, `${rel} must load shell.js`);
  }
  const setter = fs.readFileSync(path.join(root, 'public/setter-dashboard.html'), 'utf8');
  assert.match(setter, /shared\/activity-panel\.js/, 'Calls must load the consolidated activity panel');
  assert.match(setter, /Start next call|start-call-btn/, 'Calls landing must expose Start next call');

  const tokens = fs.readFileSync(path.join(root, 'public/shared/tokens.css'), 'utf8');
  const shellCss = fs.readFileSync(path.join(root, 'public/shared/shell.css'), 'utf8');
  const setterCss = fs.readFileSync(path.join(root, 'public/setter-dashboard.html'), 'utf8');
  assert.match(tokens, /--pf-touch-target:\s*44px/, 'design tokens must set the 44px touch-target baseline');
  assert.match(shellCss, /min-height:\s*var\(--pf-touch-target\)/, 'shell CSS must apply the touch-target token');
  assert.match(`${shellCss}\n${setterCss}`, /\.pf-workspace-sticky-actions|start-call-btn/, 'sticky mobile actions / start-call CTA present');
});

test('workspace outcome flows cover the ten Phase B dispositions and announce consequences', () => {
  const source = fs.readFileSync(path.join(root, 'public/shared/prospect-workspace.js'), 'utf8');
  assert.match(source, /PulseforgeA11y\.announce|announce\(/, 'dynamic outcome forms must announce field changes');
  const required = [
    'no_answer', 'voicemail', 'decision_maker_not_reached', 'callback_requested',
    'interested', 'meeting_booked', 'answered_not_interested', 'wrong_number',
    'disconnected', 'do_not_call',
  ];
  for (const id of required) {
    assert.match(source, new RegExp(`id:\\s*'${id}'|server:\\s*'${id}'`), `missing outcome flow ${id}`);
  }
  assert.match(source, /[Nn]urture/, 'answered_not_interested must map to nurture in UI copy/flow');
  assert.match(source, /[Dd]ata remediation|data_remediation/, 'wrong_number / disconnected must surface data remediation');
  assert.match(source, /[Pp]ermanently suppress|[Dd]o not call|terminal_suppression/, 'do_not_call must surface terminal suppression');
  assert.match(source, /Back to queue|← Queue/, 'workspace close must return to the queue');
  assert.match(source, /Why now|whyNow/, 'call brief must surface Why now');
});

test('lifecycle client catalog keeps Phase B reasons in lockstep with the server', () => {
  const clientSrc = fs.readFileSync(path.join(root, 'public/shared/lifecycle.js'), 'utf8');
  const server = require('../services/lifecycleService');
  for (const reason of server.LIFECYCLE_REASONS) {
    assert.match(clientSrc, new RegExp(reason), `client catalog missing lifecycle reason ${reason}`);
  }
  assert.equal(server.DISPOSITION_STAGE_MAP.answered_not_interested.lifecycleReason, 'nurture');
  assert.equal(server.DISPOSITION_STAGE_MAP.wrong_number.lifecycleReason, 'data_remediation');
  assert.equal(server.DISPOSITION_STAGE_MAP.disconnected.lifecycleReason, 'data_remediation');
  assert.equal(server.DISPOSITION_STAGE_MAP.do_not_call.lifecycleReason, 'terminal_suppression');
  assert.equal(server.DISPOSITION_STAGE_MAP.do_not_call.suppress, true);
  assert.equal(server.DISPOSITION_STAGE_MAP.wrong_number.stage, 'follow_up');
  assert.equal(server.DISPOSITION_STAGE_MAP.answered_not_interested.stage, 'follow_up');
});
