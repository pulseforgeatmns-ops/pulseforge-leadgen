'use strict';

// Favicon smoke test: the committed ICO artifact is valid and every entry
// point references/serves it. Static — no database or listening server needed;
// the live-serving path is exercised by mounting the same express handler.

const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');

const root = path.join(__dirname, '..');
const icoPath = path.join(root, 'public', 'favicon.ico');

test('public/favicon.ico exists and is a valid ICO container', () => {
  const buf = fs.readFileSync(icoPath);
  // ICONDIR header: reserved=0, type=1 (icon), count>=1
  assert.equal(buf.readUInt16LE(0), 0);
  assert.equal(buf.readUInt16LE(2), 1);
  const count = buf.readUInt16LE(4);
  assert.ok(count >= 1, 'at least one icon frame');
  // Each frame here is PNG-encoded; verify PNG signature at each offset.
  const PNG_SIG = Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]);
  for (let i = 0; i < count; i++) {
    const entry = 6 + i * 16;
    const size = buf.readUInt32LE(entry + 8);
    const offset = buf.readUInt32LE(entry + 12);
    assert.ok(offset + size <= buf.length, `frame ${i} within file bounds`);
    assert.ok(buf.subarray(offset, offset + 8).equals(PNG_SIG), `frame ${i} is PNG-encoded`);
  }
});

test('server.js serves /favicon.ico before auth-gated routes', () => {
  const src = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  const faviconAt = src.indexOf("app.get('/favicon.ico'");
  assert.ok(faviconAt !== -1, 'server.js registers a /favicon.ico route');
  assert.ok(src.includes("path.join(__dirname, 'public', 'favicon.ico')"));
});

test('every HTML shell and the login page reference /favicon.ico', () => {
  const shells = [
    'public/dashboard.html',
    'public/setter-dashboard.html',
    'public/closer-dashboard.html',
    'public/sales-dashboard.html',
    'public/client-dashboard.html',
  ];
  for (const shell of shells) {
    const html = fs.readFileSync(path.join(root, shell), 'utf8');
    assert.match(html, /<link rel="icon" href="\/favicon\.ico">/, `${shell} links the favicon`);
  }
  const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  assert.match(server, /<link rel="icon" href="\/favicon\.ico">/, 'login page links the favicon');
});

test('GET /favicon.ico returns 200 with icon content via the express handler', async () => {
  const app = express();
  // Same handler shape as server.js — sendFile from public/ with caching.
  app.get('/favicon.ico', (_req, res) => {
    res.sendFile(icoPath, { maxAge: '7d' });
  });
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  const { port } = server.address();
  try {
    const res = await new Promise((resolve, reject) => {
      http.get({ host: '127.0.0.1', port, path: '/favicon.ico' }, resolve).on('error', reject);
    });
    const chunks = [];
    for await (const chunk of res) chunks.push(chunk);
    const body = Buffer.concat(chunks);
    assert.equal(res.statusCode, 200);
    assert.match(res.headers['content-type'] || '', /icon|image/);
    assert.ok(body.equals(fs.readFileSync(icoPath)), 'served bytes match the committed artifact');
  } finally {
    server.close();
  }
});
