'use strict';

const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const root = path.join(__dirname, '..');

test('pricing calculator is mounted as an authenticated internal route', () => {
  const server = fs.readFileSync(path.join(root, 'server.js'), 'utf8');
  const route = fs.readFileSync(path.join(root, 'routes', 'pricingCalculator.js'), 'utf8');

  assert.match(server, /require\('\.\/routes\/pricingCalculator'\)/);
  assert.match(server, /req\.path === '\/public\/pricing-calculator\.html'/);
  assert.match(route, /router\.get\('\/pricing-calculator'/);
  assert.match(route, /requireRole\('admin', 'manager', 'sales', 'closer'\)/);
  assert.match(route, /pricing-calculator\.html/);
});

test('shared shell exposes Pricing navigation to internal operator roles', () => {
  const shell = fs.readFileSync(path.join(root, 'public', 'shared', 'shell.js'), 'utf8');

  assert.match(shell, /id: 'pricing', label: 'Pricing'/);
  assert.match(shell, /href: \{ default: '\/pricing-calculator' \}/);
  assert.match(shell, /path\.startsWith\('\/pricing-calculator'\)/);
});

test('pricing calculator page includes required quote inputs and outputs', () => {
  const html = fs.readFileSync(path.join(root, 'public', 'pricing-calculator.html'), 'utf8');
  const requiredIds = [
    'squareFeet',
    'bedrooms',
    'bathrooms',
    'furnished',
    'condition',
    'addonOven',
    'addonFridge',
    'addonCabinets',
    'addonWindows',
    'addonPetHair',
    'travelZone',
    'hoursOverride',
    'notes',
    'recommendedQuote',
    'quoteRange',
    'estimatedHours',
    'revenuePerHour',
    'adjustments',
    'warnLow',
    'moveMinimum',
  ];

  for (const id of requiredIds) {
    assert.match(html, new RegExp(`id="${id}"`), `${id} is present`);
  }

  assert.match(html, /data-value="standard"/);
  assert.match(html, /data-value="deep"/);
  assert.match(html, /data-value="move"/);
  assert.match(html, /moveMinimum.*value="325"/s);
  assert.match(html, /bathFee.*value="25"/s);
  assert.match(html, /ovenFee.*value="50"/s);
  assert.match(html, /fridgeFee.*value="50"/s);
  assert.match(html, /cabinetFee.*value="50"/s);
  assert.match(html, /revenue opportunity model/);
});
