'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');

const ROOT = path.join(__dirname, '..');
const SCAN_FILES = [
  'services/governedOutbound.js',
  'services/governedOutboundAdapters.js',
  'services/governedOutboundContext.js',
  'services/governedOutboundControlDispatch.js',
  'services/governedOutboundStore.js',
  'services/governedOutboundReplies.js',
  'services/governedOutboundReplenishment.js',
  'services/governedOutboundRefill.js',
  'services/governedTenantSchedule.js',
  'services/maxOutboundControlLoop.js',
];

const DANGEROUS = [
  { label: 'default tenant literal', pattern: /\|\|\s*['"]10['"]/ },
  { label: 'nullish default tenant literal', pattern: /\?\?\s*['"]10['"]/ },
  { label: 'source.tenant_id authorization', pattern: /source\.tenant_id/ },
  { label: 'source.tenantId authorization', pattern: /source\.tenantId/ },
];

function stripComments(source) {
  return source
    .replace(/\/\*[\s\S]*?\*\//g, '')
    .replace(/^\s*\/\/.*$/gm, '');
}

test('governed outbound production code avoids implicit tenant authorization patterns', () => {
  const violations = [];
  for (const rel of SCAN_FILES) {
    const abs = path.join(ROOT, rel);
    if (!fs.existsSync(abs)) continue;
    const raw = fs.readFileSync(abs, 'utf8');
    const lines = stripComments(raw).split('\n');
    lines.forEach((line, index) => {
      if (line.includes('GOVERNED_OUTBOUND_CONTRACT_EXCEPTION')) return;
      for (const rule of DANGEROUS) {
        if (rule.pattern.test(line)) {
          violations.push(`${rel}:${index + 1} ${rule.label}: ${line.trim()}`);
        }
      }
    });
  }
  assert.deepEqual(
    violations,
    [],
    `Dangerous governed-outbound tenant patterns found:\n${violations.join('\n')}`,
  );
});
