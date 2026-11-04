'use strict';
const { JevProvider } = require('./providers/JevProvider');
const { snapshotInput } = require('./privacy');
const { evaluateCases } = require('./jevEvaluation');
const { ROUTES } = require('./schema');

function validateCases(cases) {
  if (!Array.isArray(cases) || cases.length < 1 || cases.length > 60) throw new Error('expected 1-60 cases');
  const seen = new Set();
  for (const c of cases) {
    if (!c || typeof c.id !== 'string' || !/^[a-zA-Z0-9._-]{1,100}$/.test(c.id) || seen.has(c.id)) throw new Error('invalid/duplicate id');
    seen.add(c.id);
    if (c.provenance !== 'synthetic') throw new Error('probe accepts synthetic cases only');
    if (typeof c.operator_message !== 'string' || !c.operator_message.trim() || c.operator_message.length > 1000) throw new Error('invalid operator_message');
    if (/(?:https?:\/\/|[\w.+-]+@[\w.-]+\.[a-z]{2,}|(?:sk[-_]|jv_live_|gh[pousr]_|github_pat_)[\w-]+)/i.test(c.operator_message)) throw new Error('potential sensitive content in operator_message');
    if (!ROUTES.includes(c.expected_route) || c.expected_route === 'unknown') throw new Error('invalid expected route');
    if (c.label_status !== 'unreviewed' && c.label_status !== 'human_reviewed') throw new Error('invalid label status');
    if (typeof c.category !== 'string' || !/^[a-z_]{1,48}$/.test(c.category)) throw new Error('invalid category');
    if (c.legacy_route != null && !ROUTES.includes(c.legacy_route)) throw new Error('invalid legacy route');
  }
}
async function probeJev(cases, { provider, timeoutMs = 5000 } = {}) {
  validateCases(cases);
  if (!provider || typeof provider.evaluate !== 'function') throw new Error('provider required');
  if (!Number.isInteger(timeoutMs) || timeoutMs < 100 || timeoutMs > 10000) throw new Error('invalid timeout');
  const rows = [];
  for (const entry of cases) {
    const controller = new AbortController();
    // A timeout guard cannot invoke or affect any production routing path.
    let timer;
    let decision = null, failure = null;
    try {
      // Bound elapsed time even if an injected provider ignores AbortSignal.
      const timeout = new Promise((_, reject) => {
        timer = setTimeout(() => {
          controller.abort();
          const error = new Error('provider timed out');
          error.code = 'timeout';
          reject(error);
        }, timeoutMs);
      });
      const result = await Promise.race([
        Promise.resolve().then(() => provider.evaluate(snapshotInput({ question: entry.operator_message }), { signal: controller.signal })),
        timeout,
      ]);
      decision = result.decision;
    } catch (error) {
      failure = error?.code === 'timeout' || error?.name === 'AbortError' ? 'timeout'
        : ['http_error', 'invalid_response'].includes(error?.code) ? error.code : 'provider_error';
    } finally { clearTimeout(timer); }
    rows.push({
      id: entry.id, category: entry.category, expected_route: entry.expected_route,
      label_status: entry.label_status, provenance: 'synthetic',
      legacy_route: entry.legacy_route || null, jev_decision: decision,
      probe_error: failure,
    });
  }
  const report = evaluateCases(rows);
  return { ...report, actual_provider_invoked: true, provider_requests: rows.length,
    failures: rows.filter(r => r.probe_error).map(r => ({ id: r.id, code: r.probe_error })),
    operator_messages_in_output: false };
}
async function main(argv = process.argv.slice(2), env = process.env) {
  if (argv.length !== 1 || !argv[0].endsWith('.json')) throw new Error('Usage: node packages/decision-service/jevProbe.js <synthetic-cases.json>');
  if (!env.JEV_API_KEY) throw new Error('JEV_API_KEY missing; no network calls made');
  const cases = JSON.parse(require('node:fs').readFileSync(argv[0], 'utf8'));
  validateCases(cases);
  const provider = new JevProvider({ apiKey: env.JEV_API_KEY, model: env.JEV_MODEL || 'jev-latest' });
  return probeJev(cases, { provider });
}
if (require.main === module) {
  main().then(report => process.stdout.write(JSON.stringify(report, null, 2) + '\n'))
    .catch(error => { process.stderr.write('JEV probe failed: ' + error.message + '\n'); process.exitCode = 1; });
}
module.exports = { validateCases, probeJev, main };
