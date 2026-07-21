'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const test = require('node:test');
const { execFile } = require('node:child_process');
const { promisify } = require('node:util');
const {
  DEFAULT_DRAFT,
  EXPECTED_DRAFT_HASH,
  finalizeAuthorization,
  offlineObservedEvidence,
  parseArguments,
} = require('../scripts/finalizeRevenuePhase16bAuthorization');
const {
  canonicalAuthorizationHash,
  validatePhase16bAuthorization,
} = require('../utils/revenuePhase16b');

const execFileAsync = promisify(execFile);
const root = path.join(__dirname, '..');
const draftBytes = fs.readFileSync(DEFAULT_DRAFT);
const CONFIRMED = Object.freeze({
  scheduledDate: '2026-07-14',
  completionDate: '2026-07-14',
  paymentDate: '2026-07-14',
  paymentMethod: 'stripe_card',
  approvedAt: '2026-07-20T23:00:00Z',
});

let sandbox;
let attestations;
test.before(() => {
  sandbox = fs.mkdtempSync(path.join(os.tmpdir(), 'phase16b-finalizer-'));
  attestations = {
    operator: path.join(sandbox, 'operator.txt'),
    approver: path.join(sandbox, 'approver.txt'),
  };
  fs.writeFileSync(attestations.operator,
    'I, Jacob Maynard, attest that every value in this exact authorization is final, correct, and authorized.\n');
  fs.writeFileSync(attestations.approver,
    'I, Jacob Maynard, Founder, approve this exact Revenue Phase 1.6B authorization.\n');
});
test.after(() => {
  fs.rmSync(sandbox, { recursive: true, force: true });
});

function baseOptions(overrides = {}) {
  return {
    draft: DEFAULT_DRAFT,
    output: path.join(sandbox, `signed-${Math.random().toString(36).slice(2)}.json`),
    operatorAttestationFile: attestations.operator,
    approverAttestationFile: attestations.approver,
    ...CONFIRMED,
    ...overrides,
  };
}

function tamperedDraftPath(mutate, { rehash = true } = {}) {
  const draft = JSON.parse(draftBytes.toString('utf8'));
  mutate(draft);
  if (rehash) draft.authorization_hash = canonicalAuthorizationHash(draft);
  const draftPath = path.join(sandbox, `draft-${Math.random().toString(36).slice(2)}.json`);
  fs.writeFileSync(draftPath, `${JSON.stringify(draft, null, 2)}\n`);
  return draftPath;
}

test('finalizer signs a separate file, leaves the draft untouched, and validates offline', () => {
  const options = baseOptions();
  const result = finalizeAuthorization(options);

  assert.equal(result.status, 'finalized');
  assert.equal(result.database_connectivity_used, false);
  assert.equal(result.unsigned_draft_unchanged, true);
  assert.equal(result.non_executable_outside_window, true);
  assert.equal(result.unsigned_draft_hash, EXPECTED_DRAFT_HASH);
  assert.notEqual(result.final_authorization_hash, EXPECTED_DRAFT_HASH);

  assert.ok(fs.readFileSync(DEFAULT_DRAFT).equals(draftBytes),
    'the unsigned draft must remain byte-identical');

  const signed = JSON.parse(fs.readFileSync(options.output, 'utf8'));
  assert.equal(signed.authorization_hash, canonicalAuthorizationHash(signed));
  assert.equal(signed.authorization_hash, result.final_authorization_hash);
  assert.equal(signed.approved, true);
  assert.equal(signed.executable, true);
  assert.equal(signed.approved_at, CONFIRMED.approvedAt);
  const runtime = signed.canary.operator_only_runtime_values;
  assert.deepEqual(runtime.scheduled_start, {
    local_date: '2026-07-14', timezone: 'America/New_York', precision: 'day', operator_confirmed: true,
  });
  assert.equal(runtime.completion_date.local_date, '2026-07-14');
  assert.equal(runtime.payment_received_at.local_date, '2026-07-14');
  assert.equal(runtime.payment_method, 'stripe_card');
  assert.deepEqual(signed.remaining_operator_only_values, []);

  // Full validator agreement without any database: valid inside the window…
  const inWindow = new Date(Date.parse(signed.window.start) + 60000);
  assert.equal(validatePhase16bAuthorization(signed, {
    now: inWindow,
    observed: offlineObservedEvidence(signed, inWindow.toISOString()),
  }).valid, true);
  // …and non-executable before the start and after the end.
  for (const instant of [
    new Date(Date.parse(signed.window.start) - 1000),
    new Date(Date.parse(signed.window.end) + 1000),
  ]) {
    const outside = validatePhase16bAuthorization(signed, {
      now: instant,
      observed: offlineObservedEvidence(signed, instant.toISOString()),
    });
    assert.equal(outside.valid, false, `must be non-executable at ${instant.toISOString()}`);
    assert.ok(outside.failures.some(failure => /window is not active/.test(failure)));
  }
});

test('finalizer defaults approved_at to the current UTC time when not supplied', () => {
  const before = Date.now();
  const options = baseOptions({ approvedAt: undefined });
  delete options.approvedAt;
  const result = finalizeAuthorization(options);
  const after = Date.now();
  const approvedAt = Date.parse(result.approved_at);
  assert.ok(approvedAt >= before && approvedAt <= after,
    'default approved_at must be the finalization instant in UTC');
  assert.match(result.approved_at, /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/,
    'default approved_at must be a millisecond UTC ISO-8601 timestamp');
});

test('finalizer rejects every altered immutable draft value', () => {
  const cases = [
    ['authorization ID', draft => { draft.authorization_id = '00000000-0000-4000-8000-000000000000'; }, /authorization_id was altered/],
    ['correlation ID', draft => { draft.canary.operator_only_runtime_values.correlation_id = '11111111-1111-4111-8111-111111111111'; }, /correlation_id was altered/],
    ['idempotency key', draft => { draft.canary.operator_only_runtime_values.idempotency_keys.payment_succeeded = 'phase16b-altered'; }, /idempotency key was altered: payment_succeeded/],
    ['financial value', draft => { draft.canary.collected_revenue_cents = 14999; }, /financial value was altered: collected_revenue_cents/],
    ['reconciliation financial value', draft => { draft.reconciliation.expected_totals.net_collected_revenue_cents = 15001; }, /financial value was altered: net_collected_revenue_cents/],
    ['prohibition', draft => { draft.required_false.refunds_allowed = true; }, /prohibition was altered: refunds_allowed/],
    ['stop condition', draft => { draft.stop_conditions.pop(); }, /stop conditions were altered/],
    ['pre-signed input', draft => { draft.approved = true; draft.executable = true; draft.production_execution_permitted = true; }, /not the unsigned non-executable draft/],
  ];
  for (const [name, mutate, expected] of cases) {
    assert.throws(
      () => finalizeAuthorization(baseOptions({ draft: tamperedDraftPath(mutate) })),
      expected,
      `${name} alteration must be rejected`
    );
  }

  // A tampered draft that keeps a self-consistent hash is still rejected by
  // the pinned immutable draft hash even when no guarded field is touched.
  assert.throws(
    () => finalizeAuthorization(baseOptions({
      draft: tamperedDraftPath(draft => { draft.purpose = `${draft.purpose} (altered)`; }),
    })),
    /canonical hash mismatch/
  );
  assert.throws(
    () => finalizeAuthorization(baseOptions({
      draft: tamperedDraftPath(draft => { draft.purpose = `${draft.purpose} (altered)`; }, { rehash: false }),
    })),
    /canonical hash mismatch/
  );
});

test('finalizer enforces date-level historical precision and ordering', () => {
  assert.throws(
    () => finalizeAuthorization(baseOptions({ completionDate: '2026-02-31' })),
    /completion date is not a valid operator-confirmed day-precision date/
  );
  assert.throws(
    () => finalizeAuthorization(baseOptions({ scheduledDate: '07/14/2026' })),
    /scheduled date is not a valid/
  );
  assert.throws(
    () => finalizeAuthorization(baseOptions({ paymentDate: '2026-07-13' })),
    /non-decreasing/
  );
  assert.throws(
    () => finalizeAuthorization(baseOptions({ paymentMethod: '<PAYMENT_METHOD>' })),
    /payment method is unresolved/
  );
  assert.throws(
    () => finalizeAuthorization(baseOptions({ approvedAt: '2099-01-01T00:00:00Z' })),
    /must not be in the future/
  );
});

test('finalizer rejects placeholder attestations and attestations naming the wrong person', () => {
  const emptyAttestation = path.join(sandbox, 'empty.txt');
  fs.writeFileSync(emptyAttestation, '   \n');
  assert.throws(
    () => finalizeAuthorization(baseOptions({ operatorAttestationFile: emptyAttestation })),
    /operator attestation is empty or a placeholder/
  );
  const wrongName = path.join(sandbox, 'wrong-name.txt');
  fs.writeFileSync(wrongName, 'I, Somebody Else, attest to this authorization.\n');
  assert.throws(
    () => finalizeAuthorization(baseOptions({ approverAttestationFile: wrongName })),
    /approver attestation must name Jacob Maynard/
  );
  const placeholder = path.join(sandbox, 'placeholder.txt');
  fs.writeFileSync(placeholder, '<OPERATOR_SIGNATURE>\n');
  assert.throws(
    () => finalizeAuthorization(baseOptions({ operatorAttestationFile: placeholder })),
    /operator attestation is empty or a placeholder/
  );
});

test('finalizer never modifies the draft in place and never overwrites outputs', () => {
  assert.throws(
    () => finalizeAuthorization(baseOptions({ output: DEFAULT_DRAFT })),
    /must be a separate file/
  );
  const options = baseOptions();
  finalizeAuthorization(options);
  assert.throws(
    () => finalizeAuthorization(baseOptions({ output: options.output })),
    /Refusing to overwrite/
  );
  assert.ok(fs.readFileSync(DEFAULT_DRAFT).equals(draftBytes));
});

test('finalizer CLI entry point works end to end and requires every operator fact', async () => {
  const output = path.join(sandbox, 'cli-signed.json');
  const { stdout } = await execFileAsync(process.execPath, [
    path.join(root, 'scripts', 'finalizeRevenuePhase16bAuthorization.js'),
    '--operator-attestation-file', attestations.operator,
    '--approver-attestation-file', attestations.approver,
    '--scheduled-date', CONFIRMED.scheduledDate,
    '--completion-date', CONFIRMED.completionDate,
    '--payment-date', CONFIRMED.paymentDate,
    '--payment-method', CONFIRMED.paymentMethod,
    '--approved-at', CONFIRMED.approvedAt,
    '--output', output,
  ], { cwd: root });
  const report = JSON.parse(stdout);
  assert.equal(report.status, 'finalized');
  assert.equal(report.database_connectivity_used, false);
  assert.equal(fs.existsSync(output), true);

  await assert.rejects(
    execFileAsync(process.execPath, [
      path.join(root, 'scripts', 'finalizeRevenuePhase16bAuthorization.js'),
      '--operator-attestation-file', attestations.operator,
    ], { cwd: root }),
    error => /--approver-attestation-file is required/.test(error.stderr)
  );
  assert.throws(() => parseArguments(['--unknown', 'value']), /Unknown argument/);
});
