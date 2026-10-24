'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { buildSpreadsheetProposal, formatSpreadsheetProposal, isoDate } = require('../packages/max/stateIngestion/spreadsheetProposal');
const sourceHash = 'a'.repeat(64);
function snapshot(extra = {}) { return { clientId: 1, companies: [], prospects: [{ id: 'p1', client_id: 1, company_name: 'Sample Business', phone: '603-555-0100' }], contacts: [], activities: [], tasks: [], suppressions: [], effects: [], ...extra }; }
function proposal(values, context = snapshot(), rowNumber = 3) { return buildSpreadsheetProposal({ structuredData: { filename: 'example.xlsx', sheets: [{ sheet: 'Sheet1', rows: [{ rowNumber, values: { company: 'Sample Business', phone: '(603) 555-0100', ...values }, columnProvenance: { columns: { Details: { canonical: 'notes', cell: `L${rowNumber}`, rawValue: values.notes } } } }] }] }, context, scope: { clientId: 1, sourceHash } }); }
test('rejects asynchronous, absent, incomplete, and cross-tenant snapshots', () => {
  assert.throws(() => proposal({}, Promise.resolve(snapshot())), /awaited/);
  assert.throws(() => proposal({}, snapshot({ contacts: undefined })), /Incomplete/);
  assert.throws(() => proposal({}, snapshot({ clientId: 2 })), /tenant mismatch/);
  assert.throws(() => proposal({}, snapshot({ contacts: [{ client_id: 2 }] })), /Cross-tenant/);
});
test('exact name alone and conflicting identifiers remain unresolved', () => {
  const p = proposal({ phone: '' });
  assert.equal(p.rows[0].accountResolution.status, 'ambiguous');
  assert.equal(p.summary.selectableOperations, 0);
  const ties = proposal({}, snapshot({ prospects: [{ id: 'one', company_name: 'Sample Business', phone: '6035550100' }, { id: 'two', company_name: 'Sample Business', phone: '6035550100' }] }));
  assert.equal(ties.rows[0].accountResolution.status, 'ambiguous');
});
test('notes with meaningful appended information are additive, exact notes no-op', () => {
  const context = snapshot({ activities: [{ prospect_id: 'p1', text: 'Spoke with Kristy' }] });
  assert.ok(proposal({ notes: 'Spoke with Kristy. Mark is now the decision maker.' }, context).operations.some(op => op.type === 'ADD_NOTE'));
  assert.equal(proposal({ notes: 'Spoke with Kristy' }, context).operations.length, 0);
});
test('blank source values never clear CRM, and different nonblank values are held', () => {
  const context = snapshot({ prospects: [{ id: 'p1', company_name: 'Sample Business', phone: '6035550100', email: 'existing@example.com' }] });
  assert.equal(proposal({ email: '' }, context).operations.length, 0);
  const p = proposal({ email: 'different@example.com', notes: 'Update' }, context);
  assert.ok(p.rows[0].conflicts.some(conflict => conflict.code === 'FIELD_CONFLICT'));
  assert.ok(p.operations.every(op => op.blocked));
});
test('first names and multiple contacts stay separate and unresolved', () => {
  const context = snapshot({ contacts: [{ id: 'c1', prospect_id: 'p1', name: 'Mike Smith' }, { id: 'c2', prospect_id: 'p1', name: 'Mike Jones' }] });
  const row = proposal({ contact: 'Mike\nKristy', notes: 'Mark is now the one in charge of decisions.' }, context).rows[0];
  assert.deepEqual(row.contacts.map(item => item.name), ['Mike', 'Kristy']);
  assert.ok(row.contacts.every(item => item.status === 'unresolved'));
  assert.ok(row.conflicts.some(item => item.code === 'DECISION_MAKER_REVIEW'));
});
test('institutional identity contradictions hold every operation', () => {
  const p = proposal({ company: 'Northern State University', email: 'person@other.edu', notes: 'Need to email information.' }, snapshot({ prospects: [{ id: 'p1', company_name: 'Northern State University', phone: '6035550100' }] }));
  assert.ok(p.rows[0].conflicts.some(item => item.code === 'IDENTITY_CONFLICT'));
  assert.ok(p.operations.every(item => item.blocked));
});
test('historical dates and source note merge into one dated call, no deadline inferred', () => {
  const p = proposal({ first_call_date: '2026-09-27', follow_up_call_date: '2026-10-02', notes: '9/27: Spoke with contact.\n10/2: Called and left message.', follow_up_needed: 'Yes' });
  const calls = p.operations.filter(op => op.type === 'ADD_ACTIVITY');
  assert.equal(calls.length, 2);
  assert.deepEqual(calls.map(op => op.after.occurredOn), ['2026-09-27', '2026-10-02']);
  assert.equal(p.operations.find(op => op.type === 'ADD_TASK').after.dueDate, null);
  assert.equal(p.operations.find(op => op.type === 'ADD_TASK').after.externalSendingAuthorized, false);
});
test('date-only notes provide no invented outcome and ambiguous years are held', () => {
  const p = proposal({ first_call_date: '2026-10-02', notes: '10/2: ', follow_up_needed: 'Yes' });
  const activities = p.operations.filter(op => op.type === 'ADD_ACTIVITY');
  assert.equal(activities.length, 1);
  assert.match(activities[0].after.details, /outcome unspecified/);
  assert.ok(p.rows[0].conflicts.some(item => item.code === 'INCOMPLETE_NOTE'));
  assert.ok(proposal({ notes: '10/2: Called' }).rows[0].conflicts.some(item => item.code === 'NOTE_DATE_AMBIGUOUS'));
  assert.equal(isoDate('2026-02-30'), null);
});
test('call suppression is channel-specific and prevents tasks; application state is distinct', () => {
  const removed = proposal({ follow_up_needed: 'No - Remove from call list', notes: 'Not interested' });
  assert.equal(removed.operations.find(op => op.type === 'SUPPRESS_CALL').after.channel, 'call');
  assert.ok(!removed.operations.some(op => op.type === 'ADD_TASK'));
  const progress = proposal({ follow_up_needed: 'No - application in progress', notes: 'Told to contact management' });
  assert.ok(progress.operations.some(op => op.field === 'ao_current_status' && op.after === 'application_in_progress'));
  assert.ok(!progress.operations.some(op => op.type === 'ADD_TASK'));
});
test('multiple phone values and labels remain visible and cannot overwrite scalar field', () => {
  const p = proposal({ phone: 'Main: 603-555-0100\nFacilities: 603-555-0101' });
  assert.equal(p.rows[0].phoneNumbers.length, 2);
  assert.ok(p.rows[0].conflicts.some(item => item.code === 'MULTIPLE_PHONES'));
  assert.ok(!p.operations.some(op => op.field === 'phone'));
});
test('effects survive file/row changes and unchanged replay yields no business operation', () => {
  const first = proposal({ notes: 'A substantive note', follow_up_needed: 'Yes' });
  const current = snapshot();
  current.effects = first.operations.map((operation, index) => {
    const observed = { id: `effect-${index}`, prospect_id: 'p1', status: 'open', deadline: null, notes: operation.after.text, metadata: operation.after, routing_snapshot: operation.after, first_action: operation.after.description };
    (operation.type === 'ADD_TASK' ? (current.tasks ||= []) : current.activities).push(observed);
    return { semanticKey: operation.semanticKey, operation, observed };
  });
  const replay = proposal({ notes: 'A substantive note', follow_up_needed: 'Yes' }, current, 19);
  assert.equal(replay.operations.length, 0);
  assert.equal(replay.rows[0].outcome, 'unchanged');
});
test('format includes actual values, target, operation IDs, source cells and holds', () => {
  const p = proposal({ notes: 'Exact source text' });
  const output = formatSpreadsheetProposal(p);
  assert.match(output, /p1/); assert.match(output, /Exact source text/); assert.match(output, /Sheet1!L3/); assert.match(output, /op_/); assert.match(output, /Nothing saved/);
});

test('exact hash-pinned workbook: all twelve prospects, structural rows and operational facts', async () => {
  const fs = require('node:fs');
  const crypto = require('node:crypto');
  const { extract } = require('../packages/max/composer/adapters/spreadsheet');
  const bytes = fs.readFileSync(require('node:path').join(__dirname, 'fixtures/anchor-cleaning-actual.xlsx'));
  const hash = crypto.createHash('sha256').update(bytes).digest('hex');
  assert.equal(hash, 'b1cddfa475f244e27d8c81a381976b30911b34f53ea6c449f868a54208ba6a75');
  const parsed = await extract({}, { buffer: bytes, filename: 'Anchor Cleaning Prospect List.xlsx' });
  assert.equal(parsed.extractionStatus, 'ready');
  // Deliberate isolated test baseline, not a claim about production account IDs.
  const fixtureRows = parsed.structuredData.sheets[0].rows;
  const context = snapshot({ prospects: fixtureRows.map(row => ({ id: `isolated-${row.rowNumber}`, client_id: 1, company_name: row.values.company, address: row.values.address })) });
  const p = buildSpreadsheetProposal({ structuredData: parsed.structuredData, context, tenantId: 1, fileHash: hash });
  assert.equal(p.rows.length, 12);
  assert.deepEqual(p.rows.map(row => row.rowNumber), [3,4,5,6,7,8,9,10,11,12,13,14]);
  const row = number => p.rows.find(item => item.rowNumber === number);
  const operations = (number, type) => row(number).operations.filter(op => op.type === type);
  assert.equal(p.sourceRows.filter(item => item.classification === 'header').length, 1);
  assert.ok(p.sourceRows.some(item => item.rowNumber === 42 && item.classification === 'legend'));
  assert.ok(p.rows.every(item => item.evidence.every(evidence => evidence.cell && evidence.fileHash === hash)));
  assert.ok(operations(3, 'SET_ACCOUNT_FIELD').some(op => op.after === 'application_in_progress'));
  assert.equal(operations(3, 'ADD_TASK').length, 0);
  assert.ok(operations(3, 'ADD_ACTIVITY').some(op => op.after.occurredOn === '2026-09-23'));
  assert.ok(row(4).contacts.some(contact => contact.name === 'Lori'));
  assert.ok(row(4).conflicts.some(item => item.code === 'STALE_HYPERLINK'));
  assert.equal(operations(4, 'ADD_TASK')[0].after.kind, 'prepare_email_draft');
  assert.equal(operations(5, 'ADD_ACTIVITY').length, 0);
  assert.match(operations(5, 'ADD_NOTE')[0].after.text, /hodgescompanies.com/);
  assert.deepEqual(row(6).contacts.map(contact => contact.name), ['William Gagnon', 'Mike Goodrow']);
  assert.equal(row(6).phoneNumbers.length, 2);
  assert.equal(operations(6, 'ADD_ACTIVITY').filter(op => op.after.kind === 'phone_call' && op.after.occurredOn === '2026-10-02').length, 1);
  assert.ok(row(6).contactAssertions.some(contact => contact.possibleAlias === 'Billy'));
  assert.ok(row(7).conflicts.some(item => item.code === 'IDENTITY_CONFLICT'));
  assert.ok(row(7).operations.every(op => op.blocked));
  assert.ok(operations(7, 'ADD_ACTIVITY').some(op => op.after.occurredOn === '2026-10-02'));
  assert.ok(row(8).contactAssertions.some(contact => contact.name === 'Mark'));
  assert.ok(row(8).contactAssertions.some(contact => contact.name === 'Mike Arsenault'));
  assert.match(operations(9, 'ADD_NOTE')[0].after.text, /calls out sick/);
  assert.equal(operations(10, 'ADD_TASK')[0].after.kind, 'research');
  assert.equal(operations(11, 'SUPPRESS_CALL').length, 1);
  assert.equal(operations(4, 'SUPPRESS_CALL').length, 0);
  assert.equal(operations(11, 'ADD_TASK').length, 0);
  assert.ok(operations(12, 'ADD_ACTIVITY').some(op => op.after.kind === 'in_person_visit' && op.after.occurredOn === '2026-09-25'));
  assert.equal(row(12).followUp.status, 'unspecified');
  assert.equal(operations(12, 'ADD_TASK').length, 0);
  assert.equal(row(13).followUp.status, 'unspecified');
  assert.ok(operations(13, 'ADD_NOTE').some(op => op.after.category === 'provider_assertion' && op.after.text === 'Nash Family Investment Properties'));
  assert.ok(row(14).conflicts.some(item => item.code === 'INCOMPLETE_NOTE'));
  assert.equal(operations(14, 'ADD_ACTIVITY').length, 1);
  assert.ok(p.operations.filter(op => op.type === 'ADD_TASK').every(op => op.after.dueDate === null && op.after.externalSendingAuthorized === false));
  const formatted = formatSpreadsheetProposal(p);
  for (const item of p.rows) assert.ok(formatted.includes(item.company));
});

test('cross-account contradictory identifiers cannot attach another account email', () => {
  const context = snapshot({ prospects: [{ id: 'p1', company_name: 'Sample Business', phone: '6035550100' }, { id: 'other', company_name: 'Other Business', email: 'other@example.com' }] });
  const p = proposal({ email: 'other@example.com' }, context);
  assert.equal(p.rows[0].accountResolution.status, 'ambiguous');
  assert.ok(p.operations.every(op => op.blocked));
});
test('hidden/formula/error source rows are explicitly held', () => {
  for (const extra of [{ hidden: true }, { cells: { A3: { cellRef: 'A3', formula: '1+1' } } }, { cells: { A3: { cellRef: 'A3', type: 'e' } } }]) {
    const p = buildSpreadsheetProposal({ structuredData: { sheets: [{ sheet: 'Sheet1', rows: [{ rowNumber: 3, values: { company: 'Sample Business', phone: '6035550100', notes: 'Source text' }, ...extra }] }] }, context: snapshot(), sourceHash, tenantId: 1 });
    assert.equal(p.rows[0].outcome, 'needs_review');
    assert.ok(p.operations.every(op => op.blocked));
  }
});
test('reviewed resolutions are hash-bound and cannot select another account contact', () => {
  const structuredData = { sheets: [{ sheet: 'Sheet1', rows: [{ rowNumber: 3, values: { company: 'Sample Business', contact: 'Mike' } }] }] };
  const context = snapshot({ contacts: [{ id: 'outside', prospect_id: 'other', name: 'Mike' }] });
  const decision = { sourceHash, sheet: 'Sheet1', rowNumber: 3, accountId: 'p1', identityEvidence: 'Operator verified address against source.' };
  const build = resolutions => buildSpreadsheetProposal({ structuredData, context, tenantId: 1, sourceHash, resolutions });
  assert.throws(() => build([{ ...decision, sourceHash: 'b'.repeat(64) }]), /source mismatch/);
  assert.throws(() => build([{ ...decision, identityEvidence: '' }]), /identity evidence/);
  assert.throws(() => build([{ ...decision, accountId: 'other' }]), /outside/);
  assert.throws(() => build([{ ...decision, contacts: [{ sourceName: 'Mike', contactId: 'outside' }] }]), /outside/);
  const resolved = build([{ ...decision, contacts: [{ sourceName: 'Mike', create: true }] }]);
  assert.equal(resolved.rows[0].contacts[0].status, 'reviewed_new_contact');
  assert.ok(resolved.operations.some(op => op.type === 'ADD_CONTACT' && !op.blocked));
});
test('confirmed new account has stable target and exact creation dependencies', () => {
  const structuredData = { sheets: [{ sheet: 'Sheet1', rows: [{ rowNumber: 3, values: { company: 'Entirely New Business', email: 'new@example.com', first_call_date: '2026-09-01' } }] }] };
  const resolutions = [{ sourceHash, sheet: 'Sheet1', rowNumber: 3, createAccount: true, identityEvidence: 'Operator verified unique organization registration.' }];
  const build = () => buildSpreadsheetProposal({ structuredData, context: snapshot(), sourceHash, tenantId: 1, scope: { aoId: 2 }, resolutions });
  const p = build();
  const creation = p.operations.find(op => op.type === 'CREATE_ACCOUNT');
  assert.equal(creation.blocked, false);
  assert.equal(creation.after.email, undefined);
  assert.match(creation.target.accountId, /^[a-f0-9-]{36}$/);
  assert.equal(creation.target.accountId, build().operations[0].target.accountId);
  for (const op of p.operations.filter(op => op !== creation)) assert.deepEqual(op.dependsOn, [creation.id]);
});
test('deleted imported notes and tasks are integrity holds, not no-ops', () => {
  const first = proposal({ notes: 'Substantive note', follow_up_needed: 'Yes' });
  const current = snapshot({ effects: first.operations.map(operation => ({ semanticKey: operation.semanticKey, observed: { id: 'deleted', prospect_id: 'p1', metadata: operation.after } })) });
  const replay = proposal({ notes: 'Substantive note', follow_up_needed: 'Yes' }, current);
  assert.equal(replay.rows[0].outcome, 'needs_review');
  assert.ok(replay.rows[0].conflicts.some(item => item.code === 'PERSISTED_EFFECT_CHANGED'));
  assert.ok(replay.operations.every(op => op.blocked));
});
test('existing native notes are equality evidence and unresolved contacts are never unchanged', () => {
  assert.equal(proposal({ notes: 'Actual existing note' }, snapshot({ activities: [{ prospect_id: 'p1', notes: 'Actual existing note' }] })).operations.length, 0);
  assert.equal(proposal({ contact: 'Jane Smith' }).rows[0].outcome, 'needs_review');
});

test('snapshot requires every comparison collection, including empty historical/task/effect lists', () => {
  for (const field of ['prospects', 'companies', 'contacts', 'activities', 'tasks', 'suppressions', 'effects']) {
    assert.throws(() => proposal({}, snapshot({ [field]: undefined })), new RegExp(`Incomplete CRM snapshot: ${field}`));
  }
});
test('two distinct same-day call narratives remain distinct without guessing correspondence', () => {
  const p = proposal({ first_call_date: '2026-10-02', notes: '10/2: Called at 9am and left voicemail.\n10/2: Called at 4pm and spoke with office manager.' });
  const activities = p.operations.filter(op => op.type === 'ADD_ACTIVITY');
  assert.equal(activities.length, 2);
  assert.ok(activities.some(op => op.after.details.includes('9am')));
  assert.ok(activities.some(op => op.after.details.includes('4pm')));
  assert.ok(p.rows[0].conflicts.some(item => item.code === 'EVENT_ASSOCIATION_REVIEW'));
  assert.ok(activities.every(op => op.blocked));
});
test('first and follow-up date fields on same day retain both events', () => {
  const p = proposal({ first_call_date: '2026-10-02', follow_up_call_date: '2026-10-02' });
  const activities = p.operations.filter(op => op.type === 'ADD_ACTIVITY');
  assert.equal(activities.length, 2);
  assert.ok(activities.some(op => op.after.details.startsWith('First phone call')));
  assert.ok(activities.some(op => op.after.details.startsWith('Follow-up phone call')));
});
test('reviewed source amendment carries prior value and exact writer authorization flag', () => {
  const context = snapshot({ prospects: [{ id: 'p1', company_name: 'Sample Business', phone: '6035550100', email: 'prior@example.com' }] });
  const structuredData = { sheets: [{ sheet: 'Sheet1', rows: [{ rowNumber: 3, values: { company: 'Sample Business', phone: '6035550100', email: 'new@example.com' } }] }] };
  const resolutions = [{ sourceHash, sheet: 'Sheet1', rowNumber: 3, identityEvidence: 'Jake verified corrected business email.', fields: [{ field: 'email', decision: 'use_source' }] }];
  const p = buildSpreadsheetProposal({ structuredData, context, sourceHash, tenantId: 1, resolutions });
  const amendment = p.operations.find(op => op.field === 'email');
  assert.equal(amendment.before, 'prior@example.com');
  assert.equal(amendment.after, 'new@example.com');
  assert.equal(amendment.amendmentReviewed, true);
  assert.equal(amendment.blocked, false);
});

test('candidate summaries expose scoped identity evidence without treating it as a match', () => {
  const context = snapshot({ contacts: [{ id: 'c1', prospect_id: 'p1', name: 'Mike Smith', email: 'mike@example.com' }, { id: 'c2', prospect_id: 'p1', name: 'Mike Jones', phone: '6035550101' }, { id: 'other-contact', prospect_id: 'other', name: 'Mike Elsewhere' }] });
  const p = proposal({ contact: 'Mike' }, context);
  assert.equal(p.rows[0].candidateSummaries[0].name, 'Sample Business');
  assert.equal(p.rows[0].candidateSummaries[0].phone, '603-555-0100');
  assert.deepEqual(p.rows[0].contacts[0].candidateSummaries.map(item => item.name), ['Mike Smith', 'Mike Jones']);
  assert.equal(p.rows[0].contacts[0].status, 'unresolved');
  assert.ok(!p.rows[0].contacts[0].candidateSummaries.some(item => item.id === 'other-contact'));
});

test('explicit future task deadlines are action-bound; relative/ambiguous dates hold', () => {
  const { taskDeadline } = require('../packages/max/stateIngestion/spreadsheetProposal');
  assert.deepEqual(taskDeadline('Email information by 2026-10-12', '2026-10-07'), { dueDate: '2026-10-12' });
  assert.deepEqual(taskDeadline('Research landlord by 10/12/2026', '2026-10-07'), { dueDate: '2026-10-12' });
  assert.deepEqual(taskDeadline('10/2: Called and left message. Need to email information.', '2026-10-07'), { dueDate: null });
  assert.equal(taskDeadline('Email info tomorrow', '2026-10-07').error, 'TASK_DATE_AMBIGUOUS');
  assert.equal(taskDeadline('Email info by 10/12', '2026-10-07').error, 'TASK_DATE_AMBIGUOUS');
  assert.equal(taskDeadline('Email info by 2026-02-30', '2026-10-07').error, 'INVALID_TASK_DATE');
  assert.equal(taskDeadline('Email info by 2026-09-30', '2026-10-07').error, 'PAST_TASK_DATE');
  assert.equal(taskDeadline('Email by 2026-10-12\nCall by 2026-10-13', '2026-10-07').error, 'MULTIPLE_TASK_DATES');
});
test('source-bound provider resolution links separate scoped entity with evidence', () => {
  const context = snapshot({ prospects: [{ id: 'p1', company_name: 'Sample Business', phone: '6035550100' }, { id: 'provider', company_name: 'Building Management' }] });
  const structuredData = { sheets: [{ sheet: 'Sheet1', rows: [{ rowNumber: 3, values: { company: 'Sample Business', phone: '6035550100', provider: 'Building Management' } }] }] };
  const resolution = { sourceHash, sheet: 'Sheet1', rowNumber: 3, providerId: 'provider', identityEvidence: 'Jake confirmed building manager relationship.' };
  const build = decision => buildSpreadsheetProposal({ structuredData, context, tenantId: 1, sourceHash, resolutions: [decision] });
  const p = build(resolution);
  assert.ok(p.operations.some(op => op.type === 'ADD_PROVIDER_RELATIONSHIP' && op.after.providerId === 'provider' && op.after.verified && !op.blocked));
  assert.equal(p.rows[0].provider.status, 'reviewed_match');
  assert.throws(() => build({ ...resolution, providerId: 'outside' }), /within scoped/);
  assert.throws(() => build({ ...resolution, providerId: 'p1' }), /within scoped/);
});
test('changed historical detail cannot silently create a second event', () => {
  const context = snapshot({ activities: [{ id: 'existing-call', prospect_id: 'p1', kind: 'phone_call', occurredOn: '2026-10-02', details: 'Called and left message.' }] });
  const p = proposal({ first_call_date: '2026-10-02', notes: '10/2: Called and left message. Corrected recipient detail.' }, context);
  assert.ok(p.rows[0].conflicts.some(item => item.code === 'HISTORICAL_EVENT_REVIEW'));
  assert.ok(p.operations.every(op => op.blocked));
});
test('contact method changes disclose separate automated-outreach admission hold', () => {
  const p = proposal({ email: 'new@example.com' });
  const operation = p.operations.find(op => op.field === 'email');
  assert.equal(operation.outreachReviewRequired, true);
  assert.equal(operation.admissionChange.field, 'ao_outreach_review_required');
  assert.equal(operation.admissionChange.after, true);
  assert.match(formatSpreadsheetProposal(p), /Hold automated outreach pending separate admission approval/);
});

test('explicit next action gets stated future deadline while unrelated dates do not', () => {
  const structuredData = { sheets: [{ sheet: 'Sheet1', rows: [{ rowNumber: 3, values: { company: 'Sample Business', phone: '6035550100', next_step: 'Email information by 10/12/2026' } }] }] };
  const p = buildSpreadsheetProposal({ structuredData, context: snapshot(), tenantId: 1, sourceHash, asOf: '2026-10-07' });
  const task = p.operations.find(op => op.type === 'ADD_TASK');
  assert.equal(task.after.dueDate, '2026-10-12');
  assert.equal(task.after.externalSendingAuthorized, false);
  assert.equal(task.blocked, false);
  const { taskDeadline } = require('../packages/max/stateIngestion/spreadsheetProposal');
  assert.equal(taskDeadline('Call completed. Application deadline 2026-10-12', '2026-10-07').dueDate, null);
});

test('matching task description with changed business semantics is a conflict, not a no-op', () => {
  const first = proposal({ notes: 'Need to email information.', follow_up_needed: 'Yes' });
  const task = first.operations.find(op => op.type === 'ADD_TASK');
  const base = { id: 'existing-task', prospect_id: 'p1', status: 'open', assignment_category: 'FOLLOW_UP_REQUIRED', motion: 'AO_LED', first_action: task.after.description, deadline: null, routing_snapshot: task.after };
  const equal = proposal({ notes: 'Need to email information.', follow_up_needed: 'Yes' }, snapshot({ tasks: [base] }));
  assert.ok(!equal.operations.some(op => op.type === 'ADD_TASK'));
  for (const change of [{ assigned_ao_id: 123 }, { assignment_category: 'OTHER' }, { motion: 'AUTOMATED' }, { status: 'cancelled' }, { deadline: '2027-12-01' }, { routing_snapshot: { ...task.after, kind: 'research' } }, { routing_snapshot: { ...task.after, externalSendingAuthorized: true } }, { routing_snapshot: {} }]) {
    const p = proposal({ notes: 'Need to email information.', follow_up_needed: 'Yes' }, snapshot({ tasks: [{ ...base, ...change }] }));
    assert.ok(p.rows[0].conflicts.some(item => item.code === 'TASK_SEMANTICS_CONFLICT'));
    assert.ok(p.operations.find(op => op.type === 'ADD_TASK').blocked);
  }
});
test('cross-row contradictory phone amendments are held before approval', () => {
  const rows = [3, 4].map((rowNumber, index) => ({ rowNumber, values: { company: 'Sample Business', phone: `603555010${index + 1}` } }));
  const resolutions = rows.map(row => ({ sourceHash, sheet: 'Sheet1', rowNumber: row.rowNumber, accountId: 'p1', identityEvidence: 'Operator verified this source account.', fields: [{ field: 'phone', decision: 'use_source' }] }));
  const p = buildSpreadsheetProposal({ structuredData: { sheets: [{ sheet: 'Sheet1', rows }] }, context: snapshot(), sourceHash, tenantId: 1, resolutions });
  const changes = p.operations.filter(op => op.field === 'phone');
  assert.equal(changes.length, 2);
  assert.ok(changes.every(op => op.blocked));
  assert.ok(p.rows.every(row => row.conflicts.some(item => item.code === 'CROSS_ROW_FIELD_CONFLICT')));
  assert.equal(p.summary.selectableOperations, 0);
});
test('equivalent repeated field changes have one selectable effect with combined source evidence', () => {
  const rows = [3, 4].map(rowNumber => ({ rowNumber, values: { company: 'Sample Business', phone: '6035550100', email: 'new@example.com' } }));
  const p = buildSpreadsheetProposal({ structuredData: { sheets: [{ sheet: 'Sheet1', rows }] }, context: snapshot(), sourceHash, tenantId: 1 });
  const changes = p.operations.filter(op => op.field === 'email');
  assert.equal(changes.filter(op => !op.blocked).length, 1);
  const primary = changes.find(op => !op.blocked);
  assert.deepEqual(primary.evidence.map(item => item.row), [3, 4]);
  assert.equal(changes.find(op => op.blocked).duplicateOf, primary.id);
});

test('planner and persistence share canonical replay integrity checks', () => {
  const { existingEffectIntact } = require('../packages/max/stateIngestion/spreadsheetProposal');
  const operation = { type: 'ADD_TASK', target: { accountId: 'p1' }, after: { kind: 'research', description: 'Research ownership', dueDate: '2027-10-01', externalSendingAuthorized: false } };
  const observed = { id: 'task', client_id: 1, prospect_id: 'p1', assigned_ao_id: 10, assignment_category: 'FOLLOW_UP_REQUIRED', motion: 'AO_LED', priority: 'normal', deadline: '2027-10-01', status: 'open', first_action: 'Research ownership', routing_snapshot: operation.after };
  const effect = { operation, observed };
  const build = change => snapshot({ tasks: [{ ...observed, ...change }] });
  assert.equal(existingEffectIntact(effect, operation, build({})), true);
  assert.equal(existingEffectIntact(effect, operation, build({ status: 'completed' })), true);
  for (const change of [{ assigned_ao_id: 11 }, { deadline: '2027-10-02' }, { prospect_id: 'other' }, { assignment_category: 'OTHER' }, { motion: 'AUTOMATED' }, { status: 'cancelled' }]) assert.equal(existingEffectIntact(effect, operation, build(change)), false);
});
test('unchanged note text cannot hide altered canonical activity type or owner', () => {
  const first = proposal({ notes: 'Verified source note' });
  const operation = first.operations.find(op => op.type === 'ADD_NOTE');
  const observed = { id: 'note', tenant_id: 1, prospect_id: 'p1', ao_id: 10, activity_type: 'note', notes: 'Verified source note', metadata: operation.after };
  for (const change of [{ ao_id: 11 }, { activity_type: 'call' }]) {
    const p = proposal({ notes: 'Verified source note' }, snapshot({ activities: [{ ...observed, ...change }], effects: [{ operation, observed, semanticKey: operation.semanticKey }] }));
    assert.ok(p.rows[0].conflicts.some(item => item.code === 'PERSISTED_EFFECT_CHANGED'));
    assert.equal(p.rows[0].outcome, 'needs_review');
  }
});
