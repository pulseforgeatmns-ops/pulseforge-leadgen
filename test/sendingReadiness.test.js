const assert = require('assert');
const {
  evaluateSendingReadiness,
  exactSequenceName,
} = require('../utils/sendingReadiness');

const client = {
  id: 10,
  sender_email: 'jacob@goanchorcleaning.com',
  sender_name: 'Jacob Maynard',
  sending_domain: 'goanchorcleaning.com',
};
const prospect = {
  id: 99,
  client_id: 10,
  first_name: 'Avery',
  email: 'avery@example.org',
  email_status: 'valid',
  email_verified: true,
  do_not_contact: false,
  vertical: 'law_firm',
};
const sequenceCatalog = { law_firm: [{ day: 0, subject: '{{business_name}}', body: 'Hi {{first_name}}' }] };
const clientSequenceMap = { 10: { law_firm: 'law_firm' } };
const brevoState = {
  domain: { verified: true, authenticated: true },
  sender: { email: client.sender_email, active: true },
  errors: [],
};

function poolWith({ bounced = false, logs = [], currentProspect = prospect } = {}) {
  return {
    async query(sql) {
      if (sql.includes('FROM prospects')) return { rows: [{ ...currentProspect, company_fields: { name: 'Avery Legal' } }] };
      if (sql.includes('FROM touchpoints')) return { rows: bounced ? [{ action_type: 'email_bounced' }] : [] };
      if (sql.includes('FROM agent_log')) return { rows: logs };
      throw new Error(`Unexpected query: ${sql}`);
    },
  };
}

async function run() {
  assert.strictEqual(exactSequenceName(client, prospect, sequenceCatalog), null);
  assert.strictEqual(exactSequenceName(client, prospect, sequenceCatalog, clientSequenceMap), 'law_firm');
  assert.strictEqual(exactSequenceName(client, { ...prospect, vertical: 'legal_services' }, sequenceCatalog, clientSequenceMap), null);

  const ready = await evaluateSendingReadiness({
    client, prospect, sequenceCatalog, clientSequenceMap, brevoState, pool: poolWith(),
  });
  assert.strictEqual(ready.sendable, true);
  assert.deepStrictEqual(ready.failures, []);

  const blocked = await evaluateSendingReadiness({
    client: { ...client, sender_name: null, sender_email: 'wrong@example.com' },
    prospect: { ...prospect, first_name: '', email_status: 'role', do_not_contact: true },
    sequenceCatalog: {},
    brevoState: {
      domain: { verified: true, authenticated: false },
      sender: { email: client.sender_email, active: false },
      errors: [],
    },
    pool: poolWith({
      bounced: true,
      currentProspect: { ...prospect, email: '', first_name: '', email_status: 'role', do_not_contact: true },
      logs: [{ action: 'email_pending', status: 'pending', payload: { sequence: 'cleaning' } }],
    }),
  });
  const codes = new Set(blocked.failures.map(failure => failure.code));
  [
    'client_sender_configured',
    'client_sender_domain_matches',
    'brevo_domain_authenticated',
    'brevo_sender_active',
    'exact_vertical_sequence_exists',
    'prospect_has_email',
    'prospect_email_verified',
    'prospect_not_dnc',
    'prospect_not_bounced',
    'prospect_not_in_active_sequence',
  ].forEach(code => assert(codes.has(code), `Expected failure ${code}`));

  const missingRequiredToken = await evaluateSendingReadiness({
    client,
    prospect: { ...prospect, first_name: '' },
    sequenceCatalog,
    clientSequenceMap,
    brevoState,
    pool: poolWith({ currentProspect: { ...prospect, first_name: '' } }),
  });
  const tokenFailure = missingRequiredToken.failures.find(
    failure => failure.code === 'template_required_tokens_present'
  );
  assert(tokenFailure, 'Expected required template token failure');
  assert.deepStrictEqual(tokenFailure.details.missing_tokens, ['first_name']);

  const unknownToken = await evaluateSendingReadiness({
    client,
    prospect,
    sequenceCatalog: { law_firm: [{ day: 0, subject: '{{pratice_area}}', body: 'Hi {{first_name}}' }] },
    clientSequenceMap,
    brevoState,
    pool: poolWith(),
  });
  const templateFailure = unknownToken.failures.find(failure => failure.code === 'template_tokens_known');
  assert(templateFailure, 'Expected unknown template token failure');
  assert.deepStrictEqual(templateFailure.details.unknown_tokens, ['pratice_area']);

  console.log('sendingReadiness tests passed');
}

run().catch(err => {
  console.error(err);
  process.exit(1);
});
