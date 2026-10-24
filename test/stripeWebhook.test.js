const assert = require('node:assert/strict');
const test = require('node:test');
const Stripe = require('stripe');

process.env.STRIPE_WEBHOOK_SECRET ||= 'whsec_test';
process.env.STRIPE_SECRET_KEY ||= 'sk_test_123';

const { createStripeWebhookHandler, processStripeEvent } = require('../routes/stripeWebhook');

function responseRecorder() {
  return {
    statusCode: null,
    body: null,
    status(code) { this.statusCode = code; return this; },
    send(body) { this.body = body; return this; },
    sendStatus(code) { this.statusCode = code; return this; },
  };
}

test('rejects a payload whose Stripe signature cannot be verified', async () => {
  const stripeClient = new Stripe(process.env.STRIPE_SECRET_KEY);
  const rawPayload = JSON.stringify({
    id: 'evt_tampered',
    type: 'invoice.paid',
    livemode: false,
    data: { object: { id: 'in_test' } },
  });
  const signature = stripeClient.webhooks.generateTestHeaderString({
    payload: rawPayload,
    secret: process.env.STRIPE_WEBHOOK_SECRET,
  });
  const handler = createStripeWebhookHandler({
    stripeClient,
    db: { query() { throw new Error('DB should not be reached'); } },
  });
  const res = responseRecorder();

  await handler({
    body: Buffer.from(`${rawPayload} `),
    headers: { 'stripe-signature': signature },
  }, res);

  assert.equal(res.statusCode, 400);
  assert.match(res.body, /Webhook Error/);
});

test('returns 200 without processing a duplicate Stripe event', async () => {
  let queries = 0;
  const handler = createStripeWebhookHandler({
    stripeClient: { webhooks: { constructEvent() { return { id: 'evt_replay', type: 'ping', livemode: false, data: { object: {} } }; } } },
    db: {
      async query() {
        queries += 1;
        return { rows: [] };
      },
    },
  });
  const res = responseRecorder();

  await handler({ body: Buffer.from('{}'), headers: {} }, res);

  assert.equal(res.statusCode, 200);
  assert.equal(queries, 1);
});

test('passes raw request bytes to Stripe signature verification', async () => {
  let verifiedBody = null;
  const handler = createStripeWebhookHandler({
    stripeClient: {
      webhooks: {
        constructEvent(body) {
          verifiedBody = body;
          return { id: 'evt_replay_raw', type: 'ping', livemode: false, data: { object: {} } };
        },
      },
    },
    db: { async query() { return { rows: [] }; } },
  });
  const res = responseRecorder();
  const body = Buffer.from('{"untouched":true}');

  await handler({ body, headers: {} }, res);

  assert.equal(res.statusCode, 200);
  assert.strictEqual(verifiedBody, body);
  assert.ok(Buffer.isBuffer(verifiedBody));
});

test('persists an invoice even when client_id cannot be resolved', async () => {
  const queries = [];
  const db = {
    async query(sql, params = []) {
      queries.push({ sql, params });
      if (sql.includes('SELECT id FROM clients')) return { rows: [] };
      if (sql.includes('RETURNING id')) return { rows: [{ id: 'in_unlinked' }] };
      return { rows: [] };
    },
  };

  await processStripeEvent({
    id: 'evt_unlinked',
    type: 'invoice.finalized',
    data: { object: { id: 'in_unlinked', customer: 'cus_unknown', status: 'open' } },
  }, db);

  const invoiceWrite = queries.find(query => query.sql.includes('INSERT INTO stripe_invoices'));
  assert.ok(invoiceWrite);
  assert.equal(invoiceWrite.params[1], null);
  assert.ok(queries.some(query => query.sql.includes('UPDATE stripe_events SET processed_at')));
});

test('funding reversal clears cash settlement markers for the Stripe customer', async () => {
  const queries = [];
  const db = {
    async query(sql, params = []) {
      queries.push({ sql, params });
      if (sql.includes('SELECT id FROM clients')) return { rows: [{ id: 10 }] };
      return { rows: [] };
    },
  };

  await processStripeEvent({
    id: 'evt_reversed',
    type: 'customer_cash_balance_transaction.created',
    data: {
      object: {
        id: 'ccsbtxn_reversed',
        customer: 'cus_anchor',
        type: 'funding_reversed',
      },
    },
  }, db);

  const reversalWrite = queries.find(query => query.sql.includes('SET funds_settled_at = NULL'));
  assert.ok(reversalWrite);
  assert.deepEqual(reversalWrite.params, ['cus_anchor']);
});
