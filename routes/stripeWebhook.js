const express = require('express');
const Stripe = require('stripe');
const pool = require('../db');

const STRIPE_API_VERSION = '2026-06-24.dahlia';
const CASH_SETTLED_EVENT = 'cash_balance.funds_available';
const CASH_BALANCE_TRANSACTION_EVENT = 'customer_cash_balance_transaction.created';

if (!process.env.STRIPE_WEBHOOK_SECRET) {
  throw new Error('[stripe webhook] STRIPE_WEBHOOK_SECRET is required; refusing to start without signature verification.');
}

if (!process.env.STRIPE_SECRET_KEY) {
  throw new Error('[stripe webhook] STRIPE_SECRET_KEY is required.');
}

const stripe = new Stripe(process.env.STRIPE_SECRET_KEY, {
  apiVersion: STRIPE_API_VERSION,
});
const router = express.Router();

function stripeId(value) {
  if (!value) return null;
  return typeof value === 'string' ? value : value.id || null;
}

function unixTimestamp(value) {
  return Number.isFinite(value) ? new Date(value * 1000) : null;
}

function invoiceSubscriptionId(invoice) {
  return stripeId(invoice.subscription) || stripeId(invoice.parent?.subscription_details?.subscription);
}

function serviceLine(object) {
  return object?.metadata?.service_line || null;
}

async function resolveClientId(object, db = pool) {
  const metadataId = Number.parseInt(object?.metadata?.client_id, 10);
  if (Number.isInteger(metadataId) && metadataId > 0) return metadataId;

  const customerId = stripeId(object?.customer);
  if (!customerId) return null;
  const result = await db.query(
    'SELECT id FROM clients WHERE stripe_customer_id = $1 LIMIT 1',
    [customerId]
  );
  return result.rows[0]?.id || null;
}

async function persistCustomerLink(object, clientId, db = pool) {
  const customerId = stripeId(object?.customer) || (object?.object === 'customer' ? object.id : null);
  if (!clientId || !customerId) return;

  const result = await db.query(`
    UPDATE clients
    SET stripe_customer_id = $1
    WHERE id = $2
      AND (stripe_customer_id IS NULL OR stripe_customer_id = $1)
    RETURNING id
  `, [customerId, clientId]);
  if (!result.rows.length) {
    console.warn('[stripe webhook] customer ID is already linked to a different client; link was not overwritten', {
      client_id: clientId,
      stripe_customer_id: customerId,
    });
  }
}

async function upsertInvoice(invoice, clientId, options = {}, db = pool) {
  const status = options.status || invoice.status || 'open';
  const paidAt = options.paidAt || null;
  const result = await db.query(`
    INSERT INTO stripe_invoices (
      id, client_id, stripe_customer_id, subscription_id, status, collection_method,
      amount_due, amount_paid, amount_remaining, currency, due_date,
      hosted_invoice_url, invoice_pdf, paid_at, service_line
    ) VALUES (
      $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
    )
    ON CONFLICT (id) DO UPDATE SET
      client_id = COALESCE(EXCLUDED.client_id, stripe_invoices.client_id),
      stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, stripe_invoices.stripe_customer_id),
      subscription_id = COALESCE(EXCLUDED.subscription_id, stripe_invoices.subscription_id),
      status = EXCLUDED.status,
      collection_method = COALESCE(EXCLUDED.collection_method, stripe_invoices.collection_method),
      amount_due = EXCLUDED.amount_due,
      amount_paid = EXCLUDED.amount_paid,
      amount_remaining = EXCLUDED.amount_remaining,
      currency = EXCLUDED.currency,
      due_date = COALESCE(EXCLUDED.due_date, stripe_invoices.due_date),
      hosted_invoice_url = COALESCE(EXCLUDED.hosted_invoice_url, stripe_invoices.hosted_invoice_url),
      invoice_pdf = COALESCE(EXCLUDED.invoice_pdf, stripe_invoices.invoice_pdf),
      paid_at = COALESCE(EXCLUDED.paid_at, stripe_invoices.paid_at),
      service_line = COALESCE(EXCLUDED.service_line, stripe_invoices.service_line),
      updated_at = now()
    RETURNING id
  `, [
    invoice.id,
    clientId,
    stripeId(invoice.customer),
    invoiceSubscriptionId(invoice),
    status,
    invoice.collection_method || null,
    invoice.amount_due || 0,
    invoice.amount_paid || 0,
    invoice.amount_remaining || 0,
    invoice.currency || 'usd',
    unixTimestamp(invoice.due_date),
    invoice.hosted_invoice_url || null,
    invoice.invoice_pdf || null,
    paidAt,
    serviceLine(invoice),
  ]);
  return result.rows[0];
}

async function upsertSubscription(subscription, clientId, options = {}, db = pool) {
  const firstItem = subscription.items?.data?.[0] || {};
  await db.query(`
    INSERT INTO stripe_subscriptions (
      id, client_id, stripe_customer_id, status, collection_method,
      current_period_start, current_period_end, cancel_at_period_end,
      canceled_at, price_id, amount, interval, service_line
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
    ON CONFLICT (id) DO UPDATE SET
      client_id = COALESCE(EXCLUDED.client_id, stripe_subscriptions.client_id),
      stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, stripe_subscriptions.stripe_customer_id),
      status = EXCLUDED.status,
      collection_method = COALESCE(EXCLUDED.collection_method, stripe_subscriptions.collection_method),
      current_period_start = COALESCE(EXCLUDED.current_period_start, stripe_subscriptions.current_period_start),
      current_period_end = COALESCE(EXCLUDED.current_period_end, stripe_subscriptions.current_period_end),
      cancel_at_period_end = EXCLUDED.cancel_at_period_end,
      canceled_at = COALESCE(EXCLUDED.canceled_at, stripe_subscriptions.canceled_at),
      price_id = COALESCE(EXCLUDED.price_id, stripe_subscriptions.price_id),
      amount = COALESCE(EXCLUDED.amount, stripe_subscriptions.amount),
      interval = COALESCE(EXCLUDED.interval, stripe_subscriptions.interval),
      service_line = COALESCE(EXCLUDED.service_line, stripe_subscriptions.service_line),
      updated_at = now()
  `, [
    subscription.id,
    clientId,
    stripeId(subscription.customer),
    options.status || subscription.status || 'active',
    subscription.collection_method || null,
    unixTimestamp(subscription.current_period_start),
    unixTimestamp(subscription.current_period_end),
    Boolean(subscription.cancel_at_period_end),
    options.canceledAt || unixTimestamp(subscription.canceled_at),
    stripeId(firstItem.price),
    firstItem.price?.unit_amount ?? null,
    firstItem.price?.recurring?.interval || null,
    serviceLine(subscription),
  ]);
}

async function upsertCheckoutSession(session, clientId, db = pool) {
  await db.query(`
    INSERT INTO stripe_checkout_sessions (
      id, client_id, stripe_customer_id, payment_intent_id, invoice_id, status,
      payment_status, amount_total, currency, service_line, completed_at
    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ON CONFLICT (id) DO UPDATE SET
      client_id = COALESCE(EXCLUDED.client_id, stripe_checkout_sessions.client_id),
      stripe_customer_id = COALESCE(EXCLUDED.stripe_customer_id, stripe_checkout_sessions.stripe_customer_id),
      payment_intent_id = COALESCE(EXCLUDED.payment_intent_id, stripe_checkout_sessions.payment_intent_id),
      invoice_id = COALESCE(EXCLUDED.invoice_id, stripe_checkout_sessions.invoice_id),
      status = EXCLUDED.status,
      payment_status = EXCLUDED.payment_status,
      amount_total = EXCLUDED.amount_total,
      currency = EXCLUDED.currency,
      service_line = COALESCE(EXCLUDED.service_line, stripe_checkout_sessions.service_line),
      completed_at = COALESCE(EXCLUDED.completed_at, stripe_checkout_sessions.completed_at),
      updated_at = now()
  `, [
    session.id,
    clientId,
    stripeId(session.customer),
    stripeId(session.payment_intent),
    stripeId(session.invoice),
    session.status || null,
    session.payment_status || null,
    session.amount_total || 0,
    session.currency || 'usd',
    serviceLine(session),
    unixTimestamp(session.created),
  ]);
}

async function handleRefund(charge, db = pool) {
  const invoiceId = stripeId(charge.invoice);
  const paymentIntentId = stripeId(charge.payment_intent);
  const amountRefunded = charge.amount_refunded || 0;

  if (invoiceId) {
    await db.query(`
      UPDATE stripe_invoices
      SET amount_refunded = $2, updated_at = now()
      WHERE id = $1
    `, [invoiceId, amountRefunded]);
  }

  if (paymentIntentId) {
    await db.query(`
      UPDATE stripe_checkout_sessions
      SET amount_refunded = $2, updated_at = now()
      WHERE payment_intent_id = $1
    `, [paymentIntentId, amountRefunded]);
  }

  if (!invoiceId && !paymentIntentId) {
    console.warn('[stripe webhook] refund has no linked invoice or payment intent', { charge_id: charge.id });
  }
}

async function handleCreditNote(creditNote, clientId, db = pool) {
  const invoiceId = stripeId(creditNote.invoice);
  if (!invoiceId) {
    console.warn('[stripe webhook] credit note has no invoice', { credit_note_id: creditNote.id });
    return;
  }

  await db.query(`
    INSERT INTO stripe_credit_notes (id, invoice_id, client_id, amount, currency, status)
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (id) DO UPDATE SET
      invoice_id = EXCLUDED.invoice_id,
      client_id = COALESCE(EXCLUDED.client_id, stripe_credit_notes.client_id),
      amount = EXCLUDED.amount,
      currency = EXCLUDED.currency,
      status = EXCLUDED.status,
      updated_at = now()
  `, [
    creditNote.id,
    invoiceId,
    clientId,
    creditNote.amount || 0,
    creditNote.currency || 'usd',
    creditNote.status || null,
  ]);
  await db.query(`
    UPDATE stripe_invoices invoice
    SET amount_credited = COALESCE((
      SELECT SUM(amount)::integer
      FROM stripe_credit_notes
      WHERE invoice_id = invoice.id AND status IS DISTINCT FROM 'void'
    ), 0), updated_at = now()
    WHERE invoice.id = $1
  `, [invoiceId]);
}

async function handleCashFundsAvailable(cashBalance, db = pool) {
  const customerId = stripeId(cashBalance.customer);
  if (!customerId) {
    console.warn('[stripe webhook] cash balance availability event has no customer');
    return;
  }
  // Stripe only provides a customer-level cash-balance object in this event.
  // Marking their unpaid-settlement invoice mirrors that fact without treating
  // invoice.paid as cash in hand.
  await db.query(`
    UPDATE stripe_invoices
    SET funds_settled_at = now(), updated_at = now()
    WHERE stripe_customer_id = $1
      AND status = 'paid'
      AND funds_settled_at IS NULL
  `, [customerId]);
}

async function handleCashBalanceTransaction(transaction, db = pool) {
  if (transaction.type === 'funding_reversed') {
    const customerId = stripeId(transaction.customer);
    if (customerId) {
      // Stripe's event is customer-scoped rather than invoice-scoped. Clear the
      // local cash-in-hand marker so reporting cannot continue counting funds
      // that Stripe says were reversed. A later funds_available event can set it
      // again after the replacement funds arrive.
      await db.query(`
        UPDATE stripe_invoices
        SET funds_settled_at = NULL, updated_at = now()
        WHERE stripe_customer_id = $1
          AND funds_settled_at IS NOT NULL
      `, [customerId]);
    }
    console.warn('[stripe webhook] customer cash-balance funding reversed', {
      customer_id: customerId,
      transaction_id: transaction.id,
    });
  }
}

async function processStripeEvent(event, db = pool) {
  const object = event.data?.object || {};
  const clientId = await resolveClientId(object, db);
  await persistCustomerLink(object, clientId, db);
  if (!clientId && ![CASH_SETTLED_EVENT, CASH_BALANCE_TRANSACTION_EVENT].includes(event.type)) {
    console.warn('[stripe webhook] unable to resolve client_id; retaining event without a client link', {
      event_id: event.id,
      type: event.type,
      customer_id: stripeId(object.customer),
    });
  }

  switch (event.type) {
    case 'invoice.finalized':
      await upsertInvoice(object, clientId, {}, db);
      break;
    case 'invoice.paid':
      await upsertInvoice(object, clientId, { status: 'paid', paidAt: new Date() }, db);
      break;
    case 'invoice.payment_failed':
      await upsertInvoice(object, clientId, {}, db);
      console.warn('[stripe webhook] invoice payment failed', { invoice_id: object.id, client_id: clientId });
      break;
    case 'checkout.session.completed':
      await upsertCheckoutSession(object, clientId, db);
      break;
    case 'customer.subscription.created':
    case 'customer.subscription.updated':
      await upsertSubscription(object, clientId, {}, db);
      break;
    case 'customer.subscription.deleted':
      await upsertSubscription(object, clientId, {
        status: 'canceled',
        canceledAt: unixTimestamp(object.canceled_at) || new Date(),
      }, db);
      break;
    case 'charge.refunded':
      await handleRefund(object, db);
      break;
    case 'credit_note.created':
      await handleCreditNote(object, clientId, db);
      break;
    case 'customer.created':
      // The link is persisted above. Keeping this explicit prevents a future
      // cleanup from treating Customer creation as an unhandled event.
      break;
    case CASH_SETTLED_EVENT:
      await handleCashFundsAvailable(object, db);
      break;
    case CASH_BALANCE_TRANSACTION_EVENT:
      await handleCashBalanceTransaction(object, db);
      break;
    default:
      console.info('[stripe webhook] ignored event type', { event_id: event.id, type: event.type });
  }

  await db.query('UPDATE stripe_events SET processed_at = now() WHERE id = $1', [event.id]);
}

function createStripeWebhookHandler({ stripeClient = stripe, db = pool } = {}) {
  return async function stripeWebhookHandler(req, res) {
    const signature = req.headers['stripe-signature'];
    let event;
    try {
      event = stripeClient.webhooks.constructEvent(req.body, signature, process.env.STRIPE_WEBHOOK_SECRET);
    } catch (err) {
      return res.status(400).send(`Webhook Error: ${err.message}`);
    }

    try {
      const inserted = await db.query(`
        INSERT INTO stripe_events (id, type, livemode, payload)
        VALUES ($1, $2, $3, $4::jsonb)
        ON CONFLICT (id) DO NOTHING
        RETURNING id
      `, [event.id, event.type, Boolean(event.livemode), JSON.stringify(event)]);
      if (!inserted.rows.length) return res.sendStatus(200);
    } catch (err) {
      console.error('[stripe webhook] failed to record event receipt', { event_id: event.id, error: err.message });
      return res.status(500).send('Unable to record webhook event');
    }

    // Event receipt/deduplication is durable before acknowledgement. The
    // remaining writes happen after the response so Stripe is never held up by
    // slow reporting work.
    res.sendStatus(200);
    setImmediate(() => {
      processStripeEvent(event, db).catch(err => {
        console.error('[stripe webhook] event processing failed', { event_id: event.id, error: err.message });
      });
    });
  };
}

router.post('/', createStripeWebhookHandler());

module.exports = router;
module.exports.CASH_BALANCE_TRANSACTION_EVENT = CASH_BALANCE_TRANSACTION_EVENT;
module.exports.CASH_SETTLED_EVENT = CASH_SETTLED_EVENT;
module.exports.STRIPE_API_VERSION = STRIPE_API_VERSION;
module.exports.createStripeWebhookHandler = createStripeWebhookHandler;
module.exports.processStripeEvent = processStripeEvent;
module.exports.resolveClientId = resolveClientId;
module.exports.persistCustomerLink = persistCustomerLink;
