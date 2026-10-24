'use strict';

/**
 * Non-empirical operator transport proof. Requires relay consent env vars and
 * SIGNAL_OPERATOR_TRANSPORT_TEST_APPROVED=1. Does not touch Telegram or research tables.
 */
require('dotenv').config();

const { operationFromEnv } = require('../services/signalOperator/operationGate');
const { createBrevoRelay, operationalTransportTest } = require('../services/signalOperator/relay');
const { sendSignalOperatorAlert } = require('../packages/signal-v1/operator/sendSignalOperatorAlert');

async function main() {
  const testId = process.argv[2] || `cli-${Date.now()}`;
  const gate = operationFromEnv();
  const readiness = gate();
  if (!readiness.ok) {
    console.error(JSON.stringify({ ok: false, reason: readiness.reason }));
    process.exit(1);
  }
  const relay = createBrevoRelay({
    enabled: process.env.SIGNAL_OPERATOR_RELAY_ENABLED === '1',
    consented: process.env.SIGNAL_OPERATOR_RELAY_CONSENT === '1',
    operationalTestApproved: process.env.SIGNAL_OPERATOR_TRANSPORT_TEST_APPROVED === '1',
    apiKey: process.env.BREVO_API_KEY,
    gate,
  });
  const started = Date.now();
  const alert = operationalTransportTest(testId);
  const deliveries = await sendSignalOperatorAlert(alert, { transports: [relay], sentAt: new Date() });
  console.log(JSON.stringify({
    ok: true,
    testId,
    latencyMs: Date.now() - started,
    deliveries: deliveries.map(d => ({ provider: d.provider, receipt: d.receipt, accepted: d.accepted })),
  }));
}

main().catch(err => {
  console.error(JSON.stringify({ ok: false, error: String(err.message || err) }));
  process.exit(1);
});
