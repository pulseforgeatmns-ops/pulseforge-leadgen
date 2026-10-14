#!/usr/bin/env node
'use strict';
// Opt-in, disposable-only demo. No DATABASE_URL argument and no production import.
const { createExeterPortalHarness, DEMO_NOTICE } = require('../test/helpers/exeterPortalHarness');
async function main() {
  if (!process.argv.includes('--disposable-local')) throw new Error('Pass --disposable-local to create a temporary synthetic demo. Existing databases are never used.');
  const h = await createExeterPortalHarness();
  let closing = false;
  const close = async () => { if (closing) return; closing = true; await h.close(); };
  process.once('SIGINT', () => close().then(() => process.exit(0)));
  process.once('SIGTERM', () => close().then(() => process.exit(0)));
  console.log(DEMO_NOTICE);
  console.log(`Disposable loopback portal: ${h.base}/portal/anchor`);
  console.log('Test identities have deliberately invalid password hashes. No real accounts or credentials were created.');
  console.log(`In a local browser console on this origin, run: await fetch('/__test/session/client', {method:'POST'}); location.href='/portal/anchor'`);
  console.log('Use operator instead of client for the synthetic operator session. Ctrl-C removes the disposable database.');
}
main().catch(error => { console.error(error.message); process.exitCode = 1; });
