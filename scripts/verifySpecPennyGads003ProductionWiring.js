#!/usr/bin/env node
'use strict';

/**
 * SPEC-PENNY-GADS-003 — Production Google Ads credential wiring and read-only smoke test.
 *
 * Railway (after env vars are set on the service):
 *   cd /app && node scripts/verifySpecPennyGads003ProductionWiring.js --confirm-production
 *
 * Optional one-time account binding (requires ANCHOR_GOOGLE_ADS_* env on Railway, never commit):
 *   cd /app && node scripts/verifySpecPennyGads003ProductionWiring.js --confirm-production --apply-binding
 *
 * Read-only toward Google Ads API (googleAds:search + OAuth token refresh only).
 * Does not mutate campaigns, budgets, bids, keywords, or assets.
 */

require('dotenv').config({ quiet: true });

const https = require('node:https');
const pool = require('../db');
const {
  assessGoogleAdsReadiness,
  readGoogleAdsEvidence,
  resolveGoogleAdsApiVersion,
  resolveAdAccountsForClient,
  PLATFORM,
  googleAds,
} = require('../packages/penny-paid-acquisition');

const { requiredGoogleAdsEnv } = googleAds;

const CLIENT_ID = 10;
const OTHER_CLIENT_ID = 1;
const DEPLOY_FLOOR_SHA = 'd54b6a52976e9a3a4afa1ebf9522a5d0a988b770';
const PRODUCTION_LOGIN_URL = 'https://pulseforge-leadgen-production.up.railway.app/login';

const GOOGLE_ENV_KEYS = Object.freeze([
  'GOOGLE_ADS_DEVELOPER_TOKEN',
  'GOOGLE_ADS_CLIENT_ID',
  'GOOGLE_ADS_CLIENT_SECRET',
]);

const OPTIONAL_GOOGLE_ENV_KEYS = Object.freeze([
  'GOOGLE_ADS_API_VERSION',
  'GOOGLE_ADS_MANAGER_ACCOUNT_ID',
]);

const BINDING_ENV_KEYS = Object.freeze([
  'ANCHOR_GOOGLE_ADS_CUSTOMER_ID',
  'ANCHOR_GOOGLE_ADS_REFRESH_TOKEN',
]);

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const applyBinding = argv.includes('--apply-binding');
  const help = argv.includes('--help') || argv.includes('-h');
  const unknown = argv.filter(
    (arg) => !['--confirm-production', '--apply-binding', '--help', '-h'].includes(arg)
  );
  if (unknown.length) {
    throw new Error(`Unknown argument(s): ${unknown.join(', ')}`);
  }
  return { confirmProduction, applyBinding, help };
}

function printUsage() {
  console.log(`SPEC-PENNY-GADS-003 — Anchor Google Ads production wiring (tenant ${CLIENT_ID})

Usage:
  node scripts/verifySpecPennyGads003ProductionWiring.js --confirm-production
  node scripts/verifySpecPennyGads003ProductionWiring.js --confirm-production --apply-binding

Safety:
  Refuses without --confirm-production.
  Google Ads calls are read-only (OAuth token + googleAds:search).
  --apply-binding inserts one active ad_accounts row when none exists; never updates tokens in logs.
`);
}

function envPresence(keys) {
  return Object.fromEntries(keys.map((key) => [key, Boolean(String(process.env[key] || '').trim())]));
}

function httpStatus(url) {
  return new Promise((resolve, reject) => {
    https.get(url, (res) => {
      res.resume();
      resolve(res.statusCode || 0);
    }).on('error', reject);
  });
}

function sameSha(current, floor) {
  if (!current || !floor) return false;
  return current === floor || current.startsWith(floor) || floor.startsWith(current);
}

function compareStatusAtOrAfter(status) {
  return status === 'ahead' || status === 'identical';
}

function githubJson(path) {
  return new Promise((resolve, reject) => {
    const req = https.request({
      hostname: 'api.github.com',
      path,
      headers: { 'User-Agent': 'pulseforge-spec-penny-gads-003' },
    }, (res) => {
      let body = '';
      res.on('data', (chunk) => { body += chunk; });
      res.on('end', () => {
        try {
          resolve(JSON.parse(body));
        } catch (err) {
          reject(err);
        }
      });
    });
    req.on('error', reject);
    req.end();
  });
}

async function fetchMainHeadSha() {
  const parsed = await githubJson('/repos/pulseforgeatmns-ops/pulseforge-leadgen/commits/main');
  return parsed.sha || null;
}

async function shaAtOrAfter(current, floor) {
  if (!current || !floor) return false;
  if (sameSha(current, floor)) return true;
  try {
    const parsed = await githubJson(
      `/repos/pulseforgeatmns-ops/pulseforge-leadgen/compare/${floor}...${current}`
    );
    return compareStatusAtOrAfter(parsed.status);
  } catch {
    return false;
  }
}

async function inspectAdAccounts(db) {
  const res = await db.query(`
    SELECT id, client_id, platform, account_id, is_active,
           (refresh_token IS NOT NULL AND length(refresh_token) > 0) AS has_refresh
      FROM ad_accounts
     WHERE client_id = $1
     ORDER BY platform, created_at
  `, [CLIENT_ID]);
  return res.rows.map((row) => ({
    id: row.id,
    client_id: row.client_id,
    platform: row.platform,
    account_id: row.account_id,
    is_active: row.is_active,
    has_refresh: row.has_refresh === true,
  }));
}

async function verifyTenantIsolation(db) {
  const anchorGoogle = await resolveAdAccountsForClient({
    clientId: CLIENT_ID,
    platform: PLATFORM.GOOGLE_ADS,
    pool: db,
  });
  const otherGoogle = await resolveAdAccountsForClient({
    clientId: OTHER_CLIENT_ID,
    platform: PLATFORM.GOOGLE_ADS,
    pool: db,
  });
  const crossLeak = otherGoogle.some((row) => anchorGoogle.some((a) => a.id === row.id));
  return {
    anchorGoogleAccountCount: anchorGoogle.length,
    otherClientGoogleAccountCount: otherGoogle.length,
    crossTenantLeak: crossLeak,
  };
}

async function applyGoogleAdsBinding(db) {
  const bindingEnv = envPresence(BINDING_ENV_KEYS);
  if (!bindingEnv.ANCHOR_GOOGLE_ADS_CUSTOMER_ID || !bindingEnv.ANCHOR_GOOGLE_ADS_REFRESH_TOKEN) {
    const err = new Error('--apply-binding requires ANCHOR_GOOGLE_ADS_CUSTOMER_ID and ANCHOR_GOOGLE_ADS_REFRESH_TOKEN.');
    err.code = 'binding_env_missing';
    throw err;
  }

  const customerId = String(process.env.ANCHOR_GOOGLE_ADS_CUSTOMER_ID).trim();
  const refreshToken = String(process.env.ANCHOR_GOOGLE_ADS_REFRESH_TOKEN).trim();

  const client = await db.connect();
  try {
    await client.query('BEGIN');

    const existing = await client.query(`
      SELECT id, platform, is_active
        FROM ad_accounts
       WHERE client_id = $1
         AND platform = 'google_ads'
       FOR UPDATE
    `, [CLIENT_ID]);

    const active = existing.rows.filter((row) => row.is_active === true);
    if (active.length) {
      const err = new Error('Active google_ads row already exists for client_id=10; refusing duplicate insert.');
      err.code = 'google_ads_already_linked';
      throw err;
    }
    if (existing.rows.length) {
      const err = new Error('Inactive google_ads rows exist for client_id=10; resolve manually before insert.');
      err.code = 'google_ads_duplicate_rows';
      throw err;
    }

    const crossTenant = await client.query(`
      SELECT id, client_id
        FROM ad_accounts
       WHERE platform = 'google_ads'
         AND account_id = $1
         AND client_id <> $2
       LIMIT 1
    `, [customerId, CLIENT_ID]);
    if (crossTenant.rows.length) {
      const err = new Error('Google Ads customer id is already linked to another tenant.');
      err.code = 'google_ads_cross_tenant';
      throw err;
    }

    const insert = await client.query(`
      INSERT INTO ad_accounts (
        client_id, platform, account_id, refresh_token, is_active
      ) VALUES ($1, 'google_ads', $2, $3, true)
      RETURNING id, client_id, platform, account_id, is_active
    `, [CLIENT_ID, customerId, refreshToken]);

    await client.query('COMMIT');
    return {
      inserted: true,
      row: {
        id: insert.rows[0].id,
        client_id: insert.rows[0].client_id,
        platform: insert.rows[0].platform,
        account_id: insert.rows[0].account_id,
        is_active: insert.rows[0].is_active,
      },
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

function summarizeEvidence(evidence) {
  if (!evidence) return null;
  return {
    availability: evidence.availability,
    apiVersion: evidence.apiVersion || null,
    campaignEvidenceStatus: evidence.campaignEvidenceStatus || evidence.readiness?.campaignEvidenceStatus || null,
    conversionEvidenceStatus: evidence.conversionEvidenceStatus || evidence.readiness?.conversionEvidenceStatus || null,
    currency: evidence.account?.currency || evidence.readiness?.currency || null,
    timezone: evidence.account?.timezone || evidence.readiness?.timezone || null,
    campaignCount: Array.isArray(evidence.campaigns) ? evidence.campaigns.length : 0,
    aggregates: evidence.aggregates || null,
    error: evidence.error || null,
  };
}

async function run(options = {}) {
  if (options.help) {
    printUsage();
    return { help: true };
  }
  if (!options.confirmProduction) {
    const err = new Error('Refusing to run without --confirm-production.');
    err.code = 'confirm_production_required';
    throw err;
  }
  if (!process.env.DATABASE_URL) {
    const err = new Error('Missing DATABASE_URL.');
    err.code = 'runtime_env_missing';
    throw err;
  }

  const db = options.pool || pool;
  const loginStatus = await httpStatus(PRODUCTION_LOGIN_URL);
  let mainSha = null;
  try {
    mainSha = await fetchMainHeadSha();
  } catch {
    mainSha = null;
  }

  const googleEnv = envPresence(GOOGLE_ENV_KEYS);
  const optionalGoogleEnv = envPresence(OPTIONAL_GOOGLE_ENV_KEYS);
  const bindingEnv = envPresence(BINDING_ENV_KEYS);
  const versionInfo = resolveGoogleAdsApiVersion();
  const missingGoogleEnv = requiredGoogleAdsEnv();

  let bindingResult = { applied: false, skipped: true };
  if (options.applyBinding) {
    bindingResult = await applyGoogleAdsBinding(db);
    bindingResult.skipped = false;
  }

  const adAccountsBeforeProbe = await inspectAdAccounts(db);
  const tenantIsolation = await verifyTenantIsolation(db);

  const readiness = await assessGoogleAdsReadiness({
    clientId: CLIENT_ID,
    pool: db,
    http: options.http,
  });

  let evidenceSummary = null;
  if (readiness.credentialStatus === 'READY' && readiness.accountStatus === 'READY') {
    const accounts = await resolveAdAccountsForClient({
      clientId: CLIENT_ID,
      platform: PLATFORM.GOOGLE_ADS,
      pool: db,
    });
    if (accounts[0]) {
      const evidence = await readGoogleAdsEvidence({ account: accounts[0], http: options.http });
      evidenceSummary = summarizeEvidence(evidence);
    }
  } else if (missingGoogleEnv.length === 0) {
    const accounts = await resolveAdAccountsForClient({
      clientId: CLIENT_ID,
      platform: PLATFORM.GOOGLE_ADS,
      pool: db,
    });
    if (accounts[0]) {
      const evidence = await readGoogleAdsEvidence({ account: accounts[0], http: options.http });
      evidenceSummary = summarizeEvidence(evidence);
      if (evidence.readiness) {
        Object.assign(readiness, evidence.readiness);
      }
    }
  }

  const googleAdsRows = adAccountsBeforeProbe.filter((row) => row.platform === 'google_ads');
  const mainAtOrAfterFloor = await shaAtOrAfter(mainSha, DEPLOY_FLOOR_SHA);
  const pass =
    loginStatus === 200
    && mainAtOrAfterFloor
    && missingGoogleEnv.length === 0
    && googleAdsRows.some((row) => row.is_active && row.has_refresh)
    && tenantIsolation.crossTenantLeak === false
    && readiness.accountStatus === 'READY'
    && readiness.credentialStatus === 'READY'
    && Boolean(readiness.currency)
    && Boolean(readiness.timezone);

  return {
    spec: 'SPEC-PENNY-GADS-003',
    deployment: {
      floorSha: DEPLOY_FLOOR_SHA,
      mainHeadSha: mainSha,
      mainAtOrAfterFloor,
      productionLoginUrl: PRODUCTION_LOGIN_URL,
      productionLoginStatus: loginStatus,
    },
    googleAdsEnv: {
      required: googleEnv,
      optional: optionalGoogleEnv,
      missingRequiredKeys: missingGoogleEnv,
    },
    anchorBindingEnv: {
      requiredForApplyBinding: bindingEnv,
    },
    apiVersion: versionInfo,
    adAccountsClient10: adAccountsBeforeProbe,
    binding: bindingResult,
    tenantIsolation,
    readiness,
    evidence: evidenceSummary,
    safety: {
      googleAdsMutateEndpointsUsed: false,
      note: 'Adapter uses OAuth token refresh and googleAds:search only; no googleAds:mutate in codebase.',
    },
    pass,
    blockers: pass
      ? []
      : [
        loginStatus !== 200 ? `production login HTTP ${loginStatus}` : null,
        !mainAtOrAfterFloor ? 'main HEAD before deploy floor SHA' : null,
        missingGoogleEnv.length ? `missing Google Ads env: ${missingGoogleEnv.join(', ')}` : null,
        !googleAdsRows.some((row) => row.is_active && row.has_refresh)
          ? 'no active google_ads ad_accounts row with refresh_token for client_id=10'
          : null,
        tenantIsolation.crossTenantLeak ? 'cross-tenant Google Ads account leak detected' : null,
        readiness.accountStatus !== 'READY' ? `accountStatus=${readiness.accountStatus}` : null,
        readiness.credentialStatus !== 'READY' ? `credentialStatus=${readiness.credentialStatus}` : null,
        !readiness.currency ? 'currency missing from identity read' : null,
        !readiness.timezone ? 'timezone missing from identity read' : null,
      ].filter(Boolean),
  };
}

module.exports = {
  CLIENT_ID,
  DEPLOY_FLOOR_SHA,
  parseArgs,
  envPresence,
  sameSha,
  compareStatusAtOrAfter,
  shaAtOrAfter,
  applyGoogleAdsBinding,
  run,
};

if (require.main === module) {
  const options = parseArgs();
  run(options)
    .then((report) => {
      if (report.help) return;
      console.log(JSON.stringify(report, null, 2));
      process.exitCode = report.pass ? 0 : 2;
    })
    .catch((err) => {
      console.log(JSON.stringify({
        spec: 'SPEC-PENNY-GADS-003',
        error: { code: err.code || null, message: err.message },
      }, null, 2));
      process.exitCode = 1;
    })
    .finally(() => pool.end());
}
