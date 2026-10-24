#!/usr/bin/env node
'use strict';

/**
 * Audit Anchor Google Ads business logo sources (read-only by default).
 * Optional upload links a canonical square logo as account-level LOGO customer asset.
 *
 * Railway (requires GOOGLE_ADS_CLIENT_ID, GOOGLE_ADS_CLIENT_SECRET, optional DEVELOPER_TOKEN):
 *   node scripts/auditAndReplaceAnchorGoogleAdsLogo.js --confirm-production
 *   node scripts/auditAndReplaceAnchorGoogleAdsLogo.js --confirm-production --upload
 *
 * Uses tenant client_id=10 refresh_token from ad_accounts when ANCHOR_GOOGLE_ADS_REFRESH_TOKEN is unset.
 */

require('dotenv').config({ quiet: true });

const fs = require('node:fs');
const path = require('node:path');
const axios = require('axios');
const pool = require('../db');
const {
  googleAds,
  resolveAdAccountsForClient,
  PLATFORM,
} = require('../packages/penny-paid-acquisition');

const {
  resolveGoogleAdsApiVersion,
  requiredGoogleAdsEnv,
  googleAdsDeveloperTokenWarnings,
  gaqlSearchAll,
  googleAdsToken,
  googleAdsHeaders,
} = googleAds;

const CLIENT_ID = 10;
const LOGO_VERSION = '20260930';
const LOGO_FILENAME = `google-ads-business-logo-v${LOGO_VERSION}.png`;
const LOGO_PATH = path.join(__dirname, '../sites/anchor-cleaning/assets/brand', LOGO_FILENAME);

const WEBSITE_LOGO_CANDIDATES = Object.freeze([
  'https://goanchorcleaning.com/assets/brand/favicon-32x32-v20260916.png?v=20260916',
  'https://goanchorcleaning.com/assets/brand/apple-touch-icon-v20260916.png?v=20260916',
  'https://goanchorcleaning.com/assets/brand/icon-512-v20260916.png?v=20260916',
  'https://goanchorcleaning.com/assets/brand/social-avatar-v20260916.png?v=20260916',
  'https://goanchorcleaning.com/assets/brand/anchor-logo-canonical.png?v=20260916',
]);

function parseArgs(argv = process.argv.slice(2)) {
  const confirmProduction = argv.includes('--confirm-production');
  const upload = argv.includes('--upload');
  const help = argv.includes('--help') || argv.includes('-h');
  const unknown = argv.filter(
    (arg) => !['--confirm-production', '--upload', '--help', '-h'].includes(arg),
  );
  if (unknown.length) throw new Error(`Unknown argument(s): ${unknown.join(', ')}`);
  return { confirmProduction, upload, help };
}

function printUsage() {
  console.log(`Anchor Google Ads business logo audit / replace (client_id=${CLIENT_ID})

Usage:
  node scripts/auditAndReplaceAnchorGoogleAdsLogo.js --confirm-production
  node scripts/auditAndReplaceAnchorGoogleAdsLogo.js --confirm-production --upload

Safety:
  Refuses without --confirm-production.
  Default is read-only GAQL audit.
  --upload creates an IMAGE asset and links customer_asset.field_type=LOGO (does not change bids/targeting).
`);
}

function normalizeCustomerId(raw) {
  return String(raw || '').trim().replace(/-/g, '');
}

function pick(row, ...keys) {
  for (const key of keys) {
    if (row[key] != null) return row[key];
  }
  return null;
}

function assetImageUrl(asset = {}) {
  const image = asset.imageAsset || asset.image_asset || {};
  const full = image.fullSize || image.full_size || {};
  return pick(full, 'url') || pick(image, 'previewSize', 'preview_size', 'url') || null;
}

function classifyLogoSource(asset = {}, customerAsset = null) {
  const source = String(pick(asset, 'source') || '').toUpperCase();
  const fieldType = String(pick(customerAsset, 'fieldType', 'field_type') || '').toUpperCase();
  if (source.includes('AUTOMATICALLY') || source === 'AUTOMATICALLY_CREATED') {
    return 'automatically_created_asset';
  }
  if (fieldType === 'LOGO') return 'account_customer_asset_logo';
  const type = String(pick(asset, 'type') || '').toUpperCase();
  if (type === 'LOGO') return 'google_ads_logo_asset';
  if (type === 'IMAGE') return 'campaign_or_library_image_asset';
  return 'unknown';
}

async function loadGoogleAccount() {
  const accounts = await resolveAdAccountsForClient({
    clientId: CLIENT_ID,
    platform: PLATFORM.GOOGLE_ADS,
    pool,
  });
  const account = (accounts || []).find((row) => row.platform === 'google_ads' && row.is_active);
  if (!account) {
    const err = new Error('No active google_ads ad_accounts row for client_id=10.');
    err.code = 'missing_google_ads_account';
    throw err;
  }
  if (!account.refresh_token && !process.env.ANCHOR_GOOGLE_ADS_REFRESH_TOKEN) {
    const err = new Error('Missing refresh_token on ad_accounts and ANCHOR_GOOGLE_ADS_REFRESH_TOKEN.');
    err.code = 'missing_refresh_token';
    throw err;
  }
  return {
    ...account,
    refresh_token: account.refresh_token || process.env.ANCHOR_GOOGLE_ADS_REFRESH_TOKEN,
  };
}

async function auditLogoAssets(account, token, apiVersion, http) {
  const customerId = normalizeCustomerId(account.account_id);

  const customerAssets = await gaqlSearchAll(customerId, `
    SELECT
      customer_asset.resource_name,
      customer_asset.field_type,
      customer_asset.status,
      asset.id,
      asset.name,
      asset.type,
      asset.source,
      asset.image_asset.full_size.url,
      asset.image_asset.full_size.width_pixels,
      asset.image_asset.full_size.height_pixels
    FROM customer_asset
    WHERE customer_asset.field_type IN ('LOGO', 'BUSINESS_LOGO')
  `, token, http, { apiVersion, account });

  const logoFieldAssets = await gaqlSearchAll(customerId, `
    SELECT
      asset.id,
      asset.name,
      asset.type,
      asset.source,
      asset.image_asset.full_size.url,
      asset.image_asset.full_size.width_pixels,
      asset.image_asset.full_size.height_pixels
    FROM asset
    WHERE asset.type = LOGO
  `, token, http, { apiVersion, account });

  const autoAssets = await gaqlSearchAll(customerId, `
    SELECT
      asset.id,
      asset.name,
      asset.type,
      asset.source,
      asset.image_asset.full_size.url
    FROM asset
    WHERE asset.source = AUTOMATICALLY_CREATED
      AND asset.type IN (IMAGE, LOGO)
  `, token, http, { apiVersion, account });

  const assetGroupLogos = await gaqlSearchAll(customerId, `
    SELECT
      campaign.name,
      asset_group.name,
      asset_group_asset.field_type,
      asset.id,
      asset.name,
      asset.source,
      asset.image_asset.full_size.url
    FROM asset_group_asset
    WHERE asset_group_asset.field_type IN ('LOGO', 'BUSINESS_LOGO')
      AND campaign.status IN ('ENABLED', 'PAUSED')
  `, token, http, { apiVersion, account });

  const rows = [];
  for (const row of customerAssets) {
    const asset = row.asset || {};
    rows.push({
      layer: 'account_customer_asset',
      fieldType: pick(row.customerAsset || row.customer_asset, 'fieldType', 'field_type'),
      status: pick(row.customerAsset || row.customer_asset, 'status'),
      assetId: asset.id,
      assetName: asset.name,
      assetType: asset.type,
      source: asset.source,
      url: assetImageUrl(asset),
      classification: classifyLogoSource(asset, row.customerAsset || row.customer_asset),
    });
  }
  for (const row of logoFieldAssets) {
    const asset = row.asset || {};
    rows.push({
      layer: 'asset_library_logo_type',
      assetId: asset.id,
      assetName: asset.name,
      assetType: asset.type,
      source: asset.source,
      url: assetImageUrl(asset),
      classification: classifyLogoSource(asset),
    });
  }
  for (const row of autoAssets) {
    const asset = row.asset || {};
    rows.push({
      layer: 'automatically_created_asset',
      assetId: asset.id,
      assetName: asset.name,
      assetType: asset.type,
      source: asset.source,
      url: assetImageUrl(asset),
      classification: 'automatically_created_asset',
    });
  }
  for (const row of assetGroupLogos) {
    const asset = row.asset || {};
    rows.push({
      layer: 'campaign_asset_group',
      campaign: pick(row.campaign, 'name'),
      assetGroup: pick(row.assetGroup || row.asset_group, 'name'),
      fieldType: pick(row.assetGroupAsset || row.asset_group_asset, 'fieldType', 'field_type'),
      assetId: asset.id,
      assetName: asset.name,
      source: asset.source,
      url: assetImageUrl(asset),
      classification: classifyLogoSource(asset),
    });
  }

  const primary = rows.find((r) => r.layer === 'account_customer_asset' && String(r.fieldType).toUpperCase() === 'LOGO')
    || rows.find((r) => r.classification === 'automatically_created_asset')
    || rows[0]
    || null;

  return {
    customerId,
    websiteLogoCandidates: WEBSITE_LOGO_CANDIDATES,
    assets: rows,
    inferredDisplaySource: primary,
    notes: [
      'Definitive Search-ad logo source is the linked LOGO customer_asset when present.',
      'If only automatically_created_asset rows exist, Google likely derived the mark from the website favicon / structured data.',
      'Compare inferredDisplaySource.url against websiteLogoCandidates when auditing favicon-derived marks.',
    ],
  };
}

async function mutateAssets(customerId, operations, token, apiVersion, account, http) {
  const res = await http.post(
    `https://googleads.googleapis.com/${apiVersion}/customers/${customerId}/assets:mutate`,
    { operations },
    { headers: googleAdsHeaders(token, account) },
  );
  return res.data;
}

async function mutateCustomerAssets(customerId, operations, token, apiVersion, account, http) {
  const res = await http.post(
    `https://googleads.googleapis.com/${apiVersion}/customers/${customerId}/customerAssets:mutate`,
    { operations },
    { headers: googleAdsHeaders(token, account) },
  );
  return res.data;
}

async function uploadCanonicalLogo(account, token, apiVersion, http) {
  if (!fs.existsSync(LOGO_PATH)) {
    const err = new Error(`Missing logo file: ${LOGO_PATH}. Run generate-google-ads-business-logo.mjs first.`);
    err.code = 'missing_logo_file';
    throw err;
  }
  const customerId = normalizeCustomerId(account.account_id);
  const bytes = fs.readFileSync(LOGO_PATH);
  const assetName = `Anchor Cleaning — Google Ads business logo (canonical ${LOGO_VERSION})`;

  const assetResult = await mutateAssets(customerId, [{
    create: {
      name: assetName,
      type: 'IMAGE',
      imageAsset: {
        mimeType: 'IMAGE_PNG',
        data: bytes.toString('base64'),
      },
    },
  }], token, apiVersion, account, http);

  const resourceName = assetResult.results?.[0]?.resourceName;
  if (!resourceName) {
    throw new Error('Asset mutate succeeded but no resourceName returned.');
  }

  const linkResult = await mutateCustomerAssets(customerId, [{
    create: {
      asset: resourceName,
      fieldType: 'LOGO',
    },
  }], token, apiVersion, account, http);

  return {
    assetResourceName: resourceName,
    customerAsset: linkResult.results?.[0]?.resourceName || null,
    localFile: LOGO_PATH,
    recommendation: 'In Google Ads UI → Account settings → Business information, confirm the new logo preview. Remove/disable unwanted automatically created logo assets if Google still prefers them.',
  };
}

async function main() {
  const args = parseArgs();
  if (args.help) {
    printUsage();
    return;
  }
  if (!args.confirmProduction) {
    console.error('Refusing: pass --confirm-production');
    printUsage();
    process.exit(1);
  }

  const versionInfo = resolveGoogleAdsApiVersion();
  const missingEnv = requiredGoogleAdsEnv();
  const report = {
    spec: 'ANCHOR-GOOGLE-ADS-LOGO',
    clientId: CLIENT_ID,
    apiVersion: versionInfo.version,
    missingEnv,
    warnings: googleAdsDeveloperTokenWarnings(),
    websiteLogoCandidates: WEBSITE_LOGO_CANDIDATES,
    logoFile: LOGO_PATH,
    uploadRequested: args.upload,
  };

  if (missingEnv.length) {
    report.status = 'blocked_missing_oauth_env';
    report.nextAction = `Set ${missingEnv.join(', ')} on Railway, then re-run. Refresh token is already stored on ad_accounts for client_id=10.`;
    console.log(JSON.stringify(report, null, 2));
    process.exit(2);
  }

  const account = await loadGoogleAccount();
  const token = await googleAdsToken(account.refresh_token, axios);
  report.audit = await auditLogoAssets(account, token, versionInfo.version, axios);

  if (args.upload) {
    report.upload = await uploadCanonicalLogo(account, token, versionInfo.version, axios);
    report.status = 'uploaded';
  } else {
    report.status = 'audit_complete';
    report.nextAction = 'Re-run with --upload after reviewing audit.inferredDisplaySource.';
  }

  console.log(JSON.stringify(report, null, 2));
}

main()
  .catch((err) => {
    console.error(JSON.stringify({ error: err.message, code: err.code || 'failed' }, null, 2));
    process.exit(1);
  })
  .finally(() => pool.end().catch(() => {}));
