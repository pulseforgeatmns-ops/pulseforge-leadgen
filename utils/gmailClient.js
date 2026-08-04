'use strict';

/**
 * Read-only Gmail client for market-intel ingestion.
 * Reuses the same credential/token env vars as Riley without modifying Riley.
 */

const fs = require('fs');
const path = require('path');
const { google } = require('googleapis');

const CREDENTIALS_PATH = './gmail_credentials.json';
const TOKEN_PATH = './gmail_token.json';
const DOWNLOADS_CREDENTIALS_PATH = path.join(process.env.HOME || '.', 'Downloads', 'riley_credentials.json');
const SCOPES = ['https://www.googleapis.com/auth/gmail.readonly'];
const TOKEN_REFRESH_WINDOW_MS = 5 * 60 * 1000;

function parseJsonSource(label, raw) {
  if (!raw) return null;
  try {
    return JSON.parse(String(raw).trim());
  } catch (err) {
    throw new Error(`${label} is not valid JSON: ${err.message}`);
  }
}

function readFileIfPresent(filePath) {
  return fs.existsSync(filePath) ? fs.readFileSync(filePath, 'utf8') : null;
}

function tokenExpiresSoon(token) {
  const expiry = Number(token?.expiry_date || token?.expires_at || 0);
  if (!expiry) return true;
  return expiry <= Date.now() + TOKEN_REFRESH_WINDOW_MS;
}

function loadRileyEnvToken() {
  if (!process.env.RILEY_ACCESS_TOKEN && !process.env.RILEY_REFRESH_TOKEN) return null;
  return {
    access_token: process.env.RILEY_ACCESS_TOKEN || null,
    refresh_token: process.env.RILEY_REFRESH_TOKEN || null,
    expiry_date: Number(process.env.RILEY_TOKEN_EXPIRY || process.env.RILEY_ACCESS_TOKEN_EXPIRY || 0) || null,
    token_type: 'Bearer',
  };
}

async function refreshAccessToken({ client_id, client_secret, refresh_token }) {
  if (!refresh_token) throw new Error('Gmail refresh_token is missing');
  const res = await fetch('https://oauth2.googleapis.com/token', {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body: new URLSearchParams({
      client_id,
      client_secret,
      refresh_token,
      grant_type: 'refresh_token',
    }),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) {
    const details = data.error_description || data.error || res.statusText;
    throw new Error(`Google token refresh failed: ${details}`);
  }
  return {
    access_token: data.access_token,
    refresh_token,
    expiry_date: Date.now() + Number(data.expires_in || 3600) * 1000,
    token_type: data.token_type || 'Bearer',
    scope: data.scope,
  };
}

function loadOAuthCredentials() {
  const rawCreds = process.env.GMAIL_CREDENTIALS
    || readFileIfPresent(CREDENTIALS_PATH)
    || readFileIfPresent(DOWNLOADS_CREDENTIALS_PATH);

  if (rawCreds) {
    const credentials = parseJsonSource('GMAIL_CREDENTIALS', rawCreds);
    const credKeys = credentials.installed || credentials.web;
    if (!credKeys?.client_id || !credKeys?.client_secret) {
      throw new Error('GMAIL_CREDENTIALS must contain installed or web OAuth client JSON');
    }
    return credKeys;
  }

  if (process.env.GOOGLE_CLIENT_ID && process.env.GOOGLE_CLIENT_SECRET) {
    return {
      client_id: process.env.GOOGLE_CLIENT_ID,
      client_secret: process.env.GOOGLE_CLIENT_SECRET,
    };
  }

  throw new Error('Missing Gmail OAuth credentials. Set GMAIL_CREDENTIALS to the full JSON string.');
}

async function getAuthClient() {
  const credentials = loadOAuthCredentials();
  const { client_id, client_secret } = credentials;
  const oAuth2Client = new google.auth.OAuth2(client_id, client_secret, 'http://localhost:3001');

  const rileyEnvToken = loadRileyEnvToken();
  if (rileyEnvToken) {
    let token = rileyEnvToken;
    if (tokenExpiresSoon(token) && token.refresh_token) {
      token = await refreshAccessToken({
        client_id,
        client_secret,
        refresh_token: token.refresh_token,
      });
      process.env.RILEY_ACCESS_TOKEN = token.access_token;
      process.env.RILEY_TOKEN_EXPIRY = String(token.expiry_date);
    }
    oAuth2Client.setCredentials(token);
    return oAuth2Client;
  }

  const rawToken = process.env.GMAIL_TOKEN || readFileIfPresent(TOKEN_PATH);
  if (!rawToken) {
    throw new Error('Missing Gmail tokens. Set GMAIL_TOKEN or RILEY_ACCESS_TOKEN/RILEY_REFRESH_TOKEN.');
  }

  const token = parseJsonSource('GMAIL_TOKEN', rawToken);
  let latest = { ...token };
  if (token.refresh_token && tokenExpiresSoon(token)) {
    latest = await refreshAccessToken({
      client_id,
      client_secret,
      refresh_token: token.refresh_token,
    });
  }
  oAuth2Client.setCredentials(latest);
  return oAuth2Client;
}

async function createGmailClient(auth) {
  const client = auth || await getAuthClient();
  return google.gmail({ version: 'v1', auth: client });
}

/**
 * List message ids matching a Gmail search query, newest-first, capped by limit.
 */
async function listMessageIds(gmail, { query, limit = 1000, pageSize = 100 } = {}) {
  const ids = [];
  let pageToken;
  const max = Math.max(1, Number(limit) || 1000);

  while (ids.length < max) {
    const res = await gmail.users.messages.list({
      userId: 'me',
      q: query,
      maxResults: Math.min(pageSize, max - ids.length),
      pageToken,
    });
    for (const msg of res.data.messages || []) {
      if (msg.id) ids.push(msg.id);
      if (ids.length >= max) break;
    }
    pageToken = res.data.nextPageToken;
    if (!pageToken) break;
  }

  return ids;
}

async function getFullMessage(gmail, gmailId) {
  const res = await gmail.users.messages.get({
    userId: 'me',
    id: gmailId,
    format: 'full',
  });
  return res.data;
}

async function fetchLabeledMessages({
  query,
  limit = 1000,
  gmail = null,
  onProgress = null,
} = {}) {
  const client = gmail || await createGmailClient();
  const ids = await listMessageIds(client, { query, limit });
  const messages = [];
  for (let i = 0; i < ids.length; i += 1) {
    const full = await getFullMessage(client, ids[i]);
    messages.push(full);
    if (typeof onProgress === 'function') onProgress({ fetched: i + 1, total: ids.length, gmailId: ids[i] });
  }
  return messages;
}

/**
 * List Gmail labels for the authenticated mailbox (read-only).
 */
async function listGmailLabels(gmail = null) {
  const client = gmail || await createGmailClient();
  const res = await client.users.labels.list({ userId: 'me' });
  return res.data.labels || [];
}

/**
 * Exact-match label lookup by display name (case-sensitive, Gmail label names are exact).
 */
function findLabelByName(labels, labelName) {
  const wanted = String(labelName || '').trim();
  if (!wanted) return null;
  return (labels || []).find((label) => label && label.name === wanted) || null;
}

/**
 * Count discoverable message ids for a query without fetching full bodies.
 */
async function countMatchingMessages({
  query,
  limit = 1000,
  gmail = null,
} = {}) {
  const client = gmail || await createGmailClient();
  const ids = await listMessageIds(client, { query, limit });
  return {
    query,
    discoveredCount: ids.length,
    cappedByLimit: ids.length >= Math.max(1, Number(limit) || 1000),
    sampleIds: ids.slice(0, 5),
  };
}

module.exports = {
  SCOPES,
  countMatchingMessages,
  createGmailClient,
  fetchLabeledMessages,
  findLabelByName,
  getAuthClient,
  getFullMessage,
  listGmailLabels,
  listMessageIds,
  loadOAuthCredentials,
};
