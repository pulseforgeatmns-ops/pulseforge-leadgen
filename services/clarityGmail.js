'use strict';

const crypto = require('node:crypto');

const GMAIL_READONLY_SCOPE = 'https://www.googleapis.com/auth/gmail.readonly';
const PILOT_CLIENT_ID = 10;
const PILOT_MAILBOX = 'jacob@goanchorcleaning.com';
const GMAIL_BASE = 'https://gmail.googleapis.com/gmail/v1/users/me';
const OAUTH_TOKEN_URL = 'https://oauth2.googleapis.com/token';
const MAX_REQUEST_MS = 15000;

function clarityError(code, { retryable = false, httpStatus } = {}) {
  const error = new Error(code); // Provider response text may contain secrets. Never copy it.
  error.code = code;
  error.retryable = retryable;
  if (httpStatus) error.httpStatus = httpStatus;
  return error;
}

function assertClientId(clientId) {
  if (!Number.isSafeInteger(clientId) || clientId !== PILOT_CLIENT_ID) throw clarityError('clarity_tenant_not_authorized');
  return clientId;
}

function normalizeEmail(email) {
  const value = typeof email === 'string' ? email.trim().toLowerCase() : '';
  if (!/^[a-z0-9.!#$%&'*+/=?^_`{|}~-]+@[a-z0-9.-]+\.[a-z]{2,}$/i.test(value)) throw clarityError('clarity_email_invalid');
  return value;
}

function validateSender(sender) {
  const value = normalizeEmail(sender);
  // The operator must verify this exact address against an authentic digest.
  if (!/^[a-z0-9._+-]+@microsoft\.com$/.test(value)) throw clarityError('clarity_digest_sender_invalid');
  return value;
}

function assertScope(scopes) {
  const list = Array.isArray(scopes) ? scopes : String(scopes || '').trim().split(/\s+/);
  if (list.length !== 1 || list[0] !== GMAIL_READONLY_SCOPE) throw clarityError('clarity_scope_not_readonly');
  return [GMAIL_READONLY_SCOPE];
}

function oauthClient(env) {
  if (!env.CLARITY_GOOGLE_CLIENT_ID || !env.CLARITY_GOOGLE_CLIENT_SECRET) throw clarityError('clarity_oauth_client_missing');
  return { clientId: env.CLARITY_GOOGLE_CLIENT_ID, clientSecret: env.CLARITY_GOOGLE_CLIENT_SECRET };
}

function encryptionKey(env) {
  const encoded = env.CLARITY_TOKEN_ENCRYPTION_KEY;
  if (typeof encoded !== 'string' || !/^[A-Za-z0-9+/]{43}=$/.test(encoded)) throw clarityError('clarity_encryption_key_missing');
  const key = Buffer.from(encoded, 'base64');
  if (key.length !== 32) throw clarityError('clarity_encryption_key_invalid');
  return key;
}

function binding(integration) {
  assertClientId(integration?.client_id);
  const mailbox = normalizeEmail(integration.mailbox_email);
  if (mailbox !== PILOT_MAILBOX) throw clarityError('clarity_mailbox_not_authorized');
  assertScope(integration.oauth_scopes);
  if (integration.consent_status !== 'authorized' || !integration.consented_at || Number.isNaN(Date.parse(integration.consented_at))) throw clarityError('clarity_consent_missing');
  if (!integration.oauth_client_id) throw clarityError('clarity_oauth_client_mismatch');
  if (typeof integration.project_id !== 'string' || !/^[A-Za-z0-9]{3,64}$/.test(integration.project_id)) throw clarityError('clarity_project_id_missing');
  return JSON.stringify({ version: 1, client_id: integration.client_id, mailbox_email: mailbox, digest_sender: validateSender(integration.digest_sender), project_id: integration.project_id, oauth_scopes: [GMAIL_READONLY_SCOPE], consented_at: new Date(integration.consented_at).toISOString(), oauth_client_id: integration.oauth_client_id });
}

function grantFingerprint(integration) { return crypto.createHash('sha256').update(`${binding(integration)}:${integration.token_key_id}:${integration.refresh_token_encrypted}`).digest('hex'); }

function encryptRefreshToken(refreshToken, integration, env = process.env) {
  if (typeof refreshToken !== 'string' || refreshToken.length < 8 || refreshToken.length > 16384) throw clarityError('clarity_refresh_token_invalid');
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', encryptionKey(env), iv);
  cipher.setAAD(Buffer.from(binding(integration)));
  const ciphertext = Buffer.concat([cipher.update(refreshToken, 'utf8'), cipher.final()]);
  return ['v1', iv.toString('base64url'), cipher.getAuthTag().toString('base64url'), ciphertext.toString('base64url')].join('.');
}

function decryptRefreshToken(integration, env = process.env) {
  const aad = binding(integration);
  if (integration.token_key_id !== (env.CLARITY_TOKEN_KEY_ID || 'v1')) throw clarityError('clarity_encryption_key_version');
  const key = encryptionKey(env);
  try {
    const [version, iv, tag, ciphertext, extra] = String(integration.refresh_token_encrypted || '').split('.');
    if (version !== 'v1' || extra || !iv || !tag || !ciphertext) throw new Error('invalid envelope');
    const decipher = crypto.createDecipheriv('aes-256-gcm', key, Buffer.from(iv, 'base64url'));
    decipher.setAAD(Buffer.from(aad));
    decipher.setAuthTag(Buffer.from(tag, 'base64url'));
    return Buffer.concat([decipher.update(Buffer.from(ciphertext, 'base64url')), decipher.final()]).toString('utf8');
  } catch (_) { throw clarityError('clarity_token_decryption_failed'); }
}

function assertIntegration({ clientId, integration, env, requireEnabled = true }) {
  assertClientId(clientId);
  if (!integration || integration.client_id !== clientId) throw clarityError('clarity_tenant_mismatch');
  if (requireEnabled && (env.CLARITY_ENABLED !== 'true' || env.CLARITY_INGESTION_ENABLED !== 'true' || integration.enabled !== true)) throw clarityError('clarity_ingestion_disabled');
  binding(integration);
  if (integration.oauth_client_id !== oauthClient(env).clientId) throw clarityError('clarity_oauth_client_mismatch');
}

async function requestJson(fetchImpl, url, options = {}) {
  let response;
  try {
    response = await fetchImpl(url, { ...options, redirect: 'error', signal: AbortSignal.timeout(MAX_REQUEST_MS) });
  } catch (_) { throw clarityError('clarity_google_network_error', { retryable: true }); }
  let data;
  try { data = await response.json(); } catch (_) { throw clarityError('clarity_google_response_invalid', { retryable: response.status >= 500 }); }
  if (!response.ok) {
    const invalidPageToken = response.status === 400 && Array.isArray(data?.error?.errors)
      && data.error.errors.some((entry) => ['invalidArgument', 'invalidParameter'].includes(entry.reason) && entry.location === 'pageToken');
    const code = invalidPageToken ? 'clarity_page_token_rejected' : data?.error === 'invalid_grant' ? 'clarity_oauth_reauthorization_required' : 'clarity_google_request_failed';
    throw clarityError(code, { retryable: response.status === 429 || response.status >= 500, httpStatus: response.status });
  }
  return data;
}

function validateRedirectUri(redirectUri) {
  let uri;
  try { uri = new URL(redirectUri); } catch (_) { throw clarityError('clarity_redirect_invalid'); }
  if (uri.protocol !== 'http:' || uri.hostname !== '127.0.0.1' || uri.pathname !== '/oauth/callback' || uri.search || uri.hash || uri.username || uri.password || !uri.port) throw clarityError('clarity_redirect_invalid');
  return uri.toString();
}

/** Builds only a URL and ephemeral state. It neither grants access nor calls Google. */
function createOAuthAuthorization({ clientId, mailboxEmail, digestSender, projectId, redirectUri, env = process.env, now = Date.now() }) {
  assertClientId(clientId);
  if (normalizeEmail(mailboxEmail) !== PILOT_MAILBOX) throw clarityError('clarity_mailbox_not_authorized');
  const sender = validateSender(digestSender);
  if (typeof projectId !== 'string' || !/^[A-Za-z0-9]{3,64}$/.test(projectId)) throw clarityError('clarity_project_id_missing');
  const client = oauthClient(env);
  encryptionKey(env); // Fail before requesting consent if storage cannot be protected.
  const pending = { clientId, mailboxEmail: PILOT_MAILBOX, digestSender: sender, projectId, redirectUri: validateRedirectUri(redirectUri), oauthClientId: client.clientId, state: crypto.randomBytes(32).toString('base64url'), codeVerifier: crypto.randomBytes(48).toString('base64url'), createdAt: Number(now), used: false };
  const params = new URLSearchParams({ client_id: client.clientId, redirect_uri: pending.redirectUri, response_type: 'code', scope: GMAIL_READONLY_SCOPE, access_type: 'offline', prompt: 'consent select_account', login_hint: PILOT_MAILBOX, include_granted_scopes: 'false', state: pending.state, code_challenge: crypto.createHash('sha256').update(pending.codeVerifier).digest('base64url'), code_challenge_method: 'S256' });
  return { authorizationUrl: `https://accounts.google.com/o/oauth2/v2/auth?${params}`, pending };
}

/** Called only by the explicit operator-run consent command; does not enable polling. */
async function completeOAuthAuthorization({ pending, code, state, store, env = process.env, fetchImpl = global.fetch, now = Date.now() }) {
  const elapsed = Number(now) - Number(pending?.createdAt);
  if (!pending || pending.used || elapsed < 0 || elapsed > 10 * 60 * 1000 || typeof state !== 'string' || Buffer.byteLength(state) !== Buffer.byteLength(pending.state) || !crypto.timingSafeEqual(Buffer.from(state), Buffer.from(pending.state))) throw clarityError('clarity_oauth_state_invalid');
  if (typeof code !== 'string' || !code || code.length > 4096) throw clarityError('clarity_oauth_code_invalid');
  pending.used = true;
  assertClientId(pending.clientId);
  const client = oauthClient(env);
  if (client.clientId !== pending.oauthClientId) throw clarityError('clarity_oauth_client_mismatch');
  const tokens = await requestJson(fetchImpl, OAUTH_TOKEN_URL, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ client_id: client.clientId, client_secret: client.clientSecret, code, code_verifier: pending.codeVerifier, redirect_uri: pending.redirectUri, grant_type: 'authorization_code' }) });
  assertScope(tokens.scope);
  if (!tokens.access_token || !tokens.refresh_token) throw clarityError('clarity_oauth_refresh_token_missing');
  const profile = await requestJson(fetchImpl, `${GMAIL_BASE}/profile`, { headers: { Authorization: `Bearer ${tokens.access_token}` } });
  if (normalizeEmail(profile.emailAddress) !== pending.mailboxEmail) throw clarityError('clarity_mailbox_mismatch');
  const integration = { client_id: pending.clientId, mailbox_email: pending.mailboxEmail, digest_sender: pending.digestSender, project_id: pending.projectId, oauth_client_id: client.clientId, oauth_scopes: [GMAIL_READONLY_SCOPE], consent_status: 'authorized', consented_at: new Date(now).toISOString(), enabled: false, token_key_id: env.CLARITY_TOKEN_KEY_ID || 'v1' };
  integration.refresh_token_encrypted = encryptRefreshToken(tokens.refresh_token, integration, env);
  await store.saveIntegration({ clientId: pending.clientId, integration });
  return { client_id: pending.clientId, mailbox_email: pending.mailboxEmail, consent_status: 'authorized', enabled: false };
}

function createClarityGmail({ env = process.env, fetchImpl = global.fetch, now = () => Date.now() } = {}) {
  const cache = new Map();
  async function accessToken(clientId, integration) {
    assertIntegration({ clientId, integration, env });
    const key = grantFingerprint(integration);
    const cached = cache.get(key);
    if (cached?.expiresAt > now() + 60000) return cached.token;
    const client = oauthClient(env);
    const tokens = await requestJson(fetchImpl, OAUTH_TOKEN_URL, { method: 'POST', headers: { 'Content-Type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ client_id: client.clientId, client_secret: client.clientSecret, refresh_token: decryptRefreshToken(integration, env), grant_type: 'refresh_token' }) });
    // Google can omit scope on refresh; the authenticated encrypted original
    // grant remains authoritative. If returned, broader scopes fail closed.
    if (tokens.scope !== undefined) assertScope(tokens.scope);
    if (!tokens.access_token) throw clarityError('clarity_oauth_access_token_missing');
    const profile = await requestJson(fetchImpl, `${GMAIL_BASE}/profile`, { headers: { Authorization: `Bearer ${tokens.access_token}` } });
    if (normalizeEmail(profile.emailAddress) !== integration.mailbox_email) throw clarityError('clarity_mailbox_mismatch');
    const expiresIn = Math.min(3600, Math.max(0, Number(tokens.expires_in) || 0));
    if (cache.size >= 10) cache.clear();
    cache.set(key, { token: tokens.access_token, expiresAt: now() + expiresIn * 1000 });
    return tokens.access_token;
  }
  return {
    async listMessages({ clientId, integration, after, before, maxResults = 25, pageToken }) {
      assertIntegration({ clientId, integration, env });
      const from = new Date(after).getTime();
      const until = new Date(before).getTime();
      if (!Number.isFinite(from) || !Number.isFinite(until) || until <= from || until - from > 90 * 86400000 || !Number.isSafeInteger(maxResults) || maxResults < 1 || maxResults > 25) throw clarityError('clarity_search_bounds_invalid');
      if (pageToken !== undefined && (typeof pageToken !== 'string' || pageToken.length > 2048)) throw clarityError('clarity_page_token_invalid');
      const q = `from:${validateSender(integration.digest_sender)} subject:Clarity after:${Math.floor(from / 1000)} before:${Math.ceil(until / 1000)}`;
      const params = new URLSearchParams({ q, maxResults: String(maxResults), includeSpamTrash: 'false' });
      if (pageToken) params.set('pageToken', pageToken);
      const token = await accessToken(clientId, integration);
      const result = await requestJson(fetchImpl, `${GMAIL_BASE}/messages?${params}`, { headers: { Authorization: `Bearer ${token}` } });
      return { messages: Array.isArray(result.messages) ? result.messages.slice(0, maxResults) : [], nextPageToken: result.nextPageToken || null };
    },
    async getMessage({ clientId, integration, messageId }) {
      assertIntegration({ clientId, integration, env });
      if (typeof messageId !== 'string' || !/^[A-Za-z0-9_-]{1,128}$/.test(messageId)) throw clarityError('clarity_message_id_invalid');
      const token = await accessToken(clientId, integration);
      return requestJson(fetchImpl, `${GMAIL_BASE}/messages/${encodeURIComponent(messageId)}?format=full`, { headers: { Authorization: `Bearer ${token}` } });
    },
  };
}

module.exports = { GMAIL_READONLY_SCOPE, PILOT_CLIENT_ID, PILOT_MAILBOX, clarityError, assertClientId, assertScope, assertIntegration, grantFingerprint, encryptRefreshToken, decryptRefreshToken, createOAuthAuthorization, completeOAuthAuthorization, createClarityGmail };
