'use strict';

// Explicit operator-owned bindings. Secrets remain in env variables, never in artifacts.
const PROVIDERS = { linkedin_page: 'buffer', linkedin_personal: 'buffer', facebook_page: 'facebook', google_business: 'google_business' };
function configuredAccounts(env = process.env) {
  let rows;
  try { rows = JSON.parse(env.PAIGE_SOCIAL_ACCOUNTS || '[]'); } catch { throw new Error('social_accounts_invalid'); }
  if (!Array.isArray(rows)) throw new Error('social_accounts_invalid');
  const keys = new Set();
  const destinations = new Set();
  return rows.map(a => {
    if (!a.id || !Number.isInteger(a.clientId) || a.clientId < 1 || (!PROVIDERS[a.platform] || PROVIDERS[a.platform] !== a.provider) || !a.externalAccountId) throw new Error('social_account_invalid');
    if (keys.has(a.id)) throw new Error('social_account_duplicate');
    keys.add(a.id);
    const destination = `${a.provider}:${a.externalAccountId}`;
    if (a.enabled !== false && destinations.has(destination)) throw new Error('social_account_destination_duplicate');
    if (a.enabled !== false) destinations.add(destination);
    if (a.provider === 'facebook' && (!/^\d+$/.test(a.externalAccountId) || !/^v\d+\.\d+$/.test(a.apiVersion || ''))) throw new Error('facebook_account_invalid');
    if (a.provider === 'google_business' && !/^accounts\/[^/]+\/locations\/[^/]+$/.test(a.externalAccountId)) throw new Error('google_business_account_invalid');
    return a;
  });
}
function publicAccount(a) {
  return { id: a.id, clientId: a.clientId, platform: a.platform, provider: a.provider,
    externalAccountId: String(a.externalAccountId), apiVersion: a.apiVersion || null };
}
function listSocialAccounts(clientId, env = process.env) {
  return configuredAccounts(env).filter(a => a.enabled !== false && a.clientId === Number(clientId)).map(publicAccount);
}
function resolveSocialAccount({ tenantId, clientId, platform, accountId }, env = process.env) {
  if (!Number.isInteger(Number(clientId)) || Number(clientId) < 1 || String(tenantId) !== String(clientId)) throw new Error('tenant_scope_required');
  const rows = configuredAccounts(env).filter(a => a.enabled !== false && a.clientId === Number(clientId) && a.platform === platform && (!accountId || a.id === accountId));
  if (rows.length !== 1) throw new Error(rows.length ? 'social_account_selection_required' : 'social_account_not_connected');
  const raw = rows[0];
  const credentials = {};
  const fields = raw.provider === 'google_business' ? ['clientId', 'clientSecret', 'refreshToken'] : ['accessToken'];
  for (const field of fields) {
    const ref = raw.credentialEnv?.[field];
    if (!ref || !/^[A-Z][A-Z0-9_]+$/.test(ref) || !env[ref]) throw new Error('social_account_credentials_missing');
    credentials[field] = env[ref];
  }
  return { account: publicAccount(raw), credentials };
}
module.exports = { listSocialAccounts, resolveSocialAccount };
