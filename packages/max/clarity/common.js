'use strict';
const crypto = require('node:crypto');
function failure(code, statusCode = 400) { return Object.assign(new Error(code), { code, statusCode }); }
function tenant(value) {
  if (!/^\d+$/.test(String(value)) || !Number.isSafeInteger(Number(value)) || Number(value) <= 0) throw failure('clarity_tenant_required');
  return Number(value);
}
function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object') return Object.fromEntries(Object.keys(value).sort().map(k => [k, stable(value[k])]));
  return value;
}
function digest(value) { return crypto.createHash('sha256').update(JSON.stringify(stable(value))).digest('hex'); }
function enabled(env = process.env) { return env.CLARITY_ENABLED === 'true'; }
function id(value) { if (!/^[a-f0-9-]{36}$/i.test(String(value))) throw failure('clarity_invalid_id'); return value; }
module.exports = { failure, tenant, stable, digest, enabled, id };
