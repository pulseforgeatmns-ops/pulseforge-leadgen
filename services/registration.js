'use strict';

/**
 * SPEC-115 — Client registration and workspace-first provisioning.
 *
 * Workspace is created first. The user then belongs to that workspace.
 * Intelligence namespaces start empty. Verification is required before login.
 */

const crypto = require('crypto');
const bcrypt = require('bcryptjs');
const defaultPool = require('../db');
const {
  createAndProvisionTenant,
  createMemoryTenantStore,
  getTenantWorkspace,
  activateTenant,
  ensureTenantWorkspaceSchema,
  publicTenant,
  publicWorkspace,
} = require('./tenantWorkspace');
const { assertCanonicalBusinessVertical } = require('../utils/canonicalVerticals');
const { LIFECYCLE, publicLifecycle } = require('./workspaceLifecycle');
const { buildRegistrationGreeting } = require('../packages/max/workspace/TenantContextResolver');

const ACCOUNT_REQUIRED = ['name', 'email', 'password'];
const WORKSPACE_REQUIRED = ['companyName', 'vertical', 'country', 'timezone'];
const TOKEN_TTL_MS = 48 * 60 * 60 * 1000;
const MIN_PASSWORD = 8;

function asText(value, max = 500) {
  if (value == null) return '';
  return String(value).trim().slice(0, max);
}

function hashToken(token) {
  return crypto.createHash('sha256').update(String(token), 'utf8').digest('hex');
}

function newToken() {
  return crypto.randomBytes(32).toString('hex');
}

function validationError(message, missing) {
  const err = new Error(message);
  err.code = 'registration_validation';
  err.status = 400;
  if (missing) err.missing = missing;
  return err;
}

function validateAccountInput(raw = {}) {
  const input = {
    name: asText(raw.name || raw.fullName || raw.full_name, 200),
    email: asText(raw.email, 200).toLowerCase(),
    password: raw.password != null ? String(raw.password) : '',
    phone: asText(raw.phone, 40) || null,
  };
  const missing = ACCOUNT_REQUIRED.filter((key) => !input[key]);
  if (missing.length) {
    throw validationError(`Missing required account fields: ${missing.join(', ')}`, missing);
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(input.email)) {
    throw validationError('Email is invalid');
  }
  if (input.password.length < MIN_PASSWORD) {
    throw validationError(`Password must be at least ${MIN_PASSWORD} characters`);
  }
  return input;
}

function validateWorkspaceInput(raw = {}) {
  const input = {
    companyName: asText(raw.companyName || raw.company_name || raw.workspaceName, 200),
    vertical: asText(raw.vertical || raw.industry, 120),
    country: asText(raw.country, 80),
    timezone: asText(raw.timezone || raw.time_zone, 80),
    website: asText(raw.website, 300) || null,
    logoUrl: asText(raw.logoUrl || raw.logo_url || raw.logo, 500) || null,
    teamSize: asText(raw.teamSize || raw.team_size, 80) || null,
  };
  const missing = WORKSPACE_REQUIRED.filter((key) => !input[key]);
  if (missing.length) {
    throw validationError(`Missing required workspace fields: ${missing.join(', ')}`, missing);
  }
  try {
    input.vertical = assertCanonicalBusinessVertical(input.vertical);
  } catch (err) {
    if (err.code === 'tenant_validation') {
      throw validationError(err.message);
    }
    throw err;
  }
  return input;
}

function publicUser(user) {
  if (!user) return null;
  return {
    id: Number(user.id),
    name: user.name,
    email: user.email,
    phone: user.phone || null,
    role: user.role,
    client_id: user.client_id != null ? Number(user.client_id) : null,
    email_verified: user.email_verified === true,
    active: user.active !== false,
  };
}

function createMemoryRegistrationStore(seed = {}) {
  const tenantStore = seed.tenantStore || createMemoryTenantStore(seed.clients || []);
  let nextUserId = seed.nextUserId || 5000;
  const users = new Map();
  const tokens = [];

  for (const row of seed.users || []) {
    users.set(Number(row.id), { ...row, id: Number(row.id) });
    nextUserId = Math.max(nextUserId, Number(row.id) + 1);
  }

  return {
    tenantStore,
    async findUserByEmail(email) {
      const needle = String(email || '').toLowerCase();
      return [...users.values()].find((u) => String(u.email).toLowerCase() === needle) || null;
    },
    async findUserById(id) {
      const row = users.get(Number(id));
      return row ? { ...row } : null;
    },
    async insertUser(row) {
      if (await this.findUserByEmail(row.email)) {
        const err = new Error('Email already exists');
        err.code = 'email_taken';
        err.status = 409;
        throw err;
      }
      const id = row.id != null ? Number(row.id) : nextUserId++;
      const user = { ...row, id, active: row.active !== false };
      users.set(id, user);
      return { ...user };
    },
    async markEmailVerified(userId, at) {
      const user = users.get(Number(userId));
      if (!user) return null;
      user.email_verified = true;
      user.email_verified_at = at;
      return { ...user };
    },
    async insertToken(row) {
      tokens.push({ ...row });
      return { ...row };
    },
    async findValidToken(tokenHash, now) {
      return (
        tokens.find(
          (t) =>
            t.token_hash === tokenHash &&
            !t.used_at &&
            new Date(t.expires_at).getTime() > now.getTime()
        ) || null
      );
    },
    async consumeToken(tokenHash, at) {
      const row = tokens.find((t) => t.token_hash === tokenHash && !t.used_at);
      if (!row) return null;
      row.used_at = at;
      return { ...row };
    },
    _users: users,
    _tokens: tokens,
  };
}

function createPostgresRegistrationStore(pool) {
  return {
    tenantStore: null,
    async findUserByEmail(email) {
      const result = await pool.query(
        `SELECT id, name, email, phone, role, client_id, active, email_verified, email_verified_at, password_hash
         FROM users WHERE LOWER(email) = LOWER($1) LIMIT 1`,
        [email]
      );
      return result.rows[0] || null;
    },
    async findUserById(id) {
      const result = await pool.query(
        `SELECT id, name, email, phone, role, client_id, active, email_verified, email_verified_at
         FROM users WHERE id = $1 LIMIT 1`,
        [id]
      );
      return result.rows[0] || null;
    },
    async insertUser(row) {
      try {
        const result = await pool.query(
          `INSERT INTO users (name, email, password_hash, role, client_id, phone, email_verified, active)
           VALUES ($1, $2, $3, $4, $5, $6, FALSE, TRUE)
           RETURNING id, name, email, phone, role, client_id, active, email_verified, email_verified_at`,
          [row.name, row.email, row.password_hash, row.role, row.client_id, row.phone]
        );
        return result.rows[0];
      } catch (err) {
        if (err.code === '23505') {
          const taken = new Error('Email already exists');
          taken.code = 'email_taken';
          taken.status = 409;
          throw taken;
        }
        throw err;
      }
    },
    async markEmailVerified(userId, at) {
      const result = await pool.query(
        `UPDATE users
         SET email_verified = TRUE, email_verified_at = $2, active = TRUE
         WHERE id = $1
         RETURNING id, name, email, phone, role, client_id, active, email_verified, email_verified_at`,
        [userId, at]
      );
      return result.rows[0] || null;
    },
    async insertToken(row) {
      const result = await pool.query(
        `INSERT INTO account_verification_tokens (user_id, token_hash, expires_at)
         VALUES ($1, $2, $3)
         RETURNING id, user_id, token_hash, expires_at, used_at, created_at`,
        [row.user_id, row.token_hash, row.expires_at]
      );
      return result.rows[0];
    },
    async findValidToken(tokenHash, now) {
      const result = await pool.query(
        `SELECT id, user_id, token_hash, expires_at, used_at
         FROM account_verification_tokens
         WHERE token_hash = $1 AND used_at IS NULL AND expires_at > $2
         LIMIT 1`,
        [tokenHash, now]
      );
      return result.rows[0] || null;
    },
    async consumeToken(tokenHash, at) {
      const result = await pool.query(
        `UPDATE account_verification_tokens
         SET used_at = $2
         WHERE token_hash = $1 AND used_at IS NULL
         RETURNING id, user_id, token_hash, expires_at, used_at`,
        [tokenHash, at]
      );
      return result.rows[0] || null;
    },
  };
}

async function ensureRegistrationSchema(pool = defaultPool) {
  await ensureTenantWorkspaceSchema(pool);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS phone TEXT`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified BOOLEAN`);
  await pool.query(`ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified_at TIMESTAMPTZ`);
  await pool.query(`UPDATE users SET email_verified = TRUE WHERE email_verified IS NULL`);
  await pool.query(`ALTER TABLE users ALTER COLUMN email_verified SET DEFAULT FALSE`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS account_verification_tokens (
      id SERIAL PRIMARY KEY,
      user_id INTEGER NOT NULL REFERENCES users(id),
      token_hash TEXT NOT NULL UNIQUE,
      expires_at TIMESTAMPTZ NOT NULL,
      used_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
}

function defaultAppUrl() {
  return String(process.env.APP_URL || process.env.DASHBOARD_URL || 'http://localhost:3000').replace(/\/$/, '');
}

function buildVerifyUrl(appUrl, token) {
  return `${appUrl}/verify-email?token=${encodeURIComponent(token)}`;
}

function verificationEmailBody({ name, verifyUrl }) {
  const who = name || 'there';
  return [
    `Hi ${who},`,
    '',
    'Welcome to PulseForge. Confirm your email to open your workspace.',
    '',
    verifyUrl,
    '',
    'This link expires in 48 hours.',
    '',
    'If you did not create this account, ignore this email.',
  ].join('\n');
}

async function sendVerificationEmail({ to, name, verifyUrl, mailer }) {
  if (typeof mailer === 'function') {
    return mailer({ to, name, verifyUrl, subject: 'Verify your PulseForge workspace' });
  }
  if (!process.env.BREVO_API_KEY) {
    console.log('[registration] verification URL (mailer unavailable):', verifyUrl);
    return { sent: false, reason: 'mailer_unavailable' };
  }
  const axios = require('axios');
  const payload = {
    sender: {
      name: process.env.BREVO_FROM_NAME || 'PulseForge',
      email: process.env.BREVO_FROM_EMAIL || 'jacob@gopulseforge.com',
    },
    to: [{ email: to, name: name || to }],
    subject: 'Verify your PulseForge workspace',
    textContent: verificationEmailBody({ name, verifyUrl }),
    htmlContent: `<p>${verificationEmailBody({ name, verifyUrl }).replace(/\n/g, '<br>')}</p>`,
    tags: ['registration', 'verify-email'],
  };
  try {
    await axios.post('https://api.brevo.com/v3/smtp/email', payload, {
      headers: { 'api-key': process.env.BREVO_API_KEY, 'Content-Type': 'application/json' },
      timeout: 15000,
    });
    return { sent: true };
  } catch (err) {
    console.error('[registration] verification email failed:', err.response?.data || err.message);
    return { sent: false, reason: 'mailer_failed' };
  }
}

async function registerCustomer({
  pool = defaultPool,
  store,
  input = {},
  mailer,
  now = new Date(),
  appUrl = defaultAppUrl(),
} = {}) {
  const account = validateAccountInput(input);
  const workspaceFields = validateWorkspaceInput(input);

  if (!store && pool) {
    await ensureRegistrationSchema(pool);
  }
  const mem = store || createPostgresRegistrationStore(pool);

  const existing = await mem.findUserByEmail(account.email);
  if (existing) {
    const err = new Error('Email already exists');
    err.code = 'email_taken';
    err.status = 409;
    throw err;
  }

  // Workspace first. The user belongs to it — not the other way around.
  const provisioned = await createAndProvisionTenant({
    pool: store ? null : pool,
    store: mem.tenantStore || (store && store.tenantStore) || undefined,
    input: {
      companyName: workspaceFields.companyName,
      primaryContact: account.name,
      email: account.email,
      vertical: workspaceFields.vertical,
      country: workspaceFields.country,
      timezone: workspaceFields.timezone,
      website: workspaceFields.website,
      logoUrl: workspaceFields.logoUrl,
      phone: account.phone,
      teamSize: workspaceFields.teamSize,
    },
    origin: 'self_service',
    lifecycle: LIFECYCLE.PROVISIONED,
  });

  const passwordHash = await bcrypt.hash(account.password, 12);
  const user = await mem.insertUser({
    name: account.name,
    email: account.email,
    password_hash: passwordHash,
    phone: account.phone,
    role: 'client',
    client_id: provisioned.client.id,
    email_verified: false,
    active: true,
  });

  const token = newToken();
  const expiresAt = new Date(now.getTime() + TOKEN_TTL_MS);
  await mem.insertToken({
    user_id: user.id,
    token_hash: hashToken(token),
    expires_at: expiresAt,
  });

  const verifyUrl = buildVerifyUrl(appUrl, token);
  const mail = await sendVerificationEmail({
    to: account.email,
    name: account.name,
    verifyUrl,
    mailer,
  });

  return {
    user: publicUser(user),
    client: provisioned.client,
    workspace: provisioned.workspace,
    status: provisioned.status,
    lifecycle: publicLifecycle(LIFECYCLE.PROVISIONED),
    greeting: buildRegistrationGreeting(),
    provisioned: true,
    verification: {
      required: true,
      sent: mail.sent === true,
      expires_at: expiresAt.toISOString(),
    },
  };
}

async function verifyRegistrationToken({
  pool = defaultPool,
  store,
  token,
  now = new Date(),
} = {}) {
  const raw = asText(token, 200);
  if (!raw) {
    throw validationError('Verification token is required');
  }
  if (!store && pool) {
    await ensureRegistrationSchema(pool);
  }
  const mem = store || createPostgresRegistrationStore(pool);
  const tokenHash = hashToken(raw);
  const row = await mem.findValidToken(tokenHash, now);
  if (!row) {
    const err = new Error('Verification token is invalid or expired');
    err.code = 'verification_invalid';
    err.status = 400;
    throw err;
  }
  await mem.consumeToken(tokenHash, now);
  const user = await mem.markEmailVerified(row.user_id, now);
  if (!user) {
    const err = new Error('Account not found');
    err.code = 'user_not_found';
    err.status = 404;
    throw err;
  }
  return { user: publicUser(user), verified: true };
}

function assertClientWorkspace(user) {
  const clientId = user?.client_id != null ? Number(user.client_id) : null;
  if (!user || user.role !== 'client') {
    return { ok: true, clientId: clientId && clientId > 0 ? clientId : null };
  }
  if (!Number.isFinite(clientId) || clientId <= 0) {
    const err = new Error('No workspace provisioned.');
    err.code = 'no_workspace';
    err.status = 403;
    throw err;
  }
  return { ok: true, clientId };
}

function establishRegisteredSession(session, user) {
  if (!session) {
    const err = new Error('Session is required');
    err.status = 500;
    throw err;
  }
  if (user.email_verified !== true) {
    const err = new Error('Verify your email before signing in.');
    err.code = 'email_unverified';
    err.status = 403;
    throw err;
  }
  const bound = assertClientWorkspace(user);
  session.user = {
    id: user.id,
    name: user.name,
    email: user.email,
    role: user.role,
    client_id: bound.clientId,
    email_verified: true,
  };
  session.authenticated = true;
  if (user.role === 'client') {
    activateTenant(session, bound.clientId);
  } else if (user.client_id) {
    activateTenant(session, Number(user.client_id));
  }
  return {
    ok: true,
    user: session.user,
    active_client_id: session.active_client_id || null,
  };
}

async function loadRegisteredWorkspace({ pool, store, user }) {
  const bound = assertClientWorkspace(user);
  if (bound.clientId == null) {
    const err = new Error('No workspace provisioned.');
    err.code = 'no_workspace';
    err.status = 403;
    throw err;
  }
  return getTenantWorkspace({ pool, store: store?.tenantStore || store, clientId: bound.clientId });
}

module.exports = {
  ACCOUNT_REQUIRED,
  WORKSPACE_REQUIRED,
  TOKEN_TTL_MS,
  MIN_PASSWORD,
  validateAccountInput,
  validateWorkspaceInput,
  publicUser,
  createMemoryRegistrationStore,
  createPostgresRegistrationStore,
  ensureRegistrationSchema,
  registerCustomer,
  verifyRegistrationToken,
  assertClientWorkspace,
  establishRegisteredSession,
  loadRegisteredWorkspace,
  hashToken,
  buildVerifyUrl,
  verificationEmailBody,
  sendVerificationEmail,
};
