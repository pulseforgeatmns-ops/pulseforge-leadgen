const bcrypt = require('bcryptjs');
const pool = require('../db');

const ROLES = ['admin', 'manager', 'setter', 'closer', 'sales', 'viewer', 'client'];
const ROLE_CHECK = ROLES.map(role => `'${role}'`).join(', ');
let initPromise;

function isApiRequest(req) {
  return req.path.startsWith('/api/') || req.originalUrl.startsWith('/api/') || req.get('accept')?.includes('application/json');
}

async function ensureUsersTable() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS users (
      id SERIAL PRIMARY KEY,
      name TEXT NOT NULL,
      email TEXT NOT NULL UNIQUE,
      password_hash TEXT NOT NULL,
      role TEXT NOT NULL CHECK (role in (${ROLE_CHECK})),
      client_id INTEGER,
      active BOOLEAN NOT NULL DEFAULT true,
      created_at TIMESTAMPTZ DEFAULT NOW(),
      last_login_at TIMESTAMPTZ
    )
  `);
  await pool.query('ALTER TABLE users ADD COLUMN IF NOT EXISTS client_id INTEGER');
  await pool.query('SELECT pg_advisory_lock(91720260517)');
  try {
    const { rows: existing } = await pool.query(`
      SELECT con.conname
      FROM pg_constraint con
      JOIN pg_class cls ON cls.oid = con.conrelid
      WHERE cls.relname = 'users'
        AND con.contype = 'c'
        AND pg_get_constraintdef(con.oid) ILIKE '%role%'
    `);
    for (const row of existing) {
      await pool.query(`ALTER TABLE users DROP CONSTRAINT IF EXISTS "${row.conname}"`);
    }
    await pool.query(`
      ALTER TABLE users ADD CONSTRAINT users_role_check
      CHECK (role IN (${ROLE_CHECK}))
    `);
  } finally {
    await pool.query('SELECT pg_advisory_unlock(91720260517)');
  }
}

async function getUserCount() {
  const { rows } = await pool.query('SELECT COUNT(*)::int AS count FROM users');
  return rows[0]?.count || 0;
}

async function initializeAuth() {
  await ensureUsersTable();
  const count = await getUserCount();
  if (count === 0 && process.env.ADMIN_EMAIL && process.env.ADMIN_PASSWORD) {
    const hash = await bcrypt.hash(process.env.ADMIN_PASSWORD, 12);
    await pool.query(`
      INSERT INTO users (name, email, password_hash, role, active)
      VALUES ($1, $2, $3, 'admin', true)
      ON CONFLICT (email) DO NOTHING
    `, [
      process.env.ADMIN_NAME || 'Pulseforge Admin',
      process.env.ADMIN_EMAIL.toLowerCase().trim(),
      hash,
    ]);
  }
}

function initAuth() {
  if (!initPromise) initPromise = initializeAuth();
  return initPromise;
}

function requireAuth(req, res, next) {
  if (req.session?.user) {
    req.user = req.session.user;
    return next();
  }

  if (req.session?.authenticated) {
    req.user = { id: null, name: 'Legacy Admin', email: 'legacy@pulseforge.local', role: 'admin' };
    req.session.user = req.user;
    return next();
  }

  if (isApiRequest(req)) return res.status(401).json({ error: 'Unauthorized' });
  return res.redirect('/login');
}

function requireRole(...roles) {
  return (req, res, next) => {
    const check = () => {
      if (roles.includes(req.user?.role)) return next();
      if (req.user?.role === 'sales' && !isApiRequest(req)) return res.redirect('/sales');
      if (req.user?.role === 'setter' && !isApiRequest(req)) return res.redirect('/setter');
      if (req.user?.role === 'closer' && !isApiRequest(req)) return res.redirect('/closer');
      if (['admin', 'manager', 'viewer', 'client'].includes(req.user?.role) && !isApiRequest(req)) return res.redirect('/dashboard');
      return res.status(403).json({ error: 'Forbidden' });
    };

    if (req.user) return check();
    return requireAuth(req, res, err => (err ? next(err) : check()));
  };
}

module.exports = {
  ROLES,
  bcrypt,
  ensureUsersTable,
  getUserCount,
  initAuth,
  requireAuth,
  requireRole,
};
