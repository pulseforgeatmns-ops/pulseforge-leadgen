const express = require('express');
const pool = require('../db');
const { ROLES, bcrypt, initAuth, requireAuth, requireRole } = require('../middleware/auth');

const router = express.Router();
const adminOnly = [requireAuth, requireRole('admin')];

function publicUser(row) {
  return {
    id: row.id,
    name: row.name,
    email: row.email,
    role: row.role,
    active: row.active,
    created_at: row.created_at,
    last_login_at: row.last_login_at,
  };
}

function validateRole(role) {
  return ROLES.includes(role);
}

router.get('/admin/users', ...adminOnly, (req, res) => {
  res.send(`<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Pulseforge — Users</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=JetBrains+Mono:wght@300;400;500&family=DM+Sans:wght@300;400;500;600&display=swap" rel="stylesheet">
<style>
:root { --bg:#040810; --bg1:#070d1a; --purple:#6030b1; --purple-bright:#8b5cf6; --teal:#00d4b4; --green:#00e676; --red:#ff3b5c; --white:#e8f0fe; --gray:#3a4a6a; --gray-light:#6b7fa0; --border:rgba(96,48,177,0.18); }
* { box-sizing:border-box; margin:0; padding:0; }
body { min-height:100vh; background:var(--bg); color:var(--white); font-family:'DM Sans',sans-serif; padding:2rem; }
.top { display:flex; justify-content:space-between; align-items:center; gap:1rem; margin-bottom:1.5rem; }
.title { font-family:'Bebas Neue',sans-serif; font-size:1.7rem; letter-spacing:4px; }
.sub { font-family:'JetBrains Mono',monospace; color:var(--gray-light); font-size:0.62rem; letter-spacing:2px; text-transform:uppercase; margin-top:3px; }
a, button { font-family:'JetBrains Mono',monospace; font-size:0.62rem; letter-spacing:1px; text-transform:uppercase; }
a { color:var(--teal); text-decoration:none; }
.panel { background:var(--bg1); border:1px solid var(--border); border-radius:10px; overflow:hidden; margin-bottom:1rem; }
.form { display:grid; grid-template-columns:1.2fr 1.4fr 1fr 0.8fr auto; gap:0.75rem; padding:1rem; border-bottom:1px solid var(--border); }
input, select { width:100%; background:rgba(255,255,255,0.04); border:1px solid rgba(255,255,255,0.08); border-radius:6px; padding:0.7rem; color:var(--white); outline:none; }
button { border:1px solid var(--border); color:var(--gray-light); background:transparent; border-radius:5px; padding:0.65rem 0.8rem; cursor:pointer; }
button.primary { background:rgba(96,48,177,0.2); color:var(--purple-bright); border-color:rgba(139,92,246,0.35); }
button.danger { color:var(--red); border-color:rgba(255,59,92,0.25); }
button.good { color:var(--green); border-color:rgba(0,230,118,0.25); }
table { width:100%; border-collapse:collapse; }
th { font-family:'JetBrains Mono',monospace; font-size:0.58rem; letter-spacing:1.5px; text-transform:uppercase; color:var(--gray); padding:0.75rem 1rem; text-align:left; border-bottom:1px solid var(--border); }
td { padding:0.75rem 1rem; border-bottom:1px solid rgba(255,255,255,0.04); color:rgba(255,255,255,0.76); }
.actions { display:flex; flex-wrap:wrap; gap:0.4rem; }
.badge { font-family:'JetBrains Mono',monospace; font-size:0.58rem; padding:3px 7px; border-radius:4px; border:1px solid var(--border); color:var(--gray-light); text-transform:uppercase; }
.active { color:var(--green); border-color:rgba(0,230,118,0.25); }
.inactive { color:var(--red); border-color:rgba(255,59,92,0.25); }
.msg { min-height:1.2rem; color:var(--gray-light); font-size:0.78rem; margin-bottom:0.75rem; }
@media (max-width: 900px) { body { padding:1rem; } .form { grid-template-columns:1fr; } table { font-size:0.8rem; } th:nth-child(5), td:nth-child(5) { display:none; } }
</style>
</head>
<body>
<div class="top">
  <div><div class="title">USER MANAGEMENT</div><div class="sub">Admin only · Pulseforge Lead Engine</div></div>
  <a href="/dashboard">Back to dashboard</a>
</div>
<div class="msg" id="msg"></div>
<section class="panel">
  <form class="form" id="addForm">
    <input id="name" placeholder="Name" required>
    <input id="email" type="email" placeholder="Email" required>
    <input id="password" type="password" minlength="8" placeholder="Password" required>
    <select id="role"><option value="setter">Setter</option><option value="manager">Manager</option><option value="admin">Admin</option></select>
    <button class="primary" type="submit">Add User</button>
  </form>
  <table>
    <thead><tr><th>Name</th><th>Email</th><th>Role</th><th>Status</th><th>Last Login</th><th>Actions</th></tr></thead>
    <tbody id="rows"><tr><td colspan="6">Loading...</td></tr></tbody>
  </table>
</section>
<script>
const roles = ['admin','manager','setter'];
const msg = document.getElementById('msg');
function say(text) { msg.textContent = text; setTimeout(() => { if (msg.textContent === text) msg.textContent = ''; }, 3500); }
async function api(path, options = {}) {
  const res = await fetch(path, { credentials:'include', headers:{'Content-Type':'application/json'}, ...options });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || 'Request failed');
  return data;
}
function date(v) { return v ? new Date(v).toLocaleString() : '-'; }
async function load() {
  const users = await api('/api/users');
  document.getElementById('rows').innerHTML = users.map(u => \`
    <tr>
      <td>\${u.name}</td>
      <td>\${u.email}</td>
      <td><select data-role="\${u.id}">\${roles.map(r => \`<option value="\${r}" \${r === u.role ? 'selected' : ''}>\${r}</option>\`).join('')}</select></td>
      <td><span class="badge \${u.active ? 'active' : 'inactive'}">\${u.active ? 'active' : 'inactive'}</span></td>
      <td>\${date(u.last_login_at)}</td>
      <td><div class="actions">
        <button class="\${u.active ? 'danger' : 'good'}" data-active="\${u.id}" data-value="\${!u.active}">\${u.active ? 'Deactivate' : 'Reactivate'}</button>
        <button data-reset="\${u.id}">Reset Password</button>
        <button class="danger" data-delete="\${u.id}">Delete</button>
      </div></td>
    </tr>
  \`).join('');
}
document.getElementById('addForm').addEventListener('submit', async e => {
  e.preventDefault();
  try {
    await api('/api/users', {
      method:'POST',
      body:JSON.stringify({
        name: document.getElementById('name').value,
        email: document.getElementById('email').value,
        password: document.getElementById('password').value,
        role: document.getElementById('role').value
      })
    });
    e.target.reset(); say('User created'); load();
  } catch (err) { say(err.message); }
});
document.addEventListener('change', async e => {
  const id = e.target.dataset.role;
  if (!id) return;
  try { await api('/api/users/' + id, { method:'PATCH', body:JSON.stringify({ role:e.target.value }) }); say('Role updated'); load(); } catch (err) { say(err.message); load(); }
});
document.addEventListener('click', async e => {
  const activeId = e.target.dataset.active;
  const resetId = e.target.dataset.reset;
  const deleteId = e.target.dataset.delete;
  try {
    if (activeId) { await api('/api/users/' + activeId, { method:'PATCH', body:JSON.stringify({ active:e.target.dataset.value === 'true' }) }); say('Status updated'); load(); }
    if (resetId) {
      const password = prompt('New password, minimum 8 characters');
      if (password) { await api('/api/users/' + resetId + '/reset-password', { method:'POST', body:JSON.stringify({ password }) }); say('Password reset'); }
    }
    if (deleteId && confirm('Hard delete this user? Deactivate is safer.')) { await api('/api/users/' + deleteId, { method:'DELETE' }); say('User deleted'); load(); }
  } catch (err) { say(err.message); }
});
load().catch(err => say(err.message));
</script>
</body>
</html>`);
});

router.get('/api/users', ...adminOnly, async (req, res) => {
  await initAuth();
  const { rows } = await pool.query(`
    SELECT id, name, email, role, active, created_at, last_login_at
    FROM users
    ORDER BY created_at DESC
  `);
  res.json(rows.map(publicUser));
});

router.post('/api/users', ...adminOnly, async (req, res) => {
  await initAuth();
  const { name, email, password, role } = req.body;
  if (!name || !email || !password || !validateRole(role)) return res.status(400).json({ error: 'Invalid user' });
  if (String(password).length < 8) return res.status(400).json({ error: 'Password must be at least 8 characters' });
  const hash = await bcrypt.hash(password, 12);
  try {
    const { rows } = await pool.query(`
      INSERT INTO users (name, email, password_hash, role)
      VALUES ($1, $2, $3, $4)
      RETURNING id, name, email, role, active, created_at, last_login_at
    `, [name.trim(), email.toLowerCase().trim(), hash, role]);
    res.status(201).json(publicUser(rows[0]));
  } catch (err) {
    if (err.code === '23505') return res.status(409).json({ error: 'Email already exists' });
    throw err;
  }
});

router.patch('/api/users/:id', ...adminOnly, async (req, res) => {
  await initAuth();
  const id = Number(req.params.id);
  const updates = [];
  const values = [];
  if (req.body.role !== undefined) {
    if (!validateRole(req.body.role)) return res.status(400).json({ error: 'Invalid role' });
    values.push(req.body.role);
    updates.push(`role = $${values.length}`);
  }
  if (req.body.active !== undefined) {
    if (id === req.user.id && req.body.active === false) return res.status(400).json({ error: 'Cannot deactivate your own account' });
    values.push(Boolean(req.body.active));
    updates.push(`active = $${values.length}`);
  }
  if (!updates.length) return res.status(400).json({ error: 'No updates provided' });
  values.push(id);
  const { rows } = await pool.query(`
    UPDATE users SET ${updates.join(', ')}
    WHERE id = $${values.length}
    RETURNING id, name, email, role, active, created_at, last_login_at
  `, values);
  if (!rows.length) return res.status(404).json({ error: 'User not found' });
  res.json(publicUser(rows[0]));
});

router.post('/api/users/:id/reset-password', ...adminOnly, async (req, res) => {
  await initAuth();
  const { password } = req.body;
  if (!password || String(password).length < 8) return res.status(400).json({ error: 'Password must be at least 8 characters' });
  const hash = await bcrypt.hash(password, 12);
  const { rowCount } = await pool.query('UPDATE users SET password_hash = $1 WHERE id = $2', [hash, req.params.id]);
  if (!rowCount) return res.status(404).json({ error: 'User not found' });
  res.json({ success: true });
});

router.delete('/api/users/:id', ...adminOnly, async (req, res) => {
  await initAuth();
  const { rowCount } = await pool.query('DELETE FROM users WHERE id = $1', [req.params.id]);
  if (!rowCount) return res.status(404).json({ error: 'User not found' });
  res.json({ success: true });
});

module.exports = router;
