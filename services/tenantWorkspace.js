'use strict';

/**
 * SPEC-114 — Client tenant creation, workspace provision, and isolation.
 *
 * A new tenant starts empty. Intelligence is never copied from Pulseforge,
 * Anchor, or the hand-authored Fedir AIM seed.
 */

const defaultPool = require('../db');
const {
  buildTenantGreeting,
  greetingForWorkspace,
} = require('../packages/max/workspace/TenantContextResolver');
const {
  LIFECYCLE,
  deriveWorkspaceLifecycle,
  publicLifecycle,
} = require('./workspaceLifecycle');

const REQUIRED_FIELDS = [
  'companyName',
  'primaryContact',
  'email',
  'industry',
  'country',
  'timezone',
];

const CLIENT_INTEL_NOT_STARTED = 'Not Started';
const AIM_NONE = 'No Published AIM';
const EMPTY = 'Empty';

const NEW_TENANT_AGENTS = ['max'];

function asText(value, max = 500) {
  if (value == null) return '';
  return String(value).trim().slice(0, max);
}

function slugify(name) {
  const slug = asText(name, 80)
    .toLowerCase()
    .replace(/['’]/g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '');
  return slug || 'client';
}

function namespacesFor(clientId, tenantKey) {
  const key = tenantKey || `tenant-${clientId}`;
  return {
    knowledge_namespace: `tenant:${clientId}:knowledge`,
    mission_namespace: `tenant:${clientId}:mission`,
    prospect_namespace: `tenant:${clientId}:prospect`,
    outcome_namespace: `tenant:${clientId}:outcome`,
    aim_namespace: `tenant:${clientId}:aim:${key}`,
    campaign_namespace: `tenant:${clientId}:campaign`,
    memory_namespace: `tenant:${clientId}:memory`,
  };
}

function initialWorkspaceStatus() {
  return {
    clientIntelligence: { status: CLIENT_INTEL_NOT_STARTED, present: false, approved: false },
    aim: { status: AIM_NONE, present: false, published: false, inProgress: false },
    missions: { status: EMPTY, count: 0 },
    prospects: { status: EMPTY, count: 0 },
    campaigns: { status: EMPTY, count: 0 },
    outcomes: { status: EMPTY, count: 0 },
    knowledge: { status: EMPTY, count: 0 },
    memory: { status: EMPTY, count: 0 },
    needsOnboarding: true,
  };
}

function validateCreateClientInput(raw = {}) {
  const input = {
    companyName: asText(raw.companyName || raw.company_name || raw.name, 200),
    primaryContact: asText(raw.primaryContact || raw.primary_contact, 200),
    email: asText(raw.email, 200).toLowerCase(),
    industry: asText(raw.industry || raw.vertical, 120),
    country: asText(raw.country, 80),
    timezone: asText(raw.timezone || raw.time_zone, 80),
    website: asText(raw.website, 300) || null,
    logoUrl: asText(raw.logoUrl || raw.logo_url || raw.logo, 500) || null,
    phone: asText(raw.phone, 40) || null,
    notes: asText(raw.notes, 4000) || null,
    teamSize: asText(raw.teamSize || raw.team_size, 80) || null,
  };

  const missing = REQUIRED_FIELDS.filter((key) => !input[key]);
  if (missing.length) {
    const err = new Error(`Missing required fields: ${missing.join(', ')}`);
    err.code = 'tenant_validation';
    err.status = 400;
    err.missing = missing;
    throw err;
  }
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(input.email)) {
    const err = new Error('Email is invalid');
    err.code = 'tenant_validation';
    err.status = 400;
    throw err;
  }
  return input;
}

function createMemoryTenantStore(seed = []) {
  let nextId = 1000;
  const clients = new Map();
  const workspaces = new Map();
  const namespaces = {
    knowledge: new Map(),
    missions: new Map(),
    prospects: new Map(),
    outcomes: new Map(),
    aim: new Map(),
    campaigns: new Map(),
    memory: new Map(),
    blueprints: new Map(),
  };

  for (const row of seed) {
    const id = Number(row.id);
    clients.set(id, { ...row, id });
    if (row.workspace) workspaces.set(id, row.workspace);
    nextId = Math.max(nextId, id + 1);
  }

  return {
    async nextClientId() {
      const id = nextId;
      nextId += 1;
      return id;
    },
    async insertClient(row) {
      if ([...clients.values()].some((c) => c.slug === row.slug)) {
        const err = new Error(`Client slug "${row.slug}" already exists`);
        err.code = 'tenant_slug_taken';
        err.status = 409;
        throw err;
      }
      clients.set(row.id, { ...row });
      return { ...row };
    },
    async getClient(id) {
      const row = clients.get(Number(id));
      return row ? { ...row } : null;
    },
    async findBySlug(slug) {
      return [...clients.values()].find((c) => c.slug === slug) || null;
    },
    async insertWorkspace(row) {
      workspaces.set(Number(row.client_id), { ...row });
      for (const key of ['knowledge', 'missions', 'prospects', 'outcomes', 'aim', 'campaigns', 'memory']) {
        namespaces[key].set(Number(row.client_id), []);
      }
      namespaces.blueprints.set(Number(row.client_id), []);
      return { ...row };
    },
    async getWorkspace(clientId) {
      const row = workspaces.get(Number(clientId));
      return row ? { ...row } : null;
    },
    listClients() {
      return [...clients.values()].map((c) => ({ ...c }));
    },
    putNamespace(kind, clientId, items) {
      namespaces[kind].set(Number(clientId), items || []);
    },
    listNamespace(kind, clientId) {
      return [...(namespaces[kind].get(Number(clientId)) || [])];
    },
    _clients: clients,
    _workspaces: workspaces,
    _namespaces: namespaces,
  };
}

async function uniqueSlug(store, base) {
  let slug = base;
  let n = 2;
  while (await store.findBySlug(slug)) {
    slug = `${base}-${n}`;
    n += 1;
  }
  return slug;
}

function createPostgresTenantStore(pool) {
  return {
    async nextClientId() {
      return null;
    },
    async insertClient(row) {
      const result = await pool.query(
        `INSERT INTO clients (
           name, slug, business_name, primary_contact, email, industry, vertical,
           country, timezone, website, logo_url, phone, notes, team_size,
           sender_name, max_email, enabled_agents, active
         ) VALUES (
           $1, $2, $1, $3, $4, $5, $5,
           $6, $7, $8, $9, $10, $11, $12,
           $3, $4, $13, true
         )
         RETURNING *`,
        [
          row.name,
          row.slug,
          row.primary_contact,
          row.email,
          row.industry,
          row.country,
          row.timezone,
          row.website,
          row.logo_url,
          row.phone,
          row.notes,
          row.team_size || null,
          row.enabled_agents,
        ]
      );
      return result.rows[0];
    },
    async getClient(id) {
      const result = await pool.query('SELECT * FROM clients WHERE id = $1 LIMIT 1', [id]);
      return result.rows[0] || null;
    },
    async findBySlug(slug) {
      const result = await pool.query('SELECT id, slug FROM clients WHERE slug = $1 LIMIT 1', [slug]);
      return result.rows[0] || null;
    },
    async insertWorkspace(row) {
      const result = await pool.query(
        `INSERT INTO tenant_workspaces (
           client_id, tenant_key, knowledge_namespace, mission_namespace,
           prospect_namespace, outcome_namespace, aim_namespace,
           campaign_namespace, memory_namespace, origin, lifecycle,
           platform_knowledge_isolated, created_by
         ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, true, $12)
         RETURNING *`,
        [
          row.client_id,
          row.tenant_key,
          row.knowledge_namespace,
          row.mission_namespace,
          row.prospect_namespace,
          row.outcome_namespace,
          row.aim_namespace,
          row.campaign_namespace,
          row.memory_namespace,
          row.origin || 'operator',
          row.lifecycle || LIFECYCLE.PROVISIONED,
          row.created_by,
        ]
      );
      return result.rows[0];
    },
    async getWorkspace(clientId) {
      const result = await pool.query(
        'SELECT * FROM tenant_workspaces WHERE client_id = $1 LIMIT 1',
        [clientId]
      );
      return result.rows[0] || null;
    },
  };
}

async function countOrZero(pool, sql, params) {
  try {
    const result = await pool.query(sql, params);
    return Number(result.rows[0]?.count || 0);
  } catch (err) {
    if (/does not exist|relation .* does not exist/i.test(err.message || '')) return 0;
    throw err;
  }
}

async function loadLiveCounts(pool, clientId) {
  const id = Number(clientId);
  const [
    prospects,
    missions,
    outcomes,
    knowledge,
    blueprints,
    approvedBlueprints,
    aims,
    draftAims,
    campaigns,
  ] = await Promise.all([
    countOrZero(pool, 'SELECT COUNT(*)::int AS count FROM prospects WHERE client_id = $1', [id]),
    countOrZero(
      pool,
      `SELECT COUNT(*)::int AS count FROM missions
       WHERE client_id = $1 OR tenant_id = $2 OR tenant_id = $3`,
      [id, String(id), `tenant:${id}`]
    ),
    countOrZero(
      pool,
      `SELECT COUNT(*)::int AS count FROM content_outcomes WHERE client_id = $1`,
      [id]
    ).catch(async () =>
      countOrZero(pool, 'SELECT COUNT(*)::int AS count FROM outcomes WHERE client_id = $1', [id])
    ),
    countOrZero(
      pool,
      `SELECT COUNT(*)::int AS count FROM knowledge_nodes WHERE client_id = $1`,
      [id]
    ),
    countOrZero(
      pool,
      `SELECT COUNT(*)::int AS count FROM cie_business_blueprints
       WHERE client_id = $1 AND status IN ('approved', 'in_review', 'draft')`,
      [id]
    ),
    countOrZero(
      pool,
      `SELECT COUNT(*)::int AS count FROM cie_business_blueprints
       WHERE client_id = $1 AND status = 'approved'`,
      [id]
    ),
    countOrZero(
      pool,
      `SELECT COUNT(*)::int AS count FROM aim_models
       WHERE client_id = $1 AND status IN ('published', 'complete')`,
      [id]
    ),
    countOrZero(
      pool,
      `SELECT COUNT(*)::int AS count FROM aim_models
       WHERE client_id = $1 AND status IN ('draft', 'review', 'in_progress')`,
      [id]
    ),
    countOrZero(pool, 'SELECT COUNT(*)::int AS count FROM campaigns WHERE client_id = $1', [id]),
  ]);
  return {
    prospects,
    missions,
    outcomes,
    knowledge,
    blueprints,
    approvedBlueprints,
    aims,
    draftAims,
    campaigns,
    memory: 0,
  };
}

function statusFromCounts(counts, extras = {}) {
  const blueprintApproved =
    Number(counts.approvedBlueprints || 0) > 0 || extras.blueprintApproved === true;
  const blueprintPresent =
    Number(counts.blueprints || 0) > 0 || extras.blueprintPresent === true || blueprintApproved;
  const aimPublished = Number(counts.aims || 0) > 0 || extras.aimPresent === true;
  const aimInProgress = Number(counts.draftAims || 0) > 0 || extras.aimInProgress === true;
  const aimPresent = aimPublished || aimInProgress;
  const missions = Number(counts.missions || 0);
  const prospects = Number(counts.prospects || 0);
  const campaigns = Number(counts.campaigns || 0);
  const outcomes = Number(counts.outcomes || 0);
  const knowledge = Number(counts.knowledge || 0);
  const memory = Number(counts.memory || 0);
  return {
    clientIntelligence: {
      status: blueprintApproved
        ? 'Approved'
        : blueprintPresent
          ? 'In Progress'
          : CLIENT_INTEL_NOT_STARTED,
      present: blueprintPresent,
      approved: blueprintApproved,
    },
    aim: {
      status: aimPublished ? 'Published AIM' : aimInProgress ? 'In Progress' : AIM_NONE,
      present: aimPresent,
      published: aimPublished,
      inProgress: aimInProgress && !aimPublished,
    },
    missions: { status: missions > 0 ? 'Present' : EMPTY, count: missions },
    prospects: { status: prospects > 0 ? 'Present' : EMPTY, count: prospects },
    campaigns: { status: campaigns > 0 ? 'Present' : EMPTY, count: campaigns },
    outcomes: { status: outcomes > 0 ? 'Present' : EMPTY, count: outcomes },
    knowledge: { status: knowledge > 0 ? 'Present' : EMPTY, count: knowledge },
    memory: { status: memory > 0 ? 'Present' : EMPTY, count: memory },
    needsOnboarding: !blueprintPresent && !aimPresent && missions === 0 && prospects === 0,
  };
}

/**
 * Look up a published AIM that belongs to THIS tenant.
 * Never falls back to the in-memory Fedir seed by slug/clientKey.
 */
function aimBelongsToTenant(aim, clientId) {
  if (!aim) return false;
  if (aim.client_id == null && aim.clientId == null) return false;
  return Number(aim.client_id ?? aim.clientId) === Number(clientId);
}

async function getPublishedAimForTenant({ pool, store, clientId, aimLookup }) {
  if (typeof aimLookup === 'function') {
    const found = aimLookup({ clientId }) || null;
    return aimBelongsToTenant(found, clientId) ? found : null;
  }
  if (store && typeof store.listNamespace === 'function') {
    const aims = store.listNamespace('aim', clientId);
    return aims.find((a) => a && (a.status === 'published' || a.status === 'complete')) || null;
  }
  if (!pool) return null;
  try {
    const result = await pool.query(
      `SELECT id, client_key, client_id, status, version
       FROM aim_models
       WHERE client_id = $1 AND status IN ('published', 'complete')
       ORDER BY version DESC
       LIMIT 1`,
      [clientId]
    );
    return result.rows[0] || null;
  } catch (err) {
    if (/does not exist|relation .* does not exist/i.test(err.message || '')) return null;
    throw err;
  }
}

async function ensureTenantWorkspaceSchema(pool = defaultPool) {
  await pool.query(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS primary_contact TEXT`);
  await pool.query(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS industry TEXT`);
  await pool.query(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS country TEXT`);
  await pool.query(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS timezone TEXT`);
  await pool.query(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS logo_url TEXT`);
  await pool.query(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS notes TEXT`);
  await pool.query(`ALTER TABLE clients ADD COLUMN IF NOT EXISTS team_size TEXT`);
  await pool.query(`
    CREATE TABLE IF NOT EXISTS tenant_workspaces (
      client_id INTEGER PRIMARY KEY REFERENCES clients(id),
      tenant_key TEXT NOT NULL UNIQUE,
      knowledge_namespace TEXT NOT NULL,
      mission_namespace TEXT NOT NULL,
      prospect_namespace TEXT NOT NULL,
      outcome_namespace TEXT NOT NULL,
      aim_namespace TEXT NOT NULL,
      platform_knowledge_isolated BOOLEAN NOT NULL DEFAULT TRUE,
      provisioned_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      created_by INTEGER,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
  `);
  await pool.query(`ALTER TABLE tenant_workspaces ADD COLUMN IF NOT EXISTS origin TEXT`);
  await pool.query(`ALTER TABLE tenant_workspaces ADD COLUMN IF NOT EXISTS lifecycle TEXT`);
  await pool.query(`ALTER TABLE tenant_workspaces ADD COLUMN IF NOT EXISTS campaign_namespace TEXT`);
  await pool.query(`ALTER TABLE tenant_workspaces ADD COLUMN IF NOT EXISTS memory_namespace TEXT`);
}

async function createAndProvisionTenant({
  pool = defaultPool,
  store,
  input,
  actor,
  origin = 'operator',
  lifecycle = LIFECYCLE.PROVISIONED,
} = {}) {
  const fields = validateCreateClientInput(input);
  if (!store && pool) {
    await ensureTenantWorkspaceSchema(pool);
  }
  const mem = store || (pool ? createPostgresTenantStore(pool) : createMemoryTenantStore());
  const baseSlug = slugify(fields.companyName);
  const slug = await uniqueSlug(mem, baseSlug);
  const createdBy = actor?.id != null ? Number(actor.id) : null;
  const workspaceOrigin = origin === 'self_service' ? 'self_service' : 'operator';
  const workspaceLifecycle = lifecycle || LIFECYCLE.PROVISIONED;

  let client;
  const assignedId =
    typeof mem.nextClientId === 'function' ? await mem.nextClientId() : null;
  if (assignedId != null) {
    client = await mem.insertClient({
      id: assignedId,
      name: fields.companyName,
      slug,
      business_name: fields.companyName,
      primary_contact: fields.primaryContact,
      email: fields.email,
      industry: fields.industry,
      vertical: fields.industry,
      country: fields.country,
      timezone: fields.timezone,
      website: fields.website,
      logo_url: fields.logoUrl,
      phone: fields.phone,
      notes: fields.notes,
      team_size: fields.teamSize,
      sender_name: fields.primaryContact,
      max_email: fields.email,
      enabled_agents: NEW_TENANT_AGENTS,
      active: true,
    });
  } else {
    client = await mem.insertClient({
      name: fields.companyName,
      slug,
      primary_contact: fields.primaryContact,
      email: fields.email,
      industry: fields.industry,
      country: fields.country,
      timezone: fields.timezone,
      website: fields.website,
      logo_url: fields.logoUrl,
      phone: fields.phone,
      notes: fields.notes,
      team_size: fields.teamSize,
      enabled_agents: NEW_TENANT_AGENTS,
    });
  }

  const ns = namespacesFor(client.id, slug);
  const workspace = await mem.insertWorkspace({
    client_id: client.id,
    tenant_key: slug,
    ...ns,
    origin: workspaceOrigin,
    lifecycle: workspaceLifecycle,
    created_by: createdBy,
  });

  const status = initialWorkspaceStatus();
  const publicWs = publicWorkspace(workspace);
  return {
    client: publicTenant(client),
    workspace: publicWs,
    status,
    lifecycle: publicLifecycle(workspaceLifecycle),
    greeting: greetingForWorkspace(publicWs, client.name),
    provisioned: true,
  };
}

function publicTenant(client) {
  if (!client) return null;
  return {
    id: Number(client.id),
    name: client.name,
    slug: client.slug,
    business_name: client.business_name || client.name,
    primary_contact: client.primary_contact || null,
    email: client.email || null,
    industry: client.industry || client.vertical || null,
    country: client.country || null,
    timezone: client.timezone || null,
    website: client.website || null,
    logo_url: client.logo_url || null,
    phone: client.phone || null,
    notes: client.notes || null,
    team_size: client.team_size || null,
    enabled_agents: client.enabled_agents || NEW_TENANT_AGENTS,
    active: client.active !== false,
  };
}

function publicWorkspace(row) {
  if (!row) return null;
  return {
    client_id: Number(row.client_id),
    tenant_key: row.tenant_key,
    knowledge_namespace: row.knowledge_namespace,
    mission_namespace: row.mission_namespace,
    prospect_namespace: row.prospect_namespace,
    outcome_namespace: row.outcome_namespace,
    aim_namespace: row.aim_namespace,
    campaign_namespace: row.campaign_namespace || `tenant:${row.client_id}:campaign`,
    memory_namespace: row.memory_namespace || `tenant:${row.client_id}:memory`,
    origin: row.origin || 'operator',
    lifecycle: row.lifecycle || LIFECYCLE.PROVISIONED,
    platform_knowledge_isolated: row.platform_knowledge_isolated !== false,
    provisioned_at: row.provisioned_at || row.created_at || null,
  };
}

async function getTenantWorkspace({
  pool = defaultPool,
  store,
  clientId,
  aimLookup,
} = {}) {
  const id = Number(clientId);
  if (!Number.isFinite(id) || id <= 0) {
    const err = new Error('client_id is required');
    err.status = 400;
    err.code = 'tenant_validation';
    throw err;
  }

  if (!store && pool) {
    await ensureTenantWorkspaceSchema(pool);
  }
  const mem = store || (pool ? createPostgresTenantStore(pool) : null);
  if (!mem) {
    const err = new Error('Tenant store unavailable');
    err.status = 500;
    throw err;
  }

  const client = await mem.getClient(id);
  if (!client) {
    const err = new Error('Client not found');
    err.status = 404;
    err.code = 'tenant_not_found';
    throw err;
  }

  let workspace = await mem.getWorkspace(id);
  if (!workspace && store) {
    const ns = namespacesFor(id, client.slug);
    workspace = await mem.insertWorkspace({
      client_id: id,
      tenant_key: client.slug || slugify(client.name),
      ...ns,
      created_by: null,
    });
  }

  let counts = {
    prospects: 0,
    missions: 0,
    outcomes: 0,
    knowledge: 0,
    blueprints: 0,
    approvedBlueprints: 0,
    aims: 0,
    draftAims: 0,
    campaigns: 0,
    memory: 0,
  };
  if (store && typeof store.listNamespace === 'function') {
    const aims = store.listNamespace('aim', id);
    const blueprints = store.listNamespace('blueprints', id);
    counts = {
      prospects: store.listNamespace('prospects', id).length,
      missions: store.listNamespace('missions', id).length,
      outcomes: store.listNamespace('outcomes', id).length,
      knowledge: store.listNamespace('knowledge', id).length,
      blueprints: blueprints.length,
      approvedBlueprints: blueprints.filter((b) => b && b.status === 'approved').length,
      aims: aims.filter((a) => a && (a.status === 'published' || a.status === 'complete')).length,
      draftAims: aims.filter((a) => a && a.status !== 'published' && a.status !== 'complete').length,
      campaigns: store.listNamespace('campaigns', id).length,
      memory: store.listNamespace('memory', id).length,
    };
  } else if (pool) {
    counts = await loadLiveCounts(pool, id);
  }

  const publishedAim = await getPublishedAimForTenant({
    pool,
    store,
    clientId: id,
    aimLookup,
  });
  if (publishedAim) counts.aims = Math.max(counts.aims, 1);

  const status = statusFromCounts(counts, { aimPresent: Boolean(publishedAim) });
  const publicWs = publicWorkspace(workspace);
  const lifecycleStage = deriveWorkspaceLifecycle(status, publicWs?.lifecycle);
  if (publicWs) publicWs.lifecycle = lifecycleStage;
  return {
    client: publicTenant(client),
    workspace: publicWs,
    status,
    lifecycle: publicLifecycle(lifecycleStage),
    greeting: status.needsOnboarding
      ? greetingForWorkspace(publicWs, client.name)
      : null,
    publishedAim: publishedAim
      ? { id: publishedAim.id, status: publishedAim.status, client_id: publishedAim.client_id }
      : null,
  };
}

function activateTenant(session, clientId) {
  const id = Number(clientId);
  if (!session || !Number.isFinite(id) || id <= 0) {
    const err = new Error('Cannot activate tenant without a session and client_id');
    err.status = 400;
    err.code = 'tenant_validation';
    throw err;
  }
  session.active_client_id = id;
  return { ok: true, active_client_id: id };
}

function assertSameNamespace(workspace, kind, tenantId) {
  const key = `${kind}_namespace`;
  const expected = `tenant:${tenantId}:${kind === 'aim' ? `aim:${workspace.tenant_key}` : kind.replace(/s$/, '')}`;
  if (kind === 'aim') {
    return workspace.aim_namespace === `tenant:${tenantId}:aim:${workspace.tenant_key}`;
  }
  const suffix = {
    knowledge: 'knowledge',
    mission: 'mission',
    prospect: 'prospect',
    outcome: 'outcome',
  }[kind] || kind;
  return workspace[key] === `tenant:${tenantId}:${suffix}`;
}

module.exports = {
  REQUIRED_FIELDS,
  ensureTenantWorkspaceSchema,
  CLIENT_INTEL_NOT_STARTED,
  AIM_NONE,
  NEW_TENANT_AGENTS,
  slugify,
  namespacesFor,
  initialWorkspaceStatus,
  validateCreateClientInput,
  createMemoryTenantStore,
  createAndProvisionTenant,
  getTenantWorkspace,
  getPublishedAimForTenant,
  activateTenant,
  publicTenant,
  publicWorkspace,
  buildTenantGreeting,
  greetingForWorkspace,
  assertSameNamespace,
  statusFromCounts,
};
