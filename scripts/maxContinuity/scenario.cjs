'use strict';
// Shared #1107 HTTP assertions; never starts the application.
const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const http = require('node:http');
const express = require('express');
const session = require('express-session');
const { makeHarness, fixturePassword } = require('../../test/helpers/loginIdentityHarness');
const { createWorkspaceEngine } = require('../../packages/max/workspace/WorkspaceEngine');
const { createTestAmoRuntime } = require('../../packages/max/workspace/tests/amoTestRuntime');
const { createMemoryStore } = require('../../services/clientIntelligenceInterview');
const { createMemoryStore: objectiveStore } = require('../../services/operatorObjectives');
const { ConversationContinuity } = require('../../packages/max/context/ConversationContinuity');
const { ConversationContextStore } = require('../../packages/max/context/ConversationContextStore');
const amo = require('../../packages/acquisition-mission');


function loadRouter(auth, runtime, guard) {
  const deny = guard.deny;
  const dependencies = {
    express,
    '../middleware/auth': auth,
    '../utils/maxRuntime': { getMaxRuntime: async () => runtime() },
    '../utils/clientContext': { normalizeClientId: value => String(value) },
    '../packages/max/workspace/TenantContextResolver': require('../../packages/max/workspace/TenantContextResolver'),
    '../utils/clientFacingPresentation': { presentMaxResultForClient: value => value },
    '../services/apiUsageService': { isApiUsageQuestion: () => false, parseApiSpendCommand: () => null, answerApiUsageQuestion: deny },
    '../utils/meteredAnthropic': { forAgent: () => class { constructor() { deny(); } } },
    '../utils/meteredGemini': { createGeminiClient: deny },
    '../services/reviewNavigation': { reviewNavigationForRequest: async () => null },
    './reviewNavigation': { createReviewNavigationRouter: () => express.Router() },
    '../services/tenantWorkspace': { getTenantWorkspace: async () => ({ status: {}, client: { name: 'Synthetic tenant' } }) },
    '../services/pilotOnboarding': { maxAcquisitionReply: () => null },
    '../services/aicPersistence': { loadPublishedAimForClient: async () => null },
  };
  const module = { exports: {} };
  vm.runInNewContext(fs.readFileSync(require('node:path').join(__dirname, '../../routes/maxWorkspace.js'), 'utf8'), {
    module, process: { env: {} }, console: { info() {}, error() {} }, URL,
    require(name) {
      if (!Object.hasOwn(dependencies, name)) throw new Error(`Unapproved route dependency: ${name}`);
      return dependencies[name];
    },
  }, { filename: 'routes/maxWorkspace.js' });
  return module.exports;
}

async function request(port, path, body, cookie) {
  const payload = JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = http.request({ host: '127.0.0.1', port, path, method: 'POST', headers: {
      'content-type': 'application/json', 'content-length': Buffer.byteLength(payload), ...(cookie ? { cookie } : {}),
    } }, res => {
      let data = '';
      res.on('data', chunk => { data += chunk; });
      res.on('end', () => resolve({ status: res.statusCode, body: res.headers['content-type']?.includes('application/json') ? JSON.parse(data) : data,
        cookie: res.headers['set-cookie']?.[0]?.split(';')[0] }));
    });
    req.setTimeout(5000, () => req.destroy(new Error('http_timeout')));
    req.on('error', reject); req.end(payload);
  });
}


async function runScenario({ db, t, guard, checkpoint = () => {} }) {
  // Mission referents are reread from this isolated database, never from a client envelope.
  await db.query('CREATE TABLE fixture_missions (id text, tenant_id text)');
  await db.query("INSERT INTO fixture_missions VALUES ('mission-1','10')");
  let loads = 0;
  const continuities = [];
  function replacement() {
    const continuity = new ConversationContinuity({ env: { MAX_CONVERSATION_CONTEXT: 'true' }, store: new ConversationContextStore({ db }) });
    continuities.push(continuity);
    const workspace = createWorkspaceEngine({ disableLlm: true,
      acquisitionMissionRuntime: createTestAmoRuntime(),
      clientIntelligenceOpts: { store: createMemoryStore() }, operatorObjectiveOpts: { store: objectiveStore() },
      conversationContinuity: continuity,
      conversationReferenceLoader: async ({ tenantId, missionId }) => {
        loads++;
        const { rows } = await db.query('SELECT id FROM fixture_missions WHERE id=$1 AND tenant_id=$2', [missionId, tenantId]);
        return { found: rows.length === 1, mission: rows[0] };
      },
    });
    return { workspace, openWorkspace: input => workspace.open(input), askWorkspace: input => workspace.ask(input) };
  }
  t.after(async () => { for (const c of continuities) await c.close(); });
  let runtime = replacement();
  const auth = makeHarness({ user: { client_id: 10 } });
  const app = express();
  app.use(express.json());
  app.use(session({ secret: require('node:crypto').randomBytes(32).toString('hex'), resave: false, saveUninitialized: false }));
  app.post('/login', ...auth.handlers['POST /login']);
  app.use(loadRouter(auth.auth, () => runtime, guard));
  const server = app.listen(0, '127.0.0.1');
  await new Promise(resolve => server.once('listening', resolve));
  const port = server.address().port;
  guard.allowHttp(port);
  t.after(() => new Promise(resolve => { server.closeAllConnections(); server.close(resolve); }));
  const ask = (id, cookie, question = 'Approve it.') => request(port, '/api/v1/max/workspace/ask', { sessionId: id, question }, cookie);
  assert.equal((await ask('unknown')).status, 401);
  const login = await request(port, '/login', { email: 'identity@example.test', password: fixturePassword });
  assert.equal(login.status, 302);
  const cookie = login.cookie;
  const opened = await request(port, '/api/v1/max/workspace/open', { page: 'command-deck', missionId: 'mission-1' }, cookie);
  assert.equal(opened.status, 200);
  checkpoint('authenticated_http');
  const id = opened.body.sessionId;
  assert.ok(id);
  const original = runtime.workspace.sessions.get(id);
  original.context.customerData = 'CUSTOMER_SECRET';
  original.context.approval = { approved: true };
  original.context.executionPolicy = { allow: true };
  Object.assign(original, { pendingApproval: { approved: true }, executionAuthority: true, customerData: 'CUSTOMER_SECRET' });
  original.messages.push({ role: 'user', content: 'TRANSCRIPT_SECRET' });
  assert.equal((await ask(id, cookie, 'Hello')).status, 200);
  await continuities[0].drain();
  const durable = (await db.query('SELECT context FROM max_conversation_context WHERE session_id=$1', [id])).rows[0].context;
  assert.deepEqual(durable.candidates, ['mission-1']);
  assert.doesNotMatch(JSON.stringify(durable), /SECRET|approved|executionAuthority|pendingApproval/);

  checkpoint('durable_projection');
  runtime = replacement();
  assert.equal(runtime.workspace.sessions.get(id), null);
  // Foreign authenticated identity is established through the same login handler.
  auth.user.client_id = 11;
  const foreignLogin = await request(port, '/login', { email: 'identity@example.test', password: fixturePassword });
  auth.user.client_id = 10;
  assert.equal((await ask(id, foreignLogin.cookie)).status, 400);
  assert.equal(runtime.workspace.sessions.get(id), null);
  assert.deepEqual((await db.query('SELECT context FROM max_conversation_context WHERE session_id=$1', [id])).rows[0].context, durable, 'foreign request cannot rewrite owner context');
  amo.clearExecutionRouterAudit();
  // Inspect recovery before the new question can append its own transcript.
  const recover = runtime.workspace.recoverConversationSession.bind(runtime.workspace);
  let recoveredSnapshot;
  runtime.workspace.recoverConversationSession = async input => {
    const result = await recover(input);
    if (result.recovered) recoveredSnapshot = structuredClone(runtime.workspace.sessions.get(input.sessionId));
    return result;
  };
  const before = loads;
  const responses = await Promise.all([ask(id, cookie), ask(id, cookie)]);
  assert.deepEqual(responses.map(x => x.status), [200, 200]);
  assert.equal(loads - before, 1, 'concurrent recovery coalesces reference validation');
  assert.deepEqual(recoveredSnapshot.messages, []);
  assert.equal(recoveredSnapshot.context.requiresEvidenceRefresh, true);
  assert.equal(recoveredSnapshot.context.missionId, 'mission-1');
  assert.doesNotMatch(JSON.stringify(recoveredSnapshot), /SECRET|pendingApproval|executionAuthority|customerData|executionPolicy|"approval"/);
  assert.deepEqual(amo.listExecutionRouterAudit(), []);
  assert.equal((await ask(id, foreignLogin.cookie)).status, 403);

  checkpoint('concurrent_recovery_and_tenant_isolation');
  for (const variant of ['expired', 'corrupt', 'missing-mission']) {
    await continuities.at(-1).drain();
    await db.query(`UPDATE max_conversation_context SET context=$2::jsonb, expires_at=$3 WHERE session_id=$1`,
      [id, JSON.stringify(variant === 'corrupt' ? { ...durable, approval: true } : variant === 'missing-mission' ? { ...durable, candidates: ['gone'], focusId: 'gone' } : durable),
        new Date(Date.now() + (variant === 'expired' ? -1000 : 60000))]);
    runtime = replacement();
    assert.equal((await ask(id, cookie)).status, 400, variant);
    assert.equal(runtime.workspace.sessions.get(id), null, variant);
    checkpoint(variant);
  }
  // Real pool failure, with no remote connection attempt: normal workspace traffic remains available.
  await db.end();
  runtime = replacement();
  const fresh = await request(port, '/api/v1/max/workspace/open', { page: 'command-deck' }, cookie);
  assert.equal(fresh.status, 200);
  assert.equal((await ask(fresh.body.sessionId, cookie, 'Hello')).status, 200);
  await continuities.at(-1).drain();
  assert.equal((await ask(id, cookie)).status, 400);
  assert.deepEqual(amo.listExecutionRouterAudit(), []);
  guard.assertClean();
  checkpoint('database_outage_and_no_authority');
}
module.exports = { runScenario };
