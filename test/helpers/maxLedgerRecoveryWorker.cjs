'use strict';
// Child process for test/maxConversationLedgerPostgres.test.js. Each run is a
// separate Node process (a "restart"): `write` holds a conversation and
// persists it through the real ConversationContextStore; `recover` is a fresh
// engine that only has the durable row. Disposable PostgreSQL only.
process.env.DOTENV_CONFIG_QUIET = 'true';
(async () => {
  const config = JSON.parse(process.env.MAX_LEDGER_WORKER_CONFIG);
  if (!/^postgres(?:ql)?:\/\/[^@]*@?127\.0\.0\.1:/.test(config.connectionString)) throw new Error('disposable localhost database only');
  const { Pool } = require('pg');
  // The hermetic harness denies every socket. Keep that guard, and allow only
  // loopback connections to the one disposable PostgreSQL port under test.
  const net = require('node:net');
  const realConnect = net.Socket.prototype.connect;
  const { createHermeticWorkspaceEngine } = require('../../packages/max/workspace/tests/workspaceTestHarness');
  const guardedConnect = net.Socket.prototype.connect;
  const allowedPort = Number(new URL(config.connectionString).port);
  net.Socket.prototype.connect = function connect(...args) {
    const opts = args[0] && typeof args[0] === 'object' && !Array.isArray(args[0]) ? args[0] : { port: args[0], host: args[1] };
    if (opts.host === '127.0.0.1' && Number(opts.port) === allowedPort) return realConnect.apply(this, args);
    return guardedConnect.apply(this, args);
  };
  const { ConversationContinuity } = require('../../packages/max/context/ConversationContinuity');
  const { ConversationContextStore } = require('../../packages/max/context/ConversationContextStore');
  const { ConversationLedgerRecorder } = require('../../packages/max/context/ConversationLedgerRecorder');
  const env = { MAX_CONVERSATION_CONTEXT: 'true', MAX_CONVERSATION_LEDGER: 'true' };
  const db = new Pool({ connectionString: config.connectionString, connectionTimeoutMillis: 2000 });
  db.on('error', () => {});
  const continuity = new ConversationContinuity({ env, store: new ConversationContextStore({ db }) });
  const ws = createHermeticWorkspaceEngine({
    conversationContinuity: continuity,
    conversationLedger: new ConversationLedgerRecorder({ env }),
    // Stands in for the tenant-scoped mission read: only m_live still exists.
    conversationReferenceLoader: async ({ tenantId, missionId }) => (missionId === 'm_live'
      ? { found: true, mission: { id: 'm_live', tenantId } } : { found: false, mission: null }),
  });
  const out = { pid: process.pid };
  try {
    if (config.mode === 'write') {
      const opened = await ws.open({ tenantId: config.tenantId, page: 'command-deck' });
      const sid = opened.sessionId;
      const session = ws._sessions.get(sid);
      const recorder = ws._conversationLedger;
      // A record read exactly as production's record intelligence returns it.
      recorder.observe({ session, text: "Show me Levi's five strongest prospects. Don't change anything.", result: {
        workspaceOwnership: { owner: 'knowledge_retrieval' },
        structured: { metadata: { items: config.prospects.map((id) => ({ recordRef: `prospect:${config.tenantId}:${id}`, recordId: id, claim: 'Jane Borrower private notes' })) } },
      } });
      session.context.acquisitionMissionId = 'm_gone';
      recorder.observe({ session, text: 'Approved.', result: { workspaceOwnership: { owner: 'active_mission' }, resolution: { action: 'execution_approved' }, routingTrace: {} } });
      session.context.acquisitionMissionId = 'm_live';
      // A real engine turn: the ledger is updated after production answers.
      await ws.ask({ sessionId: sid, question: "Don't send anything yet. Which of those has the freshest information?" });
      await continuity.drain();
      out.sessionId = sid;
      out.ledger = ws.getConversationLedger({ tenantId: config.tenantId, sessionId: sid });
    } else {
      out.recovery = await ws.recoverConversationSession({ tenantId: config.tenantId, sessionId: config.sessionId });
      out.sessionRestored = Boolean(ws._sessions.get(config.sessionId));
      out.ledger = ws.getConversationLedger({ tenantId: config.tenantId, sessionId: config.sessionId });
    }
  } finally {
    await continuity.close().catch(() => {});
    await db.end().catch(() => {});
  }
  process.stdout.write(`${JSON.stringify(out)}\n`);
})().catch((error) => { process.stderr.write(`worker_failed ${error.message}\n`); process.exitCode = 1; });
