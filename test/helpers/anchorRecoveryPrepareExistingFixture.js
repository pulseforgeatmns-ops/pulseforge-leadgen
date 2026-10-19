'use strict';
const { hash, missionScope } = require('../../packages/acquisition-mission/DailyOutboundPolicy');
const prepare = require('../../services/anchorEmailRecoveryPrepareExisting');
const { recoveryFixture, clone } = require('./anchorRecoveryManifestFixture');

function prepareExistingFixture() {
  const f = recoveryFixture(); f.control.manifest = null; f.calls = []; f.inserts = []; f.committed = []; f.releases = [];
  f.unresolved = [f.unresolved[0]];
  f.operator = { id: 'local-operator', client_id: 10, role: 'admin', active: true };
  f.prospect.company_name = 'Fresh Synthetic Enterprise';
  const candidate = { candidateId: f.prospect.company_id, prospectId: f.prospect.id, companyId: f.prospect.company_id,
    recipientEmail: f.prospect.email, subject: f.item.snapshot.message.subject, body: f.item.snapshot.message.body,
    senderEmail: f.client.sender_email, senderName: f.client.sender_name, sendingDomain: f.client.sending_domain };
  const mission = { id: 'local-current-day-prepared-mission', tenantId: '10', clientId: 10, stage: 'ready',
    objective: 'Synthetic bounded first-touch preparation', targetSegment: 'Synthetic services',
    structuredMission: { region: 'Synthetic local region' }, constraints: ['Synthetic email-only first touch'], planCancelled: false };
  f.missionRow = { id: mission.id, tenant_id: '10', client_id: 10, stage: 'ready', status: 'active',
    objective: mission.objective, target_segment: mission.targetSegment, orchestration_mission_id: f.program.source_mission_id,
    payload: mission, created_at: '2026-10-19T13:00:00.000Z', updated_at: '2026-10-19T15:00:00.000Z' };
  f.program.scope_hash = hash(missionScope(mission));
  const queueItem = { id: candidate.candidateId, prospectId: candidate.candidateId, companyId: candidate.companyId,
    email: candidate.recipientEmail, sendable: true, dnc: false,
    paige: { candidateId: candidate.candidateId, subject: candidate.subject, body: candidate.body,
      bindingScope: 'prospect', attributableIntelligence: { source: 'synthetic-current-artifact' } } };
  const payloads = {
    scout: { candidateUniverse: [{ id: candidate.candidateId }] },
    max: { rankedTargets: [{ id: candidate.candidateId }] },
    paige: { variants: [{ label: 'Primary', candidateId: candidate.candidateId, companyId: candidate.companyId,
      companyName: f.prospect.company_name, subject: candidate.subject, body: candidate.body }] },
    emmett: { queue: { items: [queueItem] }, capacity: { recommended: 1 }, governor: { outcome: 'proceed', halt: false },
      senderIdentity: { senderEmail: candidate.senderEmail, sendingDomain: candidate.sendingDomain } },
  };
  const kinds = { scout: 'discovery', max: 'prioritization', paige: 'variants', emmett: 'capacity' };
  f.artifactRows = Object.entries(kinds).map(([specialist, kind]) => {
    const at = '2026-10-19T14:00:00.000Z';
    const contribution = { id: `local-current-${specialist}`, missionId: mission.id, specialist, kind, at, payload: payloads[specialist] };
    return { id: contribution.id, mission_id: mission.id, tenant_id: '10', specialist, kind, payload: contribution, at };
  });
  f.config = { version: 1, action: 'APPROVED_PREPARE_EXISTING', id: 'local-prepare-grant',
    approvalReference: 'local-synthetic-freeze-approval', operatorId: 'local-operator', operatorRole: 'admin', testMode: false, tenantId: '10', localDay: '2026-10-19',
    programId: f.program.id, policyHash: f.program.policy_hash, scopeHash: f.program.scope_hash, missionId: mission.id,
    missionHash: hash(f.missionRow.payload), contributionSetHash: prepare.contributionSetHash(f.artifactRows),
    preparedRevision: prepare.preparedRevision(mission.id, f.artifactRows.map(row => row.payload)),
    artifacts: Object.fromEntries(f.artifactRows.map(row => [row.specialist, { id: row.id, hash: hash(row.payload) }])),
    maxItems: 1, validFrom: '2026-10-19T15:30:00.000Z', expiresAt: '2026-10-19T16:30:00.000Z',
    quarantineDecisionHash: f.quarantine.decision_hash, candidate };
  f.env = { ANCHOR_EMAIL_RECOVERY_PREPARE_EXISTING_ID: f.config.id, ANCHOR_EMAIL_RECOVERY_MAINTENANCE: 'true', ANCHOR_GOVERNED_OUTBOUND_ENABLED: 'false' };
  f.clientConnection = {
    async query(config) {
      const sql = typeof config === 'string' ? config : config.text;
      const values = typeof config === 'string' ? [] : config.values;
      f.calls.push({ sql, values, timeout: config.query_timeout });
      if (f.fail?.test(sql)) throw Object.assign(new Error('synthetic failure'), { code: '42703' });
      if (/^BEGIN/.test(sql)) { f.staged = []; return { rows: [] }; }
      if (sql === 'ROLLBACK') { f.staged = []; return { rows: [] }; }
      if (sql === 'COMMIT') { f.committed.push(...f.staged); f.staged = []; return { rows: [] }; }
      if (/^SET LOCAL/.test(sql)) return { rows: [] };
      if (/pg_try_advisory/.test(sql)) return { rows: [{ locked: f.overlap !== true }] };
      if (/pg_advisory/.test(sql)) return { rows: [] };
      if (/^INSERT/.test(sql)) {
        f.inserts.push({ sql, values });
        if (f.conflict?.test(sql)) return { rows: [] };
        f.staged.push({ sql, values }); return { rows: [{ id: values[0] }] };
      }
      let rows;
      if (/to_regclass/.test(sql)) rows = [{ control_table: 'anchor_email_recovery_control' }];
      else if (/FROM anchor_email_recovery_control/.test(sql)) rows = f.control ? [f.control] : [];
      else if (/FROM acquisition_outbound_programs/.test(sql)) rows = [f.program];
      else if (/FROM acquisition_outbound_envelopes/.test(sql)) rows = f.existingEnvelope ? [{ id: 'already-frozen' }] : [];
      else if (/FROM acquisition_outbound_events/.test(sql)) rows = f.existingGrant ? [{ id: 'already-consumed' }] : [];
      else if (/FROM acquisition_missions/.test(sql)) rows = f.missionRow ? [f.missionRow] : [];
      else if (/FROM acquisition_mission_contributions/.test(sql)) rows = f.artifactRows;
      else if (/FROM users/.test(sql)) rows = f.operator ? [f.operator] : [];
      else if (/FROM clients/.test(sql)) rows = [f.client];
      else if (/FROM tenant_mailbox_integrations/.test(sql)) rows = [f.integration];
      else if (/SELECT p.id,p.client_id/.test(sql)) rows = f.prospect ? [f.prospect] : [];
      else if (/i.id AS item_id/.test(sql)) rows = f.unresolved;
      else if (/FROM anchor_email_quarantines/.test(sql)) rows = f.quarantine ? [f.quarantine] : [];
      else if (/FROM companies c WHERE c.client_id/.test(sql)) rows = f.companies;
      else if (/SELECT name,to_jsonb/.test(sql)) rows = [f.companies[0]];
      else if (/SELECT id FROM prospects p/.test(sql)) rows = f.suppression === 'review' ? [{ id: f.prospect.id }] : [];
      else if (/FROM acquisition_outbound_lifecycle/.test(sql)) rows = f.suppression === 'lifecycle' ? [{ state: 'dnc' }] : [];
      else if (/FROM acquisition_outbound_items/.test(sql)) rows = f.suppression === 'prior' ? [{ id: 'prior' }] : [];
      else if (/FROM acquisition_mission_outbound_executions/.test(sql)) rows = [];
      else if (/SELECT p.id FROM prospects/.test(sql)) rows = [];
      else if (/FROM ao_leads/.test(sql)) rows = [];
      else if (/FROM prospects p JOIN companies/.test(sql)) rows = [];
      else throw new Error(`Unexpected synthetic query: ${sql}`);
      if (f.onQuery) f.onQuery(sql);
      return { rows: clone(rows) };
    },
    release(discard) { f.releases.push(discard); },
  };
  f.pool = { async connect() { return f.clientConnection; } };
  return f;
}
module.exports = { prepareExistingFixture };
