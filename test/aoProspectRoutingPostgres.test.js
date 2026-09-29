'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const test = require('node:test');
const { Pool } = require('pg');
const { startDisposablePostgres } = require('./helpers/disposablePostgres');

const root = path.join(__dirname, '..');
const migration = fs.readFileSync(path.join(root, 'migrations', '2026-09-22-ao-prospect-routing.sql'), 'utf8');
const rollback = fs.readFileSync(path.join(root, 'migrations', '2026-09-22-ao-prospect-routing.rollback.sql'), 'utf8');

const ids = {
  company10: '00000000-0000-0000-0000-000000000010',
  company11: '00000000-0000-0000-0000-000000000011',
  p1: '10000000-0000-0000-0000-000000000001',
  p2: '10000000-0000-0000-0000-000000000002',
  p3: '10000000-0000-0000-0000-000000000003',
  p4: '10000000-0000-0000-0000-000000000004',
  foreign: '11000000-0000-0000-0000-000000000001',
};

async function baseSchema(db) {
  await db.query(`
    CREATE TABLE clients(id INTEGER PRIMARY KEY, name TEXT);
    CREATE TABLE users(
      id INTEGER PRIMARY KEY, client_id INTEGER NOT NULL REFERENCES clients(id),
      name TEXT, email TEXT, role TEXT, active BOOLEAN DEFAULT true, territory TEXT
    );
    CREATE TABLE companies(
      id UUID PRIMARY KEY, client_id INTEGER NOT NULL REFERENCES clients(id),
      name TEXT, location TEXT, website TEXT, industry TEXT
    );
    CREATE TABLE prospects(
      id UUID PRIMARY KEY, client_id INTEGER NOT NULL REFERENCES clients(id),
      company_id UUID REFERENCES companies(id), first_name TEXT, last_name TEXT,
      email TEXT, phone TEXT, vertical TEXT, icp_score INTEGER, status TEXT,
      service_area_match TEXT, do_not_contact BOOLEAN DEFAULT false, is_hot BOOLEAN DEFAULT false,
      next_action_due_at TIMESTAMPTZ, next_action_status TEXT,
      created_at TIMESTAMPTZ DEFAULT NOW(), updated_at TIMESTAMPTZ DEFAULT NOW()
    );
    CREATE INDEX prospects_next_action_idx
      ON prospects(client_id, next_action_status, next_action_due_at)
      WHERE next_action_status = 'pending';
    CREATE TABLE touchpoints(
      id BIGSERIAL PRIMARY KEY, client_id INTEGER NOT NULL, prospect_id UUID NOT NULL,
      action_type TEXT, channel TEXT, outcome TEXT, created_at TIMESTAMPTZ DEFAULT NOW()
    );
  `);
}

async function seed(db) {
  await db.query("INSERT INTO clients VALUES (10,'Anchor'),(11,'Other')");
  await db.query(`INSERT INTO users(id,client_id,name,email,role,active,territory) VALUES
    (101,10,'AO One','one@example.com','ao',true,'Manchester'),
    (102,10,'AO Two','two@example.com','ao',true,'Manchester'),
    (201,11,'Foreign AO','foreign@example.com','ao',true,'Manchester')`);
  await db.query(`INSERT INTO companies(id,client_id,name,location) VALUES
    ($1,10,'One Property Management','Manchester, NH'),
    ($2,11,'Foreign Company','Manchester, NH')`, [ids.company10, ids.company11]);
  for (const [id, clientId, companyId, suffix] of [
    [ids.p1, 10, ids.company10, 'one'],
    [ids.p2, 10, ids.company10, 'two'],
    [ids.p3, 10, ids.company10, 'three'],
    [ids.p4, 10, ids.company10, 'four'],
    [ids.foreign, 11, ids.company11, 'foreign'],
  ]) {
    await db.query(`INSERT INTO prospects(
      id,client_id,company_id,first_name,last_name,email,phone,vertical,icp_score,status,service_area_match
    ) VALUES($1,$2,$3,'Pat',$4,$5,'6035550101','property_manager',85,'cold','Manchester, NH')`,
    [id, clientId, companyId, suffix, `${suffix}@example.com`]);
  }
}

function validDebrief(owner) {
  return {
    person_spoken_to: 'Office manager', role: 'Office manager', decision_maker: 'Facilities director',
    current_cleaning_solution: 'Incumbent vendor', stated_context: 'Needs backup coverage.',
    problem_or_risk: 'Incumbent misses turnovers and they need backup coverage.',
    opportunity_timing: 'later', opportunity_type: 'backup_overflow', opportunity_strength: 'moderate',
    recommended_next_step: 'Follow up about backup coverage.',
    follow_up_due_at: '2026-10-15T14:00:00.000Z', next_owner: String(owner),
    real_reason_to_continue: true, prescribed_before_diagnosing: false,
  };
}

test('AO routing enforces tenant ownership, active uniqueness, suppression, and atomic debriefs', {
  timeout: 180000,
}, async () => {
  const postgres = await startDisposablePostgres('ao-routing-pg-');
  const db = new Pool({ connectionString: postgres.connectionString });
  try {
    await baseSchema(db);
    await seed(db);
    await db.query(migration);
    await db.query(migration);

    const taskService = require('../services/aoProspectTaskService');
    const debriefService = require('../services/aoAdvisoryDebriefService');

    await assert.rejects(
      db.query(`INSERT INTO ao_prospect_tasks(
        client_id,prospect_id,assigned_ao_id,assignment_category,motion
      ) VALUES(10,$1,101,'HIGH_VALUE_ICP','AO_LED')`, [ids.foreign]),
      error => error.code === '23503'
    );
    await assert.rejects(
      db.query('UPDATE prospects SET assigned_ao_id=201 WHERE id=$1', [ids.p1]),
      error => error.code === '23503'
    );

    await Promise.all([
      taskService.generateWeeklyAoTasks({ clientId: 10, prospectIds: [ids.p4], db }),
      taskService.generateWeeklyAoTasks({ clientId: 10, prospectIds: [ids.p4], db }),
    ]);
    assert.equal((await db.query(`SELECT count(*)::int AS n FROM ao_prospect_tasks
      WHERE client_id=10 AND prospect_id=$1 AND status IN ('open','in_progress')`, [ids.p4])).rows[0].n, 1,
    'concurrent generation must converge on one active task');

    await taskService.generateWeeklyAoTasks({ clientId: 10, prospectIds: [ids.p1, ids.p2], db });
    await taskService.generateWeeklyAoTasks({ clientId: 10, prospectIds: [ids.p1, ids.p2], db });
    let active = (await db.query(`SELECT * FROM ao_prospect_tasks
      WHERE client_id=10 AND status IN ('open','in_progress') ORDER BY prospect_id`)).rows;
    const retried = active.filter(row => [ids.p1, ids.p2].includes(row.prospect_id));
    assert.equal(retried.length, 2, 'generation retries must not duplicate active tasks');
    assert.equal(new Set(retried.map(row => row.assigned_ao_id)).size, 2, 'batch allocation should account for assignments made in the same run');

    const p1Task = retried.find(row => row.prospect_id === ids.p1);
    await assert.rejects(
      db.query(`INSERT INTO ao_prospect_tasks(
        client_id,prospect_id,assigned_ao_id,assignment_category,motion
      ) VALUES(10,$1,$2,'HIGH_VALUE_ICP','AO_LED')`, [ids.p1, p1Task.assigned_ao_id]),
      error => error.code === '23505'
    );

    await db.query('UPDATE users SET active=false WHERE id=$1', [p1Task.assigned_ao_id]);
    await taskService.generateWeeklyAoTasks({ clientId: 10, prospectIds: [ids.p1], db });
    active = (await db.query(`SELECT * FROM ao_prospect_tasks
      WHERE client_id=10 AND prospect_id=$1 AND status IN ('open','in_progress')`, [ids.p1])).rows;
    assert.equal(active.length, 1);
    assert.notEqual(active[0].assigned_ao_id, p1Task.assigned_ao_id, 'reassignment must update the one active task');

    await db.query('UPDATE prospects SET do_not_contact=true WHERE id=$1', [ids.p1]);
    assert.equal((await db.query("SELECT status FROM ao_prospect_tasks WHERE prospect_id=$1", [ids.p1])).rows[0].status, 'cancelled');
    assert.equal((await taskService.listOpenTasks({ clientId: 10, db })).some(row => row.prospect_id === ids.p1), false);

    await taskService.generateWeeklyAoTasks({ clientId: 10, prospectIds: [ids.p3], db });
    const task = (await db.query("SELECT * FROM ao_prospect_tasks WHERE prospect_id=$1 AND status='open'", [ids.p3])).rows[0];
    await assert.rejects(
      debriefService.submitDebrief({ clientId: 11, prospectId: ids.p3, taskId: task.id, aoOwnerId: task.assigned_ao_id, debrief: validDebrief(task.assigned_ao_id), db }),
      error => error.statusCode === 404
    );
    const otherOwner = task.assigned_ao_id === 101 ? 102 : 101;
    await assert.rejects(
      debriefService.submitDebrief({ clientId: 10, prospectId: ids.p3, taskId: task.id, aoOwnerId: otherOwner, debrief: validDebrief(otherOwner), db }),
      error => error.statusCode === 403
    );

    const failingDb = {
      query: (...args) => db.query(...args),
      async connect() {
        const client = await db.connect();
        const originalQuery = client.query;
        const release = client.release.bind(client);
        const query = client.query.bind(client);
        client.query = (sql, params) => {
          return String(sql).includes('UPDATE prospects SET')
            ? Promise.reject(new Error('injected prospect update failure'))
            : query(sql, params);
        };
        client.release = () => {
          client.query = originalQuery;
          client.release = release;
          return release();
        };
        return client;
      },
    };
    await assert.rejects(
      debriefService.submitDebrief({ clientId: 10, prospectId: ids.p3, taskId: task.id, aoOwnerId: task.assigned_ao_id, debrief: validDebrief(task.assigned_ao_id), db: failingDb, ensureSchema: false }),
      /injected prospect update failure/
    );
    assert.equal((await db.query('SELECT count(*)::int AS n FROM ao_advisory_debriefs')).rows[0].n, 0,
      'failed logical save must roll back the inserted debrief');
    assert.equal((await db.query('SELECT status FROM ao_prospect_tasks WHERE id=$1', [task.id])).rows[0].status, 'open');

    const saved = await debriefService.submitDebrief({
      clientId: 10, prospectId: ids.p3, taskId: task.id, aoOwnerId: task.assigned_ao_id,
      debrief: validDebrief(task.assigned_ao_id), db,
    });
    assert.equal(saved.evaluation.next_action, 'AO_FOLLOW_UP');
    assert.equal((await db.query('SELECT status FROM ao_prospect_tasks WHERE id=$1', [task.id])).rows[0].status, 'completed');

    await db.query(rollback);
    const columns = (await db.query(`SELECT column_name FROM information_schema.columns
      WHERE table_name='prospects' AND column_name='next_action_due_at'`)).rows;
    assert.equal(columns.length, 1, 'rollback must preserve Max next_action_due_at');
    assert.equal((await db.query("SELECT to_regclass('prospects_next_action_idx') AS name")).rows[0].name,
      'prospects_next_action_idx');
  } finally {
    await db.end();
    await postgres.stop();
  }
});
