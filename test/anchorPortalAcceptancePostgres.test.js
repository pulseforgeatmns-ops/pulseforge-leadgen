'use strict';
const { test, before, after } = require('node:test');
const assert = require('node:assert/strict');
const { createExeterPortalHarness, illustrativePhoto, DEMO_NAME, DEMO_NOTICE } = require('./helpers/exeterPortalHarness');
let h; let client; let operator;
const api = '/api/anchor-portal';
before(async () => { h = await createExeterPortalHarness(); client = await h.login('client'); operator = await h.login('operator'); });
after(async () => { if (h) await h.close(); });

async function call(path, cookie = client, method = 'GET', body) { return h.request(`${api}${path}`, { cookie, method, body }); }

test('synthetic fixture is labelled, has three reports/photos and open/resolved proof', async () => {
  const me = await call('/me'); assert.equal(me.status, 200); assert.equal(me.body.locations.length, 1);
  assert.equal(me.body.locations[0].name, DEMO_NAME); assert.equal(me.body.locations[0].is_demo, true);
  const reports = await call(`/visits?location_id=${h.location.id}`); assert.equal(reports.body.visits.length, 3);
  for (const report of reports.body.visits) {
    const detail = await call(`/visits/${report.id}`); assert.match(detail.body.visit.visit_notes, /Synthetic sample/);
    assert.equal(detail.body.visit.evidence.length, 1); assert.match(detail.body.visit.evidence[0].caption, /Synthetic illustration/);
  }
  const issues = await call(`/issues?location_id=${h.location.id}`);
  assert.ok(issues.body.issues.some(i => i.status === 'open'));
  assert.ok(issues.body.issues.find(i => i.status === 'resolved').evidence.some(p => p.purpose === 'resolution'));
  assert.match(DEMO_NOTICE, /does not represent actual service/);
});

test('report creation persists dates, areas, author and three photos across session reload', async () => {
  const created = await call('/visits', operator, 'POST', {
    location_id: h.location.id, service_date: '2026-10-14T14:00:00Z', visit_notes: 'Synthetic acceptance report',
    areas: [{ title: 'Synthetic renovated classroom', notes: 'Illustrative final detail', outcome: 'completed' }],
    photos: [illustrativePhoto(0), illustrativePhoto(1), illustrativePhoto(2)],
  });
  assert.equal(created.status, 201, JSON.stringify(created.body));
  const id = created.body.visit.id;
  const reloaded = await call(`/visits/${id}`, await h.login('client'));
  assert.equal(reloaded.status, 200); assert.equal(reloaded.body.visit.status, 'completed');
  assert.equal(reloaded.body.visit.evidence.length, 3); assert.ok(reloaded.body.visit.items.length);
  assert.equal(reloaded.body.visit.completed_by, h.users.operator.id);
  const persisted = await h.pool.query('SELECT completed_by FROM anchor_portal_visits WHERE id=$1', [id]);
  assert.equal(persisted.rows[0].completed_by, h.users.operator.id);
});

test('client problem/photo → operator in progress → resolution proof persists on client reload', async () => {
  const created = await call('/issues', client, 'POST', { location_id: h.location.id, description: 'Synthetic dust concern', room_category: 'Construction Zone A', issue_type: 'cleaning_quality', severity: 'high', photo_data_url: illustrativePhoto(0).data_url });
  assert.equal(created.status, 201, JSON.stringify(created.body)); const id = created.body.issue.id;
  const progress = await call(`/issues/${id}`, operator, 'PATCH', { status: 'in_progress' });
  assert.equal(progress.status, 200); assert.ok(['in_progress', 'acknowledged'].includes(progress.body.issue.status));
  const resolved = await call(`/issues/${id}`, operator, 'PATCH', { status: 'resolved', resolution: 'Synthetic detail pass verified.', resolution_photo_data_url: illustrativePhoto(1).data_url, resolution_photo_caption: 'Synthetic resolution illustration' });
  assert.equal(resolved.status, 200, JSON.stringify(resolved.body));
  const read = await call(`/issues?location_id=${h.location.id}`, await h.login('client'));
  const issue = read.body.issues.find(i => i.id === id); assert.equal(issue.status, 'resolved');
  assert.equal(issue.resolution, 'Synthetic detail pass verified.');
  assert.ok(issue.evidence.some(e => e.purpose === 'reported')); assert.ok(issue.evidence.some(e => e.purpose === 'resolution'));
});

test('invalid/oversize images and mismatched visit reject atomically', async () => {
  const counts = async () => (await h.pool.query(`SELECT (SELECT count(*) FROM anchor_portal_issues)::int issues,(SELECT count(*) FROM anchor_portal_evidence)::int evidence,(SELECT count(*) FROM anchor_portal_visits)::int visits`)).rows[0];
  const initial = await counts();
  for (const data_url of ['data:image/png;base64,aGVsbG8=', `data:image/png;base64,${Buffer.alloc(1900000).toString('base64')}`]) {
    const result = await call('/issues', client, 'POST', { location_id: h.location.id, description: 'Must not persist', photo_data_url: data_url });
    assert.ok(result.status >= 400 && result.status < 500, JSON.stringify(result)); assert.deepEqual(await counts(), initial);
  }
  const mismatch = await call('/issues', client, 'POST', { location_id: h.location.id, visit_id: h.foreignVisit.id, description: 'Mismatched foreign visit' });
  assert.ok(mismatch.status >= 400 && mismatch.status < 500); assert.deepEqual(await counts(), initial);
  const badReport = await call('/visits', operator, 'POST', { location_id: h.location.id, service_date: '2026-10-14', visit_notes: 'Invalid', areas: [{title:'Synthetic zone'}], photos: [illustrativePhoto(0), {data_url:'data:image/png;base64,aGVsbG8='}] });
  assert.ok(badReport.status >= 400 && badReport.status < 500); assert.deepEqual(await counts(), initial);
  const badResolution = await call(`/issues/${h.issues[0].id}`, operator, 'PATCH', {status:'resolved',resolution:'Synthetic',resolution_photo_data_url:'data:image/png;base64,aGVsbG8='});
  assert.ok(badResolution.status >= 400 && badResolution.status < 500);
  assert.equal((await h.pool.query('SELECT status FROM anchor_portal_issues WHERE id=$1',[h.issues[0].id])).rows[0].status, 'open');
});

test('tenant/location/visit/issue reads and writes are isolated; expired sessions reject', async () => {
  assert.equal((await call('/me', null)).status, 401);
  const unassigned = await h.login('unassigned'); assert.deepEqual((await call('/me', unassigned)).body.locations, []);
  for (const identity of ['foreignOperator', 'foreignClient', 'unassigned']) {
    const cookie = await h.login(identity);
    for (const path of [`/visits/${h.visits[0].id}`, `/issues?location_id=${h.location.id}`, `/locations/${h.location.id}/scope`]) {
      const result = await call(path, cookie); assert.ok([403,404].includes(result.status), `${identity} ${path}: ${JSON.stringify(result)}`);
    }
    const result = await call(`/issues/${h.issues[0].id}`, cookie, 'PATCH', {status:'resolved',resolution:'Forbidden'}); assert.ok([403,404].includes(result.status));
  }
  for (const path of [`/visits/${h.foreignVisit.id}`, `/issues?location_id=${h.unassignedLocation.id}`]) assert.ok([403,404].includes((await call(path)).status));
  assert.equal((await call('/visits', client, 'POST', {location_id:h.location.id})).status, 403);
  const cookie = await h.login('client'); await h.request('/__test/logout', {cookie,method:'POST'});
  assert.equal((await call('/me', cookie)).status, 401);
});

test('duplicate issue/report requests are idempotent and database photo failure rolls back parent rows', async () => {
  const payload = { location_id:h.location.id,description:'Synthetic repeated submit',request_id:'synthetic-issue-retry-1',photo_data_url:illustrativePhoto(0).data_url };
  const [first, second] = await Promise.all([call('/issues',client,'POST',payload),call('/issues',client,'POST',payload)]);
  assert.equal(first.status,201,JSON.stringify(first.body)); assert.equal(second.status,201,JSON.stringify(second.body)); assert.equal(first.body.issue.id,second.body.issue.id);
  const report = {location_id:h.location.id,service_date:'2026-10-14',visit_notes:'Synthetic repeat',areas:[{title:'Synthetic renovation zone'}],photos:[illustrativePhoto(1)],request_id:'synthetic-report-retry-1'};
  const [a,b] = await Promise.all([call('/visits',operator,'POST',report),call('/visits',operator,'POST',report)]);
  assert.equal(a.status,201,JSON.stringify(a.body)); assert.equal(b.status,201,JSON.stringify(b.body)); assert.equal(a.body.visit.id,b.body.visit.id);
  const before = (await h.pool.query('SELECT count(*)::int n FROM anchor_portal_issues')).rows[0].n;
  await h.pool.query(`CREATE FUNCTION synthetic_reject_photo() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'Synthetic insertion fault'; END $$; CREATE TRIGGER synthetic_reject_photo BEFORE INSERT ON anchor_portal_evidence FOR EACH ROW EXECUTE FUNCTION synthetic_reject_photo()`);
  try {
    const failed = await call('/issues',client,'POST',{location_id:h.location.id,description:'Synthetic rollback assertion',photo_data_url:illustrativePhoto(0).data_url});
    assert.equal(failed.status,500);
    assert.equal((await h.pool.query('SELECT count(*)::int n FROM anchor_portal_issues')).rows[0].n,before);
  } finally { await h.pool.query('DROP TRIGGER synthetic_reject_photo ON anchor_portal_evidence; DROP FUNCTION synthetic_reject_photo()'); }
});

test('cleaner ownership and completion guards protect report/photos', async () => {
  const cookie = await h.login('cleaner');
  const {rows} = await h.pool.query(`INSERT INTO anchor_portal_visits(location_id,cleaner_id,status,scheduled_at) VALUES ($1,$2,'scheduled',NOW()),($1,$3,'scheduled',NOW()) RETURNING *`,[h.location.id,h.users.operator.id,h.users.cleaner.id]);
  for (const action of ['start','complete','evidence']) {
    const result = await call(`/visits/${rows[0].id}/${action}`,cookie,'POST',action==='evidence'?illustrativePhoto(0):{});
    assert.equal(result.status,403,JSON.stringify(result.body));
  }
  const premature = await call(`/visits/${rows[1].id}/complete`,cookie,'POST',{}); assert.ok(premature.status>=400&&premature.status<500);
  const started = await call(`/visits/${rows[1].id}/start`,cookie,'POST',{}); assert.equal(started.status,200);
  const items = started.body.visit.items.map(item=>({id:item.id,outcome:'completed'}));
  const completed=await call(`/visits/${rows[1].id}/complete`,cookie,'POST',{visit_notes:'Synthetic completion guard check',items,photos:[illustrativePhoto(2)]}); assert.equal(completed.status,200,JSON.stringify(completed.body));
  const repeated=await call(`/visits/${rows[1].id}/complete`,cookie,'POST',{visit_notes:'Must not change completed report'}); assert.equal(repeated.status,200);
  assert.equal(repeated.body.visit.completed_at,completed.body.visit.completed_at);
  assert.equal(repeated.body.visit.evidence.length,1);
  assert.equal(repeated.body.visit.completed_by,h.users.cleaner.id);
});

test('HTTPS public origin works behind TLS termination and cross-origin writes are denied', async()=>{
  const previous=process.env.APP_URL;process.env.APP_URL='https://portal.example.test';
  try {
    const body={location_id:h.location.id,description:'Synthetic proxy origin check'};
    for(const [origin,expected] of [['https://portal.example.test',201],['https://attacker.example.test',403]]){
      const response=await fetch(`${h.base}${api}/issues`,{method:'POST',headers:{Cookie:client,'Content-Type':'application/json',Origin:origin,'X-Forwarded-Proto':'https'},body:JSON.stringify(body)});
      assert.equal(response.status,expected,await response.text());
    }
  } finally {if(previous===undefined)delete process.env.APP_URL;else process.env.APP_URL=previous;}
});

test('cross-building visit links reject atomically and capped open history cannot hide resolution',async()=>{
  const bad=await call('/issues',client,'POST',{location_id:h.location.id,visit_id:h.foreignVisit.id,description:'Invalid foreign service link',photo_data_url:illustrativePhoto(0).data_url});
  assert.equal(bad.status,400);
  await h.pool.query(`INSERT INTO anchor_portal_issues (location_id,description,status) SELECT $1,'Synthetic pagination issue ' || value,'open' FROM generate_series(1,110) AS value`,[h.location.id]);
  const updated=await call(`/issues/${h.issues[0].id}`,operator,'PATCH',{status:'resolved',resolution:'Synthetic pagination resolution',resolution_photo_data_url:illustrativePhoto(0).data_url});
  assert.equal(updated.status,200);assert.equal(updated.body.issue.id,h.issues[0].id);assert.equal(updated.body.issue.evidence.length,1);
  const resolved=await call(`/issues?location_id=${h.location.id}&status=resolved`);
  assert.ok(resolved.body.issues.some(issue=>issue.id===h.issues[0].id));
  const more=await call(`/issues?location_id=${h.location.id}&status=active&offset=100`);
  assert.ok(more.body.issues.length>0);
});

test('concurrent partial issue edits derive status from the locked row',async()=>{
  const created=await call('/issues',client,'POST',{location_id:h.location.id,description:'Synthetic locked status test'});
  const id=created.body.issue.id;
  const blocker=await h.pool.connect();await blocker.query('BEGIN');await blocker.query('SELECT id FROM anchor_portal_issues WHERE id=$1 FOR UPDATE',[id]);
  async function waitForLocks(count){
    const deadline=Date.now()+3000;
    while(Date.now()<deadline){const {rows}=await h.pool.query(`SELECT count(*)::int n FROM pg_stat_activity WHERE wait_event_type='Lock' AND query LIKE 'SELECT * FROM anchor_portal_issues WHERE id = $1 FOR UPDATE%'`);if(rows[0].n>=count)return;await new Promise(resolve=>setTimeout(resolve,10));}
    throw new Error('Expected issue transactions did not reach lock');
  }
  let resolution,partial;
  try {
    resolution=call(`/issues/${id}`,operator,'PATCH',{status:'resolved',resolution:'Synthetic locked resolution'});await waitForLocks(1);
    partial=call(`/issues/${id}`,operator,'PATCH',{resolution:'Synthetic additional resolution note'});await waitForLocks(2);
    await blocker.query('COMMIT');
    assert.equal((await resolution).status,200);assert.equal((await partial).status,200);
    assert.equal((await h.pool.query('SELECT status FROM anchor_portal_issues WHERE id=$1',[id])).rows[0].status,'resolved');
  } finally {await blocker.query('ROLLBACK');blocker.release();await Promise.allSettled([resolution,partial].filter(Boolean));}
});
