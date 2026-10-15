'use strict';

const pool = require('../db');
const { validateRaster } = require('../utils/anchorPortalImages');
const {
  ANCHOR_OPERATOR_CLIENT_ID,
  copyScopeToVisitItems,
} = require('../utils/anchorPortalSchema');

class AnchorPortalError extends Error {
  constructor(code, message, status = 400) {
    super(message);
    this.code = code;
    this.status = status;
  }
}

const OUTCOMES_REQUIRING_REASON = new Set(['unable_to_complete', 'issue_detected']);
const OPERATOR_ROLES = new Set(['admin', 'manager']);
const CLIENT_ROLES = new Set(['facility_client']);
const CLEANER_ROLES = new Set(['cleaner']);

function isOperator(user) {
  return OPERATOR_ROLES.has(user?.role);
}

function isCleaner(user) {
  return CLEANER_ROLES.has(user?.role);
}

function isFacilityClient(user) {
  return CLIENT_ROLES.has(user?.role);
}

function assertPortalUser(user) {
  const platformAdmin = user?.role === 'admin' && user.client_id == null && user.id;
  if (!user?.id || user.active === false || (!platformAdmin && Number(user.client_id) !== ANCHOR_OPERATOR_CLIENT_ID)) {
    throw new AnchorPortalError('forbidden', 'You do not have access to this portal.', 403);
  }
}

async function userLocationIds(userId) {
  const { rows } = await pool.query(
    `SELECT ul.location_id FROM anchor_portal_user_locations ul
     JOIN anchor_portal_locations l ON l.id = ul.location_id
     WHERE ul.user_id = $1 AND l.operator_client_id = $2`, [userId, ANCHOR_OPERATOR_CLIENT_ID]);
  return rows.map(r => r.location_id);
}

async function assertLocationAccess(user, locationId) {
  assertPortalUser(user);
  const id = Number(locationId);
  if (!Number.isInteger(id) || id <= 0) throw new AnchorPortalError('invalid_location', 'Invalid location.', 400);
  const { rows } = await pool.query(
    `SELECT id FROM anchor_portal_locations WHERE id = $1 AND operator_client_id = $2`,
    [id, ANCHOR_OPERATOR_CLIENT_ID]);
  if (!rows.length || (!isOperator(user) && !(await userLocationIds(user.id)).includes(id))) {
    throw new AnchorPortalError('forbidden', 'You do not have access to this location.', 403);
  }
  return id;
}

async function listLocationsForUser(user) {
  assertPortalUser(user);
  const { rows } = await pool.query(
    `SELECT l.id, l.name, l.slug, l.address, l.is_demo, l.next_scheduled_at
     FROM anchor_portal_locations l
     WHERE l.operator_client_id = $1
       AND ($2::boolean OR EXISTS (SELECT 1 FROM anchor_portal_user_locations ul WHERE ul.location_id = l.id AND ul.user_id = $3))
     ORDER BY l.is_demo DESC, l.name ASC`,
    [ANCHOR_OPERATOR_CLIENT_ID, isOperator(user), user.id]);
  return rows;
}

async function transaction(fn) {
  const db = await pool.connect();
  try { await db.query('BEGIN'); const result = await fn(db); await db.query('COMMIT'); return result; }
  catch (err) { await db.query('ROLLBACK'); throw err; }
  finally { db.release(); }
}

function textValue(value, max = 5000) {
  const text = String(value ?? '').trim();
  if (text.length > max) throw new AnchorPortalError('invalid_text', `Text must be at most ${max} characters.`);
  return text;
}

function validatePhoto(value, caption = '') {
  const match = /^data:image\/(jpeg|png|webp);base64,([A-Za-z0-9+/]+={0,2})$/.exec(String(value || ''));
  if (!match || match[2].length % 4 !== 0) throw new AnchorPortalError('invalid_photo', 'Choose a JPEG, PNG, or WebP image.');
  const bytes = Buffer.from(match[2], 'base64');
  if (!bytes.length || bytes.length > 1800000 || bytes.toString('base64') !== match[2]) {
    throw new AnchorPortalError('invalid_photo', 'Photo must be no larger than 1.8 MB.');
  }
  try { validateRaster(bytes, match[1]); }
  catch (_) { throw new AnchorPortalError('invalid_photo', 'Choose a complete JPEG, PNG, or WebP photo up to 12 megapixels.'); }
  return { data_url: String(value), mime_type: `image/${match[1]}`, caption: textValue(caption, 300) || null };
}

function validatePhotos(photos) {
  if (photos == null) return [];
  if (!Array.isArray(photos) || photos.length > 3) throw new AnchorPortalError('invalid_photo', 'Choose up to three photos.');
  return photos.map(photo => validatePhoto(photo.data_url, photo.caption));
}

async function insertEvidence(db, { visitId = null, issueId = null, purpose = 'service', photos }) {
  for (const photo of photos) await db.query(
    `INSERT INTO anchor_portal_evidence (visit_id, issue_id, kind, mime_type, data_url, caption, purpose)
     VALUES ($1, $2, 'photo', $3, $4, $5, $6)`,
    [visitId, issueId, photo.mime_type, photo.data_url, photo.caption, purpose]);
}

async function assertVisitWriter(user, visit) {
  await assertLocationAccess(user, visit.location_id);
  if (!isOperator(user) && (!isCleaner(user) || (visit.cleaner_id && visit.cleaner_id !== user.id))) {
    throw new AnchorPortalError('forbidden', 'This service report is not assigned to you.', 403);
  }
}

function requestId(value) {
  if (!value) return null;
  const id = String(value);
  if (!/^[a-zA-Z0-9_-]{8,100}$/.test(id)) throw new AnchorPortalError('invalid_request', 'Invalid request identifier.');
  return id;
}

async function getLocationScope(locationId) {
  const { rows: sections } = await pool.query(
    `SELECT id, title, sort_order
     FROM anchor_portal_scope_sections
     WHERE location_id = $1
     ORDER BY sort_order, id`,
    [locationId]
  );
  const sectionIds = sections.map(s => s.id);
  if (!sectionIds.length) return [];

  const { rows: items } = await pool.query(
    `SELECT id, section_id, label, sort_order
     FROM anchor_portal_scope_items
     WHERE section_id = ANY($1::int[])
     ORDER BY sort_order, id`,
    [sectionIds]
  );
  const bySection = new Map(sectionIds.map(id => [id, []]));
  for (const item of items) {
    bySection.get(item.section_id).push(item);
  }
  return sections.map(section => ({
    id: section.id,
    title: section.title,
    sort_order: section.sort_order,
    items: bySection.get(section.id) || [],
  }));
}

async function replaceLocationScope(locationId, sections) {
  await pool.query(`DELETE FROM anchor_portal_scope_sections WHERE location_id = $1`, [locationId]);
  if (!Array.isArray(sections) || !sections.length) return getLocationScope(locationId);

  for (let si = 0; si < sections.length; si += 1) {
    const section = sections[si];
    const title = String(section.title || '').trim();
    if (!title) continue;
    const { rows } = await pool.query(
      `INSERT INTO anchor_portal_scope_sections (location_id, title, sort_order)
       VALUES ($1, $2, $3)
       RETURNING id`,
      [locationId, title, si]
    );
    const sectionId = rows[0].id;
    const items = Array.isArray(section.items) ? section.items : [];
    for (let ii = 0; ii < items.length; ii += 1) {
      const label = String(items[ii].label ?? items[ii] ?? '').trim();
      if (!label) continue;
      await pool.query(
        `INSERT INTO anchor_portal_scope_items (section_id, label, sort_order)
         VALUES ($1, $2, $3)`,
        [sectionId, label, ii]
      );
    }
  }
  return getLocationScope(locationId);
}

function formatVisitRow(row) {
  return {
    id: row.id,
    location_id: row.location_id,
    location_name: row.location_name,
    cleaner_id: row.cleaner_id,
    cleaner_name: row.cleaner_name,
    scheduled_at: row.scheduled_at,
    started_at: row.started_at,
    completed_at: row.completed_at,
    status: row.status,
    visit_notes: row.visit_notes,
    completed_by: row.completed_by,
    completed_by_name: row.completed_by_name || row.cleaner_name,
  };
}

async function getVisitWithItems(visitId) {
  const { rows } = await pool.query(
    `SELECT v.*, l.name AS location_name, u.name AS cleaner_name, cu.name AS completed_by_name
     FROM anchor_portal_visits v
     JOIN anchor_portal_locations l ON l.id = v.location_id
     LEFT JOIN users u ON u.id = v.cleaner_id
     LEFT JOIN users cu ON cu.id = v.completed_by
     WHERE v.id = $1`,
    [visitId]
  );
  const visit = rows[0];
  if (!visit) return null;

  const { rows: items } = await pool.query(
    `SELECT id, section_title, item_label, sort_order, outcome, exception_reason, completed_at
     FROM anchor_portal_visit_items
     WHERE visit_id = $1
     ORDER BY sort_order, id`,
    [visitId]
  );

  const { rows: evidence } = await pool.query(
    `SELECT id, kind, mime_type, caption, created_at, purpose,
            CASE WHEN length(data_url) > 120 THEN left(data_url, 80) || '…' ELSE data_url END AS data_url_preview,
            data_url
     FROM anchor_portal_evidence
     WHERE visit_id = $1
     ORDER BY created_at ASC`,
    [visitId]
  );

  return {
    ...formatVisitRow(visit),
    items,
    evidence: evidence.map(e => ({
      id: e.id,
      kind: e.kind,
      purpose: e.purpose,
      mime_type: e.mime_type,
      caption: e.caption,
      created_at: e.created_at,
      data_url: e.data_url,
    })),
  };
}

async function listVisits(user, { locationId, limit = 20 } = {}) {
  assertPortalUser(user);
  const locId = locationId ? await assertLocationAccess(user, locationId) : null;
  const max = Math.min(Math.max(Number(limit) || 20, 1), 50);

  if (isOperator(user) && !locId) {
    const { rows } = await pool.query(
      `SELECT v.*, l.name AS location_name, u.name AS cleaner_name, cu.name AS completed_by_name
       FROM anchor_portal_visits v
       JOIN anchor_portal_locations l ON l.id = v.location_id
       LEFT JOIN users u ON u.id = v.cleaner_id
     LEFT JOIN users cu ON cu.id = v.completed_by
       WHERE l.operator_client_id = $1
       ORDER BY COALESCE(v.completed_at, v.scheduled_at) DESC
       LIMIT $2`,
      [ANCHOR_OPERATOR_CLIENT_ID, max]
    );
    return rows.map(formatVisitRow);
  }

  const locations = locId ? [locId] : await userLocationIds(user.id);
  if (!locations.length) return [];

  const { rows } = await pool.query(
    `SELECT v.*, l.name AS location_name, u.name AS cleaner_name, cu.name AS completed_by_name
     FROM anchor_portal_visits v
     JOIN anchor_portal_locations l ON l.id = v.location_id
     LEFT JOIN users u ON u.id = v.cleaner_id
     LEFT JOIN users cu ON cu.id = v.completed_by
     WHERE v.location_id = ANY($1::int[])
     ORDER BY COALESCE(v.completed_at, v.scheduled_at) DESC
     LIMIT $2`,
    [locations, max]
  );
  return rows.map(formatVisitRow);
}

async function getTodayVisitForCleaner(user) {
  assertPortalUser(user);
  if (!isCleaner(user) && !isOperator(user)) {
    throw new AnchorPortalError('forbidden', 'Cleaner access only.', 403);
  }
  const locations = isOperator(user)
    ? (await listLocationsForUser(user)).map(l => l.id)
    : await userLocationIds(user.id);
  if (!locations.length) return null;

  const { rows } = await pool.query(
    `SELECT v.id
     FROM anchor_portal_visits v
     WHERE v.location_id = ANY($1::int[])
       AND ($2::boolean OR v.cleaner_id IS NULL OR v.cleaner_id = $3)
       AND v.status IN ('scheduled', 'in_progress')
       AND v.scheduled_at::date <= (NOW() AT TIME ZONE 'America/New_York')::date
       AND v.scheduled_at::date >= (NOW() AT TIME ZONE 'America/New_York')::date - INTERVAL '1 day'
     ORDER BY v.scheduled_at ASC
     LIMIT 1`,
    [locations, isOperator(user), user.id]
  );
  if (!rows.length) return null;
  return getVisitWithItems(rows[0].id);
}

async function startVisit(user, visitId) {
  const visit = await getVisitWithItems(visitId);
  if (!visit) throw new AnchorPortalError('not_found', 'Service report not found.', 404);
  await assertVisitWriter(user, visit);
  await transaction(async db => {
    const { rows: [locked] } = await db.query('SELECT * FROM anchor_portal_visits WHERE id=$1 FOR UPDATE', [visitId]);
    assertMutableVisit(user, locked);
    const { rows } = await db.query('SELECT COUNT(*)::int AS count FROM anchor_portal_visit_items WHERE visit_id=$1', [visitId]);
    if (!rows[0].count) await copyScopeToVisitItems(visitId, visit.location_id, db);
    await db.query(`UPDATE anchor_portal_visits SET status='in_progress', started_at=COALESCE(started_at,NOW()),
      cleaner_id=COALESCE(cleaner_id,$2) WHERE id=$1`, [visitId, isCleaner(user) ? user.id : visit.cleaner_id]);
  });
  return getVisitWithItems(visitId);
}

function assertMutableVisit(user, visit) {
  if (isCleaner(user) && visit.cleaner_id && visit.cleaner_id !== user.id) throw new AnchorPortalError('forbidden', 'This service report is assigned to another team member.', 403);
  if (!['scheduled','in_progress'].includes(visit.status)) throw new AnchorPortalError('invalid_state', 'This service report is already completed or cancelled.', 409);
}

async function updateVisitItem(user, visitId, itemId, payload) {
  const visit = await getVisitWithItems(visitId);
  if (!visit) throw new AnchorPortalError('not_found', 'Service report not found.', 404);
  await assertVisitWriter(user, visit);
  const outcome = String(payload.outcome || '').trim();
  if (!['completed','unable_to_complete','issue_detected','not_applicable','pending'].includes(outcome)) throw new AnchorPortalError('invalid_outcome', 'Invalid checklist outcome.');
  const reason = textValue(payload.exception_reason, 2000);
  if (OUTCOMES_REQUIRING_REASON.has(outcome) && !reason) throw new AnchorPortalError('reason_required', 'Exception outcomes require a reason.');
  await transaction(async db => {
    const { rows: [locked] } = await db.query('SELECT * FROM anchor_portal_visits WHERE id=$1 FOR UPDATE', [visitId]);
    assertMutableVisit(user, locked);
    const { rowCount } = await db.query(`UPDATE anchor_portal_visit_items SET outcome=$3, exception_reason=$4,
      completed_at=CASE WHEN $3='pending' THEN NULL ELSE COALESCE(completed_at,NOW()) END WHERE id=$1 AND visit_id=$2`,
      [itemId,visitId,outcome,OUTCOMES_REQUIRING_REASON.has(outcome)?reason:null]);
    if (!rowCount) throw new AnchorPortalError('not_found', 'Checklist item not found.', 404);
    await db.query(`UPDATE anchor_portal_visits SET status='in_progress', started_at=COALESCE(started_at,NOW()),
      cleaner_id=COALESCE(cleaner_id,$2) WHERE id=$1`, [visitId, isCleaner(user)?user.id:null]);
  });
  return getVisitWithItems(visitId);
}

async function addVisitEvidence(user, visitId, { data_url, caption }) {
  const visit = await getVisitWithItems(visitId);
  if (!visit) throw new AnchorPortalError('not_found', 'Service report not found.', 404);
  await assertVisitWriter(user, visit);
  const photo = validatePhoto(data_url, caption);
  await transaction(async db => {
    const { rows: [locked] } = await db.query('SELECT * FROM anchor_portal_visits WHERE id=$1 FOR UPDATE', [visitId]);
    assertMutableVisit(user, locked);
    await insertEvidence(db, { visitId, photos: [photo] });
  });
  return getVisitWithItems(visitId);
}

async function completeVisit(user, visitId, { visit_notes, photos, items: updates } = {}) {
  const visit = await getVisitWithItems(visitId);
  if (!visit) throw new AnchorPortalError('not_found', 'Service report not found.', 404);
  await assertVisitWriter(user, visit);
  const validated = validatePhotos(photos);
  const notes = textValue(visit_notes);
  await transaction(async db => {
    const { rows: [locked] } = await db.query('SELECT * FROM anchor_portal_visits WHERE id = $1 FOR UPDATE', [visitId]);
    if (isCleaner(user) && locked.cleaner_id && locked.cleaner_id !== user.id) throw new AnchorPortalError('forbidden', 'This report is assigned to another team member.', 403);
    if (locked.status === 'completed') return;
    if (locked.status !== 'in_progress') throw new AnchorPortalError('invalid_state', 'Start this service report before completing it.', 409);
    if (updates != null) {
      if (!Array.isArray(updates) || updates.length > 200) throw new AnchorPortalError('invalid_items', 'Invalid service checklist.');
      for (const item of updates) {
        const outcome = item.outcome;
        const reason = textValue(item.exception_reason, 2000);
        if (!['completed','unable_to_complete','issue_detected','not_applicable'].includes(outcome)) throw new AnchorPortalError('invalid_outcome', 'Choose an outcome for each checklist item.');
        if (OUTCOMES_REQUIRING_REASON.has(outcome) && !reason) throw new AnchorPortalError('reason_required', 'Explain every incomplete task or issue.');
        const { rowCount } = await db.query(`UPDATE anchor_portal_visit_items SET outcome=$3, exception_reason=$4, completed_at=COALESCE(completed_at,NOW()) WHERE id=$1 AND visit_id=$2`,
          [item.id, visitId, outcome, OUTCOMES_REQUIRING_REASON.has(outcome) ? reason : null]);
        if (!rowCount) throw new AnchorPortalError('invalid_items', 'Checklist item does not belong to this service report.');
      }
    }
    const { rows: items } = await db.query('SELECT outcome FROM anchor_portal_visit_items WHERE visit_id = $1', [visitId]);
    if (!items.length || items.some(item => item.outcome === 'pending')) throw new AnchorPortalError('incomplete_checklist', 'All checklist items must be marked before completing.');
    await insertEvidence(db, { visitId, photos: validated });
    await db.query(`UPDATE anchor_portal_visits SET status = 'completed', completed_at = NOW(),
      completed_by = $2, cleaner_id = COALESCE(cleaner_id, $2), visit_notes = $3 WHERE id = $1`, [visitId, user.id, notes || null]);
  });
  return getVisitWithItems(visitId);
}

async function createVisit(user, payload) {
  const locationId = await assertLocationAccess(user, payload.location_id);
  if (!isOperator(user) && !isCleaner(user)) throw new AnchorPortalError('forbidden', 'Service team access only.', 403);
  const date = new Date(payload.service_date);
  if (!payload.service_date || !Number.isFinite(+date) || +date > Date.now() + 60000) throw new AnchorPortalError('invalid_date', 'Choose a valid service date that is not in the future.');
  const notes = textValue(payload.visit_notes);
  const areas = payload.areas;
  if (!Array.isArray(areas) || !areas.length || areas.length > 50) throw new AnchorPortalError('invalid_areas', 'Select between one and 50 facility areas.');
  const items = areas.map(area => {
    const title = textValue(area.title, 200);
    const note = textValue(area.notes, 1000);
    const outcome = area.outcome || 'completed';
    if (!title || !['completed','unable_to_complete','issue_detected','not_applicable'].includes(outcome)) throw new AnchorPortalError('invalid_areas', 'Each area needs a name and valid outcome.');
    if (OUTCOMES_REQUIRING_REASON.has(outcome) && !note) throw new AnchorPortalError('reason_required', 'Please explain the service exception.');
    return {title, note, outcome};
  });
  const photos = validatePhotos(payload.photos);
  const key = requestId(payload.request_id);
  const id = await transaction(async db => {
    if (key) {
      await db.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`portal-report:${user.id}:${key}`]);
      const { rows } = await db.query('SELECT id, location_id FROM anchor_portal_visits WHERE completed_by = $1 AND request_id = $2', [user.id, key]);
      if (rows.length) {
        if (rows[0].location_id !== locationId) throw new AnchorPortalError('request_conflict', 'This request was already used for a different building.', 409);
        return rows[0].id;
      }
    }
    const { rows: [visit] } = await db.query(`INSERT INTO anchor_portal_visits
      (location_id, cleaner_id, completed_by, scheduled_at, started_at, completed_at, status, visit_notes, request_id)
      VALUES ($1,$2,$2,$3,$3,$3,'completed',$4,$5) RETURNING id`, [locationId, user.id, date.toISOString(), notes || null, key]);
    for (let index = 0; index < items.length; index++) {
      const item = items[index];
      await db.query(`INSERT INTO anchor_portal_visit_items
        (visit_id, section_title, item_label, sort_order, outcome, exception_reason, completed_at)
        VALUES ($1,$2,$3,$4,$5,$6,$7)`, [visit.id, item.title, item.note || 'Area service recorded', index, item.outcome,
        OUTCOMES_REQUIRING_REASON.has(item.outcome) ? item.note : null, date.toISOString()]);
    }
    await insertEvidence(db, { visitId: visit.id, photos });
    return visit.id;
  });
  return getVisitWithItems(id);
}

async function listIssues(user, { locationId, status, offset = 0 } = {}) {
  assertPortalUser(user);
  if (status === 'in_progress') status = 'acknowledged';
  const locId = locationId ? await assertLocationAccess(user, locationId) : null;
  const locations = locId
    ? [locId]
    : (isOperator(user)
      ? (await listLocationsForUser(user)).map(l => l.id)
      : await userLocationIds(user.id));

  if (!locations.length) return [];

  const params = [locations];
  let statusClause = '';
  if (status === 'active') statusClause = " AND i.status IN ('open','acknowledged')";
  else if (status) {
    params.push(status);
    statusClause = ` AND i.status = $${params.length}`;
  }

  const pageOffset = Math.min(100000, Math.max(0, Math.trunc(Number(offset) || 0)));
  params.push(pageOffset);
  const { rows } = await pool.query(
    `SELECT i.*, l.name AS location_name, ru.name AS reported_by_name
     FROM anchor_portal_issues i
     JOIN anchor_portal_locations l ON l.id = i.location_id
     LEFT JOIN users ru ON ru.id = i.reported_by_user_id
     WHERE i.location_id = ANY($1::int[])${statusClause}
     ORDER BY
       CASE i.status WHEN 'open' THEN 0 WHEN 'acknowledged' THEN 1 ELSE 2 END,
       i.created_at DESC, i.id DESC
     LIMIT 100 OFFSET $${params.length}`,
    params
  );
  const ids = rows.map(issue => issue.id);
  if (ids.length) {
    const { rows: evidence } = await pool.query(`SELECT id, issue_id, kind, mime_type, data_url, caption, purpose, created_at
      FROM anchor_portal_evidence WHERE issue_id = ANY($1::int[]) ORDER BY created_at, id`, [ids]);
    for (const issue of rows) issue.evidence = evidence.filter(photo => photo.issue_id === issue.id);
  }
  return rows;
}

async function getIssue(user, issueId) {
  const { rows: [issue] } = await pool.query('SELECT * FROM anchor_portal_issues WHERE id=$1', [issueId]);
  if (!issue) throw new AnchorPortalError('not_found', 'Issue not found.', 404);
  await assertLocationAccess(user, issue.location_id);
  const { rows: evidence } = await pool.query(`SELECT id, issue_id, kind, mime_type, data_url, caption, purpose, created_at FROM anchor_portal_evidence WHERE issue_id=$1 ORDER BY created_at,id`, [issueId]);
  return {...issue,evidence};
}

async function createIssue(user, payload) {
  const locationId = await assertLocationAccess(user, payload.location_id);
  const description = textValue(payload.description);
  if (description.length < 8) throw new AnchorPortalError('invalid_description', 'Please describe the problem in at least eight characters.');
  const photo = payload.photo_data_url ? validatePhoto(payload.photo_data_url, payload.photo_caption) : null;
  const severity = payload.severity || 'medium';
  if (!['low','medium','high'].includes(severity)) throw new AnchorPortalError('invalid_severity', 'Choose a valid urgency.');
  const title = textValue(payload.title, 200) || null;
  const room = textValue(payload.room_category, 200) || null;
  const type = textValue(payload.issue_type, 100) || 'Other';
  const key = requestId(payload.request_id);
  if (payload.visit_id) {
    const visit = await getVisitWithItems(payload.visit_id);
    if (!visit || visit.location_id !== locationId) throw new AnchorPortalError('invalid_visit', 'Service report does not belong to this building.');
  }
  const issue = await transaction(async db => {
    if (key) {
      await db.query('SELECT pg_advisory_xact_lock(hashtext($1))', [`portal-issue:${user.id}:${key}`]);
      const { rows } = await db.query('SELECT * FROM anchor_portal_issues WHERE reported_by_user_id = $1 AND request_id = $2', [user.id, key]);
      if (rows.length) {
        if (rows[0].location_id !== locationId) throw new AnchorPortalError('request_conflict', 'This request was already used for a different building.', 409);
        return rows[0];
      }
    }
    const { rows: [created] } = await db.query(`INSERT INTO anchor_portal_issues
      (location_id,visit_id,reported_by_user_id,source,title,description,room_category,severity,status,issue_type,request_id)
      VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'open',$9,$10) RETURNING *`,
      [locationId,payload.visit_id || null,user.id,isOperator(user)?'operator':isCleaner(user)?'cleaner':'client',title,description,room,severity,type,key]);
    if (photo) await insertEvidence(db, {issueId:created.id,purpose:'reported',photos:[photo]});
    return created;
  });
  return getIssue(user, issue.id);
}

async function updateIssue(user, issueId, payload) {
  assertPortalUser(user);
  if (!isOperator(user)) throw new AnchorPortalError('forbidden', 'Service manager access only.', 403);
  const { rows: [issue] } = await pool.query('SELECT * FROM anchor_portal_issues WHERE id = $1', [issueId]);
  if (!issue) throw new AnchorPortalError('not_found', 'Issue not found.', 404);
  await assertLocationAccess(user, issue.location_id);
  const resolution = textValue(payload.resolution);
  const photo = payload.resolution_photo_data_url ? validatePhoto(payload.resolution_photo_data_url, payload.resolution_photo_caption) : null;
  await transaction(async db => {
    const { rows: [locked] } = await db.query('SELECT * FROM anchor_portal_issues WHERE id = $1 FOR UPDATE', [issueId]);
    const status = payload.status === 'in_progress' ? 'acknowledged' : (payload.status || locked.status);
    if (!['open','acknowledged','resolved'].includes(status)) throw new AnchorPortalError('invalid_status', 'Invalid issue status.');
    if (status === 'resolved' && !resolution && !locked.resolution) throw new AnchorPortalError('resolution_required', 'Add a resolution note.');
    if (photo && status !== 'resolved') throw new AnchorPortalError('invalid_status', 'Photo proof belongs to a resolved issue.');
    await db.query(`UPDATE anchor_portal_issues SET status=$2,
      acknowledged_at=CASE WHEN $2='open' THEN NULL ELSE COALESCE(acknowledged_at,NOW()) END,
      acknowledged_by=CASE WHEN $2='open' THEN NULL ELSE COALESCE(acknowledged_by,$3) END,
      resolution=COALESCE(NULLIF($4,''),resolution),
      resolved_at=CASE WHEN $2='resolved' THEN COALESCE(resolved_at,NOW()) ELSE NULL END
      WHERE id=$1`, [issueId,status,user.id,resolution]);
    if (photo) {
      const { rows } = await db.query(`SELECT id FROM anchor_portal_evidence WHERE issue_id=$1 AND purpose='resolution' AND data_url=$2`, [issueId,photo.data_url]);
      if (!rows.length) await insertEvidence(db,{issueId,purpose:'resolution',photos:[photo]});
    }
  });
  return getIssue(user, issue.id);
}

async function getClientHome(user, locationId) {
  const locId = await assertLocationAccess(user, locationId);

  const { rows: locRows } = await pool.query(
    `SELECT id, name, address, is_demo, next_scheduled_at FROM anchor_portal_locations WHERE id = $1`,
    [locId]
  );
  const location = locRows[0];

  const { rows: lastCompleted } = await pool.query(
    `SELECT completed_at, status
     FROM anchor_portal_visits
     WHERE location_id = $1 AND status = 'completed'
     ORDER BY completed_at DESC NULLS LAST
     LIMIT 1`,
    [locId]
  );

  const { rows: recentVisits } = await pool.query(
    `SELECT v.id, v.status, v.completed_at, v.scheduled_at, cu.name AS completed_by_name, u.name AS cleaner_name
     FROM anchor_portal_visits v
     LEFT JOIN users cu ON cu.id = v.completed_by
     LEFT JOIN users u ON u.id = v.cleaner_id
     WHERE v.location_id = $1
     ORDER BY COALESCE(v.completed_at, v.scheduled_at) DESC
     LIMIT 8`,
    [locId]
  );

  const completedCount = recentVisits.filter(v => v.status === 'completed').length;
  const openIssues = await listIssues(user, { locationId: locId, status: 'open' });
  const acknowledgedIssues = await listIssues(user, { locationId: locId, status: 'acknowledged' });

  const { rows: issueStats } = await pool.query(
    `SELECT
       COUNT(*) FILTER (WHERE status IN ('open','acknowledged'))::int AS active_count,
       COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '90 days')::int AS reported_90d,
       COUNT(*) FILTER (WHERE status = 'resolved' AND created_at > NOW() - INTERVAL '90 days')::int AS resolved_90d
     FROM anchor_portal_issues
     WHERE location_id = $1`,
    [locId]
  );

  let serviceStatus = 'No completed visits yet';
  if (lastCompleted.length) {
    const lastVisitId = (
      await pool.query(
        `SELECT id FROM anchor_portal_visits WHERE location_id = $1 AND status = 'completed'
         ORDER BY completed_at DESC LIMIT 1`,
        [locId]
      )
    ).rows[0]?.id;

    if (lastVisitId) {
      const { rows: badItems } = await pool.query(
        `SELECT COUNT(*)::int AS count
         FROM anchor_portal_visit_items
         WHERE visit_id = $1 AND outcome IN ('unable_to_complete', 'issue_detected')`,
        [lastVisitId]
      );
      serviceStatus = badItems[0].count
        ? 'Completed with documented exceptions'
        : 'All scheduled work completed';
    }
  }

  return {
    location,
    last_service: lastCompleted[0]?.completed_at || null,
    next_service: location.next_scheduled_at,
    service_status: serviceStatus,
    open_issues_count: issueStats[0]?.active_count || 0,
    recent_quality: {
      visits_window: 8,
      visits_completed: completedCount,
      issues_reported_90d: issueStats[0]?.reported_90d || 0,
      issues_resolved_90d: issueStats[0]?.resolved_90d || 0,
    },
    recent_visits: recentVisits,
    open_issues: [...openIssues, ...acknowledgedIssues],
  };
}

async function getOperatorSummary(user) {
  if (!isOperator(user)) throw new AnchorPortalError('forbidden', 'Operators only.', 403);

  const { rows: missed } = await pool.query(
    `SELECT vi.id, vi.section_title, vi.item_label, vi.outcome, vi.exception_reason,
            v.completed_at, l.name AS location_name, v.id AS visit_id
     FROM anchor_portal_visit_items vi
     JOIN anchor_portal_visits v ON v.id = vi.visit_id
     JOIN anchor_portal_locations l ON l.id = v.location_id
     WHERE l.operator_client_id = $1
       AND vi.outcome IN ('unable_to_complete', 'issue_detected')
       AND v.completed_at > NOW() - INTERVAL '30 days'
     ORDER BY v.completed_at DESC
     LIMIT 40`,
    [ANCHOR_OPERATOR_CLIENT_ID]
  );

  const openIssues = await listIssues(user, { status: 'open' });
  const acknowledgedIssues = await listIssues(user, { status: 'acknowledged' });

  const { rows: locations } = await pool.query(
    `SELECT id, name, is_demo, next_scheduled_at FROM anchor_portal_locations
     WHERE operator_client_id = $1 ORDER BY name`,
    [ANCHOR_OPERATOR_CLIENT_ID]
  );

  return {
    locations,
    open_issues: openIssues,
    acknowledged_issues: acknowledgedIssues,
    recent_exceptions: missed,
  };
}

module.exports = {
  AnchorPortalError,
  assertLocationAccess,
  isOperator,
  isCleaner,
  isFacilityClient,
  listLocationsForUser,
  getLocationScope,
  replaceLocationScope,
  getVisitWithItems,
  listVisits,
  getTodayVisitForCleaner,
  startVisit,
  updateVisitItem,
  addVisitEvidence,
  completeVisit,
  createVisit,
  validatePhoto,
  assertPortalUser,
  listIssues,
  createIssue,
  updateIssue,
  getClientHome,
  getOperatorSummary,
  ANCHOR_OPERATOR_CLIENT_ID,
};
