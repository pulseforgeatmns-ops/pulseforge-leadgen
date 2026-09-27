'use strict';

const pool = require('../db');
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

async function userLocationIds(userId) {
  const { rows } = await pool.query(
    `SELECT location_id FROM anchor_portal_user_locations WHERE user_id = $1`,
    [userId]
  );
  return rows.map(r => r.location_id);
}

async function assertLocationAccess(user, locationId) {
  const id = Number(locationId);
  if (!Number.isInteger(id) || id <= 0) {
    throw new AnchorPortalError('invalid_location', 'Invalid location.', 400);
  }
  if (isOperator(user)) return id;
  if (!user?.id) throw new AnchorPortalError('forbidden', 'Forbidden.', 403);
  const allowed = await userLocationIds(user.id);
  if (!allowed.includes(id)) {
    throw new AnchorPortalError('forbidden', 'You do not have access to this location.', 403);
  }
  return id;
}

async function listLocationsForUser(user) {
  if (isOperator(user)) {
    const { rows } = await pool.query(
      `SELECT id, name, slug, address, is_demo, next_scheduled_at
       FROM anchor_portal_locations
       WHERE operator_client_id = $1
       ORDER BY is_demo DESC, name ASC`,
      [ANCHOR_OPERATOR_CLIENT_ID]
    );
    return rows;
  }
  if (!user?.id) return [];
  const { rows } = await pool.query(
    `SELECT l.id, l.name, l.slug, l.address, l.is_demo, l.next_scheduled_at
     FROM anchor_portal_locations l
     JOIN anchor_portal_user_locations ul ON ul.location_id = l.id
     WHERE ul.user_id = $1
     ORDER BY l.name ASC`,
    [user.id]
  );
  return rows;
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
  };
}

async function getVisitWithItems(visitId) {
  const { rows } = await pool.query(
    `SELECT v.*, l.name AS location_name, u.name AS cleaner_name
     FROM anchor_portal_visits v
     JOIN anchor_portal_locations l ON l.id = v.location_id
     LEFT JOIN users u ON u.id = v.cleaner_id
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
    `SELECT id, kind, mime_type, caption, created_at,
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
      mime_type: e.mime_type,
      caption: e.caption,
      created_at: e.created_at,
      data_url: e.data_url,
    })),
  };
}

async function listVisits(user, { locationId, limit = 20 } = {}) {
  const locId = locationId ? await assertLocationAccess(user, locationId) : null;
  const max = Math.min(Math.max(Number(limit) || 20, 1), 50);

  if (isOperator(user) && !locId) {
    const { rows } = await pool.query(
      `SELECT v.*, l.name AS location_name, u.name AS cleaner_name
       FROM anchor_portal_visits v
       JOIN anchor_portal_locations l ON l.id = v.location_id
       LEFT JOIN users u ON u.id = v.cleaner_id
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
    `SELECT v.*, l.name AS location_name, u.name AS cleaner_name
     FROM anchor_portal_visits v
     JOIN anchor_portal_locations l ON l.id = v.location_id
     LEFT JOIN users u ON u.id = v.cleaner_id
     WHERE v.location_id = ANY($1::int[])
     ORDER BY COALESCE(v.completed_at, v.scheduled_at) DESC
     LIMIT $2`,
    [locations, max]
  );
  return rows.map(formatVisitRow);
}

async function getTodayVisitForCleaner(user) {
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
       AND v.status IN ('scheduled', 'in_progress')
       AND v.scheduled_at::date <= (NOW() AT TIME ZONE 'America/New_York')::date
       AND v.scheduled_at::date >= (NOW() AT TIME ZONE 'America/New_York')::date - INTERVAL '1 day'
     ORDER BY v.scheduled_at ASC
     LIMIT 1`,
    [locations]
  );
  if (!rows.length) return null;
  return getVisitWithItems(rows[0].id);
}

async function startVisit(user, visitId) {
  const visit = await getVisitWithItems(visitId);
  if (!visit) throw new AnchorPortalError('not_found', 'Visit not found.', 404);
  await assertLocationAccess(user, visit.location_id);
  if (!isCleaner(user) && !isOperator(user)) {
    throw new AnchorPortalError('forbidden', 'Only cleaners can start visits.', 403);
  }
  if (visit.status === 'completed') {
    throw new AnchorPortalError('invalid_state', 'Visit is already completed.', 409);
  }

  const { rows: itemRows } = await pool.query(
    `SELECT COUNT(*)::int AS count FROM anchor_portal_visit_items WHERE visit_id = $1`,
    [visitId]
  );
  if (itemRows[0].count === 0) {
    await copyScopeToVisitItems(visitId, visit.location_id);
  }

  await pool.query(
    `UPDATE anchor_portal_visits
     SET status = 'in_progress',
         started_at = COALESCE(started_at, NOW()),
         cleaner_id = COALESCE(cleaner_id, $2)
     WHERE id = $1`,
    [visitId, isCleaner(user) ? user.id : visit.cleaner_id]
  );
  return getVisitWithItems(visitId);
}

async function updateVisitItem(user, visitId, itemId, payload) {
  const visit = await getVisitWithItems(visitId);
  if (!visit) throw new AnchorPortalError('not_found', 'Visit not found.', 404);
  await assertLocationAccess(user, visit.location_id);
  if (!isCleaner(user) && !isOperator(user)) {
    throw new AnchorPortalError('forbidden', 'Only cleaners can update checklist items.', 403);
  }
  if (visit.status === 'completed') {
    throw new AnchorPortalError('invalid_state', 'Visit is already completed.', 409);
  }

  const outcome = String(payload.outcome || '').trim();
  const allowed = ['completed', 'unable_to_complete', 'issue_detected', 'not_applicable', 'pending'];
  if (!allowed.includes(outcome)) {
    throw new AnchorPortalError('invalid_outcome', 'Invalid checklist outcome.', 400);
  }
  const reason = String(payload.exception_reason || '').trim();
  if (OUTCOMES_REQUIRING_REASON.has(outcome) && !reason) {
    throw new AnchorPortalError('reason_required', 'Exception outcomes require a reason.', 400);
  }

  const { rowCount } = await pool.query(
    `UPDATE anchor_portal_visit_items
     SET outcome = $3,
         exception_reason = $4,
         completed_at = CASE WHEN $3 IN ('completed', 'unable_to_complete', 'issue_detected', 'not_applicable')
           THEN COALESCE(completed_at, NOW()) ELSE NULL END
     WHERE id = $1 AND visit_id = $2`,
    [itemId, visitId, outcome, OUTCOMES_REQUIRING_REASON.has(outcome) ? reason : null]
  );
  if (!rowCount) throw new AnchorPortalError('not_found', 'Checklist item not found.', 404);

  if (visit.status === 'scheduled') {
    await pool.query(
      `UPDATE anchor_portal_visits SET status = 'in_progress', started_at = COALESCE(started_at, NOW())
       WHERE id = $1`,
      [visitId]
    );
  }
  return getVisitWithItems(visitId);
}

async function addVisitEvidence(user, visitId, { data_url, mime_type, caption }) {
  const visit = await getVisitWithItems(visitId);
  if (!visit) throw new AnchorPortalError('not_found', 'Visit not found.', 404);
  await assertLocationAccess(user, visit.location_id);
  if (!isCleaner(user) && !isOperator(user)) {
    throw new AnchorPortalError('forbidden', 'Forbidden.', 403);
  }
  const url = String(data_url || '').trim();
  if (!url.startsWith('data:image/') || url.length > 2_500_000) {
    throw new AnchorPortalError('invalid_photo', 'Photo must be a reasonable image data URL.', 400);
  }
  await pool.query(
    `INSERT INTO anchor_portal_evidence (visit_id, kind, mime_type, data_url, caption)
     VALUES ($1, 'photo', $2, $3, $4)`,
    [visitId, mime_type || null, url, caption || null]
  );
  return getVisitWithItems(visitId);
}

async function completeVisit(user, visitId, { visit_notes } = {}) {
  const visit = await getVisitWithItems(visitId);
  if (!visit) throw new AnchorPortalError('not_found', 'Visit not found.', 404);
  await assertLocationAccess(user, visit.location_id);
  if (!isCleaner(user) && !isOperator(user)) {
    throw new AnchorPortalError('forbidden', 'Only cleaners can complete visits.', 403);
  }

  const pending = visit.items.filter(i => i.outcome === 'pending');
  if (pending.length) {
    throw new AnchorPortalError('incomplete_checklist', 'All checklist items must be marked before completing.', 400);
  }

  await pool.query(
    `UPDATE anchor_portal_visits
     SET status = 'completed',
         completed_at = NOW(),
         visit_notes = COALESCE($2, visit_notes)
     WHERE id = $1`,
    [visitId, visit_notes || null]
  );
  return getVisitWithItems(visitId);
}

async function listIssues(user, { locationId, status } = {}) {
  const locId = locationId ? await assertLocationAccess(user, locationId) : null;
  const locations = locId
    ? [locId]
    : (isOperator(user)
      ? (await listLocationsForUser(user)).map(l => l.id)
      : await userLocationIds(user.id));

  if (!locations.length) return [];

  const params = [locations];
  let statusClause = '';
  if (status) {
    params.push(status);
    statusClause = ` AND i.status = $${params.length}`;
  }

  const { rows } = await pool.query(
    `SELECT i.*, l.name AS location_name, ru.name AS reported_by_name
     FROM anchor_portal_issues i
     JOIN anchor_portal_locations l ON l.id = i.location_id
     LEFT JOIN users ru ON ru.id = i.reported_by_user_id
     WHERE i.location_id = ANY($1::int[])${statusClause}
     ORDER BY
       CASE i.status WHEN 'open' THEN 0 WHEN 'acknowledged' THEN 1 ELSE 2 END,
       i.created_at DESC
     LIMIT 100`,
    params
  );
  return rows;
}

async function createIssue(user, payload) {
  const locationId = await assertLocationAccess(user, payload.location_id);
  const description = String(payload.description || '').trim();
  if (description.length < 8) {
    throw new AnchorPortalError('invalid_description', 'Please describe the issue.', 400);
  }

  let source = 'client';
  if (isCleaner(user)) source = 'cleaner';
  if (isOperator(user)) source = 'operator';

  const severity = ['low', 'medium', 'high'].includes(payload.severity) ? payload.severity : 'medium';

  const { rows } = await pool.query(
    `INSERT INTO anchor_portal_issues
       (location_id, visit_id, reported_by_user_id, source, title, description,
        room_category, severity, status)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 'open')
     RETURNING *`,
    [
      locationId,
      payload.visit_id || null,
      user?.id || null,
      source,
      payload.title || null,
      description,
      payload.room_category || null,
      severity,
    ]
  );
  const issue = rows[0];

  const photo = String(payload.photo_data_url || '').trim();
  if (photo.startsWith('data:image/') && photo.length <= 2_500_000) {
    await pool.query(
      `INSERT INTO anchor_portal_evidence (issue_id, kind, mime_type, data_url, caption)
       VALUES ($1, 'photo', $2, $3, $4)`,
      [issue.id, payload.photo_mime_type || null, photo, payload.photo_caption || null]
    );
  }
  return issue;
}

async function updateIssue(user, issueId, payload) {
  if (!isOperator(user)) {
    throw new AnchorPortalError('forbidden', 'Operators only.', 403);
  }
  const { rows } = await pool.query(`SELECT * FROM anchor_portal_issues WHERE id = $1`, [issueId]);
  const issue = rows[0];
  if (!issue) throw new AnchorPortalError('not_found', 'Issue not found.', 404);

  const status = payload.status || issue.status;
  if (!['open', 'acknowledged', 'resolved'].includes(status)) {
    throw new AnchorPortalError('invalid_status', 'Invalid status.', 400);
  }

  const acknowledgedAt = status === 'open'
    ? null
    : (issue.acknowledged_at || (status !== 'open' ? new Date().toISOString() : null));
  const resolvedAt = status === 'resolved'
    ? (payload.resolved_at || issue.resolved_at || new Date().toISOString())
    : null;

  const { rows: updated } = await pool.query(
    `UPDATE anchor_portal_issues
     SET status = $2,
         acknowledged_at = $3,
         acknowledged_by = CASE WHEN $2 IN ('acknowledged', 'resolved')
           THEN COALESCE(acknowledged_by, $4) ELSE NULL END,
         assigned_to = COALESCE($5, assigned_to),
         resolution = COALESCE($6, resolution),
         resolved_at = $7
     WHERE id = $1
     RETURNING *`,
    [
      issueId,
      status,
      acknowledgedAt,
      user.id,
      payload.assigned_to || null,
      payload.resolution || null,
      resolvedAt,
    ]
  );
  return updated[0];
}

async function getClientHome(user, locationId) {
  const locId = await assertLocationAccess(user, locationId);

  const { rows: locRows } = await pool.query(
    `SELECT id, name, address, next_scheduled_at FROM anchor_portal_locations WHERE id = $1`,
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
    `SELECT id, status, completed_at, scheduled_at
     FROM anchor_portal_visits
     WHERE location_id = $1
     ORDER BY COALESCE(completed_at, scheduled_at) DESC
     LIMIT 8`,
    [locId]
  );

  const completedCount = recentVisits.filter(v => v.status === 'completed').length;
  const openIssues = await listIssues(user, { locationId: locId, status: 'open' });
  const acknowledgedIssues = await listIssues(user, { locationId: locId, status: 'acknowledged' });

  const { rows: issueStats } = await pool.query(
    `SELECT
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
    open_issues_count: openIssues.length + acknowledgedIssues.length,
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
  listIssues,
  createIssue,
  updateIssue,
  getClientHome,
  getOperatorSummary,
  ANCHOR_OPERATOR_CLIENT_ID,
};
