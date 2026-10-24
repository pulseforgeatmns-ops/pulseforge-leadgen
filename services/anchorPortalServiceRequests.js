'use strict';
// Persistence only: never schedules visits or sends external communications.
module.exports = function createServiceRequests({pool, AnchorPortalError: ErrorType, assertPortalUser, assertLocationAccess, isOperator, isFacilityClient}) {
  function allowed(user) {
    assertPortalUser(user);
    if (!isOperator(user) && !isFacilityClient(user)) throw new ErrorType('forbidden', 'Clients and operators only.', 403);
  }
  function input(body) {
    if (!body || typeof body !== 'object' || Array.isArray(body)) throw new ErrorType('invalid_request', 'Send a request object.');
    const allowedKeys = new Set(['location_id','request_key','preferred_date','preferred_window','notes']);
    if (Object.keys(body).some(k => !allowedKeys.has(k))) throw new ErrorType('invalid_request', 'Unexpected request fields.');
    const {request_key:key, preferred_date:date, preferred_window:window, notes = ''} = body;
    if (typeof key !== 'string' || !/^[A-Za-z0-9_-]{8,100}$/.test(key)) throw new ErrorType('invalid_request_key', 'A valid request key is required.');
    if (typeof date !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(date) || !Number.isFinite(Date.parse(date)) || new Date(date).toISOString().slice(0,10) !== date) throw new ErrorType('invalid_date', 'Choose a valid preferred date.');
    if (typeof window !== 'string' || !window.trim() || window.trim().length > 200 || typeof notes !== 'string' || notes.trim().length > 5000) throw new ErrorType('invalid_text', 'Enter a time window up to 200 characters and notes up to 5000 characters.');
    return {key,date,window:window.trim(),notes:notes.trim()};
  }
  async function listServiceRequests(user, locationId) {
    allowed(user); const location = await assertLocationAccess(user, locationId);
    const {rows} = await pool.query(`SELECT * FROM anchor_portal_service_requests WHERE operator_client_id=10 AND location_id=$1 AND ($2::boolean OR requester_id=$3) ORDER BY created_at DESC,id DESC LIMIT 100`, [location,isOperator(user),user.id]);
    return rows;
  }
  async function createServiceRequest(user, body = {}) {
    allowed(user); const data = input(body); const location = await assertLocationAccess(user, body.location_id);
    // DO NOTHING plus a fresh statement makes concurrent retries safe under READ COMMITTED.
    const {rows} = await pool.query(`INSERT INTO anchor_portal_service_requests (operator_client_id,location_id,requester_id,request_key,preferred_date,preferred_window,notes) VALUES (10,$1,$2,$3,$4,$5,$6) ON CONFLICT (requester_id,request_key) DO NOTHING RETURNING *`, [location,user.id,data.key,data.date,data.window,data.notes]);
    if (rows.length) return rows[0];
    const {rows:existing} = await pool.query(`SELECT * FROM anchor_portal_service_requests WHERE operator_client_id=10 AND requester_id=$1 AND request_key=$2`, [user.id,data.key]);
    const row = existing[0];
    const date = row?.preferred_date instanceof Date ? row.preferred_date.toISOString().slice(0,10) : String(row?.preferred_date).slice(0,10);
    if (!row || row.location_id !== location || date !== data.date || row.preferred_window !== data.window || row.notes !== data.notes) throw new ErrorType('idempotency_conflict', 'This request key was already used for different details.', 409);
    return row;
  }
  async function updateServiceRequest(user, id, body = {}) {
    allowed(user);
    if (!body || typeof body !== 'object' || Array.isArray(body) || Object.keys(body).length !== 1 || !['acknowledged','declined','cancelled'].includes(body.status)) throw new ErrorType('invalid_status', 'Choose acknowledge, decline, or cancel.');
    if (!Number.isSafeInteger(Number(id)) || Number(id) <= 0) throw new ErrorType('invalid_request', 'Invalid request.');
    const {rows} = await pool.query('SELECT * FROM anchor_portal_service_requests WHERE id=$1 AND operator_client_id=10', [id]);
    const request = rows[0];
    if (!request) throw new ErrorType('not_found', 'Request not found.', 404);
    await assertLocationAccess(user, request.location_id);
    if (body.status === 'cancelled' ? request.requester_id !== user.id : !isOperator(user)) throw new ErrorType('forbidden', 'You cannot make this change.', 403);
    if (request.status === body.status) return request;
    const from = body.status === 'cancelled' ? ['requested','acknowledged'] : ['requested'];
    const {rows:updated} = await pool.query(`UPDATE anchor_portal_service_requests SET status=$2,updated_by=$3,updated_at=now() WHERE id=$1 AND operator_client_id=10 AND status=ANY($4::text[]) RETURNING *`, [id,body.status,user.id,from]);
    if (!updated.length) throw new ErrorType('invalid_transition', 'This request has changed or is already closed. Reload to see its status.', 409);
    return updated[0];
  }
  return {listServiceRequests,createServiceRequest,updateServiceRequest};
};
