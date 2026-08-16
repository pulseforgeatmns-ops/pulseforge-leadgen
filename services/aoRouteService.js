const pool = require('../db');
const { listQueue } = require('./aoFieldService');
const {
  isUsableAddress,
  geocodeAddress,
  resolveStartPoint,
  sortStopsByMode,
  enrichStopRow,
  buildNextStopDebrief,
  ANCHOR_OFFICE_DEFAULT,
} = require('../utils/aoRoutePlanner');

const ANCHOR_OFFICE_ADDRESS = process.env.AO_ANCHOR_OFFICE_ADDRESS || ANCHOR_OFFICE_DEFAULT;

async function cancelActiveRoutes(aoOwnerId, clientId, db = pool) {
  await db.query(`
    UPDATE ao_routes
    SET status = 'cancelled', completed_at = NOW()
    WHERE ao_owner_id = $1 AND client_id = $2 AND status = 'active'
  `, [aoOwnerId, clientId]);
}

async function listQueueTasksWithDetails({ aoOwnerId, clientId, filter }) {
  const params = [aoOwnerId, clientId];
  let where = `t.ao_owner_id = $1 AND l.client_id = $2 AND t.status = 'open'`;

  const today = new Date().toISOString().slice(0, 10);
  if (filter === 'today') {
    params.push(today);
    where += ` AND t.due_date = $${params.length}`;
  } else if (filter === 'overdue') {
    params.push(today);
    where += ` AND t.due_date < $${params.length}`;
  } else if (filter === 'week') {
    const weekEnd = new Date();
    weekEnd.setDate(weekEnd.getDate() + 7);
    params.push(today, weekEnd.toISOString().slice(0, 10));
    where += ` AND t.due_date BETWEEN $${params.length - 1} AND $${params.length}`;
  } else if (filter === 'high') {
    where += ` AND t.priority IN ('high', 'warm')`;
  } else if (filter === 'direct_mail') {
    where += ` AND l.attribution_source = 'direct_mail_campaign'`;
  } else if (filter === 'waiting') {
    where += ` AND t.waiting_on_jake = true`;
  }

  const { rows } = await pool.query(`
    SELECT
      t.*,
      l.business_name,
      l.address AS lead_address,
      l.interest_level,
      l.attribution_source,
      l.campaign_name,
      l.original_visit_note,
      c.contact_name,
      c.contact_title,
      c.phone AS contact_phone,
      c.email AS contact_email
    FROM ao_follow_up_tasks t
    JOIN ao_leads l ON l.id = t.lead_id
    LEFT JOIN ao_contacts c ON c.id = t.contact_id
    WHERE ${where}
    ORDER BY t.priority DESC, t.due_date ASC, t.created_at ASC
  `, params);
  return rows;
}

async function getActiveRoute({ aoOwnerId, clientId, aoName }) {
  const { rows } = await pool.query(`
    SELECT * FROM ao_routes
    WHERE ao_owner_id = $1 AND client_id = $2 AND status = 'active'
    ORDER BY created_at DESC
    LIMIT 1
  `, [aoOwnerId, clientId]);
  if (!rows.length) return null;

  const route = rows[0];
  const stops = await loadRouteStops(route.id, aoName);
  return formatRouteResponse(route, stops);
}

async function loadRouteStops(routeId, aoName) {
  const { rows } = await pool.query(`
    SELECT
      s.id AS stop_id,
      s.route_id,
      s.task_id,
      s.lead_id,
      s.sequence,
      s.address,
      s.lat,
      s.lng,
      s.status AS stop_status,
      t.due_date,
      t.priority,
      t.next_action,
      t.last_interaction_summary,
      t.suggested_message,
      l.business_name,
      l.attribution_source,
      l.campaign_name,
      l.original_visit_note,
      c.contact_name,
      c.contact_title,
      c.phone AS contact_phone,
      c.email AS contact_email
    FROM ao_route_stops s
    JOIN ao_follow_up_tasks t ON t.id = s.task_id
    JOIN ao_leads l ON l.id = s.lead_id
    LEFT JOIN ao_contacts c ON c.id = t.contact_id
    WHERE s.route_id = $1
    ORDER BY s.sequence ASC
  `, [routeId]);
  return rows.map(row => enrichStopRow(row, aoName));
}

function formatRouteResponse(route, stops) {
  const pending = stops.filter(s => s.status === 'pending');
  return {
    id: route.id,
    queue_filter: route.queue_filter,
    sort_mode: route.sort_mode,
    start_point_type: route.start_point_type,
    start_address: route.start_address,
    status: route.status,
    created_at: route.created_at,
    stops,
    pending_count: pending.length,
    current_stop: pending[0] || null,
    needs_address: stops.filter(s => !s.address_usable),
  };
}

async function createRoute({
  aoOwnerId,
  clientId,
  aoName,
  filter = 'today',
  sortMode = 'closest_first',
  startPointType = 'current_location',
  startLat,
  startLng,
  startAddress,
  manualTaskOrder = null,
}) {
  const tasks = await listQueueTasksWithDetails({ aoOwnerId, clientId, filter });
  const withAddress = [];
  const needsAddress = [];

  for (const task of tasks) {
    const address = task.lead_address || null;
    if (isUsableAddress(address)) {
      withAddress.push(task);
    } else {
      needsAddress.push({
        task_id: task.id,
        lead_id: task.lead_id,
        business_name: task.business_name,
        address: address || null,
        address_usable: false,
      });
    }
  }

  if (!withAddress.length && !needsAddress.length) {
    return { error: 'No open tasks in this queue filter', status: 400 };
  }

  let startPoint = resolveStartPoint({
    startPointType,
    startLat,
    startLng,
    startAddress,
    anchorOfficeAddress: ANCHOR_OFFICE_ADDRESS,
  });

  if (startPointType === 'custom' && startAddress && !startPoint) {
    const geocoded = await geocodeAddress(startAddress);
    if (geocoded) {
      startPoint = { lat: geocoded.lat, lng: geocoded.lng, label: startAddress };
    }
  }

  if (startPointType === 'anchor_office' && !startPoint) {
    startPoint = resolveStartPoint({
      startPointType: 'anchor_office',
      anchorOfficeAddress: ANCHOR_OFFICE_ADDRESS,
    });
  }

  const geocodedStops = [];
  for (const task of withAddress) {
    const address = task.lead_address.trim();
    let lat = null;
    let lng = null;
    const coords = await geocodeAddress(address);
    if (coords) {
      lat = coords.lat;
      lng = coords.lng;
    }
    geocodedStops.push({
      task_id: task.id,
      lead_id: task.lead_id,
      address,
      lat,
      lng,
      sequence: manualTaskOrder ? manualTaskOrder.indexOf(task.id) : geocodedStops.length,
      ...task,
    });
  }

  let orderedStops;
  if (sortMode === 'manual' && Array.isArray(manualTaskOrder) && manualTaskOrder.length) {
    const orderMap = new Map(manualTaskOrder.map((id, idx) => [id, idx]));
    orderedStops = [...geocodedStops].sort((a, b) => {
      const ai = orderMap.has(a.task_id) ? orderMap.get(a.task_id) : 999;
      const bi = orderMap.has(b.task_id) ? orderMap.get(b.task_id) : 999;
      return ai - bi;
    });
  } else {
    orderedStops = sortStopsByMode(
      geocodedStops.map((s, idx) => ({ ...s, sequence: idx })),
      startPoint,
      sortMode,
    );
  }

  if (!orderedStops.length) {
    return {
      error: 'No stops with usable addresses — add addresses to queue items first',
      status: 400,
      needs_address: needsAddress,
    };
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await client.query(`
      UPDATE ao_routes
      SET status = 'cancelled', completed_at = NOW()
      WHERE ao_owner_id = $1 AND client_id = $2 AND status = 'active'
    `, [aoOwnerId, clientId]);

    const { rows: routeRows } = await client.query(`
      INSERT INTO ao_routes (
        client_id, ao_owner_id, queue_filter, sort_mode,
        start_point_type, start_lat, start_lng, start_address
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
      RETURNING *
    `, [
      clientId,
      aoOwnerId,
      filter,
      sortMode,
      startPointType,
      startPoint?.lat ?? null,
      startPoint?.lng ?? null,
      startPoint?.label || startAddress || null,
    ]);
    const route = routeRows[0];

    for (let i = 0; i < orderedStops.length; i += 1) {
      const stop = orderedStops[i];
      await client.query(`
        INSERT INTO ao_route_stops (
          route_id, task_id, lead_id, sequence, address, lat, lng
        ) VALUES ($1,$2,$3,$4,$5,$6,$7)
      `, [route.id, stop.task_id, stop.lead_id, i + 1, stop.address, stop.lat, stop.lng]);
    }

    await client.query('COMMIT');

    const stops = await loadRouteStops(route.id, aoName);
    return {
      route: formatRouteResponse(route, stops),
      needs_address: needsAddress,
      skipped_no_address: needsAddress.length,
    };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function getRouteById(routeId, aoOwnerId, aoName) {
  const { rows } = await pool.query(`
    SELECT * FROM ao_routes WHERE id = $1 AND ao_owner_id = $2 LIMIT 1
  `, [routeId, aoOwnerId]);
  if (!rows.length) return null;
  const stops = await loadRouteStops(routeId, aoName);
  return formatRouteResponse(rows[0], stops);
}

async function updateStopStatus({ stopId, aoOwnerId, status, aoName }) {
  const valid = ['done', 'skipped', 'moved_later'];
  if (!valid.includes(status)) {
    return { error: 'Invalid stop status', status: 400 };
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const { rows } = await client.query(`
      SELECT s.*, r.ao_owner_id, r.id AS route_id, r.status AS route_status, t.id AS task_id
      FROM ao_route_stops s
      JOIN ao_routes r ON r.id = s.route_id
      JOIN ao_follow_up_tasks t ON t.id = s.task_id
      WHERE s.id = $1 AND r.ao_owner_id = $2
      FOR UPDATE
    `, [stopId, aoOwnerId]);
    const stop = rows[0];
    if (!stop) {
      await client.query('ROLLBACK');
      return null;
    }

    await client.query(`
      UPDATE ao_route_stops
      SET status = $2, completed_at = NOW()
      WHERE id = $1
    `, [stopId, status]);

    if (status === 'done') {
      await client.query(`
        UPDATE ao_follow_up_tasks SET status = 'done', completed_at = NOW()
        WHERE id = $1
      `, [stop.task_id]);
    } else if (status === 'moved_later') {
      const d = new Date();
      d.setDate(d.getDate() + 2);
      await client.query(`
        UPDATE ao_follow_up_tasks
        SET due_date = $2, status = 'rescheduled'
        WHERE id = $1
      `, [stop.task_id, d.toISOString().slice(0, 10)]);
    }

    await client.query(`
      UPDATE ao_routes SET status = 'completed', completed_at = NOW()
      WHERE id = $1
        AND NOT EXISTS (
          SELECT 1 FROM ao_route_stops
          WHERE route_id = $1 AND status = 'pending'
        )
    `, [stop.route_id]);

    await client.query('COMMIT');

    const route = await getRouteById(stop.route_id, aoOwnerId, aoName);
    const nextStop = route?.current_stop || null;
    return { route, next_stop: nextStop, next_stop_debrief: buildNextStopDebrief(nextStop, aoName) };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

async function cancelRoute(routeId, aoOwnerId, aoName) {
  const { rows } = await pool.query(`
    UPDATE ao_routes
    SET status = 'cancelled', completed_at = NOW()
    WHERE id = $1 AND ao_owner_id = $2 AND status = 'active'
    RETURNING *
  `, [routeId, aoOwnerId]);
  if (!rows.length) return null;
  const stops = await loadRouteStops(routeId, aoName);
  return formatRouteResponse(rows[0], stops);
}

async function updateLeadAddress({ leadId, aoOwnerId, address }) {
  const trimmed = String(address || '').trim();
  if (!trimmed) return { error: 'Address required', status: 400 };

  const { rows } = await pool.query(`
    UPDATE ao_leads
    SET address = $3, updated_at = NOW()
    WHERE id = $1 AND ao_owner_id = $2
    RETURNING id, business_name, address
  `, [leadId, aoOwnerId, trimmed]);
  if (!rows.length) return null;

  let lat = null;
  let lng = null;
  if (isUsableAddress(trimmed)) {
    const coords = await geocodeAddress(trimmed);
    if (coords) {
      lat = coords.lat;
      lng = coords.lng;
    }
  }

  await pool.query(`
    UPDATE ao_route_stops
    SET address = $3, lat = $4, lng = $5
    WHERE lead_id = $1
      AND route_id IN (SELECT id FROM ao_routes WHERE ao_owner_id = $2 AND status = 'active')
  `, [leadId, aoOwnerId, trimmed, lat, lng]);

  return {
    ...rows[0],
    address_usable: isUsableAddress(trimmed),
    lat,
    lng,
  };
}

async function advanceRouteAfterVisit({ aoOwnerId, taskId, aoName }) {
  const { rows } = await pool.query(`
    SELECT s.id, s.route_id, r.status AS route_status
    FROM ao_route_stops s
    JOIN ao_routes r ON r.id = s.route_id
    WHERE s.task_id = $1 AND r.ao_owner_id = $2 AND r.status = 'active'
    LIMIT 1
  `, [taskId, aoOwnerId]);
  if (!rows.length) return null;

  const stop = rows[0];
  await pool.query(`
    UPDATE ao_route_stops SET status = 'done', completed_at = NOW()
    WHERE id = $1 AND status = 'pending'
  `, [stop.id]);

  await pool.query(`
    UPDATE ao_routes SET status = 'completed', completed_at = NOW()
    WHERE id = $1
      AND NOT EXISTS (
        SELECT 1 FROM ao_route_stops
        WHERE route_id = $1 AND status = 'pending'
      )
  `, [stop.route_id]);

  const route = await getRouteById(stop.route_id, aoOwnerId, aoName);
  const nextStop = route?.current_stop || null;
  return {
    route,
    next_stop: nextStop,
    next_stop_debrief: buildNextStopDebrief(nextStop, aoName),
  };
}

async function geocodeStartPoint({ startPointType, startLat, startLng, startAddress }) {
  if (startPointType === 'current_location' && startLat != null && startLng != null) {
    return { lat: Number(startLat), lng: Number(startLng) };
  }
  if (startPointType === 'anchor_office') {
    return resolveStartPoint({ startPointType: 'anchor_office', anchorOfficeAddress: ANCHOR_OFFICE_ADDRESS });
  }
  if (startPointType === 'custom' && startAddress) {
    const geocoded = await geocodeAddress(startAddress);
    if (geocoded) return geocoded;
    if (startLat != null && startLng != null) return { lat: Number(startLat), lng: Number(startLng) };
  }
  return null;
}

module.exports = {
  ANCHOR_OFFICE_ADDRESS,
  listQueueTasksWithDetails,
  getActiveRoute,
  createRoute,
  getRouteById,
  updateStopStatus,
  cancelRoute,
  updateLeadAddress,
  advanceRouteAfterVisit,
  geocodeStartPoint,
  cancelActiveRoutes,
};
