-- AO route planner: planned stop lists for field AOs (V1 address-based sorting)

CREATE TABLE IF NOT EXISTS ao_routes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  client_id INTEGER NOT NULL REFERENCES clients(id),
  ao_owner_id INTEGER NOT NULL REFERENCES users(id),
  queue_filter TEXT NOT NULL DEFAULT 'today',
  sort_mode TEXT NOT NULL DEFAULT 'closest_first'
    CHECK (sort_mode IN ('farthest_first', 'closest_first', 'shortest_route', 'manual')),
  start_point_type TEXT NOT NULL DEFAULT 'current_location'
    CHECK (start_point_type IN ('current_location', 'anchor_office', 'custom')),
  start_lat NUMERIC,
  start_lng NUMERIC,
  start_address TEXT,
  status TEXT NOT NULL DEFAULT 'active'
    CHECK (status IN ('active', 'completed', 'cancelled')),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ao_route_stops (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  route_id UUID NOT NULL REFERENCES ao_routes(id) ON DELETE CASCADE,
  task_id UUID NOT NULL REFERENCES ao_follow_up_tasks(id),
  lead_id UUID NOT NULL REFERENCES ao_leads(id),
  sequence INTEGER NOT NULL,
  address TEXT,
  lat NUMERIC,
  lng NUMERIC,
  status TEXT NOT NULL DEFAULT 'pending'
    CHECK (status IN ('pending', 'done', 'skipped', 'moved_later')),
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ao_routes_owner_active
  ON ao_routes(ao_owner_id, client_id, status)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS idx_ao_route_stops_route_seq
  ON ao_route_stops(route_id, sequence);
