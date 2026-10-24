-- Explicit operator-run migration only. Never run from web startup.
BEGIN;
CREATE TABLE public.anchor_portal_service_requests (
  id SERIAL PRIMARY KEY,
  operator_client_id INTEGER NOT NULL CHECK (operator_client_id = 10),
  location_id INTEGER NOT NULL REFERENCES public.anchor_portal_locations(id),
  requester_id INTEGER NOT NULL REFERENCES public.users(id),
  request_key TEXT NOT NULL CHECK (length(request_key) BETWEEN 8 AND 100),
  preferred_date DATE NOT NULL,
  preferred_window TEXT NOT NULL CHECK (length(preferred_window) BETWEEN 1 AND 200),
  notes TEXT NOT NULL DEFAULT '' CHECK (length(notes) <= 5000),
  status TEXT NOT NULL DEFAULT 'requested' CHECK (status IN ('requested','acknowledged','declined','cancelled')),
  updated_by INTEGER REFERENCES public.users(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (requester_id, request_key)
);
CREATE INDEX anchor_portal_service_requests_location_created_idx
  ON public.anchor_portal_service_requests (location_id, created_at DESC, id DESC);
COMMIT;
-- Separately approved minimum runtime grants (replace reviewed role identifier):
-- GRANT SELECT, INSERT ON public.anchor_portal_service_requests TO reviewed_runtime_role;
-- GRANT UPDATE (status, updated_by, updated_at) ON public.anchor_portal_service_requests TO reviewed_runtime_role;
-- GRANT USAGE ON SEQUENCE public.anchor_portal_service_requests_id_seq TO reviewed_runtime_role;
-- No DELETE, schema CREATE, ownership, role, account, or expiry changes.
