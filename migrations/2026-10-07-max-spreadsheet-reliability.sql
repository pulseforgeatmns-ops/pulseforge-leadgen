-- Apply through migration review; ingestion does not run schema mutations.
BEGIN;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS ao_outreach_review_required boolean NOT NULL DEFAULT false;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS ao_source_address text;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS website text;
ALTER TABLE prospects ADD COLUMN IF NOT EXISTS ao_call_suppressed boolean NOT NULL DEFAULT false;
ALTER TABLE prospects DROP CONSTRAINT IF EXISTS prospects_ao_current_status_check;
ALTER TABLE prospects ADD CONSTRAINT prospects_ao_current_status_check CHECK (ao_current_status IS NULL OR ao_current_status IN ('researching','ready_to_call','call_attempted','contacted','gatekeeper_reached','decision_maker_reached','follow_up_needed','warm','walkthrough_target','walkthrough_booked','proposal_needed','proposal_sent','won','lost','not_a_fit','dead','application_in_progress'));
CREATE TABLE max_spreadsheet_proposals (
 id uuid PRIMARY KEY, client_id integer NOT NULL REFERENCES clients(id), actor_id integer NOT NULL REFERENCES users(id),
 ao_id integer NOT NULL REFERENCES users(id), conversation_id text NOT NULL, source_hash text NOT NULL,
 digest text NOT NULL, baseline_hash text NOT NULL, plan jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','committed','superseded')), receipt jsonb,
 approved_by integer REFERENCES users(id), approved_at timestamptz,
 FOREIGN KEY(client_id,ao_id) REFERENCES users(client_id,id)
);
CREATE TABLE max_spreadsheet_commit_requests (
 client_id integer NOT NULL REFERENCES clients(id), idempotency_key text NOT NULL,
 request_digest text NOT NULL, proposal_id uuid NOT NULL REFERENCES max_spreadsheet_proposals(id),
 PRIMARY KEY(client_id,idempotency_key)
);
CREATE TABLE max_spreadsheet_effects (
 client_id integer NOT NULL REFERENCES clients(id), semantic_key text NOT NULL, proposal_id uuid NOT NULL REFERENCES max_spreadsheet_proposals(id),
 operation jsonb NOT NULL, observed jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(client_id,semantic_key)
);
CREATE TABLE max_spreadsheet_contacts (
 id uuid PRIMARY KEY, client_id integer NOT NULL, prospect_id uuid NOT NULL, data jsonb NOT NULL,
 FOREIGN KEY(client_id,prospect_id) REFERENCES prospects(client_id,id)
);
CREATE TABLE max_spreadsheet_suppressions (
 id uuid PRIMARY KEY, client_id integer NOT NULL, prospect_id uuid NOT NULL, contact_id uuid,
 channel text NOT NULL CHECK(channel='call'), data jsonb NOT NULL,
 FOREIGN KEY(client_id,prospect_id) REFERENCES prospects(client_id,id),
 FOREIGN KEY(contact_id) REFERENCES max_spreadsheet_contacts(id)
);
CREATE TABLE max_spreadsheet_relationships (
 id uuid PRIMARY KEY, client_id integer NOT NULL, prospect_id uuid NOT NULL, provider_id uuid NOT NULL, data jsonb NOT NULL,
 FOREIGN KEY(client_id,prospect_id) REFERENCES prospects(client_id,id),
 FOREIGN KEY(client_id,provider_id) REFERENCES prospects(client_id,id)
);
CREATE FUNCTION max_spreadsheet_proposal_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.client_id,NEW.actor_id,NEW.ao_id,NEW.conversation_id,NEW.source_hash,NEW.digest,NEW.baseline_hash,NEW.plan)
 IS DISTINCT FROM (OLD.client_id,OLD.actor_id,OLD.ao_id,OLD.conversation_id,OLD.source_hash,OLD.digest,OLD.baseline_hash,OLD.plan)
 THEN RAISE EXCEPTION 'Spreadsheet proposal is immutable'; END IF;
 IF OLD.status<>'pending' AND NEW IS DISTINCT FROM OLD THEN RAISE EXCEPTION 'Finalized spreadsheet proposal is immutable'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER max_spreadsheet_proposal_immutable BEFORE UPDATE ON max_spreadsheet_proposals FOR EACH ROW EXECUTE FUNCTION max_spreadsheet_proposal_immutable();
COMMIT;
