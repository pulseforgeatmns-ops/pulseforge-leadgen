-- Extend the existing governed contracts to the supported tenant set.
-- Apply after 2026-09-18-anchor-daily-outbound.sql; safe to replay.
BEGIN;
SET LOCAL lock_timeout = '5s';
ALTER TABLE acquisition_outbound_programs
  DROP CONSTRAINT IF EXISTS acquisition_outbound_programs_tenant_id_check;
ALTER TABLE acquisition_outbound_programs
  ADD CONSTRAINT acquisition_outbound_programs_tenant_id_check CHECK (tenant_id IN ('10','13'));

-- Database ingestion is the suppression boundary. It does not wait for an LLM,
-- a successful mission correlation, a scheduler tick, or a background queue.
CREATE OR REPLACE FUNCTION acquisition_outbound_suppress(
  t TEXT, e TEXT, p TEXT, c TEXT, event_key TEXT, kind TEXT, detail JSONB DEFAULT '{}'
) RETURNS VOID LANGUAGE plpgsql AS $$
BEGIN
  IF t IS NULL OR t NOT IN ('10','13') OR e IS NULL OR trim(e) = '' THEN RETURN; END IF;
  e := lower(trim(e));
  INSERT INTO acquisition_outbound_events(id,tenant_id,event_type,payload)
    VALUES(event_key,t,kind,detail || jsonb_build_object('email',e,'prospectId',p,'companyId',c))
    ON CONFLICT DO NOTHING;
  IF NOT FOUND THEN RETURN; END IF;
  INSERT INTO acquisition_outbound_lifecycle(tenant_id,email,prospect_id,company_id,state,next_action)
    VALUES(t,e,p,c,CASE WHEN kind IN ('unsubscribed','hard_bounce','spam','invalid','dnc') THEN 'dnc' ELSE kind END,
      CASE WHEN kind IN ('ao_activity','booked','proposal_pending','human_owned') THEN 'ao_owned'
        WHEN kind IN ('unsubscribed','hard_bounce','spam','invalid','dnc','converted','closed') THEN 'stop' ELSE 'review_reply' END)
    ON CONFLICT(tenant_id,email) DO UPDATE SET suppressed=true,
      company_id=COALESCE(EXCLUDED.company_id,acquisition_outbound_lifecycle.company_id),
      prospect_id=COALESCE(EXCLUDED.prospect_id,acquisition_outbound_lifecycle.prospect_id),
      state=CASE WHEN acquisition_outbound_lifecycle.state='dnc' THEN 'dnc' ELSE EXCLUDED.state END,
      next_action=CASE WHEN acquisition_outbound_lifecycle.state='dnc' THEN 'stop' ELSE EXCLUDED.next_action END,
      last_event_at=now();
  UPDATE acquisition_outbound_items SET status='suppressed',reason=kind
    WHERE tenant_id=t AND status='pending'
      AND (email=e OR (c IS NOT NULL AND company_id=c));
  IF to_regclass('tenant_outreach_scheduled_sends') IS NOT NULL THEN
    UPDATE tenant_outreach_scheduled_sends SET status='CANCELLED',skip_reason=kind,cancelled_at=now()
      WHERE tenant_id=t AND status IN ('SCHEDULED','PAUSED','EXECUTING')
        AND (lower(recipient_email)=e OR prospect_id=p);
  END IF;
END $$;

CREATE OR REPLACE FUNCTION acquisition_outbound_observe_row() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE r JSONB := to_jsonb(NEW); p JSONB; lead JSONB; t TEXT; e TEXT; c TEXT; pid TEXT; k TEXT;
BEGIN
  IF TG_TABLE_NAME='acquisition_mission_provider_events' THEN
    t:=r->>'tenant_id'; pid:=r->>'prospect_id'; k:=r->>'event_type';
    SELECT payload->>'email' INTO e FROM acquisition_mission_outbound_executions WHERE id=r->>'execution_record_id' AND tenant_id=t;
    INSERT INTO acquisition_outbound_events(id,tenant_id,event_type,payload)
      SELECT 'provider:' || (r->>'id'),t,k,r WHERE t IN ('10','13') ON CONFLICT DO NOTHING;
    IF k NOT IN ('replied','unsubscribed','hard_bounce','spam','blocked','invalid') THEN RETURN NEW; END IF;
  ELSIF TG_TABLE_NAME='tenant_outreach_messages' THEN
    IF r->>'direction'<>'inbound' THEN RETURN NEW; END IF;
    t:=r->>'tenant_id'; pid:=r->>'prospect_id'; e:=r->'sender'->>'email'; k:='reply_received';
  ELSIF TG_TABLE_NAME='touchpoints' THEN
    IF r->>'action_type' NOT IN ('inbound_reply','reply','email_reply','reply_received','unsubscribed','out_of_office') THEN RETURN NEW; END IF;
    pid:=r->>'prospect_id'; t:=r->>'client_id'; k:='reply_received';
  ELSIF TG_TABLE_NAME='prospects' THEN
    IF TG_OP='UPDATE' AND (r->>'do_not_contact') IS NOT DISTINCT FROM (to_jsonb(OLD)->>'do_not_contact')
      AND (r->>'setter_status') IS NOT DISTINCT FROM (to_jsonb(OLD)->>'setter_status')
      AND (r->>'closer_status') IS NOT DISTINCT FROM (to_jsonb(OLD)->>'closer_status') THEN RETURN NEW; END IF;
    IF r->>'do_not_contact'='true' THEN k:='dnc';
    ELSIF r->>'setter_status'='won' OR r->>'closer_status' IN ('won','closed_won','client') THEN k:='converted';
    ELSIF r->>'closer_status' IN ('quote_sent','proposal_sent') THEN k:='proposal_pending';
    ELSIF r->>'closer_status' IN ('lost','closed_lost') THEN k:='closed';
    ELSIF r->>'setter_status' IN ('booked','appointment_set') OR r->>'closer_status' IN ('booked','walkthrough_booked','meeting_booked') THEN k:='booked';
    ELSIF NULLIF(r->>'closer_status','') IS NOT NULL THEN k:='human_owned';
    ELSE RETURN NEW; END IF;
    pid:=r->>'id'; t:=r->>'client_id';
  ELSIF TG_TABLE_NAME='ao_leads' THEN
    lead:=r; pid:=r->>'crm_prospect_id'; t:=r->>'client_id'; k:='ao_activity';
  ELSIF TG_TABLE_NAME='ao_follow_up_tasks' THEN
    IF TG_OP='UPDATE' AND NEW IS NOT DISTINCT FROM OLD THEN RETURN NEW; END IF;
    SELECT to_jsonb(l) INTO lead FROM ao_leads l WHERE l.id::text=r->>'lead_id';
    pid:=lead->>'crm_prospect_id'; t:=lead->>'client_id'; k:='ao_activity';
  END IF;
  IF k='ao_activity' AND pid IS NULL AND t IN ('10','13') THEN
    FOR p IN SELECT to_jsonb(x) FROM prospects x JOIN companies co ON co.id=x.company_id
      WHERE x.client_id::text=t AND co.client_id::text=t AND lower(trim(co.name))=lower(trim(lead->>'business_name')) LOOP
      PERFORM acquisition_outbound_suppress(t,p->>'email',p->>'id',p->>'company_id',
        TG_TABLE_NAME || ':' || (r->>'id') || ':' || (p->>'id') || ':' || md5(r::text),k,r);
    END LOOP;
    RETURN NEW;
  END IF;
  IF pid IS NOT NULL THEN
    SELECT to_jsonb(x) INTO p FROM prospects x WHERE x.id::text=pid AND (t IS NULL OR x.client_id::text=t);
    t:=COALESCE(t,p->>'client_id'); e:=COALESCE(e,p->>'email'); c:=p->>'company_id';
  END IF;
  IF c IS NULL AND t IN ('10','13') AND e IS NOT NULL THEN
    SELECT to_jsonb(x) INTO p FROM prospects x WHERE x.client_id::text=t AND lower(x.email)=lower(e) LIMIT 1;
    c:=p->>'company_id';
  END IF;
  PERFORM acquisition_outbound_suppress(t,e,pid,c,TG_TABLE_NAME || ':' || (r->>'id') || ':' || k ||
    CASE WHEN k IN ('ao_activity','booked','converted','proposal_pending','closed','human_owned') THEN ':' || md5(r::text) ELSE '' END,k,r);
  RETURN NEW;
END $$;

COMMIT;
