-- Max v1 AO briefing: CRM promotion link + escalation ignored status + converted lead state
ALTER TABLE ao_leads ADD COLUMN IF NOT EXISTS crm_prospect_id INTEGER REFERENCES prospects(id);

ALTER TABLE ao_leads DROP CONSTRAINT IF EXISTS ao_leads_status_check;
ALTER TABLE ao_leads ADD CONSTRAINT ao_leads_status_check
  CHECK (status IN (
    'new_visit', 'decision_maker_absent', 'needs_follow_up', 'walkthrough_requested',
    'walkthrough_booked', 'walkthrough_completed', 'proposal_needed',
    'closed_won', 'closed_lost', 'not_a_fit', 'do_not_contact', 'converted_to_crm'
  ));

ALTER TABLE ao_escalations DROP CONSTRAINT IF EXISTS ao_escalations_status_check;
ALTER TABLE ao_escalations ADD CONSTRAINT ao_escalations_status_check
  CHECK (status IN ('new', 'seen', 'in_progress', 'resolved', 'ignored'));
