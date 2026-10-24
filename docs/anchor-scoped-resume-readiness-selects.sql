-- Anchor tenant-10 scoped resume READINESS EXPORT PROPOSAL, 2026-10-19.
-- SELECT-only. Prepared locally; not executed against any database by its author.
-- Run each numbered statement separately, only under the approved read-only check.
-- No DDL, mutation, transaction commands, application functions, locks, provider calls,
-- authentication, preparation, approval, schema initialization, or sending occur here.
-- Explicit text placeholders intentionally return NO DATA until replaced.
-- The explicit program must remain the SOLE tenant-10 non-revoked program.
-- Q0/Q0b are metadata-only. Q1 returns allowed mission IDs for Q2/Q3.
-- Q2/Q3 require explicit mission IDs; Q3 additionally requires exact candidate keys
-- from Q2's current Emmett queue. Do not expand to all tenant missions or prospects.
-- Q6-Q8 reference optional recovery tables: RUN ONLY after Q0 proves they exist.
-- Missing tables/columns, NULL, stale or truncated exports are UNKNOWN, not zero/pass.
-- Limits <=200 are output bounds, never proof the complete population was examined.
-- Export exact JSON without changing strings/types; do not compute canonical hashes
-- from PostgreSQL jsonb::text. Runtime JavaScript canonicalization is authoritative.
-- These are observational projections, NOT eligible/sendable decisions or approvals.

-- Q0: metadata for optional review flags and recovery readiness tables
SELECT 'column' AS object_kind,v.object_name,
  a.attname IS NOT NULL AS present,
  CASE WHEN a.attname IS NOT NULL THEN pg_catalog.format_type(a.atttypid,a.atttypmod) END AS data_type,
  a.attnotnull AS not_null,pg_catalog.pg_get_expr(d.adbin,d.adrelid) AS default_expression
FROM (VALUES ('ao_outreach_review_required'),('ao_call_suppressed')) v(object_name)
LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=to_regclass('public.prospects')
  AND a.attname=v.object_name AND a.attnum>0 AND NOT a.attisdropped
LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
UNION ALL
SELECT 'table',v.object_name,to_regclass('public.'||v.object_name) IS NOT NULL,NULL,NULL,NULL
FROM (VALUES ('prospects'),('companies'),('clients'),('users'),('ao_leads'),('ao_contacts'),('ao_prospect_tasks'),('touchpoints'),('acquisition_outbound_replies'),('acquisition_outbound_programs'),('acquisition_missions'),('acquisition_mission_contributions'),
 ('acquisition_outbound_envelopes'),('acquisition_outbound_items'),('acquisition_outbound_preparation'),
 ('acquisition_mission_outbound_executions'),('acquisition_outbound_lifecycle'),('acquisition_outbound_inbox_health'),
 ('tenant_mailbox_integrations'),('anchor_email_recovery_control'),('anchor_email_quarantines'),
 ('anchor_email_recovery_reservations'),('anchor_scoped_resume_grants'),('anchor_scoped_resume_consumption')) v(object_name)
ORDER BY object_kind,object_name LIMIT 200;

-- Q0b: only if current program is unknown or Q1 returns zero rows; metadata only
SELECT id,mode,source_mission_id,authorized_at FROM public.acquisition_outbound_programs
WHERE tenant_id='10' AND mode<>'revoked' ORDER BY authorized_at DESC,id LIMIT 20;

-- Q1: verify explicit current program; limits, source scope and TODAY artifacts
WITH input AS (
  SELECT '__REQUIRED_PROGRAM_ID__'::text AS program_id
), p AS (
  SELECT p.* FROM public.acquisition_outbound_programs p CROSS JOIN input x
  WHERE p.tenant_id='10' AND p.mode<>'revoked' AND p.id=x.program_id
    AND x.program_id ~ '^outbound_[a-f0-9]{24}$'
    AND (SELECT count(*) FROM public.acquisition_outbound_programs z
         WHERE z.tenant_id='10' AND z.mode<>'revoked')=1
)
SELECT now() AS observed_at,(now() AT TIME ZONE 'America/New_York')::date AS local_day,
 p.id,p.mode,p.source_mission_id,p.policy_hash,p.scope_hash,p.authorized_by,p.authorized_at,
 -- All policy keys participate in the policy hash and strict runtime validation;
 -- this policy is program authority, never mailbox secrets or credentials.
 p.policy,
 m.stage AS source_stage,m.status AS source_status,
 jsonb_build_object('id',m.payload->'id','tenantId',m.payload->'tenantId',
   'objective',m.payload->'objective','targetSegment',m.payload->'targetSegment',
   'structuredMission',m.payload->'structuredMission','constraints',m.payload->'constraints',
   'resolvedObjective',m.payload->'resolvedObjective','planCancelled',m.payload->'planCancelled') AS source_scope_inputs,
 pr.mission_id AS today_preparation_mission_id,pr.attempts AS today_preparation_attempts,
 pr.last_attempt_at AS preparation_last_attempt_at,
 e.id AS today_envelope_id,e.mission_id AS today_envelope_mission_id,
 e.revision,e.status AS envelope_status,e.approval_id,e.manifest_hash
FROM p LEFT JOIN public.acquisition_missions m ON m.id=p.source_mission_id AND m.tenant_id='10'
LEFT JOIN public.acquisition_outbound_preparation pr ON pr.program_id=p.id
 AND pr.local_day=(now() AT TIME ZONE 'America/New_York')::date
LEFT JOIN public.acquisition_outbound_envelopes e ON e.program_id=p.id AND e.tenant_id='10'
 AND e.local_day=(now() AT TIME ZONE 'America/New_York')::date LIMIT 1;

-- Q2: current artifact IDs and selected preparation fields, not discovery history
WITH input AS (
  SELECT '__REQUIRED_PROGRAM_ID__'::text AS program_id
), p AS (
  SELECT p.* FROM public.acquisition_outbound_programs p CROSS JOIN input x
  WHERE p.tenant_id='10' AND p.mode<>'revoked' AND p.id=x.program_id
    AND x.program_id ~ '^outbound_[a-f0-9]{24}$'
    AND (SELECT count(*) FROM public.acquisition_outbound_programs z
         WHERE z.tenant_id='10' AND z.mode<>'revoked')=1
), requested_missions AS (
  -- Replace only with source/today-preparation/today-envelope IDs from Q1.
  SELECT unnest(ARRAY['__REQUIRED_MISSION_ID__']::text[]) AS mission_id
), allowed_missions AS (
  SELECT source_mission_id AS mission_id FROM p
  UNION SELECT pr.mission_id FROM public.acquisition_outbound_preparation pr JOIN p ON p.id=pr.program_id
    WHERE pr.local_day=(now() AT TIME ZONE 'America/New_York')::date
  UNION SELECT e.mission_id FROM public.acquisition_outbound_envelopes e JOIN p ON p.id=e.program_id
    WHERE e.tenant_id='10' AND e.local_day=(now() AT TIME ZONE 'America/New_York')::date
), m AS (
  SELECT m.* FROM public.acquisition_missions m JOIN allowed_missions a ON a.mission_id=m.id
  WHERE m.tenant_id='10' AND m.id IN (SELECT mission_id FROM requested_missions)
    AND (SELECT count(*) FROM requested_missions) BETWEEN 1 AND 3
), artifacts AS (
  SELECT m.id AS mission_id,m.payload AS mission_payload,c.id,c.specialist,c.kind,c.at,c.payload,
    CASE WHEN jsonb_typeof(c.payload->'payload')='object'
         THEN c.payload->'payload' ELSE c.payload END AS body,
    CASE c.specialist WHEN 'max' THEN 'maxContributionId'
      WHEN 'paige' THEN 'paigeContributionId' WHEN 'emmett' THEN 'emmettContributionId' END AS pointer_key
  FROM m JOIN public.acquisition_mission_contributions c ON c.mission_id=m.id AND c.tenant_id='10'
  WHERE (c.specialist,c.kind) IN (('max','prioritization'),('paige','variants'),('emmett','capacity'))
    AND c.payload->'superseded' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->'superseded' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->'payload'->'superseded' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'rolledBack' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->>'commitStatus' IS DISTINCT FROM 'rolled_back'
    AND c.payload->'payload'->'rolledBack' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->>'commitStatus' IS DISTINCT FROM 'rolled_back'
    AND c.payload->'payload'->'payload'->'rolledBack' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->'payload'->>'commitStatus' IS DISTINCT FROM 'rolled_back'
), ranked AS (
  SELECT a.*,row_number() OVER(PARTITION BY mission_id,specialist,kind ORDER BY
    (id=coalesce(nullif(mission_payload->'revisionState'->>pointer_key,''),
                 mission_payload->'pendingOperatorDecision'->'executionReview'->'artifactBinding'->>pointer_key)) DESC NULLS LAST,
    at DESC,id DESC) AS rn,
    count(*) OVER(PARTITION BY mission_id,specialist,kind) AS active_artifact_count
  FROM artifacts a
), current_artifacts AS (
  SELECT * FROM ranked WHERE rn=1
)
SELECT a.mission_id,m.stage,m.status,m.updated_at,
 m.payload->'revisionState' AS revision_state,
 m.payload->'pendingOperatorDecision'->'executionReview'->'artifactBinding' AS review_artifact_binding,
 a.id AS contribution_id,a.specialist,a.kind,a.at,a.active_artifact_count,
 a.payload->>'id' AS payload_id,a.payload->>'missionId' AS payload_mission_id,
 a.payload->>'at' AS payload_at,
 CASE WHEN a.specialist='max' THEN jsonb_build_object('rankedTargets',a.body->'rankedTargets')
 WHEN a.specialist='paige' THEN jsonb_build_object('variants',a.body->'variants','outreachSequence',a.body->'outreachSequence','cta',a.body->'cta')
 WHEN a.specialist='emmett' THEN jsonb_build_object('capacity',a.body->'capacity','governor',a.body->'governor',
   'senderIdentity',a.body->'senderIdentity','inboxId',a.body->'inboxId','domain',a.body->'domain',
   'queueCount',CASE WHEN jsonb_typeof(a.body->'queue'->'items')='array' THEN jsonb_array_length(a.body->'queue'->'items') END,
   'queueItems',(SELECT jsonb_agg(jsonb_build_object('id',q.item->'id','prospectId',q.item->'prospectId',
       'candidateId',q.item->'candidateId','companyId',q.item->'companyId','email',q.item->'email',
       'sendable',q.item->'sendable','dnc',q.item->'dnc','paige',q.item->'paige') ORDER BY q.ord)
     FROM jsonb_array_elements(CASE WHEN jsonb_typeof(a.body->'queue'->'items')='array'
       THEN a.body->'queue'->'items' ELSE '[]'::jsonb END) WITH ORDINALITY q(item,ord) WHERE q.ord<=200)) END AS preparation_fields,
 'UNVERIFIED: compare canonical selection, revision, copy and upstream chain in pure local code' AS interpretation
FROM current_artifacts a JOIN m ON m.id=a.mission_id ORDER BY a.mission_id,a.specialist LIMIT 200;

-- Q3: selected candidate CRM identity/provenance and prior attempt counts; no automatic contact choice
WITH input AS (
  SELECT '__REQUIRED_PROGRAM_ID__'::text AS program_id
), p AS (
  SELECT p.* FROM public.acquisition_outbound_programs p CROSS JOIN input x
  WHERE p.tenant_id='10' AND p.mode<>'revoked' AND p.id=x.program_id
    AND x.program_id ~ '^outbound_[a-f0-9]{24}$'
    AND (SELECT count(*) FROM public.acquisition_outbound_programs z
         WHERE z.tenant_id='10' AND z.mode<>'revoked')=1
), requested_missions AS (
  -- Replace only with source/today-preparation/today-envelope IDs from Q1.
  SELECT unnest(ARRAY['__REQUIRED_MISSION_ID__']::text[]) AS mission_id
), allowed_missions AS (
  SELECT source_mission_id AS mission_id FROM p
  UNION SELECT pr.mission_id FROM public.acquisition_outbound_preparation pr JOIN p ON p.id=pr.program_id
    WHERE pr.local_day=(now() AT TIME ZONE 'America/New_York')::date
  UNION SELECT e.mission_id FROM public.acquisition_outbound_envelopes e JOIN p ON p.id=e.program_id
    WHERE e.tenant_id='10' AND e.local_day=(now() AT TIME ZONE 'America/New_York')::date
), m AS (
  SELECT m.* FROM public.acquisition_missions m JOIN allowed_missions a ON a.mission_id=m.id
  WHERE m.tenant_id='10' AND m.id IN (SELECT mission_id FROM requested_missions)
    AND (SELECT count(*) FROM requested_missions) BETWEEN 1 AND 3
), artifacts AS (
  SELECT m.id AS mission_id,m.payload AS mission_payload,c.id,c.specialist,c.kind,c.at,c.payload,
    CASE WHEN jsonb_typeof(c.payload->'payload')='object'
         THEN c.payload->'payload' ELSE c.payload END AS body,
    CASE c.specialist WHEN 'max' THEN 'maxContributionId'
      WHEN 'paige' THEN 'paigeContributionId' WHEN 'emmett' THEN 'emmettContributionId' END AS pointer_key
  FROM m JOIN public.acquisition_mission_contributions c ON c.mission_id=m.id AND c.tenant_id='10'
  WHERE (c.specialist,c.kind) IN (('max','prioritization'),('paige','variants'),('emmett','capacity'))
    AND c.payload->'superseded' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->'superseded' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->'payload'->'superseded' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'rolledBack' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->>'commitStatus' IS DISTINCT FROM 'rolled_back'
    AND c.payload->'payload'->'rolledBack' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->>'commitStatus' IS DISTINCT FROM 'rolled_back'
    AND c.payload->'payload'->'payload'->'rolledBack' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->'payload'->>'commitStatus' IS DISTINCT FROM 'rolled_back'
), ranked AS (
  SELECT a.*,row_number() OVER(PARTITION BY mission_id,specialist,kind ORDER BY
    (id=coalesce(nullif(mission_payload->'revisionState'->>pointer_key,''),
                 mission_payload->'pendingOperatorDecision'->'executionReview'->'artifactBinding'->>pointer_key)) DESC NULLS LAST,
    at DESC,id DESC) AS rn,
    count(*) OVER(PARTITION BY mission_id,specialist,kind) AS active_artifact_count
  FROM artifacts a
), current_artifacts AS (
  SELECT * FROM ranked WHERE rn=1
), candidate_keys AS (
  SELECT a.mission_id,a.id AS capacity_id,coalesce(nullif(q.item->>'prospectId',''),q.item->>'id') AS candidate_key,
    q.item,q.ord
  FROM current_artifacts a
  CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(a.body->'queue'->'items')='array'
    THEN a.body->'queue'->'items' ELSE '[]'::jsonb END) WITH ORDINALITY q(item,ord)
  WHERE a.specialist='emmett' AND q.ord<=200
    AND coalesce(nullif(q.item->>'prospectId',''),q.item->>'id')=ANY(ARRAY['__REQUIRED_CANDIDATE_KEY__']::text[])
), crm AS (
  SELECT k.*,r AS prospect_row,c AS company_row
  FROM candidate_keys k JOIN public.prospects r ON r.client_id=10
  JOIN public.companies c ON c.id=r.company_id AND c.client_id=10
  WHERE r.id::text=k.candidate_key OR r.company_id::text=k.candidate_key
    OR lower(to_jsonb(c)->>'domain')=lower(k.candidate_key)
    OR to_jsonb(c)->>'google_place_id'=k.candidate_key
)
SELECT x.mission_id,x.capacity_id,x.candidate_key,x.ord,
 (x.prospect_row).id::text AS prospect_id,(x.prospect_row).company_id::text AS company_id,
 to_jsonb(x.prospect_row)->>'first_name' AS first_name,to_jsonb(x.prospect_row)->>'last_name' AS last_name,
 (x.prospect_row).email AS recipient_email,(x.company_row).name AS company_name,
 to_jsonb(x.company_row)->>'domain' AS company_domain,to_jsonb(x.company_row)->>'website' AS company_website,
 to_jsonb(x.prospect_row)->'do_not_contact' AS do_not_contact,to_jsonb(x.prospect_row)->'is_synthetic' AS is_synthetic,
 CASE WHEN NOT (to_jsonb(x.prospect_row) ? 'ao_outreach_review_required') THEN 'MISSING_COLUMN'
   WHEN to_jsonb(x.prospect_row)->'ao_outreach_review_required'='null'::jsonb THEN 'NULL_UNKNOWN'
   WHEN to_jsonb(x.prospect_row)->'ao_outreach_review_required'='false'::jsonb THEN 'EXPLICIT_FALSE'
   WHEN to_jsonb(x.prospect_row)->'ao_outreach_review_required'='true'::jsonb THEN 'EXPLICIT_TRUE_HOLD'
   ELSE 'INCOMPATIBLE_UNKNOWN' END AS review_state,
 to_jsonb(x.prospect_row)->'ao_call_suppressed' AS call_only_flag_observation,
 to_jsonb(x.prospect_row)->'email_verified' AS email_verified,
 to_jsonb(x.prospect_row)->'email_status' AS email_status,
 to_jsonb(x.prospect_row)->'email_verification_method' AS email_verification_method,
 jsonb_build_object('id',(x.prospect_row).id::text,'client_id',(x.prospect_row).client_id,
   'company_id',(x.prospect_row).company_id::text,'email',(x.prospect_row).email,
   'email_verified',to_jsonb(x.prospect_row)->'email_verified','email_status',to_jsonb(x.prospect_row)->'email_status',
   'enrichment_provenance',to_jsonb(x.prospect_row)->'enrichment_provenance',
   'acquisition_metadata',to_jsonb(x.prospect_row)->'acquisition_metadata',
   'contact_classification',to_jsonb(x.prospect_row)->'contact_classification',
   'source',to_jsonb(x.prospect_row)->'source','source_type',to_jsonb(x.prospect_row)->'source_type',
   'source_ref',to_jsonb(x.prospect_row)->'source_ref','import_metadata',to_jsonb(x.prospect_row)->'import_metadata',
   'acquisition_source',to_jsonb(x.prospect_row)->'acquisition_source',
   'acquisition_knowledge_object_id',to_jsonb(x.prospect_row)->'acquisition_knowledge_object_id',
   'acquisition_projection_id',to_jsonb(x.prospect_row)->'acquisition_projection_id') AS fingerprint_prospect_input,
 jsonb_build_object('id',(x.company_row).id::text,'client_id',(x.company_row).client_id,'name',(x.company_row).name,
   'domain',to_jsonb(x.company_row)->'domain','website',to_jsonb(x.company_row)->'website',
   'acquisition_metadata',to_jsonb(x.company_row)->'acquisition_metadata') AS fingerprint_company_input,
 (SELECT count(*) FROM public.acquisition_outbound_items i WHERE i.tenant_id='10'
    AND (i.prospect_id=(x.prospect_row).id::text OR i.company_id=(x.prospect_row).company_id::text
      OR lower(btrim(i.email))=lower(btrim((x.prospect_row).email)))
    AND (i.attempted_at IS NOT NULL OR i.provider_message_id IS NOT NULL OR i.status IN ('attempted','uncertain','sent'))) AS prior_governed_attempt_count,
 (SELECT count(*) FROM public.acquisition_mission_outbound_executions e WHERE e.tenant_id='10'
    AND (e.prospect_id=x.candidate_key OR e.prospect_id=(x.prospect_row).id::text
      OR lower(btrim(e.payload->>'email'))=lower(btrim((x.prospect_row).email)))
    AND (e.attempted_at IS NOT NULL OR e.provider_message_id IS NOT NULL OR e.status IN ('attempted','sent','failed'))) AS prior_canonical_attempt_count,
 (SELECT count(*) FROM public.acquisition_outbound_lifecycle l WHERE l.tenant_id='10' AND l.suppressed
    AND (l.prospect_id=(x.prospect_row).id::text OR l.company_id=(x.prospect_row).company_id::text
      OR lower(btrim(l.email))=lower(btrim((x.prospect_row).email)))) AS suppression_count,
 count(*) OVER() AS total_matching_rows_before_limit,
 'UNVERIFIED: multiple CRM matches require the canonical resolver; no eligibility asserted' AS interpretation
FROM crm x ORDER BY x.mission_id,x.ord,(x.prospect_row).id LIMIT 200;

-- Q4: TODAY frozen messages, sender, IDs and exact canonical approval fields
WITH input AS (
  SELECT '__REQUIRED_PROGRAM_ID__'::text AS program_id
), p AS (
  SELECT p.* FROM public.acquisition_outbound_programs p CROSS JOIN input x
  WHERE p.tenant_id='10' AND p.mode<>'revoked' AND p.id=x.program_id
    AND x.program_id ~ '^outbound_[a-f0-9]{24}$'
    AND (SELECT count(*) FROM public.acquisition_outbound_programs z
         WHERE z.tenant_id='10' AND z.mode<>'revoked')=1
)
SELECT e.id AS envelope_id,e.mission_id,e.local_day,e.status AS envelope_status,e.revision,e.manifest_hash,
 e.approval_id,i.id AS item_id,i.candidate_id,i.prospect_id,i.company_id,i.email,
 i.status,i.reason,i.attempted_at,i.provider_message_id,
 i.snapshot->'message' AS frozen_message,i.snapshot->'sender' AS frozen_sender,
 i.snapshot->'revision' AS frozen_revision,
 (SELECT count(*) FROM jsonb_array_elements(CASE WHEN jsonb_typeof(e.manifest)='array' THEN e.manifest ELSE '[]'::jsonb END) f
   WHERE f->>'candidateId'=i.candidate_id) AS manifest_candidate_match_count,
 (SELECT count(*) FROM jsonb_array_elements(CASE WHEN jsonb_typeof(e.manifest)='array' THEN e.manifest ELSE '[]'::jsonb END) f
   WHERE f=i.snapshot) AS manifest_exact_snapshot_match_count,
 a.specialist AS approval_specialist,a.kind AS approval_kind,a.payload->'payload' AS canonical_approval_fields,
 count(*) OVER() AS total_items_before_limit
FROM p JOIN public.acquisition_outbound_envelopes e ON e.program_id=p.id AND e.tenant_id='10'
LEFT JOIN public.acquisition_outbound_items i ON i.envelope_id=e.id AND i.tenant_id='10'
LEFT JOIN public.acquisition_mission_contributions a ON a.id=e.approval_id AND a.mission_id=e.mission_id AND a.tenant_id='10'
WHERE e.local_day=(now() AT TIME ZONE 'America/New_York')::date ORDER BY e.id,i.id LIMIT 200;

-- Q5: tenant-10 sender and program/tenant counts; database evidence, not provider readiness
WITH input AS (
  SELECT '__REQUIRED_PROGRAM_ID__'::text AS program_id
), p AS (
  SELECT p.* FROM public.acquisition_outbound_programs p CROSS JOIN input x
  WHERE p.tenant_id='10' AND p.mode<>'revoked' AND p.id=x.program_id
    AND x.program_id ~ '^outbound_[a-f0-9]{24}$'
    AND (SELECT count(*) FROM public.acquisition_outbound_programs z
         WHERE z.tenant_id='10' AND z.mode<>'revoked')=1
)
SELECT now() AS observed_at,p.id,p.mode,p.policy->'dailyCap' AS daily_cap,p.policy->'totalCap' AS total_cap,
 p.policy->'operatorDelegatedMaximumDailyCapacity' AS delegated_daily_cap,
 p.policy->'spacingMinutes' AS spacing_minutes,p.policy->'preparationAttemptsPerDay' AS preparation_attempts_per_day,
 c.active,c.autosend_enabled,c.sender_email,c.sender_name,c.sending_domain,
 to_jsonb(c)->'email_sending_enabled' AS email_sending_enabled,
 to_jsonb(c)->'warmup_start_date' AS warmup_start_date,
 mi.id AS inbox_id,mi.status AS inbox_status,mi.mailbox_address,h.last_success_at AS inbox_last_success_at,
 (SELECT count(*) FROM public.users u WHERE u.client_id=10 AND u.active=true
   AND u.id::text IN (SELECT jsonb_array_elements_text(CASE WHEN jsonb_typeof(p.policy->'aoOwnerIds')='array'
      THEN p.policy->'aoOwnerIds' ELSE '[]'::jsonb END))) AS active_approved_ao_owner_count,
 (SELECT count(*) FROM pg_catalog.pg_trigger WHERE tgname='acquisition_outbound_observe'
   AND tgenabled IN ('O','A') AND NOT tgisinternal) AS enabled_suppression_trigger_count,
 totals.*,
 (SELECT count(*) FROM public.acquisition_outbound_replies r WHERE r.tenant_id='10' AND r.classified_at IS NULL) AS tenant_reply_backlog,
 'UNKNOWN: provider authentication, live governor/capacity, environment gates and runtime checks not evaluated' AS current_send_availability
FROM p JOIN public.clients c ON c.id=10
LEFT JOIN public.tenant_mailbox_integrations mi ON mi.tenant_id='10' AND mi.id=p.policy->>'inboxIntegrationId'
LEFT JOIN public.acquisition_outbound_inbox_health h ON h.tenant_id='10' AND h.integration_id=mi.id
CROSS JOIN LATERAL (
 SELECT count(*) FILTER(WHERE e.program_id=p.id) AS program_prepared_total,
   count(*) FILTER(WHERE e.program_id=p.id AND i.attempted_at IS NOT NULL) AS program_attempted_total,
   count(*) FILTER(WHERE i.attempted_at IS NOT NULL AND e.local_day=(now() AT TIME ZONE 'America/New_York')::date) AS tenant_attempted_today,
   count(*) FILTER(WHERE e.program_id=p.id AND i.status='pending') AS program_pending_all_days,
   count(*) FILTER(WHERE e.program_id=p.id AND i.status='pending' AND e.local_day=(now() AT TIME ZONE 'America/New_York')::date) AS program_pending_today,
   count(*) FILTER(WHERE e.program_id=p.id AND i.status='sent') AS program_sent_count,
   count(*) FILTER(WHERE i.status IN ('attempted','uncertain')) AS tenant_raw_unresolved_count,
   max(i.attempted_at) AS tenant_last_attempt
 FROM public.acquisition_outbound_items i JOIN public.acquisition_outbound_envelopes e ON e.id=i.envelope_id AND e.tenant_id='10'
 WHERE i.tenant_id='10'
) totals LIMIT 1;

-- Q6 OPTIONAL: installed control state, current-program quarantines and reservations (Q0 first)
WITH input AS (
  SELECT '__REQUIRED_PROGRAM_ID__'::text AS program_id
), p AS (
  SELECT p.* FROM public.acquisition_outbound_programs p CROSS JOIN input x
  WHERE p.tenant_id='10' AND p.mode<>'revoked' AND p.id=x.program_id
    AND x.program_id ~ '^outbound_[a-f0-9]{24}$'
    AND (SELECT count(*) FROM public.acquisition_outbound_programs z
         WHERE z.tenant_id='10' AND z.mode<>'revoked')=1
)
SELECT p.id,
 (SELECT jsonb_agg(jsonb_build_object('singleton',r.singleton,'maintenanceArmed',r.maintenance_armed,
   'manifestId',r.manifest->>'id','manifestKind',r.manifest->>'kind','manifestProgramId',r.manifest->>'programId',
   'validFrom',r.manifest->>'validFrom','expiresAt',r.manifest->>'expiresAt','scopes',r.manifest->'scopes'))
   FROM public.anchor_email_recovery_control r) AS installed_control_observation,
 (SELECT count(*) FROM public.anchor_email_quarantines q WHERE q.tenant_id='10') AS tenant_quarantine_count,
 (SELECT count(*) FROM public.anchor_email_quarantines q WHERE q.tenant_id='10' AND q.program_id=p.id) AS program_quarantine_count,
 (SELECT count(*) FROM public.anchor_email_recovery_reservations r JOIN public.acquisition_outbound_items i ON i.id=r.item_id AND i.tenant_id='10'
   JOIN public.acquisition_outbound_envelopes e ON e.id=i.envelope_id AND e.tenant_id='10' WHERE e.program_id=p.id) AS program_reservations,
 (SELECT count(*) FROM public.anchor_email_recovery_reservations r JOIN public.acquisition_outbound_items i ON i.id=r.item_id AND i.tenant_id='10'
   JOIN public.acquisition_outbound_envelopes e ON e.id=i.envelope_id AND e.tenant_id='10' WHERE e.program_id=p.id AND r.dispatch_started_at IS NOT NULL) AS program_dispatch_started
FROM p LIMIT 1;

-- Q7 OPTIONAL: selected-identity quarantine/reservation matches, including conservative alias inputs
WITH input AS (
  SELECT '__REQUIRED_PROGRAM_ID__'::text AS program_id
), p AS (
  SELECT p.* FROM public.acquisition_outbound_programs p CROSS JOIN input x
  WHERE p.tenant_id='10' AND p.mode<>'revoked' AND p.id=x.program_id
    AND x.program_id ~ '^outbound_[a-f0-9]{24}$'
    AND (SELECT count(*) FROM public.acquisition_outbound_programs z
         WHERE z.tenant_id='10' AND z.mode<>'revoked')=1
), requested_missions AS (
  -- Replace only with source/today-preparation/today-envelope IDs from Q1.
  SELECT unnest(ARRAY['__REQUIRED_MISSION_ID__']::text[]) AS mission_id
), allowed_missions AS (
  SELECT source_mission_id AS mission_id FROM p
  UNION SELECT pr.mission_id FROM public.acquisition_outbound_preparation pr JOIN p ON p.id=pr.program_id
    WHERE pr.local_day=(now() AT TIME ZONE 'America/New_York')::date
  UNION SELECT e.mission_id FROM public.acquisition_outbound_envelopes e JOIN p ON p.id=e.program_id
    WHERE e.tenant_id='10' AND e.local_day=(now() AT TIME ZONE 'America/New_York')::date
), m AS (
  SELECT m.* FROM public.acquisition_missions m JOIN allowed_missions a ON a.mission_id=m.id
  WHERE m.tenant_id='10' AND m.id IN (SELECT mission_id FROM requested_missions)
    AND (SELECT count(*) FROM requested_missions) BETWEEN 1 AND 3
), artifacts AS (
  SELECT m.id AS mission_id,m.payload AS mission_payload,c.id,c.specialist,c.kind,c.at,c.payload,
    CASE WHEN jsonb_typeof(c.payload->'payload')='object'
         THEN c.payload->'payload' ELSE c.payload END AS body,
    CASE c.specialist WHEN 'max' THEN 'maxContributionId'
      WHEN 'paige' THEN 'paigeContributionId' WHEN 'emmett' THEN 'emmettContributionId' END AS pointer_key
  FROM m JOIN public.acquisition_mission_contributions c ON c.mission_id=m.id AND c.tenant_id='10'
  WHERE (c.specialist,c.kind) IN (('max','prioritization'),('paige','variants'),('emmett','capacity'))
    AND c.payload->'superseded' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->'superseded' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->'payload'->'superseded' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'rolledBack' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->>'commitStatus' IS DISTINCT FROM 'rolled_back'
    AND c.payload->'payload'->'rolledBack' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->>'commitStatus' IS DISTINCT FROM 'rolled_back'
    AND c.payload->'payload'->'payload'->'rolledBack' IS DISTINCT FROM 'true'::jsonb
    AND c.payload->'payload'->'payload'->>'commitStatus' IS DISTINCT FROM 'rolled_back'
), ranked AS (
  SELECT a.*,row_number() OVER(PARTITION BY mission_id,specialist,kind ORDER BY
    (id=coalesce(nullif(mission_payload->'revisionState'->>pointer_key,''),
                 mission_payload->'pendingOperatorDecision'->'executionReview'->'artifactBinding'->>pointer_key)) DESC NULLS LAST,
    at DESC,id DESC) AS rn,
    count(*) OVER(PARTITION BY mission_id,specialist,kind) AS active_artifact_count
  FROM artifacts a
), current_artifacts AS (
  SELECT * FROM ranked WHERE rn=1
), candidate_keys AS (
  SELECT a.mission_id,a.id AS capacity_id,coalesce(nullif(q.item->>'prospectId',''),q.item->>'id') AS candidate_key,
    q.item,q.ord
  FROM current_artifacts a
  CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(a.body->'queue'->'items')='array'
    THEN a.body->'queue'->'items' ELSE '[]'::jsonb END) WITH ORDINALITY q(item,ord)
  WHERE a.specialist='emmett' AND q.ord<=200
    AND coalesce(nullif(q.item->>'prospectId',''),q.item->>'id')=ANY(ARRAY['__REQUIRED_CANDIDATE_KEY__']::text[])
), crm AS (
  SELECT k.*,r AS prospect_row,c AS company_row
  FROM candidate_keys k JOIN public.prospects r ON r.client_id=10
  JOIN public.companies c ON c.id=r.company_id AND c.client_id=10
  WHERE r.id::text=k.candidate_key OR r.company_id::text=k.candidate_key
    OR lower(to_jsonb(c)->>'domain')=lower(k.candidate_key)
    OR to_jsonb(c)->>'google_place_id'=k.candidate_key
)
SELECT x.mission_id,x.candidate_key,(x.prospect_row).id::text AS prospect_id,(x.company_row).id::text AS company_id,
 (SELECT count(*) FROM public.anchor_email_recovery_reservations r WHERE r.prospect_id=(x.prospect_row).id::text
   OR r.company_id=(x.company_row).id::text OR r.recipient_email=lower(btrim((x.prospect_row).email))) AS consumed_identity_reservations,
 (SELECT count(*) FROM public.anchor_email_quarantines q WHERE q.tenant_id='10' AND
   (q.prospect_id=(x.prospect_row).id::text OR q.company_id=(x.company_row).id::text
    OR q.recipient_email=lower(btrim((x.prospect_row).email)))) AS direct_quarantine_matches,
 -- Alias validity needs every immutable quarantine identity. This exports ONLY
 -- company alias inputs/hashes, never other quarantined recipients or messages.
 (SELECT jsonb_agg(jsonb_build_object('id',q.id,'decisionHash',q.decision_hash,'companyId',q.company_id,
   'companyNameKey',q.company_name_key,'companyDomain',q.company_domain,'currentName',c.name,
   'currentDomain',to_jsonb(c)->'domain','currentWebsite',to_jsonb(c)->'website'))
  FROM (SELECT * FROM public.anchor_email_quarantines WHERE tenant_id='10' ORDER BY id LIMIT 200) q
  LEFT JOIN public.companies c ON c.id::text=q.company_id AND c.client_id=10) AS quarantine_alias_inputs,
 (SELECT count(*) FROM public.anchor_email_quarantines WHERE tenant_id='10') AS total_quarantines,
 count(*) OVER() AS total_matching_rows_before_limit
FROM crm x ORDER BY x.mission_id,x.ord,(x.prospect_row).id LIMIT 200;

-- Q8 OPTIONAL: current-program scoped grant and consumed operation bounds (Q0 first)
WITH input AS (
  SELECT '__REQUIRED_PROGRAM_ID__'::text AS program_id
), p AS (
  SELECT p.* FROM public.acquisition_outbound_programs p CROSS JOIN input x
  WHERE p.tenant_id='10' AND p.mode<>'revoked' AND p.id=x.program_id
    AND x.program_id ~ '^outbound_[a-f0-9]{24}$'
    AND (SELECT count(*) FROM public.acquisition_outbound_programs z
         WHERE z.tenant_id='10' AND z.mode<>'revoked')=1
)
SELECT p.id,g.manifest_id,g.manifest_hash,g.installed_at,g.revoked_at,
 g.manifest->>'validFrom' AS valid_from,g.manifest->>'expiresAt' AS expires_at,g.manifest->'scopes' AS scopes,
 CASE WHEN jsonb_typeof(g.manifest->'admissions')='array' THEN jsonb_array_length(g.manifest->'admissions') END AS admitted_identity_count,
 (SELECT count(*) FROM public.anchor_scoped_resume_consumption c WHERE c.tenant_id='10' AND c.program_id=p.id) AS consumed_operations,
 (SELECT sum(c.item_count) FROM public.anchor_scoped_resume_consumption c WHERE c.tenant_id='10' AND c.program_id=p.id) AS consumed_items,
 (SELECT count(*) FROM public.anchor_scoped_resume_consumption c WHERE c.tenant_id='10' AND c.program_id=p.id
   AND c.local_day=(now() AT TIME ZONE 'America/New_York')::date) AS consumed_operations_today,
 count(g.manifest_id) OVER() AS total_grants_before_limit
FROM p LEFT JOIN public.anchor_scoped_resume_grants g ON g.tenant_id='10' AND g.program_id=p.id
ORDER BY g.installed_at DESC LIMIT 200;
