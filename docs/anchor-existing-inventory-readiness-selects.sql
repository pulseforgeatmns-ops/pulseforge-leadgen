-- Additional Q9/Q10: existing-program inventory observation, requested 2026-10-19.
-- SELECT-only; no production execution by author. This does not call loadCleanInventory.
-- Scope is frozen to the independently observed market/cities; program/source/policy
-- placeholders MUST be replaced with currently verified values. Scope drift => no rows.
-- Approved AO owners 24/25/26 are handoff authority, never an ownership exemption.
-- Existing legacy selector coalesces NULL/missing review to false. We preserve that
-- uncertainty and DO NOT report these rows as SPEC262 eligible. Review false, DNC
-- false and synthetic false must be literal booleans at the locked runtime boundary.
-- Domain SQL is a conservative observational approximation of ownershipDomain(URL).
-- Exact JS domain/email/provenance validation and duplicate selection remain required.
-- Counts include held/quality buckets; detail EXCLUDES held incident/contact/company
-- aliases, prior touches, AO ownership, explicit DNC, synthetic and explicit review holds.
-- No optional quarantine/reservation tables referenced: latest metadata says absent.
-- If those tables appear, STOP and incorporate their immutable identity/alias holds.
-- Export counts first. 200 quality buckets and 25 contacts are hard output ceilings;
-- do not mistake a truncated sample for full inventory or change bounds silently.
-- No old envelope copy is returned, because no current prepared batch was identified.

-- Q9: program-scoped counts and quality buckets
WITH program AS (
 SELECT p.* FROM public.acquisition_outbound_programs p
 WHERE p.tenant_id='10' AND p.mode<>'revoked'
   AND p.id='__REQUIRED_PROGRAM_ID__'
   AND p.policy_hash='__REQUIRED_POLICY_HASH__'
   AND p.source_mission_id='__REQUIRED_SOURCE_MISSION_ID__'
   AND (SELECT count(*) FROM public.acquisition_outbound_programs z WHERE z.tenant_id='10' AND z.mode<>'revoked')=1
), source AS (
 SELECT m.* FROM program p JOIN public.acquisition_missions m ON m.tenant_id='10' AND m.id=p.source_mission_id
 WHERE m.payload->'structuredMission'->'market'->>'segment'='commercial_cleaning_buyers'
   AND m.payload->'structuredMission'->'market'->'eligibleSubsegments'='["property_manager","str_manager","realtor","commercial_office"]'::jsonb
   AND m.payload->'structuredMission'->'geography'->'cities'='["Manchester","Hooksett","Bedford","Auburn","Goffstown","Londonderry"]'::jsonb
), scoped AS (
 SELECT r.id::text AS prospect_id,r.company_id::text AS company_id,r.email,c.name AS company_name,
   to_jsonb(r) AS r,to_jsonb(c) AS c,
   regexp_replace(regexp_replace(regexp_replace(lower(btrim(coalesce(nullif(to_jsonb(r)->>'vertical',''),c.industry,''))),
     '[[:space:].-]+','_','g'),'[^a-z0-9_]','','g'),'_+','_','g') AS vertical_key,
   (SELECT string_agg(w,'') FROM regexp_split_to_table(regexp_replace(btrim(regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+',' ','g')),' (commercial|residential) (division|department)$',''),' ') w WHERE w<>'' AND w NOT IN ('llc','inc','incorporated','ltd','limited','corp','corporation','co','company','properties','property','management')) AS company_name_key,
   array_remove(ARRAY[nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'domain',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'website',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(lower(split_part(btrim(coalesce(r.email,'')),'@',2)),'')],NULL) AS ownership_domains
 FROM public.prospects r JOIN public.companies c ON c.id=r.company_id AND c.client_id=r.client_id
 CROSS JOIN source s WHERE r.client_id=10 AND r.email IS NOT NULL
   AND btrim(regexp_replace(regexp_replace(regexp_replace(lower(btrim(coalesce(nullif(to_jsonb(r)->>'vertical',''),c.industry,''))),
     '[[:space:].-]+','_','g'),'[^a-z0-9_]','','g'),'_+','_','g'),'_')
      IN ('property_manager','property_management','str_manager','realtor','commercial_office')
   AND (to_jsonb(r)->'service_area_match'='true'::jsonb OR (
     to_jsonb(r)->'service_area_match' IS DISTINCT FROM 'false'::jsonb
     AND EXISTS(SELECT 1 FROM unnest(ARRAY['manchester','hooksett','bedford','auburn','goffstown','londonderry']) city
       WHERE (' '||btrim(regexp_replace(lower(concat_ws(' ',to_jsonb(r)->>'service_area_match',c.location,to_jsonb(r)->>'location')),
         '[^a-z0-9]+',' ','g'))||' ') LIKE '% '||city||' %')))
), quality AS (
 SELECT s.*,
   CASE WHEN NOT(r ? 'ao_outreach_review_required') THEN 'MISSING_COLUMN'
     WHEN r->'ao_outreach_review_required'='null'::jsonb THEN 'NULL_UNKNOWN'
     WHEN r->'ao_outreach_review_required'='false'::jsonb THEN 'EXPLICIT_FALSE'
     WHEN r->'ao_outreach_review_required'='true'::jsonb THEN 'EXPLICIT_TRUE_HOLD'
     ELSE 'INCOMPATIBLE_UNKNOWN' END AS review_state,
   coalesce(r->'acquisition_metadata'->'contactResolution'->>'finalState',
     r->'acquisition_metadata'->'contactResolution'->>'classification',r->>'contact_classification') AS contact_classification,
   coalesce((SELECT v FROM unnest(ARRAY[lower(btrim(r->>'email_provenance_source')),
       lower(btrim(r->'enrichment_provenance'->'email'->>'original_source')),
       lower(btrim(r->'enrichment_provenance'->'email'->>'source'))]) WITH ORDINALITY x(v,n)
       WHERE nullif(v,'') IS NOT NULL AND v NOT IN ('existing_crm','existing_prospect_email','existing_bouncer_verified_email') ORDER BY n LIMIT 1),
     nullif(lower(btrim(r->>'email_provenance_source')),''),nullif(lower(btrim(r->'enrichment_provenance'->'email'->>'source')),''),
     nullif(lower(btrim(r->>'verificationSource')),'')) AS resolved_email_provenance,
   (r->'email_verified'='true'::jsonb AND lower(r->>'email_status') IN ('valid','verified')) IS TRUE AS verified_status_pair,
   (r->'assigned_ao_id' IS NOT NULL AND r->'assigned_ao_id'<>'null'::jsonb)
     OR (r->'closer_id' IS NOT NULL AND r->'closer_id'<>'null'::jsonb)
     OR nullif(r->>'last_contacted_at','') IS NOT NULL OR nullif(r->>'last_reply_at','') IS NOT NULL
     OR EXISTS(SELECT 1 FROM public.ao_prospect_tasks t WHERE t.client_id=10 AND t.prospect_id::text=s.prospect_id)
     OR EXISTS(SELECT 1 FROM public.touchpoints t WHERE t.client_id=10 AND t.prospect_id::text=s.prospect_id
       AND t.action_type IN ('email_sent','sent','outbound_email','call','call_attempt','inbound_reply','reply','email_reply','reply_received')) AS own_ownership_or_touch_hold,
   EXISTS(SELECT 1 FROM public.acquisition_outbound_items i WHERE i.tenant_id='10'
     AND (i.prospect_id=s.prospect_id OR i.company_id=s.company_id OR lower(btrim(i.email))=lower(btrim(s.email)))
     AND (i.attempted_at IS NOT NULL OR i.provider_message_id IS NOT NULL OR i.status IN ('attempted','uncertain','sent'))) AS direct_governed_attempt_hold,
   EXISTS(SELECT 1 FROM public.acquisition_mission_outbound_executions x WHERE x.tenant_id='10'
     AND x.status IN ('sent','attempted','failed')
     AND (x.prospect_id=s.prospect_id OR lower(btrim(x.payload->>'email'))=lower(btrim(s.email)))) AS canonical_attempt_hold,
   EXISTS(SELECT 1 FROM public.acquisition_outbound_lifecycle l WHERE l.tenant_id='10' AND l.suppressed
     AND (l.company_id=s.company_id OR lower(btrim(l.email))=lower(btrim(s.email)))) AS lifecycle_hold,
   lower(coalesce(r->>'operational_status','')) IN ('booked','converted','client','do_not_email','bounced','replied')
     OR lower(coalesce(r->>'setter_status','')) IN ('booked','appointment_set','won')
     OR nullif(r->>'closer_status','') IS NOT NULL
     OR EXISTS(SELECT 1 FROM public.touchpoints t WHERE t.client_id=10 AND t.prospect_id::text=s.prospect_id
       AND t.action_type IN ('inbound_reply','reply','email_reply','reply_received','unsubscribed','out_of_office')) AS reply_or_pipeline_hold
 FROM scoped s
), owner_rows AS (
 SELECT r.id::text AS prospect_id,r.company_id::text AS company_id,
   (SELECT string_agg(w,'') FROM regexp_split_to_table(regexp_replace(btrim(regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+',' ','g')),' (commercial|residential) (division|department)$',''),' ') w WHERE w<>'' AND w NOT IN ('llc','inc','incorporated','ltd','limited','corp','corporation','co','company','properties','property','management')) AS company_name_key,array_remove(ARRAY[nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'domain',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'website',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(lower(split_part(btrim(coalesce(r.email,'')),'@',2)),'')],NULL) AS ownership_domains
 FROM public.prospects r JOIN public.companies c ON c.id=r.company_id AND c.client_id=10
 WHERE r.client_id=10 AND (to_jsonb(r)->>'assigned_ao_id' IS NOT NULL OR to_jsonb(r)->>'closer_id' IS NOT NULL
   OR to_jsonb(r)->>'last_contacted_at' IS NOT NULL OR to_jsonb(r)->>'last_reply_at' IS NOT NULL
   OR to_jsonb(r)->'do_not_contact'='true'::jsonb
   OR EXISTS(SELECT 1 FROM public.ao_prospect_tasks t WHERE t.client_id=10 AND t.prospect_id::text=r.id::text)
   OR EXISTS(SELECT 1 FROM public.touchpoints t WHERE t.client_id=10 AND t.prospect_id::text=r.id::text
     AND t.action_type IN ('email_sent','sent','outbound_email','call','call_attempt','inbound_reply','reply','email_reply','reply_received')))
), held_companies AS (
 SELECT DISTINCT c.id::text AS company_id,(SELECT string_agg(w,'') FROM regexp_split_to_table(regexp_replace(btrim(regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+',' ','g')),' (commercial|residential) (division|department)$',''),' ') w WHERE w<>'' AND w NOT IN ('llc','inc','incorporated','ltd','limited','corp','corporation','co','company','properties','property','management')) AS company_name_key,
   array_remove(ARRAY[nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'domain',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'website',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),'')],NULL) AS domains
 FROM public.acquisition_outbound_items i JOIN public.companies c ON c.id::text=i.company_id AND c.client_id=10
 WHERE i.tenant_id='10' AND (i.attempted_at IS NOT NULL OR i.provider_message_id IS NOT NULL OR i.status IN ('attempted','uncertain','sent'))
), ao_rows AS (
 SELECT (SELECT string_agg(w,'') FROM regexp_split_to_table(regexp_replace(btrim(regexp_replace(lower(coalesce(l.business_name,'')),'[^a-z0-9]+',' ','g')),' (commercial|residential) (division|department)$',''),' ') w WHERE w<>'' AND w NOT IN ('llc','inc','incorporated','ltd','limited','corp','corporation','co','company','properties','property','management')) AS business_name_key,(SELECT string_agg(w,'') FROM regexp_split_to_table(regexp_replace(btrim(regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+',' ','g')),' (commercial|residential) (division|department)$',''),' ') w WHERE w<>'' AND w NOT IN ('llc','inc','incorporated','ltd','limited','corp','corporation','co','company','properties','property','management')) AS company_name_key,
   array_remove(ARRAY[nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'domain',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'website',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(lower(split_part(btrim(coalesce(r.email,'')),'@',2)),'')],NULL) ||
     coalesce((SELECT array_agg(nullif(lower(split_part(btrim(a.email),'@',2)),'')) FROM public.ao_contacts a WHERE a.lead_id=l.id),'{}'::text[]) AS domains
 FROM public.ao_leads l LEFT JOIN public.prospects r ON r.id=l.crm_prospect_id AND r.client_id=10
 LEFT JOIN public.companies c ON c.id=r.company_id AND c.client_id=10 WHERE l.client_id=10
), inspected AS (
 SELECT q.*,
  EXISTS(SELECT 1 FROM owner_rows o WHERE o.prospect_id=q.prospect_id OR o.company_id=q.company_id
    OR (q.company_name_key IS NOT NULL AND o.company_name_key=q.company_name_key)
    OR o.ownership_domains && q.ownership_domains) AS related_ownership_hold,
  EXISTS(SELECT 1 FROM ao_rows a WHERE q.company_name_key IN (a.business_name_key,a.company_name_key)
    OR a.domains && q.ownership_domains) AS ao_alias_hold,
  EXISTS(SELECT 1 FROM held_companies h WHERE h.company_id=q.company_id
    OR (q.company_name_key IS NOT NULL AND h.company_name_key=q.company_name_key)
    OR h.domains && q.ownership_domains) AS attempted_company_alias_hold
 FROM quality q
)
SELECT now() AS observed_at,vertical_key,review_state,
 r->'do_not_contact' AS do_not_contact,r->'is_synthetic' AS is_synthetic,
 verified_status_pair,resolved_email_provenance,
 own_ownership_or_touch_hold,related_ownership_hold,ao_alias_hold,
 direct_governed_attempt_hold,attempted_company_alias_hold,canonical_attempt_hold,lifecycle_hold,reply_or_pipeline_hold,
 count(*) AS candidate_count,sum(count(*)) OVER() AS total_scoped_contacts,count(*) OVER() AS total_quality_buckets,
 'OBSERVED QUALITY ONLY: no send-ready count; missing review schema blocks every scoped email admission' AS interpretation
FROM inspected GROUP BY vertical_key,review_state,r->'do_not_contact',r->'is_synthetic',verified_status_pair,resolved_email_provenance,
 own_ownership_or_touch_hold,related_ownership_hold,ao_alias_hold,direct_governed_attempt_hold,attempted_company_alias_hold,
 canonical_attempt_hold,lifecycle_hold,reply_or_pipeline_hold
ORDER BY vertical_key,review_state,candidate_count DESC LIMIT 200;

-- Q10: bounded nonheld review candidate details
WITH program AS (
 SELECT p.* FROM public.acquisition_outbound_programs p
 WHERE p.tenant_id='10' AND p.mode<>'revoked'
   AND p.id='__REQUIRED_PROGRAM_ID__'
   AND p.policy_hash='__REQUIRED_POLICY_HASH__'
   AND p.source_mission_id='__REQUIRED_SOURCE_MISSION_ID__'
   AND (SELECT count(*) FROM public.acquisition_outbound_programs z WHERE z.tenant_id='10' AND z.mode<>'revoked')=1
), source AS (
 SELECT m.* FROM program p JOIN public.acquisition_missions m ON m.tenant_id='10' AND m.id=p.source_mission_id
 WHERE m.payload->'structuredMission'->'market'->>'segment'='commercial_cleaning_buyers'
   AND m.payload->'structuredMission'->'market'->'eligibleSubsegments'='["property_manager","str_manager","realtor","commercial_office"]'::jsonb
   AND m.payload->'structuredMission'->'geography'->'cities'='["Manchester","Hooksett","Bedford","Auburn","Goffstown","Londonderry"]'::jsonb
), scoped AS (
 SELECT r.id::text AS prospect_id,r.company_id::text AS company_id,r.email,c.name AS company_name,
   to_jsonb(r) AS r,to_jsonb(c) AS c,
   regexp_replace(regexp_replace(regexp_replace(lower(btrim(coalesce(nullif(to_jsonb(r)->>'vertical',''),c.industry,''))),
     '[[:space:].-]+','_','g'),'[^a-z0-9_]','','g'),'_+','_','g') AS vertical_key,
   (SELECT string_agg(w,'') FROM regexp_split_to_table(regexp_replace(btrim(regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+',' ','g')),' (commercial|residential) (division|department)$',''),' ') w WHERE w<>'' AND w NOT IN ('llc','inc','incorporated','ltd','limited','corp','corporation','co','company','properties','property','management')) AS company_name_key,
   array_remove(ARRAY[nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'domain',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'website',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(lower(split_part(btrim(coalesce(r.email,'')),'@',2)),'')],NULL) AS ownership_domains
 FROM public.prospects r JOIN public.companies c ON c.id=r.company_id AND c.client_id=r.client_id
 CROSS JOIN source s WHERE r.client_id=10 AND r.email IS NOT NULL
   AND btrim(regexp_replace(regexp_replace(regexp_replace(lower(btrim(coalesce(nullif(to_jsonb(r)->>'vertical',''),c.industry,''))),
     '[[:space:].-]+','_','g'),'[^a-z0-9_]','','g'),'_+','_','g'),'_')
      IN ('property_manager','property_management','str_manager','realtor','commercial_office')
   AND (to_jsonb(r)->'service_area_match'='true'::jsonb OR (
     to_jsonb(r)->'service_area_match' IS DISTINCT FROM 'false'::jsonb
     AND EXISTS(SELECT 1 FROM unnest(ARRAY['manchester','hooksett','bedford','auburn','goffstown','londonderry']) city
       WHERE (' '||btrim(regexp_replace(lower(concat_ws(' ',to_jsonb(r)->>'service_area_match',c.location,to_jsonb(r)->>'location')),
         '[^a-z0-9]+',' ','g'))||' ') LIKE '% '||city||' %')))
), quality AS (
 SELECT s.*,
   CASE WHEN NOT(r ? 'ao_outreach_review_required') THEN 'MISSING_COLUMN'
     WHEN r->'ao_outreach_review_required'='null'::jsonb THEN 'NULL_UNKNOWN'
     WHEN r->'ao_outreach_review_required'='false'::jsonb THEN 'EXPLICIT_FALSE'
     WHEN r->'ao_outreach_review_required'='true'::jsonb THEN 'EXPLICIT_TRUE_HOLD'
     ELSE 'INCOMPATIBLE_UNKNOWN' END AS review_state,
   coalesce(r->'acquisition_metadata'->'contactResolution'->>'finalState',
     r->'acquisition_metadata'->'contactResolution'->>'classification',r->>'contact_classification') AS contact_classification,
   coalesce((SELECT v FROM unnest(ARRAY[lower(btrim(r->>'email_provenance_source')),
       lower(btrim(r->'enrichment_provenance'->'email'->>'original_source')),
       lower(btrim(r->'enrichment_provenance'->'email'->>'source'))]) WITH ORDINALITY x(v,n)
       WHERE nullif(v,'') IS NOT NULL AND v NOT IN ('existing_crm','existing_prospect_email','existing_bouncer_verified_email') ORDER BY n LIMIT 1),
     nullif(lower(btrim(r->>'email_provenance_source')),''),nullif(lower(btrim(r->'enrichment_provenance'->'email'->>'source')),''),
     nullif(lower(btrim(r->>'verificationSource')),'')) AS resolved_email_provenance,
   (r->'email_verified'='true'::jsonb AND lower(r->>'email_status') IN ('valid','verified')) IS TRUE AS verified_status_pair,
   (r->'assigned_ao_id' IS NOT NULL AND r->'assigned_ao_id'<>'null'::jsonb)
     OR (r->'closer_id' IS NOT NULL AND r->'closer_id'<>'null'::jsonb)
     OR nullif(r->>'last_contacted_at','') IS NOT NULL OR nullif(r->>'last_reply_at','') IS NOT NULL
     OR EXISTS(SELECT 1 FROM public.ao_prospect_tasks t WHERE t.client_id=10 AND t.prospect_id::text=s.prospect_id)
     OR EXISTS(SELECT 1 FROM public.touchpoints t WHERE t.client_id=10 AND t.prospect_id::text=s.prospect_id
       AND t.action_type IN ('email_sent','sent','outbound_email','call','call_attempt','inbound_reply','reply','email_reply','reply_received')) AS own_ownership_or_touch_hold,
   EXISTS(SELECT 1 FROM public.acquisition_outbound_items i WHERE i.tenant_id='10'
     AND (i.prospect_id=s.prospect_id OR i.company_id=s.company_id OR lower(btrim(i.email))=lower(btrim(s.email)))
     AND (i.attempted_at IS NOT NULL OR i.provider_message_id IS NOT NULL OR i.status IN ('attempted','uncertain','sent'))) AS direct_governed_attempt_hold,
   EXISTS(SELECT 1 FROM public.acquisition_mission_outbound_executions x WHERE x.tenant_id='10'
     AND x.status IN ('sent','attempted','failed')
     AND (x.prospect_id=s.prospect_id OR lower(btrim(x.payload->>'email'))=lower(btrim(s.email)))) AS canonical_attempt_hold,
   EXISTS(SELECT 1 FROM public.acquisition_outbound_lifecycle l WHERE l.tenant_id='10' AND l.suppressed
     AND (l.company_id=s.company_id OR lower(btrim(l.email))=lower(btrim(s.email)))) AS lifecycle_hold,
   lower(coalesce(r->>'operational_status','')) IN ('booked','converted','client','do_not_email','bounced','replied')
     OR lower(coalesce(r->>'setter_status','')) IN ('booked','appointment_set','won')
     OR nullif(r->>'closer_status','') IS NOT NULL
     OR EXISTS(SELECT 1 FROM public.touchpoints t WHERE t.client_id=10 AND t.prospect_id::text=s.prospect_id
       AND t.action_type IN ('inbound_reply','reply','email_reply','reply_received','unsubscribed','out_of_office')) AS reply_or_pipeline_hold
 FROM scoped s
), owner_rows AS (
 SELECT r.id::text AS prospect_id,r.company_id::text AS company_id,
   (SELECT string_agg(w,'') FROM regexp_split_to_table(regexp_replace(btrim(regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+',' ','g')),' (commercial|residential) (division|department)$',''),' ') w WHERE w<>'' AND w NOT IN ('llc','inc','incorporated','ltd','limited','corp','corporation','co','company','properties','property','management')) AS company_name_key,array_remove(ARRAY[nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'domain',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'website',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(lower(split_part(btrim(coalesce(r.email,'')),'@',2)),'')],NULL) AS ownership_domains
 FROM public.prospects r JOIN public.companies c ON c.id=r.company_id AND c.client_id=10
 WHERE r.client_id=10 AND (to_jsonb(r)->>'assigned_ao_id' IS NOT NULL OR to_jsonb(r)->>'closer_id' IS NOT NULL
   OR to_jsonb(r)->>'last_contacted_at' IS NOT NULL OR to_jsonb(r)->>'last_reply_at' IS NOT NULL
   OR to_jsonb(r)->'do_not_contact'='true'::jsonb
   OR EXISTS(SELECT 1 FROM public.ao_prospect_tasks t WHERE t.client_id=10 AND t.prospect_id::text=r.id::text)
   OR EXISTS(SELECT 1 FROM public.touchpoints t WHERE t.client_id=10 AND t.prospect_id::text=r.id::text
     AND t.action_type IN ('email_sent','sent','outbound_email','call','call_attempt','inbound_reply','reply','email_reply','reply_received')))
), held_companies AS (
 SELECT DISTINCT c.id::text AS company_id,(SELECT string_agg(w,'') FROM regexp_split_to_table(regexp_replace(btrim(regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+',' ','g')),' (commercial|residential) (division|department)$',''),' ') w WHERE w<>'' AND w NOT IN ('llc','inc','incorporated','ltd','limited','corp','corporation','co','company','properties','property','management')) AS company_name_key,
   array_remove(ARRAY[nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'domain',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'website',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),'')],NULL) AS domains
 FROM public.acquisition_outbound_items i JOIN public.companies c ON c.id::text=i.company_id AND c.client_id=10
 WHERE i.tenant_id='10' AND (i.attempted_at IS NOT NULL OR i.provider_message_id IS NOT NULL OR i.status IN ('attempted','uncertain','sent'))
), ao_rows AS (
 SELECT (SELECT string_agg(w,'') FROM regexp_split_to_table(regexp_replace(btrim(regexp_replace(lower(coalesce(l.business_name,'')),'[^a-z0-9]+',' ','g')),' (commercial|residential) (division|department)$',''),' ') w WHERE w<>'' AND w NOT IN ('llc','inc','incorporated','ltd','limited','corp','corporation','co','company','properties','property','management')) AS business_name_key,(SELECT string_agg(w,'') FROM regexp_split_to_table(regexp_replace(btrim(regexp_replace(lower(coalesce(c.name,'')),'[^a-z0-9]+',' ','g')),' (commercial|residential) (division|department)$',''),' ') w WHERE w<>'' AND w NOT IN ('llc','inc','incorporated','ltd','limited','corp','corporation','co','company','properties','property','management')) AS company_name_key,
   array_remove(ARRAY[nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'domain',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(regexp_replace(split_part(split_part(regexp_replace(lower(btrim(coalesce(to_jsonb(c)->>'website',''))),'^[a-z]+://',''),'/',1),':',1),'^www\.',''),''),nullif(lower(split_part(btrim(coalesce(r.email,'')),'@',2)),'')],NULL) ||
     coalesce((SELECT array_agg(nullif(lower(split_part(btrim(a.email),'@',2)),'')) FROM public.ao_contacts a WHERE a.lead_id=l.id),'{}'::text[]) AS domains
 FROM public.ao_leads l LEFT JOIN public.prospects r ON r.id=l.crm_prospect_id AND r.client_id=10
 LEFT JOIN public.companies c ON c.id=r.company_id AND c.client_id=10 WHERE l.client_id=10
), inspected AS (
 SELECT q.*,
  EXISTS(SELECT 1 FROM owner_rows o WHERE o.prospect_id=q.prospect_id OR o.company_id=q.company_id
    OR (q.company_name_key IS NOT NULL AND o.company_name_key=q.company_name_key)
    OR o.ownership_domains && q.ownership_domains) AS related_ownership_hold,
  EXISTS(SELECT 1 FROM ao_rows a WHERE q.company_name_key IN (a.business_name_key,a.company_name_key)
    OR a.domains && q.ownership_domains) AS ao_alias_hold,
  EXISTS(SELECT 1 FROM held_companies h WHERE h.company_id=q.company_id
    OR (q.company_name_key IS NOT NULL AND h.company_name_key=q.company_name_key)
    OR h.domains && q.ownership_domains) AS attempted_company_alias_hold
 FROM quality q
)
SELECT prospect_id,company_id,company_name,email AS recipient_email,vertical_key,
 c->>'domain' AS company_domain,c->>'website' AS company_website,
 r->>'first_name' AS first_name,r->>'last_name' AS last_name,
 r->'service_area_match' AS service_area_match,c->>'location' AS company_location,
 r->'do_not_contact' AS do_not_contact,r->'is_synthetic' AS is_synthetic,review_state,
 r->'email_verified' AS email_verified,r->>'email_status' AS email_status,
 r->>'email_verification_method' AS verification_method,resolved_email_provenance,contact_classification,
 r->'enrichment_provenance'->'email' AS email_provenance,
 r->'acquisition_metadata'->'contactResolution' AS contact_resolution,
 count(*) OVER() AS total_nonheld_scoped_candidates_before_limit,
 'REVIEW CANDIDATE ONLY: exact JS email/domain/provenance, duplicate and identity fingerprint checks still required; no permission to prepare/send' AS interpretation
FROM inspected WHERE NOT own_ownership_or_touch_hold AND NOT related_ownership_hold AND NOT ao_alias_hold
 AND NOT direct_governed_attempt_hold AND NOT attempted_company_alias_hold AND NOT canonical_attempt_hold
 AND NOT lifecycle_hold AND NOT reply_or_pipeline_hold
 AND r->'do_not_contact' IS DISTINCT FROM 'true'::jsonb
 AND r->'is_synthetic' IS DISTINCT FROM 'true'::jsonb
 AND review_state<>'EXPLICIT_TRUE_HOLD'
 AND verified_status_pair
 AND nullif(btrim(r->'enrichment_provenance'->'email'->>'source'),'') IS NOT NULL
 AND left(resolved_email_provenance,8)<>'pattern_'
 AND EXISTS(SELECT 1 FROM unnest(ARRAY['existing_crm','existing_prospect_email','existing_bouncer_verified_email',
   'website_email','hunter','prospeo','scraped','provider_chain','tier0_email_localpart']) prefix
   WHERE resolved_email_provenance=prefix OR resolved_email_provenance LIKE prefix||'+%'
     OR position('+'||prefix IN resolved_email_provenance)>0)
ORDER BY (contact_classification='VERIFIED_FOUNDER_EMAIL') DESC NULLS LAST,r->>'updated_at' DESC NULLS LAST,prospect_id
LIMIT 25;
