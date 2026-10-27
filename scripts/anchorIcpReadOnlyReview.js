'use strict';

// Operator inspection only. Never loads application bootstrap/schema helpers.
const fs = require('node:fs');
const { createHash } = require('node:crypto');
const { Client } = require('pg');

const SOURCES = ['touchpoints','agent_log','email_events','ao_prospect_activity','ao_prospect_updates',
  'ao_advisory_debriefs','ao_follow_up_tasks','ao_route_stops','ao_max_conversation_reports','cal_queue',
  'tenant_outreach_messages','tenant_outreach_events','tenant_outreach_scheduled_sends',
  'acquisition_mission_outbound_executions','acquisition_outbound_items'];
const AUXILIARY = ['max_spreadsheet_proposals','max_spreadsheet_contacts','max_spreadsheet_suppressions',
  'max_spreadsheet_relationships','anchor_email_recovery_control','anchor_email_quarantines'];
const ROSTER = ['Farley White Management Company','The MEG Companies','Brady Sullivan Properties',
  'Integrated Realty Resources','Elm Grove Companies',"St. Paul's School",'Tilton School',
  'Bishop Guertin High School','Southern New Hampshire University','The Derryfield School',
  'CPManagement','EJG Property Management LLC','NH CORE Properties','Allegro MicroSystems',
  'Summit Packaging Systems','Proterial Cable America','Marmon Aerospace & Defense','Sanmina Corporation','GE Aerospace'];

async function inspect(client, proposal) {
  const report = { specId:'SPEC-ANCHOR-ICP-001', status:'REVIEW_REQUIRED', readOnlyVerified:false,
    tenantId:10, ownershipChanges:0, productionWrites:0, outboundTriggered:0,
    limitations:[], sourceCoverage:[], tony:[], candidates:[], startedAt:new Date().toISOString() };
  await client.query('BEGIN TRANSACTION READ ONLY');
  try {
    const mode = (await client.query('SHOW transaction_read_only')).rows[0]?.transaction_read_only;
    if (mode !== 'on') throw Object.assign(new Error('Read-only mode unavailable'),{code:'READ_ONLY_REQUIRED'});
    report.readOnlyVerified = true;
    await client.query("SET LOCAL statement_timeout='20s'");
    await client.query("SET LOCAL lock_timeout='3s'");
    const names = ['clients','users','companies','prospects','ao_leads','ao_contacts',...SOURCES,...AUXILIARY];
    const catalog = await client.query(`SELECT c.relname AS name,c.relkind,
      has_table_privilege(current_user,c.oid,'SELECT') AS can_select,
      row_security_active(c.oid) AS rls_active
      FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
      WHERE n.nspname='public' AND c.relname=ANY($1::text[])`,[names]);
    const tables = new Map(catalog.rows.map(r=>[r.name,r]));
    for (const name of names) {
      const c=tables.get(name);
      report.sourceCoverage.push({name,installed:!!c,canSelect:c?.can_select===true,
        rlsActive:c?.rls_active ?? null,baseTable:!!c && ['r','p'].includes(c.relkind)});
      if (!c || !c.can_select || c.rls_active || !['r','p'].includes(c.relkind)) {
        report.limitations.push(`UNVERIFIED_SOURCE_COVERAGE:${name}`);
      }
    }
    const usable=name=>{const c=tables.get(name);return !!c && c.can_select && ['r','p'].includes(c.relkind);};
    if (!['clients','users','companies','prospects'].every(usable)) {
      report.status='BLOCKED_REQUIRED_TABLE_ACCESS';
      return report;
    }
    report.tenants=(await client.query('SELECT id,name FROM public.clients ORDER BY id')).rows;
    report.tony=(await client.query(`SELECT id,name,email,client_id,role,active,
      to_jsonb(u)->>'ao_operational_status' AS operational_status FROM public.users u
      WHERE client_id=10 AND role='ao' AND name ~* '(^|[[:space:]])Tony([[:space:]]|$)' ORDER BY id`)).rows;
    const activeTony=report.tony.filter(r=>r.active===true && r.operational_status==='active');
    report.tonyVerified=report.tony.length===1 && activeTony.length===1;
    if (!report.tonyVerified) report.limitations.push('TONY_IDENTITY_UNRESOLVED');
    // These details were absent from the first successful reader. This revised
    // collector is an unexecuted follow-up; never backfill them into old evidence.
    report.schemaColumns = (await client.query(`SELECT table_name,column_name,data_type,udt_name,
      is_nullable,column_default FROM information_schema.columns
      WHERE table_schema='public' AND table_name=ANY($1::text[]) ORDER BY table_name,ordinal_position`,[names])).rows;
    report.constraints = (await client.query(`SELECT c.relname AS table_name,k.conname,k.contype,
      pg_get_constraintdef(k.oid) AS definition FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
      JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public'
      AND c.relname=ANY($1::text[]) ORDER BY c.relname,k.conname`,[names])).rows;
    report.indexes = (await client.query(`SELECT tablename,indexname,indexdef FROM pg_indexes
      WHERE schemaname='public' AND tablename=ANY($1::text[]) ORDER BY tablename,indexname`,[names])).rows;
    report.anchorReviewState = (await client.query(`SELECT
      to_jsonb(p) ? 'ao_outreach_review_required' AS column_present,
      to_jsonb(p)->'ao_outreach_review_required' AS review_state,count(*)::integer AS records
      FROM public.prospects p WHERE client_id=10 GROUP BY 1,2 ORDER BY 1,2`)).rows;
    report.aoOwners = (await client.query(`SELECT id,name,client_id,role,active,
      to_jsonb(u)->>'ao_operational_status' AS operational_status FROM public.users u
      WHERE client_id=10 AND id=ANY($1::integer[]) ORDER BY id`,[[24,25,26]])).rows;
    report.anchorScope = (await client.query(`SELECT id,name,to_jsonb(c)->'service_area' AS service_area,
      to_jsonb(c)->'verticals' AS verticals,to_jsonb(c)->'target_verticals' AS target_verticals,
      to_jsonb(c)->'vertical_tiers' AS vertical_tiers FROM public.clients c WHERE id=10`)).rows;
    report.recoveryControl = usable('anchor_email_recovery_control') ? (await client.query(`SELECT
      singleton,maintenance_armed,manifest->>'id' AS manifest_id,manifest->>'version' AS manifest_version,
      manifest->>'tenantId' AS manifest_tenant,manifest->>'validFrom' AS valid_from,
      manifest->>'expiresAt' AS expires_at,manifest->'scopes' AS scopes
      FROM public.anchor_email_recovery_control`)).rows : null;
    report.quarantineSummary = usable('anchor_email_quarantines') ? (await client.query(`SELECT
      to_jsonb(q)->>'tenant_id' AS tenant_id,to_jsonb(q)->>'expected_status' AS expected_status,
      count(*)::integer AS records FROM public.anchor_email_quarantines q GROUP BY 1,2 ORDER BY 1,2`)).rows : null;
    report.schemaBaselineSha256 = createHash('sha256').update(JSON.stringify({columns:report.schemaColumns,
      constraints:report.constraints,indexes:report.indexes,scope:report.anchorScope})).digest('hex');

    for (const item of proposal.records) {
      const aliases=[item.identity.name,...(item.entity_resolution?.resolution_group_for_checking || [])]
        .map(s=>String(s).toLowerCase().replace(/[^a-z0-9]/g,''));
      const keys=[...new Set(aliases.filter(s=>s.length>=5))];
      const companies=(await client.query(`SELECT id::text,client_id,name,
        to_jsonb(c)->>'location' AS location,to_jsonb(c)->>'domain' AS domain,to_jsonb(c)->>'website' AS website
        FROM public.companies c WHERE EXISTS(SELECT 1 FROM unnest($1::text[]) k
          WHERE regexp_replace(lower(c.name),'[^a-z0-9]','','g')=k
            OR (length(k)>=8 AND strpos(regexp_replace(lower(c.name),'[^a-z0-9]','','g'),k)>0))
        ORDER BY client_id,id LIMIT 501`,[keys])).rows;
      const companyIds=companies.map(r=>r.id);
      const prospects=(await client.query(`SELECT id::text,client_id,company_id::text,email,
        to_jsonb(p)->>'assigned_ao_id' AS assigned_ao_id,
        to_jsonb(p)->>'last_contacted_at' AS last_contacted_at,to_jsonb(p)->>'phone' AS phone,
        to_jsonb(p)->'ao_outreach_review_required' AS outreach_review_required,
        to_jsonb(p)->'do_not_contact' AS do_not_contact,
        to_jsonb(p)->>'ao_last_touch_at' AS ao_last_touch_at
        FROM public.prospects p WHERE company_id::text=ANY($1::text[])
          OR EXISTS(SELECT 1 FROM unnest($2::text[]) k
            WHERE length(k)>=8 AND strpos(regexp_replace(lower(COALESCE(to_jsonb(p)->>'notes','')),'[^a-z0-9]','','g'),k)>0)
        ORDER BY client_id,id LIMIT 501`,[companyIds,keys])).rows;
      const leads=usable('ao_leads')?(await client.query(`SELECT id::text,
        to_jsonb(l)->>'client_id' AS client_id,to_jsonb(l)->>'crm_prospect_id' AS crm_prospect_id,
        to_jsonb(l)->>'ao_owner_id' AS ao_owner_id,business_name,
        to_jsonb(l)->>'first_contact_date' AS first_contact_date,
        to_jsonb(l)->>'last_contact_date' AS last_contact_date
        FROM public.ao_leads l WHERE EXISTS(SELECT 1 FROM unnest($1::text[]) k
          WHERE length(k)>=8 AND strpos(regexp_replace(lower(business_name),'[^a-z0-9]','','g'),k)>0)
        ORDER BY id LIMIT 501`,[keys])).rows:[];
      const prospectIds=[...new Set([...prospects.map(r=>r.id),...leads.map(r=>r.crm_prospect_id).filter(Boolean)])];
      const leadIds=leads.map(r=>r.id);
      const fieldContacts=usable('ao_contacts') && leadIds.length?(await client.query(`SELECT id::text,
        to_jsonb(a)->>'lead_id' AS lead_id,to_jsonb(a)->>'email' AS email,
        COALESCE(to_jsonb(a)->>'phone',to_jsonb(a)->>'contact_phone') AS phone,
        to_jsonb(a)->>'contact_name' AS contact_name FROM public.ao_contacts a
        WHERE to_jsonb(a)->>'lead_id'=ANY($1::text[]) LIMIT 501`,[leadIds])).rows:[];
      const emails=[...new Set([...prospects,...fieldContacts].map(r=>r.email?.trim().toLowerCase()).filter(Boolean))];
      const phones=[...new Set([...prospects,...fieldContacts].map(r=>String(r.phone||'').replace(/\D/g,'').slice(-10)).filter(p=>p.length===10))];
      const evidence=[];
      const candidateLimitations=['NAME_MATCHES_REQUIRE_ENTITY_REVIEW'];
      for (const name of SOURCES) {
        if (!usable(name)) continue;
        // Static table allowlist; JSON access tolerates schema differences while
        // source coverage stays explicitly unverified until the mapping is reviewed.
        const rows=(await client.query(`SELECT jsonb_strip_nulls(jsonb_build_object(
          'id',j->>'id','client_id',j->>'client_id','tenant_id',j->>'tenant_id',
          'prospect_id',j->>'prospect_id','company_id',j->>'company_id','lead_id',j->>'lead_id',
          'activity_type',j->>'activity_type','action_type',j->>'action_type','action',j->>'action',
          'channel',j->>'channel','status',j->>'status','outcome',j->>'outcome',
          'event',j->>'event','event_type',j->>'event_type','type',j->>'type','direction',j->>'direction',
          'recipient_email',j->>'recipient_email','email',j->>'email','to_email',j->>'to_email',
          'message_id',j->>'message_id','external_ref',j->>'external_ref',
          'created_at',j->>'created_at','ran_at',j->>'ran_at','sent_at',j->>'sent_at',
          'provider_message_id',j->>'provider_message_id')) AS evidence
          FROM (SELECT to_jsonb(t) j FROM public."${name}" t) source
          WHERE j->>'prospect_id'=ANY($1::text[]) OR j->>'crm_prospect_id'=ANY($1::text[])
            OR j->>'company_id'=ANY($2::text[]) OR j->>'lead_id'=ANY($3::text[])
            OR lower(j->>'recipient_email')=ANY($4::text[]) OR lower(j->>'email')=ANY($4::text[])
            OR j->'payload'->>'prospect_id'=ANY($1::text[])
            OR lower(j->'payload'->>'email')=ANY($4::text[])
            OR lower(j->>'to_email')=ANY($4::text[])
            OR right(regexp_replace(COALESCE(j->>'phone',j->>'recipient_phone',j->'payload'->>'phone',''),'[^0-9]','','g'),10)=ANY($5::text[])
          LIMIT 501`,[prospectIds,companyIds,leadIds,emails,phones])).rows;
        if(rows.length>=501) candidateLimitations.push(`TRUNCATED_SOURCE:${name}`);
        evidence.push({source:name,records:rows.map(r=>r.evidence)});
      }
      if ([companies,prospects,leads].some(r=>r.length>=501)) candidateLimitations.push('TRUNCATED_IDENTITY_MATCHES');
      // AO contacts can be unlinked from canonical CRM rows. Include their IDs and
      // addresses as evidence rather than assuming no contact from CRM-only reads.
      if(fieldContacts.length) candidateLimitations.push('FIELD_CONTACT_CHANNEL_HISTORY_REQUIRES_SEPARATE_ALIAS_REVIEW');
      if(fieldContacts.length>=501) candidateLimitations.push('TRUNCATED_FIELD_CONTACTS');
      report.candidates.push({reviewKey:item.review_key,name:item.identity.name,
        coldStatus:item.eligibility.cold_cohort_status==='EXCLUDED_PRIOR_OUTREACH'?'EXCLUDED_SNHU':'REVIEW_REQUIRED',
        companies,prospects,fieldLeads:leads,fieldContacts,evidence,limitations:candidateLimitations,
        eligibleForImport:false,executableWrites:[]});
    }
    report.limitations.push('LIVE_EVENT_MAPPING_AND_CHANNEL_COMPLETENESS_REQUIRE_REVIEW');
    return report;
  } finally { await client.query('ROLLBACK'); }
}

async function main() {
  const [proposalPath,resultPath]=process.argv.slice(2);
  if (!proposalPath || !resultPath) throw Object.assign(new Error('Two file paths are required'),{code:'PATHS_REQUIRED'});
  const proposal=JSON.parse(fs.readFileSync(proposalPath,'utf8'));
  if(proposal.spec_id!=='SPEC-ANCHOR-ICP-001' || proposal.records?.length!==19
    || proposal.records.some((r,i)=>r.identity?.name!==ROSTER[i])) {
    throw Object.assign(new Error('Fixed roster required'),{code:'ROSTER_REQUIRED'});
  }
  const connectionString=process.env.DATABASE_URL;
  if(!connectionString) throw Object.assign(new Error('Database connection unavailable'),{code:'DATABASE_CONNECTION_REQUIRED'});
  const client=new Client({connectionString,connectionTimeoutMillis:10000,
    application_name:'anchor-icp-read-only-review',options:'-c default_transaction_read_only=on',
    ssl:process.env.DATABASE_SSL==='false'?false:{rejectUnauthorized:true}});
  try {
    await client.connect();
    const report=await inspect(client,proposal);
    fs.writeFileSync(resultPath,JSON.stringify(report,null,2)+'\n',{mode:0o600});
    process.stdout.write(JSON.stringify({resultPath,status:report.status,readOnlyVerified:report.readOnlyVerified,
      candidates:report.candidates.length,productionWrites:0})+'\n');
  } finally { await client.end(); }
}
if(require.main===module) main().catch(error=>{
  process.stderr.write(JSON.stringify({status:'BLOCKED',code:error.code || 'INSPECTION_FAILED',productionWrites:0})+'\n');
  process.exitCode=1;
});
module.exports={inspect};
