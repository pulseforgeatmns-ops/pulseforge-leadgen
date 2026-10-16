'use strict';
// This opt-in is deliberately specific to one already-provisioned, expiring demo.
const PINS = Object.freeze({
  PROJECT_ID:'c0623962-d804-4e6a-8c87-497e4b1749b2',
  ENVIRONMENT_ID:'979b11cf-e8fc-4b8c-88e5-77b3eb8dec31',
  SERVICE_ID:'80724c43-420c-4598-8c69-eecb9cc25745',
});
const ORIGIN='https://anchor-exeter-web-production.up.railway.app';
const EXPIRES_AT=Date.parse('2026-10-22T17:36:00.000Z');
const IDENTITIES=Object.freeze({client:{id:1,email:'client@exeter-fixture.invalid',role:'facility_client'},operator:{id:2,email:'operator@exeter-fixture.invalid',role:'manager'}});
function publicConfig(env) {
  const enabled=env.PORTAL_PUBLIC_DEMO === 'enabled';
  if(env.PORTAL_PUBLIC_DEMO !== undefined && !['disabled','enabled'].includes(env.PORTAL_PUBLIC_DEMO))throw new Error('Invalid public demo opt-in');
  if(!enabled){if(Object.keys(PINS).some(k=>env['PORTAL_PUBLIC_DEMO_'+k]!==undefined))throw new Error('Unexpected public demo pins');return false;}
  if(env.PORTAL_MODE!=='hosted'||env.PORTAL_ORIGIN!==ORIGIN||Date.now()>=EXPIRES_AT||Object.entries(PINS).some(([k,v])=>env['PORTAL_PUBLIC_DEMO_'+k]!==v))throw new Error('Public demo scope mismatch or expiry');
  return true;
}
async function publicState(pool) {
  const {rows:markers}=await pool.query('SELECT marker,expires_at FROM portal_demo_marker');
  if(markers.length!==1||markers[0].marker!=='anchor-portal-synthetic-only-v1'||new Date(markers[0].expires_at).getTime()!==EXPIRES_AT||Date.now()>=EXPIRES_AT)throw new Error('Public demo marker invalid');
  const {rows}=await pool.query('SELECT id,email,role,client_id,active,valid_until FROM users ORDER BY id');
  if(rows.length!==2)throw new Error('Public demo identities invalid');
  for(const expected of Object.values(IDENTITIES)){
    const user=rows.find(row=>row.id===expected.id);
    if(!user||user.email!==expected.email||user.role!==expected.role||user.client_id!==10||user.active!==true||new Date(user.valid_until).getTime()!==EXPIRES_AT)throw new Error('Public demo identity invalid');
  }
  const {rows:locations}=await pool.query('SELECT id,slug FROM anchor_portal_locations');
  if(locations.length!==1||locations[0].slug!=='synthetic-exeter-demo')throw new Error('Public demo location invalid');
  const {rows:invalid}=await pool.query('SELECT 1 FROM anchor_portal_locations WHERE is_demo IS NOT TRUE OR operator_client_id IS DISTINCT FROM 10 LIMIT 1');
  if(invalid.length)throw new Error('Public demo data invalid');
}
module.exports={PINS,ORIGIN,EXPIRES_AT,IDENTITIES,publicConfig,publicState};
