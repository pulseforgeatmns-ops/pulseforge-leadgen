'use strict';
const {createHash}=require('node:crypto');
function activationConfig(env,tenantId,namespace='operational') {
  const start=env.SIGNAL_DISTRIBUTION_WINDOW_START,end=env.SIGNAL_DISTRIBUTION_WINDOW_END;
  const valid=s=>typeof s==='string'&&/^\d{4}-\d\d-\d\dT.*(?:Z|[+-]\d\d:\d\d)$/.test(s)&&Number.isFinite(Date.parse(s));
  const smsLimit=Number(env.SIGNAL_DISTRIBUTION_SMS_MAX_MESSAGES||5);
  if(!valid(start)||!valid(end)||Date.parse(end)<=Date.parse(start)||Date.parse(end)-Date.parse(start)>86400000||!Number.isInteger(smsLimit)||smsLimit<0||smsLimit>20)throw new Error('invalid_distribution_activation_window');
  const startsAt=new Date(start).toISOString(),endsAt=new Date(end).toISOString();
  const id=createHash('sha256').update(JSON.stringify([tenantId,namespace,startsAt,endsAt])).digest('hex');
  return {id,startsAt,endsAt,smsLimit};
}
module.exports={activationConfig};
