'use strict';
const {isValidSolanaAddress:address}=require('../../packages/signal-v1/acquisition/solanaAddress');
function config(env=process.env) {
  if(env.SIGNAL_DASHBOARD_ENABLED!=='1')return {enabled:false};
  const tenantId=env.SIGNAL_DASHBOARD_TENANT_ID,ownerUserId=env.SIGNAL_DASHBOARD_OWNER_USER_ID,wallet=env.SIGNAL_DASHBOARD_PUBLIC_WALLET;
  if(!/^\d+$/.test(tenantId||'')||!/^\d+$/.test(ownerUserId||'')||!address(wallet))throw new Error('dashboard_identity_configuration_required');
  // Public addresses only. No wallet secret/signing options are supported.
  return {enabled:true,tenantId,ownerUserId,wallet};
}
module.exports={config};
