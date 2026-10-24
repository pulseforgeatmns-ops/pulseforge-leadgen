'use strict';
const {requestScope}=require('./signalDistribution');
// Separate explicit mutation route. The live-view GET handlers remain read-only.
function createDistributionReferenceRouter({Router,requireAuth,requireRole,getStore,enabled=()=>false,appUrl=()=>process.env.APP_URL}) {
  const router=Router();
  router.post('/api/v1/signal/distribution/:monitorId/reference',requireAuth,requireRole('admin','manager'),async(req,res)=>{
    res.set('Cache-Control','no-store');
    if(!enabled())return res.status(503).json({error:'distribution_disabled'});
    let scope;
    try {
      scope=requestScope(req);
      if(scope.namespace!=='operational')throw Error('test_reference_not_supported');
      // Require JSON and browser's exact trusted origin. Host/proxy headers cannot
      // choose the origin, and missing Origin never bypasses the CSRF guard.
      const expected=new URL(appUrl());
      if(!['https:','http:'].includes(expected.protocol)||req.get('Origin')!==expected.origin||!req.is('application/json'))throw Error('origin_denied');
    }catch{return res.status(403).json({error:'reference_scope_or_origin_denied'});}
    const input=req.body||{};
    if(!/^[a-zA-Z0-9:_-]{1,160}$/.test(req.params.monitorId)||input.confirm!==true||input.source!=='EXPLICIT_MANUAL_OPERATOR'
      ||typeof input.priceUsd!=='number'||!Number.isFinite(input.priceUsd)||input.priceUsd<=0
      ||typeof input.recordedAt!=='string'||!Number.isFinite(Date.parse(input.recordedAt))
      ||Object.keys(input).some(k=>!['confirm','source','priceUsd','recordedAt'].includes(k)))return res.status(400).json({error:'invalid_explicit_reference'});
    try {
      const store=await getStore();
      const monitor=await store.getMonitor({...scope,id:req.params.monitorId});
      if(!monitor)return res.status(404).json({error:'monitor_not_found'});
      await store.setManualReference({id:monitor.id,priceUsd:input.priceUsd,recordedAt:input.recordedAt,provenance:input.source});
      return res.status(201).json({saved:true,manualReferenceOnly:true,noTradeExecuted:true});
    }catch(err){
      const known=['invalid_manual_reference','reference_missing_monitor_or_already_set'];
      return res.status(known.includes(err.message)?409:503).json({error:known.includes(err.message)?err.message:'reference_unavailable'});
    }
  });
  return router;
}
module.exports={createDistributionReferenceRouter};
