'use strict';
// Deliberately factory-only. Read routes do not initialize schema, research or providers.
function requestScope(req) {
  if (req.session?.impersonation?.active) throw new Error('impersonation_not_supported');
  const user = req.session?.user;
  if (!user || !['admin','manager'].includes(user.role)) throw new Error('forbidden');
  // Only trusted session identity/workspace. Query/body/header tenant overrides forbidden.
  const tenantId = user.client_id ?? req.session.active_client_id;
  if (!/^\d+$/.test(String(tenantId || ''))) throw new Error('workspace_required');
  const namespace = req.query?.namespace || 'operational';
  if (!['operational','test'].includes(namespace)) throw new Error('invalid_namespace');
  if (req.query?.tenantId !== undefined || req.query?.tenant_id !== undefined) throw new Error('tenant_override_forbidden');
  return {tenantId:String(tenantId),namespace};
}
function createDistributionRouter({Router, requireAuth, requireRole, getStore, getDeliveryStore, enabled = () => false}) {
  if (![Router,requireAuth,requireRole,getStore,getDeliveryStore].every(fn=>typeof fn === 'function')) throw new Error('route_dependencies_required');
  const router = Router();
  const guard = [requireAuth,requireRole('admin','manager')];
  const handle = fn => async (req,res) => {
    res.set('Cache-Control','no-store');
    if (!enabled()) return res.status(503).json({error:'distribution_disabled'});
    let scope; try {scope=requestScope(req);} catch (_) {return res.status(403).json({error:'distribution_scope_denied'});}
    try {return await fn(req,res,scope);} catch (_) {return res.status(503).json({error:'distribution_unavailable'});}
  };
  router.get('/api/v1/signal/distribution',...guard,handle(async (_req,res,scope)=>{
    const store = await getStore(); return res.json({namespace:scope.namespace,monitors:await store.listMonitors(scope),admissions:await store.listAdmissions(scope)});
  }));
  router.get('/api/v1/signal/distribution/:monitorId',...guard,handle(async (req,res,scope)=>{
    if (!/^[a-zA-Z0-9:_-]{1,160}$/.test(req.params.monitorId)) return res.status(400).json({error:'invalid_monitor'});
    const store = await getStore();
    const monitor = await store.getMonitor({...scope,id:req.params.monitorId});
    if (!monitor) return res.status(404).json({error:'monitor_not_found'});
    const deliveries = await getDeliveryStore();
    return res.json({namespace:scope.namespace,monitor,
      transitions:await store.getTransitions({...scope,monitorId:monitor.id}),
      observations:await store.getObservations({...scope,monitorId:monitor.id}),
      deliveries:await deliveries.getDeliveries({...scope,monitorId:monitor.id})});
  }));
  router.get('/signal-distribution',...guard,handle(async (_req,res)=>{
    return res.sendFile(require('node:path').resolve(__dirname,'../public/signal-distribution.html'));
  }));
  return router;
}
module.exports = {createDistributionRouter,requestScope};
