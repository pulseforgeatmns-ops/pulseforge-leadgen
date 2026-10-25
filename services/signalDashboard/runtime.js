'use strict';
const {config}=require('./config');
const {DashboardStore}=require('./store');
const {DashboardService}=require('./service');
let service;
function getService() {
  const c=config();if(!c.enabled)throw new Error('dashboard_disabled');
  if(!service){const pool=require('../../db');service=new DashboardService(new DashboardStore(pool,c),{zoneStore:new(require('./zoneStore').ZoneStore)(pool,c)});}
  return service;
}
module.exports={getService};
