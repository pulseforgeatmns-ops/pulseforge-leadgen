'use strict';
const {config}=require('./config');
const {DashboardStore}=require('./store');
const {DashboardService}=require('./service');
let service;
function getService() {
  const c=config();if(!c.enabled)throw new Error('dashboard_disabled');
  if(!service)service=new DashboardService(new DashboardStore(require('../../db'),c));
  return service;
}
module.exports={getService};
