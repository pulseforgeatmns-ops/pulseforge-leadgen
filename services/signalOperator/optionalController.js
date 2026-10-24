'use strict';
function startOptionalPilotController(env=process.env) {
  if(env.SIGNAL_OPERATION_MODE!=='pilot' || env.SIGNAL_PILOT_CONTROLLER_ENABLED!=='1')return null;
  return require('./pilotController').startSignalPilotController();
}
module.exports={startOptionalPilotController};
