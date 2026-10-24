'use strict';
require('dotenv').config();
const pool = require('../db');
const { buildWarmOutcomeValidation } = require('../utils/maxOutcomeValidation');
const { assertAllowed, boundedInteger, optionalPositiveInteger, tokenizeArgs } = require('../utils/maxCli');

function parseArgs(argv=process.argv.slice(2)) {
  const parsed=tokenizeArgs(argv);
  assertAllowed(parsed,{values:['--client-id','--since-days']});
  return {clientId:optionalPositiveInteger(parsed.values.get('--client-id'),'--client-id'),sinceDays:boundedInteger(parsed.values.get('--since-days'),'--since-days',{defaultValue:90,max:365})};
}
if(require.main===module) buildWarmOutcomeValidation(parseArgs(),pool).then(r=>console.log(JSON.stringify(r,null,2)))
  .catch(e=>{console.error(e.stack||e.message);process.exitCode=1}).finally(()=>pool.end());
module.exports={parseArgs};
