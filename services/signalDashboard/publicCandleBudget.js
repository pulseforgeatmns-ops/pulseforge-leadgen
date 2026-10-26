'use strict';
// Reuse Signal's existing deployment-wide Gecko reservation. This does not
// initialize distribution workers, activation windows, schemas or delivery.
class SharedGeckoCandleBudget {
 constructor(pool){this.pool=pool;}
 async consume(lane){
  if(lane!=='existing')throw Error('invalid_candle_budget_lane');
  const c=await this.pool.connect();
  try{
   await c.query('BEGIN');
   const gate=await c.query('SELECT blocked_until,clock_timestamp() AS now FROM signal_distribution_request_budget WHERE id=1 FOR UPDATE');
   if(!gate.rowCount){await c.query('ROLLBACK');return false;}
   const row=gate.rows[0];
   if(row.blocked_until&&+new Date(row.blocked_until)>+new Date(row.now)){await c.query('ROLLBACK');return false;}
   const count=await c.query("SELECT count(*)::int AS count FROM signal_distribution_request_attempts WHERE attempted_at>clock_timestamp()-interval '60 seconds'");
   if(count.rows[0].count>=6){await c.query('ROLLBACK');return false;}
   await c.query("INSERT INTO signal_distribution_request_attempts(lane,attempted_at) VALUES('existing',clock_timestamp())");
   await c.query('COMMIT');return true;
  }catch(error){await c.query('ROLLBACK').catch(()=>{});throw error;}finally{c.release();}
 }
 async backoff(untilMs){
  if(!Number.isFinite(untilMs))throw Error('invalid_candle_backoff');
  await this.pool.query("UPDATE signal_distribution_request_budget SET blocked_until=GREATEST(blocked_until,clock_timestamp()+interval '60 seconds',$1) WHERE id=1",[new Date(untilMs)]);
 }
}
module.exports={SharedGeckoCandleBudget};
