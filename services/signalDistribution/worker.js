'use strict';
function createDistributionWorker({store,createProvider,gate=()=>({ok:false})}) {
  let running=false;
  return {async pollOnce() {
    if(running||!gate().ok)return {skipped:true};
    running=true;let row;
    try {
      row=await store.claim();if(!row)return {empty:true};
      // Persisted pool pin survives process restarts; no cache or pair substitution.
      const provider=createProvider();
      const snapshot=await provider.getSnapshot({tokenAddress:row.token_address,poolAddress:row.state.pairId||undefined});
      if(!gate().ok)throw new Error('distribution_gate_closed');
      return await store.observe(row,snapshot);
    }catch(err){
      if(row)await store.failed(row,'MARKET_UNAVAILABLE_OR_BUDGET_DEFERRED');
      return {observed:false};
    }finally{running=false;}
  }};
}
module.exports={createDistributionWorker};
