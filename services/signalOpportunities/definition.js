'use strict';
const {createHash}=require('node:crypto');
// Never mutate a released definition. Any rule/weight change requires a new version.
const DEFINITION=Object.freeze({version:'opportunity-v1.0.0',windowMs:30*60000,maxMarketAgeMs:90000,
  minLiquidity:20000,rapidMs:12*60000,minMomentum:0.03,maxExtension:0.6,volumeRatio:1.25,
  watch:15,opportunity:45,hot:75,minLeadLagSamples:10,
  weights:Object.freeze({observed:5,independent:20,rapid:10,liquidity:10,price:10,volume:10,buyers:10,
    echo:-15,extended:-25,liquidityLoss:-30,promotionDivergence:-20,distribution:-50})});
const hash=x=>createHash('sha256').update(JSON.stringify(x)).digest('hex');
const VERSION_HASH=hash(DEFINITION);
module.exports={DEFINITION,VERSION_HASH,hash};
