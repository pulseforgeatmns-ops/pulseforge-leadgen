'use strict';
// Process-local confinement for the standalone CLI. Never install in the app.
const { AsyncLocalStorage } = require('node:async_hooks');
const https = require('node:https');
const http = require('node:http');
const net = require('node:net');
const tls = require('node:tls');
function installTransport({ databaseUrl }) {
  const database = new URL(databaseUrl);
  if (!['postgres:','postgresql:'].includes(database.protocol) || !database.hostname) throw Error('bounded_database_target_required');
  const databaseHost = database.hostname.replace(/^\[|\]$/g,'');
  const databasePort = Number(database.port || 5432);
  const allowed = new AsyncLocalStorage();
  const databaseSockets = new WeakSet();
  const request = https.request.bind(https);
  const connect = net.Socket.prototype.connect;
  const tlsConnect = tls.connect;
  const deny = () => { throw Object.assign(Error('bounded_external_transport_denied'),{code:'bounded_external_transport_denied'}); };
  const permitted = (host,port) => host===databaseHost && Number(port)===databasePort
    || allowed.getStore()===true && host==='maps.googleapis.com' && Number(port)===443;
  net.Socket.prototype.connect = function (...args) {
    const input = Array.isArray(args[0]) ? args[0] : args;
    const options = typeof input[0]==='object' ? input[0] : {port:input[0],host:input[1]};
    if (!permitted(options.host,options.port)) return deny();
    if (options.host===databaseHost && Number(options.port)===databasePort) databaseSockets.add(this);
    return connect.apply(this,args);
  };
  tls.connect = function (...args) {
    const options = typeof args[0]==='object' ? args[0] : {port:args[0],host:args[1]};
    // pg supplies an already-connected socket for TLS; its TCP target was
    // checked above. No new TLS connection to an arbitrary host is permitted.
    if (options.socket ? !(databaseSockets.has(options.socket) && (options.servername==null || options.servername===databaseHost))
      : !permitted(options.host || options.servername,options.port || 443)) return deny();
    return tlsConnect.apply(this,args);
  };
  for (const module of [https,http]) { module.request=deny; module.get=deny; }
  globalThis.fetch=deny;
  const dns=require('node:dns');
  for(const target of [dns,dns.promises]) {
    if (target.Resolver?.prototype) for(const key of Object.getOwnPropertyNames(target.Resolver.prototype))
      if(key.startsWith('resolve') || ['reverse','setServers'].includes(key)) target.Resolver.prototype[key]=deny;
    const lookup=target.lookup.bind(target);
    target.lookup=(host,...args)=>host===databaseHost || allowed.getStore()===true && host==='maps.googleapis.com'
      ? lookup(host,...args) : deny();
    for(const key of Object.keys(target)) if(key.startsWith('resolve') || ['reverse','lookupService','Resolver','setServers'].includes(key)) target[key]=deny;
  }
  require('node:worker_threads').Worker=deny;
  require('node:dgram').createSocket=deny;
  for (const key of ['exec','execFile','spawn','fork','execSync','execFileSync','spawnSync']) require('node:child_process')[key]=deny;
  return function fetchPlaces(input,{signal,redirect}={}) {
    const url=new URL(input);
    if (url.origin!=='https://maps.googleapis.com' || url.username || url.password || url.hash || redirect!=='error'
      || !['/maps/api/place/textsearch/json','/maps/api/place/details/json'].includes(url.pathname)) return deny();
    return allowed.run(true,()=>new Promise((resolve,reject)=>{
      const req=request(url,{method:'GET',signal,agent:false},res=>{
        const chunks=[];let bytes=0;
        res.on('data',chunk=>{bytes+=chunk.length;if(bytes>2*1024*1024){res.destroy(Error('bounded_body_too_large'));return;}chunks.push(chunk);});
        res.on('error',reject);
        res.on('end',()=>{try { const data=JSON.parse(Buffer.concat(chunks).toString('utf8'));
          resolve({ok:res.statusCode>=200&&res.statusCode<300,status:res.statusCode,redirected:false,json:async()=>data});
        } catch (_) {reject(Error('bounded_invalid_json'));}});
      });
      req.on('error',reject); req.end();
    }));
  };
}
module.exports={installTransport};
