'use strict';
// Preload for disposable PostgreSQL integration tests. Loopback DB connections
// are allowed; all external provider transports fail even if errors are caught.
let attempts = 0;
const deny = label => { attempts++; const error=Error(`Unexpected external transport: ${label}`);
  process.stderr.write(`${error.stack}\n`); throw error; };
for (const scheme of ['http', 'https']) {
  const transport=require(`node:${scheme}`);
  for (const method of ['request','get']) {
    const original=transport[method];
    transport[method]=function(...args) {
      const input=args[0];let host;
      try { host=typeof input==='string'||input instanceof URL ? new URL(input).hostname : input?.hostname||input?.host; } catch {}
      if(!['127.0.0.1','::1','[::1]'].includes(host)) return deny(`${scheme}.${method}`);
      return original.apply(this,args);
    };
  }
}
globalThis.fetch = () => deny('global.fetch');
require('node:tls').connect = () => deny('tls');
const connect = require('node:net').Socket.prototype.connect;
require('node:net').Socket.prototype.connect = function (...args) {
  const input = Array.isArray(args[0]) ? args[0][0] : args[0];
  const host = typeof input === 'object' ? input.host : args[1];
  if (!['127.0.0.1', '::1'].includes(host)) return deny('non-loopback TCP');
  return connect.apply(this, args);
};
process.on('exit', () => { if (attempts) {
  process.stderr.write(`Blocked ${attempts} unexpected external transport attempts.\n`); process.exitCode = 1;
} });
module.exports = { attempts: () => attempts };

const dns=require('node:dns');
for(const name of ['resolve','resolve4','resolve6','resolveMx','resolveTxt','reverse']) {
  dns[name]=()=>deny(`dns.${name}`);dns.promises[name]=()=>deny(`dns.promises.${name}`);
}
function literal(address,options) {
  if(!['127.0.0.1','::1'].includes(address)) return deny('dns.lookup');
  const value={address,family:address==='::1'?6:4};return options?.all?[value]:value;
}
dns.lookup=(address,options,callback)=>{
  const cb=typeof options==='function'?options:callback;const value=literal(address,options);
  process.nextTick(()=>Array.isArray(value)?cb(null,value):cb(null,value.address,value.family));
};
dns.promises.lookup=async(address,options)=>literal(address,options);
require('node:dgram').createSocket=()=>deny('udp');
