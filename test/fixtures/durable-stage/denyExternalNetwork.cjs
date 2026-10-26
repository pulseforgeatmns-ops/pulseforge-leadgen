'use strict';
// Native-worker integration fixture: only the disposable PostgreSQL TCP endpoint
// is permitted. Even a swallowed forbidden attempt fails the child's exit code.
const net = require('node:net');
let attempts = 0;
const deny = name => { attempts++; throw Error(`Unexpected external network in native-stage fixture: ${name}`); };
const original = net.Socket.prototype.connect;
net.Socket.prototype.connect = function (...args) {
  const first = Array.isArray(args[0]) ? args[0][0] : args[0];
  const port = typeof first === 'object' ? first.port : first;
  const host = typeof first === 'object' ? first.host : args[1];
  if (String(port) !== process.env.TEST_STAGE_PG_PORT || host !== '127.0.0.1') return deny('tcp');
  return original.apply(this, args);
};
for (const protocol of ['http', 'https']) {
  require(`node:${protocol}`).request = () => deny(protocol);
  require(`node:${protocol}`).get = () => deny(protocol);
}
globalThis.fetch = () => deny('fetch');
for (const name of ['lookup', 'resolve', 'resolve4', 'resolve6', 'resolveMx', 'resolveTxt', 'reverse']) {
  require('node:dns')[name] = () => deny(`dns.${name}`);
  require('node:dns').promises[name] = () => deny(`dns.${name}`);
}
process.on('exit', () => { if (attempts) process.exitCode = 86; });
