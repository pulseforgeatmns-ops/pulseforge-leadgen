'use strict';

// Preload for the synthetic owner-evidence regression command. Even a swallowed
// unexpected network exception fails the worker process at exit.
let attempts = 0;
const deny = name => function () {
  attempts++;
  throw new Error(`Unexpected network in offline website-evidence suite: ${name}`);
};
for (const protocol of ['http', 'https']) {
  const transport = require(`node:${protocol}`);
  transport.request = deny(`${protocol}.request`);
  transport.get = deny(`${protocol}.get`);
}
require('node:net').Socket.prototype.connect = deny('net.Socket.connect');
require('node:tls').connect = deny('tls.connect');
globalThis.fetch = deny('global.fetch');
const dns = require('node:dns');
for (const name of ['lookup', 'resolve', 'resolve4', 'resolve6', 'resolveMx', 'resolveTxt', 'reverse']) {
  dns[name] = deny(`dns.${name}`);
  dns.promises[name] = deny(`dns.promises.${name}`);
}
process.on('exit', () => {
  if (attempts) {
    process.stderr.write(`Offline website-evidence suite blocked ${attempts} unexpected network attempts.\n`);
    process.exitCode = 1;
  }
});
