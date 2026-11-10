'use strict';

// Install before application modules. Every unexpected attempt is sticky, even
// when a fail-soft application path catches the thrown error.
function installGuard(database, { blockSubprocess = false } = {}) {
  const net = require('node:net');
  const dns = require('node:dns');
  const restores = [];
  const httpPorts = new Set();
  let attempts = 0;
  function deny() {
    attempts++;
    const error = new Error('egress_denied');
    error.code = 'EGRESS_DENIED';
    throw error;
  }
  function patch(object, name, replacement) {
    const original = object[name];
    object[name] = replacement(original);
    restores.push(() => { object[name] = original; });
  }
  patch(net.Socket.prototype, 'connect', original => function (...args) {
    const options = Array.isArray(args[0]) ? args[0][0] : net._normalizeArgs(args)[0];
    const db = options.host === database.host && Number(options.port) === database.port;
    const http = options.host === '127.0.0.1' && httpPorts.has(Number(options.port));
    if (!db && !http) return deny();
    return original.apply(this, args);
  });
  patch(dns, 'lookup', original => function (host, ...args) {
    if (host !== database.host) return deny();
    return original.call(this, host, ...args);
  });
  for (const name of ['resolve', 'resolve4', 'resolve6', 'resolveMx', 'resolveTxt', 'reverse']) {
    patch(dns, name, () => deny);
    patch(dns.promises, name, () => deny);
  }
  patch(dns.promises, 'lookup', () => deny);
  patch(require('node:tls'), 'connect', () => deny);
  patch(require('node:dgram'), 'createSocket', () => deny);
  patch(globalThis, 'fetch', () => deny);
  if (blockSubprocess) {
    for (const name of ['spawn', 'spawnSync', 'exec', 'execSync', 'execFile', 'execFileSync', 'fork']) {
      patch(require('node:child_process'), name, () => deny);
    }
  }
  const onExit = () => { if (attempts) process.exitCode = 1; };
  process.on('exit', onExit);
  return {
    deny,
    allowHttp(port) { httpPorts.add(port); },
    assertClean() { if (attempts) throw new Error('unexpected_egress_attempt'); },
    selfTest() {
      const before = attempts;
      const socket = new net.Socket();
      try {
        socket.connect({ host: 'production.invalid', port: 443 });
        throw new Error('egress_guard_failed');
      } catch (error) {
        if (error.code !== 'EGRESS_DENIED' || attempts !== before + 1) throw new Error('egress_guard_failed');
        attempts = before; // Only this deliberate synchronous probe is exempt.
      } finally { socket.destroy(); }
    },
    restore() { for (const restore of restores.reverse()) restore(); process.off('exit', onExit); },
  };
}
module.exports = { installGuard };
