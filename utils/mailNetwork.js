'use strict';

/**
 * IPv4-first DNS helpers for tenant-owned GENERIC_SMTP_IMAP mail transports.
 *
 * Railway and similar runtimes may advertise IPv6 locally while lacking a usable
 * IPv6 route to the provider. Nodemailer can otherwise pick an IPv6 AAAA answer
 * first. These helpers keep the configured hostname for TLS/SNI and credentials
 * while connecting over IPv4 when available.
 */

const dns = require('node:dns');
const net = require('node:net');
const tls = require('node:tls');
const { promisify } = require('node:util');

const dnsResolve4 = promisify(dns.resolve4);
const dnsLookup = promisify(dns.lookup);

function isIpAddress(value) {
  return Boolean(value) && net.isIP(String(value)) !== 0;
}

async function resolveIpv4Addresses(hostname, deps = {}) {
  const host = String(hostname || '').trim();
  if (!host) {
    throw Object.assign(new Error('Mail hostname is required for IPv4 resolution.'), { code: 'mail_host_required' });
  }
  if (isIpAddress(host)) {
    return [host];
  }

  const resolve4 = deps.resolve4 || dnsResolve4;
  const lookup = deps.lookup || dnsLookup;

  try {
    const addresses = await resolve4(host);
    if (Array.isArray(addresses) && addresses.length) {
      return [...new Set(addresses)];
    }
  } catch (err) {
    if (!isBenignDnsEmptyError(err)) throw err;
  }

  try {
    const result = await lookup(host, { family: 4, verbatim: false });
    const address = typeof result === 'string' ? result : result?.address;
    if (address) return [address];
  } catch (err) {
    throw Object.assign(new Error(`IPv4 resolution failed for ${host}: ${err.message || err}`), {
      code: err.code || 'EDNS',
      hostname: host,
    });
  }

  throw Object.assign(new Error(`No IPv4 address found for ${host}`), {
    code: 'EDNS',
    hostname: host,
  });
}

function isBenignDnsEmptyError(err) {
  const code = err?.code;
  return code === 'ENODATA' || code === 'ENOTFOUND' || code === 'ESERVFAIL' || code === 'EAI_AGAIN';
}

function lookupPreferIpv4(hostname, options, callback) {
  if (typeof options === 'function') {
    callback = options;
    options = {};
  }
  dns.lookup(hostname, { ...(options || {}), family: 4, verbatim: false }, callback);
}

function connectIpv4Socket({
  hostname,
  port,
  secure = false,
  tlsOptions = {},
  resolve4 = resolveIpv4Addresses,
  netConnect = net.connect,
  tlsConnect = tls.connect,
}) {
  return new Promise((resolve, reject) => {
    resolve4(hostname)
      .then((addresses) => attemptAddress(addresses, 0))
      .catch(reject);

    function attemptAddress(addresses, index) {
      if (index >= addresses.length) {
        reject(Object.assign(new Error(`IPv4 connection failed for ${hostname}`), {
          code: 'ESOCKET',
          hostname,
        }));
        return;
      }

      const address = addresses[index];
      const connectOpts = {
        host: address,
        port: Number(port),
        family: 4,
        servername: tlsOptions.servername || hostname,
        rejectUnauthorized: tlsOptions.rejectUnauthorized !== false,
        minVersion: tlsOptions.minVersion,
      };

      const socket = secure ? tlsConnect(connectOpts) : netConnect(connectOpts);
      const onConnect = () => {
        cleanup();
        resolve({ socket, secured: secure, address });
      };
      const onError = (err) => {
        cleanup();
        attemptAddress(addresses, index + 1);
      };
      const cleanup = () => {
        socket.removeListener('connect', onConnect);
        socket.removeListener('secureConnect', onConnect);
        socket.removeListener('error', onError);
      };

      socket.once('error', onError);
      if (secure) socket.once('secureConnect', onConnect);
      else socket.once('connect', onConnect);
    }
  });
}

function createIpv4PreferringSmtpGetSocket(hostname, deps = {}) {
  const servername = String(hostname || '').trim();
  const connect = deps.connectIpv4Socket || connectIpv4Socket;
  const resolve4 = deps.resolveIpv4Addresses || resolveIpv4Addresses;

  return function getSocket(options, callback) {
    const targetHost = servername || options.host;
    const port = options.port;
    const secure = Boolean(options.secure);

    connect({
      hostname: targetHost,
      port,
      secure,
      tlsOptions: {
        servername: targetHost,
        rejectUnauthorized: options.tls?.rejectUnauthorized,
        minVersion: options.tls?.minVersion,
      },
      resolve4,
      netConnect: deps.netConnect,
      tlsConnect: deps.tlsConnect,
    })
      .then(({ socket, secured }) => callback(null, { connection: socket, secured }))
      .catch((err) => callback(err));
  };
}

function genericSmtpImapTlsOptions(hostname) {
  const host = String(hostname || '').trim();
  if (!host || isIpAddress(host)) {
    return null;
  }
  return {
    servername: host,
    family: 4,
  };
}

module.exports = {
  lookupPreferIpv4,
  resolveIpv4Addresses,
  connectIpv4Socket,
  createIpv4PreferringSmtpGetSocket,
  genericSmtpImapTlsOptions,
};
