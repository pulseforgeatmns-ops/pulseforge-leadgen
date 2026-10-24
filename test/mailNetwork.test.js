'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const { EventEmitter } = require('node:events');

const {
  lookupPreferIpv4,
  resolveIpv4Addresses,
  connectIpv4Socket,
  createIpv4PreferringSmtpGetSocket,
  genericSmtpImapTlsOptions,
} = require('../utils/mailNetwork');
const { createSmtpTransport, PROVIDER_TYPES } = require('../services/tenantMailbox');

function fakeSocket({ fail = false, emitEvent = 'secureConnect' } = {}) {
  const socket = new EventEmitter();
  socket.setKeepAlive = () => {};
  queueMicrotask(() => {
    if (fail) socket.emit('error', Object.assign(new Error('connect ENETUNREACH'), { code: 'ENETUNREACH' }));
    else socket.emit(emitEvent);
  });
  return socket;
}

describe('SPEC-248 generic SMTP/IMAP IPv4 network helpers', () => {
  it('prefers IPv4 when hostname resolves both families', async () => {
    const addresses = await resolveIpv4Addresses('mail.example.com', {
      resolve4: async () => ['203.0.113.10', '198.51.100.20'],
      lookup: async () => {
        throw new Error('lookup should not run when resolve4 succeeds');
      },
    });
    assert.deepEqual(addresses, ['203.0.113.10', '198.51.100.20']);
  });

  it('lookupPreferIpv4 requests family 4 from dns.lookup', async () => {
    let captured = null;
    const original = require('node:dns').lookup;
    require('node:dns').lookup = (hostname, options, callback) => {
      captured = { hostname, options };
      callback(null, '203.0.113.44', 4);
    };
    try {
      await new Promise((resolve, reject) => {
        lookupPreferIpv4('mail.example.com', {}, (err, address, family) => {
          if (err) reject(err);
          else {
            assert.equal(address, '203.0.113.44');
            assert.equal(family, 4);
            resolve();
          }
        });
      });
      assert.equal(captured.hostname, 'mail.example.com');
      assert.equal(captured.options.family, 4);
    } finally {
      require('node:dns').lookup = original;
    }
  });

  it('connectIpv4Socket keeps TLS servername on provider hostname', async () => {
    let tlsArgs = null;
    const socket = await connectIpv4Socket({
      hostname: 'mail.adm.tools',
      port: 465,
      secure: true,
      resolve4: async () => ['203.0.113.55'],
      tlsConnect: (opts) => {
        tlsArgs = opts;
        return fakeSocket({ emitEvent: 'secureConnect' });
      },
    });
    assert.equal(tlsArgs.host, '203.0.113.55');
    assert.equal(tlsArgs.servername, 'mail.adm.tools');
    assert.equal(tlsArgs.family, 4);
    assert.equal(socket.secured, true);
  });

  it('falls back to the next IPv4 address when the first connect fails', async () => {
    const attempts = [];
    const socket = await connectIpv4Socket({
      hostname: 'mail.example.com',
      port: 465,
      secure: true,
      resolve4: async () => ['203.0.113.1', '203.0.113.2'],
      tlsConnect: (opts) => {
        attempts.push(opts.host);
        return fakeSocket({ fail: attempts.length === 1, emitEvent: 'secureConnect' });
      },
    });
    assert.deepEqual(attempts, ['203.0.113.1', '203.0.113.2']);
    assert.equal(socket.address, '203.0.113.2');
  });

  it('returns explicit failure when no IPv4 path succeeds', async () => {
    await assert.rejects(
      () => connectIpv4Socket({
        hostname: 'mail.example.com',
        port: 465,
        secure: true,
        resolve4: async () => ['203.0.113.9'],
        tlsConnect: () => fakeSocket({ fail: true }),
      }),
      (err) => err.code === 'ESOCKET'
    );
  });

  it('createIpv4PreferringSmtpGetSocket wires secured SMTP sockets for nodemailer', async () => {
    const getSocket = createIpv4PreferringSmtpGetSocket('mail.adm.tools', {
      connectIpv4Socket: async () => ({
        socket: fakeSocket({ emitEvent: 'secureConnect' }),
        secured: true,
        address: '203.0.113.77',
      }),
    });
    const result = await new Promise((resolve, reject) => {
      getSocket({ host: 'mail.adm.tools', port: 465, secure: true }, (err, socketOptions) => {
        if (err) reject(err);
        else resolve(socketOptions);
      });
    });
    assert.equal(result.secured, true);
    assert.ok(result.connection);
  });

  it('GENERIC_SMTP_IMAP transport keeps hostname config and adds IPv4 getSocket', () => {
    const captured = { transportOptions: null };
    const nodemailer = {
      createTransport(options) {
        captured.transportOptions = options;
        return { verify: async () => true };
      },
    };
    createSmtpTransport({
      providerType: PROVIDER_TYPES.GENERIC_SMTP_IMAP,
      smtpHost: 'mail.adm.tools',
      smtpPort: 465,
      smtpTlsMode: 'SSL_TLS',
      mailboxAddress: 'hello@example.com',
    }, 'secret-value', {
      nodemailer,
      smtpGetSocket: () => {},
    });
    assert.equal(captured.transportOptions.host, 'mail.adm.tools');
    assert.equal(captured.transportOptions.tls.servername, 'mail.adm.tools');
    assert.equal(typeof captured.transportOptions.getSocket, 'function');
    assert.equal(captured.transportOptions.auth.user, 'hello@example.com');
    assert.equal(captured.transportOptions.auth.pass, 'secret-value');
  });

  it('genericSmtpImapTlsOptions is reusable for non-Babrun providers', () => {
    const tls = genericSmtpImapTlsOptions('smtp.other-provider.example');
    assert.deepEqual(tls, {
      servername: 'smtp.other-provider.example',
      family: 4,
    });
  });
});
