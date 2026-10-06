#!/usr/bin/env node
'use strict';

/**
 * One-time local GramJS authentication for SIGNAL-V1-007.
 * Prints TELEGRAM_SESSION_STRING to stdout — store as a secret; never commit.
 */

const readline = require('node:readline');

const { TelegramClient } = require('telegram');
const { StringSession } = require('telegram/sessions');
const { Logger, LogLevel } = require('telegram/extensions/Logger');

function fail(message) {
  console.error(message);
  process.exit(1);
}

function loadApiCredentials() {
  const apiId = Number(process.env.TELEGRAM_API_ID);
  const apiHash = process.env.TELEGRAM_API_HASH;

  if (!apiId || Number.isNaN(apiId)) {
    fail('Missing or invalid TELEGRAM_API_ID. Set it in your local environment and retry.');
  }
  if (!apiHash) {
    fail('Missing TELEGRAM_API_HASH. Set it in your local environment and retry.');
  }

  return { apiId, apiHash };
}

function promptLine(question) {
  const rl = readline.createInterface({ input: process.stdin, output: process.stdout });
  return new Promise(resolve => {
    rl.question(question, answer => {
      rl.close();
      resolve(String(answer).trim());
    });
  });
}

function promptHidden(question) {
  return new Promise(resolve => {
    process.stdout.write(question);
    const stdin = process.stdin;
    stdin.setRawMode(true);
    stdin.resume();
    stdin.setEncoding('utf8');

    let value = '';
    const onData = char => {
      const c = char.toString();
      if (c === '\n' || c === '\r' || c === '\u0004') {
        stdin.setRawMode(false);
        stdin.pause();
        stdin.removeListener('data', onData);
        process.stdout.write('\n');
        resolve(value);
      } else if (c === '\u0003') {
        stdin.setRawMode(false);
        process.exit(130);
      } else if (c === '\u007f') {
        value = value.slice(0, -1);
      } else {
        value += c;
      }
    };
    stdin.on('data', onData);
  });
}

function safeAuthErrorMessage(err) {
  if (err && typeof err.errorMessage === 'string') return err.errorMessage;
  if (err && typeof err.message === 'string') return err.message;
  return 'Authentication error';
}

async function main() {
  console.warn(
    'This session string grants access to your Telegram account.\n' +
      'Store it only as a secret. Do not commit or share it.'
  );

  const { apiId, apiHash } = loadApiCredentials();
  const session = new StringSession('');
  const client = new TelegramClient(session, apiId, apiHash, {
    connectionRetries: 3,
    baseLogger: new Logger(LogLevel.NONE),
  });

  try {
    await client.start({
      phoneNumber: async () => promptLine('Phone number (international format, e.g. +15551234567): '),
      phoneCode: async isCodeViaApp => {
        const label = isCodeViaApp
          ? 'Login code (from the Telegram app): '
          : 'Login code (SMS): ';
        return promptLine(label);
      },
      password: async () => promptHidden('Two-factor password: '),
      onError: err => {
        console.error(`Telegram authentication error: ${safeAuthErrorMessage(err)}`);
        return false;
      },
    });

    const sessionString = client.session.save();
    if (!sessionString) {
      fail('Authentication completed but no session string was produced.');
    }

    console.log(`TELEGRAM_SESSION_STRING=${sessionString}`);
  } finally {
    try {
      await client.disconnect();
    } catch {
      // ignore disconnect errors
    }
  }
}

main().catch(err => {
  console.error(`Telegram authentication failed: ${safeAuthErrorMessage(err)}`);
  process.exit(1);
});
