'use strict';
// Test preload: any PostgreSQL connection attempt from the evaluation CLI is
// recorded on stderr and refused, so the test can assert the CLI is offline.
const pg = require('pg');
const refuse = function refuse() {
  process.stderr.write('PG_CONNECT_ATTEMPT\n');
  return Promise.reject(new Error('offline evaluation must not connect to PostgreSQL'));
};
pg.Client.prototype.connect = refuse;
pg.Pool.prototype.connect = refuse;
