'use strict';

const { TRANSACTION_TIMEOUT_MS } = require('./claimPolicy');

// pg-pool removes its idle error listener when a client is checked out. Own that
// listener until release, including backend termination while no query is active.
async function withAttentionTransaction(pool, callback, { bounded = false } = {}) {
  const client = await pool.connect();
  let connectionError = null;
  let rollbackError = null;
  let closed = false;
  let rejectConnection;
  const connectionFailed = new Promise((resolve, reject) => { rejectConnection = reject; });
  const onError = error => {
    connectionError = error;
    rejectConnection(error);
  };
  client.on?.('error', onError);
  const db = {
    query(...args) {
      if (closed || connectionError) {
        return Promise.reject(connectionError || Object.assign(new Error('Attention transaction is closed'), {
          code: 'ATTENTION_TRANSACTION_CLOSED',
        }));
      }
      return client.query(...args);
    },
  };
  try {
    return await Promise.race([
      (async () => {
        await db.query('BEGIN');
        if (bounded) {
          await db.query(`SET LOCAL statement_timeout = '${TRANSACTION_TIMEOUT_MS}ms'`);
          await db.query(`SET LOCAL idle_in_transaction_session_timeout = '${TRANSACTION_TIMEOUT_MS}ms'`);
        }
        const result = await callback(db);
        await db.query('COMMIT');
        return result;
      })(),
      connectionFailed,
    ]);
  } catch (error) {
    closed = true;
    try { await client.query('ROLLBACK'); } catch (failure) { rollbackError = failure; }
    throw error;
  } finally {
    closed = true;
    // Discard a broken connection. A late callback cannot use this handle after
    // rollback/release or accidentally write into a different pool transaction.
    client.release(connectionError || rollbackError || undefined);
    client.removeListener?.('error', onError);
  }
}

module.exports = { withAttentionTransaction };
