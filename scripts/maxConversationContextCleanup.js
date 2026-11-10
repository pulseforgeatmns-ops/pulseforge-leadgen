'use strict';

const db = require('../db');

async function main() {
  let total = 0;
  const maxBatches = 20;

  try {
    for (let batch = 0; batch < maxBatches; batch++) {
      const result = await db.query(`
        WITH expired AS (
          SELECT tenant_id, session_id
          FROM max_conversation_context
          WHERE expires_at <= NOW()
          ORDER BY expires_at
          LIMIT 500
          FOR UPDATE SKIP LOCKED
        )
        DELETE FROM max_conversation_context AS context
        USING expired
        WHERE context.tenant_id = expired.tenant_id
          AND context.session_id = expired.session_id
      `);

      const deleted = result.rowCount;
      total += deleted;

      if (deleted < 500) break;
    }

    console.log('[MAX_CONTEXT_CLEANUP]', JSON.stringify({
      ok: true,
      deleted: total
    }));
  } catch (error) {
    console.error('[MAX_CONTEXT_CLEANUP] failed');
    process.exitCode = 1;
  } finally {
    await db.end();
  }
}

main();
