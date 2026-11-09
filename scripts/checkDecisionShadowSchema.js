'use strict';

// SELECT-only preflight. Deliberately has no apply mode or startup DDL.
const { createShadowPool } = require('../packages/decision-service/ShadowEventSink');
const { inspectShadowSchema } = require('../packages/decision-service/shadowSchema');
async function main() {
  if (!process.env.DATABASE_URL) throw Object.assign(new Error(), { code: 'UNCONFIGURED' });
  const db = createShadowPool();
  try {
    const report = await inspectShadowSchema(db);
    console.log(JSON.stringify(report, null, 2));
    process.exitCode = report.ready ? 0 : 2;
  } finally { await db.end(); }
}
if (require.main === module) main().catch(() => {
  console.error('Shadow schema check failed; inspect database access privately. No schema changes were applied.');
  process.exitCode = 1;
});
