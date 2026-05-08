require('dotenv').config();
const pool = require('./db');

const DRY_RUN = process.argv.includes('--dry-run');

const RENAMES = [
  { from: ['linkedin_agent', 'link_agent', 'link_agent1'],                      to: 'link'      },
  { from: ['facebook_agent', 'faye_agent', 'faye_agent1'],                      to: 'faye'      },
  { from: ['emmett_agent', 'emmett_agent1', 'email_agent', 'email'],             to: 'emmett'    },
  { from: ['sam_agent'],                                                          to: 'sam'       },
  { from: ['cal_agent'],                                                          to: 'cal'       },
  { from: ['analytics_agent'],                                                    to: 'analytics' },
  { from: ['scout_agent', 'Scout'],                                               to: 'scout'     },
  { from: ['sketch_agent', 'Sketch'],                                             to: 'sketch'    },
  { from: ['max_agent', 'Max'],                                                   to: 'max'       },
  { from: ['rex_agent', 'Rex'],                                                   to: 'rex'       },
  { from: ['riley_agent'],                                                         to: 'riley'     },
  { from: ['vera_agent', 'Vera'],                                                  to: 'vera'      },
  { from: ['paige_agent', 'Paige'],                                               to: 'paige'     },
  { from: ['penny_agent'],                                                         to: 'penny'     },
  { from: ['ivy_agent'],                                                           to: 'ivy'       },
  { from: ['facebook_page_publisher', 'linkedin_page_publisher',
           'google_business_publisher', 'blog_publisher'],                        to: 'paige'     },
];

async function run() {
  console.log(DRY_RUN ? '--- DRY RUN — no writes ---\n' : '--- LIVE RUN ---\n');

  let totalRenamed = 0;

  for (const rule of RENAMES) {
    for (const oldName of rule.from) {
      const { rows } = await pool.query(
        `SELECT COUNT(*)::int AS n FROM agent_log WHERE agent_name = $1`, [oldName]
      );
      const n = rows[0].n;
      if (n === 0) {
        console.log(`  '${oldName}' → '${rule.to}' : 0 rows (nothing to do)`);
        continue;
      }
      if (DRY_RUN) {
        console.log(`  Would rename '${oldName}' → '${rule.to}' : ${n} rows`);
      } else {
        await pool.query(
          `UPDATE agent_log SET agent_name = $1 WHERE agent_name = $2`, [rule.to, oldName]
        );
        console.log(`  Renamed '${oldName}' → '${rule.to}' : ${n} rows updated`);
        totalRenamed += n;
      }
    }
  }

  console.log(DRY_RUN ? '\nDry run complete.' : `\nDone — ${totalRenamed} rows renamed.`);
  await pool.end();
}

run().catch(err => { console.error(err); process.exit(1); });
