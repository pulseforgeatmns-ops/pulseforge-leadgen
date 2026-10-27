'use strict';

// Anchor's import hold is an admission requirement, distinct from opt-outs and
// call-only suppression. Unknown review state never authorizes tenant 10.
function anchorOutreachReviewRequired(row, clientId = row?.client_id ?? row?.tenant_id) {
  return String(clientId) === '10' && row?.ao_outreach_review_required !== false;
}

function anchorOutreachReviewSql(alias) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) throw new Error('Invalid prospect SQL alias');
  return `(${alias}.client_id <> 10 OR ((to_jsonb(${alias})->>'ao_outreach_review_required')::boolean IS FALSE AND ${companyReviewClearSql(alias)}))`;
}

// Enrichment can add another prospect under the same company. Its default false
// flag must not override a held or unknown sibling. Never span tenant ownership.
function companyReviewClearSql(alias) {
  const peer = `${alias}_anchor_review_peer`;
  return `NOT EXISTS (SELECT 1 FROM prospects ${peer}
    WHERE ${peer}.client_id = 10 AND ${peer}.client_id = ${alias}.client_id
      AND ${alias}.company_id IS NOT NULL AND ${peer}.company_id = ${alias}.company_id
      AND (to_jsonb(${peer})->>'ao_outreach_review_required')::boolean IS NOT FALSE)`;
}

function anchorOutreachReviewValueSql(alias) {
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(alias)) throw new Error('Invalid prospect SQL alias');
  const value = `(to_jsonb(${alias})->>'ao_outreach_review_required')::boolean`;
  return `(CASE WHEN ${alias}.client_id=10 THEN
    CASE WHEN ${value} IS NULL THEN NULL WHEN NOT ${companyReviewClearSql(alias)} THEN true ELSE ${value} END
    ELSE COALESCE(${value},false) END)`;
}

async function assertAnchorOutreachReviewCleared(db, prospectId, clientId, { lock = false } = {}) {
  if (String(clientId) !== '10') return;
  if (!prospectId) throw reviewError();
  const { rows } = await db.query(`SELECT id, client_id, company_id,
    ${anchorOutreachReviewValueSql('p')} AS ao_outreach_review_required
    FROM prospects p WHERE id=$1 AND client_id=$2${lock ? ' FOR SHARE' : ''}`, [prospectId, 10]);
  if (rows.length !== 1 || String(rows[0].id) !== String(prospectId) || String(rows[0].client_id) !== '10'
    || anchorOutreachReviewRequired(rows[0], 10)) throw reviewError();
  if (lock && rows[0].company_id != null) {
    const companyId = rows[0].company_id;
    // FOR UPDATE also conflicts with the FK key-share lock acquired by a new
    // linked contact. Either that insert commits before this re-read, or the
    // in-flight handoff finishes before the insert can commit.
    const company = await db.query('SELECT id, client_id FROM companies WHERE id=$1 AND client_id=$2 FOR UPDATE', [companyId, 10]);
    if (company.rows.length !== 1 || String(company.rows[0].id) !== String(companyId)
      || String(company.rows[0].client_id) !== '10') throw reviewError();
    const peers = await db.query(`SELECT id, client_id, company_id,
      (to_jsonb(p)->>'ao_outreach_review_required')::boolean AS ao_outreach_review_required
      FROM prospects p WHERE company_id=$1 AND client_id=$2 ORDER BY id FOR SHARE`, [companyId, 10]);
    if (!peers.rows.some(p => String(p.id) === String(prospectId))
      || peers.rows.some(p => String(p.client_id) !== '10' || String(p.company_id) !== String(companyId)
        || anchorOutreachReviewRequired(p, 10))) throw reviewError();
  }
}

// Queue handoffs serialize against ordinary writes to the hold, rather than
// trusting an earlier worker snapshot. Actions here must not update this row.
async function withAnchorOutreachReview(pool, prospectId, clientId, action) {
  if (String(clientId) !== '10') return action();
  const db = await pool.connect();
  let started = false;
  try {
    await db.query('BEGIN');
    started = true;
    await db.query("SET LOCAL lock_timeout='5s'");
    await db.query("SET LOCAL statement_timeout='10s'");
    await assertAnchorOutreachReviewCleared(db, prospectId, clientId, { lock: true });
    const result = await action();
    await db.query('COMMIT');
    started = false;
    return result;
  } finally {
    let discard = false;
    if (started) {
      try { await db.query('ROLLBACK'); } catch (_) { discard = true; }
    }
    db.release(discard);
  }
}

function reviewError() {
  return Object.assign(new Error('Separate outreach review is required before routing or contact'), {
    code: 'outreach_review_required', status: 409, providerBoundaryCrossed: false,
  });
}

module.exports = { anchorOutreachReviewRequired, anchorOutreachReviewSql, anchorOutreachReviewValueSql, assertAnchorOutreachReviewCleared, withAnchorOutreachReview };
