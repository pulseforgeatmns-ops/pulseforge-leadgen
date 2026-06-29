require('dotenv').config();

const pool = require('../db');
const { ANCHOR_DRAFT_SEQUENCES } = require('../utils/anchorEmailTemplates');
const { renderTemplate } = require('../utils/templateMerge');
const { evaluateSendingReadiness } = require('../utils/sendingReadiness');

const ANCHOR_CLIENT_ID = 10;
const ANCHOR_SEQUENCE_MAP = {
  10: {
    law_firm: 'anchor_law_firm_draft',
    accounting: 'anchor_accounting_draft',
  },
};

function legacyFillTemplate(template, prospect) {
  const rawName = prospect.first_name || prospect.name?.split(' ')[0] || '';
  const firstName = rawName && rawName !== '—' ? rawName : 'there';
  let businessName = prospect.company || '';
  if (!businessName) {
    businessName = (prospect.notes?.split('—')[0]?.trim() || '')
      .replace(/\.(com|net|org|io|us)$/i, '')
      .replace(/^(www\.)/i, '')
      .trim();
  }
  if (!businessName || businessName.length < 4) businessName = 'your business';
  return template
    .replace(/{{first_name}}/g, firstName)
    .replace(/{{business_name}}/g, businessName);
}

async function run() {
  const client = (await pool.query(
    'SELECT * FROM clients WHERE id = $1',
    [ANCHOR_CLIENT_ID]
  )).rows[0];
  const prospects = (await pool.query(`
    SELECT p.*, c.name AS company, row_to_json(c) AS company_fields
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.client_id = $1
    ORDER BY p.id
  `, [ANCHOR_CLIENT_ID])).rows;

  let comparisons = 0;
  let mismatches = 0;
  let readinessTokenBlocked = 0;
  let sample = null;

  for (const prospect of prospects) {
    const sequenceName = ANCHOR_SEQUENCE_MAP[ANCHOR_CLIENT_ID][prospect.vertical];
    // Actual Anchor rows have no first_name. Supply one only for the strict
    // legacy/new output comparison, then evaluate the untouched row below.
    const comparisonProspect = { ...prospect, first_name: 'Avery' };
    for (const step of ANCHOR_DRAFT_SEQUENCES[sequenceName] || []) {
      for (const template of [step.subject, step.body]) {
        const rendered = renderTemplate(template, comparisonProspect);
        comparisons++;
        if (!rendered.ok || rendered.output !== legacyFillTemplate(template, comparisonProspect)) {
          mismatches++;
        }
      }
      if (!sample) {
        sample = {
          company: prospect.company,
          subject: renderTemplate(step.subject, comparisonProspect).output,
          body: renderTemplate(step.body, comparisonProspect).output,
        };
      }
    }

    const readiness = await evaluateSendingReadiness({
      client,
      prospect,
      sequenceCatalog: ANCHOR_DRAFT_SEQUENCES,
      clientSequenceMap: ANCHOR_SEQUENCE_MAP,
      assignedSequenceName: sequenceName,
      brevoState: {
        domain: { verified: true, authenticated: true },
        sender: { email: client.sender_email, active: true },
        errors: [],
      },
      pool,
    });
    if (readiness.failures.some(failure =>
      failure.code === 'template_required_tokens_present'
      && failure.details.missing_tokens.includes('first_name')
    )) {
      readinessTokenBlocked++;
    }
  }

  const fallback = renderTemplate('We help {{size|firms like yours}}.', prospects[0]);
  console.log(JSON.stringify({
    prospects: prospects.length,
    comparisons,
    mismatches,
    actual_missing_first_name: prospects.filter(prospect => !prospect.first_name).length,
    readiness_token_blocked: readinessTokenBlocked,
    fallback_output: fallback.output,
    sample,
  }, null, 2));
}

run()
  .then(() => process.exit(0))
  .catch(error => {
    console.error(error.stack || error.message);
    process.exit(1);
  });
