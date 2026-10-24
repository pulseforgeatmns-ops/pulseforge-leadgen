require('dotenv').config();

const fs = require('fs');
const path = require('path');
const pool = require('../db');
const { ANCHOR_DRAFT_SEQUENCES } = require('../utils/anchorEmailTemplates');
const { renderTemplate } = require('../utils/templateMerge');
const { CLIENT_SEQUENCE_MAP, getBrevoState } = require('../utils/sendingReadiness');
const { ensureBusinessNameShortColumns } = require('../utils/businessNameShort');

const CLIENT_ID = 10;
const OUTPUT_PATH = path.join(__dirname, '..', 'docs', 'ANCHOR_EMAIL_TEMPLATE_REVIEW.md');
const VERIFIED_EMAIL_STATUSES = new Set(['valid', 'verified']);

function lineBreaks(text) {
  return String(text || '');
}

function displayName(prospect) {
  return [prospect.first_name, prospect.last_name].filter(Boolean).join(' ');
}

function canRenderForReview(prospect) {
  const status = String(prospect.email_status || '').toLowerCase();
  return prospect.vertical === 'law_firm'
    && Boolean(prospect.first_name)
    && Boolean(prospect.email)
    && VERIFIED_EMAIL_STATUSES.has(status)
    && prospect.email_verified === true
    && prospect.do_not_contact !== true;
}

function exclusionReasons(prospect) {
  const status = String(prospect.email_status || '').toLowerCase();
  const reasons = [];
  if (!prospect.first_name) reasons.push('missing first_name');
  if (!prospect.email) reasons.push('missing email');
  if (!VERIFIED_EMAIL_STATUSES.has(status) || prospect.email_verified !== true) {
    reasons.push('email not verified');
  }
  if (prospect.do_not_contact === true) reasons.push('DNC');
  return reasons.length ? reasons : ['not renderable'];
}

function renderStep(step, prospect) {
  const subject = renderTemplate(step.subject, prospect, prospect.company_fields);
  const body = renderTemplate(step.body, prospect, prospect.company_fields);
  return {
    subject: subject.ok ? subject.output : '[render failed]',
    body: body.ok
      ? lineBreaks(body.output)
      : `Render failed: ${JSON.stringify({
        unknown: body.unknownTokens,
        missing: body.missingRequiredTokens,
      })}`,
  };
}

function renderWithFullBusinessName(step, prospect) {
  return renderTemplate(
    step.body.replace(/{{business_name_short}}/g, '{{business_name}}'),
    prospect,
    prospect.company_fields
  );
}

function renderedLinesContaining(text, needle) {
  const target = String(needle || '').trim();
  if (!target) return [];
  return String(text || '')
    .split('\n')
    .map(line => line.trim())
    .filter(line => line.includes(target));
}

function brevoSummary(brevoState) {
  const domainOk = brevoState.domain?.verified === true && brevoState.domain?.authenticated === true;
  const senderOk = brevoState.sender?.active === true;
  if (domainOk && senderOk) {
    return 'Brevo domain auth and sender checks passed during this render.';
  }
  if (domainOk && !senderOk) {
    return 'Brevo domain auth passed during this render, but `brevo_sender_active` did not pass.';
  }
  const errors = brevoState.errors?.length ? ` Errors: ${brevoState.errors.join('; ')}` : '';
  return `Brevo readiness did not fully pass during this render.${errors}`;
}

async function main() {
  await ensureBusinessNameShortColumns(pool);
  const clientResult = await pool.query(`
    SELECT id, name, business_name, active, enabled_agents,
           sender_email, sender_name, sending_domain
    FROM clients
    WHERE id = $1
  `, [CLIENT_ID]);
  const client = clientResult.rows[0];
  if (!client) throw new Error(`Client ${CLIENT_ID} not found`);
  const brevoState = await getBrevoState(client);

  const companyResult = await pool.query(`
    SELECT id, name, business_name_short, business_name_short_confidence, business_name_short_flags
    FROM companies
    WHERE client_id = $1
    ORDER BY name ASC, id ASC
  `, [CLIENT_ID]);

  const prospectResult = await pool.query(`
    SELECT p.*, row_to_json(c) AS company_fields, c.name AS company
    FROM prospects p
    LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
    WHERE p.client_id = $1
      AND COALESCE(p.setter_visible, false) = true
    ORDER BY p.id
  `, [CLIENT_ID]);

  const prospects = prospectResult.rows;
  const renderable = prospects.filter(canRenderForReview);
  const excluded = prospects.filter(p => !renderable.some(r => r.id === p.id));

  const out = [];
  out.push('# Anchor Cleaning Email Templates: Real Prospect Review');
  out.push('');
  out.push('Status: **Mapped for client 10 review; Emmett still disabled**');
  out.push('');
  out.push('Generated: 2026-06-29');
  out.push('');
  out.push('No email was sent. These samples were rendered offline through `utils/templateMerge.renderTemplate()` using live client 10 prospect/company rows. The selector now maps `client_id=10` verticals in `utils/sendingReadiness.js`: `law_firm -> anchor_law_firm_draft`, `accounting -> anchor_accounting_draft`.');
  out.push('');
  out.push(`Client 10 currently has \`enabled_agents = [${client.enabled_agents.join(', ')}]\`, so Emmett remains disabled. The readiness gate also still blocks live sending unless every check passes. ${brevoSummary(brevoState)}`);
  out.push('');
  out.push('## Render set');
  out.push('');
  out.push(`Rendered ${renderable.length} real prospects with real first names, verified email, mapped vertical, and non-DNC status. Evaluated ${prospects.length} setter-visible client 10 prospects with no LIMIT.`);
  out.push('');

  out.push('## Client 10 business-name short values');
  out.push('');
  out.push('All client 10 companies were evaluated with no LIMIT.');
  out.push('');
  for (const company of companyResult.rows) {
    const flags = company.business_name_short_flags?.length ? `; flags: ${company.business_name_short_flags.join(', ')}` : '';
    out.push(`- ${company.name} -> ${company.business_name_short || company.name} (${company.business_name_short_confidence || 'fallback'}${flags})`);
  }
  out.push('');

  out.push('## In-sentence before/after checks');
  out.push('');
  out.push('Before uses the prior full-name merge. After uses `business_name_short`.');
  out.push('');
  for (const prospect of renderable) {
    const sequenceName = CLIENT_SEQUENCE_MAP[CLIENT_ID][prospect.vertical];
    const sequence = ANCHOR_DRAFT_SEQUENCES[sequenceName];
    const shortName = prospect.company_fields?.business_name_short || prospect.company;
    const comparisons = [];
    for (const step of sequence) {
      if (!step.body.includes('{{business_name_short}}')) continue;
      const before = renderWithFullBusinessName(step, prospect);
      const after = renderStep(step, prospect);
      comparisons.push({
        day: step.day,
        before: before.ok ? renderedLinesContaining(before.output, prospect.company) : [],
        after: renderedLinesContaining(after.body.replace(/  \n/g, '\n'), shortName),
      });
    }
    out.push(`### ${prospect.company}`);
    out.push('');
    out.push(`Short name: ${shortName}`);
    out.push('');
    for (const comparison of comparisons) {
      out.push(`Day ${comparison.day}:`);
      for (const line of comparison.before) out.push(`- Before: ${line}`);
      for (const line of comparison.after) out.push(`- After: ${line}`);
    }
    out.push('');
  }

  for (const prospect of renderable) {
    const sequenceName = CLIENT_SEQUENCE_MAP[CLIENT_ID][prospect.vertical];
    const sequence = ANCHOR_DRAFT_SEQUENCES[sequenceName];
    out.push(`## ${prospect.company}`);
    out.push('');
    out.push(`Prospect: ${displayName(prospect)}`);
    out.push(`Email: ${prospect.email}`);
    out.push(`Vertical: ${prospect.vertical}`);
    out.push(`Business name short: ${prospect.company_fields?.business_name_short || prospect.company}`);
    out.push(`Sequence: ${sequenceName}`);
    out.push('Readiness note: renderable, but not sendable while Brevo sender is inactive and Emmett is not enabled.');
    out.push('');

    for (const step of sequence) {
      const rendered = renderStep(step, prospect);
      out.push(`### Day ${step.day}`);
      out.push('');
      out.push(`**Subject:** ${rendered.subject}`);
      out.push('');
      out.push(rendered.body);
      out.push('');
    }
  }

  out.push('## Evaluated but not rendered');
  out.push('');
  out.push('These setter-visible client 10 prospects were evaluated with no LIMIT, but were not included in the real-email render set because live template/send prerequisites are incomplete.');
  out.push('');
  for (const prospect of excluded) {
    out.push(`- ${prospect.company}: ${exclusionReasons(prospect).join(', ')}`);
  }
  out.push('');
  out.push('## Approval checklist');
  out.push('');
  out.push('- [ ] Real-prospect law firm sequence reviewed.');
  out.push('- [ ] Day 13 law-firm subject approved: `a reliable backup, on file`.');
  out.push('- [ ] Accounting Day 0 copy approved: `worth 10 minutes?`.');
  out.push('- [ ] Recipient business-name mentions feel natural and do not repeat within an email.');
  out.push('- [ ] Anchor service/accountability claims approved.');
  out.push('- [ ] Signature email and phone approved.');
  out.push('- [ ] Offline render approved.');
  out.push('');
  out.push('Approval of this document does not activate Emmett. Client 10 still has only Scout enabled, and live sending remains blocked by the Emmett readiness gate until the client agent config and Brevo sender checks are explicitly cleared.');
  out.push('');

  fs.writeFileSync(OUTPUT_PATH, out.join('\n'));
  console.log(`Wrote ${OUTPUT_PATH}`);
  console.log(`Rendered ${renderable.length} of ${prospects.length} setter-visible prospects`);
}

main()
  .catch(err => {
    console.error(err.stack || err.message);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
