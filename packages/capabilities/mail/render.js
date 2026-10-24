'use strict';

/**
 * Printable HTML for mail packages (SPEC-033).
 * Individual letter/envelope/summary + combined campaign document (PDF path).
 */

/**
 * @param {object} pkg - MailPackage
 * @returns {{ letterHtml: string, envelopeHtml: string, summaryHtml: string, packageHtml: string }}
 */
function renderMailPackageHtml(pkg) {
  const letterHtml = renderLetterHtml(pkg);
  const envelopeHtml = renderEnvelopeHtml(pkg);
  const summaryHtml = renderSummaryHtml(pkg);
  const packageHtml = wrapDocument(
    `Mail Package — ${escapeHtml((pkg.letter && pkg.letter.companyName) || '')}`,
    `${letterHtml}${envelopeHtml}${summaryHtml}${renderChecklistHtml(pkg)}`
  );
  return { letterHtml, envelopeHtml, summaryHtml, packageHtml };
}

/**
 * Combined campaign printable HTML (Campaign PDF artifact path).
 * @param {object[]} packages
 * @param {object} [meta]
 * @returns {string}
 */
function renderCampaignHtml(packages = [], meta = {}) {
  const title = escapeHtml(meta.campaignName || 'Campaign Mail Packages');
  const summary = meta.campaignSummary || {};
  const header = `
    <section class="campaign-header">
      <h1>${title}</h1>
      <p class="meta">Revision ${escapeHtml(String(meta.revision || 1))} · ${escapeHtml(meta.generatedAt || '')}</p>
      <ul class="counts">
        <li>Prospects: <strong>${Number(summary.prospects) || 0}</strong></li>
        <li>Ready to Print: <strong>${Number(summary.readyToPrint) || 0}</strong></li>
        <li>Needs Review: <strong>${Number(summary.needsReview) || 0}</strong></li>
        <li>Missing Addresses: <strong>${Number(summary.missingAddresses) || 0}</strong></li>
        <li>Est. Print: <strong>${escapeHtml(summary.estimatedPrintTimeLabel || '—')}</strong></li>
        <li>Est. Assembly: <strong>${escapeHtml(summary.estimatedAssemblyTimeLabel || '—')}</strong></li>
      </ul>
    </section>`;

  const bodies = packages
    .filter((p) => p.status !== 'skipped')
    .map((pkg, i) => {
      const rendered = renderMailPackageHtml(pkg);
      const status = escapeHtml(pkg.status || '');
      return `
        <article class="pkg" data-status="${status}">
          <header class="pkg-banner">
            <h2>${i + 1}. ${escapeHtml((pkg.letter && pkg.letter.companyName) || 'Prospect')}</h2>
            <span class="status status-${status}">${status.replace(/_/g, ' ')}</span>
          </header>
          ${rendered.letterHtml}
          ${rendered.envelopeHtml}
          ${rendered.summaryHtml}
          ${renderChecklistHtml(pkg)}
        </article>`;
    })
    .join('\n<div class="page-break"></div>\n');

  return wrapDocument(title, `${header}${bodies}`, { campaign: true });
}

/**
 * @param {object} pkg
 * @returns {string}
 */
function renderLetterHtml(pkg) {
  const letter = pkg.letter || {};
  return `
    <section class="letter page-block">
      <h3>Letter</h3>
      <div class="letter-body">
        <pre class="letter-text">${escapeHtml(letter.body || '')}</pre>
      </div>
    </section>`;
}

/**
 * @param {object} pkg
 * @returns {string}
 */
function renderEnvelopeHtml(pkg) {
  const env = pkg.envelope || {};
  return `
    <section class="envelope page-block">
      <h3>Envelope</h3>
      <div class="envelope-grid">
        <div class="return">
          <div class="label">Return</div>
          <pre>${escapeHtml(env.returnAddress || '')}</pre>
        </div>
        <div class="recipient">
          <div class="label">Recipient</div>
          <p>${escapeHtml(env.recipientName || '')}</p>
          <p>${escapeHtml(env.companyName || '')}</p>
          <pre>${escapeHtml(env.mailingAddress || '')}</pre>
        </div>
      </div>
    </section>`;
}

/**
 * @param {object} pkg
 * @returns {string}
 */
function renderSummaryHtml(pkg) {
  const s = pkg.personalizationSummary || {};
  const facts = (s.personalizationFacts || [])
    .map((f) => `<li>${escapeHtml(f)}</li>`)
    .join('');
  const warnings = (s.missingDataWarnings || [])
    .map((w) => `<li class="warn">${escapeHtml(w)}</li>`)
    .join('');
  return `
    <section class="summary page-block">
      <h3>Personalization Summary</h3>
      <p><strong>Why selected:</strong> ${escapeHtml(s.whySelected || '')}</p>
      <p><strong>Letter confidence:</strong> ${Number(s.letterConfidence || 0).toFixed(2)}</p>
      <div class="facts">
        <div class="label">Personalization facts used</div>
        <ul>${facts || '<li>None recorded</li>'}</ul>
      </div>
      <div class="warnings">
        <div class="label">Missing data warnings</div>
        <ul>${warnings || '<li>None</li>'}</ul>
      </div>
    </section>`;
}

/**
 * @param {object} pkg
 * @returns {string}
 */
function renderChecklistHtml(pkg) {
  const items = (pkg.insertChecklist || [])
    .map((item) => {
      const mark = item.included !== false ? '☑' : '☐';
      const req = item.required ? ' (required)' : '';
      return `<li>${mark} ${escapeHtml(item.label || item.id)}${escapeHtml(req)}</li>`;
    })
    .join('');
  return `
    <section class="checklist page-block">
      <h3>Insert Checklist</h3>
      <ul>${items}</ul>
    </section>`;
}

/**
 * @param {string} title
 * @param {string} body
 * @param {object} [opts]
 * @returns {string}
 */
function wrapDocument(title, body, opts = {}) {
  return `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${title}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@400;500;600;700&family=Source+Serif+4:opsz,wght@8..60,600;8..60,700&display=swap" rel="stylesheet" />
  <style>
    :root {
      --ink: #1c1917;
      --muted: #57534e;
      --line: #d6d3d1;
      --page: #fffdf8;
      --accent: #0f766e;
      --warn: #b45309;
      --ready: #047857;
      --review: #b45309;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: #e7e5e4;
      color: var(--ink);
      font-family: "Source Sans 3", "Segoe UI", sans-serif;
      line-height: 1.5;
      -webkit-font-smoothing: antialiased;
    }
    .sheet {
      width: min(816px, 100%);
      margin: 24px auto;
      background: var(--page);
      padding: 40px 48px 52px;
    }
    h1, h2, h3 {
      font-family: "Source Serif 4", Georgia, serif;
      color: #1c1917;
      letter-spacing: -0.01em;
    }
    h1 { font-size: 28px; margin-bottom: 8px; }
    h2 { font-size: 20px; }
    h3 { font-size: 16px; margin-bottom: 10px; color: var(--accent); }
    .meta { color: var(--muted); font-size: 13px; margin-bottom: 16px; }
    .counts { list-style: none; display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 6px 18px; margin: 16px 0 28px; font-size: 14px; }
    .page-block { margin-top: 22px; padding-top: 18px; border-top: 1px solid var(--line); }
    .letter-text, .envelope pre, .return pre { white-space: pre-wrap; font-family: "Source Serif 4", Georgia, serif; font-size: 15px; }
    .envelope-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 24px; margin-top: 8px; }
    .label { font-size: 11px; text-transform: uppercase; letter-spacing: 0.06em; color: var(--muted); margin-bottom: 6px; }
    .facts ul, .warnings ul, .checklist ul { padding-left: 1.1rem; font-size: 14px; }
    .warn { color: var(--warn); }
    .pkg-banner { display: flex; justify-content: space-between; align-items: baseline; gap: 12px; margin: 8px 0 4px; }
    .status { font-size: 12px; text-transform: uppercase; letter-spacing: 0.04em; }
    .status-ready_to_print, .status-approved { color: var(--ready); }
    .status-needs_review { color: var(--review); }
    .page-break { height: 0; }
    ${opts.campaign ? '.pkg { margin-top: 28px; }' : ''}
    @media print {
      body { background: white; }
      .sheet { margin: 0; width: 100%; box-shadow: none; }
      .page-break { break-after: page; page-break-after: always; }
      .pkg { break-inside: avoid; }
    }
  </style>
</head>
<body>
  <div class="sheet">
    ${body}
  </div>
</body>
</html>`;
}

/**
 * @param {string} value
 * @returns {string}
 */
function escapeHtml(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

module.exports = {
  renderMailPackageHtml,
  renderCampaignHtml,
  renderLetterHtml,
  renderEnvelopeHtml,
  renderSummaryHtml,
  renderChecklistHtml,
  escapeHtml,
};
