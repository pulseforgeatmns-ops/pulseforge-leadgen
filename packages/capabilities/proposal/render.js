'use strict';

/**
 * Web + printable HTML renderer for proposals (SPEC-027B).
 */

/**
 * @param {object} document - ProposalDocument
 * @param {object} [opts]
 * @returns {{ html: string, printableHtml: string }}
 */
function renderProposalHtml(document, opts = {}) {
  const title = escapeHtml(document.title || 'Commercial Growth Proposal');
  const preparedFor = escapeHtml(document.preparedFor || '');
  const sectionsHtml = (document.sections || [])
    .map((s) => renderSection(s))
    .join('\n');

  const pricingNote = document.pricing
    ? `<p class="pricing-label">${escapeHtml(document.pricing.label)}</p>`
    : '';

  const flow = (document.nextStepsFlow || [])
    .map((step, i, arr) => {
      const arrow = i < arr.length - 1 ? '<div class="flow-arrow">↓</div>' : '';
      return `<div class="flow-step">${escapeHtml(step)}</div>${arrow}`;
    })
    .join('\n');

  const body = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>${title} — ${preparedFor}</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Source+Sans+3:wght@400;500;600;700&family=Source+Serif+4:opsz,wght@8..60,600;8..60,700&display=swap" rel="stylesheet" />
  <style>
    :root {
      --navy: #0f2744;
      --teal: #0d9488;
      --ink: #1e293b;
      --muted: #475569;
      --line: #e2e8f0;
      --page: #ffffff;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      background: #f1f5f9;
      color: var(--ink);
      font-family: "Source Sans 3", "Segoe UI", sans-serif;
      line-height: 1.55;
      -webkit-font-smoothing: antialiased;
    }
    .page {
      width: min(816px, 100%);
      margin: 24px auto;
      background: var(--page);
      padding: 48px 52px 56px;
    }
    .cover h1 {
      font-family: "Source Serif 4", Georgia, serif;
      font-size: 32px;
      color: var(--navy);
      margin-bottom: 28px;
      letter-spacing: -0.01em;
    }
    .cover .meta { color: var(--muted); font-size: 15px; white-space: pre-line; }
    .section { margin-top: 36px; padding-top: 28px; border-top: 1px solid var(--line); }
    .section h2 {
      font-family: "Source Serif 4", Georgia, serif;
      font-size: 20px;
      color: var(--navy);
      margin-bottom: 12px;
    }
    .section p { color: var(--ink); font-size: 15px; margin-bottom: 12px; white-space: pre-line; }
    .section ul { padding-left: 1.15rem; color: var(--ink); font-size: 15px; }
    .section li { margin-bottom: 6px; }
    .uncertain { border-left: 3px solid #ca8a04; padding-left: 12px; }
    .flow { margin-top: 12px; }
    .flow-step {
      font-weight: 600;
      color: var(--navy);
      font-size: 15px;
    }
    .flow-arrow { color: var(--teal); margin: 4px 0 4px 8px; }
    .pricing-label { color: var(--teal); font-weight: 600; font-size: 13px; letter-spacing: 0.04em; text-transform: uppercase; margin-bottom: 8px; }
    .footer {
      margin-top: 48px;
      padding-top: 16px;
      border-top: 1px solid var(--line);
      font-size: 12px;
      color: var(--muted);
    }
    @media print {
      body { background: #fff; }
      .page { margin: 0; width: 100%; padding: 24px 28px; }
    }
  </style>
</head>
<body>
  <article class="page" id="proposal">
    <header class="cover">
      <h1>${title}</h1>
      ${pricingNote}
      <div class="meta">Prepared for:
${preparedFor}

Prepared by:
${escapeHtml(document.preparedBy || 'Pulseforge')}</div>
    </header>
    ${sectionsHtml}
    <section class="section">
      <h2>Path Forward</h2>
      <div class="flow">${flow}</div>
    </section>
    <footer class="footer">Confidential — prepared by Pulseforge for ${preparedFor}. Review required before client delivery.</footer>
  </article>
</body>
</html>`;

  return {
    html: body,
    printableHtml: body,
    sharePath: opts.sharePath || null,
  };
}

function renderSection(section) {
  if (section.id === 'cover') return ''; // cover rendered in header
  const uncertain = section.uncertain ? ' uncertain' : '';
  const bullets =
    section.bullets && section.bullets.length
      ? `<ul>${section.bullets.map((b) => `<li>${escapeHtml(b)}</li>`).join('')}</ul>`
      : '';
  return `<section class="section${uncertain}" data-section="${escapeHtml(section.id)}">
  <h2>${escapeHtml(section.title)}</h2>
  <p>${escapeHtml(section.body)}</p>
  ${bullets}
</section>`;
}

function escapeHtml(value) {
  return String(value || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

module.exports = {
  renderProposalHtml,
  escapeHtml,
};
