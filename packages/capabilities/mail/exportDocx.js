'use strict';

/**
 * Word-compatible DOCX HTML export (SPEC-033).
 * v1 emits HTML that Microsoft Word opens; full OOXML deferred.
 */

const { escapeHtml } = require('./render');

/**
 * @param {object} pkg
 * @returns {{ docxHtml: string, filename: string, format: string }}
 */
function buildPackageDocx(pkg) {
  const letter = pkg.letter || {};
  const company = letter.companyName || 'prospect';
  const safeName = String(company)
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '_')
    .replace(/^_|_$/g, '')
    .slice(0, 48);
  const html = `<!DOCTYPE html>
<html xmlns:o="urn:schemas-microsoft-com:office:office"
      xmlns:w="urn:schemas-microsoft-com:office:word"
      xmlns="http://www.w3.org/TR/REC-html40">
<head>
  <meta charset="UTF-8" />
  <title>Letter — ${escapeHtml(company)}</title>
  <!--[if gte mso 9]><xml>
    <w:WordDocument><w:View>Print</w:View></w:WordDocument>
  </xml><![endif]-->
  <style>
    body { font-family: Georgia, serif; font-size: 12pt; line-height: 1.45; color: #1c1917; }
    pre { font-family: Georgia, serif; white-space: pre-wrap; }
    h1 { font-size: 16pt; }
  </style>
</head>
<body>
  <h1>Personalized Letter — ${escapeHtml(company)}</h1>
  <pre>${escapeHtml(letter.body || '')}</pre>
</body>
</html>`;

  return {
    docxHtml: html,
    filename: `letter_${safeName || 'prospect'}.doc`,
    format: 'word_html',
  };
}

/**
 * @param {object[]} packages
 * @param {object} [meta]
 * @returns {{ docxHtml: string, filename: string, format: string }}
 */
function buildCampaignDocx(packages = [], meta = {}) {
  const name = meta.campaignName || 'campaign';
  const parts = packages
    .filter((p) => p.status !== 'skipped')
    .map((pkg, i) => {
      const letter = pkg.letter || {};
      return `<h2>${i + 1}. ${escapeHtml(letter.companyName || 'Prospect')}</h2>
<pre>${escapeHtml(letter.body || '')}</pre>
<hr />`;
    })
    .join('\n');

  const html = `<!DOCTYPE html>
<html xmlns:o="urn:schemas-microsoft-com:office:office"
      xmlns:w="urn:schemas-microsoft-com:office:word"
      xmlns="http://www.w3.org/TR/REC-html40">
<head>
  <meta charset="UTF-8" />
  <title>${escapeHtml(name)} — Letters</title>
  <style>
    body { font-family: Georgia, serif; font-size: 12pt; line-height: 1.45; }
    pre { font-family: Georgia, serif; white-space: pre-wrap; }
  </style>
</head>
<body>
  <h1>${escapeHtml(name)} — Direct Mail Letters</h1>
  ${parts}
</body>
</html>`;

  return {
    docxHtml: html,
    filename: 'campaign_letters.doc',
    format: 'word_html',
  };
}

module.exports = {
  buildPackageDocx,
  buildCampaignDocx,
};
