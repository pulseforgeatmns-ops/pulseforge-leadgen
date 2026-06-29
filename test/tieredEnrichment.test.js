const assert = require('assert');
const pool = require('../db');
const {
  _test: {
    buildEmailCandidates,
    deriveNameFromVerifiedEmail,
    emailMatchesName,
    extractEmailsFromHtml,
    extractNamesFromText,
    parseNameFromCompany,
    passesDataBar,
  },
} = require('../tieredEnrichmentAgent');

async function run() {
  assert.deepStrictEqual(parseNameFromCompany('Kelly Patrick J CPA'), {
    first_name: 'Patrick',
    last_name: 'Kelly',
    full_name: 'Patrick J Kelly',
    tier: 0,
    confidence: 0.72,
    source: 'company_name_reversed',
    role: null,
  });

  const joseph = parseNameFromCompany('Attorney Joseph Kelly Levasseur, PLLC');
  assert.strictEqual(joseph.first_name, 'Joseph');
  assert.strictEqual(joseph.last_name, 'Levasseur');
  assert.strictEqual(parseNameFromCompany('George T. Campbell, Attorney at Law').first_name, 'George');
  assert.strictEqual(parseNameFromCompany('Ward Law Group, PLLC'), null);
  assert.strictEqual(parseNameFromCompany('Cohen & Winters, PLLC'), null);
  assert.strictEqual(parseNameFromCompany('Horn Wright, LLP'), null);
  assert.strictEqual(parseNameFromCompany('Morrison Mahoney LLP'), null);

  const html = `
    <a href="mailto:jane.smith@examplelaw.com">Jane</a>
    <a href="mailto:info@examplelaw.com">Info</a>
    <img src="avatar@2x.png">
  `;
  const emails = extractEmailsFromHtml(html, 'examplelaw.com');
  assert.deepStrictEqual(emails.map(item => item.email).sort(), ['info@examplelaw.com', 'jane.smith@examplelaw.com']);

  const names = extractNamesFromText('Jane Smith, Managing Partner. Pat Lee is the office manager.', 'test_page');
  assert(names.some(name => name.first_name === 'Jane' && name.last_name === 'Smith'));
  assert(!extractNamesFromText('The personal injury attorney you trust is here to help.', 'test_page')
    .some(name => name.full_name === 'You Trust'));
  assert(!extractNamesFromText('Sheehan.com named Lambert an attorney award winner.', 'test_page')
    .some(name => name.full_name === 'Sheehan.Com Lambert'));
  assert(!extractNamesFromText('Award NHAJ attorney profile and association listing.', 'test_page')
    .some(name => name.full_name === 'Award Nhaj'));
  assert(!extractNamesFromText('Michael elected attorney and team founding story.', 'test_page')
    .some(name => name.full_name === 'Michael Elected'));

  const candidates = buildEmailCandidates({
    existingEmail: '',
    foundEmails: [{ email: 'jane.smith@examplelaw.com', tier: 1, source: 'website_email', confidence: 0.86 }],
    names: [{ first_name: 'Jane', last_name: 'Smith', confidence: 0.86 }],
    domain: 'examplelaw.com',
  });
  assert(candidates.some(candidate => candidate.email === 'jane.smith@examplelaw.com'));
  assert(candidates.some(candidate => candidate.email === 'jane@examplelaw.com'));
  assert.strictEqual(emailMatchesName('ktrudel@sheehan.com', { first_name: 'Sheehan', last_name: 'Lambert' }), false);
  assert.strictEqual(emailMatchesName('info@nhattorney.com', { first_name: 'Normand', last_name: 'Higham' }), true);
  assert.strictEqual(emailMatchesName('jane.smith@examplelaw.com', { first_name: 'Jane', last_name: 'Smith' }), true);

  const verifiedEmailRow = email => ({
    first_name: '',
    last_name: '',
    email,
    email_status: 'valid',
    email_verification_method: 'bouncer',
  });
  assert.deepStrictEqual(deriveNameFromVerifiedEmail(verifiedEmailRow('michaila@manningzimmermanlaw.com')), {
    tier: 0,
    source: 'tier0_email_localpart',
    email: 'michaila@manningzimmermanlaw.com',
    first_name: 'Michaila',
    last_name: null,
    full_name: 'Michaila',
    confidence: 0.82,
    reason: 'known_first_name_localpart',
  });
  assert.strictEqual(deriveNameFromVerifiedEmail(verifiedEmailRow('info@examplelaw.com')), null);
  assert.strictEqual(deriveNameFromVerifiedEmail(verifiedEmailRow('vdaharpa@att.net')).rejected, true);
  assert.strictEqual(deriveNameFromVerifiedEmail(verifiedEmailRow('vdaharpa@att.net')).reason, 'professional_or_firm_suffix');
  assert.strictEqual(deriveNameFromVerifiedEmail(verifiedEmailRow('ktrudel@sheehan.com')).rejected, true);
  assert.deepStrictEqual(deriveNameFromVerifiedEmail(verifiedEmailRow('ktrudel@sheehan.com'), [
    { first_name: 'Katherine', last_name: 'Trudel', full_name: 'Katherine Trudel', confidence: 0.8 },
  ]), {
    tier: 0,
    source: 'tier0_email_localpart',
    email: 'ktrudel@sheehan.com',
    first_name: 'Katherine',
    last_name: 'Trudel',
    full_name: 'Katherine Trudel',
    confidence: 0.88,
    reason: 'first_initial_last_name_match',
    matched_candidate: 'Katherine Trudel',
  });
  assert.strictEqual(deriveNameFromVerifiedEmail(verifiedEmailRow('jward@wardlawnh.com'), [
    { first_name: 'John', last_name: 'Ward', full_name: 'John Ward', confidence: 0.8 },
  ]).first_name, 'John');

  assert.strictEqual(passesDataBar({
    first_name: 'Jane',
    email: 'jane@examplelaw.com',
    email_status: 'valid',
    email_verification_method: 'bouncer',
  }), true);
  assert.strictEqual(passesDataBar({
    first_name: 'Jane',
    email: 'jane@examplelaw.com',
    email_status: 'valid',
    email_verification_method: 'mx_lookup',
  }), false);

  console.log('tieredEnrichment tests passed');
}

run()
  .catch(err => {
    console.error(err);
    process.exitCode = 1;
  })
  .finally(async () => {
    await pool.end();
  });
