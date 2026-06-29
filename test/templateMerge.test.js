const assert = require('assert');
const {
  inspectSequenceTemplates,
  renderTemplate,
} = require('../utils/templateMerge');

const fullProspect = {
  first_name: 'Avery',
  email: 'avery@example.org',
  phone: '',
  company_fields: { name: 'Avery Legal', industry: null },
};

let result = renderTemplate('Hi {{first_name}}', fullProspect);
assert.strictEqual(result.ok, true);
assert.strictEqual(result.output, 'Hi Avery');

result = renderTemplate('Hi {{first_name}}', { ...fullProspect, first_name: '' });
assert.strictEqual(result.ok, false);
assert.deepStrictEqual(result.missingRequiredTokens, ['first_name']);

result = renderTemplate('Call {{phone}}', fullProspect);
assert.strictEqual(result.ok, false);
assert.deepStrictEqual(result.missingRequiredTokens, ['phone']);

result = renderTemplate('{{email|the team}}', fullProspect);
assert.strictEqual(result.ok, true);
assert.strictEqual(result.output, 'avery@example.org');

result = renderTemplate('{{phone|the team}}', fullProspect);
assert.strictEqual(result.ok, true);
assert.strictEqual(result.output, 'the team');

result = renderTemplate('{{pratice_area}}', fullProspect);
assert.strictEqual(result.ok, false);
assert.deepStrictEqual(result.unknownTokens, ['pratice_area']);

result = renderTemplate('{{}}', fullProspect);
assert.strictEqual(result.ok, false);
assert.deepStrictEqual(result.unknownTokens, ['']);

result = renderTemplate('{{first_name', fullProspect);
assert.strictEqual(result.ok, false);
assert.deepStrictEqual(result.unknownTokens, ['malformed_token_syntax']);

result = renderTemplate('{{industry|firms like yours}}', fullProspect);
assert.strictEqual(result.ok, true);
assert.strictEqual(result.output, 'firms like yours');

const sequenceInspection = inspectSequenceTemplates([
  { subject: '{{business_name}}', body: 'Hi {{first_name}}, {{industry|firms like yours}}' },
], { ...fullProspect, first_name: '' });
assert.deepStrictEqual(sequenceInspection.unknownTokens, []);
assert.deepStrictEqual(sequenceInspection.missingRequiredTokens, ['first_name']);

console.log('templateMerge tests passed');
