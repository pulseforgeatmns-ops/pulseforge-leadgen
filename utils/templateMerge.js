const TOKEN_PATTERN = /{{\s*([^{}]*?)\s*}}/g;
const FIELD_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]*$/;
const { deriveBusinessNameShort } = require('./businessNameShort');

function isEmptyTokenValue(value) {
  return value == null || (typeof value === 'string' && (!value.trim() || value.trim() === '—'));
}

function parseTemplateTokens(template) {
  const tokens = [];
  const source = String(template ?? '');
  for (const match of source.matchAll(TOKEN_PATTERN)) {
    const expression = match[1];
    const separator = expression.indexOf('|');
    const field = (separator === -1 ? expression : expression.slice(0, separator)).trim();
    const hasFallback = separator !== -1;
    const fallback = hasFallback ? expression.slice(separator + 1).trim() : null;
    tokens.push({
      raw: match[0],
      expression,
      field,
      fallback,
      hasFallback,
      required: !hasFallback,
      valid: FIELD_NAME_PATTERN.test(field),
      index: match.index,
    });
  }
  return tokens;
}

function legacyFirstName(prospect) {
  if (!isEmptyTokenValue(prospect?.first_name)) return prospect.first_name;
  const fromName = typeof prospect?.name === 'string' ? prospect.name.trim().split(/\s+/)[0] : '';
  return isEmptyTokenValue(fromName) ? null : fromName;
}

function legacyBusinessName(prospect, company) {
  const companyName = company?.name ?? prospect?.company;
  if (!isEmptyTokenValue(companyName)) return companyName;
  const fromNotes = typeof prospect?.notes === 'string'
    ? prospect.notes.split('—')[0].trim()
      .replace(/\.(com|net|org|io|us)$/i, '')
      .replace(/^(www\.)/i, '')
      .trim()
    : '';
  return !isEmptyTokenValue(fromNotes) && fromNotes.length >= 4 ? fromNotes : 'your business';
}

function legacyBusinessNameShort(prospect, company) {
  if (!isEmptyTokenValue(company?.business_name_short)) return company.business_name_short;
  if (!isEmptyTokenValue(prospect?.business_name_short)) return prospect.business_name_short;
  const businessName = legacyBusinessName(prospect, company);
  const derived = deriveBusinessNameShort(businessName);
  return isEmptyTokenValue(derived.business_name_short) ? businessName : derived.business_name_short;
}

function resolveTokenField(field, prospect = {}, companyFields) {
  const company = companyFields || prospect.company_fields || {};

  // Compatibility aliases preserve Emmett's pre-engine behavior. All other
  // fields are discovered directly from the selected prospect/company rows.
  if (field === 'first_name') {
    return { exists: Object.prototype.hasOwnProperty.call(prospect, 'first_name'), value: legacyFirstName(prospect) };
  }
  if (field === 'business_name') {
    return { exists: true, value: legacyBusinessName(prospect, company) };
  }
  if (field === 'business_name_short') {
    return { exists: true, value: legacyBusinessNameShort(prospect, company) };
  }
  if (Object.prototype.hasOwnProperty.call(prospect, field)) {
    return { exists: true, value: prospect[field] };
  }
  if (company && Object.prototype.hasOwnProperty.call(company, field)) {
    return { exists: true, value: company[field] };
  }
  return { exists: false, value: undefined };
}

function inspectTemplate(template, prospect = {}, companyFields) {
  const source = String(template ?? '');
  const tokens = parseTemplateTokens(template);
  const unknownTokens = [];
  const missingRequiredTokens = [];

  for (const token of tokens) {
    const resolved = token.valid
      ? resolveTokenField(token.field, prospect, companyFields)
      : { exists: false, value: undefined };
    if (!resolved.exists) {
      unknownTokens.push(token.field || token.expression);
    } else if (token.required && isEmptyTokenValue(resolved.value)) {
      missingRequiredTokens.push(token.field);
    }
  }
  const unmatchedSyntax = source.replace(TOKEN_PATTERN, '');
  if (unmatchedSyntax.includes('{{') || unmatchedSyntax.includes('}}')) {
    unknownTokens.push('malformed_token_syntax');
  }

  return {
    tokens,
    unknownTokens: [...new Set(unknownTokens)],
    missingRequiredTokens: [...new Set(missingRequiredTokens)],
  };
}

function withHouseGreetingFallback(sequence = []) {
  return sequence.map(step => ({
    ...step,
    subject: typeof step?.subject === 'string'
      ? step.subject.replaceAll('{{first_name}}', '{{first_name|}}')
      : step?.subject,
    body: typeof step?.body === 'string'
      ? step.body.replaceAll('{{first_name}}', '{{first_name|}}')
      : step?.body,
  }));
}

function renderTemplate(template, prospect = {}, companyFields) {
  const source = String(template ?? '');
  const inspection = inspectTemplate(source, prospect, companyFields);
  if (inspection.unknownTokens.length || inspection.missingRequiredTokens.length) {
    return { ok: false, output: null, ...inspection };
  }

  let cursor = 0;
  let output = '';
  for (const match of source.matchAll(TOKEN_PATTERN)) {
    const [raw, expression] = match;
    let prefix = source.slice(cursor, match.index);
    const separator = expression.indexOf('|');
    const field = (separator === -1 ? expression : expression.slice(0, separator)).trim();
    const fallback = separator === -1 ? null : expression.slice(separator + 1).trim();
    const resolved = resolveTokenField(field, prospect, companyFields);
    const usesEmptyFallback = isEmptyTokenValue(resolved.value) && fallback === '';
    if (usesEmptyFallback) prefix = prefix.replace(/[ \t]+$/, '');
    output += prefix + (isEmptyTokenValue(resolved.value) ? fallback : String(resolved.value));
    cursor = match.index + raw.length;
  }
  output += source.slice(cursor);

  return { ok: true, output, ...inspection };
}

function inspectSequenceTemplates(sequence = [], prospect = {}, companyFields) {
  const unknownTokens = new Set();
  const missingRequiredTokens = new Set();
  const tokens = [];
  for (const step of sequence) {
    for (const part of [step?.subject, step?.body]) {
      const inspection = inspectTemplate(part, prospect, companyFields);
      inspection.tokens.forEach(token => tokens.push(token));
      inspection.unknownTokens.forEach(token => unknownTokens.add(token));
      inspection.missingRequiredTokens.forEach(token => missingRequiredTokens.add(token));
    }
  }
  return {
    tokens,
    unknownTokens: [...unknownTokens],
    missingRequiredTokens: [...missingRequiredTokens],
  };
}

module.exports = {
  inspectSequenceTemplates,
  inspectTemplate,
  isEmptyTokenValue,
  parseTemplateTokens,
  renderTemplate,
  resolveTokenField,
  withHouseGreetingFallback,
};
