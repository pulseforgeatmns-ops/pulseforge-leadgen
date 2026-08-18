/**
 * Anchor Cleaning website walkthrough form — validation only.
 * Keep field names in sync with sites/anchor-cleaning/index.html.
 */

const SPACE_TYPES = Object.freeze([
  'law_office',
  'accounting',
  'medical_office',
  'general_office',
  'retail',
  'other',
]);

const SPACE_TYPE_LABELS = Object.freeze({
  law_office: 'Law office',
  accounting: 'Accounting / professional office',
  medical_office: 'Medical office',
  general_office: 'General office',
  retail: 'Retail / showroom',
  other: 'Other',
});

const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;

function cleanText(value, max) {
  const text = String(value == null ? '' : value).replace(/\s+/g, ' ').trim();
  if (!text) return '';
  return text.slice(0, max);
}

function digitsOnly(value) {
  return String(value == null ? '' : value).replace(/\D/g, '');
}

function validateWalkthroughPayload(body = {}) {
  const errors = {};
  const name = cleanText(body.name, 120);
  const businessName = cleanText(body.business_name, 200);
  const email = cleanText(body.email, 320).toLowerCase();
  const phoneRaw = cleanText(body.phone, 40);
  const phoneDigits = digitsOnly(phoneRaw);
  const city = cleanText(body.city, 80);
  const spaceType = cleanText(body.space_type, 40);

  if (name.length < 2) errors.name = 'Name is required.';
  if (businessName.length < 2) errors.business_name = 'Business name is required.';
  if (!EMAIL_RE.test(email)) errors.email = 'A valid email is required.';
  if (phoneDigits.length < 10 || phoneDigits.length > 15) {
    errors.phone = 'A valid phone number is required.';
  }
  if (city.length < 2) errors.city = 'City / town is required.';
  if (!SPACE_TYPES.includes(spaceType)) errors.space_type = 'Choose a type of space.';

  if (Object.keys(errors).length) {
    return { ok: false, errors };
  }

  return {
    ok: true,
    values: {
      name,
      business_name: businessName,
      email,
      phone: phoneRaw,
      phone_digits: phoneDigits,
      city,
      space_type: spaceType,
      space_type_label: SPACE_TYPE_LABELS[spaceType],
    },
  };
}

module.exports = {
  SPACE_TYPES,
  SPACE_TYPE_LABELS,
  validateWalkthroughPayload,
};
