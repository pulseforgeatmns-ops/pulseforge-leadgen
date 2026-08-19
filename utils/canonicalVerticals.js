'use strict';

/**
 * PEC-116 — Canonical business vertical registry.
 *
 * A client's business vertical is what they sell (commercial cleaning, coaching,
 * legal services, etc.). ICP / target segments live in Client Intelligence and AIM.
 *
 * This module is the single source of truth for tenant onboarding selectors,
 * server validation, and the clients.vertical CHECK constraint.
 */

const { normalizeVertical } = require('./normalize');

/** @type {ReadonlyArray<{ value: string, label: string }>} */
const CANONICAL_BUSINESS_VERTICALS = Object.freeze([
  { value: 'accounting', label: 'Accounting' },
  { value: 'architecture_engineering', label: 'Architecture & Engineering' },
  { value: 'auto', label: 'Auto Repair' },
  { value: 'business_coaching', label: 'Business Coaching' },
  { value: 'cleaning', label: 'Cleaning' },
  { value: 'commercial_cleaning', label: 'Commercial Cleaning' },
  { value: 'commercial_hvac', label: 'Commercial HVAC' },
  { value: 'commercial_insurance', label: 'Commercial Insurance' },
  { value: 'commercial_landscaping', label: 'Commercial Landscaping' },
  { value: 'commercial_roofing', label: 'Commercial Roofing' },
  { value: 'dental', label: 'Dental' },
  { value: 'electrical', label: 'Electrical' },
  { value: 'equipment_rental', label: 'Equipment Rental' },
  { value: 'facility_services', label: 'Facility Services' },
  { value: 'fitness', label: 'Fitness' },
  { value: 'freight_brokerage', label: 'Freight Brokerage' },
  { value: 'home_renovation', label: 'Home Renovation' },
  { value: 'home_services', label: 'Home Services' },
  { value: 'hvac', label: 'HVAC' },
  { value: 'janitorial', label: 'Janitorial' },
  { value: 'landscaping', label: 'Landscaping' },
  { value: 'legal', label: 'Legal' },
  { value: 'marketing_automation', label: 'Marketing Automation' },
  { value: 'med_spa', label: 'Med Spa' },
  { value: 'msp_it_services', label: 'IT / Managed Services' },
  { value: 'pest_control', label: 'Pest Control' },
  { value: 'plumbing', label: 'Plumbing' },
  { value: 'property_management', label: 'Property Management' },
  { value: 'restaurant', label: 'Restaurant' },
  { value: 'restoration', label: 'Restoration' },
  { value: 'roofing', label: 'Roofing' },
  { value: 'salon', label: 'Salon' },
  { value: 'staffing_recruiting', label: 'Staffing & Recruiting' },
  { value: 'wholesale_distribution', label: 'Wholesale Distribution' },
]);

const CANONICAL_VALUE_SET = new Set(CANONICAL_BUSINESS_VERTICALS.map((v) => v.value));
const LABEL_BY_VALUE = Object.freeze(
  CANONICAL_BUSINESS_VERTICALS.reduce((map, entry) => {
    map[entry.value] = entry.label;
    return map;
  }, {})
);

function listCanonicalBusinessVerticals() {
  return CANONICAL_BUSINESS_VERTICALS.map((entry) => ({ ...entry }));
}

function isCanonicalBusinessVertical(value) {
  const normalized = normalizeVertical(value);
  return Boolean(normalized && CANONICAL_VALUE_SET.has(normalized));
}

function labelForBusinessVertical(value) {
  const normalized = normalizeVertical(value);
  return normalized ? LABEL_BY_VALUE[normalized] || null : null;
}

function unsupportedVerticalMessage(rawValue) {
  const text = String(rawValue == null ? '' : rawValue).trim();
  const label = labelForBusinessVertical(text) || text || 'That selection';
  return `"${label}" is not currently a supported business vertical.`;
}

function assertCanonicalBusinessVertical(rawValue) {
  const normalized = normalizeVertical(rawValue);
  if (!normalized) {
    const err = new Error('Business vertical is required');
    err.code = 'tenant_validation';
    err.status = 400;
    err.missing = ['vertical'];
    throw err;
  }
  if (!CANONICAL_VALUE_SET.has(normalized)) {
    const err = new Error(unsupportedVerticalMessage(rawValue));
    err.code = 'tenant_validation';
    err.status = 400;
    throw err;
  }
  return normalized;
}

function mapVerticalConstraintError(err) {
  if (err && err.code === '23514' && /vertical_canonical_chk/.test(err.constraint || '')) {
    const mapped = new Error('The selected business vertical is not currently supported.');
    mapped.code = 'tenant_validation';
    mapped.status = 400;
    return mapped;
  }
  return err;
}

function sqlInList(values) {
  return values.map((value) => `'${String(value).replace(/'/g, "''")}'`).join(', ');
}

async function ensureCanonicalVerticalConstraint(pool) {
  const allowed = CANONICAL_BUSINESS_VERTICALS.map((entry) => entry.value);
  await pool.query(`
    DO $$
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'clients_vertical_canonical_chk'
          AND conrelid = 'clients'::regclass
      ) THEN
        ALTER TABLE clients ADD CONSTRAINT clients_vertical_canonical_chk
          CHECK (vertical IS NULL OR vertical IN (${sqlInList(allowed)}));
      END IF;
    END $$;
  `);
}

module.exports = {
  CANONICAL_BUSINESS_VERTICALS,
  listCanonicalBusinessVerticals,
  isCanonicalBusinessVertical,
  labelForBusinessVertical,
  unsupportedVerticalMessage,
  assertCanonicalBusinessVertical,
  mapVerticalConstraintError,
  ensureCanonicalVerticalConstraint,
};
