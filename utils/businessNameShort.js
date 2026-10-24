const LEGAL_SUFFIX_PATTERN = /(?:[\s,]+(?:p\.?\s*l\.?\s*l\.?\s*c\.?|l\.?\s*l\.?\s*c\.?|l\.?\s*l\.?\s*p\.?|p\.?\s*a\.?|p\.?\s*c\.?|inc\.?|co\.?|corp\.?))+$/i;
const LAW_PREFIX_PATTERN = /^(?:(?:the\s+)?law\s+offices?\s+of|offices?\s+of)\s+/i;
const TRAILING_DESCRIPTOR_PATTERNS = [
  { label: 'injury_lawyers', pattern: /(?:[\s,]+injury\s+lawyers)$/i },
];

function cleanName(value) {
  return String(value || '')
    .replace(/\s+/g, ' ')
    .replace(/^[\s,]+|[\s,]+$/g, '')
    .trim();
}

function stripTrailingPunctuation(value) {
  return cleanName(value).replace(/[\s,.;:]+$/g, '').trim();
}

function isOddShortName(value) {
  const cleaned = stripTrailingPunctuation(value);
  return !/[A-Za-z0-9]/.test(cleaned)
    || /^[&.,;:/\\-]/.test(cleaned)
    || /[&/\\-]$/.test(cleaned);
}

function deriveBusinessNameShort(name) {
  const original = cleanName(name);
  const flags = [];
  const stripped = [];
  if (!original) {
    return {
      business_name_short: '',
      confidence: 'fallback',
      flags: ['empty_input'],
      stripped,
    };
  }

  let candidate = original;
  const withoutPrefix = candidate.replace(LAW_PREFIX_PATTERN, '');
  if (withoutPrefix !== candidate) {
    candidate = withoutPrefix;
    stripped.push('leading_law_office_prefix');
  }

  let withoutSuffix = candidate.replace(LEGAL_SUFFIX_PATTERN, '');
  while (withoutSuffix !== candidate) {
    candidate = stripTrailingPunctuation(withoutSuffix);
    stripped.push('legal_suffix');
    withoutSuffix = candidate.replace(LEGAL_SUFFIX_PATTERN, '');
  }

  for (const descriptor of TRAILING_DESCRIPTOR_PATTERNS) {
    const withoutDescriptor = candidate.replace(descriptor.pattern, '');
    if (withoutDescriptor !== candidate) {
      candidate = stripTrailingPunctuation(withoutDescriptor);
      stripped.push(`descriptor:${descriptor.label}`);
      flags.push('low_confidence_descriptor_stripped');
    }
  }

  candidate = stripTrailingPunctuation(candidate);
  if (!candidate || isOddShortName(candidate)) {
    return {
      business_name_short: original,
      confidence: 'fallback',
      flags: [...new Set([...flags, 'fallback_odd_result'])],
      stripped,
    };
  }

  return {
    business_name_short: candidate,
    confidence: flags.length ? 'low' : 'high',
    flags: [...new Set(flags)],
    stripped,
  };
}

// Share pending work only on the same query object. A client can be inside a
// transaction whose successful DDL is later rolled back; settled success is never
// a process-wide (or even client-lifetime) statement of committed schema readiness.
const pendingBusinessNameShortColumns = new WeakMap();

function ensureBusinessNameShortColumns(db) {
  if (!pendingBusinessNameShortColumns.has(db)) {
    const pending = Promise.resolve().then(async () => {
      // Resolve the visible relation, including caller-owned transactions. Pools must
      // use a consistent search_path across connections for this and the ALTER.
      // Avoid ALTER's exclusive lock on a ready schema.
      const { rows } = await db.query(`
        SELECT count(*) = 3 AS ready
        FROM pg_catalog.pg_attribute
        WHERE attrelid = to_regclass('companies')
          AND NOT attisdropped
          AND attname IN ('business_name_short', 'business_name_short_confidence', 'business_name_short_flags')
      `);
      if (rows[0]?.ready) return;
      await db.query(`
        ALTER TABLE companies
          ADD COLUMN IF NOT EXISTS business_name_short TEXT,
          ADD COLUMN IF NOT EXISTS business_name_short_confidence TEXT,
          ADD COLUMN IF NOT EXISTS business_name_short_flags TEXT[] DEFAULT ARRAY[]::TEXT[]
      `);
    }).finally(() => {
      pendingBusinessNameShortColumns.delete(db);
    });
    pendingBusinessNameShortColumns.set(db, pending);
  }
  return pendingBusinessNameShortColumns.get(db);
}

module.exports = {
  deriveBusinessNameShort,
  ensureBusinessNameShortColumns,
};
