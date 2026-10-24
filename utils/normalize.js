function normalizeVertical(value) {
  if (value == null) return null;

  const raw = String(value).trim();
  if (!raw) return null;

  const normalized = raw
    .toLowerCase()
    .replace(/[\s.-]+/g, '_')
    .replace(/[^a-z0-9_]/g, '')
    .replace(/_+/g, '_')
    .replace(/^_+|_+$/g, '');

  return normalized || null;
}

module.exports = {
  normalizeVertical,
};
