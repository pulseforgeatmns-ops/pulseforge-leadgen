const ASSET_EXTENSION_RE = /\.(?:webp|png|jpe?g|gif|svg|pdf)(?:$|[?#])/i;
const URL_ENCODED_RE = /%[0-9a-f]{2}/i;
const PLACEHOLDER_DOMAIN_RE = /^(?:mail\.com|example(?:\.[a-z0-9-]+)*|test(?:\.[a-z0-9-]+)*)$/i;

function normalizeEmailAddress(value) {
  return String(value || '').trim();
}

function invalidOutreachEmailReason(value) {
  const email = normalizeEmailAddress(value);
  if (!email) return 'missing_email';
  if (URL_ENCODED_RE.test(email)) return 'url_encoded_chars';
  if (/\s/.test(email)) return 'contains_spaces';
  if (ASSET_EXTENSION_RE.test(email)) return 'asset_extension';
  if ((email.match(/@/g) || []).length !== 1) return 'invalid_at_count';

  const [local, domainRaw] = email.split('@');
  const domain = String(domainRaw || '').toLowerCase();
  if (!local || !domain) return 'invalid_format';
  if (domain.startsWith('www.')) return 'leading_www_domain';
  if (domain.includes('..')) return 'malformed_domain';
  if (PLACEHOLDER_DOMAIN_RE.test(domain)) return 'placeholder_domain';
  if (!/^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?(?:\.[a-z0-9](?:[a-z0-9-]*[a-z0-9])?)+$/i.test(domain)) {
    return 'malformed_domain';
  }
  if (!/\.[a-z]{2,}$/i.test(domain)) return 'missing_tld';
  if (!/^[^\s@<>()[\],;:"']+$/.test(local)) return 'malformed_local';

  return null;
}

function isValidOutreachEmail(value) {
  return !invalidOutreachEmailReason(value);
}

function appendQuarantineNote(existingNotes, reason) {
  const marker = `[QUARANTINED:${reason}]`;
  const notes = String(existingNotes || '').trim();
  if (notes.includes(marker)) return existingNotes || marker;
  return notes ? `${notes} ${marker}` : marker;
}

module.exports = {
  appendQuarantineNote,
  invalidOutreachEmailReason,
  isValidOutreachEmail,
  normalizeEmailAddress,
};
