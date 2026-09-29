/* ==========================================================================
   Studio Substral — the closing assessment instrument.

   The field behaves like an instrument, not a marketing form: it validates
   what it was given, reports plainly what it did, and never invents a result.
   The server is authoritative on admission; this is only a fast first pass so
   the visitor is not made to wait for a round trip to learn they typed a
   search engine into it.
   ========================================================================== */

const FALLBACK_MAILBOX = 'hello@studiosubstral.com';

/* Domains that cannot be the subject of an assessment. Kept in step with
   packages/capabilities/websiteOpportunityIntelligence/discoveryAdmission.js */
const NOT_A_SUBJECT = new Set([
  'google.com',
  'www.google.com',
  'maps.google.com',
  'bing.com',
  'yahoo.com',
  'duckduckgo.com',
  'facebook.com',
  'instagram.com',
  'linkedin.com',
  'x.com',
  'twitter.com',
  'yelp.com',
  'yellowpages.com',
  'findlaw.com',
  'lawyers.com',
  'manta.com',
  'mapquest.com',
  'hotfrog.com',
]);

const LABEL = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const EMAIL = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;

/**
 * Reduce anything a person might paste to a bare registrable host.
 * @returns {{ ok: true, domain: string } | { ok: false, reason: string }}
 */
export function normalizeDomainInput(raw) {
  let value = String(raw ?? '')
    .trim()
    .toLowerCase()
    .replace(/\s+/g, '');

  if (!value) return { ok: false, reason: 'empty' };

  value = value
    .replace(/^[a-z][a-z0-9+.-]*:\/\//, '')
    .replace(/^[^@/]*@/, '')
    .split(/[/?#]/)[0]
    .replace(/:\d+$/, '')
    .replace(/\.$/, '');

  if (value.startsWith('www.')) value = value.slice(4);
  if (!value) return { ok: false, reason: 'empty' };

  if (value === 'localhost' || /^\d{1,3}(\.\d{1,3}){3}$/.test(value) || value.includes(':')) {
    return { ok: false, reason: 'not_public' };
  }

  const labels = value.split('.');
  if (labels.length < 2) return { ok: false, reason: 'no_tld' };
  if (!labels.every((label) => LABEL.test(label))) return { ok: false, reason: 'malformed' };
  if (!/^[a-z]{2,24}$/.test(labels.at(-1))) return { ok: false, reason: 'no_tld' };
  if (value.length > 253) return { ok: false, reason: 'malformed' };
  if (NOT_A_SUBJECT.has(value)) return { ok: false, reason: 'not_a_subject' };

  return { ok: true, domain: value };
}

const MESSAGES = {
  empty: 'Enter the domain you want assessed.',
  no_tld: 'That needs to be a full domain — example.com, not example.',
  malformed: 'That does not parse as a domain. Check for a typo.',
  not_public:
    'That address is not reachable from the public internet, so there is nothing to measure.',
  not_a_subject:
    'That is a search engine, directory or social profile. Enter the business’s own domain.',
  email: 'A valid email address is required — the assessment is sent, not displayed.',
};

export function initAssessment() {
  const form = document.querySelector('[data-assessment-form]');
  if (!form) return;

  const status = document.querySelector('[data-assessment-status]');
  form.noValidate = true;
  let signature = '';
  let requestKey = '';
  const keyFor = async (values) => {
    const next = JSON.stringify(values);
    if (signature === next && requestKey) return requestKey;
    signature = next;
    requestKey = crypto.randomUUID();
    // Keep only a digest and an opaque retry key in this tab, never the fields.
    try {
      const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(next));
      const hash = [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
      const saved = JSON.parse(sessionStorage.getItem('substral-request') || 'null');
      if (saved?.hash === hash && /^[a-zA-Z0-9_-]{16,80}$/.test(saved.key)) requestKey = saved.key;
      sessionStorage.setItem('substral-request', JSON.stringify({ hash, key: requestKey }));
    } catch { /* Storage restrictions do not prevent an in-memory retry. */ }
    return requestKey;
  };
  const submit = form.querySelector('[data-assessment-submit]');
  const domainField = form.elements.domain;
  const emailField = form.elements.email;

  const say = (message, tone = 'neutral') => {
    if (!status) return;
    status.textContent = message;
    status.dataset.tone = tone;
  };

  const focusInvalid = (field, message) => {
    field.setAttribute('aria-invalid', 'true');
    say(message, 'error');
    field.focus();
  };

  form.addEventListener('input', (event) => {
    if (event.target instanceof HTMLElement) {
      event.target.removeAttribute('aria-invalid');
    }
  });

  form.addEventListener('submit', async (event) => {
    event.preventDefault();

    if (submit.disabled) return;
    if (form.elements.company_website?.value.trim()) return;

    const parsed = normalizeDomainInput(domainField.value);
    if (!parsed.ok) {
      focusInvalid(domainField, MESSAGES[parsed.reason] ?? MESSAGES.malformed);
      return;
    }

    const email = String(emailField.value || '').trim();
    if (!EMAIL.test(email)) {
      focusInvalid(emailField, MESSAGES.email);
      return;
    }

    // Show the visitor exactly what we resolved their input to.
    domainField.value = parsed.domain;

    submit.disabled = true;
    form.setAttribute('aria-busy', 'true');
    say(`Sending your request for ${parsed.domain}…`);
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20000);

    try {
      const values = {
        domain: parsed.domain,
        email: email.toLowerCase(),
        context: String(form.elements.context?.value || '').trim(),
      };
      const response = await fetch(form.action, {
        method: 'POST',
        credentials: 'omit',
        signal: controller.signal,
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          ...values,
          request_key: await keyFor(values),
          company_website: form.elements.company_website?.value || '',
        }),
      });

      const body = await response.json().catch(() => ({}));

      if (response.ok && body.ok === true && body.request_id && body.review_mode === 'human') {
        form.hidden = true;
        say(
          `Your request for ${parsed.domain} has been received. A person will review the site and send a written assessment to ${email}. This is a request for human review, not an instant audit. Request reference: ${body.request_id}.`,
          'ok'
        );
        status.focus();
        return;
      }
      if (response.ok) throw new Error('Missing persistence confirmation');

      if (response.status === 429) {
        say('Too many requests. Your details are still here. Please try again in an hour.', 'error');
      } else if (body.error_code && MESSAGES[body.error_code]) {
        focusInvalid(body.details?.email && !body.details?.domain ? emailField : domainField, MESSAGES[body.error_code]);
      } else if (body.error) {
        say(String(body.error), 'error');
      } else {
        throw new Error(`HTTP ${response.status}`);
      }
    } catch {
      // A lost response can happen after a successful save. Retrying uses the
      // same key, so it is safe without claiming that the first request failed.
      say('We couldn’t confirm your request. Your details are still here. Please try again, or ', 'error');
      const link = document.createElement('a');
      link.className = 'textlink';
      link.textContent = `email ${FALLBACK_MAILBOX}`;
      link.href = `mailto:${FALLBACK_MAILBOX}?subject=${encodeURIComponent(`Assessment request — ${parsed.domain}`)}&body=${encodeURIComponent(`Domain: ${parsed.domain}\nReply to: ${email}\nDecision: ${form.elements.context?.value || ''}`)}`;
      status.append(link, '.');
    } finally {
      clearTimeout(timeout);
      form.removeAttribute('aria-busy');
      submit.disabled = false;
    }
  });
}
