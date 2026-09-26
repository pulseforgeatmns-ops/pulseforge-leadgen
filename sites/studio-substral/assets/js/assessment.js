/* ==========================================================================
   Studio Substral — the assessment instrument (Act IV).

   The field behaves like an instrument, not a marketing form: it validates
   what it was given, reports plainly what it did, and never invents a result.
   The server is authoritative on admission; this is only a fast first pass so
   the visitor is not made to wait for a round trip to learn they typed a
   search engine into it.
   ========================================================================== */

const ENDPOINT =
  'https://pulseforge-leadgen-production.up.railway.app/api/public/website-assessment';

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

  const status = form.querySelector('[data-assessment-status]');
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
    say(`Queuing ${parsed.domain} for assessment…`);

    try {
      const response = await fetch(ENDPOINT, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          domain: parsed.domain,
          email,
          context: String(form.elements.context?.value || '').trim(),
          referer: document.referrer || null,
        }),
      });

      const body = await response.json().catch(() => ({}));

      if (response.ok) {
        form.hidden = true;
        say(
          body.message ||
            `${parsed.domain} is queued. We run the measurement pass, a person reviews the findings, and the assessment goes to ${email}. If the evidence does not support a recommendation, the report will say so.`,
          'ok'
        );
        return;
      }

      if (response.status === 429) {
        say('That is more requests than we can take from one place right now. Try again shortly.', 'error');
      } else if (body.error_code && MESSAGES[body.error_code]) {
        say(MESSAGES[body.error_code], 'error');
      } else if (body.error) {
        say(String(body.error), 'error');
      } else {
        throw new Error(`HTTP ${response.status}`);
      }
    } catch {
      // Never swallow the request. Hand the visitor a route that does not
      // depend on our infrastructure being up.
      const subject = encodeURIComponent(`Assessment request — ${parsed.domain}`);
      const body = encodeURIComponent(
        `Domain: ${parsed.domain}\nSend the assessment to: ${email}\n`
      );
      say('', 'error');
      status.innerHTML =
        'The queue did not accept that request. Nothing was lost — ' +
        `<a class="textlink" href="mailto:${FALLBACK_MAILBOX}?subject=${subject}&body=${body}">send it to ${FALLBACK_MAILBOX}</a>` +
        ' and we will pick it up from there.';
      status.dataset.tone = 'error';
    } finally {
      submit.disabled = false;
    }
  });
}
