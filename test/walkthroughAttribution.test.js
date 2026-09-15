const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const path = require('node:path');
const {
  SOURCE_KIND,
  ATTRIBUTION_FIELD_KEYS,
  sanitizeAttributionFields,
  parseQueryAttribution,
  mergeSessionAttribution,
  hasPaidAttributionSignals,
  buildAttributionRecord,
  normalizeWalkthroughAttribution,
} = require('../lib/walkthroughAttribution');
const { validateWalkthroughPayload, validateAttributionInput } = require('../lib/walkthroughValidate');
const { captureWalkthroughLead, mirrorAttributionToProspect } = require('../lib/walkthroughCapture');
const pool = require('../db');

const SITE = path.join(__dirname, '..', 'sites', 'anchor-cleaning', 'index.html');

function basePayload(overrides = {}) {
  return {
    name: 'Alex Owner',
    business_name: 'Riverside Law',
    phone: '(603) 555-0142',
    email: 'alex@riverside.example',
    city: 'Manchester',
    space_type: 'law_office',
    company_website: '',
    ...overrides,
  };
}

describe('walkthrough attribution sanitize + query parse', () => {
  it('reads approved attribution params from a query string', () => {
    const parsed = parseQueryAttribution('?campaign_id=c1&ad_group_id=ag1&ad_id=a1&utm_source=chatgpt&oppref=abc');
    assert.equal(parsed.campaign_id, 'c1');
    assert.equal(parsed.ad_group_id, 'ag1');
    assert.equal(parsed.ad_id, 'a1');
    assert.equal(parsed.utm_source, 'chatgpt');
    assert.equal(parsed.oppref, 'abc');
  });

  it('ignores arbitrary query params', () => {
    const parsed = parseQueryAttribution('?campaign_id=c1&evil=<script>alert(1)</script>&token=secret');
    assert.equal(parsed.campaign_id, 'c1');
    assert.equal(parsed.evil, undefined);
    assert.equal(parsed.token, undefined);
  });

  it('drops malformed nested attribution values', () => {
    const sanitized = sanitizeAttributionFields({
      campaign_id: 'c1',
      utm_source: { nested: true },
      referrer: 'https://chatgpt.com/',
    });
    assert.equal(sanitized.campaign_id, 'c1');
    assert.equal(sanitized.utm_source, undefined);
    assert.equal(sanitized.referrer, 'https://chatgpt.com/');
  });

  it('bounds oversized attribution values safely', () => {
    const long = 'x'.repeat(3000);
    const sanitized = sanitizeAttributionFields({ campaign_id: long, utm_source: 'chatgpt' });
    assert.equal(sanitized.campaign_id.length, 128);
    assert.equal(sanitized.utm_source, 'chatgpt');
  });
});

describe('walkthrough attribution session merge', () => {
  it('preserves paid attribution across a direct reload without query params', () => {
    const paid = {
      campaign_id: 'c-paid',
      utm_source: 'chatgpt',
      landing_page_url: 'https://goanchorcleaning.com/',
      referrer: 'https://chatgpt.com/',
    };
    const reload = {
      landing_page_url: 'https://goanchorcleaning.com/',
      referrer: '',
    };
    const merged = mergeSessionAttribution(paid, reload);
    assert.equal(merged.campaign_id, 'c-paid');
    assert.equal(merged.utm_source, 'chatgpt');
  });

  it('keeps first paid attribution when refresh drops paid query params', () => {
    const first = { oppref: 'token-1', landing_page_url: 'https://goanchorcleaning.com/?oppref=token-1' };
    const second = { landing_page_url: 'https://goanchorcleaning.com/' };
    assert.equal(mergeSessionAttribution(first, second).oppref, 'token-1');
  });

  it('upgrades organic session state when a later paid click arrives', () => {
    const organic = { landing_page_url: 'https://goanchorcleaning.com/', referrer: '' };
    const paid = { utm_source: 'chatgpt', opref: 'abc', landing_page_url: 'https://goanchorcleaning.com/?utm_source=chatgpt' };
    const merged = mergeSessionAttribution(organic, paid);
    assert.equal(merged.opref, 'abc');
    assert.equal(merged.utm_source, 'chatgpt');
  });
});

describe('walkthrough attribution validation contract', () => {
  it('accepts valid optional attribution without affecting contact validation', () => {
    const result = validateWalkthroughPayload(basePayload({
      attribution: {
        campaign_id: 'c1',
        utm_source: 'chatgpt',
        landing_page_url: 'https://goanchorcleaning.com/?utm_source=chatgpt',
        referrer: 'https://chatgpt.com/',
      },
    }));
    assert.equal(result.ok, true);
    assert.equal(result.values.attribution.campaign_id, 'c1');
  });

  it('sanitizes malformed optional attribution instead of failing the lead', () => {
    const result = validateWalkthroughPayload(basePayload({
      attribution: ['not-an-object'],
    }));
    assert.equal(result.ok, true);
    assert.equal(result.values.attribution, undefined);
    assert.equal(validateAttributionInput({ attribution: { nested: { bad: true } } }), null);
  });

  it('allows no-attribution submissions unchanged', () => {
    const result = validateWalkthroughPayload(basePayload());
    assert.equal(result.ok, true);
    assert.equal(result.values.attribution, undefined);
  });
});

describe('walkthrough attribution normalization + persistence', () => {
  it('normalizes ChatGPT-specific evidence to chatgpt_ads deterministically', () => {
    const record = buildAttributionRecord({
      oppref: 'click-token',
      landing_page_url: 'https://goanchorcleaning.com/',
    });
    assert.equal(record.normalized.lead_source, 'chatgpt_ads');
    assert.equal(record.normalized.attribution_status, 'deterministic');
  });

  it('does not classify ambiguous campaign_id alone as chatgpt_ads', () => {
    const record = buildAttributionRecord({ campaign_id: '12345' });
    assert.equal(record.normalized.lead_source, 'unknown');
    assert.equal(record.normalized.attribution_status, 'inferred');
  });

  it('labels provenance as first-party and never platform API', () => {
    const record = buildAttributionRecord({ utm_source: 'chatgpt', utm_medium: 'cpc' });
    assert.equal(record.provenance.sourceKind, SOURCE_KIND);
    assert.equal(record.provenance.clientSubmitted, true);
    assert.equal(record.provenance.readOnly, true);
    assert.notEqual(record.provenance.sourceKind, 'PLATFORM_API');
    assert.match(record.provenance.observedAt, /^\d{4}-\d{2}-\d{2}T/);
  });

  it('prefers the server Referer header when present', () => {
    const record = normalizeWalkthroughAttribution(
      { utm_source: 'chatgpt', referrer: 'https://client.example/' },
      { serverReferer: 'https://chatgpt.com/' }
    );
    assert.equal(record.raw.referrer, 'https://chatgpt.com/');
  });

  it('stores raw + normalized attribution on agent_actions payload', async () => {
    const original = pool.query;
    let insert = null;
    pool.query = async (sql, params) => {
      if (/INSERT INTO agent_actions/i.test(sql)) {
        insert = { sql, params };
        return { rows: [{ id: 901 }] };
      }
      throw new Error(`Unexpected query: ${sql}`);
    };
    try {
      const record = buildAttributionRecord({ opref: 'abc', landing_page_url: 'https://goanchorcleaning.com/' });
      const validated = validateWalkthroughPayload(basePayload({ attribution: record.raw }));
      await captureWalkthroughLead(validated.values, record);
      const payload = JSON.parse(insert.params[4]);
      assert.equal(payload.source, 'website_walkthrough');
      assert.equal(payload.attribution.raw.opref, 'abc');
      assert.equal(payload.attribution.normalized.lead_source, 'chatgpt_ads');
      assert.equal(payload.attribution.provenance.sourceKind, SOURCE_KIND);
      assert.notEqual(payload.attribution.provenance.sourceKind, 'PLATFORM_API');
    } finally {
      pool.query = original;
    }
  });

  it('mirrors attribution into prospects.acquisition_metadata without clobbering existing metadata', async () => {
    const original = pool.query;
    const updates = [];
    pool.query = async (sql, params) => {
      if (/INSERT INTO agent_actions/i.test(sql)) return { rows: [{ id: 902 }] };
      if (/UPDATE prospects/i.test(sql) && /acquisition_metadata/i.test(sql)) {
        updates.push({ sql, params });
        return { rowCount: 1 };
      }
      throw new Error(`Unexpected query: ${sql}`);
    };
    try {
      const record = buildAttributionRecord({ oppref: 'tok', landing_page_url: 'https://goanchorcleaning.com/' });
      const validated = validateWalkthroughPayload(basePayload({ attribution: record.raw }));
      await captureWalkthroughLead(validated.values, record);
      assert.equal(updates.length, 0);
    } finally {
      pool.query = original;
    }
  });

  it('mirrors attribution into prospects.acquisition_metadata when the column is available', async () => {
    const originalQuery = pool.query;
    let metadataUpdate = null;
    pool.query = async (sql, params) => {
      if (/UPDATE prospects/i.test(sql) && /acquisition_metadata/i.test(sql)) {
        metadataUpdate = { sql, params };
        return { rowCount: 1 };
      }
      throw new Error(`Unexpected query: ${sql}`);
    };

    try {
      const record = buildAttributionRecord({ oppref: 'tok', landing_page_url: 'https://goanchorcleaning.com/' });
      await mirrorAttributionToProspect(501, record);
      assert.ok(metadataUpdate);
      const patch = JSON.parse(metadataUpdate.params[1]);
      assert.equal(patch.attribution.provenance.sourceKind, SOURCE_KIND);
      assert.equal(metadataUpdate.params[0], 501);
      assert.equal(metadataUpdate.params[2], 10);
      assert.match(metadataUpdate.sql, /COALESCE\(acquisition_metadata/);
      assert.match(metadataUpdate.sql, /acquisition_source = COALESCE\(acquisition_source/);
    } finally {
      pool.query = originalQuery;
    }
  });

  it('preserves website_walkthrough source semantics on stored payloads', async () => {
    const original = pool.query;
    let insert = null;
    pool.query = async (sql, params) => {
      if (/INSERT INTO agent_actions/i.test(sql)) {
        insert = params;
        return { rows: [{ id: 904 }] };
      }
      throw new Error(`Unexpected query: ${sql}`);
    };
    try {
      const record = buildAttributionRecord({ campaign_id: 'only-id' });
      const validated = validateWalkthroughPayload(basePayload({ attribution: record.raw }));
      await captureWalkthroughLead(validated.values, record);
      const payload = JSON.parse(insert[4]);
      assert.equal(payload.source, 'website_walkthrough');
      assert.equal(payload.attribution.normalized.lead_source, 'unknown');
    } finally {
      pool.query = original;
    }
  });
});

describe('Anchor landing page attribution contract', () => {
  const html = fs.readFileSync(SITE, 'utf8');

  it('captures whitelisted attribution params into sessionStorage', () => {
    assert.match(html, /anchor_first_party_attribution/);
    assert.match(html, /ATTRIBUTION_QUERY_KEYS/);
    ATTRIBUTION_FIELD_KEYS.filter((key) => !['landing_page_url', 'referrer'].includes(key)).forEach((key) => {
      assert.match(html, new RegExp(`['"]${key}['"]`));
    });
  });

  it('posts optional attribution with the walkthrough form body', () => {
    assert.match(html, /getSubmissionAttribution\(\)/);
    assert.match(html, /if \(attribution\) data\.attribution = attribution/);
    assert.match(html, /persistLandingAttribution\(\)/);
    assert.match(html, /sessionStorage\.setItem\(ATTRIBUTION_STORAGE_KEY/);
  });

  it('does not leak arbitrary query params into submission attribution', () => {
    assert.doesNotMatch(html, /URLSearchParams\([^)]*\)[\s\S]{0,120}for\s*\(\s*var\s+\w+\s+in\s+params/);
    assert.match(html, /ATTRIBUTION_QUERY_KEYS\.forEach/);
  });

  it('fires OpenAI lead_created only after successful submission and optionally passes submission_id', () => {
    assert.match(html, /trackOpenAiLeadCreated\(openAiLeadTracked, json\.submission_id\)/);
    assert.match(html, /if \(json && json\.submission_id\)/);
    assert.match(html, /lead_created/);
    assert.match(html, /type: 'customer_action'/);
  });
});
