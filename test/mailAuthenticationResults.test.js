'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  parseDeliveredAuthenticationEvidence,
  verificationStateFromDeliveredHeaders,
  deliveredAuthenticationPasses,
} = require('../utils/mailAuthenticationResults');

const SAMPLE = [
  'Authentication-Results: mx.google.com;',
  '       spf=pass (google.com: domain of hello@studiosubstral.com designates 209.85.220.41 as permitted sender) smtp.mailfrom=hello@studiosubstral.com;',
  '       dkim=pass header.i=@studiosubstral.com header.s=google header.b=AbCdEf;',
  '       dmarc=pass (p=QUARANTINE sp=QUARANTINE dis=NONE) header.from=studiosubstral.com',
  'From: Studio Substral <hello@studiosubstral.com>',
  'Reply-To: hello@studiosubstral.com',
  '',
  'Body',
].join('\r\n');

describe('mailAuthenticationResults', () => {
  it('parses SPF/DKIM/DMARC pass from delivered Authentication-Results', () => {
    const parsed = parseDeliveredAuthenticationEvidence(SAMPLE);
    assert.equal(parsed.spf, 'pass');
    assert.equal(parsed.dkim, 'pass');
    assert.equal(parsed.dmarc, 'pass');
    assert.equal(parsed.from, 'hello@studiosubstral.com');
    assert.equal(parsed.replyTo, 'hello@studiosubstral.com');
  });

  it('maps delivered headers to verification_state with delivered_message provenance', () => {
    const state = verificationStateFromDeliveredHeaders(SAMPLE);
    assert.equal(state.spf.status, 'pass');
    assert.equal(state.spf.provenance.source, 'delivered_message');
    assert.ok(deliveredAuthenticationPasses(state));
  });

  it('rejects DNS-only present status without delivered_message provenance', () => {
    assert.equal(deliveredAuthenticationPasses({
      spf: { status: 'present', provenance: { source: 'verification_state' } },
      dkim: { status: 'present', provenance: { source: 'verification_state' } },
      dmarc: { status: 'present', provenance: { source: 'verification_state' } },
    }), false);
  });
});
