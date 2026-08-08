'use strict';

/**
 * SAMPLE / DEV FIXTURE — Anchor Cleaning Growth Infrastructure answers.
 *
 * Not live client data. Used only by the Growth Infrastructure Readiness
 * dev shortcut and CLI smoke (`npm run growth:infra:smoke -- --fixture=anchor`).
 * Does not mutate DNS, GBP, social, analytics, or CRM.
 */

module.exports = Object.freeze({
  id: 'anchor',
  label: 'Anchor Cleaning (sample / dev)',
  sample: true,
  devOnly: true,
  businessName: 'Anchor Cleaning',
  description:
    'Sample/dev answers for Anchor Cleaning mixed readiness — exercises report rendering and report_ready transition without manual Q&A.',
  disclaimer:
    'SAMPLE/DEV DATA — answers are fixture text for local testing. Not a real client submission. Assessment only; no infrastructure mutations.',
  answers: Object.freeze([
    Object.freeze({
      step: 'website_domain',
      message:
        '[SAMPLE/DEV] Site is https://anchorcleaning.example — we own the domain at Cloudflare. Branded email is hello@anchorcleaning.example.',
    }),
    Object.freeze({
      step: 'gbp',
      message:
        '[SAMPLE/DEV] GBP exists and is claimed. About 12 reviews. Photos are thin.',
    }),
    Object.freeze({
      step: 'lead_flow',
      message:
        '[SAMPLE/DEV] Phone and form leads go into our CRM. Response same day. Missed calls get a callback process.',
    }),
    Object.freeze({
      step: 'estimates',
      message:
        '[SAMPLE/DEV] We do walkthroughs, have pricing inputs, and follow up twice.',
    }),
    Object.freeze({
      step: 'tracking',
      message:
        '[SAMPLE/DEV] We have GA4 but no UTMs or call tracking yet. No Search Console.',
    }),
    Object.freeze({
      step: 'assets',
      message:
        '[SAMPLE/DEV] Logo and photos ready. Facebook present. No before/after yet. Instagram thin.',
    }),
  ]),
});
