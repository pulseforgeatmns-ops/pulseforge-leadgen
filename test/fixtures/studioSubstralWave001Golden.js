'use strict';

/**
 * Manually approved Wave 001 style anchors (SPEC-PAIGE-SUBSTRAL-001).
 * Used as canonical structure checks — not verbatim regression of export batch JSON.
 */

module.exports = Object.freeze([
  {
    company: 'Nick Tracey Roofing & Exteriors',
    specificIssue: 'Weak trust proof — Google review strength is not carried through on the homepage',
    mustMatch: [
      /I was looking at your site and saw/i,
      /strong reputation/i,
      /homepage doesn't show that proof/i,
      /Happy to send over a quick assessment/i,
    ],
    mustNotMatch: [/I noticed/i, /credibility gap/i, /conversion path/i, /—/],
  },
  {
    company: 'Paramount Roofing',
    specificIssue: 'Weak trust proof — google review strength is not carried through on the homepage',
    mustMatch: [/I was looking at your site and saw/i, /assessment/i],
    mustNotMatch: [/I noticed/i, /credibility gap/i],
  },
  {
    company: 'Pyramid Roofing',
    specificIssue: 'Mobile friction — homepage fetch took 8s during audit',
    mustMatch: [/I was looking at your site and saw/i, /taking a while to load/i],
    mustNotMatch: [/I noticed/i, /8s during audit/i],
  },
  {
    company: 'F.B.I. Contracting LLC',
    specificIssue: 'Weak first-impression credibility — missing or empty page title',
    mustMatch: [/I was looking at your site and saw/i, /search and browser tabs/i],
    mustNotMatch: [/I noticed/i, /credibility gap/i],
  },
  {
    company: 'Mill City Property Management',
    specificIssue: 'Mobile friction — missing viewport meta tag for responsive layout',
    mustMatch: [/I was looking at your site and saw/i, /mobile experience could be working harder/i],
    mustNotMatch: [/viewport meta tag/i, /I noticed/i],
  },
  {
    company: 'Ferris Plumbing, Heating & Air',
    specificIssue: 'Mobile friction — missing viewport meta tag',
    mustMatch: [/mobile experience could be working harder/i],
    mustNotMatch: [/missing viewport/i],
  },
  {
    company: 'Heritage Home Service',
    specificIssue: 'Unclear homepage CTA — no obvious phone, email, form, or contact link on the homepage',
    mustMatch: [/clearest next step from the homepage/i],
    mustNotMatch: [/conversion path/i, /I noticed/i],
  },
]);
