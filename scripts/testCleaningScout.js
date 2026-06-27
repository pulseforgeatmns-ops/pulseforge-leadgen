#!/usr/bin/env node
/*
 * testCleaningScout.js — offline ICP rubric harness for the cleaning client.
 *
 * The real Scout test pass (`node leadgen.js --client_id 10 --industry "law firm"
 * --location "Manchester NH"`) needs live Google Places + Prospeo + DATABASE_URL,
 * which are only present on Railway. This harness exercises the SAME rubric
 * (leadgen.js scoreCleaningLead) against a representative sample of Manchester-NH
 * law-firm leads — shaped exactly like Google Places + enrichment output — so the
 * scoring logic and component weights can be eyeballed and tuned BEFORE spending
 * API credits on a live run.
 *
 * Sample leads below are synthetic but realistic (names/addresses illustrative).
 * Run: node scripts/testCleaningScout.js
 */
const { scoreCleaningLead } = require('../leadgen');

// Representative law-firm leads spanning the rubric's axes:
// in-area vs out-of-area, single-tenant vs suite, reachable vs unreachable,
// small/solo vs large/multi-office.
const SAMPLE_LEADS = [
  {
    company: 'Law Office of Sarah Whitfield',
    url: 'whitfieldlawnh.com',
    address: '482 Chestnut St, Manchester, NH 03101',
    phone: '(603) 555-0142',
    email: 'sarah@whitfieldlawnh.com',
    contact: 'Sarah Whitfield',
    snippet: 'Solo estate planning and family law attorney serving Manchester NH.',
  },
  {
    company: 'Bedford Family Law, PLLC',
    url: 'bedfordfamilylaw.com',
    address: '12 Kilton Rd, Bedford, NH 03110',
    phone: '(603) 555-0188',
    email: 'info@bedfordfamilylaw.com',
    contact: 'Mark Reardon',
    snippet: 'Bedford NH family law firm, attorney at law, free consultation.',
  },
  {
    company: 'Hollis & Pratt Attorneys',
    url: 'hollispratt.com',
    address: '1000 Elm St, Suite 1400, Manchester, NH 03101',
    phone: '(603) 555-0119',
    email: 'reception@hollispratt.com',
    contact: 'Front Desk',
    snippet: 'Full-service law firm, offices in Manchester and Boston.',
  },
  {
    company: 'Granite Tax & Accounting',
    url: 'granitetaxnh.com',
    address: '55 South River Rd, Bedford, NH 03110',
    phone: '(603) 555-0173',
    email: 'hello@granitetaxnh.com',
    contact: 'Dana Cormier',
    snippet: 'CPA firm and bookkeeping, small business tax preparation.',
  },
  {
    company: 'Nashua Injury Lawyers',
    url: 'nashuainjury.com',
    address: '88 Main St, Nashua, NH 03060',
    phone: '(603) 555-0150',
    email: 'intake@nashuainjury.com',
    contact: 'Paul Greer',
    snippet: 'Personal injury law firm serving Nashua and southern NH.',
  },
  {
    company: 'Beacon National Law Group',
    url: 'beaconnational.com',
    address: '900 Elm St, Floor 6, Manchester, NH 03101',
    phone: '(603) 555-0101',
    email: 'contact@beaconnational.com',
    contact: '',
    snippet: 'National law firm with offices in 14 states. Nationwide coverage.',
  },
  {
    company: 'Concord Estate Counsel',
    url: 'concordestatecounsel.com',
    address: '6 Pleasant St, Concord, NH 03301',
    phone: '',
    email: 'firm@concordestatecounsel.com',
    contact: '',
    snippet: 'Trusts and estates law office of Concord NH.',
  },
  {
    company: 'Salem Tax Advisors',
    url: 'salemtaxadvisors.com',
    address: '4 Main St, Salem, NH 03079',
    phone: '(603) 555-0166',
    email: '',
    contact: '',
    snippet: 'Enrolled agent and tax services in Salem NH.',
  },
  {
    company: 'Pinetree Legal LLP',
    url: 'pinetreelegal.com',
    address: '',
    phone: '(603) 555-0190',
    email: 'office@pinetreelegal.com',
    contact: 'Janet Cole',
    snippet: 'Manchester NH business law firm, attorneys at law.',
  },
  {
    company: 'Boston Commercial Realty Advisors',
    url: 'bostoncra.com',
    address: '101 Federal St, Boston, MA 02110',
    phone: '(617) 555-0123',
    email: 'info@bostoncra.com',
    contact: 'Greg Halloran',
    snippet: 'Commercial real estate advisory, Boston MA.',
  },
];

// Setter visibility now uses one global quality threshold for every client.
const THRESHOLD = 70;
function bucket(score) {
  if (score >= THRESHOLD) return `${THRESHOLD}-100 (setter-qualifying)`;
  if (score >= 40)        return `40-${THRESHOLD - 1}  (review)`;
  if (score >= 25)        return `25-39  (weak)`;
  return '0-24   (cull)';
}

console.log('\n=== Cleaning-client ICP test pass (law firms, Manchester NH area) ===');
console.log('Rubric: scoreCleaningLead (client_id=10, scoring_profile=cleaning_buyer)');
console.log('Source: synthetic sample modeling Google Places + enrichment output\n');

const scored = SAMPLE_LEADS.map(l => ({ lead: l, r: scoreCleaningLead(l) }))
  .sort((a, b) => b.r.total - a.r.total);

// Distribution
const dist = {};
for (const s of scored) {
  const b = bucket(s.r.total);
  dist[b] = (dist[b] || 0) + 1;
}
console.log(`Leads scored: ${scored.length}`);
console.log('Score distribution:');
for (const b of [`${THRESHOLD}-100 (setter-qualifying)`, `40-${THRESHOLD - 1}  (review)`, '25-39  (weak)', '0-24   (cull)']) {
  console.log(`  ${b}: ${dist[b] || 0}`);
}
const avg = Math.round(scored.reduce((a, s) => a + s.r.total, 0) / scored.length);
console.log(`Average score: ${avg}\n`);

// Per-lead breakdown
console.log('Per-lead component breakdown (sorted high→low):');
console.log('─'.repeat(78));
for (const { lead, r } of scored) {
  const c = r.components;
  console.log(`${String(r.total).padStart(3)}  ${lead.company}`);
  console.log(`     ${lead.address || '(no address)'}`);
  console.log(`     vertical:${c.vertical}/35  geo:${c.geography}/25  contact:${c.contact}/25  single_tenant:${c.single_tenant}/10  size:${c.size}/5  penalty:-${c.penalty}`);
  console.log(`     single-tenant basis: ${c.single_tenant_basis}`);
  console.log(`     size basis: ${c.size_basis}`);
  console.log(`     penalty basis: ${c.penalty_basis}`);
  if (r.flags.length) console.log(`     flags: ${r.flags.join(' | ')}`);
  console.log('');
}
