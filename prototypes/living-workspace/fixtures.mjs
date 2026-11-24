// Fictional, deliberately disjoint records. No production IDs or data providers.
// Domain records (kind 'domain') come first; business objects (kind 'object')
// orbit their domain and are addressable by the same shared-selection commands.
const domains = ['content', 'acquisition', 'clients', 'campaigns'];
const objectType = { content: 'Draft', acquisition: 'Prospect', clients: 'Client', campaigns: 'Campaign' };
const objectPlural = { content: 'drafts', acquisition: 'prospects', clients: 'clients', campaigns: 'campaigns' };
// [name, stage, summary, facts, evidence] — all invented for the prototype.
const businessObjects = {
  harbor: {
    content: [
      ['Storm-season checklist', 'Outline', 'A practical checklist post for waterfront property owners.', [['Channel', 'Blog'], ['Length', '≈ 800 words']], ['Fixture note: outline awaiting operator review.', 'Fixture note: nothing scheduled or published.']],
      ['Dockside customer story', 'Draft', 'A short customer story built from a fictional interview.', [['Channel', 'Newsletter'], ['Length', '≈ 450 words']], ['Fixture note: quotes are placeholders.', 'Fixture note: no approval requested.']],
    ],
    acquisition: [
      ['Northgate Bakery', 'Research', 'Neighbourhood bakery with two fictional storefronts.', [['Vertical', 'Food service'], ['Town', 'Fictional Bay'], ['Fit signal', 'Recently expanded hours']], ['Fixture note: website mentions a second location.', 'Fixture note: no contact attempted.', 'Fixture note: no execution has been authorized.']],
      ['Tidewater Dental Studio', 'Qualified', 'Two-chair dental practice in an invented harbour town.', [['Vertical', 'Dental'], ['Town', 'Port Ashby'], ['Fit signal', 'New patient promotion']], ['Fixture note: listing shows a recent renovation.', 'Fixture note: no contact attempted.']],
      ['Lantern Hill Fitness', 'New', 'Independent fitness studio with evening classes.', [['Vertical', 'Fitness'], ['Town', 'Lantern Hill'], ['Fit signal', 'Class schedule expanded']], ['Fixture note: discovered in a fictional directory.', 'Fixture note: research not started.']],
    ],
    clients: [
      ['Bayline Property Group', 'Quarterly review', 'Long-standing fictional client with three managed sites.', [['Since', '2024'], ['Sites', '3']], ['Fixture note: review deck not yet drafted.', 'Fixture note: no renewal discussed.']],
      ['Marlow Café', 'Onboarding', 'New fictional client finishing onboarding.', [['Since', '2026'], ['Sites', '1']], ['Fixture note: onboarding checklist 4 of 6.', 'Fixture note: no outreach scheduled.']],
    ],
    campaigns: [
      ['Autumn reactivation', 'Planning', 'Re-engagement series for dormant fictional customers.', [['Audience', '≈ 120 fictional contacts'], ['Steps', '3']], ['Fixture note: copy not written.', 'Fixture note: sending is not enabled.']],
      ['Referral partner series', 'Idea', 'A short partner-introduction sequence.', [['Audience', 'Partners'], ['Steps', '2']], ['Fixture note: idea captured only.', 'Fixture note: sending is not enabled.']],
    ],
  },
  cedar: {
    content: [
      ['Studio lighting guide', 'Outline', 'An explainer on natural light for small studios.', [['Channel', 'Blog'], ['Length', '≈ 1,000 words']], ['Fixture note: outline only.', 'Fixture note: nothing published.']],
      ['Behind-the-bench reel', 'Idea', 'A short video concept for a fictional workshop.', [['Channel', 'Social'], ['Length', '30 s']], ['Fixture note: concept only.', 'Fixture note: no approval requested.']],
    ],
    acquisition: [
      ['Willowmere Florist', 'Research', 'Boutique florist with a fictional wedding focus.', [['Vertical', 'Retail'], ['Town', 'Willowmere'], ['Fit signal', 'Seasonal demand']], ['Fixture note: portfolio page found.', 'Fixture note: no contact attempted.']],
      ['Copperfield Joinery', 'New', 'Small custom joinery shop.', [['Vertical', 'Trades'], ['Town', 'Copperfield'], ['Fit signal', 'Hiring an apprentice']], ['Fixture note: research not started.', 'Fixture note: no execution has been authorized.']],
    ],
    clients: [
      ['Fernleaf Gallery', 'Active', 'Fictional gallery client with monthly exhibitions.', [['Since', '2025'], ['Sites', '1']], ['Fixture note: next exhibition in planning.', 'Fixture note: no outreach scheduled.']],
    ],
    campaigns: [
      ['Winter open studio', 'Planning', 'Invitation series for a fictional open-studio weekend.', [['Audience', '≈ 60 fictional contacts'], ['Steps', '2']], ['Fixture note: dates not confirmed.', 'Fixture note: sending is not enabled.']],
    ],
  },
};

// Stage pipelines per object type (presentation only).
export const pipelines = {
  Prospect: ['New', 'Research', 'Qualified', 'Engaged', 'Won'],
  Draft: ['Idea', 'Outline', 'Draft', 'Review', 'Published'],
  Client: ['Onboarding', 'Active', 'Quarterly review', 'Renewal'],
  Campaign: ['Idea', 'Planning', 'Ready', 'Running', 'Complete'],
};
// Rich, invented detail for selected objects; everything else gets honest defaults below.
const detail = {
  'harbor-acquisition-1': {
    status: 'attention',
    signals: [['Expanded opening hours', 'strong', 'Listing changed in the last month (fixture).'], ['Second storefront', 'moderate', 'Mentioned on the website footer (fixture).'], ['Reviews mention slow online ordering', 'weak', 'Two fictional reviews.']],
    related: [['harbor-campaigns-1', 'Candidate audience for'], ['harbor-acquisition-2', 'Same neighbourhood as']],
    nextActions: [['Review the second-location evidence', 'workspace'], ['Ask Max what is still outstanding', 'max'], ['Draft an introduction', 'proposal']],
    findings: [['Likely owner-operated, two locations.', 'medium'], ['No current marketing partner identified.', 'low']],
    evidence: [['Website footer lists a second address.', 'Website (fixture)', '2026-11-18', 'high'], ['Opening hours extended to 9pm.', 'Listing (fixture)', '2026-11-12', 'high'], ['Reviews mention ordering friction.', 'Review site (fixture)', '2026-10-30', 'medium']],
    activity: [['2026-11-19', 'Scout · fixture', 'Discovered in a fictional local directory'], ['2026-11-20', 'Operator', 'Moved to Research'], ['2026-11-21', 'Max · simulated', 'Linked to Autumn reactivation as a candidate audience']],
    questions: ['Is the second storefront open yet?', 'Who makes marketing decisions?'],
  },
  'harbor-acquisition-2': {
    status: 'steady',
    signals: [['New-patient promotion', 'strong', 'Running on the fictional website.'], ['Recent renovation', 'moderate', 'Listing photos updated.']],
    related: [['harbor-content-2', 'Possible story for'], ['harbor-acquisition-1', 'Same neighbourhood as']],
    nextActions: [['Compare with Northgate Bakery', 'workspace'], ['Draft a qualification note', 'proposal']],
    findings: [['Qualified on size and intent signals.', 'medium']],
    evidence: [['Promotion page published.', 'Website (fixture)', '2026-11-10', 'high'], ['Listing shows renovated reception.', 'Listing (fixture)', '2026-11-02', 'medium']],
    activity: [['2026-11-08', 'Scout · fixture', 'Discovered'], ['2026-11-15', 'Operator', 'Marked Qualified']],
    questions: [],
  },
  'harbor-acquisition-3': {
    status: 'new',
    signals: [['Evening class schedule expanded', 'moderate', 'Fictional timetable.']],
    related: [['harbor-campaigns-2', 'Possible partner for']],
    nextActions: [['Start research', 'workspace']],
    findings: [],
    evidence: [['Discovered in a fictional directory.', 'Directory (fixture)', '2026-11-22', 'medium']],
    activity: [['2026-11-22', 'Scout · fixture', 'Discovered']],
    questions: ['Is the studio independent or part of a chain?'],
  },
  'harbor-clients-1': { status: 'attention', related: [['harbor-content-1', 'Featured in']], questions: ['Has the review date been agreed?'] },
  'harbor-campaigns-1': { status: 'steady', related: [['harbor-acquisition-1', 'May include']] },
  'harbor-content-2': { related: [['harbor-acquisition-2', 'Could feature']] },
};
const domainMeta = {
  content: { status: 'steady', note: 'Two drafts awaiting review' },
  acquisition: { status: 'attention', note: '1 prospect needs a decision' },
  clients: { status: 'attention', note: 'Quarterly review approaching' },
  campaigns: { status: 'steady', note: 'One campaign in planning' },
};
// Meaningful cross-domain relationships for the field (from → to, label).
const domainLinks = [['acquisition', 'campaigns', 'Prospects become campaign audiences'], ['clients', 'content', 'Client stories feed content'], ['campaigns', 'clients', 'Campaigns reactivate clients']];

const cap = s => s[0].toUpperCase() + s.slice(1);
function enrich(obj) {
  const d = detail[obj.id] || {};
  const tuple = (rows, keys) => (rows || []).map(r => Object.fromEntries(keys.map((k, i) => [k, r[i]])));
  return {
    ...obj,
    status: d.status || 'steady',
    signals: tuple(d.signals || [[obj.facts.at(-1)?.[1] || obj.summary, 'moderate', 'Fixture signal.']], ['label', 'strength', 'note']),
    related: tuple(d.related, ['recordId', 'relation']),
    nextActions: tuple(d.nextActions || [['Review the evidence', 'workspace']], ['label', 'kind']),
    findings: tuple(d.findings, ['text', 'confidence']),
    evidence: d.evidence ? tuple(d.evidence, ['text', 'source', 'observed', 'confidence']) : obj.evidence.map(text => ({ text: cap(text.replace(/^Fixture note: /, '')), source: 'Fixture note', observed: '2026-11-15', confidence: 'medium' })),
    activity: tuple(d.activity || [['2026-11-14', 'Operator', `Created as ${obj.stage}`]], ['at', 'actor', 'text']),
    questions: d.questions || [],
  };
}

export const fixtures = Object.fromEntries(['harbor', 'cedar'].map((id, index) => {
  const domainRecords = domains.map((domain, n) => ({
    id: `${id}-${domain}`, kind: 'domain', domain,
    name: `${index ? 'Cedar' : 'Harbor'} ${['editorial plan', 'prospect research', 'client review', 'autumn campaign'][n]}`,
    summary: ['Two draft ideas ready for investigation.', 'Three fictional businesses awaiting research.', 'A quarterly review is approaching.', 'An upcoming campaign is in planning.'][n],
    evidence: ['Fixture note: operator review requested.', 'Fixture note: no execution has been authorized.'],
  }));
  const objectRecords = domains.flatMap(domain => businessObjects[id][domain].map(([name, stage, summary, facts, evidence], n) => enrich({
    id: `${id}-${domain}-${n + 1}`, kind: 'object', domain, parent: `${id}-${domain}`,
    type: objectType[domain], name, stage, summary, facts, evidence,
  })));
  return [id, {
    id, name: index ? 'Cedar Studio' : 'Harbor Works',
    records: [...domainRecords, ...objectRecords],
    overview: {
      maxAnchor: { headline: 'Business context', subline: 'Four connected domains' },
      links: domainLinks.map(([from, to, label]) => ({ from, to, label })),
      domains: domains.map((domain, n) => ({
        id: domain, label: domain[0].toUpperCase() + domain.slice(1),
        priority: n === 1 ? 'elevated' : 'normal',
        status: domainMeta[domain].status, note: domainMeta[domain].note,
        position: { slot: ['top', 'left', 'right', 'bottom'][n] },
        summary: { compressed: ['Ideas for review', 'Research in focus', 'Relationships', 'Planned work'][n] },
        objectPlural: objectPlural[domain],
        intelligence: { active: n === 1 }, drawer: {},
      })),
    },
  }];
}));
