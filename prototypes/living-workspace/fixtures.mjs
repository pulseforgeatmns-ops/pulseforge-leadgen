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

export const fixtures = Object.fromEntries(['harbor', 'cedar'].map((id, index) => {
  const domainRecords = domains.map((domain, n) => ({
    id: `${id}-${domain}`, kind: 'domain', domain,
    name: `${index ? 'Cedar' : 'Harbor'} ${['editorial plan', 'prospect research', 'client review', 'autumn campaign'][n]}`,
    summary: ['Two draft ideas ready for investigation.', 'Three fictional businesses awaiting research.', 'A quarterly review is approaching.', 'An upcoming campaign is in planning.'][n],
    evidence: ['Fixture note: operator review requested.', 'Fixture note: no execution has been authorized.'],
  }));
  const objectRecords = domains.flatMap(domain => businessObjects[id][domain].map(([name, stage, summary, facts, evidence], n) => ({
    id: `${id}-${domain}-${n + 1}`, kind: 'object', domain, parent: `${id}-${domain}`,
    type: objectType[domain], name, stage, summary, facts, evidence,
  })));
  return [id, {
    id, name: index ? 'Cedar Studio' : 'Harbor Works',
    records: [...domainRecords, ...objectRecords],
    overview: {
      maxAnchor: { headline: 'Business context', subline: 'Four connected domains' },
      domains: domains.map((domain, n) => ({
        id: domain, label: domain[0].toUpperCase() + domain.slice(1),
        priority: n === 1 ? 'elevated' : 'normal',
        position: { slot: ['top', 'left', 'right', 'bottom'][n] },
        summary: { compressed: ['Ideas for review', 'Research in focus', 'Relationships', 'Planned work'][n] },
        objectPlural: objectPlural[domain],
        intelligence: { active: n === 1 }, drawer: {},
      })),
    },
  }];
}));
