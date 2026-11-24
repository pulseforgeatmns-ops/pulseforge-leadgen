// Fictional, deliberately disjoint records. No production IDs or data providers.
const domains = ['content', 'acquisition', 'clients', 'campaigns'];
export const fixtures = Object.fromEntries(['harbor', 'cedar'].map((id, index) => [id, {
  id, name: index ? 'Cedar Studio' : 'Harbor Works',
  records: domains.map((domain, n) => ({
    id: `${id}-${domain}`, domain,
    name: `${index ? 'Cedar' : 'Harbor'} ${['editorial plan', 'prospect research', 'client review', 'autumn campaign'][n]}`,
    summary: ['Two draft ideas ready for investigation.', 'Three fictional businesses awaiting research.', 'A quarterly review is approaching.', 'An upcoming campaign is in planning.'][n],
    evidence: ['Fixture note: operator review requested.', 'Fixture note: no execution has been authorized.'],
  })),
  overview: {
    maxAnchor: { headline: 'Business context', subline: 'Four connected domains' },
    domains: domains.map((domain, n) => ({
      id: domain, label: domain[0].toUpperCase() + domain.slice(1),
      priority: n === 1 ? 'elevated' : 'normal',
      position: { slot: ['top', 'left', 'right', 'bottom'][n] },
      summary: { compressed: ['Ideas for review', 'Research in focus', 'Relationships', 'Planned work'][n] },
      intelligence: { active: n === 1 }, drawer: {},
    })),
  },
}]));
