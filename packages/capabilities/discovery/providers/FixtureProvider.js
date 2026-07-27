'use strict';

/**
 * Injectable fixture search provider for tests (SPEC-024).
 * Never used as silent production fabrication — must be injected explicitly.
 */

/**
 * @param {object[]} [candidates] - pre-built candidate list
 * @param {object} [options]
 * @param {(query: object, profile: object) => object[]} [options.generator]
 */
function createFixtureProvider(candidates, options = {}) {
  const list = Array.isArray(candidates) ? candidates : [];
  const generator =
    typeof options.generator === 'function' ? options.generator : null;

  return {
    id: 'fixture',
    available() {
      return true;
    },
    async search(query, profile) {
      if (generator) {
        return generator(query, profile);
      }
      const industry = String(query.industry || '').toLowerCase();
      return list.filter((c) => {
        if (!industry) return true;
        const hay = `${c.companyName || ''} ${c.industry || ''} ${c.snippet || ''}`.toLowerCase();
        return (
          hay.includes(industry.split(' ')[0]) ||
          String(c.industry || '')
            .toLowerCase()
            .includes(industry.split(' ')[0])
        );
      });
    },
  };
}

/**
 * Curated Manchester commercial candidates for deterministic tests.
 * Explicitly labeled fixture — not claimed as live Places results.
 */
function manchesterFixtureCandidates() {
  return [
    {
      companyName: 'Granite State Property Management',
      website: 'granitestatepm.com',
      industry: 'Commercial Property Management',
      address: '1000 Elm St, Manchester, NH 03101',
      phone: '603-555-0101',
      placeTypes: ['real_estate_agency'],
      source: 'fixture',
      snippet: 'commercial property management manchester',
    },
    {
      companyName: 'Maynard & Associates Law Offices',
      website: 'maynardlawnh.com',
      industry: 'Law Firms',
      address: '889 Elm Street, Manchester, NH 03101',
      phone: '603-555-0102',
      placeTypes: ['lawyer'],
      source: 'fixture',
      snippet: 'law firm attorney manchester nh',
    },
    {
      companyName: 'Queen City CPA Group',
      website: 'queencitycpa.com',
      industry: 'CPA Firms',
      address: '814 Elm St, Manchester, NH 03101',
      phone: '603-555-0103',
      placeTypes: ['accounting'],
      source: 'fixture',
      snippet: 'cpa accounting firm manchester',
    },
    {
      companyName: 'Riverwalk Dental Care',
      website: 'riverwalkdental.com',
      industry: 'Medical Offices',
      address: '300 Hanover St, Manchester, NH 03104',
      phone: '603-555-0104',
      placeTypes: ['dentist'],
      source: 'fixture',
      snippet: 'dental office manchester nh',
    },
    {
      companyName: 'Bedford Professional Plaza Tenants',
      website: 'bedfordproplaza.com',
      industry: 'Professional Offices',
      address: '5 Commerce Park N, Bedford, NH 03110',
      phone: '603-555-0105',
      placeTypes: ['establishment'],
      source: 'fixture',
      snippet: 'professional offices bedford nh',
    },
    {
      companyName: 'Hooksett Medical Associates',
      website: 'hooksettmedical.com',
      industry: 'Medical Offices',
      address: '20 Alice Ave, Hooksett, NH 03106',
      phone: '603-555-0106',
      placeTypes: ['doctor'],
      source: 'fixture',
      snippet: 'medical office hooksett',
    },
    {
      companyName: 'Londonderry Law Group PLLC',
      website: 'londonderrylaw.com',
      industry: 'Law Firms',
      address: '50 Nashua Rd, Londonderry, NH 03053',
      phone: '603-555-0107',
      placeTypes: ['lawyer'],
      source: 'fixture',
      snippet: 'law office attorney londonderry',
    },
    {
      companyName: 'Auburndale Accounting LLC',
      website: 'auburndalecpa.com',
      industry: 'CPA Firms',
      address: '12 Manchester Rd, Auburn, NH 03032',
      phone: '603-555-0108',
      placeTypes: ['accounting'],
      source: 'fixture',
      snippet: 'cpa firm auburn nh',
    },
    {
      companyName: 'Goffstown Property Partners',
      website: 'goffstownpp.com',
      industry: 'Commercial Property Management',
      address: '18 Main St, Goffstown, NH 03045',
      phone: '603-555-0109',
      placeTypes: ['real_estate_agency'],
      source: 'fixture',
      snippet: 'property management goffstown',
    },
    {
      companyName: 'Elm Street Financial Advisors',
      website: 'elmstreetfa.com',
      industry: 'Professional Offices',
      address: '250 Commercial St, Manchester, NH 03101',
      phone: '603-555-0110',
      placeTypes: ['finance'],
      source: 'fixture',
      snippet: 'financial advisor professional office manchester',
    },
    // Negatives for filter tests
    {
      companyName: 'Happy Homes Residential Cleaning',
      website: 'happyhomesclean.com',
      industry: 'Residential Cleaning',
      address: '44 Maple St, Manchester, NH 03101',
      phone: '603-555-0199',
      placeTypes: ['establishment'],
      source: 'fixture',
      snippet: 'residential house cleaning maid service',
    },
    {
      companyName: 'Closed Forever Law (Permanently Closed)',
      website: null,
      industry: 'Law Firms',
      address: '1 Old Mill Rd, Manchester, NH 03101',
      phone: null,
      placeTypes: ['lawyer'],
      source: 'fixture',
      snippet: 'permanently closed law firm',
    },
  ];
}

module.exports = {
  createFixtureProvider,
  manchesterFixtureCandidates,
};
