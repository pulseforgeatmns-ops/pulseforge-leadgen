'use strict';

/**
 * Deterministic Business Intelligence reasoning (SPEC-053 / ADR-037).
 * Levels 1–5: Facts → Model → Operations → Buying psychology → Sales input.
 * Never invents company facts. Never writes channel prose.
 */

const {
  CONFIDENCE_LABEL,
  buildBusinessIntelligenceProfile,
  normalizeConfidenceLabel,
} = require('./types');
const { buildBusinessSignalsForProspect } = require('../signals');
const { buildClientPlaybook } = require('../playbook/types');

/**
 * Industry → operational archetype.
 * Keys matched via includes() on lowercased industry.
 */
const INDUSTRY_MODELS = Object.freeze([
  {
    match: [/law\s*firm/, /attorney/, /legal\s*practice/, /\blegal\b/],
    model: {
      business_model: 'Professional services',
      revenue_model: 'Billable attorney time',
      primary_customers: 'Businesses and individuals needing legal counsel',
      growth_strategy: 'Add attorneys and practice areas; deepen client relationships',
      competitive_position: 'Local / regional practice competing on trust and responsiveness',
      operational_constraints: [
        'Administrative overhead reduces billable utilization',
        'Staff capacity around scheduling, filings, and client intake',
      ],
      likely_kpis: [
        'Attorney utilization',
        'Client satisfaction',
        'Realization / collection rate',
      ],
      cost_drivers: [
        'Attorney compensation',
        'Administrative staff',
        'Office facilities',
      ],
      risk_factors: [
        'Client attrition',
        'Missed deadlines / process errors',
        'Facility or vendor distractions during billable hours',
      ],
      buying_triggers: [
        'Growth in headcount',
        'Office move or renovation',
        'Vendor reliability failures',
      ],
      decision_makers: ['Managing Partner', 'Office Manager', 'Office Administrator'],
      vendor_landscape: 'Facilities and cleaning typically managed in-house or via small vendors',
      seasonality: 'Litigation and closing calendars drive uneven office activity',
      service_angle:
        'Reliable facility operations protect productive billable hours',
    },
  },
  {
    match: [/account/, /\bcpa\b/, /bookkeep/],
    model: {
      business_model: 'Professional services',
      revenue_model: 'Billable accountant / advisor time and retainers',
      primary_customers: 'Small and mid-market businesses needing accounting / tax',
      growth_strategy: 'Expand client book; add advisory services',
      competitive_position: 'Local firm competing on accuracy, responsiveness, and trust',
      operational_constraints: [
        'Seasonal capacity crunch (tax season)',
        'Administrative work competing with billable advisory time',
      ],
      likely_kpis: ['Billable utilization', 'Client retention', 'On-time filings'],
      cost_drivers: ['Professional staff', 'Software', 'Office overhead'],
      risk_factors: ['Seasonal burnout', 'Client deadline misses', 'Office disruptions'],
      buying_triggers: [
        'Pre-tax-season readiness',
        'Office expansion',
        'Staff growth',
      ],
      decision_makers: ['Managing Partner', 'Office Manager', 'Firm Administrator'],
      vendor_landscape: 'Lean vendor set; facilities often secondary priority until failure',
      seasonality: 'Peak load January–April',
      service_angle:
        'Quiet, reliable facility care protects focus during peak billable periods',
    },
  },
  {
    match: [/property\s*management/, /real\s*estate/, /commercial\s*propert/],
    model: {
      business_model: 'Property / asset management services',
      revenue_model: 'Management fees tied to units / square footage under management',
      primary_customers: 'Property owners and asset managers',
      growth_strategy: 'Win additional properties; expand geographic coverage',
      competitive_position: 'Competes on owner trust, tenant experience, and vendor control',
      operational_constraints: [
        'Vendor coordination overhead across sites',
        'Tenant complaint volume consuming manager attention',
      ],
      likely_kpis: [
        'Tenant satisfaction',
        'Owner retention',
        'Maintenance / vendor response time',
      ],
      cost_drivers: ['On-site staff', 'Vendors', 'Turnover / make-ready'],
      risk_factors: [
        'Inconsistent building presentation',
        'Vendor no-shows',
        'Owner escalation',
      ],
      buying_triggers: [
        'Vendor transition',
        'New property acquisition',
        'Repeat tenant complaints',
      ],
      decision_makers: ['Property Manager', 'Regional Manager', 'Owner'],
      vendor_landscape: 'Multiple facility vendors; cleaning is high-friction if unreliable',
      seasonality: 'Lease cycles and weather-driven common-area load',
      service_angle:
        'Dependable cleaning reduces vendor management burden and tenant complaints',
    },
  },
  {
    match: [/dental/, /dentist/, /orthodont/],
    model: {
      business_model: 'Healthcare professional services',
      revenue_model: 'Patient chair time and procedure mix',
      primary_customers: 'Patients / families in local market',
      growth_strategy: 'Increase chair utilization; add providers or locations',
      competitive_position: 'Local practice competing on patient experience and reviews',
      operational_constraints: [
        'Clinical schedule density',
        'Front-desk and hygiene room turnaround',
      ],
      likely_kpis: ['Chair utilization', 'Patient satisfaction', 'Recall compliance'],
      cost_drivers: ['Clinical staff', 'Supplies', 'Facility presentation'],
      risk_factors: ['No-shows', 'Infection-control perception', 'Staff turnover'],
      buying_triggers: ['New operatories', 'Remodel', 'Staff growth'],
      decision_makers: ['Practice Owner', 'Practice Manager', 'Office Manager'],
      vendor_landscape: 'Clinical + facility vendors; appearance affects patient trust',
      seasonality: 'School calendars influence pediatric / family volume',
      service_angle:
        'Consistent facility standards protect patient experience between appointments',
    },
  },
  {
    match: [/medical/, /clinic/, /healthcare/, /med\s*spa/, /physician/],
    model: {
      business_model: 'Healthcare services',
      revenue_model: 'Patient visits, procedures, and care pathways',
      primary_customers: 'Patients and referring providers',
      growth_strategy: 'Add providers, services, or locations',
      competitive_position: 'Local clinic competing on access, outcomes, and experience',
      operational_constraints: [
        'Room turnover and staffing',
        'Compliance and cleanliness expectations',
      ],
      likely_kpis: ['Patient throughput', 'Satisfaction scores', 'No-show rate'],
      cost_drivers: ['Clinical labor', 'Compliance', 'Facility operations'],
      risk_factors: ['Infection-control risk', 'Reputation damage', 'Staff distraction'],
      buying_triggers: ['Expansion', 'Inspection readiness', 'Vendor gaps'],
      decision_makers: ['Practice Manager', 'Administrator', 'Owner / Medical Director'],
      vendor_landscape: 'Specialized cleaning often required; reliability is non-negotiable',
      seasonality: 'Illness seasons and elective procedure calendars',
      service_angle:
        'Reliable facility operations support clinical focus and patient confidence',
    },
  },
  {
    match: [/restaurant/, /cafe/, /dining/, /food\s*service/],
    model: {
      business_model: 'Hospitality / food service',
      revenue_model: 'Covers and average check',
      primary_customers: 'Local diners and catering clients',
      growth_strategy: 'Increase covers; expand hours or locations',
      competitive_position: 'Local competition on food quality, service, and cleanliness',
      operational_constraints: [
        'Kitchen and dining room turnaround',
        'Health-code and guest perception pressure',
      ],
      likely_kpis: ['Covers', 'Ticket time', 'Guest reviews'],
      cost_drivers: ['Food cost', 'Labor', 'Facility / grease / waste'],
      risk_factors: ['Health inspection failures', 'Negative reviews', 'Staff churn'],
      buying_triggers: ['Inspection prep', 'Remodel', 'Volume growth'],
      decision_makers: ['Owner', 'General Manager'],
      vendor_landscape: 'Specialized cleaning vendors; consistency affects guest trust',
      seasonality: 'Holiday and weather-driven demand swings',
      service_angle:
        'Consistent facility standards protect guest experience and inspection readiness',
    },
  },
  {
    match: [/manufactur/, /factory/, /industrial/, /warehouse/],
    model: {
      business_model: 'Manufacturing / industrial operations',
      revenue_model: 'Production output and fulfilled orders',
      primary_customers: 'B2B buyers of manufactured goods',
      growth_strategy: 'Increase throughput; add shifts or square footage',
      competitive_position: 'Competes on delivery reliability and quality',
      operational_constraints: [
        'Floor capacity and shift staffing',
        'Safety and cleanliness of production areas',
      ],
      likely_kpis: ['Throughput', 'On-time delivery', 'Safety incidents'],
      cost_drivers: ['Labor', 'Materials', 'Facility / equipment maintenance'],
      risk_factors: ['Downtime', 'Safety events', 'Quality escapes'],
      buying_triggers: ['Expansion', 'Audit readiness', 'Shift growth'],
      decision_makers: ['Plant Manager', 'Operations Manager', 'Owner'],
      vendor_landscape: 'Industrial cleaning often outsourced; reliability protects uptime',
      seasonality: 'Order cycles and seasonal demand',
      service_angle:
        'Dependable facility care supports safe throughput and audit readiness',
    },
  },
  {
    match: [/retail/, /store/, /boutique/, /shop\b/],
    model: {
      business_model: 'Retail',
      revenue_model: 'In-store sales and conversion',
      primary_customers: 'Local consumers',
      growth_strategy: 'Increase traffic and conversion; add locations',
      competitive_position: 'Competes on assortment, experience, and store presentation',
      operational_constraints: [
        'Store presentation standards',
        'Staff time spent on cleaning vs selling',
      ],
      likely_kpis: ['Sales per square foot', 'Conversion', 'Guest reviews'],
      cost_drivers: ['Inventory', 'Labor', 'Store facilities'],
      risk_factors: ['Poor first impression', 'Staff distraction', 'Shrink / clutter'],
      buying_triggers: ['Remodel', 'New location', 'Staffing pressure'],
      decision_makers: ['Owner', 'Store Manager', 'District Manager'],
      vendor_landscape: 'Janitorial often after-hours; consistency protects brand',
      seasonality: 'Holiday peaks and clearance cycles',
      service_angle:
        'Consistent store presentation frees staff to focus on customers',
    },
  },
  {
    match: [/salon/, /spa/, /fitness/, /gym/, /studio/],
    model: {
      business_model: 'Local consumer services',
      revenue_model: 'Appointments / memberships',
      primary_customers: 'Local consumers',
      growth_strategy: 'Increase utilization; add chairs, classes, or locations',
      competitive_position: 'Local reputation and experience-driven',
      operational_constraints: [
        'Room / station turnover',
        'Cleanliness perception between clients',
      ],
      likely_kpis: ['Utilization', 'Retention', 'Reviews'],
      cost_drivers: ['Staff', 'Rent', 'Facility presentation'],
      risk_factors: ['Negative hygiene perception', 'No-shows', 'Staff turnover'],
      buying_triggers: ['Expansion', 'Remodel', 'Review pressure'],
      decision_makers: ['Owner', 'Manager'],
      vendor_landscape: 'Cleaning often owner-managed until growth forces outsourcing',
      seasonality: 'New-year and holiday demand swings',
      service_angle:
        'Reliable facility standards protect client experience between visits',
    },
  },
]);

const DEFAULT_MODEL = Object.freeze({
  business_model: 'Local service / professional business',
  revenue_model: 'Service delivery billed to customers or clients',
  primary_customers: 'Local customers or clients',
  growth_strategy: 'Grow revenue through capacity, referrals, and retention',
  competitive_position: 'Competitive position not yet evidenced',
  operational_constraints: [
    'Management attention split between delivery and operations',
    'Vendor and facility issues compete with core work',
  ],
  likely_kpis: ['Revenue', 'Customer retention', 'Operational consistency'],
  cost_drivers: ['Labor', 'Facilities', 'Vendor spend'],
  risk_factors: ['Inconsistent operations', 'Reputation risk', 'Management distraction'],
  buying_triggers: ['Growth pressure', 'Vendor failure', 'Facility change'],
  decision_makers: ['Owner', 'Operations Manager'],
  vendor_landscape: 'Vendor landscape not yet evidenced',
  seasonality: 'Seasonality not yet evidenced',
  service_angle:
    'Dependable facility operations reduce management burden so the team can focus on core work',
});

/**
 * Derive one Business Intelligence Profile.
 * @param {object} prospect
 * @param {object} [ctx]
 * @returns {object}
 */
function deriveBusinessIntelligence(prospect, ctx = {}) {
  const playbook = ctx.playbook
    ? buildClientPlaybook(ctx.playbook)
    : null;
  const intel =
    prospect.companyIntelligence ||
    prospect.businessIntelligence ||
    ctx.companyIntelligence ||
    null;

  const company = resolveCompany(prospect, intel);
  const industry = resolveIndustry(prospect, intel, playbook);
  const evidenceRefs = [];
  const uncertainty = [];
  const facts = collectFacts(prospect, intel, evidenceRefs);

  const signalPkg =
    ctx.signalPackage ||
    buildBusinessSignalsForProspect(prospect, {
      knowledge: ctx.knowledge || {},
      playbook,
      asOf: ctx.asOf,
    });
  const activeSignals = Array.isArray(signalPkg.activeSignals)
    ? signalPkg.activeSignals
    : [];
  for (const s of activeSignals.slice(0, 5)) {
    if (s.id) evidenceRefs.push(s.id);
    else if (s.type) evidenceRefs.push(`signal:${s.type}`);
  }

  const archetype = resolveArchetype(industry);
  if (!industry) {
    uncertainty.push('Industry not evidenced — business model inferred at low confidence');
  }
  if (archetype.isDefault && industry) {
    uncertainty.push(
      `No specialized model for "${industry}" — using general local-business archetype`
    );
  }

  const model = { ...archetype.model };
  const expansion = collectExpansionSignals(activeSignals, prospect);
  if (expansion.length) {
    model.expansion_signals = expansion;
    model.buying_triggers = uniqueList([
      ...model.buying_triggers,
      ...expansion.map((e) => `Signal: ${e}`),
    ]);
  } else {
    model.expansion_signals = [];
    uncertainty.push('No expansion signals evidenced');
  }

  if (!prospect.website && !(intel && intel.website)) {
    uncertainty.push('Website not evidenced — public positioning unknown');
  }
  if (
    model.competitive_position ===
    'Competitive position not yet evidenced'
  ) {
    uncertainty.push('Competitive position not evidenced');
  }
  if (model.seasonality === 'Seasonality not yet evidenced') {
    uncertainty.push('Seasonality not evidenced');
  }
  if (
    model.vendor_landscape ===
    'Vendor landscape not yet evidenced'
  ) {
    uncertainty.push('Vendor landscape not evidenced');
  }

  const title = String(
    prospect.jobTitle ||
      prospect.title ||
      prospect.contactTitle ||
      ''
  ).trim();
  if (title) {
    model.decision_makers = uniqueList([title, ...model.decision_makers]);
    evidenceRefs.push(`title:${title}`);
  } else {
    uncertainty.push('Decision-maker title not evidenced — role inferred from industry');
  }

  if (playbook && playbook.valuePropositions && playbook.valuePropositions[0]) {
    model.service_angle = `${model.service_angle} (${playbook.valuePropositions[0]})`;
  }

  const qualityAnswers = {
    howTheyMakeMoney: model.revenue_model,
    growthConstraints: (model.operational_constraints || [])[0] || '',
    operationalPressures: (model.operational_constraints || [])
      .slice(0, 2)
      .join('; '),
    problemOwner: (model.decision_makers || [])[0] || '',
    whyBuyNow: (model.buying_triggers || [])[0] || '',
    operationalLeverage:
      'Reduce non-core facility / vendor friction so capacity returns to revenue work',
    outcomesThatMatter: (model.likely_kpis || []).slice(0, 3).join('; '),
  };

  const reasoningLayers = {
    level1_facts: facts,
    level2_business_model: model.business_model,
    level3_operational_model: [
      `Revenue: ${model.revenue_model}`,
      `Constraints: ${(model.operational_constraints || []).join('; ')}`,
      `Growth: ${model.growth_strategy}`,
    ],
    level4_buying_psychology: [
      `Triggers: ${(model.buying_triggers || []).join('; ')}`,
      `Risks: ${(model.risk_factors || []).join('; ')}`,
      `KPIs: ${(model.likely_kpis || []).join('; ')}`,
    ],
    level5_sales_input: [
      `Angle: ${model.service_angle}`,
      `Owner: ${qualityAnswers.problemOwner}`,
      `CTA seed: walkthrough / facility conversation tied to ${qualityAnswers.whyBuyNow || 'operational pressure'}`,
    ],
  };

  const confidenceScore = scoreConfidence({
    industry,
    facts,
    activeSignals,
    title,
    isDefault: archetype.isDefault,
    uncertainty,
  });

  return buildBusinessIntelligenceProfile({
    prospectId: prospect.id != null ? String(prospect.id) : null,
    company,
    industry,
    ...model,
    qualityAnswers,
    reasoningLayers,
    uncertainty,
    confidence: normalizeConfidenceLabel(confidenceScore),
    confidenceScore,
    evidenceRefs: [...new Set(evidenceRefs.filter(Boolean))],
    derivedAt: new Date().toISOString(),
  });
}

/**
 * @param {object[]} prospects
 * @param {object} [ctx]
 */
function deriveBusinessIntelligenceStage(prospects, ctx = {}) {
  const list = Array.isArray(prospects) ? prospects : [];
  const profiles = list.map((p) => deriveBusinessIntelligence(p, ctx));
  /** @type {Record<string, object>} */
  const byProspectId = {};
  for (const profile of profiles) {
    if (profile.prospectId) byProspectId[profile.prospectId] = profile;
    if (profile.company) {
      byProspectId[`company:${profile.company.toLowerCase()}`] = profile;
    }
  }
  return { profiles, byProspectId };
}

function resolveCompany(prospect, intel) {
  return String(
    (intel && (intel.companyName || intel.company || intel.name)) ||
      prospect.companyName ||
      prospect.company ||
      prospect.name ||
      ''
  ).trim();
}

function resolveIndustry(prospect, intel, playbook) {
  const fromIntel =
    intel &&
    (intel.industry ||
      (intel.firmographics && intel.firmographics.industry));
  if (fromIntel) return String(fromIntel).trim();
  if (prospect.industry) return String(prospect.industry).trim();
  if (playbook && playbook.targetMarkets && playbook.targetMarkets[0]) {
    return String(playbook.targetMarkets[0]).trim();
  }
  return '';
}

function resolveArchetype(industry) {
  const key = String(industry || '').toLowerCase();
  if (key) {
    for (const entry of INDUSTRY_MODELS) {
      if (entry.match.some((re) => re.test(key))) {
        return {
          isDefault: false,
          model: {
            ...entry.model,
            operational_constraints: [...entry.model.operational_constraints],
            likely_kpis: [...entry.model.likely_kpis],
            cost_drivers: [...entry.model.cost_drivers],
            risk_factors: [...entry.model.risk_factors],
            buying_triggers: [...entry.model.buying_triggers],
            decision_makers: [...entry.model.decision_makers],
          },
        };
      }
    }
  }
  return {
    isDefault: true,
    model: {
      ...DEFAULT_MODEL,
      operational_constraints: [...DEFAULT_MODEL.operational_constraints],
      likely_kpis: [...DEFAULT_MODEL.likely_kpis],
      cost_drivers: [...DEFAULT_MODEL.cost_drivers],
      risk_factors: [...DEFAULT_MODEL.risk_factors],
      buying_triggers: [...DEFAULT_MODEL.buying_triggers],
      decision_makers: [...DEFAULT_MODEL.decision_makers],
    },
  };
}

function collectFacts(prospect, intel, evidenceRefs) {
  const facts = [];
  const industry = resolveIndustry(prospect, intel, null);
  if (industry) {
    facts.push(`Industry: ${industry}`);
    evidenceRefs.push(
      prospect.industry
        ? `prospect.industry:${industry}`
        : `company_intelligence.industry:${industry}`
    );
  }
  const location =
    prospect.address ||
    prospect.mailingAddress ||
    prospect.location ||
    (intel && (intel.address || intel.location)) ||
    '';
  if (location) {
    facts.push(`Location: ${location}`);
    evidenceRefs.push(`address:${location}`);
  }
  const website = prospect.website || (intel && intel.website) || '';
  if (website) {
    facts.push(`Website: ${website}`);
    evidenceRefs.push(`website:${website}`);
  }
  const employees =
    prospect.employees ||
    prospect.employeeCount ||
    (intel && (intel.employees || intel.employeeCount)) ||
    null;
  if (employees != null && employees !== '') {
    facts.push(`Employees: ${employees}`);
    evidenceRefs.push(`employees:${employees}`);
  }
  const title = prospect.jobTitle || prospect.title || '';
  if (title) facts.push(`Contact title: ${title}`);
  if (!facts.length) {
    facts.push('Limited Level-1 facts available');
  }
  return facts;
}

function collectExpansionSignals(activeSignals, prospect) {
  const out = [];
  for (const s of activeSignals) {
    const label = s.title || s.type || s.description;
    if (label && /hir|staff|headcount|expans|growth|location|renovat|lease|acquisit/i.test(String(label))) {
      out.push(String(label));
    }
  }
  if (prospect.notes && /hiring|expanding|new location/i.test(String(prospect.notes))) {
    out.push(String(prospect.notes).slice(0, 120));
  }
  return uniqueList(out).slice(0, 5);
}

function scoreConfidence(args) {
  let score = 0.2;
  if (args.industry) score += 0.2;
  if (!args.isDefault && args.industry) score += 0.15;
  if ((args.facts || []).length >= 2) score += 0.1;
  if ((args.facts || []).length >= 3) score += 0.08;
  if ((args.activeSignals || []).length) score += 0.12;
  if (args.title) score += 0.1;
  const unc = (args.uncertainty || []).length;
  if (unc >= 3) score -= 0.1;
  if (unc >= 5) score -= 0.08;
  return Math.max(0, Math.min(1, Number(score.toFixed(3))));
}

function uniqueList(list) {
  const seen = new Set();
  const out = [];
  for (const item of list || []) {
    const s = String(item).trim();
    if (!s) continue;
    const key = s.toLowerCase();
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(s);
  }
  return out;
}

module.exports = {
  INDUSTRY_MODELS,
  DEFAULT_MODEL,
  deriveBusinessIntelligence,
  deriveBusinessIntelligenceStage,
  resolveCompany,
  resolveIndustry,
  resolveArchetype,
};
