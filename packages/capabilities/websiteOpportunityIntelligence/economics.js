'use strict';

const { DEFAULT_ECONOMICS_CONFIG, ECONOMIC_CONFIDENCE } = require('./types');

function buildDefaultPlanningEconomics(config = {}) {
  const cfg = { ...DEFAULT_ECONOMICS_CONFIG, ...config };
  const estimated_operator_hours = 20;
  const estimated_contract_value = cfg.contractFloor + 750;
  const operator_labor_cost = estimated_operator_hours * cfg.operatorHourlyRate;
  const estimated_direct_costs = cfg.defaultDirectCosts;
  const estimated_contribution = estimated_contract_value - operator_labor_cost - estimated_direct_costs;

  return {
    estimated_contract_value,
    estimated_project_range: { low: cfg.contractFloor, high: cfg.contractFloor + 3500 },
    estimated_operator_hours,
    operator_hourly_rate: cfg.operatorHourlyRate,
    operator_labor_cost,
    estimated_direct_costs,
    estimated_contribution,
    capacity_window_hours: cfg.operatorCapacityHours,
    label: 'default_planning_estimate',
    purpose: 'Mission feasibility baseline — must not differentiate prospect priority alone',
    formula:
      'Default planning economics establish mission feasibility; prospect-specific values require evidence',
  };
}

function estimateProspectOperatorHours({ findings, business, config = {} }) {
  const cfg = { ...DEFAULT_ECONOMICS_CONFIG, ...config };
  let hours = 12;

  const perfIssues = findings.filter((f) => f.category === 'performance').length;
  const a11yIssues = findings.filter((f) => f.category === 'accessibility').length;
  const techIssues = findings.filter((f) => f.category === 'technical_health').length;
  const convIssues = findings.filter((f) => f.id === 'conv_no_obvious_path').length;
  const slowFetch = findings.some((f) =>
    f.ref === 'performance:fetch_ms' && (f.measurement?.fetch_ms ?? 0) >= 4000
  );

  hours += Math.min(8, perfIssues * 2);
  hours += Math.min(6, a11yIssues * 1.5);
  hours += Math.min(4, techIssues);
  if (convIssues) hours += 3;
  if (slowFetch) hours += 2;
  if (business.multi_location) hours += 6;

  return Math.round(Math.max(8, Math.min(cfg.operatorCapacityHours, hours)));
}

function estimateProspectContractValue({ findings, business, operatorHours, config = {} }) {
  const cfg = { ...DEFAULT_ECONOMICS_CONFIG, ...config };
  let low = cfg.contractFloor;
  let high = cfg.contractFloor + 1500;

  const deficiencyWeight = findings.filter((f) =>
    ['performance', 'accessibility', 'technical_health'].includes(f.category)
  ).length;

  if (deficiencyWeight >= 5) high += 2000;
  if (business.multi_location) high += 3000;
  if (operatorHours >= 25) high += 1500;

  const industry = String(business.industry || '').toLowerCase();
  if (/legal|dental|med_spa|architecture/.test(industry)) {
    low = Math.max(low, 3500);
    high = Math.max(high, 7500);
  }

  return {
    estimated_contract_value: Math.round((low + high) / 2),
    estimated_project_range: { low: Math.round(low), high: Math.round(high) },
    label: 'prospect_specific_estimate',
  };
}

function assessEconomicConfidence({ findings, business }) {
  const materialFindings = findings.filter((f) =>
    ['performance', 'accessibility', 'technical_health', 'conversion_structure'].includes(f.category)
  ).length;
  const hasIndustry = Boolean(business.industry || business.vertical);
  const hasScopeSignal = materialFindings >= 3 || business.multi_location;

  if (materialFindings >= 5 && hasIndustry && hasScopeSignal) {
    return ECONOMIC_CONFIDENCE.HIGH;
  }
  if (materialFindings >= 2 && hasIndustry) {
    return ECONOMIC_CONFIDENCE.MEDIUM;
  }
  if (materialFindings >= 1) {
    return ECONOMIC_CONFIDENCE.LOW;
  }
  return ECONOMIC_CONFIDENCE.UNKNOWN;
}

function computeProjectEconomics({ findings, business, config = {} }) {
  const cfg = { ...DEFAULT_ECONOMICS_CONFIG, ...config };
  const default_planning_economics = buildDefaultPlanningEconomics(cfg);
  const economic_confidence = assessEconomicConfidence({ findings, business });

  const estimated_operator_hours = estimateProspectOperatorHours({ findings, business, config: cfg });
  const contract = estimateProspectContractValue({
    findings,
    business,
    operatorHours: estimated_operator_hours,
    config: cfg,
  });
  const operator_labor_cost = estimated_operator_hours * cfg.operatorHourlyRate;
  const estimated_direct_costs = cfg.defaultDirectCosts;
  const estimated_contribution = contract.estimated_contract_value - operator_labor_cost - estimated_direct_costs;

  const prospect_specific_economics = {
    ...contract,
    estimated_operator_hours,
    operator_hourly_rate: cfg.operatorHourlyRate,
    operator_labor_cost,
    estimated_direct_costs,
    estimated_contribution,
    capacity_window_hours: cfg.operatorCapacityHours,
    capacity_warning:
      estimated_operator_hours >= cfg.operatorCapacityHours * 0.75
        ? 'Estimated hours consume most of rolling 30-day operator capacity at current assumptions'
        : null,
    label: 'prospect_specific_estimate',
    formula:
      'Estimated Contract Value - (Estimated Operator Hours × hourly rate) - Estimated Direct Project Costs = Estimated Contribution (before agent/overhead)',
  };

  const capacity_warning = prospect_specific_economics.capacity_warning;

  return {
    default_planning_economics,
    prospect_specific_economics,
    economic_confidence,
    estimated_contract_value: prospect_specific_economics.estimated_contract_value,
    estimated_project_range: prospect_specific_economics.estimated_project_range,
    estimated_operator_hours: prospect_specific_economics.estimated_operator_hours,
    operator_hourly_rate: prospect_specific_economics.operator_hourly_rate,
    operator_labor_cost: prospect_specific_economics.operator_labor_cost,
    estimated_direct_costs: prospect_specific_economics.estimated_direct_costs,
    estimated_contribution: prospect_specific_economics.estimated_contribution,
    capacity_window_hours: prospect_specific_economics.capacity_window_hours,
    capacity_warning,
    label: economic_confidence === ECONOMIC_CONFIDENCE.LOW || economic_confidence === ECONOMIC_CONFIDENCE.UNKNOWN
      ? 'default_planning_only'
      : 'prospect_specific_estimate',
    formula: prospect_specific_economics.formula,
  };
}

module.exports = {
  buildDefaultPlanningEconomics,
  estimateProspectOperatorHours,
  estimateProspectContractValue,
  assessEconomicConfidence,
  computeProjectEconomics,
  DEFAULT_ECONOMICS_CONFIG,
};
