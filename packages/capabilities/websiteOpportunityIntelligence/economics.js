'use strict';

const { DEFAULT_ECONOMICS_CONFIG } = require('./types');

function estimateOperatorHours({ findings, business, config = {} }) {
  const cfg = { ...DEFAULT_ECONOMICS_CONFIG, ...config };
  let hours = 12;

  const perfIssues = findings.filter((f) => f.category === 'performance').length;
  const a11yIssues = findings.filter((f) => f.category === 'accessibility').length;
  const techIssues = findings.filter((f) => f.category === 'technical_health').length;
  const convIssues = findings.filter((f) => f.id === 'conv_no_obvious_path').length;

  hours += Math.min(8, perfIssues * 2);
  hours += Math.min(6, a11yIssues * 1.5);
  hours += Math.min(4, techIssues);
  if (convIssues) hours += 3;
  if (business.multi_location) hours += 6;

  return Math.round(Math.max(8, Math.min(cfg.operatorCapacityHours, hours)));
}

function estimateContractValue({ findings, business, operatorHours, config = {} }) {
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
    label: 'estimate',
  };
}

function computeProjectEconomics({ findings, business, config = {} }) {
  const cfg = { ...DEFAULT_ECONOMICS_CONFIG, ...config };
  const estimated_operator_hours = estimateOperatorHours({ findings, business, config: cfg });
  const contract = estimateContractValue({ findings, business, operatorHours: estimated_operator_hours, config: cfg });
  const operator_labor_cost = estimated_operator_hours * cfg.operatorHourlyRate;
  const estimated_direct_costs = cfg.defaultDirectCosts;
  const estimated_contribution =
    contract.estimated_contract_value - operator_labor_cost - estimated_direct_costs;

  const capacity_warning =
    estimated_operator_hours >= cfg.operatorCapacityHours * 0.75
      ? 'Estimated hours consume most of rolling 30-day operator capacity at current assumptions'
      : null;

  return {
    ...contract,
    estimated_operator_hours,
    operator_hourly_rate: cfg.operatorHourlyRate,
    operator_labor_cost,
    estimated_direct_costs,
    estimated_contribution,
    capacity_window_hours: cfg.operatorCapacityHours,
    capacity_warning,
    label: 'estimate',
    formula:
      'Estimated Contract Value - (Estimated Operator Hours × hourly rate) - Estimated Direct Project Costs = Estimated Contribution (before agent/overhead)',
  };
}

module.exports = {
  estimateOperatorHours,
  estimateContractValue,
  computeProjectEconomics,
  DEFAULT_ECONOMICS_CONFIG,
};
