'use strict';

const { StrategyRegistry, createDefaultStrategyRegistry } = require('./StrategyRegistry');
const { OpportunityStrategy } = require('./OpportunityStrategy');
const { EngagementStrategy } = require('./EngagementStrategy');
const { RelationshipStrategy } = require('./RelationshipStrategy');
const { DecisionMakerStrategy } = require('./DecisionMakerStrategy');
const { OverflowStrategy } = require('./OverflowStrategy');
const { TechnologyStrategy } = require('./TechnologyStrategy');
const { RiskStrategy } = require('./RiskStrategy');
const {
  strategyResult,
  confidenceFromEvidence,
  assertStrategy,
} = require('./StrategyInterface');

module.exports = {
  StrategyRegistry,
  createDefaultStrategyRegistry,
  OpportunityStrategy,
  EngagementStrategy,
  RelationshipStrategy,
  DecisionMakerStrategy,
  OverflowStrategy,
  TechnologyStrategy,
  RiskStrategy,
  strategyResult,
  confidenceFromEvidence,
  assertStrategy,
};
