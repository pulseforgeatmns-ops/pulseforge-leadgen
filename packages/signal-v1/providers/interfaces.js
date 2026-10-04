'use strict';

/**
 * Provider interfaces — adapters only; V1 runs on fixtures/mocks by default.
 *
 * @typedef {object} MarketDataProvider
 * @property {(tokenAddress: string, asOf: Date) => Promise<object>} getTokenSnapshot
 * @property {(tokenAddress: string, start: Date, end: Date) => Promise<{ occurredAt: Date, price: number }[]>} getHistoricalPrice
 * @property {(tokenAddress: string, asOf: Date) => Promise<object>} getLiquidity
 *
 * @typedef {object} SocialSignalProvider
 * @property {(args: object) => Promise<object[]>} getCalls
 *
 * @typedef {object} WalletDataProvider
 * @property {(walletAddress: string, args: object) => Promise<object[]>} getTransactions
 * @property {(walletAddress: string, asOf: Date) => Promise<object>} getHoldings
 *
 * @typedef {object} HolderDataProvider
 * @property {(tokenAddress: string, asOf: Date) => Promise<object>} getHolderDistribution
 */

module.exports = {};
