'use strict';

const { randomUUID } = require('crypto');
const { DEFAULT_PAPER_CONFIG } = require('../config/defaultConfig');

/**
 * Paper-only execution — no blockchain or broker integration.
 */
class PaperPortfolio {
  /**
   * @param {import('../storage/InMemorySignalStore').InMemorySignalStore} store
   * @param {Partial<typeof DEFAULT_PAPER_CONFIG>} [config]
   */
  constructor(store, config = {}) {
    this.store = store;
    this.config = { ...DEFAULT_PAPER_CONFIG, ...config };
  }

  getOpenPosition(tokenAddress) {
    return this.store.paperPositions.find(
      p => p.tokenAddress === tokenAddress && (p.status === 'OPEN' || p.status === 'DE_RISKED')
    );
  }

  /**
   * @param {object} args
   * @param {string} args.tokenAddress
   * @param {Date|string} args.signalAt
   * @param {number} args.marketPriceAtSignal
   * @param {string} args.entrySnapshotId
   * @param {{ occurredAt: Date|string, price: number }[]} [args.pricePath]
   */
  openEntry(args) {
    const executed = simulateExecution({
      signalAt: args.signalAt,
      pricePath: args.pricePath || [{ occurredAt: args.signalAt, price: args.marketPriceAtSignal }],
      delaySeconds: this.config.executionDelaySeconds,
      side: 'buy',
      referencePrice: args.marketPriceAtSignal,
    });

    const position = this.store.insertPaperPosition({
      tokenAddress: args.tokenAddress,
      openedAt: executed.executedAt,
      entryPrice: executed.fillPrice,
      entryMarketCap: args.entryMarketCap || null,
      notionalUsd: this.config.notionalUsd,
      remainingPct: 100,
      realizedPnlUsd: 0,
      unrealizedPnlUsd: 0,
      status: 'OPEN',
      entrySnapshotId: args.entrySnapshotId,
    });

    this.store.insertPaperTransaction({
      positionId: position.id,
      executedAt: executed.executedAt,
      side: 'BUY',
      pct: 100,
      price: executed.fillPrice,
      notionalUsd: this.config.notionalUsd,
      feesUsd: executed.feesUsd,
      slippageUsd: executed.slippageUsd,
    });

    return position;
  }

  /**
   * @param {object} args
   * @param {string} args.positionId
   * @param {number} args.sellPct
   * @param {Date|string} args.signalAt
   * @param {number} args.marketPriceAtSignal
   * @param {{ occurredAt: Date|string, price: number }[]} [args.pricePath]
   */
  reducePosition(args) {
    const position = this.store.paperPositions.find(p => p.id === args.positionId);
    if (!position) throw new Error('position not found');

    const executed = simulateExecution({
      signalAt: args.signalAt,
      pricePath: args.pricePath || [{ occurredAt: args.signalAt, price: args.marketPriceAtSignal }],
      delaySeconds: this.config.executionDelaySeconds,
      side: 'sell',
      referencePrice: args.marketPriceAtSignal,
    });

    const soldNotional = (position.notionalUsd * args.sellPct) / 100;
    const pnl = soldNotional * (executed.fillPrice / position.entryPrice - 1) - executed.feesUsd - executed.slippageUsd;

    position.remainingPct -= args.sellPct;
    position.realizedPnlUsd += pnl;
    if (position.remainingPct <= 0.001) {
      position.remainingPct = 0;
      position.status = 'CLOSED';
      position.closedAt = executed.executedAt;
    } else if (args.sellPct < 100 && position.status === 'OPEN') {
      position.status = 'DE_RISKED';
    }

    this.store.insertPaperTransaction({
      positionId: position.id,
      executedAt: executed.executedAt,
      side: 'SELL',
      pct: args.sellPct,
      price: executed.fillPrice,
      notionalUsd: soldNotional,
      feesUsd: executed.feesUsd,
      slippageUsd: executed.slippageUsd,
    });

    return position;
  }

  markUnrealized(position, currentPrice) {
    if (!position || position.status === 'CLOSED') return position;
    const remainingNotional = (position.notionalUsd * position.remainingPct) / 100;
    position.unrealizedPnlUsd = remainingNotional * (currentPrice / position.entryPrice - 1);
    return position;
  }
}

function simulateExecution({ signalAt, pricePath, delaySeconds, side, referencePrice }) {
  const signalMs = new Date(signalAt).getTime();
  const targetMs = signalMs + delaySeconds * 1000;
  const path = [...pricePath]
    .map(p => ({ occurredAt: new Date(p.occurredAt), price: Number(p.price) }))
    .sort((a, b) => a.occurredAt - b.occurredAt);

  let fillPrice = referencePrice;
  for (const point of path) {
    if (point.occurredAt.getTime() >= targetMs) {
      fillPrice = point.price;
      break;
    }
  }

  const slippageBps = side === 'buy' ? 80 : 80;
  const feeBps = 30;
  const notional = 100;
  const slippageUsd = (notional * slippageBps) / 10000;
  const feesUsd = (notional * feeBps) / 10000;

  if (side === 'buy') {
    fillPrice = fillPrice * (1 + slippageBps / 10000);
  } else {
    fillPrice = fillPrice * (1 - slippageBps / 10000);
  }

  return {
    executedAt: new Date(targetMs),
    fillPrice,
    slippageUsd,
    feesUsd,
  };
}

module.exports = {
  PaperPortfolio,
  simulateExecution,
};
