'use strict';

/**
 * SPEC-117 — Queue Intelligence + campaign pacing.
 * Rank by Max priority, signal freshness, expected response, capacity, diversity.
 */

const { clone } = require('./types');
const { paceVerticals } = require('./Pacing');

function daysSince(value, now) {
  if (!value) return 999;
  const then = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(then.getTime())) return 999;
  const ts = now instanceof Date ? now : new Date(now || Date.now());
  return Math.max(0, (ts.getTime() - then.getTime()) / (86400000));
}

function queueScore(item, now) {
  const priority = Number(item.maxPriority ?? item.priority ?? 0);
  const freshnessDays = daysSince(item.buyingSignalAt || item.signalAt, now);
  const freshness = freshnessDays <= 1 ? 1 : freshnessDays <= 7 ? 0.7 : freshnessDays <= 21 ? 0.4 : 0.15;
  const expectedResponse = Number(item.expectedResponse ?? item.replyLikelihood ?? 0.05);
  const icp = Number(item.icpScore ?? item.icp_score ?? 0) / 100;
  return {
    total: round3(priority * 0.35 + freshness * 0.25 + expectedResponse * 0.25 + icp * 0.15),
    priority,
    freshness,
    expectedResponse,
    icp,
    freshnessDays,
  };
}

function round3(value) {
  return Math.round(value * 1000) / 1000;
}

function buildTodayQueue(input = {}) {
  const now = input.now || new Date();
  const recommended = Math.max(0, Number(input.recommendedCapacity ?? input.capacity?.recommended ?? 0));
  const prospects = Array.isArray(input.prospects) ? input.prospects : [];
  const scored = prospects.map((prospect) => {
    const parts = queueScore(prospect, now);
    return {
      prospectId: prospect.id || prospect.prospectId,
      email: prospect.email,
      vertical: String(prospect.vertical || 'unknown').toLowerCase(),
      company: prospect.company || prospect.company_name || null,
      maxPriority: parts.priority,
      buyingSignalAt: prospect.buyingSignalAt || prospect.signalAt || null,
      expectedResponse: parts.expectedResponse,
      icpScore: Number(prospect.icpScore ?? prospect.icp_score ?? 0),
      paige: prospect.paige || prospect.content || null,
      contentSource: prospect.contentSource || prospect.paige?.author || prospect.paige?.source || null,
      dnc: prospect.dnc === true || prospect.do_not_contact === true,
      ranking: parts,
      maxReason: prospect.maxReason || prospect.reason || null,
    };
  });

  scored.sort((a, b) => b.ranking.total - a.ranking.total);
  const paced = paceVerticals(scored);
  const selected = paced.slice(0, recommended).map((item, index) => ({
    ...clone(item),
    position: index + 1,
    sendable: Boolean(item.paige?.subject && item.paige?.body && !item.dnc && (item.contentSource === 'paige' || item.paige?.author === 'paige' || item.paige?.source === 'paige')),
  }));

  return {
    kind: 'today_queue',
    spec: 'SPEC-117',
    recommended,
    candidateCount: prospects.length,
    selectedCount: selected.length,
    items: selected,
    pacing: paced.slice(0, Math.max(selected.length, 8)).map((row) => row.vertical),
    statement: recommended > 0
      ? `Today's Queue: ${selected.length} recommended. Highest expected ROI. Safest delivery order.`
      : 'Today\'s Queue is empty because recommended capacity is 0.',
  };
}

module.exports = {
  buildTodayQueue,
  queueScore,
  daysSince,
};
