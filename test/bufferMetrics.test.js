const assert = require('node:assert/strict');
const { mapBufferPostMetrics, buildBufferMetricsQuery } = require('../analyticsAgent');

const mapped = mapBufferPostMetrics([
  { type: 'impressions', name: 'Impressions', value: 250, unit: 'number' },
  { type: 'comments', name: 'Comments', value: 3, unit: 'number' },
  { type: 'reactions', name: 'Reactions', value: 17, unit: 'number' },
  { type: 'reposts', name: 'Reposts', value: 1, unit: 'number' },
  { type: 'engagementRate', name: 'Engagement rate', value: 9.2, unit: 'percentage' },
]);

assert.deepEqual(mapped, {
  likes: 17,
  comments: 3,
  shares: 1,
  reach: 250,
  clicks: null,
  engagement_rate: '8.4000',
  buffer_engagement_rate: 9.2,
  share_metric_type: 'reposts',
});

const withSharesOnly = mapBufferPostMetrics([
  { type: 'impressions', name: 'Impressions', value: 100, unit: 'number' },
  { type: 'shares', name: 'Shares', value: 4, unit: 'number' },
]);

assert.equal(withSharesOnly.shares, 4);
assert.equal(withSharesOnly.share_metric_type, 'shares');

const withoutImpressions = mapBufferPostMetrics([
  { type: 'comments', name: 'Comments', value: 3, unit: 'number' },
  { type: 'reactions', name: 'Reactions', value: 17, unit: 'number' },
]);

assert.equal(withoutImpressions.engagement_rate, null);
assert.equal(withoutImpressions.reach, null);
assert.equal(withoutImpressions.likes, 17);
assert.equal(withoutImpressions.shares, null);

const withoutReactions = mapBufferPostMetrics([
  { type: 'impressions', name: 'Impressions', value: 250, unit: 'number' },
  { type: 'comments', name: 'Comments', value: 3, unit: 'number' },
]);

assert.equal(withoutReactions.likes, null);

const query = buildBufferMetricsQuery('post_123');
assert.match(query, /post\(input: \{ id: "post_123" \}\)/);
assert.match(query, /text/);
assert.match(query, /metrics \{\s+type\s+name\s+value\s+unit\s+\}/);
assert.match(query, /metricsUpdatedAt/);
assert.doesNotMatch(query, /statistics/);
assert.doesNotMatch(query, /likes/);
assert.doesNotMatch(query, /clicks/);

console.log('buffer metrics tests passed');
