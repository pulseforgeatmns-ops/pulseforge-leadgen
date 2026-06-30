const axios = require('axios');

const SERPAPI_URL = 'https://serpapi.com/search';

function normalizeOrganicResult(item, fallbackPosition) {
  return {
    ...item,
    title: item?.title || '',
    link: item?.link || '',
    snippet: item?.snippet || '',
    position: Number(item?.position) || fallbackPosition,
  };
}

async function searchSerpApi({ query, page = 0, num = 10, engine = 'google', timeoutMs = 20000 } = {}) {
  const startedAt = Date.now();
  if (!process.env.SERPAPI_KEY) {
    return {
      organicResults: [],
      requestCount: 0,
      error: { code: 'missing_key', message: 'SERPAPI_KEY is not configured' },
      durationMs: Date.now() - startedAt,
      page,
    };
  }

  try {
    const response = await axios.get(SERPAPI_URL, {
      params: {
        api_key: process.env.SERPAPI_KEY,
        q: query,
        num,
        start: page * num,
        engine,
      },
      timeout: timeoutMs,
    });
    const apiError = response.data?.error;
    return {
      organicResults: apiError
        ? []
        : (response.data?.organic_results || []).map((item, index) => normalizeOrganicResult(item, page * num + index + 1)),
      requestCount: 1,
      error: apiError ? { code: 'api_error', message: String(apiError) } : null,
      durationMs: Date.now() - startedAt,
      page,
    };
  } catch (err) {
    return {
      organicResults: [],
      requestCount: 1,
      error: {
        code: err.response?.status === 429 ? 'rate_limited' : 'request_failed',
        message: String(err.response?.data?.error || err.message),
        status: err.response?.status || null,
      },
      durationMs: Date.now() - startedAt,
      page,
    };
  }
}

module.exports = { searchSerpApi };
