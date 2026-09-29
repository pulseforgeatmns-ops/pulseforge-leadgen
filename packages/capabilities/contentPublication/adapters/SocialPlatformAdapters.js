'use strict';
const axios = require('axios');

// Never retain provider error bodies: they can contain credentials and request headers.
function providerFailure(err) {
  const status = Number(err.response?.status) || null;
  const definite = [400, 401, 403, 404, 422, 429].includes(status);
  return { success: false, errorCode: status ? `provider_http_${status}` : 'provider_response_unknown',
    definitelyNotPublished: definite, retryable: definite, httpStatus: status };
}
function createSocialPlatformAdapters({ http = axios, googleAuthFactory } = {}) {
  const options = token => ({ timeout: 30000, headers: { Authorization: `Bearer ${token}` } });
  async function buffer(query, variables, credentials) {
    const r = await http.post('https://api.buffer.com', { query, variables }, options(credentials.accessToken));
    if (r.data?.errors?.length) throw new Error('provider_graphql_error');
    return r.data?.data;
  }
  const bufferAdapter = platform => ({
    platform,
    async publish({ artifact, account, credentials }) {
      try {
        const data = await buffer(`mutation($input: CreatePostInput!) { createPost(input: $input) { ... on PostActionSuccess { post { id } } ... on MutationError { message } } }`,
          { input: { channelId: account.externalAccountId, text: artifact.body, schedulingType: 'automatic', mode: 'shareNow', assets: [], needsApproval: false } }, credentials);
        const result = data?.createPost;
        if (result?.message && !result?.post) return { success: false, definitelyNotPublished: true, retryable: true, errorCode: 'provider_rejected' };
        return { success: Boolean(result?.post?.id), externalPostId: result?.post?.id || null, externalAccountId: account.externalAccountId };
      } catch (err) { return providerFailure(err); }
    },
    async readBack({ receipt, account, credentials }) {
      const data = await buffer(`query($input: PostInput!) { post(input: $input) { id text status sentAt externalLink channel { id service } } }`, { input: { id: receipt.externalPostId } }, credentials);
      const post = data?.post;
      return { externalPostId: post?.id, externalAccountId: post?.channel?.id, body: post?.text,
        platformMatches: post?.channel?.service === 'linkedin', status: post?.status,
        published: post?.status === 'sent', publishedAt: post?.sentAt, externalUrl: post?.externalLink || null };
    },
  });
  async function googleAuth(credentials) {
    const client = googleAuthFactory ? googleAuthFactory(credentials) : new (require('googleapis').google.auth.OAuth2)(credentials.clientId, credentials.clientSecret);
    client.setCredentials({ refresh_token: credentials.refreshToken });
    return client;
  }
  return [bufferAdapter('linkedin_page'), bufferAdapter('linkedin_personal'), {
    platform: 'facebook_page',
    async publish({ artifact, account, credentials }) {
      try {
        const r = await http.post(`https://graph.facebook.com/${account.apiVersion}/${account.externalAccountId}/feed`, { message: artifact.body }, options(credentials.accessToken));
        return { success: Boolean(r.data?.id), externalPostId: r.data?.id, externalAccountId: account.externalAccountId };
      } catch (err) { return providerFailure(err); }
    },
    async readBack({ receipt, account, credentials }) {
      if (!/^[0-9]+_[0-9]+$/.test(receipt.externalPostId)) throw new Error('invalid_provider_post_id');
      const r = await http.get(`https://graph.facebook.com/${account.apiVersion}/${receipt.externalPostId}`, {
        ...options(credentials.accessToken), params: { fields: 'id,message,from,permalink_url,created_time,is_published' },
      });
      const post = r.data;
      return { externalPostId: post?.id, externalAccountId: post?.from?.id, body: post?.message,
        platformMatches: true, status: post?.is_published === true ? 'published' : 'unverified',
        published: post?.is_published === true, publishedAt: post?.created_time, externalUrl: post?.permalink_url || null };
    },
  }, {
    platform: 'google_business',
    async publish({ artifact, account, credentials }) {
      try {
        const auth = await googleAuth(credentials);
        const r = await auth.request({ url: `https://mybusiness.googleapis.com/v4/${account.externalAccountId}/localPosts`, method: 'POST',
          data: { languageCode: 'en-US', summary: artifact.body, topicType: 'STANDARD' }, timeout: 30000, retry: false });
        return { success: Boolean(r.data?.name), externalPostId: r.data?.name, externalAccountId: account.externalAccountId };
      } catch (err) { return providerFailure(err); }
    },
    async readBack({ receipt, account, credentials }) {
      if (!receipt.externalPostId.startsWith(`${account.externalAccountId}/localPosts/`) || !/^accounts\/[^/]+\/locations\/[^/]+\/localPosts\/[^/?#]+$/.test(receipt.externalPostId)) throw new Error('invalid_provider_post_id');
      const auth = await googleAuth(credentials);
      const r = await auth.request({ url: `https://mybusiness.googleapis.com/v4/${receipt.externalPostId}`, method: 'GET', timeout: 30000 });
      const post = r.data;
      return { externalPostId: post?.name, externalAccountId: post?.name?.split('/localPosts/')[0], body: post?.summary,
        platformMatches: true, status: post?.state, published: post?.state === 'LIVE', publishedAt: post?.createTime, externalUrl: post?.searchUrl || null };
    },
  }];
}
function createSocialAdapterRegistry(deps = {}) {
  const { PlatformAdapterRegistry } = require('../PlatformAdapterRegistry');
  const registry = new PlatformAdapterRegistry();
  for (const adapter of createSocialPlatformAdapters(deps)) registry.register(adapter);
  return registry;
}
module.exports = { createSocialPlatformAdapters, createSocialAdapterRegistry, providerFailure };
