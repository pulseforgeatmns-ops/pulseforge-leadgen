require('dotenv').config();
const axios = require('axios');
const Anthropic = require('@anthropic-ai/sdk');
const pool = require('./db');
const db = require('./dbClient');

// Uses Google Ads REST API (GAQL over HTTPS) instead of the google-ads-api gRPC package —
// consistent with the rest of the codebase and avoids native binary issues on Railway.
const AGENT_NAME = 'penny';
const GOOGLE_ADS_VERSION = 'v18';
const META_API_VERSION = 'v20.0';

const anthropic = new Anthropic();

// ── SCHEMA ─────────────────────────────────────────────────────────────────
async function ensureSchema() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ad_accounts (
      id            UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
      company_id    UUID         REFERENCES companies(id),
      platform      TEXT         NOT NULL,
      account_id    TEXT         NOT NULL,
      access_token  TEXT,
      refresh_token TEXT,
      token_expiry  TIMESTAMPTZ,
      is_active     BOOLEAN      DEFAULT true,
      created_at    TIMESTAMPTZ  DEFAULT NOW()
    )
  `);
}

// ── ACCOUNTS ───────────────────────────────────────────────────────────────
async function getActiveAccounts() {
  const res = await pool.query(`
    SELECT a.*, c.name AS company_name, c.industry, c.location
    FROM ad_accounts a
    JOIN companies c ON a.company_id = c.id
    WHERE a.is_active = true
    ORDER BY c.name, a.platform
  `);
  return res.rows;
}

// ── GOOGLE ADS ─────────────────────────────────────────────────────────────
async function googleAdsToken(refreshToken) {
  const res = await axios.post('https://oauth2.googleapis.com/token', {
    client_id:     process.env.GOOGLE_ADS_CLIENT_ID,
    client_secret: process.env.GOOGLE_ADS_CLIENT_SECRET,
    refresh_token: refreshToken,
    grant_type:    'refresh_token',
  });
  return res.data.access_token;
}

function googleAdsHeaders(token) {
  const h = {
    Authorization:    `Bearer ${token}`,
    'developer-token': process.env.GOOGLE_ADS_DEVELOPER_TOKEN,
    'Content-Type':   'application/json',
  };
  if (process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID) {
    h['login-customer-id'] = process.env.GOOGLE_ADS_MANAGER_ACCOUNT_ID;
  }
  return h;
}

async function gaqlSearch(customerId, query, token) {
  const res = await axios.post(
    `https://googleads.googleapis.com/${GOOGLE_ADS_VERSION}/customers/${customerId}/googleAds:search`,
    { query },
    { headers: googleAdsHeaders(token) }
  );
  return res.data.results || [];
}

async function fetchGoogleMetrics(account) {
  const missingEnv = ['GOOGLE_ADS_DEVELOPER_TOKEN', 'GOOGLE_ADS_CLIENT_ID', 'GOOGLE_ADS_CLIENT_SECRET']
    .filter(k => !process.env[k]);
  if (missingEnv.length) return { error: `Missing env vars: ${missingEnv.join(', ')}` };
  if (!account.refresh_token) return { error: 'No refresh_token in ad_accounts row' };

  const token = await googleAdsToken(account.refresh_token);
  const cid   = account.account_id.replace(/-/g, '');

  const [campaignRows, keywordRows] = await Promise.all([
    gaqlSearch(cid, `
      SELECT
        campaign.id, campaign.name, campaign.status,
        campaign.advertising_channel_type,
        campaign_budget.amount_micros,
        metrics.impressions, metrics.clicks, metrics.ctr,
        metrics.average_cpc, metrics.conversions,
        metrics.cost_per_conversion, metrics.cost_micros
      FROM campaign
      WHERE campaign.status = 'ENABLED'
        AND segments.date DURING LAST_7_DAYS
    `, token),
    gaqlSearch(cid, `
      SELECT
        ad_group_criterion.keyword.text,
        ad_group_criterion.quality_info.quality_score,
        campaign.name, ad_group.name
      FROM ad_group_criterion
      WHERE ad_group_criterion.type = 'KEYWORD'
        AND ad_group_criterion.status = 'ENABLED'
        AND campaign.status = 'ENABLED'
      LIMIT 100
    `, token),
  ]);

  const campaigns = campaignRows.map(r => ({
    name:               r.campaign?.name || 'Unknown',
    type:               r.campaign?.advertisingChannelType || 'UNKNOWN',
    impressions:        parseInt(r.metrics?.impressions || 0),
    clicks:             parseInt(r.metrics?.clicks || 0),
    ctr:                parseFloat(r.metrics?.ctr || 0) * 100,
    avg_cpc:            parseInt(r.metrics?.averageCpc || 0) / 1_000_000,
    conversions:        parseFloat(r.metrics?.conversions || 0),
    cost_per_conversion: parseInt(r.metrics?.costPerConversion || 0) / 1_000_000,
    spend:              parseInt(r.metrics?.costMicros || 0) / 1_000_000,
    daily_budget:       r.campaignBudget?.amountMicros
                          ? parseInt(r.campaignBudget.amountMicros) / 1_000_000
                          : null,
  }));

  const keywords = keywordRows.map(r => ({
    text:          r.adGroupCriterion?.keyword?.text || '',
    quality_score: r.adGroupCriterion?.qualityInfo?.qualityScore || null,
    campaign:      r.campaign?.name || '',
    ad_group:      r.adGroup?.name || '',
  }));

  const flags = [];
  for (const c of campaigns) {
    if (c.type === 'SEARCH' && c.impressions > 100 && c.ctr < 1) {
      flags.push(`⚠️ "${c.name}": CTR ${c.ctr.toFixed(2)}% below 1% (${c.impressions.toLocaleString()} impressions)`);
    }
    if (c.daily_budget && c.spend > 0) {
      const weekBudget = c.daily_budget * 7;
      const pace = c.spend / weekBudget;
      if (pace > 1.25) {
        flags.push(`🔴 "${c.name}": Over budget — $${c.spend.toFixed(2)} vs $${weekBudget.toFixed(2)} (${((pace - 1) * 100).toFixed(0)}% over)`);
      } else if (pace < 0.75 && c.impressions > 0) {
        flags.push(`🟡 "${c.name}": Under-pacing — $${c.spend.toFixed(2)} of $${weekBudget.toFixed(2)} used`);
      }
    }
  }
  for (const kw of keywords) {
    if (kw.quality_score && kw.quality_score < 5) {
      flags.push(`🟡 Low Quality Score (${kw.quality_score}/10): "${kw.text}" in "${kw.campaign}"`);
    }
  }

  return { campaigns, keywords, flags };
}

// ── META ADS ───────────────────────────────────────────────────────────────
async function fetchMetaMetrics(account) {
  if (!account.access_token) return { error: 'No access_token in ad_accounts row' };

  const today   = new Date().toISOString().split('T')[0];
  const weekAgo = new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString().split('T')[0];

  const res = await axios.get(
    `https://graph.facebook.com/${META_API_VERSION}/act_${account.account_id}/insights`,
    {
      params: {
        access_token: account.access_token,
        fields:       'campaign_name,adset_name,impressions,clicks,ctr,spend,cpm,cpc,reach,frequency,purchase_roas',
        time_range:   JSON.stringify({ since: weekAgo, until: today }),
        level:        'adset',
        limit:        50,
      },
    }
  );

  const adsets = res.data.data || [];

  const flags = [];
  for (const a of adsets) {
    const freq  = parseFloat(a.frequency || 0);
    const ctr   = parseFloat(a.ctr || 0);
    const roas  = parseFloat(a.purchase_roas?.[0]?.value || 0);
    const impr  = parseFloat(a.impressions || 0);

    if (freq > 3) {
      flags.push(`⚠️ "${a.adset_name}": Frequency ${freq.toFixed(1)} — audience may be fatigued`);
    }
    if (roas > 0 && roas < 2.0) {
      flags.push(`🔴 "${a.adset_name}": ROAS ${roas.toFixed(2)} below 2.0 threshold`);
    }
    if (impr > 1000 && ctr < 0.5) {
      flags.push(`🟡 "${a.adset_name}": CTR ${ctr.toFixed(2)}% low (${impr.toLocaleString()} impressions)`);
    }
  }

  return { adsets, flags };
}

// ── CLAUDE REPORT ──────────────────────────────────────────────────────────
async function generateReport(companyName, platform, data) {
  const platformLabel = platform === 'google_ads' ? 'Google Ads' : 'Meta Ads';
  const metricsStr    = JSON.stringify(data, null, 2).slice(0, 3000);

  const res = await anthropic.messages.create({
    model:      'claude-sonnet-4-6',
    max_tokens: 800,
    messages: [{
      role:    'user',
      content: `You are Penny, an ad performance analyst for Pulseforge. Analyze this ${platformLabel} account data for ${companyName} and write a concise report.

Performance data (last 7 days):
${metricsStr}

Structure the report exactly like this:

**WHAT'S WORKING**
- [1-2 bullets on strongest performers and why — be specific]

**NEEDS ATTENTION**
- [1-3 bullets with specific actions — not "consider pausing" but "pause X and shift $Y to Z"]

**AD COPY SUGGESTIONS** (include this section ONLY if CTR is flagged as low)
- [2-3 headline/description variations specific to this business — not generic]

**CLIENT SUMMARY**
[1 paragraph, plain English, written for the business owner — no jargon, no technical terms]

Rules: no filler, no "great job" padding. If the account is healthy, say so briefly. If there are problems, be direct about what to fix.`,
    }],
  });

  return res.content[0].text.trim();
}

// ── SAVE ───────────────────────────────────────────────────────────────────
async function saveReport(companyName, platform, report, flags) {
  const platformLabel = platform === 'google_ads' ? 'Google Ads' : 'Meta Ads';
  const postContent   = `Ads Report · ${companyName} · ${platformLabel}`;

  // Dedup: skip if a pending report for this company/platform already queued today
  const existing = await pool.query(`
    SELECT id FROM pending_comments
    WHERE channel = 'ads_report'
      AND post_content = $1
      AND status = 'pending'
      AND created_at > NOW() - INTERVAL '24 hours'
    LIMIT 1
  `, [postContent]);

  if (existing.rows.length > 0) {
    console.log(`  ↷ Already queued today — skipping duplicate`);
    return null;
  }

  const flagBlock = flags.length > 0
    ? `\n\n---\n**FLAGS (${flags.length})**\n${flags.join('\n')}`
    : '\n\n---\n✅ No threshold violations detected.';

  const res = await pool.query(`
    INSERT INTO pending_comments (author_name, author_title, post_content, comment, post_url, channel, status)
    VALUES ($1, $2, $3, $4, NULL, 'ads_report', 'pending')
    RETURNING id
  `, [companyName, platformLabel, postContent, report + flagBlock]);

  return res.rows[0].id;
}

// ── MAIN ───────────────────────────────────────────────────────────────────
async function run() {
  console.log('\nPenny agent running...\n');

  await ensureSchema();

  const accounts = await getActiveAccounts();

  if (!accounts.length) {
    console.log('No active ad accounts configured.');
    console.log('Add rows to the ad_accounts table to get started:');
    console.log('  INSERT INTO ad_accounts (company_id, platform, account_id, refresh_token, is_active)');
    console.log("  VALUES ('<company_uuid>', 'google_ads', '<customer_id>', '<refresh_token>', true);");
    await db.logAgentAction(AGENT_NAME, 'run', null, null, { accounts: 0, reason: 'no_accounts' }, 'success');
    return;
  }

  console.log(`Found ${accounts.length} active ad account${accounts.length !== 1 ? 's' : ''}.\n`);

  let reports   = 0;
  let totalFlags = 0;

  for (const account of accounts) {
    const label = `${account.company_name} / ${account.platform}`;
    console.log(`Analyzing: ${label}`);

    try {
      let result;

      if (account.platform === 'google_ads') {
        result = await fetchGoogleMetrics(account);
      } else if (account.platform === 'meta_ads') {
        result = await fetchMetaMetrics(account);
      } else {
        console.warn(`  Unknown platform: ${account.platform}`);
        continue;
      }

      if (result.error) {
        console.warn(`  ⚠️ ${result.error}`);
        continue;
      }

      const report = await generateReport(account.company_name, account.platform, result);
      const id     = await saveReport(account.company_name, account.platform, report, result.flags);

      if (id) {
        console.log(`  ✓ Report saved (${id.slice(0, 8)}) — ${result.flags.length} flag${result.flags.length !== 1 ? 's' : ''}`);
        reports++;
        totalFlags += result.flags.length;
      }

      await db.logAgentAction(AGENT_NAME, 'analyze_account', null, null, {
        company:      account.company_name,
        platform:     account.platform,
        flags:        result.flags.length,
        report_saved: !!id,
      }, 'success');

    } catch (err) {
      const msg = err.response?.data?.error?.message || err.message;
      console.error(`  ✗ ${label}: ${msg}`);
      await db.logAgentAction(AGENT_NAME, 'analyze_account', null, null, {
        company:  account.company_name,
        platform: account.platform,
        error:    msg,
      }, 'error');
    }

    await new Promise(r => setTimeout(r, 1500));
  }

  await db.logAgentAction(AGENT_NAME, 'run', null, null, {
    accounts_analyzed: accounts.length,
    reports_saved:     reports,
    flags:             totalFlags,
  }, 'success');

  console.log(`\nPenny complete — ${reports} report${reports !== 1 ? 's' : ''} queued, ${totalFlags} total flag${totalFlags !== 1 ? 's' : ''}.`);
}

run().catch(console.error);
