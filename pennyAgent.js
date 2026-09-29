require('dotenv').config();
const Anthropic = require('@anthropic-ai/sdk');
const pool = require('./db');
const db = require('./dbClient');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');
const {
  resolveAdAccountsForClient,
  ensureAdAccountsSchema,
  AVAILABILITY,
  googleAds,
  metaAds,
} = require('./packages/penny-paid-acquisition');

// Legacy dashboard/cron reporting agent. Platform reads are delegated to the
// canonical SPEC-252 evidence adapters; this file retains narrative reporting only.
const AGENT_NAME = 'penny';
const CLIENT_ID = getRuntimeClientId();

const anthropic = new Anthropic();

function legacyFlagsFromGoogleEvidence(evidence) {
  const flags = [];
  if (!evidence || evidence.availability !== AVAILABILITY.AVAILABLE) {
    if (evidence?.reason || evidence?.error) {
      flags.push(`⚠️ Google Ads unavailable: ${evidence.reason || evidence.error}`);
    }
    return flags;
  }

  for (const c of evidence.campaigns || []) {
    if (c.advertisingChannelType === 'SEARCH' && c.impressions > 100 && (c.ctr || 0) < 0.01) {
      flags.push(`⚠️ "${c.name}": CTR ${((c.ctr || 0) * 100).toFixed(2)}% below 1% (${c.impressions.toLocaleString()} impressions)`);
    }
    const dailyBudget = c.budget?.amount;
    if (dailyBudget && c.spend > 0) {
      const weekBudget = dailyBudget * 7;
      const pace = c.spend / weekBudget;
      if (pace > 1.25) {
        flags.push(`🔴 "${c.name}": Over budget — $${c.spend.toFixed(2)} vs $${weekBudget.toFixed(2)} (${((pace - 1) * 100).toFixed(0)}% over)`);
      } else if (pace < 0.75 && c.impressions > 0) {
        flags.push(`🟡 "${c.name}": Under-pacing — $${c.spend.toFixed(2)} of $${weekBudget.toFixed(2)} used`);
      }
    }
  }
  for (const kw of evidence.keywords || []) {
    if (kw.qualityScore && kw.qualityScore < 5) {
      flags.push(`🟡 Low Quality Score (${kw.qualityScore}/10): "${kw.text}" in "${kw.campaignName}"`);
    }
  }
  return flags;
}

function legacyFlagsFromMetaEvidence(evidence) {
  const flags = [];
  if (!evidence || evidence.availability !== AVAILABILITY.AVAILABLE) {
    if (evidence?.reason || evidence?.error) {
      flags.push(`⚠️ Meta Ads unavailable: ${evidence.reason || evidence.error}`);
    }
    return flags;
  }

  for (const a of evidence.campaigns || []) {
    const freq = a.frequency || 0;
    const ctr = a.ctr || 0;
    const roas = a.platformReportedRoas || 0;
    const impr = a.impressions || 0;

    if (freq > 3) {
      flags.push(`⚠️ "${a.name}": Frequency ${freq.toFixed(1)} — audience may be fatigued`);
    }
    if (roas > 0 && roas < 2.0) {
      flags.push(`🔴 "${a.name}": ROAS ${roas.toFixed(2)} below 2.0 threshold`);
    }
    if (impr > 1000 && ctr < 0.5) {
      flags.push(`🟡 "${a.name}": CTR ${ctr.toFixed(2)}% low (${impr.toLocaleString()} impressions)`);
    }
  }
  return flags;
}

function legacyMetricsForReport(platform, evidence) {
  if (!evidence || evidence.availability !== AVAILABILITY.AVAILABLE) {
    return { error: evidence?.error || evidence?.reason || 'Platform evidence unavailable' };
  }
  if (platform === 'google_ads') {
    return {
      campaigns: (evidence.campaigns || []).map((c) => ({
        name: c.name,
        type: c.advertisingChannelType || 'UNKNOWN',
        impressions: c.impressions,
        clicks: c.clicks,
        ctr: (c.ctr || 0) * 100,
        avg_cpc: c.averageCpc,
        conversions: c.platformConversions,
        cost_per_conversion: c.costPerPlatformConversion,
        spend: c.spend,
        daily_budget: c.budget?.amount ?? null,
      })),
      keywords: (evidence.keywords || []).map((kw) => ({
        text: kw.text,
        quality_score: kw.qualityScore,
        campaign: kw.campaignName,
        ad_group: kw.adGroupName,
      })),
      flags: legacyFlagsFromGoogleEvidence(evidence),
    };
  }
  return {
    adsets: (evidence.campaigns || []).map((a) => ({
      adset_name: a.name,
      impressions: a.impressions,
      clicks: a.clicks,
      ctr: a.ctr,
      spend: a.spend,
      frequency: a.frequency,
      purchase_roas: a.platformReportedRoas != null ? [{ value: a.platformReportedRoas }] : [],
    })),
    flags: legacyFlagsFromMetaEvidence(evidence),
  };
}

async function fetchPlatformEvidence(account) {
  if (account.platform === 'google_ads') {
    const evidence = await googleAds.readGoogleAdsEvidence({ account });
    return legacyMetricsForReport('google_ads', evidence);
  }
  if (account.platform === 'meta_ads') {
    const evidence = await metaAds.readMetaAdsEvidence({ account });
    return legacyMetricsForReport('meta_ads', evidence);
  }
  return { error: `Unknown platform: ${account.platform}` };
}

// ── CLAUDE REPORT ──────────────────────────────────────────────────────────
async function generateReport(companyName, platform, data) {
  const platformLabel = platform === 'google_ads' ? 'Google Ads' : 'Meta Ads';
  const metricsStr = JSON.stringify(data, null, 2).slice(0, 3000);

  const res = await anthropic.messages.create({
    model: 'claude-sonnet-4-6',
    max_tokens: 800,
    messages: [{
      role: 'user',
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
  const postContent = `Ads Report · ${companyName} · ${platformLabel}`;

  const existing = await pool.query(`
    SELECT id FROM pending_comments
    WHERE channel = 'ads_report'
      AND post_content = $1
      AND status = 'pending'
      AND created_at > NOW() - INTERVAL '24 hours'
    LIMIT 1
  `, [postContent]);

  if (existing.rows.length > 0) {
    console.log('  ↷ Already queued today — skipping duplicate');
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
  const clientConfig = await getClientConfig(CLIENT_ID);
  if (!clientConfig) throw new Error(`Active client not found: ${CLIENT_ID}`);
  if (CLIENT_ID !== 1) {
    console.log('Penny ads analysis is enabled only for Pulseforge client_id=1.');
    return;
  }

  await ensureAdAccountsSchema(pool);

  const accounts = await resolveAdAccountsForClient({ clientId: CLIENT_ID, pool });

  if (!accounts.length) {
    console.log('No active ad accounts configured.');
    console.log('Add rows to the ad_accounts table to get started:');
    console.log("  INSERT INTO ad_accounts (company_id, client_id, platform, account_id, refresh_token, is_active)");
    console.log("  VALUES ('<company_uuid>', 1, 'google_ads', '<customer_id>', '<refresh_token>', true);");
    await db.logAgentAction(AGENT_NAME, 'run', null, null, { accounts: 0, reason: 'no_accounts' }, 'success');
    return;
  }

  console.log(`Found ${accounts.length} active ad account${accounts.length !== 1 ? 's' : ''}.\n`);

  let reports = 0;
  let totalFlags = 0;

  for (const account of accounts) {
    const stillActive = await getClientConfig(CLIENT_ID);
    if (!stillActive) {
      throw new Error(`[Penny] Client ${CLIENT_ID} deactivated mid-run — aborting`);
    }

    const label = `${account.company_name || 'Account'} / ${account.platform}`;
    console.log(`Analyzing: ${label}`);

    try {
      const result = await fetchPlatformEvidence(account);

      if (result.error) {
        console.warn(`  ⚠️ ${result.error}`);
        continue;
      }

      const report = await generateReport(account.company_name || 'Account', account.platform, result);
      const id = await saveReport(account.company_name || 'Account', account.platform, report, result.flags);

      if (id) {
        console.log(`  ✓ Report saved (${id.slice(0, 8)}) — ${result.flags.length} flag${result.flags.length !== 1 ? 's' : ''}`);
        reports++;
        totalFlags += result.flags.length;
      }

      await db.logAgentAction(AGENT_NAME, 'analyze_account', null, null, {
        company: account.company_name,
        platform: account.platform,
        flags: result.flags.length,
        report_saved: !!id,
      }, 'success');
    } catch (err) {
      const msg = err.response?.data?.error?.message || err.message;
      console.error(`  ✗ ${label}: ${msg}`);
      await db.logAgentAction(AGENT_NAME, 'analyze_account', null, null, {
        company: account.company_name,
        platform: account.platform,
        error: msg,
      }, 'failed');
    }

    await new Promise((r) => setTimeout(r, 1500));
  }

  await db.logAgentAction(AGENT_NAME, 'run', null, null, {
    accounts_analyzed: accounts.length,
    reports_saved: reports,
    flags: totalFlags,
  }, 'success');

  console.log(`\nPenny complete — ${reports} report${reports !== 1 ? 's' : ''} queued, ${totalFlags} total flag${totalFlags !== 1 ? 's' : ''}.`);
}

module.exports = { run };

if (require.main === module) {
  run().catch((err) => {
    console.error('[Penny] Fatal error:', err.message);
    process.exit(1);
  });
}
