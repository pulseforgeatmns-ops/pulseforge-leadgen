'use strict';

(function () {
  let latestBrief = null;
  const byId = id => document.getElementById(id);
  const number = value => Number.isFinite(Number(value)) ? Number(value).toLocaleString() : '—';
  const ask = prompt => document.dispatchEvent(new CustomEvent('pulseforge:living-ask', { detail: { prompt } }));

  function row(title, detail, label, prompt, tone, action) {
    const button = document.createElement('button');
    button.type = 'button';
    button.className = `lc-brief-line lc-tone-${tone}`;
    const symbol = document.createElement('span');
    symbol.className = 'lc-line-symbol';
    symbol.textContent = tone === 'violet' ? '↗' : tone === 'teal' ? '◎' : '◇';
    symbol.setAttribute('aria-hidden', 'true');
    const copy = document.createElement('span');
    const heading = document.createElement('strong');
    heading.textContent = title;
    const text = document.createElement('span');
    text.textContent = detail;
    copy.append(heading, text);
    const status = document.createElement('small');
    status.textContent = label;
    button.append(symbol, copy, status);
    button.addEventListener('click', action || (() => ask(prompt)));
    return button;
  }

  function money(value, currency) {
    try { return new Intl.NumberFormat(undefined, { style: 'currency', currency: currency || 'USD', maximumFractionDigits: 2 }).format(value); }
    catch (_) { return `${number(value)} ${currency || ''}`.trim(); }
  }

  function windowLabel(window) {
    if (!window?.start || !window?.end) return 'Reported period';
    const date = value => new Date(`${String(value).slice(0, 10)}T12:00:00Z`).toLocaleDateString(undefined, { month: 'short', day: 'numeric', timeZone: 'UTC' });
    return `${date(window.start)}–${date(window.end)}`;
  }

  function updateIdentity() {
    const shell = window.PulseforgeShell;
    byId('lcWorkspaceLabel').textContent = shell?.tenantName || '';
    if (!latestBrief) return;
    const accountName = String(shell?.context?.user?.name || '').trim();
    const firstName = /\b(admin|legacy)\b/i.test(accountName) ? '' : accountName.split(/\s+/)[0];
    byId('lcBriefTitle').textContent = firstName && firstName !== 'Legacy' ? `Here’s your brief, ${firstName}.` : 'Here’s your daily brief.';
    const count = byId('pfNeedsCount');
    if (count) {
      count.textContent = latestBrief.approvals ? number(latestBrief.approvals.pending) : '';
      count.hidden = !latestBrief.approvals;
    }
  }

  function render(model, options = {}) {
    latestBrief = model.livingBrief || {};
    const brief = latestBrief;
    const lines = byId('lcBriefLines');
    lines.replaceChildren();
    byId('lcBriefTitle').textContent = 'Here’s your daily brief.';
    byId('lcBriefIntro').textContent = 'Here’s where things stand. Choose an update to go deeper with me.';

    const pipeline = brief.pipeline;
    lines.appendChild(row('Prospects', pipeline
      ? `${number(pipeline.new24h)} new in the last 24 hours. ${number(pipeline.warm)} warm and ${number(pipeline.booked)} booked in your open pipeline.`
      : 'Prospect activity is temporarily unavailable. Open the pipeline or ask me to investigate.',
    pipeline ? `${number(pipeline.total)} open` : 'Unavailable',
    'Review the current workspace’s prospect pipeline. Explain new prospects in the last 24 hours, warm opportunities, booked calls, and the next follow-up.', 'violet'));

    const ads = brief.googleAds;
    const campaigns = Array.isArray(ads?.campaigns) ? ads.campaigns : [];
    const active = campaigns.filter(campaign => String(campaign.status).toUpperCase() === 'ENABLED');
    let adsDetail = 'Google Ads data is unavailable for this workspace. Ask me to check the connection.';
    if (ads?.available) {
      const spend = active.reduce((sum, campaign) => sum + (Number(campaign.spend) || 0), 0);
      const clicks = active.reduce((sum, campaign) => sum + (Number(campaign.clicks) || 0), 0);
      const knownConversions = active.every(campaign => campaign.conversions != null);
      const conversions = active.reduce((sum, campaign) => sum + (Number(campaign.conversions) || 0), 0);
      adsDetail = active.length
        ? `${active.length === 1 ? active[0].name : `${active.length} enabled campaigns`}: ${money(spend, ads.currency)} spent, ${number(clicks)} clicks, ${knownConversions ? `${number(conversions)} platform conversions` : 'conversions unavailable'}. ${windowLabel(ads.window)}. Paused campaign history excluded.`
        : `No enabled campaigns. ${number(campaigns.filter(campaign => String(campaign.status).toUpperCase() === 'PAUSED').length)} paused. ${windowLabel(ads.window)}.`;
    }
    lines.appendChild(row('Google Ads', adsDetail, ads?.available ? `${active.length} enabled` : 'Unavailable',
      'Give me Penny’s current Google Ads report for this workspace. Separate enabled campaigns from paused history; include the reporting window, budget, impressions, clicks, spend and platform conversions. Distinguish tracked leads from platform conversions.', 'teal'));

    const approvals = brief.approvals;
    lines.appendChild(row('Needs your approval', approvals
      ? approvals.pending ? `${number(approvals.pending)} content items are ready for your review.` : 'No content is waiting for approval.'
      : 'The content approval queue is temporarily unavailable.',
    approvals ? `${number(approvals.pending)} pending` : 'Unavailable',
    null, 'gold', () => document.dispatchEvent(new CustomEvent('pulseforge:content-review'))));

    const generated = brief.generatedAt || model.meta?.generatedAt;
    const timestamp = generated ? new Date(generated).toLocaleTimeString(undefined, { hour: 'numeric', minute: '2-digit' }) : null;
    byId('lcBriefSource').textContent = `${options.fromCache ? 'Saved brief' : 'Workspace brief'}${timestamp ? ` · ${timestamp}` : ''}. Ads use the period shown.`;
    document.querySelectorAll('[data-lc-ask]').forEach(button => { button.disabled = false; });
    byId('lcRefresh').disabled = false;
    updateIdentity();
  }

  function clear() {
    latestBrief = null;
    byId('lcBriefTitle').textContent = 'Getting your brief ready.';
    byId('lcBriefIntro').textContent = 'I’m checking this workspace’s latest activity.';
    byId('lcBriefLines').replaceChildren();
    byId('lcBriefSource').textContent = 'Preparing workspace intelligence…';
    byId('lcRefresh').disabled = true;
    document.querySelectorAll('[data-lc-ask]').forEach(button => { button.disabled = true; });
    const count = byId('pfNeedsCount');
    if (count) { count.hidden = true; count.textContent = ''; }
  }

  function unavailable() {
    clear();
    byId('lcBriefTitle').textContent = 'Your brief is taking a little longer.';
    byId('lcBriefIntro').textContent = 'I couldn’t load the current workspace’s brief. Refresh to try again.';
    byId('lcBriefSource').textContent = 'No current data available.';
    byId('lcRefresh').disabled = false;
  }

  document.querySelectorAll('[data-lc-ask]').forEach(button => button.addEventListener('click', () => ask(button.dataset.lcAsk)));
  byId('lcRefresh').addEventListener('click', () => document.dispatchEvent(new CustomEvent('pulseforge:living-refresh')));
  byId('cdSpatialMax').addEventListener('click', () => ask(''));
  byId('cdSpatialMax').addEventListener('keydown', event => {
    if (event.key === 'Enter' || event.key === ' ') { event.preventDefault(); ask(''); }
  });
  document.addEventListener('pulseforge:shell-ready', updateIdentity);
  document.addEventListener('pulseforge:tenant-changed', () => { clear(); updateIdentity(); });
  document.addEventListener('visibilitychange', () => document.body.classList.toggle('lc-motion-paused', document.hidden));
  window.LivingCommand = { render, clear, unavailable };
})();
