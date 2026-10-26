'use strict';
(() => {
  const $ = id => document.getElementById(id);
  const el = (tag, text, className) => { const n = document.createElement(tag); if (text !== undefined) n.textContent = text; if (className) n.className = className; return n; };
  const parts = location.pathname.split('/').filter(Boolean), ca = parts.length === 3 ? parts[2] : null;
  const USDC = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', SOL = 'So11111111111111111111111111111111111111112';
  const durations = {'1m':60,'5m':300,'15m':900,'1h':3600,'4h':14400};
  const names = {BUY:'Bought tokens',SELL:'Sold tokens',TRANSFER_IN:'Tokens received',TRANSFER_OUT:'Tokens transferred out',CALLER:'Token mentioned',FIRST_CALLER:'First tracked call',FRONT_RUNNERS_CALL:'Front Runners call',INDEPENDENT_CONVERGENCE:'Separate sources called this token',PROMOTION_EVENT:'Promotional post',DEX_BOOST:'DEX boost mentioned',AMPLIFIER_EVENT:'Promotion amplified',DE_RISK_MILESTONE:'Original money recovered',DISTRIBUTION_NORMAL:'Risk returned to normal',DISTRIBUTION_WATCH:'Risk changed to watch',DISTRIBUTION_HIGH:'Risk changed to high',DISTRIBUTION_CRITICAL:'Risk changed to critical',LIQUIDITY_WARNING:'Trading-pool warning',OPERATOR_NOTE:'Your note',CANDLE:'Price observation'};
  const research = {UNKNOWN:'Not established yet',DISCOVERY:'Being tracked',FIRST_CALLER:'First tracked call',INDEPENDENT_CONVERGENCE:'Calls from separate source groups'};
  const stages = {DISCOVERY:'Watching',ENTRY:'Position opened',RUNNER:'Remainder still held',DE_RISK:'Original money recovered',RE_ENTRY:'Added after selling',EXIT:'Fully sold'};
  const reasonNames = {SOURCE_FRESHNESS_UNKNOWN:"The market provider does not give its own update time.",MARKET_OBSERVATION_DELAYED:'Our latest market observation is delayed.',PRICE_DRAWDOWN_10_PERCENT:'Price has fallen at least 10% from an observed high.',SELL_PRESSURE_60_PERCENT:'Selling makes up at least 60% of observed activity.',POOL_RESERVE_VALUE_DOWN_10_PERCENT:'The trading pool is worth at least 10% less than our starting observation.',PRICE_DRAWDOWN_WITH_DISTRIBUTION_EVIDENCE:'A larger price drop is happening alongside selling or a decline in pool value.',SEVERE_PRICE_DRAWDOWN_WITH_SELL_PRESSURE:'A large price drop and heavy selling are happening together.',SEVERE_PRICE_DRAWDOWN_WITH_RESERVE_DECLINE:'A large price drop and a large decline in pool value are happening together.',STALE_MARKET_EVIDENCE:'Market evidence is too old for a current assessment.',PAIR_CHANGED_COMPARISON_BLOCKED:'The trading pool changed; older comparisons cannot be relied on.',PAIR_UNKNOWN:'The trading pool has not been identified.',PRICE_UNKNOWN:'Price is not available.',LIQUIDITY_UNKNOWN:'Trading-pool value is not available.',VOLUME5M_UNKNOWN:'Recent trading volume is not available.',BUYSELLACTIVITY_UNKNOWN:'Recent buying and selling activity is not available.',PRIOR_RISK_RETAINED_INCOMPLETE_EVIDENCE:'An earlier warning is being kept because newer evidence is incomplete.',PROMOTION_MARKET_DIVERGENCE:'Promotion is increasing while price, selling activity, or pool value is weakening.', 'Observed drawdown below 10%':'Price is within 10% of the highest price Signal has observed.', 'Observed sell activity below 50%':'Selling accounts for less than half of observed activity.', 'Pool reserve value has not declined from observed baseline':'Trading-pool value has not fallen from our starting observation.', 'Historical large-holder activity unavailable':"We cannot see how large holders have been trading historically.", 'Developer wallet identity unavailable':"We have not identified the developer's wallet.", 'Provider source freshness unavailable':"We know when we fetched the data, but not when the provider last updated it.", 'Buy/sell USD volume unavailable; transaction counts are a proxy':'The dollar value of buys versus sells is missing. Trade counts give only part of the picture.', 'Five-minute momentum baseline unavailable':'There is not yet a comparable five-minute price history.', 'No market observations':'No market observations are available yet.'};
  let interval = '1m', chart, lastData, homeData, timer, inflight = false, stopped = false, retryMs = 10000, controller, positionFilter = 'all', eventFilter = 'all', reloadPending = false, fitHistoryPending=false;
  const layers = {trades:true,calls:true,risk:true};
  const verifiedMetrics=new Map();let renderedAt=Date.now(),lastTradeKey=null;
  const zoneHistory=new Map(),zoneHistoryPending=new Set();
  const finite = v => v !== null && v !== undefined && v !== '' && Number.isFinite(Number(v));
  const title = s => String(s || 'Unknown').toLowerCase().replaceAll('_',' ').replace(/^./, c => c.toUpperCase());
  const friendly = s => reasonNames[s] || title(s);
  const date = (s, options) => Number.isFinite(Date.parse(s)) ? new Date(s).toLocaleString(undefined,options) : 'Time unknown';
  const time = s => date(s,{month:'short',day:'numeric',hour:'numeric',minute:'2-digit'});
  function fmt(v, type = 'usd', signed = false) {
    if (!finite(v)) return 'Not known yet';
    const n = Number(v), sign = signed && n > 0 ? '+' : '';
    if (type === 'USDC') return sign + n.toLocaleString(undefined,{maximumFractionDigits:2}) + ' USDC';
    if (type === 'tokens') return n.toLocaleString(undefined,{maximumFractionDigits:6});
    if (type === 'count') return n.toLocaleString(undefined,{maximumFractionDigits:0});
    if (type === 'percent') return sign + (n*100).toFixed(1) + '%';
    if (type === 'price') return '$' + n.toLocaleString(undefined,{maximumSignificantDigits:6});
    if (type === 'quotePrice') return n.toLocaleString(undefined,{maximumSignificantDigits:6}) + ' USDC';
    return sign + n.toLocaleString(undefined,{style:'currency',currency:'USD',maximumFractionDigits:2});
  }
  function amountNode(v, type, signed) {
    const n = el('span',fmt(v,type,signed));
    if (type === 'USDC' && finite(v)) { n.replaceChildren(el('span',fmt(v,'USDC',signed).replace(' USDC','')),el('span','USDC','currency-unit')); }
    return n;
  }
  function finance(p, key) { const m = p.accounting?.[key]; return m?.value != null || !p.quoteAccounting?.[key] ? m : {...p.quoteAccounting[key],unit:'USDC'}; }
  function quality(m) { if(m?.displayOnly)return 'LAST VERIFIED · '+m.ageSeconds+'s ago · checking wallet'; const state = m?.state || 'UNKNOWN'; return state + (m?.ageSeconds != null ? ' · ' + (m.ageSeconds < 60 ? m.ageSeconds + 's ago' : Math.floor(m.ageSeconds/60) + 'm ago') : ''); }
  function metric(target, label, m, {type='usd',explanation='',featured=false,signed=false,icon}={}) {
    const box = el('div',undefined,'metric' + (featured ? ' featured' : ''));
    if (icon) box.append(el('div',icon,'metric-icon'));
    box.append(el('div',label,'label'));
    const value = el('div',undefined,'value'); value.append(finite(m?.value)?amountNode(m.value,m?.unit || type,signed):el('span','—'));
    if (signed && finite(m?.value)) value.classList.add(Number(m.value) >= 0 ? 'positive' : 'negative');
    box.append(value); if (explanation) box.append(el('div',explanation,'explanation'));
    const q = el('div',quality(m),'quality'); q.dataset.state = m?.state || 'UNKNOWN';
    q.title = m?.reason || (m?.state === 'PARTIAL' ? 'Observed data with incomplete provider timing or history. Open the data details for coverage.' : 'Source: ' + (m?.source || 'Not available'));
    box.append(q); target.append(box);
  }
  function riskBadge(state) { const n = el('span',title(state),'risk-badge'); n.dataset.state = state; return n; }
  function empty(target, heading, text) { const n = el('div',undefined,'empty-state'); n.append(el('strong',heading),el('span',text)); target.append(n); }
  function sourceName(source) { return source === 'telegram-front-runners' ? 'Front Runners' : source === 'solana finalized transaction' ? 'Finalized wallet transaction' : source === 'operator' ? 'You' : source === 'distribution-v1.0' ? 'Signal risk monitor' : source === 'signal-prospective-research' ? 'Signal source research' : source || 'Source not known'; }
  function group(e) { if (['BUY','SELL','TRANSFER_IN','TRANSFER_OUT'].includes(e.category)) return 'trades'; if (e.category === 'OPERATOR_NOTE') return 'notes'; if (e.category.startsWith('DISTRIBUTION_') || ['LIQUIDITY_WARNING','DE_RISK_MILESTONE'].includes(e.category)) return 'risk'; return 'promotion'; }
  function quoteType(asset) { return asset === USDC ? 'USDC' : asset === SOL ? 'SOL' : null; }
  function tradeCash(e) { if (finite(e.evidence?.valueUsd)) return fmt(e.evidence.valueUsd); const q = quoteType(e.evidence?.quoteAsset); return q === 'USDC' ? fmt(e.evidence.quoteAmount,q) : q === 'SOL' && finite(e.evidence.quoteAmount) ? fmt(e.evidence.quoteAmount,'tokens') + ' SOL' : 'Value not known yet'; }
  function eventSummary(e, p=lastData) {
    if (['BUY','SELL','TRANSFER_IN','TRANSFER_OUT'].includes(e.category)) return `${fmt(e.evidence?.tokenAmount,'tokens')} ${p?.symbol || 'tokens'}${['BUY','SELL'].includes(e.category) ? ' · ' + tradeCash(e) : ''}`;
    if (typeof e.evidence === 'string') return e.evidence.length > 200 ? e.evidence.slice(0,200) + '…' : e.evidence;
    if (Array.isArray(e.evidence)) return e.evidence.map(friendly).join(' ');
    if (e.category === 'INDEPENDENT_CONVERGENCE') return 'Signal found calls from separate source groups. This is source evidence, not a guarantee of performance.';
    if (e.category === 'FIRST_CALLER') return 'The first eligible call recorded by Signal.';
    return 'Tracked evidence is available in the details.';
  }
  function technical(e) {
    const details = el('details',undefined,'technical-details'); details.dataset.eventId = e.id || e.occurredAt + e.category;
    details.append(el('summary','Source & evidence details'),el('pre',JSON.stringify({timestamp:e.occurredAt,source:e.source,evidence:e.evidence,knownAt:e.knownAt,provenance:e.provenance},null,2)));
    return details;
  }
  function external(url, label) { try { const u = new URL(url); if (u.protocol !== 'https:') return null; const a = el('a',label,'text-button'); a.href=u.href; a.target='_blank'; a.rel='noopener noreferrer'; return a; } catch { return null; } }
  function eventDetail(e) {
    if(e.category==='RESEARCH_ZONE'){const card=document.querySelector('[data-zone-card="'+e.evidence.id+'"]');if(card){card.scrollIntoView({behavior:'smooth',block:'center'});card.classList.add('zone-detail-selected');setTimeout(()=>card.classList.remove('zone-detail-selected'),3000);}return;}

    const target = $('markerDetail'); target.hidden=false; target.replaceChildren();
    const top=el('div',undefined,'marker-top'),heading=el('div'); heading.append(el('h3',names[e.category] || title(e.category)),el('p',time(e.occurredAt),'marker-time'));
    const close=el('button','×','icon-button'); close.type='button'; close.setAttribute('aria-label','Close event details'); close.onclick=()=>target.hidden=true; top.append(heading,close); target.append(top);
    const stats=el('dl',undefined,'marker-stats'),row=(label,value)=>{const div=el('div');div.append(el('dt',label),el('dd',value));stats.append(div);};
    if (group(e)==='trades') {
      row('Tokens',fmt(e.evidence.tokenAmount,'tokens')+' '+lastData.symbol); row(e.category==='BUY'?'Money paid':'Money received',tradeCash(e));
      row('Price per token',finite(e.evidence.executionPriceUsd)?fmt(e.evidence.executionPriceUsd,'price'):e.evidence.quoteAsset===USDC?fmt(e.evidence.executionPriceQuote,'quotePrice'):'Not known yet');
      row('Tokens left after this move',fmt(e.evidence.positionAfter,'tokens')); row('Signal at that time',research[e.evidence.signalState]||title(e.evidence.signalState)); row('Risk at that time',title(e.evidence.distributionRisk));
      target.append(stats);
      if (e.evidence.signature) { const link=external('https://solscan.io/tx/'+encodeURIComponent(e.evidence.signature),'View blockchain transaction ↗'); if(link)target.append(link); }
    } else if (e.category==='CANDLE') { row('Open',fmt(e.evidence.open,'price'));row('High',fmt(e.evidence.high,'price'));row('Low',fmt(e.evidence.low,'price'));row('Close',fmt(e.evidence.close,'price'));target.append(stats,el('p','These are collected price observations. Missing intervals are left empty.','data-note')); }
    else target.append(el('p',eventSummary(e),'muted'));
    target.append(technical(e));
  }
  function riskMeaning(risk) {
    const reasons=risk.for.filter(r=>!r.includes('UNKNOWN')&&!r.includes('UNAVAILABLE'));
    if(risk.state==='CRITICAL')return ['Critical · strong warning signs','A large price decline is happening with other warning signs. Open the evidence before making a decision.'];
    if(risk.state==='HIGH')return ['High · several warning signs','Price, selling activity, or trading-pool value need closer attention.'];
    if(risk.state==='WATCH')return reasons.length?['Watch · changes worth checking','Signal has spotted a change or a coverage gap. Check what is known below.']:['Watch · some checks are missing','Important information is unavailable. Watch does not always mean a sell-off has been detected.'];
    if(risk.state==='NORMAL')return ['Normal · no elevated warning observed','The data we can see has not triggered a higher warning. This is not an all-clear.'];
    return ['Unknown · more information needed','Signal cannot yet make a useful assessment from the available evidence.'];
  }
  function renderRisk(p) {
    const risk=$('risk'),[headline,description]=riskMeaning(p.risk);risk.dataset.state=p.risk.state;risk.replaceChildren();
    const main=el('div',undefined,'risk-main'),icon=el('div',p.risk.state==='NORMAL'?'◇':'!','risk-icon');icon.setAttribute('aria-hidden','true');
    const copy=el('div');copy.append(el('p','DISTRIBUTION RISK','eyebrow'),el('strong',headline),el('p',description),el('small','No alert does not mean safe.'));main.append(icon,copy);
    const link=el('a','See why ↘');link.href='#signal-view';risk.append(main,link);
    const target=$('riskEvidence');target.replaceChildren();
    const unknownCodes=p.risk.for.filter(s=>/UNKNOWN|UNAVAILABLE/.test(s));
    for(const [heading,items,cls,fallback] of [['Signs to pay attention to',p.risk.for.filter(s=>!/UNKNOWN|UNAVAILABLE/.test(s)),'for','No specific selling warning established in the available data.'],['What points the other way',p.risk.against,'against','No counter-evidence established yet.'],["What we don't know",[...p.risk.unknown,...unknownCodes],'unknown','No additional gaps recorded.']]) {
      const section=el('div',undefined,'evidence-section '+cls);section.append(el('h3',heading));const ul=el('ul');for(const text of [...new Set(items.length?items:[fallback])])ul.append(el('li',friendly(text)));section.append(ul);target.append(section);
    }
    const promo=p.risk.promotion,card=el('div',undefined,'promotion-card'),top=el('div',undefined,'promotion-top');top.append(el('h3','How much is it being promoted?'),el('span',title(promo.state),'quiet-badge'));card.append(top);
    const line=el('p');line.append(el('span',String(promo.promotionEventCount),'promotion-number'),document.createTextNode(' promotional posts in the last 5 minutes'));card.append(line);
    const facts=el('div',undefined,'promotion-facts');facts.append(el('span',promo.uniquePromotionalSources+' sources'),el('span',promo.promotionVelocityPerMinute.toFixed(1)+' posts / minute'));card.append(facts);
    card.append(el('p','More promotion does not mean a better investment. Sources from the same group do not count as independent agreement.'));
    card.append(el('p',p.risk.divergence.state==='OBSERVED'?'Promotion is rising while part of the market picture is weakening. This is a research warning, not an instruction to sell.':p.risk.divergence.state==='UNKNOWN'?'We cannot yet compare promotion with a reliable market trend.':'No promotion-versus-market mismatch has been observed in the available window.'));
    const details=el('details',undefined,'technical-details');details.append(el('summary','Source groups & research details'),el('pre',JSON.stringify({promotion:promo,divergence:p.risk.divergence,researchState:p.signalState},null,2)));card.append(details);target.append(card);
  }
  function moneyRow(target,label,m,explanation,{highlight=false,signed=false,type='usd'}={}) {
    const n=el('div',undefined,'money-row'+(highlight?' highlight':'')),text=el('div'),numbers=el('div');text.append(el('div',label,'label'),el('div',explanation,'explanation'));
    const v=el('div',fmt(m?.value,m?.unit||type,signed),'value');if(signed&&finite(m?.value))v.classList.add(Number(m.value)>=0?'positive':'negative');numbers.append(v,el('div',m?.state||'UNKNOWN','quality'));n.append(text,numbers);target.append(n);
  }
  function renderMoney(p) {
    const target=$('accounting');target.replaceChildren();
    for(const [key,label,explanation,signed] of [['grossCapitalDeployed','Money put in','Total spent on observed purchases.',false],['realizedCash','Money received back','All sale receipts, including money originally put in.',false],['realizedPnl','Profit / loss from sales','Sale receipts minus the cost of tokens sold.',true],['unrealizedPnl','Profit / loss still held','Change in value of tokens you still own. It can change.',true],['totalPnl','Total profit / loss','Sales profit plus the change in value of tokens still held.',true],['netCapitalAtRisk','Original money still at risk','Money put in that sales have not paid back.',false],['remainingCostBasis','Cost of tokens still held','Uses your oldest purchases first, called FIFO.',false],['feesUsd','Separately observed fees','Embedded platform fees may already be in wallet cash changes. Missing fees are not zero.',false]]) moneyRow(target,label,finance(p,key),explanation,{signed,highlight:key==='totalPnl'});
  }
  function renderHealth(p) {
    const target=$('dataHealth');target.replaceChildren();const coverage=p.coverage;
    const checks=[['Wallet balance check',coverage.reconciliation.state==='MATCH'?'Matches the observed trades':coverage.reconciliation.state==='MISMATCH'?'Needs review · trades and balance differ':'Not known yet'],['Older wallet activity',coverage.historyExhausted?'Available history checked · coverage partial':'Still being checked'],['New wallet activity',coverage.headCaughtUp?'Caught up at last check':'Catching up'],['Market timing',p.market.priceUsd.state==='DELAYED'?'Our last observation is delayed':p.market.priceUsd.value==null?'No price available':'Recent fetch · provider timing unknown'],['Large-holder history','Unavailable'],['Developer wallet','Not identified'],['Historical dollar prices',p.accounting.realizedPnl.value!=null?'Available for observed trades':'Unavailable or incomplete']];
    for(const [label,value]of checks){const n=el('div',undefined,'health-row');n.append(el('span',label),el('span',value,'health-value'));target.append(n);}
    target.append(el('p','PARTIAL means we have observations, with gaps in timing, history, or source coverage. UNKNOWN means we cannot support a value. DELAYED means an observation is old. LIVE is reserved for verified fresh source timestamps.','health-intro'));
    $('coverage').textContent=JSON.stringify({coverage,market:p.market,quoteAccounting:{cashBasis:p.quoteAccounting.cashBasis,feeItemization:p.quoteAccounting.feeItemization},peak:p.peak},null,2);
  }
  function renderTimeline(p) {
    const target=$('timeline'),open=new Set([...target.querySelectorAll('details[open]')].map(d=>d.dataset.eventId));target.replaceChildren();
    const events=[...p.events].reverse().filter(e=>eventFilter==='all'||group(e)===eventFilter);$('eventCount').textContent=p.eventCount.toLocaleString();
    for(const e of events){const n=el('article',undefined,'event'),kind=group(e),bubble=el('div',e.category==='BUY'?'B':e.category==='SELL'?'S':kind==='notes'?'✎':kind==='risk'?'!':'↗','event-bubble');bubble.dataset.kind=kind==='trades'?e.category:kind;
      const content=el('div'),heading=el('div',undefined,'event-heading'),stamp=el('time',time(e.occurredAt));stamp.dateTime=e.occurredAt;heading.append(el('h3',names[e.category]||title(e.category)),stamp);
      content.append(heading,el('p',eventSummary(e,p)),el('p',sourceName(e.source),'event-source'));const details=technical(e);details.open=open.has(details.dataset.eventId);content.append(details);n.append(bubble,content);target.append(n);
    }
    if(!events.length)target.append(el('p','No events in this view yet.','timeline-empty'));
    if(p.eventCount>p.events.length)target.append(el('p','Showing the most recent '+p.events.length+' events. Older evidence is retained.','data-note'));
    const notes=$('savedNotes');notes.replaceChildren();for(const e of p.events.filter(e=>e.category==='OPERATOR_NOTE').slice(-3).reverse()){const n=el('div',e.evidence,'saved-note'),stamp=el('time',time(e.occurredAt));stamp.dateTime=e.occurredAt;n.append(stamp);notes.append(n);}
  }
  function render(p) {
    renderOpportunityDetail(p.opportunity);
    const tradeKey=p.events.filter(e=>['BUY','SELL','TRANSFER_IN','TRANSFER_OUT'].includes(e.category)).map(e=>e.id).join(':');if(lastTradeKey!==tradeKey)verifiedMetrics.clear();lastTradeKey=tradeKey;renderedAt=Date.now();
    lastData=p;document.title=p.symbol+' · Signal';$('position').hidden=false;$('symbol').textContent=p.symbol;$('tokenAvatar').textContent=p.symbol.slice(0,1).toUpperCase();$('address').textContent=p.tokenAddress;$('address').title=p.tokenAddress;
    $('lifecycle').textContent=(stages[p.lifecycle]||title(p.lifecycle))+' · '+(research[p.signalState.value||'UNKNOWN']||title(p.signalState.value));
    const strip=$('priceStrip');strip.replaceChildren();
    for(const [label,m,type,signed]of [['Price',p.market.priceUsd,'price',false],['24h',{...p.market.change24hPercent,value:p.market.change24hPercent.value==null?null:p.market.change24hPercent.value/100},'percent',true],['Market cap',p.market.marketCapUsd,'usd',false],['Pool liquidity',p.market.liquidityUsd,'usd',false]]){const n=el('div',undefined,'price-item'),v=el('strong',fmt(m.value,type,signed));if(signed&&finite(m.value))v.classList.add(Number(m.value)>=0?'positive':'negative');n.append(el('span',label),v);if(label==='Price')n.append(el('span',m.state,'quality-tag'));strip.append(n);}
    const s=$('summary');s.replaceChildren();metric(s,'Still holding',p.accounting.positionValue,{featured:true,explanation:'Current value of tokens in your wallet.'});metric(s,'Money received back',displayFinance(p,'realizedCash'),{explanation:'From sales. Includes original money.'});metric(s,'Total profit / loss',displayFinance(p,'totalPnl'),{signed:true,explanation:'Sales profit + change in what you hold.'});metric(s,'Below the highest seen',{value:p.peak.postEntry.drawdown,state:p.peak.quality},{type:'percent',explanation:'From the highest price Signal observed after entry.'});
    const context=$('positionContext');context.replaceChildren();const held=el('span');held.append(document.createTextNode('Tokens remaining '),el('strong',fmt(p.accounting.tokensRemaining.value,'tokens')));const risk=el('span');risk.append(document.createTextNode('Original money still at risk '),el('strong',fmt(finance(p,'netCapitalAtRisk')?.value,finance(p,'netCapitalAtRisk')?.unit)));context.append(held,risk);
    renderRisk(p);renderMoney(p);renderHealth(p);renderTimeline(p);renderZones(p);chart?.setReferences([{price:p.peak.postEntry.priceUsd,label:'Highest observed',color:'#efc47c80'},{price:Number(p.accounting.weightedAverageAcquisitionPrice.value)||null,label:'Average purchase price · USD',color:'#b5a1ff80'}]);
    const m=$('market');m.replaceChildren();for(const [key,label,type]of [['priceUsd','Token price','price'],['marketCapUsd','Market cap','usd'],['liquidityUsd','Value in the trading pool','usd'],['volume24hUsd','Traded in 24 hours','usd'],['buys5m','Buy transactions · 5 min','count'],['sells5m','Sell transactions · 5 min','count'],['buyVolume5mUsd','Money spent buying · 5 min','usd'],['sellVolume5mUsd','Money received selling · 5 min','usd'],['buyers5m','Unique buyers · 5 min','count'],['sellers5m','Unique sellers · 5 min','count']])metric(m,label,p.market[key],{type});
    metric(m,'Highest seen after entry',{value:p.peak.postEntry.priceUsd,state:p.peak.quality},{type:'price'});metric(m,'Highest seen after first call',{value:p.peak.postCall.priceUsd,state:p.peak.quality},{type:'price'});
    const average=finance(p,'weightedAverageAcquisitionPrice');metric(m,'Average price paid',average?.unit==='USDC'?{...average,unit:'quotePrice'}:average,{type:'price'});
  }
  function displayFinance(p,key){
    const m=finance(p,key),cacheKey=p.tokenAddress+':'+key;
    if(finite(m?.value)){verifiedMetrics.set(cacheKey,{...m,verifiedAt:p.asOf});return m;}
    const old=verifiedMetrics.get(cacheKey),age=old?(Date.parse(p.asOf)-Date.parse(old.verifiedAt))/1000:Infinity;
    if(old&&age>=0&&age<=300&&!p.coverage.headCaughtUp&&p.coverage.reconciliation.state==='MATCH')return {...old,state:'DELAYED',displayOnly:true,ageSeconds:Math.floor(age),reason:'Last verified value, retained while the wallet is checked. This is not a current verified P&L.'};
    return m;
  }
  function renderZones(p){
    const data=p.researchZones||{zones:[],counts:{},missing:[],quality:'UNKNOWN'},summary=$('zoneSummary'),target=$('zoneCards');
    const expanded=new Set([...target.querySelectorAll('[data-zone-card]')].filter(card=>card.querySelector('details')?.open).map(card=>card.dataset.zoneCard));summary.replaceChildren();target.replaceChildren();
    const stats=el('div',undefined,'research-summary');for(const text of [(data.counts.recordedInWindow||0)+' saved setups',(data.counts.conditionsObserved||0)+' conditions observed',(data.counts.invalidated||0)+' invalidated',(data.counts.expired||0)+' expired'])stats.append(el('span',text));summary.append(stats);
    summary.append(el('p',data.lastCheckedAt?'Last evaluated '+time(data.lastCheckedAt)+' · '+data.quality:'Waiting for the first research evaluation.','data-note'));
    if(data.quality==='DELAYED')summary.append(el('p','Evaluation is delayed. These are saved watch areas; current conditions have not been verified.','data-note'));
    for(const missing of data.missing||[])summary.append(el('p',missing,'data-note'));
    if(!data.zones.length)empty(target,'No qualifying watch area yet',data.reason||'Waiting for enough completed candles and current observations.');
    for(const zone of data.zones.slice(0,6)){
      const card=el('article',undefined,'zone-card');card.dataset.zoneCard=zone.id;card.dataset.state=zone.state;const heading=el('div',undefined,'zone-heading');heading.append(el('h3',zone.kind==='RE_ENTRY'?(zone.tier==='DEEP'?'Deep re-entry watch':'Primary re-entry watch'):'Entry watch'),el('span',title(zone.state),'zone-state'));card.append(heading,el('p',fmt(zone.lower,'price')+' – '+fmt(zone.upper,'price'),'zone-range'),el('p','Recorded '+time(zone.createdAt)+' · expires '+time(zone.expiresAt),'data-note'));
      if(finite(zone.confidence))card.append(el('p',Math.round(Number(zone.confidence)*100)+'% structural confidence · research only','data-note'));if(zone.operatorOverride)card.append(el('p','Operator-adjusted working zone · computed range preserved in history.','data-note'));if(zone.stateReason)card.append(el('p',zone.stateReason,'muted'));
      if(['WATCHING','CONDITIONS_OBSERVED'].includes(zone.state)){const edit=el('details',undefined,'zone-editor'),sum=el('summary','Adjust working zone');edit.append(sum);const form=el('form',undefined,'zone-edit-form'),lo=el('input'),hi=el('input'),save=el('button','Save operator zone','text-button');lo.type=hi.type='number';lo.step=hi.step='any';lo.value=zone.lower;hi.value=zone.upper;lo.setAttribute('aria-label','Zone lower price');hi.setAttribute('aria-label','Zone upper price');save.type='submit';form.append(el('label','Lower'),lo,el('label','Upper'),hi,save);form.onsubmit=async ev=>{ev.preventDefault();save.disabled=true;save.textContent='Saving…';try{const r=await fetch('/api/v1/signal/positions/'+encodeURIComponent(ca)+'/zones/'+zone.id+'/override',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({lower:Number(lo.value),upper:Number(hi.value)})});if(!r.ok)throw Error((await r.json().catch(()=>({}))).error||'Unable to save zone');save.textContent='Saved';clearTimeout(timer);await load();}catch(e){save.disabled=false;save.textContent=e.message||'Try again';}};edit.append(form);card.append(edit);}
      const grid=el('div',undefined,'zone-copy-grid');for(const [label,rows]of [['Why here?',zone.why],['What are we waiting for?',zone.waiting],['What would cancel it?',[`A new completed five-minute candle below ${fmt(zone.invalidBelow,'price')}.`,'A 20% drop in observed trading-pool value, high distribution risk, a pool change, or expiry.']],["What don't we know?",zone.unknown]]){const part=el('div');part.append(el('h4',label));const ul=el('ul');for(const row of rows||[])ul.append(el('li',row));part.append(ul);grid.append(part);}card.append(grid);
      if(zone.paper.reference){const paper=el('div',undefined,'zone-paper-result');paper.append(el('strong','Paper price observations · no assumed trade fill'));for(const check of zone.paper.checkpoints)paper.append(el('p',check.minutes+' minute comparison: '+(check.state==='OBSERVED'?fmt(check.grossPriceChange,'percent',true)+' gross price change':'unavailable')+'. Net return, fees and slippage remain unknown.'));if(!zone.paper.checkpoints.length)paper.append(el('p','Waiting for the 15-minute and one-hour observation windows.'));card.append(paper);}
      const audit=el('details',undefined,'technical-details');audit.open=expanded.has(zone.id);audit.append(el('summary','Rule version & saved evidence'),el('p','Method '+zone.version+' · research only · not validated.'));card.append(audit);target.append(card);
      const revision=zone.knownAt||zone.createdAt,cached=zoneHistory.get(zone.id);
      if(cached){audit.append(el('pre',JSON.stringify(cached.payload,null,2)));if(cached.revision!==revision)audit.append(el('p','Saved evidence has changed. Reload to see the latest history.','data-note'));}
      if(!cached||cached.revision!==revision){const button=el('button',zoneHistoryPending.has(zone.id)?'Loading saved history…':'Load saved zone history');button.type='button';button.disabled=zoneHistoryPending.has(zone.id);audit.append(button);button.onclick=async()=>{zoneHistoryPending.add(zone.id);button.disabled=true;try{const r=await fetch('/api/v1/signal/positions/'+encodeURIComponent(ca)+'/zones/'+zone.id,{credentials:'same-origin',cache:'no-store',signal:AbortSignal.timeout(15000)});if(!r.ok)throw Error();zoneHistory.set(zone.id,{revision,payload:await r.json()});}catch{button.textContent='History unavailable. Try again.';}finally{zoneHistoryPending.delete(zone.id);if(lastData)renderZones(lastData);}};}
    }
    $('zoneMethod').textContent='Method '+(data.version||'pending')+': recent defended support is ranked using recency, proximity, repeated defense and subsequent impulse strength. Operator adjustments are stored as overrides and never rewrite the computed research record. Missing required data blocks that transition. Zones expire after two hours. Samples do not establish completed research candles. No win-rate or predictive confidence is claimed. Counts cover up to the latest 100 saved setups.';
    renderExposure();
  }
  function renderExposure(){
    const target=$('exposureResult'),input=$('researchAmount').value;if(!input){target.textContent='A read-only estimate of original money at risk. No order is created.';return;}
    const amount=Number(input),p=lastData;if(!Number.isFinite(amount)||amount<0||amount>1000000){target.textContent='Enter an amount from 0 to 1,000,000 USDC.';return;}
    const cash=p?.quoteAccounting?.netCashFlow?.value,age=p?Date.parse(p.asOf)-Date.parse(p.coverage.walletObservedAt)+(Date.now()-renderedAt):Infinity;
    if(!finite(cash)||!p.coverage.historyExhausted||!p.coverage.headCaughtUp||p.coverage.reconciliation.state!=='MATCH'||age>60000){target.textContent='Wait for a current, matching wallet check before estimating additional money at risk.';return;}
    const before=Math.max(0,-Number(cash)),after=Math.max(0,amount-Number(cash));target.textContent='An additional '+fmt(amount,'USDC')+' would change original money not yet paid back from '+fmt(before,'USDC')+' to about '+fmt(after,'USDC')+', before additional fees or slippage. All tokens still held can lose value. This does not choose a purchase amount or create an order.';
  }
  function total(positions,key){if(!positions.length)return {value:null,state:'UNKNOWN'};const values=positions.map(p=>finance(p,key)),units=new Set(values.filter(v=>v?.value!=null).map(v=>v.unit||'USD'));if(values.some(v=>!finite(v?.value))||units.size!==1)return {value:null,state:'UNKNOWN'};return {value:values.reduce((sum,m)=>sum+Number(m.value),0),unit:units.has('USDC')?'USDC':undefined,state:'PARTIAL'};}
  function renderHome(data){homeData=data;$('home').hidden=false;$('today').textContent=new Date().toLocaleDateString(undefined,{weekday:'long',month:'long',day:'numeric'});$('positionCount').textContent=String(data.positions.length);
    renderDiscovery(data.opportunityDiscovery);
    const target=$('portfolioSummary');target.replaceChildren();metric(target,'Value still held',total(data.positions,'positionValue'),{featured:true,explanation:'Tokens remaining across your tracked positions.',icon:'◫'});metric(target,'Money received back',total(data.positions,'realizedCash'),{explanation:'Total sale receipts. Not all of this is profit.',icon:'↙'});metric(target,'Total profit / loss',total(data.positions,'totalPnl'),{signed:true,explanation:'Sales profit plus the change in tokens still held.',icon:'↗'});metric(target,'Original money at risk',total(data.positions,'netCapitalAtRisk'),{explanation:'Money put in that sales have not paid back.',icon:'◇'});
    renderPositions();const signals=$('signals');signals.replaceChildren();
    for(const signal of data.liveSignals){const watch=data.positions.find(p=>p.tokenAddress===signal.tokenAddress),row=el('a',undefined,'signal-row');row.href='/signal-v1/positions/'+encodeURIComponent(signal.tokenAddress);const head=el('div',undefined,'signal-header');head.append(el('strong',watch?.symbol||signal.tokenAddress.slice(0,6)+'…'+signal.tokenAddress.slice(-4)),riskBadge(signal.risk));row.append(head,el('p',sourceName(signal.source)+' · '+(research[signal.researchState]||title(signal.researchState))));const facts=el('div',undefined,'signal-facts');facts.append(el('span','Cap '+fmt(signal.marketCap?.value)),el('span','Pool '+fmt(signal.liquidity?.value)),el('span',Math.floor(signal.ageSeconds/60)+'m since event'));row.append(facts);signals.append(row);}
    if(!data.liveSignals.length)empty(signals,'No tracked calls yet','New eligible mentions will appear here. Missing activity does not mean a token is safe.');
  }
  function amplifierLabel(p){return p.amplifiers.some(x=>x.status==='OBSERVED')?'Major amplifier observed':p.amplifiers.some(x=>x.status==='COVERAGE_UNKNOWN')?'Major amplifier coverage incomplete · arrival unknown':'AMPLIFIER NOT YET ARRIVED in observed coverage · arrival is not predicted';}
  function opportunityCard(p){
    const card=el('a',undefined,'position-card opportunity-card');card.href='/signal-v1/positions/'+encodeURIComponent(p.tokenAddress);card.dataset.state=p.opportunityState;
    const header=el('div',undefined,'card-top');header.append(el('h3',p.symbol||p.tokenAddress.slice(0,8)),el('span',p.opportunityState+' · '+p.opportunityScore+'/100','quiet-badge'));card.append(header,el('p','First seen '+time(p.firstSeenAt),'muted'));
    const facts=el('div',undefined,'card-grid');for(const [k,v]of [['Market cap',fmt(p.marketCap)],['Pool value',fmt(p.liquidity)],['Independent clusters',String(p.independentClusters.length)],['Freshness',p.evaluationDelayed?'DELAYED':p.quality]]){const cell=el('div');cell.append(el('span',k,'label'),el('strong',v));facts.append(cell);}card.append(facts);
    card.append(el('p','Best evidence: '+(p.evidence.increases.find(e=>e.code!=='EMPIRICAL_CALL')?.text||p.evidence.increases[0]?.text||'No current favorable evidence.')),el('p','Biggest risk: '+(p.evidence.decreases[0]?.text||p.unknowns[0]||'Unknown'),'data-note'),el('p',amplifierLabel(p),'data-note'));
    if(p.preFrontRunners)card.append(el('span','PRE-FRONT-RUNNERS OBSERVATION','quiet-badge'));return card;
  }
  function renderDiscovery(data){
    const h=$('hotCards'),w=$('watchCards');if(!h)return;h.replaceChildren();w.replaceChildren();
    const rows=data?.opportunities||[],hot=rows.filter(p=>p.opportunityState==='HOT'&&!p.evaluationDelayed&&p.quality!=='DELAYED'),watch=rows.filter(p=>!hot.includes(p)).sort((a,b)=>b.opportunityScore-a.opportunityScore);
    for(const p of hot)h.append(opportunityCard(p));for(const p of watch.slice(0,30))w.append(opportunityCard(p));
    if(!hot.length)empty(h,'No hot setups established','No alert means insufficient evidence, not a safe market.');if(!watch.length)empty(w,'Waiting for verified observations','New contract addresses can appear here before you hold the token.');
    const state=data?.sourceHealth,capacity=data?.capacity,counts=state?.sourceHealth;
    $('opportunityStatus').textContent=!data?.enabled?'Opportunity discovery is not activated yet.':data.quality==='DELAYED'?'Collection is delayed or unavailable. Saved scores are historical.':`Last collection ${time(state?.knownAt)} · ${counts?.activeSources??capacity?.liveSources??0} active sources · evidence coverage remains partial.`;
    const m=$('sourceMetrics');m.replaceChildren();for(const [label,value,type]of [['Active sources',counts?.activeSources??capacity?.liveSources,'count'],['Unavailable sources',counts?.unavailableSources,'count'],['CA calls / 24h',counts?.callsPerDay,'count'],['Unique tokens / 24h',counts?.uniqueTokensPerDay,'count'],['Median ingestion latency (ms)',state?.medianIngestionLatencyMs,'count'],['CA-bearing message share',counts?.caParseSuccess,'percent'],['Unknown cluster relationships',counts?.clusterUnknownRate,'percent'],['Deferred market enrichment',capacity?.deferredTokens,'count']])metric(m,label,{value:value??null,state:value==null?'UNKNOWN':'PARTIAL'},{type});
    const src=$('sourceRows');src.replaceChildren();for(const s of state?.sources||[]){const row=el('div',undefined,'health-row');row.append(el('strong',s.displayName),el('span',(s.active?'ACTIVE':'UNAVAILABLE')+' · '+s.identityStatus+' · '+s.sourceRole+' · cluster '+s.clusterId+' · '+(s.correlatedWith?.length?'correlated with '+s.correlatedWith.join(', ')+'; others UNKNOWN':'relationships UNKNOWN')));src.append(row);}if(!state?.sources?.length)src.append(el('p','Source access has not been verified by this collector.','muted'));
  }
  function renderOpportunityDetail(p){
    const section=$('tokenIntelligence');if(!section)return;section.hidden=!p;if(!p)return;
    const target=$('opportunityDetail');target.replaceChildren();target.append(el('p',`${p.opportunityState} · ${p.opportunityScore}/100 · ${p.definitionVersion} · ${p.quality}`,'quiet-badge'),el('p','Known at '+time(p.knowledgeAt),'muted'),el('p',amplifierLabel(p)));
    if(p.preFrontRunners)target.append(el('p','PRE-FRONT-RUNNERS OBSERVATION'+(p.frontRunnersLeadMs!=null?' · lead '+Math.round(p.frontRunnersLeadMs/60000)+' minutes':''),'accent'));
    for(const [heading,items]of [['WHY SCORE INCREASED',p.evidence.increases.map(e=>`+${e.points}: ${e.text}`)],['WHY SCORE DECREASED',p.evidence.decreases.map(e=>`${e.points}: ${e.text}`)],['WHAT IS UNKNOWN',p.unknowns]]){const block=el('div',undefined,'evidence-section');block.append(el('h3',heading));const list=el('ul');for(const text of items.length?items:['No evidence recorded.'])list.append(el('li',text));block.append(list);target.append(block);}
    const seq=$('callerSequence');seq.replaceChildren();for(const [i,c]of p.callerSequence.entries()){const row=el('div',undefined,'health-row');row.append(el('strong',(i+1)+'. '+c.sourceId),el('span',time(c.occurredAt)+' · MC '+fmt(c.marketCap)+' · '+c.role+(c.forwarding?' · FORWARDED':'')));seq.append(row);}
  }
  function renderPositions(){const target=$('positions');target.replaceChildren();const query=$('positionSearch').value.trim().toLowerCase(),rows=homeData.positions.filter(p=>{const qty=p.accounting.tokensRemaining.value;return (p.symbol.toLowerCase().includes(query)||p.tokenAddress.toLowerCase().includes(query))&&(positionFilter==='all'||finite(qty)&&(positionFilter==='holding'?Number(qty)>0:Number(qty)===0));});
    for(const p of rows){const card=el('a',undefined,'position-card');card.href='/signal-v1/positions/'+encodeURIComponent(p.tokenAddress);const top=el('div',undefined,'card-top'),identity=el('div',undefined,'token-identity'),copy=el('div');copy.append(el('h3',p.symbol),el('p','Solana · '+(stages[p.lifecycle]||title(p.lifecycle)),'token-subtitle'));identity.append(el('div',p.symbol.slice(0,1).toUpperCase(),'token-avatar'),copy);top.append(identity,riskBadge(p.risk));card.append(top,el('div',fmt(p.accounting.positionValue.value),'position-value'),el('p','Value of tokens still in your wallet','muted'));
      const grid=el('div',undefined,'card-grid');for(const [label,m,type,signed]of [['Money back',finance(p,'realizedCash'),'usd',false],['Total profit / loss',finance(p,'totalPnl'),'usd',true],['Below highest seen',{value:p.drawdown},'percent',false],['Tokens remaining',p.accounting.tokensRemaining,'tokens',false]]){const n=el('div'),value=el('div',fmt(m?.value,m?.unit||type,signed),'value');if(signed&&finite(m?.value))value.classList.add(Number(m.value)>=0?'positive':'negative');n.append(el('div',label,'label'),value);grid.append(n);}card.append(grid);const bottom=el('div',undefined,'card-bottom');bottom.append(el('span',p.reconciliation==='MATCH'?'Wallet balance matches · PARTIAL':'Wallet coverage · PARTIAL'),el('span','Open position ↗','open-position'));card.append(bottom);target.append(card);
    }
    if(!rows.length)empty(target,homeData.positions.length?'No positions match this view':'No watched positions yet',homeData.positions.length?'Try another search or filter.':'Configured public wallet positions will appear here.');
  }
  function connection(message,state){$('connection').textContent=message;$('connection').dataset.state=state;$('connectionDot').dataset.state=state;$('connectionShort').textContent=state==='connected'?'Auto-updating':stopped?'Sign in needed':'Reconnecting';}
  async function api(url){const r=await fetch(url,{signal:controller?.signal,credentials:'same-origin',cache:'no-store'});if(r.status===401||r.status===403){stopped=true;throw Error('Session expired. Sign in to resume.');}if(r.status===404)throw Error('This position is not being watched. Return to all positions.');if(!r.ok)throw Error('Dashboard data unavailable. Your last saved view is still here.');return r.json();}
  async function load(){if(inflight){reloadPending=true;return;}if(stopped||document.hidden)return;inflight=true;reloadPending=false;controller=new AbortController();$('refresh').disabled=true;const timeout=setTimeout(()=>controller.abort(),20000);
    try{if(ca){const p=await api('/api/v1/signal/stream/'+encodeURIComponent(ca));render(p);const requested=interval,candles=await api('/api/v1/signal/positions/'+encodeURIComponent(ca)+'/candles?interval='+requested);if(interval===requested){chart.setData(candles.candles,p.events,durations[interval]);chart.setZones(p.researchZones?.zones||[],$('showZones').checked);if(fitHistoryPending){chart.fitHistory();fitHistoryPending=false;}const sampled=candles.candles.some(c=>['SAMPLED_OBSERVATIONS_ONLY','PROVIDER_AND_SAMPLED_OBSERVATIONS'].includes(c.coverage));$('chartQuality').textContent=!candles.candles.length?'UNKNOWN · No candles available yet. Waiting for actual price observations.':candles.quality+' · '+(sampled?'Built from prices Signal actually collected. Earlier history or trades between samples may be missing.':'Provider price candles. Coverage is limited to returned observations.')+' Gaps stay empty. Provider update time is unknown.';}}
      else renderHome(await api('/api/v1/signal/positions'));
      retryMs=10000;connection('Connected · updates automatically about every 10 seconds · data coverage is shown with each value.','connected');
    }catch(error){if(!document.hidden){connection(error.name==='AbortError'?'Connection interrupted. Keeping your last view and retrying.':error.message,'offline');if(stopped){const a=el('a',' Sign in →');a.href='/login';$('connection').append(a);}}retryMs=Math.min(retryMs*2,60000);}
    finally{clearTimeout(timeout);inflight=false;controller=null;$('refresh').disabled=false;clearTimeout(timer);if(!stopped&&!document.hidden)timer=setTimeout(load,reloadPending?50:retryMs);}
  }
  if(ca){chart=new window.SignalCandleChart($('chart'),eventDetail);for(const value of Object.keys(durations)){const b=el('button',value);b.type='button';b.setAttribute('aria-pressed',String(value===interval));b.onclick=()=>{interval=value;for(const x of $('intervals').children)x.setAttribute('aria-pressed',String(x.textContent===value));chart.setData([],lastData?.events||[],durations[value]);clearTimeout(timer);load();};$('intervals').append(b);}
    for(const [label,color,letter]of [['Bought','#7fe1bc','B'],['Sold','#ff91a7','S'],['Call / promo','#b5a1ff','C'],['Risk','#efc47c','!']]){const n=el('span');n.style.color=color;n.append(el('b',letter),document.createTextNode(label));$('legend').append(n);}
  }
  $('chartModes').addEventListener('click',e=>{const button=e.target.closest('button[data-mode]');if(!button)return;for(const b of $('chartModes').children)b.setAttribute('aria-pressed',String(b===button));chart?.setMode(button.dataset.mode);});
  $('showZones').onchange=()=>chart?.setZones(lastData?.researchZones?.zones||[],$('showZones').checked);
  $('chartFullscreen').onclick=()=>{const panel=document.getElementById('market-chart');panel.classList.toggle('is-expanded');$('chartFullscreen').textContent=panel.classList.contains('is-expanded')?'Close expanded chart ×':'Expand chart ⛶';};
  document.addEventListener('keydown',e=>{if(e.key==='Escape'){document.getElementById('market-chart').classList.remove('is-expanded');$('chartFullscreen').textContent='Expand chart ⛶';}});
  $('researchAmount').oninput=renderExposure;
  $('chartReset').onclick=()=>chart?.resetView();
  $('chartHistory').onclick=()=>{interval='15m';fitHistoryPending=true;for(const button of $('intervals').children)button.setAttribute('aria-pressed',String(button.textContent===interval));clearTimeout(timer);load();};
  $('chartLayers').addEventListener('click',e=>{const b=e.target.closest('button[data-layer]');if(!b)return;layers[b.dataset.layer]=!layers[b.dataset.layer];b.setAttribute('aria-pressed',String(layers[b.dataset.layer]));chart?.setLayers(layers);});
  $('positionSearch').addEventListener('input',()=>homeData&&renderPositions());
  $('positionFilters').addEventListener('click',e=>{const b=e.target.closest('button[data-filter]');if(!b)return;positionFilter=b.dataset.filter;for(const n of $('positionFilters').children)n.setAttribute('aria-pressed',String(n===b));if(homeData)renderPositions();});
  $('eventFilters').addEventListener('click',e=>{const b=e.target.closest('button[data-filter]');if(!b)return;eventFilter=b.dataset.filter;for(const n of $('eventFilters').children)n.setAttribute('aria-pressed',String(n===b));if(lastData)renderTimeline(lastData);});
  $('copyAddress').onclick=async()=>{try{await navigator.clipboard.writeText(ca);$('copyStatus').textContent='Token address copied.';$('copyAddress').textContent='✓';setTimeout(()=>$('copyAddress').textContent='⧉',1800);}catch{$('copyStatus').textContent='Copy unavailable. Full address: '+ca;$('address').textContent=ca;$('address').style.maxWidth='none';$('address').style.whiteSpace='normal';}};
  $('refresh').onclick=()=>{clearTimeout(timer);retryMs=10000;load();};
  $('noteForm').addEventListener('submit',async e=>{e.preventDefault();const button=e.currentTarget.querySelector('button');if(!$('note').value.trim()){$('noteStatus').textContent='Write a note first.';return;}button.disabled=true;try{const r=await fetch('/api/v1/signal/positions/'+encodeURIComponent(ca)+'/annotations',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json'},body:JSON.stringify({text:$('note').value}),signal:AbortSignal.timeout(10000)});if(!r.ok)throw Error('Your note could not be saved. Please try again.');$('note').value='';$('noteStatus').textContent='Note saved. Your thinking stays separate from market evidence.';clearTimeout(timer);load();}catch(error){$('noteStatus').textContent=error.message;}finally{button.disabled=false;}});
  document.addEventListener('visibilitychange',()=>{clearTimeout(timer);if(document.hidden)controller?.abort();else{retryMs=10000;load();}});window.addEventListener('online',()=>{clearTimeout(timer);load();});window.addEventListener('pagehide',()=>{stopped=true;controller?.abort();clearTimeout(timer);});load();
})();
