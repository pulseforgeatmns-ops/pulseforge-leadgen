'use strict';
(function(root){
 const L=root.LightweightCharts;
 const styles={BUY:['B','#7fe1bc'],SELL:['S','#ff91a7'],CALLER:['C','#a4b9ff'],FIRST_CALLER:['F','#a4b9ff'],FRONT_RUNNERS_CALL:['FR','#b5a1ff'],INDEPENDENT_CONVERGENCE:['IC','#77d9eb'],PROMOTION_EVENT:['P','#d8b276'],DEX_BOOST:['DX','#d8b276'],AMPLIFIER_EVENT:['A','#d8b276'],DE_RISK_MILESTONE:['DR','#7fe1bc'],DISTRIBUTION_NORMAL:['N','#a2cdb5'],DISTRIBUTION_WATCH:['W','#efc47c'],DISTRIBUTION_HIGH:['H','#ff91a7'],DISTRIBUTION_CRITICAL:['!','#ff91a7'],LIQUIDITY_WARNING:['L','#dfaaff'],TRANSFER_IN:['IN','#a8b5cc'],TRANSFER_OUT:['OUT','#a8b5cc']};
 function timeline(candles,interval){
  if(!candles.length)return[];const first=candles[0].time,last=candles.at(-1).time,byTime=new Map(candles.map(c=>[c.time,c]));
  if((last-first)/interval>20000)return candles;
  const rows=[];for(let time=first;time<=last;time+=interval)rows.push(byTime.get(time)||{time});return rows;
 }
 function segments(candles,interval){const all=[];for(const c of candles){const last=all.at(-1);if(!last||c.time-last.at(-1).time>interval)all.push([c]);else last.push(c);}return all;}
 class SignalCandleChart{
  constructor(container,onSelect){
   if(!L)throw Error('chart_library_unavailable');this.container=container;this.onSelect=onSelect;this.mode='candles';this.interval=60;this.candles=[];this.events=[];this.zones=[];this.references=[];this.referenceHandles=[];this.extraLines=[];this.showZones=true;this.layers={trades:true,calls:true,risk:true};this.times=[];this.autoFollow=true;
   this.chart=L.createChart(container,{autoSize:true,layout:{background:{type:'solid',color:'#14171f'},textColor:'#8b99b0',fontFamily:'Inter, system-ui, sans-serif',fontSize:10,attributionLogo:true},grid:{vertLines:{color:'#222835'},horzLines:{color:'#222835'}},crosshair:{mode:L.CrosshairMode.Normal,vertLine:{color:'#b5a1ff70',labelBackgroundColor:'#42365c'},horzLine:{color:'#b5a1ff70',labelBackgroundColor:'#42365c'}},rightPriceScale:{borderColor:'#303748',scaleMargins:{top:.12,bottom:.22}},timeScale:{borderColor:'#303748',timeVisible:true,secondsVisible:false,rightOffset:5,barSpacing:9,minBarSpacing:1,fixLeftEdge:false,fixRightEdge:false},handleScroll:{mouseWheel:true,pressedMouseMove:true,horzTouchDrag:true,vertTouchDrag:false},handleScale:{mouseWheel:true,pinch:true,axisPressedMouseMove:true}});
   this.candleSeries=this.chart.addSeries(L.CandlestickSeries,{upColor:'#7fe1bc',downColor:'#ff91a7',borderVisible:false,wickUpColor:'#7fe1bc',wickDownColor:'#ff91a7'});
   this.lineSeries=this.chart.addSeries(L.LineSeries,{color:'#b5a1ff',lineWidth:2,visible:false,pointMarkersVisible:true,pointMarkersRadius:2});
   this.overlayNodes=new Map();this.overlay=document.createElement('div');this.overlay.className='chart-overlay';container.append(this.overlay);this.hits=[];
   this.chart.timeScale().subscribeVisibleLogicalRangeChange(()=>this.scheduleOverlay());
   this.chart.subscribeCrosshairMove(p=>{const hover=document.getElementById('chartHover');if(hover&&p.time){const original=this.candles.find(c=>c.time===p.time);if(!original)hover.textContent='No collected price in this interval.';else hover.textContent=new Date(original.time*1000).toLocaleString()+' · '+(this.mode==='candles'?`Open ${this.price(original.open)} · High ${this.price(original.high)} · Low ${this.price(original.low)} · Close ${this.price(original.close)}`:`Recorded close ${this.price(original.close)}`);}this.scheduleOverlay();});
   container.addEventListener('pointermove',()=>this.scheduleOverlay());container.addEventListener('pointerdown',e=>{if(!e.target.closest('button'))this.autoFollow=false;});container.addEventListener('wheel',()=>this.autoFollow=false,{passive:true});
   container.addEventListener('keydown',e=>{const scale=this.chart.timeScale(),r=scale.getVisibleLogicalRange();if(!r||!['+','=','-','ArrowLeft','ArrowRight','Home'].includes(e.key))return;e.preventDefault();this.autoFollow=false;if(e.key==='Home'){this.resetView();return;}const span=r.to-r.from,mid=(r.to+r.from)/2;if(['+','=','-'].includes(e.key)){const half=Math.max(3,span*(e.key==='-'?.6:.4));scale.setVisibleLogicalRange({from:mid-half,to:mid+half});}else{const shift=span*.2*(e.key==='ArrowLeft'?-1:1);scale.setVisibleLogicalRange({from:r.from+shift,to:r.to+shift});}});
   this.resize=new ResizeObserver(()=>this.scheduleOverlay());this.resize.observe(container);
  }
  price(v){return '$'+Number(v).toLocaleString(undefined,{maximumSignificantDigits:6});}
  setData(candles,events,interval){
   const changedInterval=this.interval!==interval,oldEnd=this.candles.at(-1)?.time,oldRange=this.chart.timeScale().getVisibleLogicalRange();this.interval=interval;this.candles=[...new Map(candles.filter(c=>Number.isSafeInteger(c.time)&&[c.open,c.high,c.low,c.close].every(Number.isFinite)&&c.low>0).map(c=>[c.time,c])).values()].sort((a,b)=>a.time-b.time);this.events=events;
   const rows=timeline(this.candles,interval);this.times=rows.map(c=>c.time);const min=Math.min(...this.candles.map(c=>c.low));const precision=Number.isFinite(min)?Math.min(12,Math.max(2,Math.ceil(-Math.log10(min))+3)):6;
   const format={type:'price',precision,minMove:Math.pow(10,-precision)};this.candleSeries.applyOptions({priceFormat:format});this.lineSeries.applyOptions({priceFormat:format});
   this.candleSeries.setData(rows.map(c=>c.open==null?{time:c.time}:{time:c.time,open:c.open,high:c.high,low:c.low,close:c.close}));
   for(const s of this.extraLines)this.chart.removeSeries(s);this.extraLines=[];
   const groups=segments(this.candles,interval),last=groups.pop()||[];this.lineSeries.setData(last.map(c=>({time:c.time,value:c.close})));
   for(const group of groups){const s=this.chart.addSeries(L.LineSeries,{color:'#b5a1ff',lineWidth:2,visible:this.mode==='line',priceLineVisible:false,lastValueVisible:false,pointMarkersVisible:true,pointMarkersRadius:2,priceFormat:format});s.setData(group.map(c=>({time:c.time,value:c.close})));this.extraLines.push(s);}
   this.candleSeries.applyOptions({visible:this.mode==='candles'});this.lineSeries.applyOptions({visible:this.mode==='line'});
   if(this.candles.length&&(changedInterval||!oldRange||!oldEnd))this.resetView();else if(oldRange){this.chart.timeScale().setVisibleLogicalRange(oldRange);if(this.autoFollow)this.chart.timeScale().scrollToRealTime();}
   this.drawReferences();this.scheduleOverlay();
  }
  setMode(mode){if(!['candles','line'].includes(mode))return;this.mode=mode;this.candleSeries.applyOptions({visible:mode==='candles'});this.lineSeries.applyOptions({visible:mode==='line'});for(const s of this.extraLines)s.applyOptions({visible:mode==='line'});this.drawReferences();this.scheduleOverlay();}
  setLayers(layers){this.layers={...this.layers,...layers};this.scheduleOverlay();}
  setZones(zones,show=true){this.zones=zones||[];this.showZones=show;this.scheduleOverlay();}
  setReferences(refs){this.references=refs||[];this.drawReferences();}
  drawReferences(){for(const [series,line]of this.referenceHandles)series.removePriceLine(line);this.referenceHandles=[];const series=this.mode==='candles'?this.candleSeries:this.lineSeries;for(const ref of this.references)if(Number.isFinite(ref.price)&&ref.price>0){const line=series.createPriceLine({price:ref.price,color:ref.color||'#b5a1ff80',lineWidth:1,lineStyle:L.LineStyle.Dashed,axisLabelVisible:false,title:ref.label});this.referenceHandles.push([series,line]);}}
  resetView(){if(!this.times.length)return;this.autoFollow=true;this.chart.timeScale().setVisibleLogicalRange({from:Math.max(-2,this.times.length-65),to:this.times.length+4});this.scheduleOverlay();}
  fitHistory(){this.autoFollow=false;this.chart.timeScale().fitContent();this.scheduleOverlay();}
  visibleEvent(e){const trade=['BUY','SELL','TRANSFER_IN','TRANSFER_OUT'].includes(e.category),risk=e.category.startsWith('DISTRIBUTION_')||['LIQUIDITY_WARNING','DE_RISK_MILESTONE'].includes(e.category);return trade?this.layers.trades:risk?this.layers.risk:this.layers.calls;}
  timeCoordinate(time){if(!this.times.length)return null;let lo=0,hi=this.times.length-1;while(lo<hi){const mid=Math.ceil((lo+hi)/2);if(this.times[mid]<=time)lo=mid;else hi=mid-1;}const scale=this.chart.timeScale(),base=scale.logicalToCoordinate(lo);if(base==null)return null;const next=this.times[lo+1],neighbor=scale.logicalToCoordinate(next!=null?lo+1:lo-1);if(neighbor==null)return base;const step=next!=null?neighbor-base:base-neighbor;return base+(time-this.times[lo])/(next!=null?next-this.times[lo]:this.interval)*step;}
  scheduleOverlay(){if(this.pending)return;this.pending=true;requestAnimationFrame(()=>{this.pending=false;this.drawOverlay();});}
  drawOverlay(){
   const wanted=new Set(),node=(key,tag,cls)=>{wanted.add(key);let n=this.overlayNodes.get(key);if(!n){n=document.createElement(tag);n.className=cls;this.overlayNodes.set(key,n);this.overlay.append(n);}return n;};this.hits=[];const width=this.container.clientWidth-65,height=this.container.clientHeight-35,series=this.mode==='candles'?this.candleSeries:this.lineSeries;if(!this.candles.length){this.overlay.replaceChildren();this.overlayNodes.clear();return;}
   if(this.showZones)for(const zone of this.zones){const from=this.timeCoordinate(Date.parse(zone.createdAt)/1000),to=this.timeCoordinate(Date.parse(zone.closedAt||zone.expiresAt)/1000),high=series.priceToCoordinate(zone.upper),low=series.priceToCoordinate(zone.lower);if([from,to,high,low].some(v=>v==null)||to<0||from>width||low<0||high>height)continue;
    const band=node('zone-band:'+zone.id,'div','research-zone-band');band.dataset.state=zone.state;band.dataset.zoneId=zone.id;band.setAttribute('aria-hidden','true');Object.assign(band.style,{left:Math.max(0,from)+'px',top:Math.max(0,high)+'px',width:Math.max(1,Math.min(width,to)-Math.max(0,from))+'px',height:Math.max(1,Math.min(height,low)-Math.max(0,high))+'px'});const b=node('zone-label:'+zone.id,'button','research-zone-label');b.type='button';b.dataset.state=zone.state;b.setAttribute('aria-label',(zone.kind==='RE_ENTRY'?'Re-entry':'Entry')+' research zone · '+zone.state);b.textContent=(zone.kind==='RE_ENTRY'?'Re-entry':'Entry')+' watch · research';Object.assign(b.style,{left:Math.max(0,Math.min(width-130,from))+'px',top:Math.max(0,high-23)+'px'});b.onclick=e=>{e.stopPropagation();this.onSelect({id:zone.id,category:'RESEARCH_ZONE',occurredAt:zone.createdAt,knownAt:zone.knownAt,evidence:zone,source:'Signal paper-observation research'});};
   }
   const lanes=new Map();for(const e of this.events){const style=styles[e.category];if(!style||!this.visibleEvent(e))continue;const time=Date.parse(e.occurredAt)/1000,x=this.timeCoordinate(time);if(x==null||x<9||x>width-9)continue;const bucket=Math.round(x/25),lane=lanes.get(bucket)||0;lanes.set(bucket,lane+1);const y=height-24-(lane%3)*23;
    const b=node('event:'+(e.id||e.category+e.occurredAt),'button','chart-event-marker');b.type='button';b.textContent=style[0];b.style.color=style[1];b.style.left=x+'px';b.style.top=y+'px';b.dataset.timestamp=String(time);b.dataset.eventId=e.id||'';b.setAttribute('aria-label',e.category+' · '+new Date(time*1000).toLocaleString());b.title=e.category+' · '+new Date(time*1000).toLocaleString();b.onclick=ev=>{ev.stopPropagation();this.onSelect(e);};this.hits.push({x,y,event:e});
   }
   for(const [key,n]of this.overlayNodes)if(!wanted.has(key)){n.remove();this.overlayNodes.delete(key);}
  }
 }
 root.SignalCandleChart=SignalCandleChart;root.signalMarkerStyles=styles;root.signalChartTimeline=timeline;
})(window);
