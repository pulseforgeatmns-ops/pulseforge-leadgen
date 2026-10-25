'use strict';
(function(root){
 const styles={BUY:['B','#7ae0b6','circle'],SELL:['S','#ff7e99','circle'],CALLER:['C','#92b7ff','triangle'],FIRST_CALLER:['F','#92b7ff','triangle'],FRONT_RUNNERS_CALL:['FR','#b6a4ff','triangle'],INDEPENDENT_CONVERGENCE:['IC','#55deef','diamond'],PROMOTION_EVENT:['P','#f5c97a','square'],DEX_BOOST:['DX','#ffb865','square'],AMPLIFIER_EVENT:['A','#f3ab6b','square'],DE_RISK_MILESTONE:['DR','#9beccb','diamond'],DISTRIBUTION_NORMAL:['N','#b2e5bd','diamond'],DISTRIBUTION_WATCH:['W','#ffd37d','diamond'],DISTRIBUTION_HIGH:['H','#ff927d','diamond'],DISTRIBUTION_CRITICAL:['!','#ff6d86','diamond'],LIQUIDITY_WARNING:['L','#f6a1ff','diamond'],TRANSFER_IN:['IN','#c7d1e6','square'],TRANSFER_OUT:['OUT','#c7d1e6','square']};
 class SignalCandleChart{
  constructor(canvas,onSelect){this.canvas=canvas;this.ctx=canvas.getContext('2d');this.onSelect=onSelect;this.candles=[];this.events=[];this.hits=[];this.range=null;this.pointers=new Map();
   this.resize=new ResizeObserver(()=>this.draw());this.resize.observe(canvas);
   canvas.addEventListener('wheel',e=>{e.preventDefault();this.zoom(e.deltaY>0?1.2:.8);},{passive:false});
   canvas.addEventListener('pointerdown',e=>{canvas.setPointerCapture(e.pointerId);this.pointers.set(e.pointerId,{x:e.offsetX,y:e.offsetY});this.drag={x:e.offsetX,y:e.offsetY,range:this.range?.slice(),moved:false};if(this.pointers.size===2)this.pinch=this.distance();});
   canvas.addEventListener('pointermove',e=>{if(!this.pointers.has(e.pointerId))return;this.pointers.set(e.pointerId,{x:e.offsetX,y:e.offsetY});
    if(this.pointers.size===2){const d=this.distance();if(this.pinch&&d)this.zoom(this.pinch/d);this.pinch=d;return;}
    if(this.drag?.range&&this.range){const dx=e.offsetX-this.drag.x;this.drag.moved=this.drag.moved||Math.abs(dx)>4;const shift=dx/(this.canvas.clientWidth-60)*(this.drag.range[1]-this.drag.range[0]);this.range=this.drag.range.map(x=>x-shift);this.draw();}});
   const up=e=>{if(!this.drag?.moved){const h=this.hits.map(h=>({...h,distance:Math.hypot(h.x-e.offsetX,h.y-e.offsetY)})).filter(h=>h.distance<18).sort((a,b)=>a.distance-b.distance)[0];if(h)this.onSelect(h.event);else this.inspect(e.offsetX);}this.pointers.delete(e.pointerId);this.drag=null;this.pinch=null;};
   canvas.addEventListener('pointerup',up);canvas.addEventListener('pointercancel',e=>{this.pointers.delete(e.pointerId);this.drag=null;this.pinch=null;});
  }
  distance(){const a=[...this.pointers.values()];return a.length===2?Math.hypot(a[0].x-a[1].x,a[0].y-a[1].y):0;}
  setData(candles,events,interval){if(this.interval!==interval){this.range=null;this.interval=interval;}const oldEnd=this.candles.at(-1)?.time;this.candles=candles;this.events=events;
   if(!this.range&&candles.length){const end=candles.at(-1).time+interval*2;this.range=[Math.max(candles[0].time,end-interval*65),end];}
   else if(this.range&&oldEnd&&this.range[1]>=oldEnd&&candles.at(-1)?.time>oldEnd){const shift=candles.at(-1).time-oldEnd;this.range=this.range.map(x=>x+shift);}this.draw();}
  zoom(factor){if(!this.range)return;const mid=(this.range[0]+this.range[1])/2,span=Math.max(this.interval*5,Math.min(this.interval*1000,(this.range[1]-this.range[0])*factor));this.range=[mid-span/2,mid+span/2];this.draw();}
  inspect(x){if(!this.range)return;const t=this.range[0]+(x-8)/(this.canvas.clientWidth-68)*(this.range[1]-this.range[0]);const c=this.candles.find(c=>t>=c.time&&t<c.time+this.interval);if(c)this.onSelect({category:'CANDLE',occurredAt:new Date(c.time*1000).toISOString(),evidence:c,provenance:{provider:c.provider,coverage:c.coverage,quality:c.quality}});}
  draw(){const w=this.canvas.clientWidth||340,h=320,dpr=root.devicePixelRatio||1;this.canvas.width=w*dpr;this.canvas.height=h*dpr;const c=this.ctx;c.setTransform(dpr,0,0,dpr,0,0);c.clearRect(0,0,w,h);this.hits=[];c.font='10px system-ui';
   if(!this.range||!this.candles.length){c.fillStyle='#a6b2c6';c.fillText('Candles UNAVAILABLE — awaiting real observations',12,140);return;}
   const visible=this.candles.filter(b=>b.time>=this.range[0]-this.interval&&b.time<=this.range[1]);if(!visible.length)return;
   const min=Math.min(...visible.map(b=>b.low)),max=Math.max(...visible.map(b=>b.high)),pad=(max-min)*.12||max*.02||.01,lo=min-pad,hi=max+pad;
   const x=t=>8+(t-this.range[0])/(this.range[1]-this.range[0])*(w-68),y=p=>250-(p-lo)/(hi-lo)*210;
   c.strokeStyle='#263349';c.fillStyle='#a6b2c6';
   for(let i=0;i<5;i++){const price=lo+(hi-lo)*i/4,yp=y(price);c.beginPath();c.moveTo(0,yp);c.lineTo(w-58,yp);c.stroke();c.fillText(price.toPrecision(4),w-56,yp+3);}
   const width=Math.max(2,Math.min(12,(w-68)/(this.range[1]-this.range[0])*this.interval*.65));
   for(const b of visible){const xp=x(b.time);c.strokeStyle=c.fillStyle=b.close>=b.open?'#7ae0b6':'#ff7e99';c.beginPath();c.moveTo(xp,y(b.high));c.lineTo(xp,y(b.low));c.stroke();c.fillRect(xp-width/2,Math.min(y(b.open),y(b.close)),width,Math.max(1,Math.abs(y(b.open)-y(b.close))));}
   const lanes=new Map();
   for(const e of this.events){const style=styles[e.category],t=Date.parse(e.occurredAt)/1000;if(!style||t<this.range[0]||t>this.range[1])continue;
    const xp=x(t),bucket=Math.round(xp/24),lane=lanes.get(bucket)||0;lanes.set(bucket,lane+1);const yp=275-(lane%4)*17;
    c.strokeStyle=style[1];c.setLineDash([2,4]);c.beginPath();c.moveTo(xp,255);c.lineTo(xp,yp-9);c.stroke();c.setLineDash([]);c.fillStyle=style[1];c.beginPath();
    if(style[2]==='circle')c.arc(xp,yp,8,0,Math.PI*2);else if(style[2]==='square')c.rect(xp-8,yp-7,16,14);else if(style[2]==='triangle'){c.moveTo(xp,yp-9);c.lineTo(xp-9,yp+7);c.lineTo(xp+9,yp+7);}else{c.moveTo(xp,yp-9);c.lineTo(xp+9,yp);c.lineTo(xp,yp+9);c.lineTo(xp-9,yp);}c.fill();c.fillStyle='#07121b';c.textAlign='center';c.font='bold 8px system-ui';c.fillText(style[0],xp,yp+3);this.hits.push({x:xp,y:yp,event:e});}
   c.textAlign='left';c.font='10px system-ui';c.fillStyle='#a6b2c6';c.fillText(new Date(this.range[0]*1000).toLocaleString(),0,310);c.textAlign='right';c.fillText(new Date(this.range[1]*1000).toLocaleTimeString(),w-58,310);c.textAlign='left';
  }
 }
 root.SignalCandleChart=SignalCandleChart;root.signalMarkerStyles=styles;
})(window);
