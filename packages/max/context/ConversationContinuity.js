'use strict';
const {ConversationContextStore, project} = require('./ConversationContextStore');
class ConversationContinuity {
  constructor({env=process.env,store,audit=()=>{}}={}) {
    this.enabled=env.MAX_CONVERSATION_CONTEXT==='true';
    this.env=env;this.store=store;this.audit=audit;this.pending=new Set();this.queues=new Map();
  }
  repository() {
    if (!this.store) {
      const {createShadowPool}=require('../../decision-service/ShadowEventSink');
      this.pool=createShadowPool(this.env);
      this.pool.on('error',()=>{});
      this.store=new ConversationContextStore({db:this.pool});
    }
    return this.store;
  }
  // This is a continuity hint only. Consumers must tenant-check and reread all
  // evidence and mission authority. Never install this object as session.context.
  async resume(tenantId,sessionId) {
    if (!this.enabled) return null;
    try { return await this.repository().load(tenantId,sessionId); }
    catch (_) {this.emit('recovery_unavailable');return null;}
  }
  remember(session) {
    if (!this.enabled) return;
    if (this.pending.size>=16) {this.emit('capacity_limit');return;}
    let tenantId,sessionId,context;
    try {tenantId=String(session.context.tenantId);sessionId=session.id;context=project(session,tenantId);}
    catch (_) {this.emit('invalid_context');return;}
    const key=JSON.stringify([tenantId,sessionId]);
    const previous=this.queues.get(key);
    const task=new Promise(resolve=>setImmediate(resolve)).then(()=>previous).then(async()=>{
      const store=this.repository();
      const prior=await store.load(tenantId,sessionId);
      const saved=await store.save(tenantId,sessionId,context,prior?.revision||null);
      this.emit(saved.saved?'saved':'revision_conflict');
    }).catch(()=>this.emit('persistence_unavailable'));
    this.queues.set(key,task);
    this.pending.add(task);task.finally(()=>{
      this.pending.delete(task);
      if(this.queues.get(key)===task)this.queues.delete(key);
    });
  }
  emit(reason){try{this.audit({event:'MAX_CONVERSATION_CONTEXT',reason});}catch(_){}}
  async drain(){await Promise.all([...this.pending]);}
  async close(){await this.drain();if(this.pool)await this.pool.end();}
}
module.exports={ConversationContinuity};
