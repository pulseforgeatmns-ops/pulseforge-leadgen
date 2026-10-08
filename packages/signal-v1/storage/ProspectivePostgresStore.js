'use strict';

const { InMemorySignalStore } = require('./InMemorySignalStore');
const { PostgresSignalStore } = require('./PostgresSignalStore');
const { ensureSignalSchema } = require('./ensureSignalSchema');
const camel = name => name.replace(/_([a-z0-9])/g, (_, c) => c.toUpperCase());
const mapRow = row => Object.fromEntries(Object.entries(row).map(([key,value])=>[camel(key),value]));

// Single-writer prospective adapter: persist first, then update the synchronous
// projection required by the existing temporal trigger engine. Never seed fixtures.
class ProspectivePostgresStore extends InMemorySignalStore {
  constructor(pool) { super(); this.pool = pool; this.db = new PostgresSignalStore(pool); }
  static async create(pool) {
    await ensureSignalSchema(pool);
    const store = new ProspectivePostgresStore(pool);
    await store.restore();
    return store;
  }
  async restore() {
    const read = async sql => (await this.pool.query(sql)).rows.map(mapRow);
    this.rawCallerEvidence = await read('SELECT * FROM signal_raw_caller_evidence');
    this.events = await read("SELECT * FROM signal_events WHERE provenance->>'collectorId'='operator-json-feed'");
    this.researchObservations = await read("SELECT * FROM signal_research_observations WHERE metadata->>'mode'='PROSPECTIVE'");
    this.prospectiveJobs = await read('SELECT * FROM signal_prospective_research_jobs');
    for (const row of await read('SELECT * FROM signal_source_registry')) {
      this.sourceRegistry.set(row.sourceId,row);
      if (row.clusterId) this.clusterMembers.set(row.sourceId,row.clusterId);
    }
    for (const row of await this.db.listResearchCohorts()) {
      if (row.metadata?.mode === 'PROSPECTIVE') {
        this.researchCohorts.set(row.id,row);
        this.researchCohortMembers.push(...await this.db.getCohortMembers(row.id));
      }
    }
    for (const row of this.researchObservations) {
      this.researchObservationOutcomes.push(...await this.db.getResearchObservationOutcomes(row.id));
    }
    for (const token of new Set([...this.events,...this.researchObservations].map(row=>row.tokenAddress))) {
      this.tokens.set(token,await this.db.getToken(token));
      this.marketObservations.push(...await this.db.getMarketObservationsForToken(token));
    }
    for (const row of await read('SELECT * FROM signal_token_research_episodes')) {
      this.tokenResearchEpisodes.set(`${row.tokenAddress}|${row.episodeKind}`,row);
    }
    this.alerts = (await read('SELECT payload FROM signal_prospective_internal_alerts')).map(r=>r.payload);
  }
  async writeRow(table, row, columns, conflict) {
    const values = columns.map(column => {
      const value = row[camel(column)] ?? null;
      return value && typeof value === 'object' && !(value instanceof Date) ? JSON.stringify(value) : value;
    });
    return this.pool.query(`INSERT INTO ${table} (${columns.join(',')}) VALUES (${values.map((_,i)=>`$${i+1}`).join(',')}) ${conflict}`,values);
  }
  async upsertToken(row) { return super.upsertToken(await this.db.upsertToken(row)); }
  async upsertSource(row) { await this.db.upsertSource(row); return super.upsertSource(row); }
  async upsertCluster(row) { await this.db.upsertCluster(row); return super.upsertCluster(row); }
  async addClusterMember(sourceId,clusterId) { await this.db.addClusterMember(sourceId,clusterId); return super.addClusterMember(sourceId,clusterId); }
  async insertEvent(row) {
    const saved = await this.db.insertEvent(row);
    return this.events.find(e=>e.id===saved.id) || super.insertEvent(saved);
  }
  async insertMarketObservation(row) { await this.db.insertMarketObservation(row); return super.insertMarketObservation(row); }
  async insertResearchObservation(row) { return super.insertResearchObservation(await this.db.insertResearchObservation(row)); }
  async upsertResearchCohort(row) {
    // Cohort start and definitions are immutable across restarts/retries.
    await this.writeRow('signal_research_cohorts',row,
      ['id','name','definition_version','metadata'], 'ON CONFLICT (id) DO NOTHING');
    const saved = (await this.db.listResearchCohorts()).find(c=>c.id===row.id);
    return super.upsertResearchCohort(saved);
  }
  async addCohortMember(row) { await this.db.addCohortMember(row); return super.addCohortMember(row); }
  async insertRawCallerEvidence(row) {
    const result = await this.writeRow('signal_raw_caller_evidence',row,
      ['id','provider','collector_id','source_id','external_message_id','occurred_at','ingested_at','raw_reference','raw_text','extracted_ca','parser_version','token_address','provenance'],
      'ON CONFLICT DO NOTHING RETURNING id');
    const saved = super.insertRawCallerEvidence(row);
    return {row:saved.row,duplicate:result.rowCount===0};
  }
  async upsertSourceRegistryEntry(row) {
    await this.writeRow('signal_source_registry',row,
      ['source_id','display_name','platform','external_ref','collector_id','source_role','cluster_id','cluster_relationship_status','provenance','active'],
      `ON CONFLICT (source_id) DO UPDATE SET active=EXCLUDED.active,external_ref=EXCLUDED.external_ref,
       provenance=EXCLUDED.provenance,cluster_relationship_status=EXCLUDED.cluster_relationship_status,updated_at=NOW()`);
    return super.upsertSourceRegistryEntry(row);
  }
  async insertProspectiveJob(row) {
    await this.writeRow('signal_prospective_research_jobs',{...row,attempts:row.attempts||0},
      ['id','token_address','observation_id','job_type','status','run_after','target_delay_seconds','payload','attempts'],
      'ON CONFLICT (id) DO NOTHING');
    return super.insertProspectiveJob(row);
  }
  async updateProspectiveJob(id,patch) {
    await this.pool.query(`UPDATE signal_prospective_research_jobs SET status=$2,attempts=$3,
      last_error=$4,completed_at=$5,updated_at=NOW() WHERE id=$1`,
    [id,patch.status,patch.attempts||0,patch.lastError||null,patch.completedAt||null]);
    return super.updateProspectiveJob(id,patch);
  }
  async insertResearchObservationOutcome(row) {
    await this.db.insertResearchObservationOutcome(row);
    return super.insertResearchObservationOutcome(row);
  }
  async updateResearchObservationOutcome(row) {
    await this.pool.query(`UPDATE signal_research_observation_outcomes SET label=$2,mfe=$3,mae=$4,
      time_to_2x_seconds=$5,time_to_minus_30_seconds=$6,return_15m=$7,return_1h=$8,return_6h=$9,
      return_24h=$10,metadata=$11::jsonb WHERE id=$1`,
    [row.id,row.label,row.mfe,row.mae,row.timeTo2xSeconds,row.timeToMinus30Seconds,
      row.return15m,row.return1h,row.return6h,row.return24h,JSON.stringify(row.metadata||{})]);
  }
  async persistEpisode(row) {
    await this.writeRow('signal_token_research_episodes',row,
      ['token_address','episode_kind','knowledge_at','observation_id'], 'ON CONFLICT DO NOTHING');
  }
  async insertAlert(row) {
    await this.pool.query(`INSERT INTO signal_prospective_internal_alerts(id,observation_id,payload)
      VALUES ($1,$2,$3::jsonb) ON CONFLICT DO NOTHING`,[row.id,row.metadata.observationId,JSON.stringify(row)]);
    const existing=this.alerts.find(a=>a.id===row.id); if(existing)return existing;
    this.alerts.push(row); return row;
  }
}
module.exports = { ProspectivePostgresStore };
