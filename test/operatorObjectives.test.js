'use strict';

const { describe, it, beforeEach } = require('node:test');
const assert = require('node:assert/strict');

const {
  createMemoryStore,
  createOperatorObjective,
  updateOperatorObjective,
  getActiveObjectives,
  getObjectiveById,
  resolveObjectiveReference,
  detectObjectiveEstablishment,
  detectObjectiveLifecycleChange,
  looksLikeObjectiveStatusRequest,
  shouldSuppressMissionForResolvedObjective,
  ensurePublicMaxLaunchObjective,
  RESOLUTION,
  OperatorObjectiveError,
} = require('../services/operatorObjectives');

describe('SPEC-095 operatorObjectives service', () => {
  /** @type {ReturnType<typeof createMemoryStore>} */
  let store;

  beforeEach(() => {
    store = createMemoryStore();
  });

  it('persists an explicit operator strategic objective', async () => {
    const obj = await createOperatorObjective(
      {
        tenantId: '1',
        scope: 'operator',
        title: 'Public Max Launch',
        objectiveText:
          'Build qualified attention around the ideas behind Pulseforge.',
        timeHorizon: 'Roughly three weeks, evidence-gated.',
        currentPhase: 'Thesis / problem exposure',
        aliases: ['the launch', 'max launch campaign'],
      },
      { store }
    );

    assert.ok(obj.id);
    assert.equal(obj.scope, 'operator');
    assert.equal(obj.clientId, null);
    assert.equal(obj.status, 'active');
    assert.equal(obj.title, 'Public Max Launch');

    const again = await getObjectiveById(obj.id, { tenantId: '1', store });
    assert.equal(again.title, 'Public Max Launch');
  });

  it('retrieves active objectives before routing (scope-relevant)', async () => {
    await createOperatorObjective(
      {
        tenantId: '1',
        scope: 'operator',
        title: 'Public Max Launch',
        objectiveText: 'Launch attention.',
      },
      { store }
    );
    await createOperatorObjective(
      {
        tenantId: '1',
        scope: 'client',
        clientId: 10,
        title: 'Commercial Cleaning - Manchester',
        objectiveText: 'Fill daycare pipeline.',
      },
      { store }
    );
    await createOperatorObjective(
      {
        tenantId: '2',
        scope: 'operator',
        title: 'Other Tenant Launch',
        objectiveText: 'Must not leak.',
      },
      { store }
    );

    const forTenant1 = await getActiveObjectives(
      { tenantId: '1', clientId: 10 },
      { store }
    );
    assert.equal(forTenant1.length, 2);
    assert.ok(forTenant1.every((o) => o.tenantId === '1'));
    assert.ok(forTenant1.some((o) => o.title === 'Public Max Launch'));
    assert.ok(
      forTenant1.some((o) => o.title === 'Commercial Cleaning - Manchester')
    );

    const tenant2 = await getActiveObjectives({ tenantId: '2' }, { store });
    assert.equal(tenant2.length, 1);
    assert.equal(tenant2[0].title, 'Other Tenant Launch');
  });

  it('enforces operator vs client scope constraints', async () => {
    // Operator scope drops client_id rather than storing a mixed row
    const op = await createOperatorObjective(
      {
        tenantId: '1',
        scope: 'operator',
        clientId: 10,
        title: 'Operator Only',
        objectiveText: 'x',
      },
      { store }
    );
    assert.equal(op.scope, 'operator');
    assert.equal(op.clientId, null);

    await assert.rejects(
      () =>
        createOperatorObjective(
          {
            tenantId: '1',
            scope: 'client',
            title: 'Bad',
            objectiveText: 'x',
          },
          { store }
        ),
      (err) => err instanceof OperatorObjectiveError && err.code === 'invalid_scope'
    );
  });

  it('resolves unique references deterministically', async () => {
    const launch = await ensurePublicMaxLaunchObjective({ store, tenantId: '1' });
    await createOperatorObjective(
      {
        tenantId: '1',
        scope: 'operator',
        title: 'Boston Expansion',
        objectiveText: 'Expand into Boston.',
        aliases: ['boston', 'the boston expansion'],
      },
      { store }
    );

    const active = await getActiveObjectives({ tenantId: '1' }, { store });
    const r1 = resolveObjectiveReference({
      message: 'Where are we with the launch?',
      objectives: active,
    });
    assert.equal(r1.status, RESOLUTION.RESOLVED);
    assert.equal(r1.objective.id, launch.id);
    assert.equal(r1.objective.title, 'Public Max Launch');

    const r2 = resolveObjectiveReference({
      message: 'Where are we with the Max launch campaign?',
      objectives: active,
    });
    assert.equal(r2.status, RESOLUTION.RESOLVED);
    assert.equal(r2.objective.title, 'Public Max Launch');

    const r3 = resolveObjectiveReference({
      message: "What's next for Boston?",
      objectives: active,
    });
    assert.equal(r3.status, RESOLUTION.RESOLVED);
    assert.equal(r3.objective.title, 'Boston Expansion');
  });

  it('fails closed on ambiguous references', async () => {
    await createOperatorObjective(
      {
        tenantId: '1',
        scope: 'operator',
        title: 'Public Max Launch',
        objectiveText: 'a',
        aliases: ['the launch', 'launch'],
      },
      { store }
    );
    await createOperatorObjective(
      {
        tenantId: '1',
        scope: 'operator',
        title: 'Boston Market Launch',
        objectiveText: 'b',
        aliases: ['the launch', 'market launch'],
      },
      { store }
    );

    const active = await getActiveObjectives({ tenantId: '1' }, { store });
    const r = resolveObjectiveReference({
      message: 'Where are we with the launch?',
      objectives: active,
    });
    assert.equal(r.status, RESOLUTION.AMBIGUOUS);
    assert.equal(r.objective, null);
    assert.ok(r.matches.length >= 2);
  });

  it('fails closed on unresolved references', () => {
    const r = resolveObjectiveReference({
      message: 'Where are we with the lunar colony?',
      objectives: [
        {
          id: '1',
          title: 'Public Max Launch',
          aliases: ['max launch'],
          scope: 'operator',
        },
      ],
    });
    assert.equal(r.status, RESOLUTION.UNRESOLVED);
    assert.equal(r.objective, null);
  });

  it('does not infer objectives from weak language', () => {
    assert.equal(
      detectObjectiveEstablishment('Boston could be interesting someday.'),
      null
    );
    assert.equal(
      detectObjectiveEstablishment('We should probably launch Max eventually.'),
      null
    );
  });

  it('detects explicit establishment language', () => {
    const d = detectObjectiveEstablishment(
      "We're preparing for your public launch over roughly the next three weeks, but the launch should be evidence-gated. The objective is to build qualified attention around the ideas behind Pulseforge. I want you to own the overall objective. Paige should handle content strategy."
    );
    assert.ok(d);
    assert.equal(d.kind, 'establish');
    assert.equal(d.title, 'Public Max Launch');
    assert.equal(d.scope, 'operator');
    assert.match(d.timeHorizon || '', /evidence/i);
  });

  it('lifecycle changes require explicit intent', async () => {
    const obj = await createOperatorObjective(
      {
        tenantId: '1',
        scope: 'operator',
        title: 'Boston Expansion',
        objectiveText: 'Expand.',
        aliases: ['boston'],
      },
      { store }
    );
    const active = await getActiveObjectives({ tenantId: '1' }, { store });
    const life = detectObjectiveLifecycleChange('Put Boston on hold.', active);
    assert.ok(life);
    assert.equal(life.status, 'paused');
    assert.equal(life.objective.id, obj.id);

    const updated = await updateOperatorObjective(
      obj.id,
      { tenantId: '1', status: 'paused' },
      { store }
    );
    assert.equal(updated.status, 'paused');
    const stillActive = await getActiveObjectives({ tenantId: '1' }, { store });
    assert.equal(stillActive.length, 0);
  });

  it('status requests suppress mission routing for resolved objectives', () => {
    const obj = {
      id: 'x',
      title: 'Public Max Launch',
      objectiveText: '…',
    };
    assert.equal(
      looksLikeObjectiveStatusRequest('Where are we with the Max launch campaign?'),
      true
    );
    assert.equal(
      shouldSuppressMissionForResolvedObjective(
        'Where are we with the Max launch campaign?',
        obj
      ),
      true
    );
    assert.equal(
      looksLikeObjectiveStatusRequest(
        'Launch a commercial cleaning campaign targeting law firms in Boston.'
      ),
      false
    );
    assert.equal(
      shouldSuppressMissionForResolvedObjective(
        'Launch a commercial cleaning campaign targeting law firms in Boston.',
        obj
      ),
      false
    );
  });

  it('same objectives + same reference → same resolution (determinism)', async () => {
    await ensurePublicMaxLaunchObjective({ store, tenantId: '1' });
    const active = await getActiveObjectives({ tenantId: '1' }, { store });
    const a = resolveObjectiveReference({
      message: 'Where are we with the launch?',
      objectives: active,
    });
    const b = resolveObjectiveReference({
      message: 'Where are we with the launch?',
      objectives: active,
    });
    assert.equal(a.status, b.status);
    assert.equal(a.objective && a.objective.id, b.objective && b.objective.id);
    assert.equal(a.confidence, b.confidence);
  });
});
