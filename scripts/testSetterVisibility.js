'use strict';

const assert = require('assert/strict');
const {
  computeSetterVisible,
  getSetterThreshold,
} = require('../utils/setterVisibility');

const qualified = {
  client_id: 1,
  status: 'cold',
  do_not_contact: false,
  icp_score: 70,
  service_area_match: 'Manchester NH',
  setter_visible: false,
};

assert.equal(getSetterThreshold(1), 70, 'client 1 threshold');
assert.equal(getSetterThreshold(10), 70, 'client 10 threshold');

assert.equal(computeSetterVisible(qualified, { reason: 'scout' }), true, 'Scout promotes qualified row');
assert.equal(computeSetterVisible({ ...qualified, icp_score: 69 }, { reason: 'scout' }), false, 'Scout rejects low score');
assert.equal(computeSetterVisible({ ...qualified, service_area_match: ' ' }, { reason: 'handoff' }), false, 'Handoff rejects blank area');
assert.equal(computeSetterVisible(qualified, { reason: 'handoff' }), true, 'Handoff promotes qualified row');

const softFailure = { ...qualified, icp_score: 10, service_area_match: null };
assert.equal(computeSetterVisible(softFailure, { reason: 'manual' }), true, 'Manual bypasses soft gate');
assert.equal(computeSetterVisible(softFailure, { reason: 'engagement' }), true, 'Engagement bypasses soft gate');

for (const reason of ['scout', 'handoff', 'manual', 'engagement', 'stage_change']) {
  assert.equal(
    computeSetterVisible({ ...qualified, status: 'dead', setter_visible: true }, { reason }),
    false,
    `${reason} cannot bypass terminal status`
  );
  assert.equal(
    computeSetterVisible({ ...qualified, do_not_contact: true, setter_visible: true }, { reason }),
    false,
    `${reason} cannot bypass DNC`
  );
}

assert.equal(
  computeSetterVisible({ ...qualified, setter_visible: true }, { reason: 'stage_change', stageStatus: 'dead' }),
  false,
  'Dead stage hides row'
);
assert.equal(
  computeSetterVisible({ ...softFailure, setter_visible: true }, { reason: 'stage_change', stageStatus: 'contacted' }),
  true,
  'Non-dead stage retains an existing override'
);
assert.equal(
  computeSetterVisible({ ...softFailure, setter_visible: false }, { reason: 'stage_change', stageStatus: 'contacted' }),
  false,
  'Stage change does not create an override'
);

console.log('setterVisibility simulations passed');
