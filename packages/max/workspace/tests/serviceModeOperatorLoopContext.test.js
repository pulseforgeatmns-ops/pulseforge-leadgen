'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  getServiceModeOperatorLoop,
} = require('../ServiceModeOperatorLoopContext');

describe('SPEC-075 ServiceModeOperatorLoopContext (Max)', () => {
  it('exposes read-only inspection wrapper without execution', async () => {
    const loopService = {
      async getServiceModeOperatorLoop(options) {
        assert.equal(options.days, 14);
        assert.equal(options.limit, 5);
        return {
          ok: true,
          kind: 'service_mode_operator_loop',
          isEvidence: false,
          actions: [],
          caveats: ['no_operator_candidates'],
          autonomousExecution: false,
        };
      },
    };

    const result = await getServiceModeOperatorLoop({
      days: 14,
      limit: 5,
      loopService,
    });

    assert.equal(result.ok, true);
    assert.equal(result.kind, 'service_mode_operator_loop');
    assert.equal(result.isEvidence, false);
    assert.equal(result.inspectionOnly, true);
    assert.equal(result.autonomousExecution, false);
    assert.equal(result.source, 'SPEC-075');
  });
});
