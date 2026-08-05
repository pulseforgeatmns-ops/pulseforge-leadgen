'use strict';

const { describe, it } = require('node:test');
const assert = require('node:assert/strict');
const {
  getProspectOperatingBrief,
} = require('../ProspectOperatingBriefContext');

describe('SPEC-074 ProspectOperatingBriefContext (Max)', () => {
  it('exposes read-only inspection wrapper without execution', async () => {
    const briefService = {
      async getProspectOperatingBrief(options) {
        assert.equal(options.companyId, 'co-1');
        return {
          ok: true,
          kind: 'prospect_operating_brief',
          isEvidence: false,
          sections: {
            suggestedNextAction: {
              actionType: 'manual_review',
              rationale: 'test',
              cautions: [],
            },
          },
          caveats: [],
          autonomousExecution: false,
        };
      },
    };

    const result = await getProspectOperatingBrief({
      companyId: 'co-1',
      briefService,
    });

    assert.equal(result.ok, true);
    assert.equal(result.kind, 'prospect_operating_brief');
    assert.equal(result.isEvidence, false);
    assert.equal(result.inspectionOnly, true);
    assert.equal(result.autonomousExecution, false);
    assert.equal(result.source, 'SPEC-074');
  });
});
