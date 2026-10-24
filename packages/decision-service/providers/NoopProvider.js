'use strict';

/** @implements {import('../types').DecisionProvider} */
class NoopProvider {
  constructor(reason = 'disabled') {
    this.name = 'noop';
    this.model = null;
    this.reason = reason;
  }
  async evaluate() {
    return { decision: null, model: null, raw_redacted_response: null, fallback_reason: this.reason };
  }
}
module.exports = { NoopProvider };
