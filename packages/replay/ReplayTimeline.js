'use strict';

/**
 * ReplayTimeline — ordered immutable observations (SPEC-018).
 *
 * Cursor navigation only. Observations are never mutated.
 * Seek by timestamp or deterministic observation id — never by positional identity.
 */
class ReplayTimeline {
  /**
   * @param {import('./types').ImmutableObservation[]} observations - already sorted ascending
   */
  constructor(observations) {
    if (!Array.isArray(observations)) {
      throw new Error('ReplayTimeline requires an observations array');
    }
    this._observations = Object.freeze(
      observations.map((obs) => freezeObservation(obs))
    );
    /** @type {number} cursor index; -1 = before first */
    this._cursor = -1;
  }

  /** @returns {number} */
  get length() {
    return this._observations.length;
  }

  /** @returns {boolean} */
  get exhausted() {
    return this._cursor >= this._observations.length - 1;
  }

  /** @returns {boolean} */
  get atStart() {
    return this._cursor <= 0;
  }

  /**
   * @returns {import('./types').ImmutableObservation|null}
   */
  current() {
    if (this._cursor < 0 || this._cursor >= this._observations.length) {
      return null;
    }
    return this._observations[this._cursor];
  }

  /**
   * Advance to the next observation.
   * @returns {import('./types').ImmutableObservation|null}
   */
  next() {
    if (this._cursor >= this._observations.length - 1) {
      return null;
    }
    this._cursor += 1;
    return this.current();
  }

  /**
   * Move to the previous observation.
   * @returns {import('./types').ImmutableObservation|null}
   */
  previous() {
    if (this._cursor <= 0) {
      this._cursor = -1;
      return null;
    }
    this._cursor -= 1;
    return this.current();
  }

  /**
   * Seek by ISO timestamp (inclusive — latest observation at or before T)
   * or by deterministic observation id.
   *
   * @param {string} target - timestamp or observationId
   * @returns {import('./types').ImmutableObservation|null}
   */
  seek(target) {
    if (target == null || target === '') {
      throw new Error('ReplayTimeline.seek requires a timestamp or observationId');
    }
    const key = String(target);

    const byId = this._observations.findIndex((obs) => obs.id === key);
    if (byId >= 0) {
      this._cursor = byId;
      return this.current();
    }

    const ts = Date.parse(key);
    if (Number.isNaN(ts)) {
      throw new Error(
        `ReplayTimeline.seek: unknown observationId and invalid timestamp: ${key}`
      );
    }

    let found = -1;
    for (let i = 0; i < this._observations.length; i++) {
      const obsTs = Date.parse(this._observations[i].observedAt);
      if (!Number.isNaN(obsTs) && obsTs <= ts) {
        found = i;
      } else if (!Number.isNaN(obsTs) && obsTs > ts) {
        break;
      }
    }
    this._cursor = found;
    return this.current();
  }

  /** Reset cursor before the first observation. */
  reset() {
    this._cursor = -1;
  }

  /**
   * Frozen copy of all observations (never the live mutable cursor state).
   * @returns {import('./types').ImmutableObservation[]}
   */
  toArray() {
    return this._observations.slice();
  }

  /**
   * Observations at or before timestamp (inclusive).
   * @param {string} timestamp
   * @returns {import('./types').ImmutableObservation[]}
   */
  upTo(timestamp) {
    const ts = Date.parse(String(timestamp));
    if (Number.isNaN(ts)) {
      throw new Error(`ReplayTimeline.upTo requires a valid timestamp: ${timestamp}`);
    }
    return this._observations.filter((obs) => {
      const obsTs = Date.parse(obs.observedAt);
      return !Number.isNaN(obsTs) && obsTs <= ts;
    });
  }
}

/**
 * @param {object} obs
 * @returns {import('./types').ImmutableObservation}
 */
function freezeObservation(obs) {
  if (!obs || typeof obs !== 'object') {
    throw new Error('ReplayTimeline observations must be objects');
  }
  if (!obs.id) {
    throw new Error('ReplayTimeline observations require deterministic id');
  }
  if (!obs.observedAt) {
    throw new Error(`Observation ${obs.id} requires observedAt`);
  }
  const frozen = {
    ...obs,
    id: String(obs.id),
    subjectId: String(obs.subjectId || ''),
    observationType: String(obs.observationType || obs.type || ''),
    observedAt: String(obs.observedAt),
    payload:
      obs.payload && typeof obs.payload === 'object'
        ? Object.freeze({ ...obs.payload })
        : Object.freeze({}),
  };
  return Object.freeze(frozen);
}

/**
 * @param {import('./types').ImmutableObservation[]} observations
 * @returns {ReplayTimeline}
 */
function createReplayTimeline(observations) {
  return new ReplayTimeline(observations);
}

module.exports = {
  ReplayTimeline,
  createReplayTimeline,
  freezeObservation,
};
