'use strict';

/**
 * Registry of platform publish adapters keyed by artifact.platform.
 */

class PlatformAdapterRegistry {
  constructor() {
    /** @type {Map<string, object>} */
    this._adapters = new Map();
  }

  /**
   * @param {object} adapter
   */
  register(adapter) {
    const platform = String(adapter.platform || '').trim();
    if (!platform) throw new Error('adapter_platform_required');
    if (this._adapters.has(platform)) {
      throw new Error(`adapter_already_registered:${platform}`);
    }
    this._adapters.set(platform, adapter);
    return adapter;
  }

  /**
   * @param {string} platform
   * @returns {object}
   */
  resolve(platform) {
    const key = String(platform || '').trim();
    const adapter = this._adapters.get(key);
    if (!adapter) throw new Error(`unsupported_platform:${key}`);
    return adapter;
  }

  /**
   * @returns {string[]}
   */
  listPlatforms() {
    return [...this._adapters.keys()].sort();
  }
}

module.exports = {
  PlatformAdapterRegistry,
};
