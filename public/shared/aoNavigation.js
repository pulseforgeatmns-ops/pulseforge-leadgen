(function (root) {
  const NAV_APPS = Object.freeze({
    google_maps: 'Google Maps',
    waze: 'Waze',
    apple_maps: 'Apple Maps',
    ask_every_time: 'Ask every time',
  });

  function isUsableAddress(address) {
    const text = String(address || '').trim();
    if (!text || text.length < 5) return false;
    if (/^(tbd|n\/a|none|unknown|address needed|—|-)$/i.test(text)) return false;
    return true;
  }

  function buildNavigateUrl(address, app) {
    const text = String(address || '').trim();
    if (!isUsableAddress(text)) return null;
    const q = encodeURIComponent(text);
    if (app === 'waze') return `https://waze.com/ul?q=${q}&navigate=yes`;
    if (app === 'apple_maps') return `http://maps.apple.com/?daddr=${q}`;
    return `https://www.google.com/maps/dir/?api=1&destination=${q}`;
  }

  function buildNavigateUrls(address) {
    if (!isUsableAddress(address)) return null;
    return {
      google_maps: buildNavigateUrl(address, 'google_maps'),
      waze: buildNavigateUrl(address, 'waze'),
      apple_maps: buildNavigateUrl(address, 'apple_maps'),
    };
  }

  function resolveNavigateUrl(address, urls, preference) {
    const navUrls = urls || buildNavigateUrls(address);
    if (!navUrls) return null;
    const pref = preference || 'google_maps';
    if (pref === 'ask_every_time') return null;
    return navUrls[pref] || navUrls.google_maps;
  }

  const AoNavigation = {
    NAV_APPS,
    isUsableAddress,
    buildNavigateUrl,
    buildNavigateUrls,
    resolveNavigateUrl,
  };

  if (typeof module !== 'undefined' && module.exports) {
    module.exports = AoNavigation;
    module.exports.buildNavigateUrl = buildNavigateUrl;
    module.exports.buildNavigateUrls = buildNavigateUrls;
    module.exports.resolveNavigateUrl = resolveNavigateUrl;
    module.exports.isUsableAddress = isUsableAddress;
  } else {
    root.AoNavigation = AoNavigation;
  }
}(typeof globalThis !== 'undefined' ? globalThis : this));
