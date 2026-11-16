/*
 * S2 tablet diagnostic bookmarklet: proves that a javascript: bookmark runs on
 * the current page at all. ES5 only, so it parses on any Chrome.
 *
 * It makes NO network request (no fetch, XHR, image, beacon, form or
 * navigation), reads no cookie or storage, sends nothing, and contains no
 * REST intent. Its only effect is a local overlay on this tab; tap the overlay
 * to remove it. Build: node scripts/s2-rest-acceptance/buildBookmarklet.js
 */
void (function () {
  var id = 's2-diagnostic-panel';
  var old = document.getElementById(id);
  if (old && old.parentNode) old.parentNode.removeChild(old);
  var d = document.createElement('div');
  d.id = id;
  d.style.cssText = 'position:fixed;top:0;right:0;left:0;z-index:2147483647;margin:0;padding:16px;background:#0a5;color:#fff;font:15px/1.45 monospace;white-space:pre-wrap;word-break:break-all;box-shadow:0 2px 12px #000';
  var m = navigator.userAgent.match(/Chrome\/(\d+)/);
  d.textContent = [
    'S2 DIAGNOSTIC: bookmarklet RAN (build S2_BUILD_ID)',
    'No network request was made. Nothing was changed on the server.',
    'kind: S2_KIND  length: S2_LENGTH chars',
    'time: ' + new Date().toISOString(),
    'page: ' + location.origin + location.pathname,
    'readyState: ' + document.readyState,
    'chrome: ' + (m ? m[1] : 'unknown'),
    'ua: ' + navigator.userAgent,
    '',
    'Tap this box to close it.'
  ].join('\n');
  d.onclick = function () { if (d.parentNode) d.parentNode.removeChild(d); };
  (document.body || document.documentElement).appendChild(d);
})();
