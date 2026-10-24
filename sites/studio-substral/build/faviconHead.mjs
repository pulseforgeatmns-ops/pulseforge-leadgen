/** Cache-bust query for root favicon bundle — bump when icons change. */
export const FAVICON_CACHE_VERSION = '6';

export function renderFaviconHeadLinks() {
  const v = FAVICON_CACHE_VERSION;
  return `<link rel="icon" href="/favicon.ico?v=${v}" sizes="any">
<link rel="icon" href="/favicon.svg?v=${v}" type="image/svg+xml">
<link rel="apple-touch-icon" href="/apple-touch-icon.png?v=${v}">
<link rel="manifest" href="/site.webmanifest?v=${v}">`;
}

/**
 * Replace any icon / apple-touch / manifest head links with the canonical
 * root-relative, versioned bundle (strips legacy assets/brand references).
 */
export function normalizeIndexFaviconHead(html) {
  const block = renderFaviconHeadLinks();
  const anchor =
    /(<link rel="stylesheet" href="assets\/css\/substral\.css">\n)[\s\S]*?(\n<meta name="theme-color")/;
  if (!anchor.test(html)) {
    throw new Error('index.html favicon head anchor not found');
  }
  return html.replace(anchor, `$1${block}$2`);
}
