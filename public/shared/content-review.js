'use strict';
(function () {
  let model = null, selectedId = null, generation = 0, previousFocus = null, previousOverflow = '', preview = null, busy = false;
  const channelNames = { facebook: 'Facebook comment', facebook_page: 'Facebook', linkedin: 'LinkedIn comment', linkedin_page: 'LinkedIn Page', linkedin_personal: 'LinkedIn', google_business: 'Google Business', google_review: 'Review response', blog: 'Blog' };
  const node = (tag, text, cls) => { const el = document.createElement(tag); if (text != null) el.textContent = String(text); if (cls) el.className = cls; return el; };
  const dialog = document.createElement('section');
  dialog.id = 'contentReview'; dialog.className = 'cr-overlay'; dialog.hidden = true;
  dialog.setAttribute('role', 'dialog'); dialog.setAttribute('aria-modal', 'true'); dialog.setAttribute('aria-labelledby', 'crTitle');
  dialog.innerHTML = '<div class="cr-backdrop" data-cr-close></div><div class="cr-panel"><header class="cr-header"><div><p class="cr-eyebrow">YOUR WORKSPACE · CONTENT</p><h2 id="crTitle">Content review</h2><p id="crSubtitle"></p></div><button type="button" class="cr-button" data-cr-close aria-label="Close content review">Close</button></header><p id="crNotice" class="cr-notice" role="status" aria-live="polite"></p><div class="cr-layout"><aside class="cr-sidebar"><label for="crChannel">Channel</label><select id="crChannel"><option value="all">All channels</option></select><div id="crList" class="cr-list" aria-label="Drafts awaiting review"></div></aside><article id="crDetail" class="cr-detail" aria-label="Selected draft preview"></article></div></div>';
  document.body.append(dialog);
  const $ = id => document.getElementById(id);
  const activeClient = () => window.PulseforgeTenantCoordinator?.getActiveClientId?.() ?? window.PulseforgeShell?.activeClientId;
  function notice(text, error = false) { $('crNotice').textContent = text; $('crNotice').classList.toggle('cr-error', error); }
  function current(token) { return !dialog.hidden && token === generation && (!model || activeClient() == null || Number(activeClient()) === Number(model.clientId)); }
  async function request(path, options = {}) {
    if (window.PulseforgeApi?.request) return window.PulseforgeApi.request(path, options);
    const response = await fetch(path, { credentials: 'same-origin', headers: { 'Content-Type': 'application/json' }, ...options, ...(options.body ? { body: JSON.stringify(options.body) } : {}) });
    const data = await response.json();
    if (!response.ok) throw Object.assign(new Error(data.message || data.error || 'Unable to load content review.'), { status: response.status });
    return data;
  }
  function button(parent, text, action, primary = false) {
    const b = node('button', text, `cr-button${primary ? ' cr-primary' : ''}`); b.type = 'button'; b.addEventListener('click', action); parent.append(b); return b;
  }
  function attachment(media, thumb = false) {
    const url = media?.url;
    if (!url || !(/^https?:\/\//i.test(url) || /^\/(?!\/)/.test(url))) return null;
    if (media.type === 'video' && thumb) return node('span', '▶', 'cr-text-thumb');
    const el = node(media.type === 'video' && !thumb ? 'video' : 'img', null, thumb ? 'cr-thumb' : 'cr-media');
    el.src = url;
    if (el.tagName === 'VIDEO') { el.controls = true; el.preload = 'metadata'; }
    else { el.alt = media.alt || 'Draft attachment'; el.loading = 'lazy'; }
    el.addEventListener('error', () => { const fallback = node('p', 'Attachment preview unavailable.', 'cr-muted'); el.replaceWith(fallback); });
    return el;
  }
  function renderList() {
    const list = $('crList'); list.replaceChildren();
    const filter = $('crChannel').value;
    const items = (model?.items || []).filter(item => filter === 'all' || item.channel === filter);
    for (const item of items) {
      const b = node('button', null, 'cr-draft'); b.type = 'button'; b.dataset.contentId = item.id;
      b.setAttribute('aria-pressed', String(item.id === selectedId));
      const thumb = attachment(item.media?.[0], true);
      if (thumb) b.append(thumb); else b.append(node('span', 'Aa', 'cr-text-thumb'));
      const copy = node('span', null, 'cr-draft-copy'); copy.append(node('strong', channelNames[item.channel] || item.channel || 'Draft'), node('span', item.body || item.title || 'Untitled draft'));
      b.append(copy); b.addEventListener('click', () => select(item.id)); list.append(b);
    }
    if (!items.length) list.append(node('p', 'No drafts in this channel.', 'cr-muted'));
  }
  async function select(id) {
    if (busy) return;
    selectedId = id; preview = null; renderList();
    const item = model?.items.find(entry => entry.id === id); if (!item) return;
    const token = ++generation;
    const detail = $('crDetail'); detail.replaceChildren();
    const head = node('div', null, 'cr-preview-head');
    head.append(node('span', channelNames[item.channel] || item.channel || 'Draft', 'cr-channel'), node('span', item.canReview ? 'Awaiting review' : 'Needs verification', 'cr-status'));
    detail.append(head, node('h3', item.title || 'Content draft'));
    const media = node('div', null, 'cr-media-group');
    for (const ref of item.media || []) { const el = attachment(ref); if (el) media.append(el); }
    if (!media.children.length) media.append(node('div', 'Text post · No image attached', 'cr-text-preview'));
    const body = node('p', item.body, 'cr-post-body');
    detail.append(media, body);
    if (item.originalPost && ['linkedin', 'facebook', 'google_review'].includes(item.channel)) {
      const context = node('details', null, 'cr-original'); context.append(node('summary', 'Original post'), node('p', item.originalPost, 'cr-post-body'));
      if (item.postUrl) { const link = node('a', 'Open original post'); link.href = item.postUrl; link.target = '_blank'; link.rel = 'noopener noreferrer'; context.append(link); }
      detail.append(context);
    }
    if (item.objective) detail.append(node('p', item.objective, 'cr-muted'));
    const destination = node('p', item.source === 'canonical' ? 'Checking the exact version and destination…' : channelNames[item.channel] || item.channel, 'cr-destination');
    const blocker = node('p', item.blocker || '', 'cr-blocker');
    const actions = node('div', null, 'cr-actions');
    const publishesOnApprove = item.decisionMode === 'legacy' && ['linkedin', 'facebook', 'blog'].includes(item.channel);
    const approve = button(actions, publishesOnApprove ? 'Approve & post' : 'Approve draft', () => decide(item, 'approve'), true);
    const reject = button(actions, 'Reject draft', () => decide(item, 'reject'));
    approve.disabled = !item.canReview || item.source === 'canonical'; reject.disabled = !item.canReview;
    detail.append(destination, blocker, actions, node('p', item.source === 'canonical' ? 'Approving saves your decision. Publishing is a separate action.' : publishesOnApprove ? 'Approve & post publishes this content. Reject removes it from the review queue.' : item.canReview ? 'Approving saves your decision. Reject removes it from the review queue.' : 'The draft is visible for review. Approval stays unavailable until its version and destination can be verified.', 'cr-footnote'));
    if (item.source !== 'canonical' || !item.canReview) return;
    try {
      const exact = await request(`/api/paige/social/${encodeURIComponent(item.artifactId)}/preview?client_id=${model.clientId}`);
      if (!current(token) || selectedId !== id) return;
      // Display the same server snapshot that the approval hash binds to.
      if (!exact.artifact || typeof exact.artifact.body !== 'string' || !exact.account?.id) throw new Error('exact_preview_unavailable');
      body.textContent = exact.artifact.body;
      let refs = exact.artifact.mediaRefs || exact.artifact.meta?.mediaRefs || exact.artifact.meta?.media_refs || exact.artifact.meta?.images || [];
      if (!Array.isArray(refs)) refs = [refs];
      refs = [...refs, exact.artifact.image_url, exact.artifact.meta?.imageUrl, exact.artifact.meta?.image_url].filter(Boolean);
      media.replaceChildren();
      for (const ref of refs) {
        const url = typeof ref === 'string' ? ref : ref.url || ref.src || ref.image_url || ref.previewUrl;
        const el = attachment({ url, alt: ref.alt || ref.caption, type: ref.type === 'video' || /\.(mp4|webm)(?:[?#]|$)/i.test(url) ? 'video' : 'image' });
        if (el) media.append(el);
      }
      if (!media.children.length) media.append(node('div', 'Text post · No image attached', 'cr-text-preview'));
      preview = exact;
      destination.textContent = `Destination · ${channelNames[exact.account?.platform] || exact.account?.platform || ''} · ${exact.account?.displayName || exact.account?.label || exact.account?.externalAccountId || exact.account?.id || ''}`;
      approve.disabled = !exact.approvalHash;
    } catch (error) {
      if (!current(token)) return;
      destination.textContent = 'Destination could not be verified.';
      blocker.textContent = /copy_doctrine|unsupported_social_media/i.test(error.message) ? 'This version needs revision before it can be approved.' : 'The publishing connection needs attention before this draft can be approved.';
    }
  }
  async function decide(item, decision) {
    if (busy || !model || Number(activeClient()) !== Number(model.clientId) || (decision === 'approve' && item.source === 'canonical' && !preview?.approvalHash)) return;
    const token = generation;
    const exact = preview;
    busy = true; $('crDetail').querySelectorAll('button').forEach(b => { b.disabled = true; });
    try {
      if (item.source === 'canonical') await request(`/api/paige/social/${encodeURIComponent(item.artifactId)}/decision?client_id=${model.clientId}`, { method: 'POST', body: { decision, accountId: exact?.account?.id, expectedApprovalHash: exact?.approvalHash } });
      else await request(`/api/approvals/${encodeURIComponent(item.pendingCommentId)}`, { method: 'POST', body: { action: decision === 'approve' ? 'approved' : 'rejected' } });
      if (!current(token)) return;
      busy = false;
      await load(); notice(decision === 'approve' ? 'Approval recorded.' : 'Draft rejected.');
      document.dispatchEvent(new CustomEvent('pulseforge:content-reviewed'));
    } catch (error) { if (current(token)) { busy = false; await select(item.id); notice(/preview_changed/i.test(error.message) ? 'This draft changed. Review the latest version before approving.' : 'The decision could not be saved. Refresh and try again.', true); } }
    finally { busy = false; }
  }
  async function load() {
    const token = ++generation; preview = null; model = null;
    $('crList').replaceChildren(node('p', 'Loading drafts…', 'cr-muted'));
    $('crDetail').replaceChildren(node('p', 'Preparing your visual review…', 'cr-muted'));
    notice('');
    try {
      const next = await request('/api/content-review');
      if (dialog.hidden || token !== generation || (activeClient() != null && Number(next.clientId) !== Number(activeClient()))) return;
      model = next;
      $('crSubtitle').textContent = `${window.PulseforgeShell?.tenantName || 'Current workspace'} · ${next.pendingCount} awaiting review`;
      const selectedChannel = $('crChannel').value;
      $('crChannel').replaceChildren(new Option('All channels', 'all'));
      [...new Set(next.items.map(item => item.channel))].forEach(channel => $('crChannel').append(new Option(channelNames[channel] || channel, channel)));
      if ([...$('crChannel').options].some(option => option.value === selectedChannel)) $('crChannel').value = selectedChannel;
      renderList(); notice((next.warnings || []).join(' '));
      const visible = next.items.filter(item => $('crChannel').value === 'all' || item.channel === $('crChannel').value);
      if (visible.length) await select(visible.some(item => item.id === selectedId) ? selectedId : visible[0].id);
      else if (next.items.length) $('crDetail').replaceChildren(node('p', 'No drafts in this channel.', 'cr-muted'));
      else { $('crDetail').replaceChildren(node('h3', 'You’re all caught up.'), node('p', 'There’s no content waiting for review in this workspace.', 'cr-muted')); }
    } catch (error) {
      if (dialog.hidden || token !== generation) return;
      $('crList').replaceChildren(); $('crDetail').replaceChildren(node('h3', 'Content review is unavailable.'));
      notice(error.status === 403 ? 'Content approvals are available to workspace operators.' : 'Your drafts could not be loaded. Please try again.', true);
      button($('crDetail'), 'Try again', load);
    }
  }
  function open() {
    if (!dialog.hidden) return load();
    previousFocus = document.activeElement; previousOverflow = document.body.style.overflow;
    dialog.hidden = false; document.body.style.overflow = 'hidden';
    dialog.querySelector('button[data-cr-close]').focus();
    load();
  }
  function close() {
    generation++; model = null; preview = null; selectedId = null; dialog.hidden = true;
    $('crChannel').replaceChildren(new Option('All channels', 'all'));
    $('crList').replaceChildren(); $('crDetail').replaceChildren(); $('crSubtitle').textContent = '';
    document.body.style.overflow = previousOverflow;
    if (previousFocus?.isConnected) previousFocus.focus();
  }
  dialog.querySelectorAll('[data-cr-close]').forEach(el => el.addEventListener('click', close));
  $('crChannel').addEventListener('change', () => { renderList(); const first = model?.items.find(item => $('crChannel').value === 'all' || item.channel === $('crChannel').value); if (first) select(first.id); });
  dialog.addEventListener('keydown', event => {
    if (event.key === 'Escape') { event.preventDefault(); event.stopPropagation(); close(); }
    if (event.key === 'Tab') {
      const focusable = [...dialog.querySelectorAll('button:not(:disabled),select,a[href],video[controls],summary')].filter(el => el.offsetParent !== null);
      const first = focusable[0], last = focusable.at(-1);
      if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
      else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
    }
  });
  document.addEventListener('pulseforge:content-review', open);
  document.addEventListener('pulseforge:tenant-changing', () => { if (!dialog.hidden) close(); });
  document.addEventListener('pulseforge:tenant-changed', () => { if (!dialog.hidden) close(); });
  window.addEventListener('hashchange', () => { if (location.hash === '#content-review') open(); });
  document.addEventListener('click', event => { const link = event.target.closest('a[href="/command-deck#content-review"]'); if (link && location.pathname === '/command-deck') { event.preventDefault(); open(); } });
  window.PulseforgeContentReview = { open, close };
  if (location.hash === '#content-review') open();
})();
