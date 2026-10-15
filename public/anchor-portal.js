'use strict';
/* Portal rendering never interpolates unescaped server text or arbitrary photo URLs. */
const state = { me: null, tab: 'home', locationId: null, generation: 0, busy: false, issueOffset: 0 };
const $ = id => document.getElementById(id);
const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const cap = () => state.me?.capabilities || {};
const locations = () => state.me?.locations || [];
const selected = () => locations().find(l => String(l.id) === String(state.locationId));
const fmtDate = value => { const date = new Date(value); return value && !Number.isNaN(date.getTime()) ? date.toLocaleString(undefined, {month:'short',day:'numeric',year:'numeric',hour:'numeric',minute:'2-digit'}) : 'Not recorded'; };
const statusLabel = s => ({acknowledged:'In Progress',in_progress:'In Progress',open:'Open',resolved:'Resolved',completed:'Completed',scheduled:'Scheduled',in_progress_visit:'In Progress'}[s] || String(s || '').replace(/_/g,' '));
const badge = s => `<span class="badge ${s === 'resolved' || s === 'completed' ? 'resolved' : s === 'open' ? 'open' : 'ack'}">${esc(statusLabel(s))}</span>`;
const panel = (title, content) => `<section class="panel"><h2>${esc(title)}</h2>${content}</section>`;
const empty = text => `<p class="muted empty">${esc(text)}</p>`;
function flash(text, kind = 'ok') { $('flash').textContent = text; $('flash').className = `msg ${kind}`; }
async function api(path, opts = {}) {
  let res;
  try { res = await fetch('/api/anchor-portal' + path, {credentials:'same-origin', ...opts, headers:{Accept:'application/json', ...(window.PORTAL_CSRF ? {'X-CSRF-Token':window.PORTAL_CSRF} : {}), ...(opts.body ? {'Content-Type':'application/json'} : {})}, body:opts.body ? JSON.stringify(opts.body) : undefined}); }
  catch (_) { throw new Error('Connection interrupted. Your form is still here. Check your connection before trying again.'); }
  if (res.status === 401 || res.redirected) throw new Error('Your session has expired. Sign in again to continue.');
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.message || 'We couldn’t save or load this information. Please try again.');
  return data;
}
function goto(tab) { if (state.busy) return; state.tab = tab; state.issueOffset = 0; render(); }
function paint(generation, html) { if (generation !== state.generation) return false; $('view').innerHTML = html; bindPhotos(); return true; }
function renderTabs() {
  const items = [['home','Overview'],['history','Service Reports'],['scope','Facility Areas'],['issues','Open Issues'],['resolved','Resolved Issues'],['report','Report a Problem']];
  if (cap().cleaner) items.push(['visit',"Today's Visit"]);
  if (cap().operator || cap().cleaner) items.push(['author','New Service Report']);
  $('tabs').innerHTML = items.map(([id,label]) => `<button type="button" class="tab ${state.tab === id ? 'active' : ''}" aria-current="${state.tab === id ? 'page' : 'false'}" data-tab="${id}" ${state.busy ? 'disabled' : ''}>${label}</button>`).join('');
  $('tabs').querySelectorAll('button').forEach(b => b.onclick = () => goto(b.dataset.tab));
  $('building').disabled = state.busy || !locations().length;
  const demo = !!selected()?.is_demo;
  $('demoBanner').classList.toggle('hidden', !demo);
  $('demoBanner').textContent = demo ? 'DEMO • Synthetic sample records and illustrative images only. These are not records of service performed for this facility.' : '';
  $('pageTitle').textContent = selected()?.name || 'Service portal';
}
function safePhoto(value) { return typeof value === 'string' && /^data:image\/(jpeg|png|webp);base64,[A-Za-z0-9+/=]+$/.test(value) ? value : ''; }
function gallery(evidence = []) {
  return `<div class="gallery">${evidence.map(p => safePhoto(p.data_url) ? `<figure><button type="button" data-photo aria-label="Enlarge ${esc(p.caption || 'service photo')}"><img src="${esc(p.data_url)}" alt="${esc(p.caption || 'Service photo proof')}" loading="lazy"></button><figcaption>${esc(p.caption || (p.purpose === 'resolution' ? 'Resolution proof' : 'Photo proof'))}</figcaption></figure>` : '').join('')}</div>`;
}
function bindPhotos() { $('view').querySelectorAll('[data-photo]').forEach(b => b.onclick = () => {
  const dialog = document.createElement('dialog'); dialog.className = 'lightbox';
  const close = document.createElement('button'); close.className = 'btn ghost'; close.textContent = 'Close photo';
  const img = document.createElement('img'); img.src = b.querySelector('img').src; img.alt = b.querySelector('img').alt;
  dialog.append(close,img); document.body.append(dialog); close.onclick = () => dialog.close(); dialog.onclose = () => { dialog.remove(); b.focus(); }; dialog.onclick = e => { if (e.target === dialog) dialog.close(); }; dialog.showModal();
}); }
function reportRows(visits) { return visits.map(v => `<div class="row"><div><strong>${esc(v.title || 'Service Report')}</strong><div class="muted">${esc(fmtDate(v.completed_at || v.scheduled_at))} · ${esc(v.completed_by_name || v.cleaner_name || 'Anchor team')}</div></div><div class="actions">${badge(v.status)}<button type="button" class="btn ghost small" data-visit="${esc(v.id)}">View report</button></div></div>`).join('') || empty('No service reports recorded for this building yet.'); }
function bindReports(g) { $('view').querySelectorAll('[data-visit]').forEach(b => b.onclick = () => detail(b.dataset.visit, g)); }
async function detail(id, previousGeneration) {
  if (previousGeneration !== state.generation || state.busy) return;
  const g = ++state.generation; $('view').innerHTML = empty('Loading service report…');
  try {
    const {visit:v} = await api(`/visits/${encodeURIComponent(id)}`);
    if (!paint(g, panel('Service Report', `<p>${esc(v.location_name)}</p><p class="muted">${esc(fmtDate(v.completed_at || v.scheduled_at))} · Completed by ${esc(v.completed_by_name || v.cleaner_name || 'Not recorded')}</p>${badge(v.status)}<h3 class="section-title">Areas serviced</h3>${(v.items || []).map(i => `<div class="check-item"><strong>${esc(i.section_title)} · ${esc(i.item_label)}</strong><p>${esc(statusLabel(i.outcome))}</p><p class="preserve muted">${esc(i.exception_reason || '')}</p></div>`).join('') || empty('No area details recorded.')}<h3 class="section-title">Service notes</h3><p class="preserve">${esc(v.visit_notes || 'No additional notes.')}</p><h3 class="section-title">Photo proof</h3>${v.evidence?.length ? gallery(v.evidence) : empty('No photos attached.')}<button type="button" class="btn ghost" id="backReports">Back</button>`))) return;
    $('backReports').onclick = () => render();
  } catch(e) { loadError(g,e); }
}
async function renderHome(g, id) {
  const [home,{issues}] = await Promise.all([api(`/home?location_id=${id}`), api(`/issues?location_id=${id}`)]);
  const cards = `<div class="grid cols-2">${[['Last service',fmtDate(home.last_service)],['Next service',fmtDate(home.next_service)],['Service status',home.service_status || 'No service recorded'],['Open issues',home.open_issues_count ?? 0]].map(([a,b]) => `<div class="panel"><div class="stat-label">${a}</div><div class="stat-value">${esc(b)}</div></div>`).join('')}</div>`;
  const activity = (issues || []).slice(0,5).map(i => `<div class="row"><div><strong>${esc(i.title || i.description)}</strong><p class="muted">${esc(fmtDate(i.resolved_at || i.created_at))}</p></div>${badge(i.status)}</div>`).join('');
  if (paint(g,cards + panel('Recent Service Reports',reportRows(home.recent_visits || [])) + panel('Recent Activity',activity || empty('No issue activity yet.')))) bindReports(g);
}
async function renderScope(g,id) { const {scope} = await api(`/locations/${id}/scope`); paint(g,panel('Facility Areas',scope.map(s => `<section><h3 class="section-title">${esc(s.title)}</h3>${s.items.map(i => `<p class="check-item">${esc(i.label)}</p>`).join('')}</section>`).join('') || empty('No facility areas have been added yet.'))); }
async function renderIssues(g,id) {
  const {issues} = await api(`/issues?location_id=${id}&offset=${state.issueOffset}${state.tab === 'resolved' ? '&status=resolved' : '&status=active'}`);
  const rows = issues.filter(i => state.tab === 'resolved' ? i.status === 'resolved' : i.status !== 'resolved');
  const html = rows.map(i => `<article class="check-item"><div class="row"><div><strong>${esc(i.title || i.issue_type || 'Service issue')}</strong><p class="muted">${esc(i.room_category || 'General area')} · ${esc(i.severity || 'medium')} urgency · ${esc(fmtDate(i.created_at))}</p></div>${badge(i.status)}</div><p class="preserve">${esc(i.description)}</p>${gallery((i.evidence || []).filter(p => p.purpose !== 'resolution'))}${i.resolution ? `<h3 class="section-title">Resolution</h3><p class="preserve">${esc(i.resolution)}</p>${gallery((i.evidence || []).filter(p => p.purpose === 'resolution'))}` : ''}${cap().operator && i.status !== 'resolved' ? `<div class="actions">${i.status === 'open' ? `<button class="btn ghost small" data-progress="${esc(i.id)}">Mark In Progress</button>` : ''}<button class="btn small" data-resolve="${esc(i.id)}">Resolve with proof</button></div>` : ''}</article>`).join('');
  if (!paint(g,panel(state.tab === 'resolved' ? 'Resolved Issues' : 'Open Issues',(html || empty(state.tab === 'resolved' ? 'No resolved issues on this page.' : 'No open issues on this page.')) + `<div class="actions">${state.issueOffset ? '<button class="btn ghost" id="previousIssues">Previous issues</button>' : ''}${issues.length === 100 ? '<button class="btn ghost" id="nextIssues">More issues</button>' : ''}</div>`))) return;
  if ($('previousIssues')) $('previousIssues').onclick = () => { state.issueOffset = Math.max(0,state.issueOffset-100); render(); };
  if ($('nextIssues')) $('nextIssues').onclick = () => { state.issueOffset += 100; render(); };
  $('view').querySelectorAll('[data-progress]').forEach(b => b.onclick = () => mutate(b,async () => { await api(`/issues/${b.dataset.progress}`,{method:'PATCH',body:{status:'in_progress'}}); flash('Issue marked In Progress.'); render(); }));
  $('view').querySelectorAll('[data-resolve]').forEach(b => b.onclick = () => resolutionForm(b.dataset.resolve));
}
function photoField(id, multiple = false) { return `<label class="field">${multiple ? 'Photos (up to 3)' : 'Photo (optional)'}<input id="${id}" type="file" accept="image/jpeg,image/png,image/webp" ${multiple ? 'multiple' : ''}></label><p class="muted">JPEG, PNG or WebP, up to 15 MB each. Photos are resized before saving.</p><div id="${id}Status" class="photo-status" role="status"></div><div id="${id}Preview" class="gallery"></div>`; }
async function compressPhoto(file) {
  if (!['image/jpeg','image/png','image/webp'].includes(file.type)) throw new Error('Choose a JPEG, PNG or WebP photo. Convert HEIC images to JPEG first.');
  if (!file.size || file.size > 15 * 1024 * 1024) throw new Error('Each source photo must be between 1 byte and 15 MB.');
  const url = URL.createObjectURL(file);
  try {
    const img = await new Promise((resolve,reject) => { const image = new Image(); image.onload = () => resolve(image); image.onerror = () => reject(new Error('This photo could not be read. Choose another image.')); image.src = url; });
    if (!img.naturalWidth || !img.naturalHeight || img.naturalWidth * img.naturalHeight > 60e6) throw new Error('This photo is too large to process safely. Choose a smaller image.');
    let scale = Math.min(1,1600 / Math.max(img.naturalWidth,img.naturalHeight));
    const canvas = document.createElement('canvas');
    for (let attempt = 0; attempt < 5; attempt++) {
      canvas.width = Math.max(1,Math.round(img.naturalWidth * scale)); canvas.height = Math.max(1,Math.round(img.naturalHeight * scale));
      const ctx = canvas.getContext('2d'); ctx.fillStyle = '#fff'; ctx.fillRect(0,0,canvas.width,canvas.height); ctx.drawImage(img,0,0,canvas.width,canvas.height);
      const result = canvas.toDataURL('image/jpeg',.82 - attempt * .09);
      if (result.length * .75 <= 1500000 && safePhoto(result)) return {data_url:result,caption:file.name.slice(0,160)};
      scale *= .75;
    }
    throw new Error('This photo is still too large. Choose a smaller image.');
  } finally { URL.revokeObjectURL(url); }
}
function photoInput(id,max = 1) {
  let photos = [], pending = false, error = '', revision = 0;
  $(id).onchange = async () => {
    const version = ++revision; const files = Array.from($(id).files || []); photos = []; error = ''; pending = true;
    const status = $(id+'Status'), preview = $(id+'Preview'); preview.innerHTML = ''; status.textContent = 'Preparing photos…';
    try {
      if (files.length > max) throw new Error(`Choose no more than ${max} photo${max > 1 ? 's' : ''}.`);
      const ready = [];
      for (const [index,file] of files.entries()) { status.textContent = `Preparing photo ${index+1} of ${files.length}…`; ready.push(await compressPhoto(file)); }
      if (version !== revision) return; photos = ready;
      preview.innerHTML = ready.map(p => `<figure><img src="${esc(p.data_url)}" alt="Preview: ${esc(p.caption)}"><figcaption>${esc(p.caption)}</figcaption></figure>`).join('');
      status.textContent = files.length ? `${files.length} photo${files.length > 1 ? 's' : ''} ready. They will save with this form.` : '';
    } catch(e) { if (version !== revision) return; error = e.message; status.textContent = error; }
    finally { if (version === revision) pending = false; }
  };
  return () => { if (pending) throw new Error('Please wait for the photo preview to finish.'); if (error) throw new Error(error); return photos; };
}
async function mutate(button, work) {
  if (state.busy) return; state.busy = true; const old = button.textContent; button.disabled = true; button.textContent = 'Saving…'; renderTabs();
  const form = button.closest('form'); if (form) form.querySelectorAll('input,select,textarea,button').forEach(el => el.disabled = true);
  try { await work(); }
  catch(e) { const target = form?.querySelector('[data-form-error]'); if (target) { target.textContent = e.message; target.classList.remove('hidden'); } else flash(e.message,'err'); }
  finally { state.busy = false; if (button.isConnected) { button.disabled = false; button.textContent = old; if (form) form.querySelectorAll('input,select,textarea,button').forEach(el => el.disabled = false); } renderTabs(); }
}
function formShell(title,body,submit='Save') { return panel(title,`<form id="portalForm"><div data-form-error class="msg err hidden" role="alert"></div>${body}<div class="actions"><button type="submit" class="btn" id="saveForm">${submit}</button><button type="button" class="btn ghost" id="cancelForm">Cancel</button></div></form>`); }
function formBind(work,back='issues') { $('cancelForm').onclick = () => goto(back); $('portalForm').onsubmit = e => {e.preventDefault(); if (!$('portalForm').reportValidity()) return; mutate($('saveForm'),work);}; }
function issueForm(g,id) {
  paint(g,formShell('Report a Problem',`<p class="notice">Building: ${esc(selected().name)}</p><label class="field">Area / room<input id="issueRoom" required maxlength="200" placeholder="e.g. East wing, second floor"></label><label class="field">Problem type<select id="issueType"><option value="cleaning">Cleaning quality</option><option value="supplies">Supplies</option><option value="damage">Damage</option><option value="access">Access</option><option value="other">Other</option></select></label><label class="field">Description<textarea id="issueDesc" required maxlength="5000" placeholder="Tell us what needs attention"></textarea></label><label class="field">Urgency<select id="issueSeverity"><option value="low">Low — routine attention</option><option value="medium" selected>Medium — please address soon</option><option value="high">High — urgent attention</option></select></label><p class="notice">For an immediate safety emergency, contact your facility emergency team or local emergency services. This portal is not an emergency response service.</p>${photoField('issuePhoto')}`,'Submit problem'));
  const photos = photoInput('issuePhoto'), requestId = crypto.randomUUID();
  formBind(async () => {const p = photos(); await api('/issues',{method:'POST',body:{request_id:requestId,location_id:Number(id),room_category:$('issueRoom').value.trim(),issue_type:$('issueType').value,description:$('issueDesc').value.trim(),severity:$('issueSeverity').value,photo_data_url:p[0]?.data_url || ''}}); flash('Problem submitted. You can track it under Open Issues.'); state.tab = 'issues'; state.issueOffset = 0; render();});
}
function resolutionForm(id) {
  const g = ++state.generation;
  paint(g,formShell('Resolve issue',`<p class="muted">Add a clear resolution note and photo proof for the client record.</p><label class="field">Resolution note<textarea id="resolution" required maxlength="5000"></textarea></label>${photoField('resolutionPhoto')}`,'Save resolution'));
  const photos = photoInput('resolutionPhoto');
  formBind(async () => { const p = photos(); await api(`/issues/${encodeURIComponent(id)}`,{method:'PATCH',body:{status:'resolved',resolution:$('resolution').value.trim(),resolution_photo_data_url:p[0]?.data_url || '',resolution_photo_caption:p[0]?.caption || ''}}); flash('Resolution saved.'); state.tab = 'resolved'; state.issueOffset = 0; render(); });
}
async function authorForm(g,id) {
  const {scope} = await api(`/locations/${id}/scope`);
  if (!paint(g,formShell('New Service Report',`<p class="notice">Building: ${esc(selected().name)}</p><label class="field">Service date and time<input type="datetime-local" id="serviceDate" required></label><fieldset><legend class="section-title">Areas serviced</legend>${scope.map((s,index) => `<label class="field"><input type="checkbox" style="width:auto" name="area" value="${index}"> ${esc(s.title)}</label>`).join('')}<label class="field">Additional area<input id="extraArea" maxlength="200" placeholder="Optional area not listed above"></label></fieldset><label class="field">Service notes<textarea id="reportNotes" required maxlength="5000"></textarea></label><p class="muted">Completed by ${esc(state.me.user.name)}. Your name is recorded with this report.</p>${photoField('reportPhotos',true)}`,'Save Service Report'))) return;
  const now = new Date(); now.setMinutes(now.getMinutes()-now.getTimezoneOffset()); $('serviceDate').value = now.toISOString().slice(0,16);
  const photos = photoInput('reportPhotos',3), requestId = crypto.randomUUID();
  formBind(async () => { const p = photos(); const areas = [...$('view').querySelectorAll('[name=area]:checked')].map(i => ({title:scope[Number(i.value)].title,outcome:'completed'})); const extra = $('extraArea').value.trim(); if (extra) areas.push({title:extra,outcome:'completed'}); if (!areas.length) throw new Error('Choose at least one serviced area.'); await api('/visits',{method:'POST',body:{request_id:requestId,location_id:Number(id),service_date:new Date($('serviceDate').value).toISOString(),visit_notes:$('reportNotes').value.trim(),areas,photos:p}}); flash('Service Report saved with photo proof.'); state.tab = 'history'; render(); },'history');
}
async function cleanerVisit(g) {
  const {visit:v} = await api('/visits/today');
  if (!v) { paint(g,panel("Today's Visit",empty('No visit assigned today.'))); return; }
  if (String(v.location_id) !== String(state.locationId)) { paint(g,panel("Today's Visit",empty('Your scheduled visit is at another assigned building. Select that building to continue.'))); return; }
  if (v.status === 'completed') { if (paint(g,panel('Visit complete',reportRows([v])))) bindReports(g); return; }
  if (v.status === 'scheduled') {
    if (paint(g,panel("Today's Visit",`<p>${esc(v.location_name)} · ${esc(fmtDate(v.scheduled_at))}</p><p class="notice">Start when you are ready to begin the checklist.</p><button type="button" class="btn" id="startVisit">Start visit</button>`))) $('startVisit').onclick = () => mutate($('startVisit'),async () => {await api(`/visits/${v.id}/start`,{method:'POST'}); render();});
    return;
  }
  const outcomes = ['completed','unable_to_complete','issue_detected','not_applicable'];
  if (!paint(g,formShell("Today's Visit",`<p>${esc(v.location_name)} · ${esc(fmtDate(v.scheduled_at))}</p>${v.items.map(i => `<div class="check-item" data-item="${esc(i.id)}"><strong>${esc(i.section_title)} · ${esc(i.item_label)}</strong><label class="field">Outcome<select class="outcome" required><option value="">Choose an outcome</option>${outcomes.map(o => `<option value="${o}" ${i.outcome === o ? 'selected' : ''}>${esc(statusLabel(o))}</option>`).join('')}</select></label><label class="field">Exception reason (required for an issue or incomplete task)<textarea class="reason" maxlength="2000">${esc(i.exception_reason || '')}</textarea></label></div>`).join('')}<label class="field">Visit notes<textarea id="visitNotes" maxlength="5000">${esc(v.visit_notes || '')}</textarea></label>${photoField('visitPhoto')}`,'Complete visit'))) return;
  const photos = photoInput('visitPhoto');
  formBind(async () => {
    const p = photos(); const entries = [...$('view').querySelectorAll('[data-item]')].map(el => ({id:el.dataset.item,outcome:el.querySelector('.outcome').value,exception_reason:el.querySelector('.reason').value.trim()}));
    if (!entries.length) throw new Error('This visit has no checklist. Ask your coordinator to add one.');
    if (entries.some(i => !i.outcome || (['unable_to_complete','issue_detected'].includes(i.outcome) && !i.exception_reason))) throw new Error('Select every outcome and explain each issue or incomplete task.');
    await api(`/visits/${v.id}/complete`,{method:'POST',body:{visit_notes:$('visitNotes').value.trim(),photos:p,items:entries}});
    flash('Visit completed.'); render();
  },'home');
}
function loadError(g,e) { if (paint(g,panel('Unable to load',`<p class="msg err">${esc(e.message)}</p><div class="actions"><button class="btn" id="retry">Try again</button><a class="btn ghost" href="/login">Sign in</a></div>`))) $('retry').onclick = () => render(); }
async function render() {
  const g = ++state.generation, id = encodeURIComponent(state.locationId || ''); renderTabs();
  if (!selected()) {paint(g,panel('No building assigned',empty('Your account has no assigned buildings. Ask your Anchor contact to add access.'))); return;}
  paint(g,empty('Loading…'));
  try {
    if (state.tab === 'home') await renderHome(g,id);
    else if (state.tab === 'history') {const {visits} = await api(`/visits?location_id=${id}&limit=50`); if (paint(g,panel('Service Reports',reportRows(visits)))) bindReports(g);}
    else if (state.tab === 'scope') await renderScope(g,id);
    else if (['issues','resolved'].includes(state.tab)) await renderIssues(g,id);
    else if (state.tab === 'report') issueForm(g,id);
    else if (state.tab === 'author' && (cap().operator || cap().cleaner)) await authorForm(g,id);
    else if (state.tab === 'visit' && cap().cleaner) await cleanerVisit(g);
  } catch(e) {loadError(g,e);}
}
async function boot() {
  state.me = await api('/me'); $('userLabel').textContent = state.me.user.name;
  $('building').innerHTML = locations().map(l => `<option value="${esc(l.id)}">${esc(l.name)}</option>`).join('');
  state.locationId = locations()[0]?.id || null;
  $('building').onchange = e => {if (state.busy) return; state.locationId = e.target.value; state.issueOffset = 0; render();};
  if (cap().cleaner && !cap().facility_client && !cap().operator) state.tab = 'visit';
  render();
}
boot().catch(e => loadError(state.generation,e));
