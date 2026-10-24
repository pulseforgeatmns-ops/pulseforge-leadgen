'use strict';

// One backend-owned, deterministic contract for read-only review navigation.
// A navigation request never approves work or proves that the browser rendered it.
function normalize(value) { return String(value || '').normalize('NFKC').replace(/[’]/g, "'").replace(/\s+/g, ' ').trim(); }
function parseReviewNavigation(question, explicit = null) {
  const original = normalize(question);
  if (explicit != null) {
    if (!['content_review', 'account_review'].includes(explicit.surface)) return { kind: 'blocked', reason: 'unsupported_review' };
    // Explicit button intent cannot smuggle a second action in accompanying prose.
    if (original && /\b(?:publish|send|schedule|delete|save|approve|reject|execute|assign)\b/i.test(original)) return { kind: 'blocked', reason: 'mixed_action' };
    return { kind: explicit.surface, ownerId: explicit.aoId == null ? null : String(explicit.aoId), ownerName: null };
  }
  // Establish ownership of the requested navigation clause before interpreting
  // safety constraints. Mission artifacts can mention reviews/content/drafts
  // without requesting either visual review surface.
  let q = original.replace(/[.!?]+$/, '').replace(/,?\s+please$/i, '').trim();
  q = q.replace(/^(?:please\s+)?(?:can|could|would|will)\s+you\s+(?:please\s+)?/i, '')
    .replace(/^(?:can|could)\s+i\s+/i, '')
    .replace(/^(?:please|let me|i'd like to|i would like to|i want to)\s+/i, '');
  const negated = /^(?:don't|do not|never|cancel|stop)\s+(?:open|opening|review|reviewing|show|showing|see)\b/i.test(q);
  q = q.replace(/^(?:don't|do not|never|cancel|stop)\s+/i, '')
    .replace(/^(opening|reviewing|showing)\b/i, value => value.slice(0,-3));
  q = q.replace(/^bring up\s+/i, 'open ').replace(/\baccount list(?=$|[.!?;,])/i, 'accounts');
  q = q.replace(/^give me (?:a |an )?(?:review|overview) of\s+/i, 'review ');
  // A secondary imperative is checked against the full input after ownership.
  q = q.split(/\s+(?:and|then)\s+(?=(?:also\s+)?(?:please\s+)?(?:approve|reject|publish|send|schedule|delete|save|execute|write|generate|create|assign|reassign)\b)|[;,.](?:\s|$)/i)[0].trim();
  const ownAccounts = /^(?:open|review|show(?: me)?|see|check)\s+(?:the\s+|my\s+)?(?:assigned\s+)?accounts$/i;
  const prefix = '(?:show(?: me)?|review|open|check|see)\\s+';
  const match = q.match(new RegExp(`^${prefix}(?:the\\s+)?(?:assigned\\s+)?accounts\\s+(?:assigned\\s+to|for|belonging to)\\s+(.+)$`, 'i'))
    || q.match(new RegExp(`^${prefix}(.+?)(?:'s)?\\s+(?:assigned\\s+)?accounts$`, 'i'));
  const content = /^(?:open|show(?: me)?|review|preview|see|check)\s+(?:the\s+|my\s+|our\s+)?(?:pending\s+)?(?:content(?: review)?|social(?: posts?)?|drafts?|posts?)(?:\s+(?:waiting|pending|awaiting|ready|that needs|for review)\b.*)?$/i.test(q)
    || /^(?:what|which)\s+(?:content|drafts?|posts?)\s+(?:is|are)\s+(?:awaiting|waiting|pending|ready)\b/i.test(q)
    || /^does\s+paige\s+have\s+pending\s+posts\s+for\s+me\s+to\s+approve$/i.test(q);
  let intent = ownAccounts.test(q) ? {kind:'account_review',ownerName:null,ownerId:null}
    : match ? {kind:'account_review',ownerName:match[1].replace(/'s$/i,'').trim(),ownerId:null}
    : content ? {kind:'content_review'} : null;
  // These compound imperative forms explicitly name a review and an operation.
  if (!intent && /^(?:generate|create|write)\s+(?:new\s+)?(?:posts|drafts|content)\s+for review$/i.test(q)) intent={kind:'blocked',reason:'mixed_action'};
  if (!intent && /^review\s+and\s+delete\s+.+(?:'s)?\s+accounts$/i.test(original)) intent={kind:'blocked',reason:'mixed_action'};
  if (!intent) return null;
  if (negated || /\b(?:do not|don't|never)\s+(?:open|show|review)\b/i.test(original)) return {kind:'blocked',reason:'negated'};
  const operative = original.replace(/\b(?:do not|don't|never)\s+(?:approve|reject|publish|send|schedule|delete|save|execute)(?:\s+(?:anything|everything|changes))?/gi,'');
  if (/\b(?:publish|send|schedule|delete|save|execute|reassign|assign|write|generate|create)\b/i.test(operative)
      || /(?:\b(?:and|then)\s+(?:also\s+)?|[;,.]\s*)(?:please\s+)?(?:approve|reject)\b/i.test(operative)) return {kind:'blocked',reason:'mixed_action'};
  return intent;
}
function resolveReviewNavigation(intent, scope = {}) {
  if (!intent) return null;
  const tenantId = scope.tenantId == null ? null : String(scope.tenantId);
  const metadata = { route: intent.kind, readOnly: true, mutatesMissionState: false, navigationStatus: 'requested' };
  const response = (prose, uiAction = null, status = 'requested') => ({ prose, uiAction, metadata: { ...metadata, navigationStatus: status }, recommendedActions: uiAction ? [uiAction] : [], suggestions: [], mission: null, resolution: null });
  if (intent.kind === 'blocked') return response(intent.reason === 'negated' ? 'No review was opened.' : 'Please separate the review request from any approval or execution request. Nothing has been changed.', null, 'not_requested');
  if (!tenantId) return response('Choose an authenticated workspace before opening a review.', null, 'unavailable');
  if (intent.kind === 'content_review') return response('Content review requested for this workspace. Rendering and data loading must complete before the review is available.', { type: 'open_content_review', label: 'Review content', href: '/command-deck#content-review', tenantId });
  if (!Array.isArray(scope.aos)) return response('The authorized operator roster could not be verified. No account review was opened.', null, 'unavailable');
  const name = normalize(intent.ownerName).toLowerCase();
  const matches = scope.aos.filter(ao => intent.ownerId != null ? String(ao.id) === intent.ownerId : name ? normalize(ao.name).toLowerCase() === name || normalize(ao.name).toLowerCase().split(' ')[0] === name : String(ao.id) === String(scope.defaultAoId));
  if (matches.length !== 1) return response('Choose the acquisition operator in the account picker so I can request the correct accounts.', null, 'clarification_required');
  const ao = matches[0];
  return response(`Account review requested for ${ao.name}. The account data still needs to load.`, { type: 'open_account_review', label: 'Review accounts', tenantId, aoId: ao.id, aoName: ao.name });
}
module.exports = { parseReviewNavigation, resolveReviewNavigation };
