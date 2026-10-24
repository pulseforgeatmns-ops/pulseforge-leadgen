'use strict';

const { DecisionService, beginShadow, completeShadow } = require('./DecisionService');
let defaultService;

// These endpoints do not enter WorkspaceEngine. Observe their existing JSON
// responses without changing status, body, exceptions, or route selection.
function observeOperatorHttp(req, res, { service, clientId, source, sessionId, missionId, routeHint } = {}) {
  try {
    const observer = service || (defaultService ||= new DecisionService());
    const shadow = beginShadow(observer, {
      question: req.body?.question || req.body?.message,
      context: { tenantId: clientId, missionId },
      sessionId,
      // Do not persist authentication session IDs in the decision audit.
    }, null, source);
    if (!shadow) return;
    const json = res.json;
    res.json = function observedJson(body) {
      try {
        // Fixed endpoint hints describe the handler that actually ran. They are
        // observation metadata only and are never added to the HTTP response.
        const observed = routeHint ? { ...body, route: body?.route || routeHint,
          resolution: body?.resolution || (body?.action ? { action: body.action } : undefined) } : body;
        completeShadow(shadow, observed, res.statusCode >= 400 ? true : null);
      } catch (_) { /* even projection bugs cannot break the HTTP response */ }
      return json.call(this, body);
    };
  } catch (_) { /* no effect on HTTP handling */ }
}
function observeLegacyChat(req, res, service, clientId) {
  return observeOperatorHttp(req, res, { service, clientId, source: 'legacy_chat' });
}
module.exports = { observeLegacyChat, observeOperatorHttp };
