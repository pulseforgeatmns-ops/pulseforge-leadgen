const axios = require('axios');
const { inspectSequenceTemplates } = require('./templateMerge');

const BREVO_API_BASE = 'https://api.brevo.com/v3';
const VERIFIED_EMAIL_STATUSES = new Set(['valid', 'verified']);
const BOUNCE_ACTION_TYPES = [
  'email_bounced',
  'email_soft_bounce',
  'email_hard_bounce',
  'hard_bounce',
  'soft_bounce',
  'bounce',
];

// Sequence access is client-scoped. Sharing a template between clients must be
// an explicit decision here; matching a global sequence key is never enough.
const CLIENT_SEQUENCE_MAP = {
  1: {
    cleaning: 'cleaning',
    restaurant: 'restaurant',
    salon: 'salon',
    fitness: 'fitness',
    property: 'property',
    landscaping: 'landscaping',
    home_services: 'home_services',
    auto: 'auto',
    auto_repair: 'auto',
    property_management: 'property',
    med_spa: 'med_spa',
    home_renovation: 'home_renovation',
  },
  2: {
    property_management: 'mshi_property_management',
    probate_attorney: 'mshi_probate_attorney',
    investor_flipper: 'mshi_investor_flipper',
    home_renovation: 'mshi',
  },
  5: {
    cleaning: 'cleaning',
    restaurant: 'restaurant',
    salon: 'salon',
    fitness: 'fitness',
    property: 'property',
    landscaping: 'landscaping',
    home_services: 'home_services',
    auto: 'auto',
    med_spa: 'med_spa',
    home_renovation: 'home_renovation',
  },
  10: {
    law_firm: 'anchor_law_firm_draft',
    accounting: 'anchor_accounting_draft',
  },
};

// These are intentional migrations, not general cross-sequence sharing.
// A client_1 med-spa prospect may continue from the legacy salon sequence
// without being treated as concurrently enrolled in two sequences.
const CLIENT_SEQUENCE_COMPATIBILITY = {
  1: {
    med_spa: new Set(['salon']),
  },
};

function clean(value) {
  return typeof value === 'string' ? value.trim() : '';
}

function normalized(value) {
  return clean(value).toLowerCase();
}

function condition(code, passed, message, details = {}) {
  return { code, passed: Boolean(passed), message, details };
}

function isCompatiblePriorSequence(clientId, requestedSequence, priorSequence) {
  if (!priorSequence || priorSequence === requestedSequence) return true;
  return CLIENT_SEQUENCE_COMPATIBILITY[Number(clientId)]?.[requestedSequence]?.has(priorSequence) || false;
}

function exactSequenceName(client, prospect, sequenceCatalog = {}, clientSequenceMap = CLIENT_SEQUENCE_MAP) {
  const vertical = normalized(prospect?.vertical);
  if (!vertical) return null;
  const mapped = clientSequenceMap[Number(client?.id)]?.[vertical];
  return mapped && Array.isArray(sequenceCatalog[mapped]) ? mapped : null;
}

async function getBrevoState(client, options = {}) {
  const apiKey = options.brevoApiKey || process.env.BREVO_API_KEY;
  const http = options.http || axios;
  const domain = normalized(client?.sending_domain);
  const senderEmail = normalized(client?.sender_email);

  if (!apiKey || !domain || !senderEmail) {
    return {
      domain: null,
      sender: null,
      errors: !apiKey ? ['BREVO_API_KEY is not configured'] : [],
    };
  }

  const headers = { 'api-key': apiKey, accept: 'application/json' };
  const errors = [];
  const [domainResult, sendersResult] = await Promise.allSettled([
    http.get(`${BREVO_API_BASE}/senders/domains/${encodeURIComponent(domain)}`, { headers, timeout: 15000 }),
    http.get(`${BREVO_API_BASE}/senders`, { headers, timeout: 15000 }),
  ]);

  let domainRecord = null;
  if (domainResult.status === 'fulfilled') {
    domainRecord = domainResult.value.data || null;
  } else {
    const status = domainResult.reason?.response?.status;
    errors.push(status === 404
      ? `Sending domain ${domain} is not registered in Brevo`
      : `Brevo domain check failed: ${domainResult.reason?.message || 'unknown error'}`);
  }

  let sender = null;
  if (sendersResult.status === 'fulfilled') {
    const senders = Array.isArray(sendersResult.value.data?.senders)
      ? sendersResult.value.data.senders
      : [];
    sender = senders.find(item => normalized(item.email) === senderEmail) || null;
  } else {
    errors.push(`Brevo sender check failed: ${sendersResult.reason?.message || 'unknown error'}`);
  }

  return { domain: domainRecord, sender, errors };
}

async function getProspectSendState(pool, client, prospect, sequenceName, sequenceCatalog) {
  if (!pool || !client?.id || !prospect?.id) {
    return {
      bounced: false,
      pendingSend: false,
      conflictingSequence: null,
      error: 'Database, client id, and prospect id are required for readiness evaluation',
    };
  }

  let bounceResult;
  let sendLogResult;
  try {
    [bounceResult, sendLogResult] = await Promise.all([
      pool.query(`
        SELECT action_type
        FROM touchpoints
        WHERE prospect_id = $1
          AND client_id = $2
          AND channel = 'email'
          AND action_type = ANY($3::text[])
        ORDER BY created_at DESC
      `, [prospect.id, client.id, BOUNCE_ACTION_TYPES]),
      pool.query(`
        SELECT action, status, payload, ran_at
        FROM agent_log
        WHERE agent_name = 'emmett'
          AND prospect_id = $1
          AND client_id = $2
          AND action IN ('email_pending', 'email_sent')
        ORDER BY ran_at DESC
      `, [prospect.id, client.id]),
    ]);
  } catch (err) {
    return {
      bounced: false,
      pendingSend: false,
      conflictingSequence: null,
      error: `Readiness database check failed: ${err.message}`,
    };
  }

  const pendingSend = sendLogResult.rows.some(row =>
    row.action === 'email_pending' && row.status === 'pending'
  );
  const sendsBySequence = new Map();
  for (const row of sendLogResult.rows) {
    const sentSequence = clean(row.payload?.sequence);
    if (row.action === 'email_sent' && sentSequence) {
      sendsBySequence.set(sentSequence, (sendsBySequence.get(sentSequence) || 0) + 1);
    }
  }
  const conflictingSequence = [...sendsBySequence.entries()].find(([sentSequence, count]) => {
    if (isCompatiblePriorSequence(client.id, sequenceName, sentSequence)) return false;
    const steps = sequenceCatalog?.[sentSequence];
    return !Array.isArray(steps) || count < steps.length;
  })?.[0] || null;

  return {
    bounced: bounceResult.rows.length > 0,
    pendingSend,
    conflictingSequence,
  };
}

async function getCurrentProspect(pool, client, prospect) {
  if (!pool || !client?.id || !prospect?.id) {
    return {
      prospect,
      error: 'Database, client id, and prospect id are required for readiness evaluation',
    };
  }
  try {
    const result = await pool.query(`
      SELECT p.*, row_to_json(c) AS company_fields, c.name AS company
      FROM prospects p
      LEFT JOIN companies c ON c.id = p.company_id AND c.client_id = p.client_id
      WHERE p.id = $1
        AND p.client_id = $2
    `, [prospect.id, client.id]);
    if (!result.rows[0]) {
      return { prospect, error: 'Prospect no longer exists for this client' };
    }
    return { prospect: { ...prospect, ...result.rows[0] }, error: null };
  } catch (err) {
    return { prospect, error: `Current prospect check failed: ${err.message}` };
  }
}

async function evaluateSendingReadiness({
  client,
  prospect,
  sequenceCatalog,
  pool,
  brevoState,
  brevoApiKey,
  http,
  clientSequenceMap,
  assignedSequenceName,
} = {}) {
  const checks = [];
  const current = await getCurrentProspect(pool, client, prospect);
  const evaluatedProspect = current.prospect;
  const senderEmail = clean(client?.sender_email);
  const senderName = clean(client?.sender_name);
  const sendingDomain = normalized(client?.sending_domain);

  checks.push(condition(
    'client_sender_configured',
    senderEmail && senderName && sendingDomain,
    'Client sender_email, sender_name, and sending_domain must all be configured.',
    { sender_email: senderEmail || null, sender_name: senderName || null, sending_domain: sendingDomain || null }
  ));
  const senderDomain = normalized(senderEmail.split('@')[1]);
  checks.push(condition(
    'client_sender_domain_matches',
    Boolean(senderDomain) && senderDomain === sendingDomain,
    'Client sender_email must use the configured sending_domain.',
    { sender_domain: senderDomain || null, sending_domain: sendingDomain || null }
  ));

  const resolvedBrevo = brevoState || await getBrevoState(client, { brevoApiKey, http });
  const domainAuthenticated = Boolean(
    resolvedBrevo.domain?.authenticated === true && resolvedBrevo.domain?.verified === true
  );
  checks.push(condition(
    'brevo_domain_authenticated',
    domainAuthenticated,
    'Sending domain must be verified and authenticated in Brevo.',
    {
      sending_domain: sendingDomain || null,
      verified: resolvedBrevo.domain?.verified ?? false,
      authenticated: resolvedBrevo.domain?.authenticated ?? false,
      errors: resolvedBrevo.errors || [],
    }
  ));

  const senderActive = Boolean(
    resolvedBrevo.sender && resolvedBrevo.sender.active === true
  );
  checks.push(condition(
    'brevo_sender_active',
    senderActive,
    'Sender email must be registered and active in Brevo.',
    {
      sender_email: senderEmail || null,
      registered: Boolean(resolvedBrevo.sender),
      active: resolvedBrevo.sender?.active ?? false,
      errors: resolvedBrevo.errors || [],
    }
  ));

  const sequenceName = exactSequenceName(client, evaluatedProspect, sequenceCatalog, clientSequenceMap);
  checks.push(condition(
    'exact_vertical_sequence_exists',
    !current.error && Boolean(sequenceName),
    'A template sequence must exist for the prospect exact vertical.',
    { vertical: clean(evaluatedProspect?.vertical) || null, sequence: sequenceName, error: current.error }
  ));

  const emailStatus = normalized(evaluatedProspect?.email_status);
  checks.push(condition(
    'prospect_has_email',
    !current.error && Boolean(clean(evaluatedProspect?.email)),
    'Prospect must have a non-empty email address.',
    { email: clean(evaluatedProspect?.email) || null, error: current.error }
  ));
  checks.push(condition(
    'prospect_email_verified',
    !current.error && VERIFIED_EMAIL_STATUSES.has(emailStatus),
    'Prospect email_status must be valid or verified.',
    { email_status: emailStatus || null, email_verified: evaluatedProspect?.email_verified ?? null, error: current.error }
  ));
  checks.push(condition(
    'prospect_not_dnc',
    !current.error && evaluatedProspect?.do_not_contact !== true,
    'Prospect must not be marked do-not-contact.',
    { do_not_contact: evaluatedProspect?.do_not_contact ?? null, error: current.error }
  ));
  const templateSequenceName = assignedSequenceName || sequenceName;
  const templateInspection = templateSequenceName
    ? inspectSequenceTemplates(sequenceCatalog?.[templateSequenceName], evaluatedProspect, evaluatedProspect?.company_fields)
    : { unknownTokens: [], missingRequiredTokens: [] };
  const fallbackCoveredTokens = [...new Set(
    (templateInspection.tokens || [])
      .filter(token => token.hasFallback)
      .map(token => token.field)
  )];
  checks.push(condition(
    'template_tokens_known',
    !current.error && templateInspection.unknownTokens.length === 0,
    templateInspection.unknownTokens.length
      ? `Template references unknown token(s): ${templateInspection.unknownTokens.join(', ')}.`
      : 'Every template token must reference a prospect or company field.',
    { unknown_tokens: templateInspection.unknownTokens, error: current.error }
  ));
  checks.push(condition(
    'template_required_tokens_present',
    !current.error && templateInspection.missingRequiredTokens.length === 0,
    templateInspection.missingRequiredTokens.length
      ? `Required template token(s) are empty: ${templateInspection.missingRequiredTokens.join(', ')}.`
      : 'Every required template token must have data.',
    {
      missing_tokens: templateInspection.missingRequiredTokens,
      fallback_covered_tokens: fallbackCoveredTokens,
      error: current.error,
    }
  ));

  const sendState = await getProspectSendState(pool, client, evaluatedProspect, sequenceName, sequenceCatalog);
  checks.push(condition(
    'prospect_not_bounced',
    !sendState.error && !sendState.bounced,
    'Prospect must not have a recorded email bounce.',
    { bounced: sendState.bounced, error: sendState.error || null }
  ));
  checks.push(condition(
    'prospect_not_in_active_sequence',
    !sendState.error && !sendState.pendingSend && !sendState.conflictingSequence,
    'Prospect must not have a pending send or be active in a different sequence.',
    {
      pending_send: sendState.pendingSend,
      conflicting_sequence: sendState.conflictingSequence,
      requested_sequence: sequenceName,
      error: sendState.error || null,
    }
  ));

  const failures = checks.filter(item => !item.passed);
  return {
    sendable: failures.length === 0,
    client_id: client?.id ?? null,
    prospect_id: prospect?.id ?? null,
    sequence: templateSequenceName,
    checks,
    failures,
  };
}

module.exports = {
  CLIENT_SEQUENCE_MAP,
  evaluateSendingReadiness,
  exactSequenceName,
  getBrevoState,
};
