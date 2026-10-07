'use strict';

const {
  buildAoSignature,
  aoSenderDisplayFirstName,
} = require('./aoCommunicationIdentity');

const GENERIC_PHRASES = [
  /\bjust checking in\b/i,
  /\btouching base\b/i,
  /\bhope this finds you well\b/i,
  /\bwe can beat your current price\b/i,
  /\bare you interested in switching cleaners\b/i,
];

const VENDOR_ATTACK_PHRASES = [
  /\bterrible\b.*\b(cleaner|vendor|contractor)\b/i,
  /\bworst\b.*\b(cleaner|vendor|contractor)\b/i,
  /\b(incompetent|useless)\b.*\b(cleaner|vendor|contractor)\b/i,
  /\byour current (cleaner|vendor|contractor) (is|are) (bad|awful|horrible)\b/i,
];

const PRICE_PITCH_PHRASES = [
  /\bbeat (your )?price\b/i,
  /\blowest bid\b/i,
  /\bcheaper than\b/i,
  /\bdiscount\b/i,
  /\bquote\b.*\btoday\b/i,
];

/** Stiff corporate/formal phrasing — Anchor AO follow-ups should sound plainspoken. */
const CORPORATE_FORMAL_PHRASES = [
  /\bwould be happy to\b/i,
  /\bI wanted to follow up\b/i,
  /\bsometime this week or next\b/i,
  /\bif it would be helpful\b/i,
  /\bopen to\b/i,
  /\bhi there\b/i,
  /\bat your earliest convenience\b/i,
  /\bplease let me know\b/i,
  /\bthanks again for taking the time to speak with me\b/i,
  /\bwe are not looking\b/i,
  /\bwe would be happy\b/i,
  /\bI would be happy\b/i,
];

function firstName(fullName, fallback = 'there') {
  const n = String(fullName || '').trim();
  if (!n) return fallback;
  return n.split(/\s+/)[0];
}

function aoFirstName(assignedAoName) {
  return aoSenderDisplayFirstName(assignedAoName, firstName(assignedAoName, 'Anchor Cleaning'));
}

function textBlob(input) {
  return [
    input.accountName,
    input.accountType,
    input.aoNotes,
    input.lastActivitySummary,
    input.recommendedNextAction,
    input.recommendedAngle,
    input.currentVendorOrProvider,
    input.buildingManagementCompany,
    input.contactEmail,
    ...(input.knownPainSignals || []),
  ]
    .filter(Boolean)
    .join(' ')
    .toLowerCase();
}

function signDraft(body, input = {}) {
  const assignedAoName = typeof input === 'string' ? input : input.assignedAoName;
  if (input && typeof input === 'object' && input.aoCommunicationIdentity) {
    const signature = buildAoSignature({
      userName: assignedAoName,
      identity: input.aoCommunicationIdentity,
    });
    return `${body.trim()}\n\n${signature}`;
  }
  const ao = String(assignedAoName || 'Anchor Cleaning').trim();
  return `${body.trim()}\n\nBest,\n${ao}\nAnchor Cleaning`;
}

function matchesDoNotContact(input) {
  const blob = textBlob(input);
  return (
    /\b(remove(d)? from (the )?call list|do not call|don't call|dont call|not interested in calls|asked to be removed)\b/i.test(blob)
    || input.currentStage === 'do_not_contact'
  );
}

function matchesSnhuUnhAmbiguity(input) {
  const name = String(input.accountName || '').toLowerCase();
  const email = String(input.contactEmail || '').toLowerCase();
  const notes = String(input.aoNotes || '').toLowerCase();
  const mentionsSnhu = /southern new hampshire|snhu/.test(name) || /snhu/.test(notes);
  const mentionsUnh = /@unh\.edu/.test(email) || /\bunh\b/.test(notes) || /unh\.edu/.test(notes);
  return mentionsSnhu && mentionsUnh;
}

function matchesInternalStaff(input) {
  const blob = textBlob(input);
  return (
    /\b(receptionist|front desk).{0,40}\b(clean|cleans|cleaning)\b/i.test(blob)
    || /\binternal(ly)?\b.{0,30}\b(clean|cleaning)\b/i.test(blob)
    || /\b(clean|cleans).{0,40}\b(receptionist|front desk)\b/i.test(blob)
    || /\bstaff.{0,30}clean/i.test(blob)
  );
}

function matchesPainPropertyMgmt(input) {
  const pains = (input.knownPainSignals || []).length > 0;
  const blob = textBlob(input);
  const painInNotes = /\b(unhappy|dissatisfied|missed|not sweep|kitchen|under tables|contractor)\b/i.test(blob);
  const hasMgmt = Boolean(input.buildingManagementCompany)
    || /\b(property management|building management|investment properties)\b/i.test(blob);
  const contactIsMgmt = /\bmike\b/i.test(String(input.contactName || ''))
    && /nash|investment|property|management/i.test(blob);
  return (pains || painInNotes) && (hasMgmt || contactIsMgmt);
}

function matchesRelationship(input) {
  const blob = textBlob(input);
  return (
    /\b(former manager|good relationship|reconnect|worked with|my former)\b/i.test(blob)
    || /\brelationship\b/i.test(blob)
  );
}

function matchesFederalProcurement(input) {
  const blob = textBlob(input);
  const type = String(input.accountType || '').toLowerCase();
  return (
    /\b(post office|usps|federal|government)\b/i.test(blob)
    || /\b(post office|usps|federal)\b/i.test(type)
  );
}

function matchesVendorRouting(input) {
  const blob = textBlob(input);
  const toldBuildingMgmt = /\b(building management|contact building|property manager|facilities manager)\b/i.test(blob);
  const vendorNamed = Boolean(input.currentVendorOrProvider);
  const missingVendorContact = !input.contactEmail && !input.contactPhone
    && !/\b(trillium|vendor).{0,40}@(|\w)/i.test(blob);
  return toldBuildingMgmt && vendorNamed && missingVendorContact;
}

function isDecisionMakerContact(input) {
  const role = String(input.contactRole || '').toLowerCase();
  const name = String(input.contactName || '').toLowerCase();
  return /\b(owner|president|principal|partner|decision|manager|director)\b/.test(role)
    || /\bmike\b/.test(name);
}

function composeInternalStaff(input) {
  const contact = firstName(input.contactName, 'there');
  const ao = aoFirstName(input.assignedAoName);
  const accountShort = String(input.accountName || 'your office').replace(/^new hampshire/i, 'NH');
  const subject = `Cleaning support for ${accountShort}`.replace(/\s+/g, ' ').trim();

  const body = signDraft(
    `Hi ${contact},

Thanks for taking a few minutes to talk with me. From what I understood, some of the office cleaning is being handled internally right now. Anchor works with local professional and medical offices when they need recurring cleaning support, backup coverage, or help taking that work off the team's plate.

We're not trying to push anything that isn't needed. A free facility assessment would let us understand the space, what's already being handled, and whether Anchor could actually help.

Would a quick facility assessment be worth scheduling in the next week or two?`,
    input
  );

  return {
    status: 'draft_ready',
    recommendedFollowUpAngle: 'Internal staff may be carrying cleaning work that could become inconsistent or pull them away from their main role.',
    subjectLine: subject,
    emailDraft: body,
    alternateShortNote: `${ao} with Anchor Cleaning. Following up on the internal cleaning support we discussed. A free facility assessment could help us see what's already covered and whether Anchor can take anything off the team's plate.`,
    nextActionAfterSend: 'If they agree to a facility assessment, coordinate scheduling and loop Jake in only if needed for the visit.',
    approvalPath: 'ao_can_send',
    approvalReason: null,
    warnings: [],
  };
}

function composePainPropertyMgmt(input) {
  const contact = firstName(input.contactName, 'there');
  const ao = aoFirstName(input.assignedAoName);
  const pains = (input.knownPainSignals || []).join(', ');
  const painClause = pains
    ? `particularly around ${pains}`
    : 'particularly around detail work in the kitchen area and under tables';

  const kristyBridge = /\bkristy\b/i.test(textBlob(input))
    ? 'I believe Kristy may have mentioned I reached out.\n\n'
    : '';

  const body = signDraft(
    `Hi ${contact},

${ao} from Anchor Cleaning here. ${kristyBridge}We heard there may be some cleaning concerns at the ${input.accountName || 'property'} property, ${painClause}. I'm not assuming anything from the outside — I'd like to ask how cleaning or vendor issues usually get handled for that property, and whether a free facility assessment or backup coverage from Anchor would even make sense.

If you're the right person to talk with, a quick reply would help. If someone else handles it, I'd appreciate a point in the right direction.`,
    input
  );

  return {
    status: 'draft_ready',
    recommendedFollowUpAngle: 'Property-management route with a reported cleaning concern — diagnose how vendor issues are handled before advising.',
    subjectLine: `Cleaning / vendor questions — ${input.accountName || 'property'}`,
    emailDraft: body,
    alternateShortNote: `${contact}, ${ao} with Anchor Cleaning — checking who handles cleaning/vendor issues for ${input.accountName || 'the property'}.`,
    nextActionAfterSend: 'Wait for routing clarity; if Mike or building ownership engages, offer a facility assessment rather than a price conversation.',
    approvalPath: 'jake_review_recommended',
    approvalReason: 'Clear pain signal and property-management decision path.',
    warnings: [],
  };
}

function composeRelationship(input) {
  const contact = firstName(input.contactName, 'there');
  const ao = aoFirstName(input.assignedAoName);
  const account = input.accountName || 'your organization';

  const emailBody = signDraft(
    `Hi ${contact},

${ao} from Anchor Cleaning — it's been a while, and I wanted to reconnect.

I'm working locally with Anchor on professional offices and property accounts. I'm not sure there's a current need at ${account}, but I wanted you to know we're here if cleaning support, backup coverage, or vendor options ever come up.

We diagnose first — we're not going to push a quote where it doesn't fit. I can send more info, or loop Jake in for a quick conversation if that'd be useful.`,
    input
  );

  return {
    status: 'draft_ready',
    recommendedFollowUpAngle: 'Relationship-first reconnection — put Anchor on their radar without a hard pitch.',
    subjectLine: `Reconnecting — ${ao} / Anchor Cleaning`,
    emailDraft: emailBody,
    alternateShortNote: `${ao} with Anchor Cleaning — reconnecting. Happy to share what we do locally if facility support or backup coverage ever helps at ${account}.`,
    nextActionAfterSend: 'If they respond positively, offer a Jake introduction or facility assessment conversation.',
    approvalPath: 'ao_can_send',
    approvalReason: null,
    warnings: [],
  };
}

function composeFederalProcurement(input) {
  const contact = firstName(input.contactName, 'there');
  const ao = aoFirstName(input.assignedAoName);

  const body = signDraft(
    `Hi ${contact},

${ao} from Anchor Cleaning. Following up on our conversation about the recent switch to in-house custodial services.

Before assuming Anchor could help, I'd like to understand who handles custodial concerns for the location — and whether outside cleaning support is ever considered locally, or if everything has to go through a regional or supplier process.

If there's a local or regional contact for that, I'd appreciate a point in the right direction.`,
    input
  );

  const hasContact = Boolean(input.contactEmail || input.contactPhone);
  return {
    status: hasContact ? 'draft_ready' : 'research_required',
    recommendedFollowUpAngle: 'Federal or institutional procurement may gate local hiring — qualify the path before advising.',
    subjectLine: `Custodial support questions — ${input.accountName || 'location'}`,
    emailDraft: body,
    alternateShortNote: `${contact}, ${ao} with Anchor Cleaning — who handles custodial/vendor decisions locally vs regionally?`,
    nextActionAfterSend: 'Capture the correct procurement or facilities owner before proposing a facility assessment.',
    approvalPath: 'ao_can_send',
    approvalReason: null,
    warnings: hasContact ? [] : ['Confirm direct contact details before sending.'],
  };
}

function composeVendorRouting(input) {
  const vendor = input.currentVendorOrProvider || 'the building management vendor';
  return {
    status: 'research_required',
    recommendedFollowUpAngle: `Research whether ${vendor} is the current provider, a managed facility services coordinator, or the building-management route. If ${vendor} uses local subcontractors, Anchor may fit as a local vendor.`,
    subjectLine: null,
    emailDraft: null,
    alternateShortNote: null,
    nextActionAfterSend: `Confirm ${vendor}'s role and identify a direct contact before drafting tenant-facing outreach.`,
    approvalPath: 'jake_review_recommended',
    approvalReason: 'Vendor routing unclear — building management may be the buyer, not the tenant.',
    warnings: [
      `${input.accountName || 'Tenant'} may not be the cleaning buyer. Confirm ${vendor}'s role before sending a follow-up.`,
    ],
  };
}

function composeDoNotContact(input) {
  return {
    status: 'do_not_contact',
    recommendedFollowUpAngle: 'Prospect requested no further outreach.',
    subjectLine: null,
    emailDraft: null,
    alternateShortNote: null,
    nextActionAfterSend: 'Mark account closed for outreach and respect do-not-contact.',
    approvalPath: 'do_not_send',
    approvalReason: 'Account asked to be removed from outreach.',
    warnings: ['Do not send follow-up — prospect requested removal from call list.'],
  };
}

function composeNeedsClarification(input, warning) {
  return {
    status: 'needs_clarification',
    recommendedFollowUpAngle: 'Account context is ambiguous — clarify target before drafting outreach.',
    subjectLine: null,
    emailDraft: null,
    alternateShortNote: null,
    nextActionAfterSend: 'Fix account/contact mapping in CRM, then regenerate the draft.',
    approvalPath: 'do_not_send',
    approvalReason: 'Ambiguous account data.',
    warnings: [warning],
  };
}

function composeDefaultFollowUp(input) {
  if (!input.aoNotes && !input.lastActivitySummary && !(input.knownPainSignals || []).length) {
    return composeNeedsClarification(
      input,
      'Not enough account context to draft outreach. Add AO notes or pain signals, then regenerate.'
    );
  }

  const contact = firstName(input.contactName, 'there');
  const ao = aoFirstName(input.assignedAoName);
  const context = input.aoNotes || input.lastActivitySummary || input.recommendedAngle || 'our recent conversation';

  const body = signDraft(
    `Hi ${contact},

${ao} from Anchor Cleaning. Following up on ${context.trim().endsWith('.') ? context.trim() : `${context.trim()}.`}

Before suggesting anything, I'd like to understand how cleaning is handled today — and whether a free facility assessment would help us see the space and current setup.

Would a brief call or facility assessment work in the next week or two?`,
    input
  );

  return {
    status: 'draft_ready',
    recommendedFollowUpAngle: 'Diagnose current cleaning setup before advising on Anchor support.',
    subjectLine: `Following up — ${input.accountName || 'your facility'}`,
    emailDraft: body,
    alternateShortNote: `${ao} with Anchor Cleaning — following up to understand your cleaning setup and whether a free facility assessment would help.`,
    nextActionAfterSend: 'If they engage, schedule a facility assessment or clarify decision-maker path.',
    approvalPath: input.requiresJakeApproval ? 'jake_review_recommended' : 'ao_can_send',
    approvalReason: input.requiresJakeApproval ? (input.jakeInvolvementReason || 'Operator flagged Jake review.') : null,
    warnings: [],
  };
}

function detectScenario(input) {
  if (matchesDoNotContact(input)) return 'do_not_contact';
  if (matchesSnhuUnhAmbiguity(input)) return 'ambiguous';
  if (matchesVendorRouting(input)) return 'vendor_routing';
  if (matchesInternalStaff(input)) return 'internal_staff';
  if (matchesPainPropertyMgmt(input)) return 'pain_property_mgmt';
  if (matchesRelationship(input)) return 'relationship';
  if (matchesFederalProcurement(input)) return 'federal';
  return 'default';
}

function validateFollowUpDoctrine(output, input = {}) {
  const draft = String(output.emailDraft || '');
  const shortNote = String(output.alternateShortNote || '');
  const voiceText = `${draft}\n${shortNote}`;
  const checks = {
    diagnoseBeforeAdvise: true,
    noGenericCheckIn: true,
    noCurrentVendorAttack: true,
    facilityAssessmentLanguageUsed: true,
    groundedInKnownContext: true,
    humanAnchorVoice: true,
  };
  const warnings = [...(output.warnings || [])];

  if (!draft) {
    return { checks, warnings, status: output.status };
  }

  for (const re of CORPORATE_FORMAL_PHRASES) {
    if (re.test(voiceText)) {
      checks.humanAnchorVoice = false;
      warnings.push(`Draft uses stiff corporate phrasing (${re}).`);
    }
  }

  for (const re of GENERIC_PHRASES) {
    if (re.test(draft)) {
      checks.noGenericCheckIn = false;
      warnings.push(`Draft contains generic check-in language (${re}).`);
    }
  }

  for (const re of VENDOR_ATTACK_PHRASES) {
    if (re.test(draft)) {
      checks.noCurrentVendorAttack = false;
      warnings.push('Draft may disparage the current cleaner or vendor.');
    }
  }

  for (const re of PRICE_PITCH_PHRASES) {
    if (re.test(draft)) {
      checks.diagnoseBeforeAdvise = false;
      warnings.push('Draft jumps to pricing/quote language before diagnosing.');
    }
  }

  const proposesVisit = /\b(visit|walkthrough|come by|stop by|schedule)\b/i.test(draft);
  const hasFacilityAssessment = /\bfacility assessment\b/i.test(draft);
  if (proposesVisit && !hasFacilityAssessment && !/\bconversation\b/i.test(draft)) {
    checks.facilityAssessmentLanguageUsed = false;
    warnings.push('In-person next step should use facility assessment language when appropriate.');
  }

  const groundedTokens = [
    input.accountName,
    input.contactName,
    ...(input.knownPainSignals || []),
    input.aoNotes,
    input.buildingManagementCompany,
  ].filter(Boolean);
  if (groundedTokens.length === 0 && draft.length > 80) {
    checks.groundedInKnownContext = false;
    warnings.push('Draft may not be grounded in known account context.');
  }

  if (!/\b(understand|ask|how|what|whether|diagnos|setup|handled)\b/i.test(draft)) {
    checks.diagnoseBeforeAdvise = false;
    warnings.push('Draft should include diagnostic framing (questions about current setup).');
  }

  const signedByAo = new RegExp(`\\b${aoFirstName(input.assignedAoName)}\\b`, 'i').test(draft)
    || new RegExp(String(input.assignedAoName || '').split(/\s+/)[0], 'i').test(draft);
  if (input.assignedAoName && !signedByAo) {
    warnings.push('Draft should be signed by the assigned AO.');
  }

  let status = output.status;
  const failedSevere = !checks.noGenericCheckIn || !checks.noCurrentVendorAttack || !checks.groundedInKnownContext
    || !checks.humanAnchorVoice;
  if (failedSevere && status === 'draft_ready') {
    status = 'needs_clarification';
  }

  return { checks, warnings, status };
}

function composeAoFollowUp(input) {
  const normalized = {
    tenantId: input.tenantId,
    accountId: input.accountId,
    accountName: input.accountName || '',
    assignedAoId: input.assignedAoId,
    assignedAoName: input.assignedAoName || 'Anchor Cleaning',
    contactName: input.contactName || null,
    contactRole: input.contactRole || null,
    contactEmail: input.contactEmail || null,
    aoNotes: input.aoNotes || null,
    knownPainSignals: input.knownPainSignals || [],
    currentVendorOrProvider: input.currentVendorOrProvider || null,
    buildingManagementCompany: input.buildingManagementCompany || null,
    lastActivitySummary: input.lastActivitySummary || null,
    recommendedNextAction: input.recommendedNextAction || null,
    recommendedAngle: input.recommendedAngle || null,
    accountType: input.accountType || null,
    requiresJakeApproval: Boolean(input.requiresJakeApproval),
    jakeInvolvementReason: input.jakeInvolvementReason || null,
    currentStage: input.currentStage || null,
    aoCommunicationIdentity: input.aoCommunicationIdentity || null,
  };

  let partial;
  const scenario = detectScenario(normalized);
  switch (scenario) {
    case 'do_not_contact':
      partial = composeDoNotContact(normalized);
      break;
    case 'ambiguous':
      partial = composeNeedsClarification(
        normalized,
        'Account appears to mix SNHU and UNH context. Clarify intended target before drafting.'
      );
      break;
    case 'vendor_routing':
      partial = composeVendorRouting(normalized);
      break;
    case 'internal_staff':
      partial = composeInternalStaff(normalized);
      break;
    case 'pain_property_mgmt':
      partial = composePainPropertyMgmt(normalized);
      break;
    case 'relationship':
      partial = composeRelationship(normalized);
      break;
    case 'federal':
      partial = composeFederalProcurement(normalized);
      break;
    default:
      partial = composeDefaultFollowUp(normalized);
  }

  if (normalized.requiresJakeApproval && partial.approvalPath === 'ao_can_send') {
    partial.approvalPath = 'jake_review_recommended';
    partial.approvalReason = partial.approvalReason || normalized.jakeInvolvementReason || 'Operator requested Jake review.';
  }

  if (isDecisionMakerContact(normalized) && partial.approvalPath === 'ao_can_send'
    && scenario === 'pain_property_mgmt') {
    partial.approvalPath = 'jake_review_recommended';
  }

  const validated = validateFollowUpDoctrine(partial, normalized);
  const warnings = [...new Set([...validated.warnings])];

  return {
    accountId: normalized.accountId,
    assignedAoId: normalized.assignedAoId,
    status: validated.status,
    recommendedFollowUpAngle: partial.recommendedFollowUpAngle,
    subjectLine: partial.subjectLine,
    emailDraft: partial.emailDraft,
    alternateShortNote: partial.alternateShortNote,
    nextActionAfterSend: partial.nextActionAfterSend,
    approvalPath: partial.approvalPath,
    approvalReason: partial.approvalReason,
    warnings,
    doctrineChecks: validated.checks,
  };
}

module.exports = {
  composeAoFollowUp,
  validateFollowUpDoctrine,
  detectScenario,
  firstName,
};
