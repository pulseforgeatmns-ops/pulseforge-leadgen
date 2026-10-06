'use strict';

const { CLAIM_TYPES } = require('../stateIngestion/types');
const { normalizeText } = require('../stateIngestion/claimParser');
const {
  EPISTEMIC_CATEGORY,
  ENTITY_KIND,
  CONTACT_ROLE,
  AMBIGUITY_KIND,
  newSituationId,
} = require('./types');
const { evidenceRef, spanFromMatch } = require('./evidence');
const {
  extractTemporalReferences,
  normalizeTemporalPhrase,
  extractTemporalCorrections,
  classifyTemporalRoles,
  hasConditionalFollowUpContext,
  TEMPORAL_ROLE,
  DAY_TOKEN,
} = require('./temporal');
const { resolvePronoun, applyRoleCorrection } = require('./referenceResolution');
const {
  canonicalAccountLabel,
  resolveAccountReference,
  extractShorthandAccountReference,
  collectKnownAccounts,
} = require('./accountResolution');
const {
  painPointDurable,
  decisionMakerDurable,
  classifyDissatisfaction,
  isInferenceUtterance,
} = require('./durableFactGuard');
function cleanAccountName(name) {
  const normalized = normalizeText(name);
  const trimmed = normalized.split(/[.,;]/)[0].trim();
  const canonical = canonicalAccountLabel(trimmed);
  return canonical.replace(/\s+(Mike|Sarah|Dave|Lisa|Tony|at)$/i, '').trim();
}

const CONTACT_INTRO_SKIP = new Set([
  'exeter', 'granite', 'abc', 'never', 'packaging', 'manufacturing', 'state', 'phillips',
]);

function capitalizeName(fragment) {
  const n = normalizeText(fragment);
  if (!n) return n;
  return n.split(/\s+/).map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
}

function extractIntroducedContacts(raw) {
  const out = [];
  const seen = new Set();
  const re = /\b(?:talked|spoke|spoken|chat(?:ted)?|met|caught)\s+(?:to|with)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)?)\b/g;
  let m;
  while ((m = re.exec(raw)) !== null) {
    const name = capitalizeName(m[1]);
    const firstToken = name.split(/\s+/)[0].toLowerCase();
    if (CONTACT_INTRO_SKIP.has(firstToken)) continue;
    if (/^(Dave|Sarah|Lisa|Mike|Tony|Rory|Jake|Billy|Facilities)$/i.test(name.split(/\s+/)[0])) {
      const key = name.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(name);
    } else if (/^[A-Z][a-z]+$/.test(name) && name.length >= 3 && !CONTACT_INTRO_SKIP.has(name.toLowerCase())) {
      const key = name.toLowerCase();
      if (seen.has(key)) continue;
      seen.add(key);
      out.push(name);
    }
  }
  return out;
}

function resolveExplicitCallbackSpeaker(lower) {
  const m = lower.match(/\b(dave|sarah|lisa|mike|billy)\b[^.!?]{0,60}\b(?:hear back|call me|expects to call|should call|said that i should hear)/i)
    || lower.match(/\b(?:hear back|call me)[^.!?]{0,20}\b(?:from|with)\s+(dave|sarah|lisa|mike|billy)\b/i);
  if (!m) return null;
  const name = m[1] || m[2];
  return capitalizeName(name);
}

function entityAccount(name, threadId) {
  return {
    id: newSituationId('acct'),
    kind: ENTITY_KIND.ACCOUNT,
    name: cleanAccountName(name),
    threadId,
  };
}

function entityContact({ name, title = null, role = CONTACT_ROLE.UNKNOWN, gender = null, accountName = null, epistemic = EPISTEMIC_CATEGORY.CONFIRMED }) {
  return {
    id: newSituationId('contact'),
    kind: ENTITY_KIND.CONTACT,
    name: normalizeText(name),
    title,
    role,
    gender,
    accountName,
    epistemic,
    decisionMaker: role === CONTACT_ROLE.DECISION_MAKER,
  };
}

function interpretThreadSegment({ text, threadId, inputId, memory, now = new Date(), accountHint = null, contextAccounts = [] }) {
  const rawOriginal = String(text || '');
  const raw = normalizeText(text);
  const lower = raw.toLowerCase();
  const entities = [];
  const claims = [];
  const events = [];
  const painPoints = [];
  const objections = [];
  const commitments = [];
  const decisionMakerSignals = [];
  const questions = [];
  const requestedActions = [];
  const commentary = [];
  const corrections = [];
  const ambiguities = [];
  const evidence = [evidenceRef({ inputId, textSpan: { text: rawOriginal.slice(0, 240) } })];

  let account = null;
  if (accountHint) {
    account = entityAccount(accountHint, threadId);
    entities.push(account);
  }

  const accountNameCandidates = [];
  const atCompany = raw.match(
    /\bat\s+(Exeter Phillips|Exeter Packaging|ABC Manufacturing|Granite State Daycare|Granite State Plastics|[A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z][A-Za-z0-9&.'\-]+){0,4})\b/
  );
  if (atCompany?.[1]) accountNameCandidates.push(cleanAccountName(atCompany[1]));
  const stopped = raw.match(
    /\b(?:stopped at|visited|left)\s+(Exeter Phillips|Exeter Packaging|ABC Manufacturing|Granite State Daycare|Granite State Plastics|[A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z][A-Za-z0-9&.'\-]+){0,4})\b/i
  );
  if (stopped?.[1]) accountNameCandidates.push(cleanAccountName(stopped[1]));
  const talkedToCompany = raw.match(
    /\btalked to\s+(?:Exeter Phillips|Exeter Packaging|ABC Manufacturing|[A-Z][A-Za-z0-9&.'\-]+(?:\s+[A-Z][A-Za-z0-9&.'\-]+){0,4})\b/i
  );
  if (talkedToCompany?.[0]) {
    const name = normalizeText(talkedToCompany[0].replace(/^talked to\s+/i, ''));
    if (!/^(Dave|Sarah|Lisa|Mike|Tony|Rory|Jake)$/i.test(name)) accountNameCandidates.push(name);
  }
  const knownAccount = raw.match(
    /\b(Exter Phillips|Exter philips|exter philips|Exeter Phillips|Exeter Packaging|ABC Manufacturing|Granite State Plastics|Granite State Daycare|Never Scouted LLC)\b/i
  );
  if (knownAccount?.[1]) accountNameCandidates.push(cleanAccountName(knownAccount[1]));
  const typoAt = lower.match(/\bat\s+(exter philips|exter phillips|exter philip)\b/);
  if (typoAt) accountNameCandidates.push(cleanAccountName(typoAt[1]));

  const shorthand = extractShorthandAccountReference(raw);
  if (shorthand) {
    const resolved = resolveAccountReference({
      phrase: shorthand,
      memory,
      contextAccounts,
    });
    if (resolved.ambiguous && resolved.ambiguity) {
      ambiguities.push(resolved.ambiguity);
    } else if (resolved.account) {
      accountNameCandidates.push(resolved.account);
    }
  }

  const chosenAccount = accountNameCandidates.find(n => n.length >= 4 && !/^(Dave|Sarah|Lisa|Mike)$/i.test(n));
  if (chosenAccount) {
    account = entityAccount(chosenAccount, threadId);
    if (!entities.find(e => e.kind === ENTITY_KIND.ACCOUNT)) entities.push(account);
  }

  let clarificationContinuation = null;
  if (!account && memory?.resolveClarificationAnswer) {
    clarificationContinuation = memory.resolveClarificationAnswer(raw);
    if (clarificationContinuation?.account) {
      account = entityAccount(clarificationContinuation.account, threadId);
      if (!entities.find(e => e.kind === ENTITY_KIND.ACCOUNT)) entities.push(account);
    }
  }

  const aoMatch = raw.match(/\b(Tony|Rory|Jake)\b/i);
  if (aoMatch) {
    entities.push({
      id: newSituationId('ao'),
      kind: ENTITY_KIND.AO,
      name: aoMatch[1],
      threadId,
    });
  }

  const contactPatterns = [
    { re: /\bDave\b/i, name: 'Dave', gender: 'male' },
    { re: /\bBilly\b/i, name: 'Billy', gender: 'male' },
    { re: /\bSarah Collins\b/i, name: 'Sarah Collins', gender: 'female' },
    { re: /\bSarah\b(?!\s+said\s+Lisa)/i, name: 'Sarah', gender: 'female' },
    { re: /\bLisa\b/i, name: 'Lisa', gender: 'female' },
    { re: /\bMike\b/i, name: 'Mike', gender: 'male' },
  ];
  for (const { re, name, gender } of contactPatterns) {
    if (re.test(raw)) {
      let role = CONTACT_ROLE.UNKNOWN;
      if (/front desk|up front|at the desk|woman at the desk/i.test(lower) && /sarah/i.test(name)) {
        role = CONTACT_ROLE.GATEKEEPER;
      }
      entities.push(entityContact({
        name,
        gender,
        accountName: account?.name || null,
        role,
      }));
    }
  }

  for (const introName of extractIntroducedContacts(raw)) {
    if (entities.some(e => e.kind === ENTITY_KIND.CONTACT && e.name?.toLowerCase() === introName.toLowerCase())) continue;
    entities.push(entityContact({
      name: introName,
      gender: /^(Dave|Mike|Billy|Jake|Rory|Tony)$/i.test(introName) ? 'male' : /^(Sarah|Lisa)$/i.test(introName) ? 'female' : null,
      accountName: account?.name || null,
    }));
  }

  if (/facilities guy|facilities contact|facilities manager/i.test(lower) && !/don'?t have his name|out until/i.test(lower)) {
    entities.push(entityContact({
      name: 'Facilities contact',
      role: CONTACT_ROLE.SUSPECTED_DECISION_MAKER,
      accountName: account?.name || null,
      epistemic: EPISTEMIC_CATEGORY.INFERRED,
    }));
  }

  if (/talked|spoke|conversation|stopped|visit|left/i.test(lower)) {
    events.push({
      id: newSituationId('evt'),
      kind: /stopped|visit|left/i.test(lower) ? 'in_person_visit' : 'conversation',
      accountName: account?.name || null,
      epistemic: EPISTEMIC_CATEGORY.CONFIRMED,
    });
  }

  const dissatisfaction = classifyDissatisfaction(raw);
  if (dissatisfaction?.commentary) {
    commentary.push({ text: raw.match(/[^.!?]+[.!?]/)?.[0] || raw.slice(0, 120), kind: 'inference' });
  }

  if (/nice lobby|traffic sucked|traffic was bad/i.test(lower)) {
    commentary.push({ text: raw.match(/nice lobby[^.]*|traffic sucked[^.]*/i)?.[0] || 'Visit commentary', kind: 'ambient' });
  }

  if (/don'?t have any issue with bathrooms anymore|no issue with bathrooms anymore/i.test(lower)) {
    painPoints.push({
      id: newSituationId('pain'),
      category: 'cleaning_service',
      description: 'Bathroom cleaning concern (resolved)',
      current: false,
      historical: true,
      epistemic: EPISTEMIC_CATEGORY.REPORTED,
      evidence: evidenceRef({ inputId, textSpan: { text: rawOriginal.slice(0, 120) } }),
    });
  }

  if (/missing(?:\s+\w+){0,4}\s+common areas|missing common areas|miss(?:ing)? (?:the )?bathrooms|bathroom issues|issues with bathrooms|hit or miss|unreliable internal cleaning|keep missing|cleener still missin/i.test(lower)) {
    const isHistorical = /used to have issues|but said it'?s been fine lately|been fine lately|used to have/i.test(lower);
    const resolvedHistorical = /don'?t have any issue with bathrooms anymore|no issue with bathrooms anymore|anymore/i.test(lower)
      && /bathroom/i.test(lower);
    const negatedSatisfaction = /not unhappy|are not unhappy|aren'?t unhappy/i.test(lower);
    if (!negatedSatisfaction && !resolvedHistorical) {
      painPoints.push({
        id: newSituationId('pain'),
        category: 'cleaning_service',
        description: /common areas/i.test(lower)
          ? 'Current cleaner missing common areas'
          : /bathroom/i.test(lower)
            ? 'Bathroom cleaning inconsistent'
            : 'Cleaning reliability concern',
        current: !isHistorical,
        historical: isHistorical,
        epistemic: isHistorical ? EPISTEMIC_CATEGORY.REPORTED : EPISTEMIC_CATEGORY.CONFIRMED,
        evidence: evidenceRef({ inputId, textSpan: { text: rawOriginal.slice(0, 120) } }),
      });
    } else if (resolvedHistorical) {
      painPoints.push({
        id: newSituationId('pain'),
        category: 'cleaning_service',
        description: 'Bathroom cleaning concern (resolved)',
        current: false,
        historical: true,
        epistemic: EPISTEMIC_CATEGORY.REPORTED,
        evidence: evidenceRef({ inputId, textSpan: { text: rawOriginal.slice(0, 120) } }),
      });
    }
  }

  if (/already have (?:a )?cleaner|already have somebody cleaning|we'?re under contract|corporate handles|happy with who we use|we already have a cleaner/i.test(lower)) {
    objections.push({
      id: newSituationId('obj'),
      kind: /under contract/i.test(lower) ? 'contract' : /corporate handles/i.test(lower) ? 'corporate' : /happy with/i.test(lower) ? 'incumbent_satisfaction' : 'incumbent_provider',
      text: raw.match(/already have[^.]+|under contract[^.]+|corporate handles[^.]+|happy with[^.]+/i)?.[0] || 'Incumbent provider',
      epistemic: EPISTEMIC_CATEGORY.REPORTED,
    });
  }

  if (/i think .+ might handle|i think .+ handles|pretty sure .+ handles/i.test(lower)) {
    const m = lower.match(/(?:i think|pretty sure) (\w+) (?:might )?handle/i);
    if (m) {
      const uncertain = /didn'?t confirm|not confirm|might/i.test(lower);
      decisionMakerSignals.push({
        id: newSituationId('dm'),
        contactName: m[1].charAt(0).toUpperCase() + m[1].slice(1),
        role: CONTACT_ROLE.SUSPECTED_DECISION_MAKER,
        epistemic: uncertain ? EPISTEMIC_CATEGORY.UNCERTAIN : EPISTEMIC_CATEGORY.UNCERTAIN,
      });
    }
  }

  if (/(\w+) thinks (\w+) might be the person who handles/i.test(lower)) {
    const m = lower.match(/(\w+) thinks (\w+) might be the person who handles/i);
    if (m) {
      decisionMakerSignals.push({
        id: newSituationId('dm'),
        contactName: m[2].charAt(0).toUpperCase() + m[2].slice(1),
        role: CONTACT_ROLE.SUSPECTED_DECISION_MAKER,
        epistemic: EPISTEMIC_CATEGORY.UNCERTAIN,
        reportedBy: m[1].charAt(0).toUpperCase() + m[1].slice(1),
      });
    }
  }

  const confirmedDm = lower.match(/\b(dave|lisa|mike|sarah)\b[^.]{0,40}\bis the decision maker\b/i)
    || lower.match(/\b(dave|lisa|mike|sarah)\s+is the decision maker\b/i);
  if (confirmedDm) {
    const name = confirmedDm[1].charAt(0).toUpperCase() + confirmedDm[1].slice(1);
    decisionMakerSignals.push({
      id: newSituationId('dm'),
      contactName: name,
      role: CONTACT_ROLE.DECISION_MAKER,
      epistemic: EPISTEMIC_CATEGORY.CONFIRMED,
    });
    entities.push(entityContact({
      name,
      role: CONTACT_ROLE.DECISION_MAKER,
      epistemic: EPISTEMIC_CATEGORY.CONFIRMED,
    }));
  }

  if (/(\w+) said (\w+) actually doesn'?t handle vendors/i.test(lower)) {
    const m = lower.match(/(\w+) said (\w+) actually doesn'?t handle vendors/i);
    if (m) {
      corrections.push({
        id: newSituationId('corr'),
        kind: 'decision_maker_role',
        contactName: m[2].charAt(0).toUpperCase() + m[2].slice(1),
        priorValue: CONTACT_ROLE.DECISION_MAKER,
        newValue: CONTACT_ROLE.INFLUENCER,
        text: m[0],
        reportedBy: m[1].charAt(0).toUpperCase() + m[1].slice(1),
      });
    }
  }

  if (/(\w+) (?:told me|said) (\w+) handles/i.test(lower)) {
    const m = lower.match(/(\w+) (?:told me|said) (\w+) handles/i);
    if (m) {
      const reporter = m[1].charAt(0).toUpperCase() + m[1].slice(1);
      const subject = m[2].charAt(0).toUpperCase() + m[2].slice(1);
      decisionMakerSignals.push({
        id: newSituationId('dm'),
        contactName: subject,
        role: CONTACT_ROLE.SUSPECTED_DECISION_MAKER,
        epistemic: EPISTEMIC_CATEGORY.REPORTED,
        reportedBy: reporter,
      });
      const reporterEntity = entities.find(e => e.name?.toLowerCase() === reporter.toLowerCase());
      if (reporterEntity) reporterEntity.role = CONTACT_ROLE.GATEKEEPER;
    }
  }

  if (/director|handles vendors|vendor decisions/i.test(lower) && /lisa/i.test(lower)) {
    const uncertain = /maybe|or maybe|think/i.test(lower);
    decisionMakerSignals.push({
      id: newSituationId('dm'),
      contactName: 'Lisa',
      role: CONTACT_ROLE.SUSPECTED_DECISION_MAKER,
      epistemic: uncertain ? EPISTEMIC_CATEGORY.UNCERTAIN : EPISTEMIC_CATEGORY.REPORTED,
      reportedBy: /sarah/i.test(lower) ? 'Sarah' : null,
    });
  }

  const mixedEntityCorrection = raw.match(
    /(\w+) at (Exeter Phillips|Exeter Packaging)\s+said[^.]*\.\s*actually,?\s+that was (\w+) at (Exeter Phillips|Exeter Packaging)/i
  );
  if (mixedEntityCorrection) {
    corrections.push({
      id: newSituationId('corr'),
      kind: 'entity_reassignment',
      priorValue: `${mixedEntityCorrection[1]} @ ${mixedEntityCorrection[2]}`,
      newValue: `${mixedEntityCorrection[3]} @ ${mixedEntityCorrection[4]}`,
      text: mixedEntityCorrection[0],
    });
    account = entityAccount(mixedEntityCorrection[4], threadId);
    entities.push(entityContact({
      name: mixedEntityCorrection[3],
      accountName: account.name,
      gender: /mike/i.test(mixedEntityCorrection[3]) ? 'male' : null,
    }));
    for (const stale of entities.filter(e => e.kind === ENTITY_KIND.CONTACT && e.name === mixedEntityCorrection[1])) {
      stale.superseded = true;
    }
  }

  const correctionAccount = raw.match(/(?:wasn'?t|not)\s+(Exeter Phillips|Exeter Packaging)[^.]*(?:was|it was)\s+(Exeter Phillips|Exeter Packaging)/i);
  if (correctionAccount) {
    corrections.push({
      id: newSituationId('corr'),
      kind: 'account_identity',
      priorValue: correctionAccount[1],
      newValue: correctionAccount[2],
      text: correctionAccount[0],
    });
    account = entityAccount(correctionAccount[2], threadId);
    entities.push(account);
  }

  const dayMatches = [...raw.matchAll(DAY_TOKEN)].map(m => m[0]);
  let temporalRefs = extractTemporalReferences(raw, now);
  const temporalCorrectionPairs = extractTemporalCorrections(raw, dayMatches);
  if (temporalCorrectionPairs.length) {
    const finalPhrase = dayMatches[dayMatches.length - 1];
    const finalDay = finalPhrase.match(/(monday|tuesday|wednesday|thursday|friday|saturday|sunday)/i)?.[1];
    for (const pair of temporalCorrectionPairs) {
      corrections.push({
        id: newSituationId('corr'),
        kind: 'temporal',
        priorValue: pair.priorValue,
        newValue: pair.newValue,
        text: raw,
      });
    }
    for (const t of temporalRefs) {
      if (finalDay && t.phrase.toLowerCase().includes(finalDay.toLowerCase())) {
        t.canonical = true;
        t.superseded = temporalCorrectionPairs.map(p => p.priorValue).join(', ');
        t.role = TEMPORAL_ROLE.CORRECTED_TIME;
      } else if (temporalCorrectionPairs.some(p => t.phrase.toLowerCase().includes(String(p.priorValue).toLowerCase().split(/\s+/)[0]))) {
        t.superseded = true;
      }
    }
  }
  temporalRefs = classifyTemporalRoles(raw, temporalRefs);

  const hearBackDeadline = /hear back|should hear back|expects (?:to )?call|call me|he'?d call|she'?d call|should call|supposed to call|come back|\bsaid\s+(?:monday|tuesday|wednesday|thursday|friday|saturday|sunday)\b/i.test(lower);
  const deferredCallback = clarificationContinuation?.deferred?.intent === 'callback' || hearBackDeadline;
  if (deferredCallback) {
    const deferredPhrase = clarificationContinuation?.deferred?.temporalPhrase;
    const deadlineRef = temporalRefs.find(t => t.role === TEMPORAL_ROLE.DEADLINE && !t.superseded)
      || temporalRefs.find(t => !t.superseded && /friday|thursday|wednesday|monday|tuesday|saturday|sunday|this week/i.test(t.phrase));
    const windowPhrase = deferredPhrase
      || deadlineRef?.phrase
      || lower.match(/this week|friday|thursday|wednesday morning|next week|monday|tuesday|wednesday|saturday|sunday/)?.[0]
      || 'unspecified';
    const resolved = normalizeTemporalPhrase(windowPhrase, now);
    const explicitSpeaker = resolveExplicitCallbackSpeaker(lower);
    commitments.push({
      id: newSituationId('commit'),
      kind: 'callback',
      responsible: explicitSpeaker || 'unresolved',
      sourceContact: explicitSpeaker || null,
      sourceContactUnresolved: !explicitSpeaker,
      windowPhrase,
      normalized: resolved.normalized,
      epistemic: EPISTEMIC_CATEGORY.REPORTED,
      accountName: account?.name || null,
      temporalRole: TEMPORAL_ROLE.DEADLINE,
    });
  }

  if (/facilities guy is out until|out until (\w+day)/i.test(lower)) {
    const m = lower.match(/out until (\w+day)/i);
    commitments.push({
      id: newSituationId('commit'),
      kind: 'availability',
      responsible: 'Facilities contact',
      windowPhrase: m ? m[1] : 'Thursday',
      normalized: normalizeTemporalPhrase(m ? m[1] : 'Thursday', now).normalized,
      epistemic: EPISTEMIC_CATEGORY.REPORTED,
      accountName: account?.name || null,
    });
  }

  if (/\?\s*$/.test(raw) || /\bshould i\b/i.test(lower)) {
    questions.push({
      id: newSituationId('q'),
      text: raw.match(/[^.?!]*\?[^.?!]*/)?.[0]?.trim() || raw,
      kind: /\bshould i wait\b/i.test(lower) ? 'advisory' : 'general',
    });
  }

  if (/put a follow[- ]?up on|schedule follow[- ]?up|follow[- ]?up on|remind me to follow[- ]?up/i.test(lower)) {
    const conditional = hasConditionalFollowUpContext(lower);
    const known = collectKnownAccounts(memory, contextAccounts);
    if (!account && known.length > 1 && /\b(for them|for that account|for they)\b/i.test(lower)) {
      ambiguities.push({
        kind: AMBIGUITY_KIND.ACTION_TARGET,
        candidates: known,
        clarification: `Which account should receive the follow-up — ${known.join(' or ')}?`,
      });
    }
    const followRef = temporalRefs.find(t => t.role === TEMPORAL_ROLE.CONDITIONAL_FOLLOW_UP_TIME && !t.superseded)
      || (hasConditionalFollowUpContext(lower)
        ? temporalRefs.find(t => /monday|tuesday|wednesday|thursday|friday|saturday|sunday/i.test(t.phrase) && !t.superseded && t.role !== TEMPORAL_ROLE.DEADLINE)
        : null);
    requestedActions.push({
      id: newSituationId('req'),
      action: 'schedule_follow_up',
      temporal: followRef?.phrase || null,
      conditional,
      condition: conditional ? {
        type: 'NO_EXPECTED_EVENT',
        event: 'callback',
        deadline: temporalRefs.find(t => t.role === TEMPORAL_ROLE.DEADLINE)?.phrase || null,
      } : null,
    });
  }

  if (/they still have bathroom issues/i.test(lower) && /should i wait/i.test(lower) && /follow[- ]?up on monday/i.test(lower)) {
    questions.push({
      id: newSituationId('q'),
      text: 'Should I wait?',
      kind: 'advisory',
    });
  }

  if (/update .+ with that|tell me who i should talk to next/i.test(lower)) {
    requestedActions.push({
      id: newSituationId('req'),
      action: /tell me who/i.test(lower) ? 'advisory_next_contact' : 'apply_prior_context_update',
    });
  }

  if (/that sounds promising|really nice|okay so/i.test(lower)) {
    commentary.push({ text: raw.match(/^[^.!?]+[.!?]/)?.[0] || raw.slice(0, 80) });
  }

  if (memory?.durableLoadFailed && /\b(he|she|they)\b/i.test(lower) && !entities.some(e => e.kind === ENTITY_KIND.CONTACT)) {
    const pronoun = raw.match(/\b(he|she|they)\b/i)?.[1] || 'he';
    const resolution = resolvePronoun({
      pronoun,
      memory,
      threadContacts: entities.filter(e => e.kind === ENTITY_KIND.CONTACT),
      accountName: account?.name || null,
    });
    if (resolution.ambiguous && resolution.ambiguity) {
      ambiguities.push(resolution.ambiguity);
    }
  }

  const pronounOnly = /^(?:he|she|they)\b/i.test(raw.trim()) || /^actually,?\s+(?:he|she)\b/i.test(raw.trim());
  if (pronounOnly || /^he'?s not actually|^he isn'?t actually/i.test(lower)) {
    const pronoun = raw.match(/\b(he|she|they)\b/i)?.[1] || 'he';
    const resolution = resolvePronoun({
      pronoun,
      memory,
      threadContacts: entities.filter(e => e.kind === ENTITY_KIND.CONTACT),
      accountName: account?.name || null,
    });
    if (resolution.ambiguous && resolution.ambiguity) {
      ambiguities.push(resolution.ambiguity);
    } else if (resolution.entity) {
      const roleFix = applyRoleCorrection({ contactEntity: resolution.entity, correctionText: raw });
      if (roleFix) {
        corrections.push({
          id: newSituationId('corr'),
          kind: 'decision_maker_role',
          ...roleFix.correction,
          contactName: roleFix.contact.name,
        });
        const idx = entities.findIndex(e => e.id === roleFix.contact.id);
        if (idx >= 0) entities[idx] = roleFix.contact;
        else entities.push(roleFix.contact);
      }
    }
  }

  const namedDmNegation = lower.match(/\b(dave|sarah|lisa|mike|billy)\b[^.!?]{0,40}isn'?t the decision maker/i)
    || lower.match(/\b(dave|sarah|lisa|mike|billy)\s+is not the decision maker/i);
  if (/isn'?t the decision maker|not actually the decision maker|not the decision maker|actually .+ isn'?t the decision maker/i.test(lower)) {
    let contact = namedDmNegation
      ? entities.find(e => e.kind === ENTITY_KIND.CONTACT && e.name?.toLowerCase() === namedDmNegation[1].toLowerCase())
      : null;
    if (!contact) {
      contact = entities.find(e => e.kind === ENTITY_KIND.CONTACT)
        || memory?.lastPrimaryContactForAccount(account?.name);
    }
    if (contact) {
      const roleFix = applyRoleCorrection({ contactEntity: contact, correctionText: raw });
      if (roleFix) {
        corrections.push({
          id: newSituationId('corr'),
          kind: 'decision_maker_role',
          ...roleFix.correction,
          contactName: roleFix.contact.name,
        });
        const idx = entities.findIndex(e => e.id === roleFix.contact.id);
        if (idx >= 0) entities[idx] = roleFix.contact;
      }
    }
  }

  if (/didn'?t get (?:her|his|their) last name|did not get (?:her|his|their) last name/i.test(lower)) {
    const subject = /lisa/i.test(lower) ? 'Lisa' : /dave/i.test(lower) ? 'Dave' : /sarah/i.test(lower) ? 'Sarah' : /billy/i.test(lower) ? 'Billy' : 'Lisa';
    for (const ent of entities.filter(e => e.kind === ENTITY_KIND.CONTACT && e.name?.toLowerCase() === subject.toLowerCase())) {
      ent.lastNameKnown = false;
    }
    claims.push({
      semantic: 'missing_contact_field',
      contactName: subject,
      field: 'last_name',
      known: false,
    });
  }

  if (/talk to his boss|speak with his boss|going to talk to his boss/i.test(lower)) {
    claims.push({
      semantic: 'influencer_escalation',
      contactName: entities.find(e => e.name === 'Dave')?.name || 'contact',
      epistemic: EPISTEMIC_CATEGORY.REPORTED,
    });
  }

  const ingestionClaims = buildIngestionClaims({
    entities,
    events,
    painPoints,
    objections,
    commitments,
    corrections,
    decisionMakerSignals,
    account,
    aoName: entities.find(e => e.kind === ENTITY_KIND.AO)?.name,
    temporalRefs,
    raw,
  });

  return {
    threadId,
    text: raw,
    entities,
    events,
    claims,
    ingestionClaims,
    painPoints,
    objections,
    commitments,
    decisionMakerSignals,
    temporalReferences: temporalRefs,
    questions,
    requestedActions,
    commentary,
    corrections,
    ambiguities,
    evidence,
    accountName: account?.name || null,
  };
}

function buildIngestionClaims({
  entities,
  events,
  painPoints,
  objections,
  commitments,
  corrections,
  decisionMakerSignals = [],
  account,
  aoName,
  temporalRefs,
  raw,
}) {
  const claims = [];
  const sourceThread = { thread_text: raw.slice(0, 500) };

  if (aoName) {
    claims.push({ claim_type: CLAIM_TYPES.AO, payload: { name: aoName }, source_record: sourceThread });
  }
  if (account?.name) {
    claims.push({ claim_type: CLAIM_TYPES.ACCOUNT, payload: { name: account.name }, source_record: sourceThread });
  }

  for (const e of entities.filter(x => x.kind === ENTITY_KIND.CONTACT && x.name !== 'Facilities contact')) {
    claims.push({
      claim_type: CLAIM_TYPES.CONTACT,
      payload: { name: e.name, title: e.title || null, role: e.role },
      source_record: sourceThread,
    });
  }

  for (const evt of events) {
    claims.push({ claim_type: CLAIM_TYPES.EVENT, payload: { kind: evt.kind }, source_record: sourceThread });
  }

  for (const pain of painPoints.filter(p => painPointDurable(p, raw))) {
    claims.push({
      claim_type: CLAIM_TYPES.PAIN_SIGNAL,
      payload: { pain: pain.description, epistemic: pain.epistemic },
      source_record: sourceThread,
    });
  }

  for (const sig of decisionMakerSignals) {
    if (!decisionMakerDurable(sig)) continue;
    if (claims.some(c => c.claim_type === CLAIM_TYPES.CONTACT && c.payload?.name === sig.contactName)) continue;
    claims.push({
      claim_type: CLAIM_TYPES.CONTACT,
      payload: {
        name: sig.contactName,
        role: sig.role,
        epistemic: sig.epistemic,
        reported_by: sig.reportedBy || null,
        decision_maker: true,
      },
      source_record: sourceThread,
    });
  }

  for (const obj of objections) {
    claims.push({
      claim_type: CLAIM_TYPES.SIGNAL,
      payload: { signal: 'objection', detail: obj.kind, epistemic: obj.epistemic },
      source_record: sourceThread,
    });
  }

  for (const commit of commitments.filter(c => c.kind === 'callback')) {
    claims.push({
      claim_type: CLAIM_TYPES.NEXT_EXPECTED_EVENT,
      payload: { kind: 'inbound_call', direction: 'inbound' },
      source_record: sourceThread,
    });
    const window = commit.windowPhrase || 'unspecified';
    claims.push({
      claim_type: CLAIM_TYPES.EXPECTED_WINDOW,
      payload: { window, normalized: commit.normalized || null },
      source_record: sourceThread,
    });
  }

  for (const commit of commitments.filter(c => c.kind === 'availability')) {
    claims.push({
      claim_type: CLAIM_TYPES.EXPECTED_WINDOW,
      payload: { window: commit.windowPhrase, note: 'contact_unavailable_until' },
      source_record: sourceThread,
    });
    claims.push({
      claim_type: CLAIM_TYPES.PARTIAL_FACT,
      payload: { note: `Facilities contact unavailable until ${commit.windowPhrase}` },
      source_record: sourceThread,
    });
  }

  if (/front desk|facilities guy|don'?t have his name|do not have his name/i.test(raw)) {
    claims.push({
      claim_type: CLAIM_TYPES.PARTIAL_FACT,
      payload: { note: 'Decision-maker or facilities contact referenced but not fully identified' },
      source_record: sourceThread,
    });
    claims.push({
      claim_type: CLAIM_TYPES.UNKNOWN_FIELD,
      payload: { field: 'facilities_decision_maker_name', value: 'UNKNOWN' },
      source_record: sourceThread,
    });
  }

  if (/interested|backup coverage|looking for backup/i.test(raw)) {
    claims.push({
      claim_type: CLAIM_TYPES.SIGNAL,
      payload: { signal: /interested/i.test(raw) ? 'interest_expressed' : 'operational_need' },
      source_record: sourceThread,
    });
  }

  if (/follow[- ]?up|awaiting|call this week|active relationship/i.test(raw) || commitments.length) {
    claims.push({
      claim_type: CLAIM_TYPES.PIPELINE_IMPLICATION,
      payload: { state: 'active_relationship_follow_up_pending' },
      source_record: sourceThread,
    });
  }

  for (const corr of corrections) {
    claims.push({
      claim_type: CLAIM_TYPES.OPERATOR_CORRECTION,
      payload: { kind: corr.kind, prior: corr.priorValue, value: corr.newValue },
      source_record: sourceThread,
    });
    if (corr.kind === 'account_identity' && corr.newValue) {
      claims.push({
        claim_type: CLAIM_TYPES.ACCOUNT,
        payload: { name: corr.newValue, corrected: true },
        source_record: sourceThread,
      });
    }
  }

  const canonicalTemporal = temporalRefs.find(t => t.canonical && !t.superseded)
    || temporalRefs.find(t => !t.superseded);
  if (canonicalTemporal && !claims.some(c => c.claim_type === CLAIM_TYPES.EXPECTED_WINDOW)) {
    claims.push({
      claim_type: CLAIM_TYPES.EXPECTED_WINDOW,
      payload: { window: canonicalTemporal.phrase, normalized: canonicalTemporal.normalized },
      source_record: sourceThread,
    });
  }

  if (/put a follow[- ]?up on|call thursday|asked me to call/i.test(raw)) {
    claims.push({
      claim_type: CLAIM_TYPES.NEXT_ACTION,
      payload: { action: 'call', target: entities.find(e => e.kind === ENTITY_KIND.CONTACT)?.name || null },
      source_record: sourceThread,
    });
  }

  const phoneMatch = raw.match(/(?:number is|phone(?:\s+is)?)\s*([+\d().\-x\s]{7,})/i);
  const emailMatch = raw.match(/([a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,})/i);
  if (phoneMatch) {
    claims.push({ claim_type: CLAIM_TYPES.CONTACT, payload: { phone: normalizeText(phoneMatch[1]) }, source_record: sourceThread });
  }
  if (emailMatch) {
    claims.push({ claim_type: CLAIM_TYPES.CONTACT, payload: { email: emailMatch[1].toLowerCase() }, source_record: sourceThread });
  }

  if (!account && /stopped at|visited|new prospect/i.test(raw)) {
    claims.push({
      claim_type: CLAIM_TYPES.PIPELINE_IMPLICATION,
      payload: { state: 'ao_reported_new_prospect' },
      source_record: sourceThread,
    });
  }

  return dedupeClaims(claims);
}

function dedupeClaims(claims) {
  const seen = new Set();
  const out = [];
  for (const claim of claims) {
    const key = `${claim.claim_type}:${JSON.stringify(claim.payload)}`;
    if (seen.has(key)) continue;
    seen.add(key);
    out.push(claim);
  }
  return out;
}

module.exports = {
  interpretThreadSegment,
  buildIngestionClaims,
};
