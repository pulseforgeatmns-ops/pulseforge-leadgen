'use strict';

/**
 * SPEC-151 — Identity Reasoning Layer.
 * Converts operator questions into operating-model queries and synthesizes
 * responses from structured concepts — not retrieved paragraphs.
 */

const { THINKING_MODES } = require('../operatorCognition/ThinkingModes');
const {
  OPERATING_MODEL,
  getRelationship,
  listSpecialistNames,
} = require('./OperatingModel');
const { assertIdentityCompliance, composeWorkspaceIntroduction, operatingModeLabel } = require('./MaxIdentity');

const REASONING_TARGETS = Object.freeze({
  ROLE: 'role',
  WHY: 'why',
  PURPOSE: 'purpose',
  PRINCIPLES: 'principles',
  BOUNDARIES: 'boundaries',
  AUTHORITY: 'authority',
  COMPARE: 'compare',
  FAILURE_MODES: 'failure_modes',
  RELATIONSHIPS: 'relationships',
  DELEGATION: 'delegation',
  OPERATOR_DECISIONS: 'operator_decisions',
  SPECIALIST_SEPARATION: 'specialist_separation',
});

function normalizeText(value) {
  return String(value || '').replace(/\s+/g, ' ').trim();
}

function capitalizeFirst(text) {
  const s = normalizeText(text);
  if (!s) return s;
  return s.charAt(0).toUpperCase() + s.slice(1);
}

function joinSentences(parts) {
  return parts
    .filter(Boolean)
    .map((part) => {
      const trimmed = normalizeText(part);
      if (!trimmed) return '';
      return /[.!?]$/.test(trimmed) ? trimmed : `${trimmed}.`;
    })
    .join(' ');
}

function parseResolvedQuestion(resolvedQuestion) {
  const rq = normalizeText(resolvedQuestion);
  const whyMatch = rq.match(/^why\(([^)]+)\)$/i);
  if (whyMatch) {
    return { target: REASONING_TARGETS.WHY, subject: whyMatch[1].toLowerCase() };
  }
  const compareMatch = rq.match(/^compare\(([^)]+)\)$/i);
  if (compareMatch) {
    const objects = compareMatch[1].split(',').map((o) => o.trim().toLowerCase()).filter(Boolean);
    return { target: REASONING_TARGETS.COMPARE, objects };
  }
  const explainMatch = rq.match(/^explain\(([^)]+)\)$/i);
  if (explainMatch) {
    return { target: REASONING_TARGETS.WHY, subject: explainMatch[1].split(':')[0].toLowerCase() };
  }
  return null;
}

function classifyDirectQuestion(question) {
  const q = normalizeText(question).toLowerCase();

  if (/\bwhen should i ignore (?:your )?advice\b/.test(q)) {
    return { target: REASONING_TARGETS.FAILURE_MODES, sections: ['failureModes', 'boundaries', 'principles'] };
  }
  if (/\bwhy shouldn'?t scout do your job\b/.test(q) || /\bwhy can'?t scout do (?:your|max'?s?) job\b/.test(q)) {
    return { target: REASONING_TARGETS.COMPARE, objects: ['max', 'scout'], sections: ['relationships', 'why', 'authority'] };
  }
  if (/\bwhy not merge scout into max\b/.test(q) || /\bwhy not combine scout and max\b/.test(q)) {
    return { target: REASONING_TARGETS.SPECIALIST_SEPARATION, sections: ['why', 'relationships', 'principles'] };
  }
  if (/\bwhy (?:does|do) pulseforge separate specialists\b/.test(q) || /\bwhy separate specialists\b/.test(q)) {
    return { target: REASONING_TARGETS.SPECIALIST_SEPARATION, sections: ['why', 'principles'] };
  }
  if (/\bwhy preserve operator authority\b/.test(q) || /\bwhy (?:does|do) (?:the )?operator retain authority\b/.test(q)) {
    return { target: REASONING_TARGETS.AUTHORITY, sections: ['principles', 'authority'] };
  }
  if (/\bwhat should never belong to you\b/.test(q) || /\bwhat (?:do you|does max) never do\b/.test(q)) {
    return { target: REASONING_TARGETS.BOUNDARIES, sections: ['boundaries'] };
  }
  if (/\bwhat decisions require me\b/.test(q) || /\bwhat (?:do i|does the operator) (?:decide|own)\b/.test(q)) {
    return { target: REASONING_TARGETS.OPERATOR_DECISIONS, sections: ['authority'] };
  }
  if (/\bscout vs rex\b/.test(q) || /\bdifference between scout and rex\b/.test(q)) {
    return { target: REASONING_TARGETS.COMPARE, objects: ['scout', 'rex'], sections: ['relationships'] };
  }
  if (/\bmax vs paige\b/.test(q) || /\bdifference between (?:you|max) and paige\b/.test(q)) {
    return { target: REASONING_TARGETS.COMPARE, objects: ['max', 'paige'], sections: ['relationships'] };
  }
  if (/\bmax vs scout\b/.test(q) || /\bdifference between (?:you|max) and scout\b/.test(q)) {
    return { target: REASONING_TARGETS.COMPARE, objects: ['max', 'scout'], sections: ['relationships'] };
  }
  if (/\bwhy\b/.test(q) && /\bscout\b/.test(q) && /\b(?:your|max'?s?) job\b/.test(q)) {
    return { target: REASONING_TARGETS.COMPARE, objects: ['max', 'scout'], sections: ['relationships', 'why', 'authority'] };
  }
  if (/^why\b/.test(q) || /\bwhy (?:that|this|it)\b/.test(q)) {
    return { target: REASONING_TARGETS.WHY, subject: 'identity', sections: ['why', 'purpose'] };
  }
  if (/\b(?:different from|differs from|compare(?:d)? to|vs\.?|versus)\b/.test(q)) {
    const allMatches = q.match(
      new RegExp(`\\b(${listSpecialistNames().join('|')}|max)\\b`, 'gi')
    );
    const objects = [];
    const seen = new Set();
    if (allMatches) {
      for (const match of allMatches) {
        const lower = match.toLowerCase();
        if (!seen.has(lower)) {
          seen.add(lower);
          objects.push(lower);
        }
      }
    }
    if (objects.length < 2) {
      objects.push(objects[0] === 'max' ? 'scout' : 'max');
    }
    return { target: REASONING_TARGETS.COMPARE, objects, sections: ['relationships'] };
  }
  if (/\b(?:should i|when should)\b/.test(q)) {
    return { target: REASONING_TARGETS.FAILURE_MODES, sections: ['failureModes', 'boundaries', 'principles'] };
  }
  if (/\bhow\b/.test(q) && /\b(?:different|work)\b/.test(q)) {
    return { target: REASONING_TARGETS.COMPARE, objects: ['max', 'scout'], sections: ['relationships'] };
  }

  return null;
}

function planOperatingModelQuery(input = {}) {
  const question = normalizeText(input.question);
  const resolvedQuestion = normalizeText(input.resolvedQuestion);
  const conversationIntent = input.conversationIntent || null;
  const continuity = Boolean(conversationIntent && conversationIntent.continuity);

  if (resolvedQuestion) {
    const parsed = parseResolvedQuestion(resolvedQuestion);
    if (parsed) return { ...parsed, via: 'resolved_question', continuity };
  }

  const direct = classifyDirectQuestion(question);
  if (direct) return { ...direct, via: 'direct_classification', continuity };

  if (continuity) {
    if (conversationIntent && conversationIntent.intent === THINKING_MODES.COMPARE) {
      const objects =
        (conversationIntent.compareObjects && conversationIntent.compareObjects.length
          ? conversationIntent.compareObjects
          : ['max', 'scout']);
      return { target: REASONING_TARGETS.COMPARE, objects, via: 'continuity_compare', continuity };
    }
    if (conversationIntent && conversationIntent.intent === THINKING_MODES.EXPLAIN) {
      return { target: REASONING_TARGETS.WHY, subject: 'identity', via: 'continuity_why', continuity };
    }
    if (conversationIntent && conversationIntent.intent === THINKING_MODES.CHALLENGE) {
      return {
        target: REASONING_TARGETS.FAILURE_MODES,
        via: 'continuity_challenge',
        continuity,
        sections: ['failureModes', 'boundaries', 'principles'],
      };
    }
  }

  return null;
}

function synthesizeWhy(subject) {
  const parts = [];
  if (subject === 'identity' || subject === 'max') {
    parts.push(
      capitalizeFirst(OPERATING_MODEL.why[0]),
      capitalizeFirst(OPERATING_MODEL.why[1]),
      capitalizeFirst(OPERATING_MODEL.why[2]),
      capitalizeFirst(OPERATING_MODEL.why[3])
    );
    parts.push(
      'My purpose is to ' +
        OPERATING_MODEL.purpose.slice(0, 3).join(', ').toLowerCase().replace(/,\s([^,]+)$/, ', and $1') +
        '.'
    );
  } else {
    const rel = getRelationship(subject);
    if (rel) {
      parts.push(capitalizeFirst(rel.reasoning));
    }
    parts.push(...OPERATING_MODEL.why.slice(0, 2).map(capitalizeFirst));
  }
  return joinSentences(parts);
}

function synthesizeCompare(objects) {
  const names = (objects || ['max', 'scout']).map((o) => o.toLowerCase());
  const parts = [];

  for (const name of names) {
    const rel = getRelationship(name);
    if (rel) {
      const label = name === 'max' ? 'Max' : capitalizeFirst(name);
      parts.push(`${label} owns ${rel.owns.charAt(0).toLowerCase()}${rel.owns.slice(1)}`);
      parts.push(capitalizeFirst(rel.reasoning));
    }
  }

  if (names.includes('max') && names.length >= 2) {
    const specialist = names.find((n) => n !== 'max');
    const specRel = getRelationship(specialist);
    if (specRel) {
      parts.push(
        `${capitalizeFirst(specialist)} optimizes ${specRel.optimizes.charAt(0).toLowerCase()}${specRel.optimizes.slice(1)}`
      );
      parts.push(
        `Max optimizes ${OPERATING_MODEL.relationships.max.optimizes.charAt(0).toLowerCase()}${OPERATING_MODEL.relationships.max.optimizes.slice(1)}`
      );
    }
  }

  return joinSentences(parts);
}

function synthesizeBoundaries() {
  return joinSentences([
    'These responsibilities never belong to me:',
    ...OPERATING_MODEL.boundaries.map((b) => capitalizeFirst(b)),
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Operator retains'))),
  ]);
}

function synthesizeAuthority(focus = 'all') {
  const parts = [];
  if (focus === 'operator' || focus === 'all') {
    parts.push('You retain final authority over: ' + OPERATING_MODEL.authority.operator.join(', ') + '.');
  }
  if (focus === 'max' || focus === 'all') {
    parts.push('I own: ' + OPERATING_MODEL.authority.max.join(', ') + '.');
  }
  parts.push(
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Operator retains'))),
    'Max never replaces operator judgment.'
  );
  return joinSentences(parts);
}

function synthesizeFailureModes() {
  return joinSentences([
    'You should weigh my advice against your own judgment when:',
    ...OPERATING_MODEL.failureModes.map((f) => capitalizeFirst(f)),
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Fail closed'))),
  ]);
}

function synthesizeSpecialistSeparation() {
  return joinSentences([
    ...OPERATING_MODEL.why.map(capitalizeFirst),
    capitalizeFirst(OPERATING_MODEL.principles.find((p) => p.startsWith('Delegate expertise'))),
  ]);
}

function synthesizeFromQuery(query) {
  if (!query) return null;

  switch (query.target) {
    case REASONING_TARGETS.WHY:
      return synthesizeWhy(query.subject || 'identity');
    case REASONING_TARGETS.COMPARE:
      return synthesizeCompare(query.objects);
    case REASONING_TARGETS.BOUNDARIES:
      return synthesizeBoundaries();
    case REASONING_TARGETS.AUTHORITY:
    case REASONING_TARGETS.OPERATOR_DECISIONS:
      return synthesizeAuthority(query.target === REASONING_TARGETS.OPERATOR_DECISIONS ? 'operator' : 'all');
    case REASONING_TARGETS.FAILURE_MODES:
      return synthesizeFailureModes();
    case REASONING_TARGETS.SPECIALIST_SEPARATION:
      return synthesizeSpecialistSeparation();
    default:
      return null;
  }
}

function shouldUseOperatingModelReasoning(input = {}) {
  const conversationIntent = input.conversationIntent || null;
  const resolvedQuestion = normalizeText(input.resolvedQuestion);
  const question = normalizeText(input.question);

  if (resolvedQuestion && parseResolvedQuestion(resolvedQuestion)) return true;
  if (classifyDirectQuestion(question)) return true;
  if (conversationIntent && conversationIntent.continuity) return true;
  if (conversationIntent && conversationIntent.thinkingMode === 'operating_model_reflection') return true;

  return false;
}

/**
 * Compose identity response by reasoning over the operating model.
 * @param {object} input
 * @returns {string|null}
 */
function composeIdentityReasoning(input = {}) {
  const query = planOperatingModelQuery(input);
  if (!query) return null;

  let prose = synthesizeFromQuery(query);
  if (!prose) return null;

  const session = input.session || null;
  const mode = operatingModeLabel(session);

  if (query.target === REASONING_TARGETS.WHY && query.subject === 'identity' && !query.continuity) {
    const intro = composeWorkspaceIntroduction(session);
    prose = joinSentences([intro, prose, mode]);
  } else if (query.continuity || query.target !== REASONING_TARGETS.WHY) {
    prose = joinSentences([prose, mode]);
  }

  assertIdentityCompliance(prose);
  return prose;
}

function reasoningMetadata(query) {
  if (!query) {
    return {
      operatingModelReflection: false,
      reasoningTarget: null,
      sectionsUsed: [],
    };
  }
  return {
    operatingModelReflection: true,
    reasoningTarget: query.target,
    sectionsUsed: query.sections || [query.target],
    via: query.via || 'operating_model_reasoning',
  };
}

module.exports = {
  REASONING_TARGETS,
  planOperatingModelQuery,
  parseResolvedQuestion,
  classifyDirectQuestion,
  shouldUseOperatingModelReasoning,
  composeIdentityReasoning,
  reasoningMetadata,
  synthesizeFromQuery,
};
