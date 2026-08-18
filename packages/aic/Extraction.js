'use strict';

/**
 * SPEC-113 Stage 2 — Extraction.
 * Concepts, not summaries. Every concept stores source provenance.
 * Missing fields stay unknown. Nothing is assumed.
 */

const {
  CONCEPT_TYPES,
  CONCEPT_STATUS,
  asText,
  asList,
  slugify,
  splitList,
} = require('./types');

const SECTION_TYPE_RULES = [
  { type: CONCEPT_TYPES.MISSION, re: /\bmission\b/i },
  { type: CONCEPT_TYPES.TRANSFORMATION, re: /\btransformation\b|\bcurrent\s*state\b|\bfuture\s*state\b/i },
  { type: CONCEPT_TYPES.ICP, re: /\bicp\b|\bideal customer\b|\bideal client\b/i },
  { type: CONCEPT_TYPES.DISQUALIFIER, re: /\bdisqualif|\bexclusion|\bwho should not\b|\bavoid\b/i },
  { type: CONCEPT_TYPES.BUYING_TRIGGER, re: /\bbuying (signal|trigger)/i },
  { type: CONCEPT_TYPES.PAIN_CATEGORY, re: /\bpain categor/i },
  { type: CONCEPT_TYPES.PAIN, re: /\bpain\s*[:\-–]|^pain$/i },
  { type: CONCEPT_TYPES.OBSERVABLE_SIGNAL, re: /\bobservable signal|\bsignals?\b/i },
  { type: CONCEPT_TYPES.OBJECTION, re: /\bobjection/i },
  { type: CONCEPT_TYPES.LANGUAGE, re: /\blanguage\b|\bvocabulary\b|\bquote/i },
  { type: CONCEPT_TYPES.EVIDENCE, re: /\bevidence\b|\bproof\b|\bcase study/i },
  { type: CONCEPT_TYPES.CONFIDENCE_RULE, re: /\bconfidence\b|\bcertainty\b|\bunknown/i },
  { type: CONCEPT_TYPES.MESSAGING, re: /\bmessaging\b|\bcta\b|\bcopy\b/i },
];

const LABELED_LINE = /^([A-Za-z][A-Za-z0-9 /_-]{0,40}):\s*(.+)$/;

function classifyHeading(heading) {
  const text = asText(heading);
  for (const rule of SECTION_TYPE_RULES) {
    if (rule.re.test(text)) return { type: rule.type, heading: text };
  }
  return { type: null, heading: text };
}

function splitSections(body) {
  const text = String(body || '').replace(/\r\n/g, '\n');
  const lines = text.split('\n');
  const sections = [];
  let current = { heading: 'Document', body: [] };
  for (const line of lines) {
    const heading = line.match(/^#{1,4}\s+(.+)$/);
    if (heading) {
      if (current.body.length || current.heading !== 'Document') {
        sections.push({
          heading: current.heading,
          body: current.body.join('\n').trim(),
        });
      }
      current = { heading: heading[1].trim(), body: [] };
    } else {
      current.body.push(line);
    }
  }
  sections.push({
    heading: current.heading,
    body: current.body.join('\n').trim(),
  });
  return sections.filter((s) => s.body || s.heading !== 'Document');
}

function labeledFields(body) {
  const fields = {};
  const unlabeled = [];
  for (const raw of String(body || '').split('\n')) {
    const line = raw.trim();
    if (!line) continue;
    const match = line.match(LABELED_LINE);
    if (match) {
      const key = match[1].trim().toLowerCase().replace(/\s+/g, '_');
      fields[key] = asText(match[2]);
    } else if (!line.startsWith('#')) {
      unlabeled.push(line.replace(/^[-*•]\s*/, ''));
    }
  }
  return { fields, unlabeled, prose: unlabeled.join(' ') };
}

function quotes(text) {
  const found = [];
  const re = /"([^"]{8,240})"/g;
  let match;
  while ((match = re.exec(String(text || '')))) found.push(match[1].trim());
  return found;
}

function conceptId(type, label, used) {
  let base = `${type}:${slugify(label)}`;
  if (!used.has(base)) {
    used.add(base);
    return base;
  }
  let i = 2;
  while (used.has(`${base}_${i}`)) i += 1;
  const id = `${base}_${i}`;
  used.add(id);
  return id;
}

function buildConcept({
  id,
  type,
  label,
  statement,
  confidence,
  document,
  section,
  excerpt,
  meta,
}) {
  const text = asText(statement);
  return {
    id,
    type,
    label: asText(label) || asText(type),
    statement: text,
    confidence: Number.isFinite(Number(confidence)) ? Number(confidence) : text ? 0.8 : 0,
    status: CONCEPT_STATUS.PROPOSED,
    provenance: {
      source: 'document',
      documentId: document && document.id,
      documentTitle: document && document.title,
      documentKind: document && document.kind,
      section: asText(section),
      excerpt: asText(excerpt).slice(0, 400),
    },
    evidenceExcerpt: asText(excerpt).slice(0, 400),
    operatorApproval: null,
    meta: meta || {},
  };
}

function painLabelFromHeading(heading) {
  const match = String(heading || '').match(/\bpain(?:\s*category)?\s*[:\-–]\s*(.+)$/i);
  if (match) return asText(match[1]);
  return null;
}

function extractFromSection(section, document, used) {
  const classified = classifyHeading(section.heading);
  const { fields, unlabeled, prose } = labeledFields(section.body);
  const concepts = [];
  const pendingEdges = [];
  const excerpt = (section.body || '').slice(0, 400);

  function add(partial) {
    const label = asText(partial.label);
    if (!label && !partial.statement) return null;
    const concept = buildConcept({
      ...partial,
      id: conceptId(partial.type, label || partial.statement, used),
      document,
      section: section.heading,
      excerpt: partial.excerpt || excerpt,
    });
    concepts.push(concept);
    return concept;
  }

  const painName = painLabelFromHeading(section.heading);
  if (classified.type === CONCEPT_TYPES.PAIN || painName) {
    const label = painName || fields.pain || fields.problem || unlabeled[0] || 'Unnamed pain';
    const statement =
      fields.definition ||
      fields.pain ||
      prose ||
      `Pain identified: ${label}`;
    const pain = add({
      type: CONCEPT_TYPES.PAIN,
      label,
      statement,
      confidence: 0.86,
      excerpt: statement.slice(0, 400),
    });
    const supported = asList(fields.supported_by || fields.supportedby);
    for (const item of supported) {
      const child = add({
        type: CONCEPT_TYPES.PAIN,
        label: item,
        statement: `${item} supports ${label}.`,
        confidence: 0.8,
        excerpt: `Supported by: ${item}`,
      });
      if (pain && child) {
        pendingEdges.push({ from: pain.id, to: child.id, relation: 'supported_by' });
      }
    }
    const observed = asList(
      fields.observed_through || fields.observedthrough || fields.signals || fields.observable_signals
    );
    for (const item of observed) {
      const child = add({
        type: CONCEPT_TYPES.OBSERVABLE_SIGNAL,
        label: item,
        statement: `${label} is observed through ${item}.`,
        confidence: 0.84,
        excerpt: `Observed through: ${item}`,
      });
      if (pain && child) {
        pendingEdges.push({ from: pain.id, to: child.id, relation: 'observed_through' });
      }
    }
    const buying = asList(fields.buying_signals || fields.buying_signal || fields.buying_triggers);
    for (const item of buying) {
      const child = add({
        type: CONCEPT_TYPES.BUYING_TRIGGER,
        label: item,
        statement: `${item} indicates urgency around ${label}.`,
        confidence: 0.78,
        excerpt: `Buying signals: ${item}`,
      });
      if (pain && child) {
        pendingEdges.push({ from: pain.id, to: child.id, relation: 'maps_to' });
      }
    }
    return { concepts, pendingEdges };
  }

  if (classified.type === CONCEPT_TYPES.MISSION || fields.mission) {
    const statement = fields.mission || prose;
    if (statement) {
      add({
        type: CONCEPT_TYPES.MISSION,
        label: 'Mission',
        statement,
        confidence: 0.9,
        excerpt: statement,
      });
    }
  }

  if (classified.type === CONCEPT_TYPES.TRANSFORMATION || fields.current || fields.future) {
    const current = fields.current || fields.current_state;
    const future = fields.future || fields.future_state;
    if (current || future || prose) {
      add({
        type: CONCEPT_TYPES.TRANSFORMATION,
        label: 'Transformation',
        statement: [current && `Current: ${current}`, future && `Future: ${future}`, !current && !future ? prose : '']
          .filter(Boolean)
          .join(' '),
        confidence: current && future ? 0.9 : 0.7,
        excerpt: section.body.slice(0, 400),
        meta: { currentState: current || '', futureState: future || '' },
      });
    }
  }

  if (classified.type === CONCEPT_TYPES.ICP || fields.icp) {
    const statement = fields.icp || prose;
    if (statement) {
      add({
        type: CONCEPT_TYPES.ICP,
        label: 'ICP',
        statement,
        confidence: 0.86,
        excerpt: statement,
        meta: { signals: asList(fields.signals) },
      });
    }
  }

  if (classified.type === CONCEPT_TYPES.DISQUALIFIER || fields.disqualifiers || fields.exclusions) {
    const items = asList(fields.disqualifiers || fields.exclusions || fields.avoid || prose);
    for (const item of items.length ? items : splitList(prose)) {
      add({
        type: CONCEPT_TYPES.DISQUALIFIER,
        label: item,
        statement: `Do not outreach: ${item}.`,
        confidence: 0.88,
        excerpt: item,
      });
    }
  }

  if (classified.type === CONCEPT_TYPES.BUYING_TRIGGER && !painName) {
    const items = asList(fields.buying_signals || fields.buying_triggers || prose);
    for (const item of items) {
      add({
        type: CONCEPT_TYPES.BUYING_TRIGGER,
        label: item,
        statement: `${item} indicates buying urgency.`,
        confidence: 0.76,
        excerpt: item,
      });
    }
  }

  if (classified.type === CONCEPT_TYPES.OBJECTION || fields.objection) {
    const items = asList(fields.objection || fields.objections || unlabeled);
    for (const item of items) {
      add({
        type: CONCEPT_TYPES.OBJECTION,
        label: item.slice(0, 80),
        statement: item,
        confidence: 0.8,
        excerpt: item,
      });
    }
  }

  if (classified.type === CONCEPT_TYPES.LANGUAGE || fields.language) {
    const items = asList(fields.language || fields.vocabulary);
    const quoted = quotes(section.body);
    for (const item of [...items, ...quoted]) {
      add({
        type: CONCEPT_TYPES.LANGUAGE,
        label: item.slice(0, 80),
        statement: item,
        confidence: quoted.includes(item) ? 0.75 : 0.8,
        excerpt: item,
      });
    }
  }

  if (classified.type === CONCEPT_TYPES.EVIDENCE || fields.evidence) {
    const statement = fields.evidence || prose;
    if (statement) {
      add({
        type: CONCEPT_TYPES.EVIDENCE,
        label: 'Evidence',
        statement,
        confidence: 0.82,
        excerpt: statement,
      });
    }
  }

  if (classified.type === CONCEPT_TYPES.CONFIDENCE_RULE) {
    if (prose || fields.confidence) {
      add({
        type: CONCEPT_TYPES.CONFIDENCE_RULE,
        label: 'Confidence',
        statement: fields.confidence || prose,
        confidence: 0.7,
        excerpt: (fields.confidence || prose).slice(0, 400),
        meta: { unknown: fields.unknown || fields.unknowns || '' },
      });
    }
    if (fields.unknown || fields.unknowns) {
      add({
        type: CONCEPT_TYPES.UNKNOWN,
        label: 'Unknown',
        statement: fields.unknown || fields.unknowns,
        confidence: 1,
        excerpt: fields.unknown || fields.unknowns,
      });
    }
  }

  if (classified.type === CONCEPT_TYPES.MESSAGING || fields.messaging || fields.cta) {
    const statement = fields.messaging || fields.cta || prose;
    if (statement) {
      add({
        type: CONCEPT_TYPES.MESSAGING,
        label: fields.cta ? 'CTA' : 'Messaging',
        statement,
        confidence: 0.8,
        excerpt: statement,
      });
    }
  }

  if (classified.type === CONCEPT_TYPES.OBSERVABLE_SIGNAL && !painName) {
    const items = asList(fields.signals || fields.observable_signals || unlabeled);
    for (const item of items) {
      add({
        type: CONCEPT_TYPES.OBSERVABLE_SIGNAL,
        label: item,
        statement: item,
        confidence: 0.8,
        excerpt: item,
      });
    }
  }

  if (fields.mission && !concepts.some((c) => c.type === CONCEPT_TYPES.MISSION)) {
    add({
      type: CONCEPT_TYPES.MISSION,
      label: 'Mission',
      statement: fields.mission,
      confidence: 0.88,
      excerpt: fields.mission,
    });
  }
  if ((fields.messaging || fields.cta) && !concepts.some((c) => c.type === CONCEPT_TYPES.MESSAGING)) {
    add({
      type: CONCEPT_TYPES.MESSAGING,
      label: fields.cta ? 'CTA' : 'Messaging',
      statement: fields.messaging || fields.cta,
      confidence: 0.8,
      excerpt: fields.messaging || fields.cta,
    });
  }
  if (fields.objection && !concepts.some((c) => c.type === CONCEPT_TYPES.OBJECTION)) {
    add({
      type: CONCEPT_TYPES.OBJECTION,
      label: fields.objection.slice(0, 80),
      statement: fields.objection,
      confidence: 0.8,
      excerpt: fields.objection,
    });
  }
  if (fields.language && !concepts.some((c) => c.type === CONCEPT_TYPES.LANGUAGE)) {
    for (const item of asList(fields.language)) {
      add({
        type: CONCEPT_TYPES.LANGUAGE,
        label: item.slice(0, 80),
        statement: item,
        confidence: 0.8,
        excerpt: item,
      });
    }
  }
  if (fields.evidence && !concepts.some((c) => c.type === CONCEPT_TYPES.EVIDENCE)) {
    add({
      type: CONCEPT_TYPES.EVIDENCE,
      label: 'Evidence',
      statement: fields.evidence,
      confidence: 0.82,
      excerpt: fields.evidence,
    });
  }

  for (const quoted of quotes(section.body)) {
    if (concepts.some((c) => c.statement === quoted)) continue;
    add({
      type: CONCEPT_TYPES.LANGUAGE,
      label: quoted.slice(0, 80),
      statement: quoted,
      confidence: 0.74,
      excerpt: quoted,
    });
  }

  return { concepts, pendingEdges };
}

function extractConcepts(documents = []) {
  const used = new Set();
  const concepts = [];
  const pendingEdges = [];
  for (const document of documents) {
    const sections = splitSections(document.body);
    for (const section of sections) {
      const extracted = extractFromSection(section, document, used);
      concepts.push(...extracted.concepts);
      pendingEdges.push(...extracted.pendingEdges);
    }
  }
  return { concepts, pendingEdges };
}

module.exports = {
  SECTION_TYPE_RULES,
  classifyHeading,
  splitSections,
  labeledFields,
  extractConcepts,
  buildConcept,
};
