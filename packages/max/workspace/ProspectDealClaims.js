'use strict';

// Narrow, deterministic extraction of explicit deal claims from recorded
// prospect notes. Pure: no database, model, network or execution dependency.
//
// Two sources only:
//   1. Labelled `Label: value` claims (the original recorded-deal-coverage
//      format), anywhere in the notes for contradiction checks and in the
//      `Fit evidence` section for values.
//   2. A small set of explicit statements in the imported `Fit evidence`
//      narrative (the review-only import stores fit evidence as prose).
//
// Every extracted claim keeps the exact recorded sentence it came from and is
// a recorded claim, unverified. `Risks`, `Next action`, `Market` and other
// sections are never mined: risk prose routinely restates or negates claims
// ("lien position unverified", "exceeds the working range").
//
// States per field:
//   claim        recorded, unverified; counts toward coverage
//   absent       not recorded, or recorded only as unknown/TBD
//   unparsed     recorded but not comparable for this record (range,
//                approximate or unsupported amount format, competing values,
//                alternatives); record-local, never blocks the cohort
//   conflict     contradictory labelled claims (existing rule); the policy
//                withholds the comparative ranking
//
// This is not an underwriting engine. Rehab budgets, purchase prices, ARVs and
// property values are never financing requests; exit timelines are never
// closing dates; nothing here produces tenant financing criteria.

const EXTRACTOR = 'fit-narrative-claims-v3';
const LABELLED_ONLY = 'labelled-claims-v1';
// Rollback switch for narrative extraction only. Labelled claims, integer-k
// amounts and record-local "unparsed" handling stay in force when disabled.
const narrativeEnabled = () => !/^(false|0|off|no|disabled)$/i.test(process.env.MAX_PROSPECT_NARRATIVE_CLAIMS || '');
const extractorVersion = () => (narrativeEnabled() ? EXTRACTOR : LABELLED_ONLY);
const FIELDS = ['amount', 'stage', 'security', 'exit', 'closing'];
const LABELS = {
  amount: ['Financing request', 'Funding request', 'Loan amount', 'Requested financing'],
  stage: ['Deal stage'], security: ['Security position', 'Lien position'],
  exit: ['Exit plan'], closing: ['Closing date'],
};
const SECTION_HEADS = /^(?:Review state|Market|Fit evidence|Risks|Next action|Public profile|Sources):/i;
const clean = value => String(value || '').trim();

function section(notes, label) {
  const lines = String(notes || '').split('\n');
  const start = lines.findIndex(line => line.toLowerCase().startsWith(label.toLowerCase() + ':'));
  if (start < 0) return null;
  const content = [lines[start].slice(label.length + 1).trim()];
  for (const line of lines.slice(start + 1)) {
    if (SECTION_HEADS.test(line)) break;
    content.push(line);
  }
  return content.join('\n').trim();
}

// ---------------------------------------------------------------- amounts
// Supported: "$12,000", "$12000", "$35k", "$35 k". Anything else (decimals,
// "M"/"million", ranges, "+", approximations) is recorded but not comparable.
function parseAmount(raw) {
  const s = clean(raw);
  let m = s.match(/^\$\s?(\d{1,3}(?:,\d{3})+|\d+)$/);
  let n = m ? Number(m[1].replace(/,/g, '')) : null;
  if (!m && (m = s.match(/^\$\s?(\d+)\s?[kK]$/))) n = Number(m[1]) * 1000;
  return Number.isSafeInteger(n) && n > 0 ? n : null;
}

// v3: an explicit range is a recorded request in its own right. It is kept
// verbatim with its bounds; it is never collapsed into one amount. Returns
// null when the bounds cannot be read unambiguously.
function parseRange(raw) {
  const m = clean(raw).match(/^\$\s?(\d{1,3}(?:,\d{3})+|\d+)\s?([kK])?\s*(?:-|–|—|to)\s*\$?\s?(\d{1,3}(?:,\d{3})+|\d+)\s?([kK])?$/);
  if (!m) return null;
  const [, a, ka, b, kb] = m;
  // "$35–45k" leaves the first bound's unit implicit: not read as a range.
  if (!ka && kb && !/,/.test(a) && Number(a) < 1000) return null;
  const min = Number(a.replace(/,/g, '')) * (ka ? 1000 : 1);
  const max = Number(b.replace(/,/g, '')) * (kb ? 1000 : 1);
  return Number.isSafeInteger(min) && Number.isSafeInteger(max) && min > 0 && max > min ? { min, max } : null;
}

const MONEY = String.raw`\$\s?\d[\d,]*(?:\.\d+)?(?:\s?(?:mm|MM|k|K|m|M)\b|\s(?:million|thousand)\b)?\+?`;
const RANGE_TAIL = String.raw`\s*(?:-|–|—|to)\s*\$?\s?\d[\d,]*(?:\.\d+)?(?:\s?(?:k|K|m|M)\b)?`;
const APPROX = String.raw`about|approximately|approx\.?|roughly|around|up\s+to|at\s+least|over|under|nearly|~`;
const REQUEST_VERB = String.raw`\b(?:request(?:s|ed|ing)?|need(?:s|ed|ing)?|seek(?:s|ing)?|sought|looking\s+for|asking\s+for|ask(?:s|ing)?\s+for|wants?|to\s+borrow|borrow(?:s|ing)?)`;
const FINANCING_NOUN = String.raw`(?:bridge\s+|private\s+|short[- ]term\s+|hard[- ]money\s+|gap\s+|rehab\s+)?(?:loan|financing|funding|capital)`;
// v2 request forms. Qualifiers describe the request itself (remaining, gap,
// rehab); "purchase", "budget", "ARV" are deliberately absent.
const REQUEST_QUALIFIER = String.raw`(?:(?:remaining|outstanding|additional|current)\s+)?(?:(?:rehab|renovation|construction|gap|bridge|private|hard[- ]money|short[- ]term)\s+)?(?:(?:loan|financing|funding|capital)\s+)?`;
const REQUEST_ADJ = String.raw`(?:(?:current|public|active|open|pending|stated|explicit|outstanding)\s+){1,3}`;
const AMOUNT_PATTERNS = [
  // "requesting $35k for rehab", "needs $15,000 equity gap"
  new RegExp(`${REQUEST_VERB}\\s+(?:(?:a|an)\\s+)?(?:(${APPROX})\\s*)?(${MONEY})(${RANGE_TAIL})?`, 'gi'),
  // "$35k bridge loan request"
  new RegExp(`(?:(${APPROX})\\s*)?(${MONEY})(${RANGE_TAIL})?\\s+(?:in\\s+)?${FINANCING_NOUN}\\s+(?:request|needed)\\b`, 'gi'),
  // "funding request of $20k"
  new RegExp(`${FINANCING_NOUN}\\s+request\\s+(?:of|for|is)\\s+(?:(${APPROX})\\s*)?(${MONEY})(${RANGE_TAIL})?`, 'gi'),
  // v2: "$35,000 remaining rehab request", "$20k gap request", "$15k request".
  // The amount must be immediately followed by the word "request"; budgets,
  // prices and values never take this form.
  new RegExp(`(?:(${APPROX})\\s*)?(${MONEY})(${RANGE_TAIL})?\\s+${REQUEST_QUALIFIER}request\\b`, 'gi'),
  // v2: "current public request for $35,000–$45,000 gap positions". Guarded
  // by NOT_REQUEST_AFTER like the verb form ("request for $250k properties").
  new RegExp(`\\b${REQUEST_ADJ}(?:${FINANCING_NOUN}\\s+)?request\\s+(?:of|for|is)\\s+(?:(${APPROX})\\s*)?(${MONEY})(${RANGE_TAIL})?`, 'gi'),
];
// Forms whose following words must be checked for a non-request object.
const GUARDED_AMOUNT_PATTERNS = new Set([AMOUNT_PATTERNS[0], AMOUNT_PATTERNS[4]]);
const COORDINATED_AMOUNT = new RegExp(`^(?:\\s+[\\w'-]+){0,4}?\\s+(?:and|or|plus)\\s+(?:another\\s+)?(${MONEY}(?:${RANGE_TAIL})?)`, 'i');
// What follows an amount that is NOT a financing request.
const NOT_REQUEST_AFTER = /^\s*\+?\s*(?:(?:rehab|renovation|construction|repair)\s+)?(?:budgets?|costs?|estimates?|purchase(?:\s+price)?|prices?|ARVs?|after[- ]repair|values?|valuations?|appraisals?|propert(?:y|ies)|houses?|homes?|SFRs?|duplex(?:es)?|deals?|flips?|projects?|listings?|down\s+payments?|portfolio|in\s+equity|of\s+equity)\b/i;

// v3: general borrowing preferences, history and other/future deals are not
// a current financing request for this deal (exact amounts and ranges alike).
const NOT_CURRENT_REQUEST = /\b(?:typically|usually|generally|normally|in\s+general|often|tends?\s+to|prefers?|preferred|preference|historically|previously|in\s+the\s+past|past|prior|earlier|last\s+(?:year|month|quarter|deal|project|flip)|in\s+(?:19|20)\d{2}|per\s+(?:deal|project|flip|property)|each\s+(?:deal|project|flip)|any\s+(?:deal|project)|on\s+average|average|next\s+(?:deal|project|flip|property|purchase)|future\s+(?:deals?|projects?|flips?|properties|purchases?)|for\s+deals\b|loans?\s+of\s+\$[\d,]+\w*\s*(?:-|–|—|to)\s*\$?[\d,]+\w*\s+(?:in\s+general|typically))\b/i;
// The financing need an amount is attached to, read from the matched words
// and the few words after them ("gap positions", "rehab request").
const NEED_RE = /\b(rehab|renovation|construction|gap|bridge|purchase|acquisition|refinanc\w*|refi|down\s+payment)\b/i;
const needOf = text => { const m = String(text || '').match(NEED_RE); return m ? m[1].toLowerCase().replace(/^renovation$/, 'rehab').replace(/^refi\w*$/, 'refinance') : null; };
// Words that make a single amount an explicit narrowing/update of a request.
// An update of a request ("narrowed to $40k", "; now $40k"). Never a request
// on its own: it only matters next to a recorded range in the same statement.
const UPDATE_AMOUNT = new RegExp(String.raw`(?:\b(?:narrowed|revised|updated|reduced|lowered|increased|firmed\s+up|finali[sz]ed|settled|confirmed)\s+(?:it\s+)?(?:down\s+|up\s+)?(?:to|at|on)|(?:^|;\s*|,\s*)now(?:\s+(?:requesting|needs?|seeking|asking\s+for))?)\s+(${MONEY})(?!\s*(?:-|–|—|to)\s*\$?\d)`, 'gi');
// The thing being updated is not the request ("purchase price reduced to").
const NOT_REQUEST_SUBJECT_BEFORE = /\b(?:price|purchase|budgets?|costs?|ARVs?|values?|valuations?|appraisals?|offers?|rents?|payoff|balance|taxes|insurance|profit|proceeds|equity|down\s+payment)\b[^,;]*$/i;
const SUPERSEDE_CUE = /\b(?:narrowed|revised|updated|reduced|lowered|increased|firmed\s+up|now|specifically|finali[sz]ed|confirmed\s+at|settled\s+on|of\s+which)\b/i;

// ------------------------------------------------------------- security
const SECURITY_RE = /\b(first|1st|second|2nd|third|3rd)(?:[- ](?:position|priority))?[- ](?:lien|mortgage|deed\s+of\s+trust|trust\s+deed|position)\b/gi;
// Debt the borrower already carries is not the security offered for this request.
const OTHER_DEBT_BEFORE = /\b(?:existing|current|prior|underlying|senior|outstanding|behind|owes?|owed|owing|carr(?:y|ies|ying)|holds?|held|payoff|paying\s+off|assum(?:e|es|ed|ing)|encumbered|subject\s+to)\b/i;
const normalizeSecurity = s => (/^(?:first|1st)/i.test(s) ? 'first lien' : /^(?:second|2nd)/i.test(s) ? 'second lien' : 'third lien');

// ------------------------------------------------------------------ exit
const EXIT_TERM = String.raw`DSCR[- ]refinanc(?:e|ing)|DSCR[- ]refi|cash[- ]out\s+refinanc(?:e|ing)|refinanc(?:e|ing)|refi|retail\s+sale|resale|sale|selling|sell|long[- ]term\s+rental|rental|hold|BRRRR`;
const EXIT_PATTERNS = [
  new RegExp(String.raw`\bexit(?:\s+(?:plan|strategy))?(?:\s+(?:is|will\s+be|would\s+be))?\s*(?:via|through|by|:|-|–|=)?\s*(?:(?:a|an)\s+)?(${EXIT_TERM})\b`, 'gi'),
  new RegExp(String.raw`\b(?:plans?\s+to|planning\s+to|intends?\s+to|intending\s+to|will(?:\s+then)?)\s+(refinance|refi|sell|resell|rent|hold)\b`, 'gi'),
  // v2: "sale exit", "planned DSCR refinance exit". Singular only: "sale
  // exits" describes a track record. BRRRR and "hold" are generic strategy
  // terms and are not accepted in this form.
  new RegExp(String.raw`\b(DSCR[- ]refinanc(?:e|ing)|DSCR[- ]refi|cash[- ]out\s+refinanc(?:e|ing)|refinanc(?:e|ing)|refi|retail\s+sale|resale|sale)\s+exit\b(?!s)`, 'gi'),
];
const SUFFIX_EXIT_PATTERN = EXIT_PATTERNS[2];
// A projected/possible/past exit is not this project's plan.
const EXIT_NOT_PLAN_BEFORE = /\b(?:projected|possible|potential|prior|previous|past|typical|usual|historical|completed)\s+(?:\w+\s+){0,2}$/i;
const EXIT_ALTERNATIVE_AFTER = /^\s*(?:\/|or\s+(?:a\s+|an\s+)?)(?:DSCR\s+|cash[- ]out\s+)?(?:refinanc|refi|sale|sell|resale|rent|hold|BRRRR)/i;
function normalizeExit(s) {
  if (/dscr/i.test(s)) return 'DSCR refinance';
  if (/cash[- ]out/i.test(s)) return 'cash-out refinance';
  if (/refi/i.test(s)) return 'refinance';
  if (/sale|sell/i.test(s)) return 'sale';
  return 'rental/hold';
}

// ----------------------------------------------------------------- stage
const PROPERTY = String.raw`(?:subject\s+)?(?:property|SFR|single[- ]family(?:\s+home)?|duplex|triplex|fourplex|quadplex|house|home|multifamily|building|condo|townhouse|lot)`;
const STAGE_PATTERNS = [
  [/\bunder\s+contract\b/gi, 'under contract'],
  [/\b(?:signed|executed)\s+purchase\s+(?:contract|agreement)\b|\bpurchase\s+(?:contract|agreement)\s+(?:is\s+)?(?:signed|executed|recorded|in\s+place)\b/gi, 'under contract'],
  [new RegExp(String.raw`\b(?:a|an|the|this)\s+(?:recently\s+)?(?:purchased|acquired)\s+${PROPERTY}\b`, 'gi'), 'property acquired'],
  [new RegExp(String.raw`\b(?:purchased|acquired|closed\s+on|owns)\s+(?:the|a|an|this)\s+${PROPERTY}\b`, 'gi'), 'property acquired'],
  [/\bowns?\s+(?:the\s+property\s+|it\s+)?free\s+and\s+clear\b|\bowned\s+free\s+and\s+clear\b/gi, 'property acquired'],
  [new RegExp(String.raw`\bproperty\s+identified\b|\bidentified\s+(?:a|the)\s+${PROPERTY}\b`, 'gi'), 'property identified'],
  // v2: "in rehab" only as a state of the property ("currently in rehab",
  // "is in rehab"), never "$35K in rehab funds needed".
  [/\b(?:rehab|renovation)\s+(?:is\s+)?(?:underway|in\s+progress|started)\b|\bmid[- ]rehab\b|\b(?:is|are|currently|now|already|still)\s+in[- ]rehab\b(?!\s+(?:funds?|funding|financing|capital|money|budgets?|costs?|loans?|draws?|requests?|needs?)\b)/gi, 'rehab underway'],
  // v2: "already purchased" / "property already acquired" for the current
  // property. Track-record forms ("already purchased 14 homes", "already
  // purchased and sold") are rejected in narrativeFindings.
  [new RegExp(String.raw`\b(?:${PROPERTY}\s+(?:was\s+|is\s+|has\s+been\s+)?)?already\s+(?:been\s+)?(?:purchased|acquired|bought|closed\s+on)\b`, 'gi'), 'property acquired'],
];
const ALREADY_STAGE = STAGE_PATTERNS[STAGE_PATTERNS.length - 1][0];
// After "already purchased": a count, plural property noun or resale means a
// track record, not the current property.
const TRACK_RECORD_AFTER = /^\s*(?:(?:and|&)\s+(?:sold|flipped|resold|rented|refinanced)\b|(?:over\s+|more\s+than\s+|about\s+|nearly\s+)?(?:\d+|two|three|four|five|six|seven|eight|nine|ten|dozens?|several|many|multiple|numerous|hundreds?)\b|(?:\w+\s+){0,2}(?:properties|homes|houses|SFRs|units|deals|doors|flips|buildings)\b)/i;
const TRACK_RECORD_CLAUSE = /\b(?:track\s+record|portfolio|over\s+the\s+(?:years|past)|since\s+(?:19|20)\d{2}|to\s+date|in\s+total|previously|historically|lifetime)\b/i;

// --------------------------------------------------------------- closing
const MONTH = String.raw`(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|June?|July?|Aug(?:ust)?|Sept?(?:ember)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)`;
const DATE = String.raw`\d{4}-\d{2}-\d{2}|\d{1,2}\/\d{1,2}(?:\/\d{2,4})?|${MONTH}\.?\s+\d{1,2}(?:st|nd|rd|th)?(?:,?\s+\d{4})?`;
const RELATIVE = String.raw`(?:~\s?|about\s+|approximately\s+|roughly\s+)?\d{1,3}\s+(?:days?|weeks?|months?)`;
const WHEN = String.raw`(?:(?:scheduled|set|targeted|expected|planned|slated)\s+(?:for\s+|on\s+|by\s+|in\s+|within\s+)?|(?:for|on|by|in|within)\s+)`;
const CLOSING_PATTERNS = [
  new RegExp(String.raw`\b(?:closing|close|closes|settlement)(?:\s+date)?\s+(?:is\s+)?${WHEN}(${DATE}|${RELATIVE})`, 'gi'),
  new RegExp(String.raw`\b(?:targeting|targeted|target|expects?\s+to|expecting\s+to|scheduled\s+to|set\s+to|plans?\s+to)\s+(?:a\s+)?clos(?:e|ing)(?:\s+date)?\s+(?:of\s+|on\s+|for\s+|by\s+|in\s+|within\s+)?(${DATE}|${RELATIVE})`, 'gi'),
];
// A timeline attached to the exit/refinance is never an acquisition closing.
const EXIT_CONTEXT_BEFORE = /\b(?:exit|refinanc\w*|refi|resale|resell|sell|sale|DSCR|take[- ]?out|payoff|repay\w*)\b/i;

// ------------------------------------------------- negation and hedging
const VERIFICATION_STATUS = /\bnot\s+(?:yet\s+)?(?:been\s+)?(?:independently\s+)?(?:verified|confirmed|validated|documented)\b/gi;
const NEGATION = /\b(?:no|not|never|without|unknown|unclear|missing|tbd|none|nor|lacks?|isn't|aren't|wasn't|doesn't|don't|didn't|hasn't|haven't|n\/a)\b/i;
// Projections and estimates are not an explicit financing request.
const AMOUNT_HEDGE = /\b(?:expects?|expected|expecting|anticipat\w*|projected|projects|projection|estimated?|estimates|budgeted|forecast\w*)\b/i;
const HEDGE = /\b(?:may|might|could|possibly|perhaps|maybe|if|whether|considering|potentially|hypothetical(?:ly)?)\b/i;
const ABSENT_VALUE = /^(?:unknown|unclear|missing|none|no\b|n\/?a\b|tbd\b|to be determined|pending|not\s+(?:recorded|known|provided|stated|available|specified|disclosed|identified|determined))/i;
const ONLY_UNVERIFIED = /^(?:unverified|not\s+(?:yet\s+)?verified|needs\s+verification)\.?$/i;

function sentences(text) {
  const out = [];
  const re = /[^\n]+?(?:[.;!?](?=\s|$)|$)/gm;
  for (const m of String(text || '').matchAll(re)) {
    const s = m[0].trim();
    if (s) out.push(s);
  }
  return out;
}
// The comma/contrast-delimited clause around a match, so "Exit via DSCR
// refinance, no seasoning issue" keeps its exit while "no first lien" does not.
function clauseAround(sentence, index, length) {
  const bounds = /,\s+|\s+(?:but|however|although|though|whereas)\s+|\s+[-–—]\s+/gi;
  let start = 0; let end = sentence.length;
  for (const b of sentence.matchAll(bounds)) {
    if (b.index + b[0].length <= index) start = b.index + b[0].length;
    else if (b.index >= index + length) { end = b.index; break; }
  }
  return { before: sentence.slice(start, index), clause: sentence.slice(start, end) };
}
function asserted(sentence, index, length, field) {
  const { before, clause } = clauseAround(sentence, index, length);
  const text = clause.replace(VERIFICATION_STATUS, ' ');
  // v2: a deal-stage phrase is negated only by words before it or inside it
  // ("no executed purchase contract", "not under contract"); a negation that
  // follows it describes something else ("executed purchase contract with no
  // financing contingency").
  const negationScope = field === 'stage'
    ? (before + sentence.slice(index, index + length)).replace(VERIFICATION_STATUS, ' ')
    : text;
  return !NEGATION.test(negationScope) && !HEDGE.test(text) && !(field === 'amount' && AMOUNT_HEDGE.test(text));
}

function narrativeFindings(fit) {
  const found = Object.fromEntries(FIELDS.map(f => [f, []]));
  found.amountUpdates = [];
  let previous = '';
  let sentenceIndex = -1;
  let group = -1;
  for (const sentence of sentences(fit)) {
    sentenceIndex += 1;
    // A ";"-joined clause belongs to the same statement group as the previous one.
    if (!/;$/.test(previous)) group += 1;
    // v2: a ";"-joined clause continues the previous one, so "Exit plan is a
    // refinance; target close in 90 days" keeps its exit context.
    const continued = /;$/.test(previous) ? previous : '';
    previous = sentence;
    const add = (field, entry, index, length) => {
      if (asserted(sentence, index, length, field)) found[field].push({ ...entry, snippet: sentence });
    };
    for (const re of AMOUNT_PATTERNS) {
      for (const m of sentence.matchAll(re)) {
        const [approx, money, range] = m.slice(1, 4);
        const after = sentence.slice(m.index + m[0].length);
        if (GUARDED_AMOUNT_PATTERNS.has(re) && NOT_REQUEST_AFTER.test(after)) continue;
        const raw = clean(money);
        if (NOT_CURRENT_REQUEST.test(clauseAround(sentence, m.index, m[0].length).clause)) continue;
        const n = parseAmount(raw.replace(/\+$/, ''));
        const value = clean(money + (range || ''));
        const bounds = range && !approx ? parseRange(value) : null;
        const reason = approx || /\+$/.test(raw) ? 'approximate' : range ? (bounds ? null : 'unsupported_format') : n == null ? 'unsupported_format' : null;
        const need = needOf(m[0] + ' ' + after.split(/\s+/).slice(0, 4).join(' '));
        add('amount', { match: clean(m[0]), value, amount: reason || bounds ? null : n, reason,
          ...(bounds ? { form: 'range', range: bounds } : {}), need, group, sentenceIndex,
          supersedeCue: SUPERSEDE_CUE.test(sentence),
          normalized: reason ? `${reason}:${value}` : bounds ? `range:${bounds.min}-${bounds.max}` : String(n) }, m.index, m[0].length);
        // "requesting $12k on one and $18k on the other": a coordinated second
        // amount is a competing request value, never silently dropped.
        const coordinated = after.match(COORDINATED_AMOUNT);
        if (coordinated && !NOT_REQUEST_AFTER.test(after.slice(coordinated.index + coordinated[0].length))) {
          const second = clean(coordinated[1]);
          const b2 = parseRange(second);
          const n2 = b2 ? null : parseAmount(second);
          add('amount', { match: clean(coordinated[0]), value: second, amount: n2, reason: n2 == null && !b2 ? 'unsupported_format' : null,
            ...(b2 ? { form: 'range', range: b2 } : {}), group, sentenceIndex,
            normalized: b2 ? `range:${b2.min}-${b2.max}` : n2 == null ? `unsupported_format:${second}` : String(n2) }, m.index, m[0].length);
        }
      }
    }
    for (const m of sentence.matchAll(UPDATE_AMOUNT)) {
      const after = sentence.slice(m.index + m[0].length);
      if (NOT_REQUEST_AFTER.test(after)) continue;
      if (NOT_REQUEST_SUBJECT_BEFORE.test(clauseAround(sentence, m.index, m[0].length).before + ' ' + m[0].replace(m[1], ''))) continue;
      const n = parseAmount(clean(m[1]));
      if (n == null || !asserted(sentence, m.index, m[0].length, 'amount')) continue;
      found.amountUpdates.push({ match: clean(m[0]), value: clean(m[1]), amount: n, normalized: String(n), group,
        need: needOf(m[0] + ' ' + after.split(/\s+/).slice(0, 4).join(' ')), snippet: sentence });
    }
    for (const m of sentence.matchAll(SECURITY_RE)) {
      if (OTHER_DEBT_BEFORE.test(clauseAround(sentence, m.index, m[0].length).before)) continue;
      add('security', { match: m[0], value: m[0], normalized: normalizeSecurity(m[1]) }, m.index, m[0].length);
    }
    for (const re of EXIT_PATTERNS) {
      for (const m of sentence.matchAll(re)) {
        if (re === SUFFIX_EXIT_PATTERN && EXIT_NOT_PLAN_BEFORE.test(clauseAround(sentence, m.index, m[0].length).before)) continue;
        const alternative = EXIT_ALTERNATIVE_AFTER.test(sentence.slice(m.index + m[0].length));
        add('exit', { match: m[0], value: m[1], normalized: normalizeExit(m[1]), reason: alternative ? 'alternatives' : null }, m.index, m[0].length);
      }
    }
    for (const [re, normalized] of STAGE_PATTERNS) {
      for (const m of sentence.matchAll(re)) {
        if (re === ALREADY_STAGE && (TRACK_RECORD_AFTER.test(sentence.slice(m.index + m[0].length)) ||
          TRACK_RECORD_CLAUSE.test(clauseAround(sentence, m.index, m[0].length).clause))) continue;
        add('stage', { match: m[0], value: m[0], normalized }, m.index, m[0].length);
      }
    }
    for (const re of CLOSING_PATTERNS) {
      for (const m of sentence.matchAll(re)) {
        // Whole sentence before the match: "Exit via refinance, which should
        // close in 6 months" is an exit timeline, not an acquisition closing.
        if (EXIT_CONTEXT_BEFORE.test(continued + ' ' + sentence.slice(0, m.index))) continue;
        const relative = new RegExp(`^${RELATIVE}$`, 'i').test(m[1]);
        add('closing', { match: m[0], value: m[1], normalized: m[1].replace(/\s+/g, ' ').toLowerCase(), relative }, m.index, m[0].length);
      }
    }
  }
  return found;
}

// Collapse one field's narrative findings into a claim or a record-local
// "unparsed" entry. Never a cohort-level conflict.
function resolveNarrative(field, entries, updates = []) {
  if (!entries.length) return null;
  const seen = new Map();
  for (const e of entries) if (!seen.has(e.normalized)) seen.set(e.normalized, e);
  let distinct = [...seen.values()];
  if (field === 'exit' && distinct.length > 1 && distinct.some(e => e.normalized === 'refinance')) {
    const specific = distinct.filter(e => /refinance/.test(e.normalized) && e.normalized !== 'refinance');
    if (specific.length) distinct = distinct.filter(e => e.normalized !== 'refinance');
  }
  if (field === 'stage' && distinct.length === 2 && distinct.some(e => e.normalized === 'rehab underway') &&
    distinct.some(e => e.normalized === 'property acquired')) distinct = distinct.filter(e => e.normalized === 'rehab underway');
  let supersedes = null;
  if (field === 'amount' && distinct.length === 2) {
    // v3: a single exact request supersedes a range only when it is clearly
    // the same statement (same sentence or ";"-joined clause), carries an
    // explicit narrowing/update cue, and names no different financing need.
    const rangeEntry = distinct.find(e => e.form === 'range');
    const exact = distinct.find(e => e.form !== 'range' && !e.reason && e.amount != null);
    if (rangeEntry && exact && exact.group === rangeEntry.group && exact.supersedeCue &&
      (!exact.need || !rangeEntry.need || exact.need === rangeEntry.need)) {
      supersedes = { value: rangeEntry.value, range: rangeEntry.range, snippet: rangeEntry.snippet };
      distinct = [exact];
    }
  }
  if (field === 'amount' && distinct.length === 1 && distinct[0].form === 'range') {
    // "Requesting $35k–$45k; narrowed to $40k": an update in the same
    // statement supersedes the range only if it is the single update value and
    // names no different financing need; otherwise both are retained as
    // non-comparable.
    const rangeEntry = distinct[0];
    const sameStatement = updates.filter(u => u.group === rangeEntry.group);
    const elsewhere = updates.filter(u => u.group !== rangeEntry.group);
    const values = [...new Map([...sameStatement, ...elsewhere].map(u => [u.normalized, u])).values()];
    // An update in a different sentence has unclear association: retained,
    // never silently applied or dropped.
    if (!elsewhere.length && values.length === 1 && (!values[0].need || !rangeEntry.need || values[0].need === rangeEntry.need)) {
      supersedes = { value: rangeEntry.value, range: rangeEntry.range, snippet: rangeEntry.snippet };
      distinct = [values[0]];
    } else if (values.length) {
      return { unparsed: { field, reason: 'multiple_values', values: [rangeEntry.value, ...values.map(u => u.value)],
        snippets: [...new Set([rangeEntry.snippet, ...values.map(u => u.snippet)])] } };
    }
  }
  const snippets = [...new Set(entries.map(e => e.snippet))];
  if (distinct.length > 1) {
    return { unparsed: { field, reason: 'multiple_values', values: distinct.map(e => e.value), snippets } };
  }
  const [only] = distinct;
  if (only.reason) return { unparsed: { field, reason: only.reason, values: [only.value], snippets } };
  return { claim: { field, value: only.value, normalized: only.normalized, origin: 'fit_narrative',
    match: only.match, snippet: only.snippet, snippets, epistemic: 'recorded_unverified',
    ...(only.amount != null ? { amount: only.amount } : {}), ...(only.form === 'range' ? { form: 'range', range: only.range } : {}),
    ...(supersedes ? { supersedes } : {}), ...(only.relative ? { relative: true } : {}) } };
}

function extractDealClaims(notes) {
  const all = String(notes || '');
  const fit = section(all, 'Fit evidence');
  const labelInput = fit || all;
  const labelledScope = all.replace(/^(?:Fit evidence|Risks):\s*/gmi, '');
  const values = {};
  const claims = {};
  const conflicts = [];
  const unparsed = [];
  const absent = [];
  const narrative = fit && narrativeEnabled() ? narrativeFindings(fit) : null;
  for (const field of FIELDS) {
    const regex = new RegExp(`(?:^|[;\\n])[ \\t]*((?:${LABELS[field].join('|')}):[ \\t]*([^;\\n]+))`, 'gi');
    const everywhere = [...labelledScope.matchAll(regex)].map(m => clean(m[2])).filter(Boolean);
    const inFit = [...labelInput.matchAll(regex)].map(m => ({ value: clean(m[2]), snippet: clean(m[1]) })).filter(m => m.value);
    if (new Set(everywhere).size > 1) { conflicts.push(field); continue; }
    if (everywhere.length) {
      // A labelled claim governs this field; narrative extraction does not.
      const [first] = inFit;
      if (!first) { absent.push(field); continue; }
      if (ABSENT_VALUE.test(first.value) || ONLY_UNVERIFIED.test(first.value)) { absent.push(field); continue; }
      const labelledRange = field === 'amount' ? parseRange(first.value) : null;
      if (field === 'amount' && parseAmount(first.value) == null && !labelledRange) {
        unparsed.push({ field, reason: 'unsupported_format', values: [first.value], snippets: [first.snippet] });
        continue;
      }
      values[field] = first.value;
      claims[field] = { field, value: first.value, origin: 'label', match: first.snippet, snippet: first.snippet,
        snippets: [first.snippet], epistemic: 'recorded_unverified',
        ...(field === 'amount' ? (labelledRange ? { amount: null, form: 'range', range: labelledRange } : { amount: parseAmount(first.value) }) : {}) };
      continue;
    }
    const resolved = narrative && resolveNarrative(field, narrative[field], field === 'amount' ? narrative.amountUpdates : []);
    if (resolved?.claim) { values[field] = resolved.claim.value; claims[field] = resolved.claim; }
    else if (resolved?.unparsed) unparsed.push(resolved.unparsed);
    else absent.push(field);
  }
  return { values, amount: claims.amount?.amount ?? null, conflicts, count: Object.keys(values).length,
    claims, unparsed, absent, extractor: extractorVersion() };
}

// Dollar ranges that appear in prospect notes. Disclosed, never used as
// criteria: authorized criteria come only from approved tenant Blueprint facts.
const NOTE_RANGE = /\$\s?\d[\d,]*(?:\.\d+)?\s?[kK]?\s*(?:-|–|—|to)\s*\$?\s?\d[\d,]*(?:\.\d+)?\s?[kK]?/g;
function dollarRangesInNotes(notes) {
  return [...new Set([...String(notes || '').matchAll(NOTE_RANGE)].map(m => clean(m[0])))];
}

module.exports = { EXTRACTOR, LABELLED_ONLY, extractorVersion, narrativeEnabled, FIELDS, LABELS, section, parseAmount, parseRange, extractDealClaims, dollarRangesInNotes };
