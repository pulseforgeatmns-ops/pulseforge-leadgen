'use strict';

const {
  MUTATION_KEYWORDS,
  TARGET_ALIASES,
} = require('./types');

/**
 * EQL Parser — tokenize + recursive-descent AST (SPEC-020).
 *
 * Read-only language. Mutation keywords are rejected at parse time.
 */

/**
 * @param {string} source
 * @returns {import('./types').EqlAst}
 */
function parseEql(source) {
  if (source == null || String(source).trim() === '') {
    throw new EqlParseError('EQL query is empty');
  }
  const tokens = tokenize(String(source));
  const parser = new Parser(tokens, String(source));
  return parser.parseQuery();
}

class EqlParseError extends Error {
  /**
   * @param {string} message
   * @param {{ line?: number, column?: number, token?: string }} [meta]
   */
  constructor(message, meta = {}) {
    super(message);
    this.name = 'EqlParseError';
    this.line = meta.line ?? null;
    this.column = meta.column ?? null;
    this.token = meta.token ?? null;
  }
}

/**
 * @typedef {object} Token
 * @property {string} type
 * @property {string|number|boolean|null} [value]
 * @property {number} line
 * @property {number} column
 */

/**
 * @param {string} source
 * @returns {Token[]}
 */
function tokenize(source) {
  /** @type {Token[]} */
  const tokens = [];
  let i = 0;
  let line = 1;
  let column = 1;

  const push = (type, value) => {
    tokens.push({ type, value, line, column });
  };

  while (i < source.length) {
    const ch = source[i];

    if (ch === '\n') {
      i += 1;
      line += 1;
      column = 1;
      continue;
    }
    if (/\s/.test(ch)) {
      i += 1;
      column += 1;
      continue;
    }

    // Line comment
    if (ch === '-' && source[i + 1] === '-') {
      while (i < source.length && source[i] !== '\n') {
        i += 1;
      }
      continue;
    }

    const startCol = column;

    // String literal
    if (ch === '"' || ch === "'") {
      const quote = ch;
      i += 1;
      column += 1;
      let value = '';
      while (i < source.length && source[i] !== quote) {
        if (source[i] === '\\' && i + 1 < source.length) {
          value += source[i + 1];
          i += 2;
          column += 2;
          continue;
        }
        if (source[i] === '\n') {
          throw new EqlParseError('Unterminated string literal', {
            line,
            column: startCol,
          });
        }
        value += source[i];
        i += 1;
        column += 1;
      }
      if (i >= source.length) {
        throw new EqlParseError('Unterminated string literal', {
          line,
          column: startCol,
        });
      }
      i += 1;
      column += 1;
      tokens.push({ type: 'STRING', value, line, column: startCol });
      continue;
    }

    // Number
    if (/[0-9]/.test(ch) || (ch === '.' && /[0-9]/.test(source[i + 1] || ''))) {
      let raw = '';
      while (i < source.length && /[0-9.]/.test(source[i])) {
        raw += source[i];
        i += 1;
        column += 1;
      }
      const num = Number(raw);
      if (!Number.isFinite(num)) {
        throw new EqlParseError(`Invalid number: ${raw}`, {
          line,
          column: startCol,
        });
      }
      tokens.push({ type: 'NUMBER', value: num, line, column: startCol });
      continue;
    }

    // Operators / punctuation
    if (ch === '(' || ch === ')' || ch === ',') {
      tokens.push({ type: ch, value: ch, line, column: startCol });
      i += 1;
      column += 1;
      continue;
    }

    if (ch === '!' && source[i + 1] === '=') {
      tokens.push({ type: 'OP', value: '!=', line, column: startCol });
      i += 2;
      column += 2;
      continue;
    }
    if (ch === '>' && source[i + 1] === '=') {
      tokens.push({ type: 'OP', value: '>=', line, column: startCol });
      i += 2;
      column += 2;
      continue;
    }
    if (ch === '<' && source[i + 1] === '=') {
      tokens.push({ type: 'OP', value: '<=', line, column: startCol });
      i += 2;
      column += 2;
      continue;
    }
    if (ch === '=' || ch === '>' || ch === '<') {
      tokens.push({ type: 'OP', value: ch, line, column: startCol });
      i += 1;
      column += 1;
      continue;
    }

    // Identifier / keyword
    if (/[A-Za-z_]/.test(ch)) {
      let raw = '';
      while (i < source.length && /[A-Za-z0-9_]/.test(source[i])) {
        raw += source[i];
        i += 1;
        column += 1;
      }
      const upper = raw.toUpperCase();
      if (MUTATION_KEYWORDS.includes(upper)) {
        throw new EqlParseError(
          `EQL is read-only; mutation keyword ${upper} is not allowed`,
          { line, column: startCol, token: raw }
        );
      }
      if (upper === 'TRUE' || upper === 'FALSE') {
        tokens.push({
          type: 'BOOLEAN',
          value: upper === 'TRUE',
          line,
          column: startCol,
        });
        continue;
      }
      if (upper === 'NULL') {
        tokens.push({ type: 'NULL', value: null, line, column: startCol });
        continue;
      }
      tokens.push({
        type: 'IDENT',
        value: raw,
        line,
        column: startCol,
      });
      continue;
    }

    throw new EqlParseError(`Unexpected character: ${ch}`, {
      line,
      column: startCol,
      token: ch,
    });
  }

  tokens.push({ type: 'EOF', value: null, line, column });
  return tokens;
}

class Parser {
  /**
   * @param {Token[]} tokens
   * @param {string} source
   */
  constructor(tokens, source) {
    this.tokens = tokens;
    this.source = source;
    this.pos = 0;
  }

  /** @returns {import('./types').EqlAst} */
  parseQuery() {
    const ast = this.parseStatement();
    // Optional trailing EXPLAIN
    if (this.checkKeyword('EXPLAIN')) {
      this.advance();
      ast.explain = true;
    }
    this.expect('EOF', 'Unexpected tokens after EQL statement');
    return ast;
  }

  /** @returns {import('./types').EqlAst} */
  parseStatement() {
    if (this.checkKeyword('FIND')) return this.parseFind();
    if (this.checkKeyword('SHOW')) return this.parseShow();
    if (this.checkKeyword('REPLAY')) return this.parseReplay();
    if (this.checkKeyword('COMPARE')) return this.parseCompare();
    if (this.checkKeyword('EXPLAIN')) return this.parseExplain();

    const tok = this.peek();
    throw new EqlParseError(
      `Expected FIND, SHOW, REPLAY, COMPARE, or EXPLAIN; got ${describeToken(tok)}`,
      { line: tok.line, column: tok.column, token: String(tok.value) }
    );
  }

  /** @returns {import('./types').EqlFindNode} */
  parseFind() {
    this.expectKeyword('FIND');
    const { target, entity } = this.parseTargetOrRef();

    /** @type {import('./types').EqlCondition[]} */
    const where = [];
    if (this.checkKeyword('WHERE')) {
      this.advance();
      where.push(this.parseCondition());
      while (this.checkKeyword('AND')) {
        this.advance();
        where.push(this.parseCondition());
      }
    }

    /** @type {import('./types').EqlOrderBy|null} */
    let orderBy = null;
    if (this.checkKeyword('ORDER')) {
      this.advance();
      this.expectKeyword('BY');
      const field = this.expectIdent('ORDER BY field');
      let direction = 'ASC';
      if (this.checkKeyword('ASC') || this.checkKeyword('DESC')) {
        direction = String(this.advance().value).toUpperCase();
      }
      orderBy = { field, direction: /** @type {'ASC'|'DESC'} */ (direction) };
    }

    /** @type {number|null} */
    let limit = null;
    if (this.checkKeyword('LIMIT')) {
      this.advance();
      const num = this.expectType('NUMBER', 'LIMIT requires a number');
      limit = /** @type {number} */ (num.value);
      if (!Number.isInteger(limit) || limit < 0) {
        throw new EqlParseError('LIMIT must be a non-negative integer', {
          line: num.line,
          column: num.column,
        });
      }
    }

    return {
      kind: 'FIND',
      target,
      entity,
      where,
      orderBy,
      limit,
      explain: false,
    };
  }

  /** @returns {import('./types').EqlShowNode} */
  parseShow() {
    this.expectKeyword('SHOW');
    const { target } = this.parseTargetOrRef({ allowEntity: false });

    /** @type {'SUPPORTING'|'CONTRADICTING'|'FOR'|null} */
    let relation = null;
    /** @type {import('./types').EqlEntityRef|null} */
    let related = null;

    if (this.checkKeyword('SUPPORTING') || this.checkKeyword('CONTRADICTING')) {
      relation = /** @type {'SUPPORTING'|'CONTRADICTING'} */ (
        String(this.advance().value).toUpperCase()
      );
      related = this.parseEntityRef();
    } else if (this.checkKeyword('FOR')) {
      // SHOW Calibration FOR Claim("…") · SHOW DailyReview FOR Today · SHOW SimilarTrades FOR Trade("…")
      relation = 'FOR';
      this.advance();
      if (this.check('(')) {
        related = this.parseEntityRef();
      } else if (
        this.check('IDENT') &&
        this.tokens[this.pos + 1] &&
        this.tokens[this.pos + 1].type === '('
      ) {
        related = this.parseEntityRef();
      } else {
        const period = this.expectIdent('FOR target');
        related = { target: 'periods', id: String(period) };
      }
    }

    /** @type {import('./types').EqlCondition[]} */
    const where = [];
    if (this.checkKeyword('WHERE')) {
      this.advance();
      where.push(this.parseCondition());
      while (this.checkKeyword('AND')) {
        this.advance();
        where.push(this.parseCondition());
      }
    }

    return {
      kind: 'SHOW',
      target,
      relation,
      related,
      where,
      explain: false,
    };
  }

  /** @returns {import('./types').EqlReplayNode} */
  parseReplay() {
    this.expectKeyword('REPLAY');

    /** @type {string|null} */
    let subject = null;
    if (this.checkKeyword('SUBJECT')) {
      this.advance();
      // SUBJECT "BTC" or SUBJECT = "BTC"
      if (this.checkOp('=')) this.advance();
      subject = String(this.expectType('STRING', 'REPLAY SUBJECT requires a string').value);
    }

    this.expectKeyword('FROM');
    const from = String(this.expectType('STRING', 'REPLAY FROM requires an ISO timestamp').value);
    this.expectKeyword('TO');
    const to = String(this.expectType('STRING', 'REPLAY TO requires an ISO timestamp').value);

    return {
      kind: 'REPLAY',
      subject,
      from,
      to,
      explain: false,
    };
  }

  /** @returns {import('./types').EqlCompareNode} */
  parseCompare() {
    this.expectKeyword('COMPARE');
    const left = this.parseCompareSide();
    this.expectKeyword('WITH');
    const right = this.parseCompareSide();
    return {
      kind: 'COMPARE',
      left,
      right,
      explain: false,
    };
  }

  /**
   * Compare side: string id, Entity("id"), or bare collection label
   * (e.g. WinningTrades / LosingTrades — SPEC-044).
   * @returns {import('./types').EqlEntityRef|string}
   */
  parseCompareSide() {
    if (this.check('STRING')) {
      return String(this.advance().value);
    }
    const ident = this.expectIdent('compare side');
    if (this.check('(')) {
      const target = resolveTarget(ident);
      this.advance();
      const idTok = this.expectType('STRING', 'Entity ref requires a string id');
      this.expect(')', 'Expected ) after entity id');
      return { target, id: String(idTok.value) };
    }
    return String(ident);
  }

  /** @returns {import('./types').EqlExplainNode} */
  parseExplain() {
    this.expectKeyword('EXPLAIN');
    /** @type {import('./types').EqlEntityRef|null} */
    let entity = null;
    if (!this.check('EOF') && !this.checkKeyword('EXPLAIN')) {
      // EXPLAIN Claim("x") as a standalone statement
      if (this.check('IDENT')) {
        entity = this.parseEntityRef();
      }
    }
    return {
      kind: 'EXPLAIN',
      entity,
      explain: true,
    };
  }

  /**
   * @param {{ allowEntity?: boolean }} [opts]
   * @returns {{ target: import('./types').EqlTarget, entity: import('./types').EqlEntityRef|null }}
   */
  parseTargetOrRef(opts = {}) {
    const allowEntity = opts.allowEntity !== false;
    const ident = this.expectIdent('query target');
    const target = resolveTarget(ident);

    if (allowEntity && this.check('(')) {
      this.advance();
      const idTok = this.expectType('STRING', 'Entity ref requires a string id');
      this.expect(')', 'Expected ) after entity id');
      return {
        target,
        entity: { target, id: String(idTok.value) },
      };
    }

    return { target, entity: null };
  }

  /** @returns {import('./types').EqlEntityRef} */
  parseEntityRef() {
    const ident = this.expectIdent('entity reference');
    const target = resolveTarget(ident);
    this.expect('(', 'Entity ref requires (id)');
    const idTok = this.expectType('STRING', 'Entity ref requires a string id');
    this.expect(')', 'Expected ) after entity id');
    return { target, id: String(idTok.value) };
  }

  /** @returns {import('./types').EqlCondition} */
  parseCondition() {
    const field = this.expectIdent('condition field');
    let operator;
    if (this.checkKeyword('CONTAINS')) {
      operator = 'CONTAINS';
      this.advance();
    } else {
      const opTok = this.expectType('OP', 'Expected comparison operator');
      operator = /** @type {import('./types').EqlOperator} */ (opTok.value);
    }
    const value = this.parseValue();
    return { field, operator, value };
  }

  /** @returns {string|number|boolean|null} */
  parseValue() {
    const tok = this.peek();
    if (tok.type === 'STRING' || tok.type === 'NUMBER' || tok.type === 'BOOLEAN' || tok.type === 'NULL') {
      this.advance();
      return /** @type {string|number|boolean|null} */ (tok.value);
    }
    throw new EqlParseError(
      `Expected literal value; got ${describeToken(tok)}`,
      { line: tok.line, column: tok.column }
    );
  }

  // --- token helpers ---

  peek() {
    return this.tokens[this.pos];
  }

  advance() {
    const tok = this.tokens[this.pos];
    this.pos += 1;
    return tok;
  }

  check(type) {
    return this.peek().type === type;
  }

  checkKeyword(word) {
    const tok = this.peek();
    return (
      tok.type === 'IDENT' &&
      String(tok.value).toUpperCase() === String(word).toUpperCase()
    );
  }

  checkOp(op) {
    const tok = this.peek();
    return tok.type === 'OP' && tok.value === op;
  }

  expect(type, message) {
    const tok = this.peek();
    if (tok.type !== type) {
      throw new EqlParseError(message || `Expected ${type}; got ${describeToken(tok)}`, {
        line: tok.line,
        column: tok.column,
        token: String(tok.value),
      });
    }
    return this.advance();
  }

  expectKeyword(word) {
    const tok = this.peek();
    if (
      tok.type !== 'IDENT' ||
      String(tok.value).toUpperCase() !== String(word).toUpperCase()
    ) {
      throw new EqlParseError(`Expected keyword ${word}; got ${describeToken(tok)}`, {
        line: tok.line,
        column: tok.column,
        token: String(tok.value),
      });
    }
    return this.advance();
  }

  expectIdent(label) {
    const tok = this.expectType('IDENT', `Expected ${label}`);
    return String(tok.value);
  }

  expectType(type, message) {
    const tok = this.peek();
    if (tok.type !== type) {
      throw new EqlParseError(message || `Expected ${type}`, {
        line: tok.line,
        column: tok.column,
        token: String(tok.value),
      });
    }
    return this.advance();
  }
}

/**
 * @param {string} raw
 * @returns {import('./types').EqlTarget}
 */
function resolveTarget(raw) {
  const key = String(raw).toLowerCase().replace(/-/g, '_');
  for (const [canonical, aliases] of Object.entries(TARGET_ALIASES)) {
    if (aliases.includes(key) || canonical === key) {
      return /** @type {import('./types').EqlTarget} */ (canonical);
    }
  }
  throw new EqlParseError(
    `Unknown query target "${raw}". Expected Subjects, Observations, Evidence, Claims, Outcomes, Recommendations, Replay Sessions, Calibrations, Accuracies, Strategy Packs, Trades, Screenshots, Daily Reviews, Weekly Reviews, Best Hypotheses, Trade Calibrations, Findings, Similar Trades, or Periods`
  );
}

/**
 * @param {Token} tok
 */
function describeToken(tok) {
  if (!tok || tok.type === 'EOF') return 'end of input';
  if (tok.type === 'IDENT') return String(tok.value);
  if (tok.type === 'STRING') return JSON.stringify(tok.value);
  if (tok.type === 'NUMBER') return String(tok.value);
  return tok.type;
}

module.exports = {
  parseEql,
  tokenize,
  resolveTarget,
  EqlParseError,
  Parser,
};
