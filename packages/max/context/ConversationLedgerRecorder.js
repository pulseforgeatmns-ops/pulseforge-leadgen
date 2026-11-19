'use strict';

/**
 * Default-off maintenance of session.conversationLedger
 * (P0-MAX-REASONING-001 PR 3). Enabled only by MAX_CONVERSATION_LEDGER=true.
 *
 * Runs after production has produced its answer and before
 * ConversationContinuity snapshots the session, so durability (when
 * MAX_CONVERSATION_CONTEXT is also on) persists the updated ledger. It never
 * throws into a turn, never reads or writes routing, mission, approval or
 * outbound state, and nothing in production routing reads its output.
 */

const {
  createLedger, applyTurn, isExpired, resolveFocus, viewLedger,
} = require('./ConversationLedger');
const { ledgerTurnFromResult } = require('./LedgerTurnAdapter');
const { produceLexicalFrame } = require('../interpretation/producers/LexicalFrameProducer');
const { recordObserverHealth } = require('../observability/ObserverHealth');

function ledgerEnabled(env = process.env) { return env.MAX_CONVERSATION_LEDGER === 'true'; }

class ConversationLedgerRecorder {
  /**
   * @param {{ env?: object, now?: () => number, frameProducer?: (input: { text: string, context: object }) => object }} options
   *   frameProducer must return a validated TurnFrame (validateTurnFrame); it
   *   defaults to the deterministic lexical producer (no network, no cost).
   */
  constructor({ env = process.env, now = Date.now, frameProducer = null } = {}) {
    this.enabled = ledgerEnabled(env);
    this.now = now;
    this.frameProducer = frameProducer;
  }

  frameContext(ledger) {
    const live = ledger.referents.filter((r) => r.status === 'active');
    const pending = live.find((r) => r.kind === 'pending_decision');
    return {
      tenantId: ledger.tenantId,
      candidates: live.map((r) => ({ id: r.id, kind: r.kind === 'prospect_set' ? 'segment' : r.kind, label: r.label })),
      focusIds: [...ledger.focus],
      pendingDecisionId: pending ? pending.id : null,
    };
  }

  frame(text, ledger) {
    try {
      const produce = this.frameProducer || produceLexicalFrame;
      const frame = produce({ text, context: this.frameContext(ledger) });
      return frame && frame.authority && frame.authority.grantsAuthority === false ? frame : null;
    } catch (_) {
      return null;
    }
  }

  /** Update the session's ledger for one completed turn. Never throws. */
  observe({ session, text, result }) {
    if (!this.enabled || !session || !session.context || session.context.tenantId == null) return null;
    try {
      const now = this.now();
      const tenantId = String(session.context.tenantId);
      const sessionId = String(session.id);
      let ledger = session.conversationLedger;
      // A ledger from another tenant or session is never reused.
      if (ledger && (ledger.tenantId !== tenantId || ledger.sessionId !== sessionId)) ledger = null;
      let expired = false;
      if (ledger && isExpired(ledger, now)) { ledger = null; expired = true; }
      ledger = ledger || createLedger({ tenantId, sessionId, now, expiredPredecessor: expired });
      const frame = this.frame(String(text || ''), ledger);
      const turn = ledgerTurnFromResult({ session, tenantId, text, result, frame, ledger, now });
      session.conversationLedger = applyTurn(ledger, turn, now);
      recordObserverHealth('conversation_ledger', 'success');
      return session.conversationLedger;
    } catch (_) {
      recordObserverHealth('conversation_ledger', 'failure', { reason: 'ledger_update_failed' });
      return null;
    }
  }

  /** Read API for future consumers: remembered context, evidence refs, authority (none). */
  context(session, now = this.now()) {
    if (!this.enabled || !session?.conversationLedger) return null;
    const ledger = session.conversationLedger;
    if (String(session.context?.tenantId) !== ledger.tenantId || String(session.id) !== ledger.sessionId) return null;
    if (isExpired(ledger, now)) return null;
    return { focus: resolveFocus(ledger, now), view: viewLedger(ledger, now) };
  }
}

module.exports = { ConversationLedgerRecorder, ledgerEnabled };
