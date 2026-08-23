'use strict';

/**
 * SPEC-145 — Investigation journal.
 * Every investigation leaves reasoning, not just conclusions.
 */

function buildJournalEntry(partial = {}) {
  return {
    sequence: partial.sequence != null ? partial.sequence : 0,
    at: partial.at || new Date().toISOString(),
    question: partial.question || null,
    priorUnknowns: Array.isArray(partial.priorUnknowns) ? partial.priorUnknowns : [],
    selectedProvider: partial.selectedProvider || null,
    providerLabel: partial.providerLabel || null,
    gap: partial.gap || null,
    rationale: partial.rationale || '',
    expectedInformationGain: partial.expectedInformationGain != null ? partial.expectedInformationGain : null,
    outcome: partial.outcome || 'pending',
    evidenceCollected: Array.isArray(partial.evidenceCollected) ? partial.evidenceCollected : [],
    resolvedGaps: Array.isArray(partial.resolvedGaps) ? partial.resolvedGaps : [],
    failed: partial.failed === true,
    nextQuestion: partial.nextQuestion || null,
    coveragePct: partial.coveragePct != null ? partial.coveragePct : null,
    boardSnapshot: partial.boardSnapshot || null,
  };
}

function createInvestigationJournal(missionId = null) {
  return {
    missionId,
    startedAt: new Date().toISOString(),
    entries: [],
    startedWith: null,
  };
}

function recordJournalStart(journal, input = {}) {
  journal.startedWith = input.startedWith || input.question || 'Identify highest-value unknowns';
  journal.entries.push(
    buildJournalEntry({
      sequence: 0,
      question: journal.startedWith,
      priorUnknowns: input.priorUnknowns || [],
      outcome: 'started',
      rationale: input.rationale || 'Investigation initialized from market understanding and memory.',
      coveragePct: input.coveragePct,
    })
  );
  return journal;
}

function recordJournalStep(journal, input = {}) {
  const sequence = journal.entries.length;
  journal.entries.push(
    buildJournalEntry({
      sequence,
      question: input.question,
      priorUnknowns: input.priorUnknowns,
      selectedProvider: input.selectedProvider,
      providerLabel: input.providerLabel,
      gap: input.gap,
      rationale: input.rationale,
      expectedInformationGain: input.expectedInformationGain,
      outcome: input.outcome,
      evidenceCollected: input.evidenceCollected,
      resolvedGaps: input.resolvedGaps,
      failed: input.failed,
      nextQuestion: input.nextQuestion,
      coveragePct: input.coveragePct,
      boardSnapshot: input.boardSnapshot,
    })
  );
  return journal;
}

function recordJournalStop(journal, input = {}) {
  journal.stoppedAt = new Date().toISOString();
  journal.stopReason = input.stopReason || null;
  journal.stopExplanation = input.stopExplanation || '';
  journal.finalCoveragePct = input.coveragePct;
  journal.entries.push(
    buildJournalEntry({
      sequence: journal.entries.length,
      question: 'Stop investigation',
      outcome: 'stopped',
      rationale: input.stopExplanation || input.stopReason,
      coveragePct: input.coveragePct,
      priorUnknowns: input.remainingUnknowns || [],
    })
  );
  return journal;
}

/**
 * Render a human-readable investigation trail.
 * @param {object} journal
 * @returns {string[]}
 */
function renderJournalTrail(journal) {
  const lines = [];
  if (journal.startedWith) lines.push(`Started with: ${journal.startedWith}`);

  for (const entry of journal.entries || []) {
    if (entry.outcome === 'started' || entry.outcome === 'stopped') continue;
    if (entry.question) {
      lines.push(entry.question.startsWith('Need ') ? entry.question : `Need ${entry.question}`);
    }
    if (entry.rationale) {
      lines.push(entry.rationale);
    } else if (entry.selectedProvider) {
      lines.push(`${entry.providerLabel || entry.selectedProvider} selected`);
    }
    if (entry.outcome === 'resolved' || (entry.resolvedGaps || []).length > 0) {
      const resolved = (entry.resolvedGaps || []).join(', ') || entry.gap;
      lines.push(`Verified ${resolved}`);
    } else if (entry.failed) {
      lines.push(`Insufficient evidence from ${entry.providerLabel || entry.selectedProvider}`);
    }
    if (entry.nextQuestion) lines.push(`Next question became: ${entry.nextQuestion}`);
  }

  if (journal.stopExplanation) lines.push(`Stopped because: ${journal.stopExplanation}`);
  return lines;
}

function serializeJournal(journal) {
  return {
    missionId: journal.missionId,
    startedAt: journal.startedAt,
    stoppedAt: journal.stoppedAt || null,
    startedWith: journal.startedWith,
    stopReason: journal.stopReason || null,
    stopExplanation: journal.stopExplanation || null,
    finalCoveragePct: journal.finalCoveragePct != null ? journal.finalCoveragePct : null,
    entryCount: (journal.entries || []).length,
    entries: journal.entries || [],
    trail: renderJournalTrail(journal),
  };
}

module.exports = {
  buildJournalEntry,
  createInvestigationJournal,
  recordJournalStart,
  recordJournalStep,
  recordJournalStop,
  renderJournalTrail,
  serializeJournal,
};
