'use strict';
// Inert protocol fixture only. Never import a business handler or provider.
process.on('disconnect', () => process.exit(74));
process.on('message', message => {
  if (message?.type !== 'run') return;
  process.send({ ...message, type: 'terminal',
    outcome: { status: 'held', reason: 'anchor_recovery_separate_preparation_approval_required' },
    evidenceRefs: { programId: 'synthetic-held-program' } }, () => process.exit(0));
});
process.send({ type: 'ready' });
