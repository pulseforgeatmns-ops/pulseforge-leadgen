'use strict';

/**
 * Apply Campaign Review operator actions (SPEC-034 / ADR-021).
 */

const {
  PROSPECT_REVIEW_STATUS,
  CAMPAIGN_REVIEW_STATUS,
  buildMissionDecision,
  buildMissionRevision,
} = require('./types');
const {
  validateProspectForApproval,
  validateCampaignApproval,
  summarizeQueue,
} = require('./validate');
const { getProspectDetail, sortQueue } = require('./assemble');

/**
 * Normalize action list from inputs.
 * @param {object} inputs
 * @returns {object[]}
 */
function normalizeActions(inputs = {}) {
  if (Array.isArray(inputs.reviewActions)) return inputs.reviewActions.slice();
  if (Array.isArray(inputs.actions)) return inputs.actions.slice();
  if (inputs.action && typeof inputs.action === 'object') return [inputs.action];
  return [];
}

/**
 * Find row by prospect id.
 * @param {object[]} queue
 * @param {string} prospectId
 * @returns {object|null}
 */
function findRow(queue, prospectId) {
  return queue.find((r) => String(r.prospectId) === String(prospectId)) || null;
}

/**
 * Apply a single action to workspace (mutates queue rows in place).
 * @param {object} workspace
 * @param {object} action
 * @param {object} ctx
 * @returns {{ decisions: object[], revisions: object[], exports: object|null, changeParts: string[] }}
 */
function applyAction(workspace, action, ctx = {}) {
  const decisions = [];
  const revisions = [];
  const changeParts = [];
  let exports = null;
  const now = ctx.now || new Date().toISOString();
  const operator = action.operator || ctx.operator || 'operator';
  const type = String(action.type || action.action || '').trim();
  const queue = workspace.queue || [];

  const markModified = (row) => {
    row.lastModified = now;
  };

  const decide = (row, act, summary) => {
    decisions.push(
      buildMissionDecision({
        action: act,
        prospectId: row ? row.prospectId : null,
        operator,
        timestamp: now,
        summary,
        revision: workspace.summary && workspace.summary.revision,
      })
    );
  };

  if (type === 'approve' || type === 'approve_selected') {
    const ids =
      type === 'approve_selected'
        ? selectedIds(action, workspace)
        : [String(action.prospectId)];
    for (const id of ids) {
      const row = findRow(queue, id);
      if (!row) continue;
      const gate = validateProspectForApproval(row, {
        confidenceThreshold: workspace.confidenceThreshold,
      });
      if (!gate.ok) {
        row.status = PROSPECT_REVIEW_STATUS.BLOCKED;
        row.validationErrors = gate.errors;
        changeParts.push(`blocked:${id}`);
        continue;
      }
      row.status = PROSPECT_REVIEW_STATUS.APPROVED;
      row.validationErrors = [];
      markModified(row);
      decide(row, 'approve', `Approved prospect ${row.company}`);
      changeParts.push(`approve:${id}`);
    }
  } else if (type === 'reject' || type === 'reject_selected') {
    const ids =
      type === 'reject_selected'
        ? selectedIds(action, workspace)
        : [String(action.prospectId)];
    for (const id of ids) {
      const row = findRow(queue, id);
      if (!row) continue;
      row.status = PROSPECT_REVIEW_STATUS.REJECTED;
      markModified(row);
      decide(row, 'reject', `Rejected prospect ${row.company}`);
      changeParts.push(`reject:${id}`);
    }
  } else if (type === 'skip') {
    const row = findRow(queue, action.prospectId);
    if (row) {
      row.status = PROSPECT_REVIEW_STATUS.SKIPPED;
      row.skipped = true;
      row.required = false;
      markModified(row);
      decide(row, 'skip', `Skipped prospect ${row.company}`);
      changeParts.push(`skip:${row.prospectId}`);
    }
  } else if (type === 'edit_letter') {
    const row = findRow(queue, action.prospectId);
    if (row) {
      const letter = { ...(row.letter || {}), ...(action.letter || {}) };
      if (action.body != null) letter.body = String(action.body);
      if (action.personalizedOpening != null) {
        letter.personalizedOpening = String(action.personalizedOpening);
      }
      row.letter = letter;
      row.letterPreview = String(letter.body || letter.personalizedOpening || '').slice(
        0,
        280
      );
      markModified(row);
      // Revalidate after edit — clear validation_failed if letter now present
      revalidateRow(row, workspace.confidenceThreshold);
      decide(row, 'edit_letter', `Edited letter for ${row.company}`);
      revisions.push(
        buildMissionRevision({
          reason: 'letter_edit',
          operator,
          timestamp: now,
          changeSummary: `Edited letter for ${row.company}`,
          revision: (workspace.summary && workspace.summary.revision) || 1,
        })
      );
      changeParts.push(`edit_letter:${row.prospectId}`);
    }
  } else if (type === 'regenerate' || type === 'regenerate_selected') {
    const ids =
      type === 'regenerate_selected'
        ? selectedIds(action, workspace)
        : [String(action.prospectId)];
    for (const id of ids) {
      const row = findRow(queue, id);
      if (!row) continue;
      // Thin-slice regenerate: bump confidence floor from package facts and clear edit flag
      row.regeneratedAt = now;
      if (row.letter && row.company) {
        row.letter = {
          ...row.letter,
          personalizedOpening:
            row.letter.personalizedOpening ||
            `Following up with ${row.company} on a local service fit.`,
          body:
            row.letter.body ||
            `Dear ${row.recipient || row.company},\n\nWe noticed ${row.company} and wanted to introduce our commercial cleaning service.\n`,
        };
        row.letterPreview = String(row.letter.body).slice(0, 280);
      }
      revalidateRow(row, workspace.confidenceThreshold);
      if (row.status === PROSPECT_REVIEW_STATUS.APPROVED) {
        row.status = PROSPECT_REVIEW_STATUS.NEEDS_REVIEW;
      }
      markModified(row);
      decide(row, 'regenerate', `Regenerated letter for ${row.company}`);
      revisions.push(
        buildMissionRevision({
          reason: 'regenerate',
          operator,
          timestamp: now,
          changeSummary: `Regenerated letter for ${row.company}`,
          revision: (workspace.summary && workspace.summary.revision) || 1,
        })
      );
      changeParts.push(`regenerate:${id}`);
    }
  } else if (type === 'replace_recipient') {
    const row = findRow(queue, action.prospectId);
    if (row) {
      const name = String(action.recipientName || action.recipient || '').trim();
      if (name) {
        row.recipient = name;
        if (row.letter) row.letter = { ...row.letter, recipientName: name };
        if (row.envelope) row.envelope = { ...row.envelope, recipientName: name };
        revalidateRow(row, workspace.confidenceThreshold);
        markModified(row);
        decide(row, 'replace_recipient', `Replaced recipient for ${row.company}`);
        changeParts.push(`replace_recipient:${row.prospectId}`);
      }
    }
  } else if (type === 'update_address') {
    const row = findRow(queue, action.prospectId);
    if (row) {
      const address = String(action.address || action.mailingAddress || '').trim();
      if (address) {
        row.address = address;
        if (row.envelope) {
          row.envelope = { ...row.envelope, mailingAddress: address };
        } else {
          row.envelope = {
            recipientName: row.recipient,
            companyName: row.company,
            mailingAddress: address,
            returnAddress: '',
          };
        }
        // Clear address-related validation
        row.validationErrors = (row.validationErrors || []).filter(
          (e) => e !== 'missing_address'
        );
        revalidateRow(row, workspace.confidenceThreshold);
        markModified(row);
        decide(row, 'update_address', `Updated address for ${row.company}`);
        changeParts.push(`update_address:${row.prospectId}`);
      }
    }
  } else if (type === 'add_note') {
    const row = findRow(queue, action.prospectId);
    if (row) {
      row.operatorNote = String(action.note || action.operatorNote || '').trim();
      markModified(row);
      decide(row, 'add_note', `Noted on ${row.company}`);
      changeParts.push(`add_note:${row.prospectId}`);
    }
  } else if (type === 'export_selected' || type === 'print_selected') {
    const ids = selectedIds(action, workspace);
    const selected = queue.filter((r) => ids.includes(String(r.prospectId)));
    exports = buildSelectionExport(selected, type, workspace);
    changeParts.push(`${type}:${ids.length}`);
  } else if (type === 'approve_campaign') {
    const gate = validateCampaignApproval(workspace);
    if (gate.ok) {
      workspace.summary.status = CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT;
      workspace.campaignApproved = true;
      decide(null, 'approve_campaign', 'Campaign approved — Ready to Print');
      changeParts.push('approve_campaign');
    } else {
      workspace.summary.status = CAMPAIGN_REVIEW_STATUS.BLOCKED;
      workspace.campaignApprovalErrors = gate.errors;
      changeParts.push(`approve_campaign_blocked:${gate.errors.join(',')}`);
    }
  } else if (type === 'restore_revision') {
    workspace.restoreRequested = Number(action.revision);
    changeParts.push(`restore_revision:${action.revision}`);
  } else if (type === 'duplicate_revision') {
    workspace.duplicateRequested = true;
    changeParts.push('duplicate_revision');
  } else if (type === 'compare_revisions') {
    workspace.compareRequested = {
      a: Number(action.revisionA),
      b: Number(action.revisionB),
    };
    changeParts.push(
      `compare_revisions:${action.revisionA}:${action.revisionB}`
    );
  }

  // Refresh summary counts after mutations
  const counts = summarizeQueue(queue);
  workspace.summary = {
    ...workspace.summary,
    ...counts,
  };
  if (workspace.summary.status !== CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT) {
    workspace.summary.status =
      counts.blockedCount > 0 && counts.readyCount === 0
        ? CAMPAIGN_REVIEW_STATUS.BLOCKED
        : CAMPAIGN_REVIEW_STATUS.IN_REVIEW;
  }

  if (workspace.selectedProspectId) {
    workspace.prospectDetail = getProspectDetail(
      workspace,
      workspace.selectedProspectId
    );
  }

  return { decisions, revisions, exports, changeParts };
}

/**
 * @param {object} action
 * @param {object} workspace
 * @returns {string[]}
 */
function selectedIds(action, workspace) {
  if (Array.isArray(action.prospectIds) && action.prospectIds.length) {
    return action.prospectIds.map(String);
  }
  if (Array.isArray(workspace.selectedProspectIds)) {
    return workspace.selectedProspectIds.map(String);
  }
  return (workspace.queue || []).map((r) => String(r.prospectId));
}

/**
 * @param {object} row
 * @param {number} threshold
 */
function revalidateRow(row, threshold) {
  if (row.status === PROSPECT_REVIEW_STATUS.SKIPPED) return;
  // Drop stale validation_failed when operator fixed fields
  const gate = validateProspectForApproval(
    {
      ...row,
      validationErrors: (row.validationErrors || []).filter(
        (e) => e !== 'validation_failed'
      ),
      mailValidationFailed: false,
    },
    { confidenceThreshold: threshold }
  );
  if (!gate.ok) {
    if (row.status === PROSPECT_REVIEW_STATUS.APPROVED) {
      row.status = PROSPECT_REVIEW_STATUS.NEEDS_REVIEW;
    } else {
      row.status = PROSPECT_REVIEW_STATUS.BLOCKED;
    }
    row.validationErrors = gate.errors;
  } else if (row.status === PROSPECT_REVIEW_STATUS.BLOCKED) {
    row.status = PROSPECT_REVIEW_STATUS.NEEDS_REVIEW;
    row.validationErrors = [];
  } else {
    row.validationErrors = [];
  }
}

/**
 * @param {object[]} selected
 * @param {string} type
 * @param {object} workspace
 * @returns {object}
 */
function buildSelectionExport(selected, type, workspace) {
  const campaignName =
    (workspace.summary && workspace.summary.campaignName) || 'Campaign';
  const lines = selected.map((r) => {
    return [
      r.recipient,
      r.company,
      r.address,
      r.status,
      r.confidence,
    ].join(',');
  });
  const csv = ['recipient,company,address,status,confidence', ...lines].join(
    '\n'
  );
  const html = `<!doctype html><html><head><title>${campaignName} — Selected</title></head><body>${selected
    .map(
      (r) =>
        `<section><h2>${escapeHtml(r.company)}</h2><pre>${escapeHtml(
          (r.letter && r.letter.body) || r.letterPreview || ''
        )}</pre></section>`
    )
    .join('\n')}</body></html>`;

  return {
    type,
    prospectIds: selected.map((r) => r.prospectId),
    mailMergeCsv: csv,
    addressLabelCsv: selected
      .map((r) => `"${r.recipient}","${r.company}","${r.address}"`)
      .join('\n'),
    printableHtml: type === 'print_selected' ? html : null,
    exportCsv: type === 'export_selected' ? csv : null,
  };
}

function escapeHtml(s) {
  return String(s || '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

/**
 * Apply all actions to a workspace; returns updated workspace + side effects.
 * @param {object} workspace
 * @param {object[]} actions
 * @param {object} [ctx]
 * @returns {object}
 */
function applyReviewActions(workspace, actions = [], ctx = {}) {
  const allDecisions = [];
  const allRevisions = [];
  const changeParts = [];
  const exportArtifacts = [];
  let working = {
    ...workspace,
    queue: (workspace.queue || []).map((r) => ({ ...r })),
    summary: { ...(workspace.summary || {}) },
  };

  if (ctx.sort) {
    working.queue = sortQueue(working.queue, ctx.sort);
  }

  for (const action of actions) {
    const result = applyAction(working, action, ctx);
    allDecisions.push(...result.decisions);
    allRevisions.push(...result.revisions);
    changeParts.push(...result.changeParts);
    if (result.exports) exportArtifacts.push(result.exports);
  }

  return {
    workspace: working,
    decisions: allDecisions,
    revisions: allRevisions,
    changeSummary: changeParts.join('; ') || 'assembled workspace',
    exportArtifacts,
  };
}

/**
 * Build execution package when campaign is Ready to Print.
 * @param {object} workspace
 * @returns {object|null}
 */
function buildExecutionPackage(workspace) {
  if (
    !workspace.campaignApproved &&
    !(
      workspace.summary &&
      workspace.summary.status === CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT
    )
  ) {
    return null;
  }

  const approved = (workspace.queue || []).filter(
    (r) => r.status === PROSPECT_REVIEW_STATUS.APPROVED
  );
  const campaignName =
    (workspace.summary && workspace.summary.campaignName) || 'Campaign';
  const revision = (workspace.summary && workspace.summary.revision) || 1;

  const mailMergeCsv = [
    'recipient_name,company_name,mailing_address,status',
    ...approved.map(
      (r) =>
        `"${r.recipient}","${r.company}","${r.address}","ready_to_print"`
    ),
  ].join('\n');

  const addressLabelCsv = [
    'recipient_name,company_name,mailing_address',
    ...approved.map((r) => `"${r.recipient}","${r.company}","${r.address}"`),
  ].join('\n');

  const printPackageHtml = `<!doctype html><html><head><meta charset="utf-8"><title>${escapeHtml(
    campaignName
  )} — Print Package r${revision}</title></head><body>
<h1>${escapeHtml(campaignName)} — Ready to Print</h1>
<p>Revision ${revision} · ${approved.length} prospect(s)</p>
${approved
  .map(
    (r) => `<article>
  <h2>${escapeHtml(r.company)}</h2>
  <p>${escapeHtml(r.recipient)} · ${escapeHtml(r.address)}</p>
  <pre>${escapeHtml((r.letter && r.letter.body) || r.letterPreview || '')}</pre>
</article>`
  )
  .join('\n')}
</body></html>`;

  return {
    status: CAMPAIGN_REVIEW_STATUS.READY_TO_PRINT,
    revision,
    campaignName,
    prospectCount: approved.length,
    printPackage: { format: 'printable_html', html: printPackageHtml },
    mailMerge: { format: 'csv', csv: mailMergeCsv },
    addressLabels: { format: 'csv', csv: addressLabelCsv },
    approvedProspectIds: approved.map((r) => r.prospectId),
  };
}

module.exports = {
  normalizeActions,
  applyAction,
  applyReviewActions,
  buildExecutionPackage,
  validateProspectForApproval,
  validateCampaignApproval,
};
