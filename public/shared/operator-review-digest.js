'use strict';

/**
 * Operator Review Digest HTML renderer (shared by /client-intel + Node tests).
 * Digest is the default operator view; evidence lives in a collapsed details drawer.
 */
(function (root, factory) {
  if (typeof module === 'object' && module.exports) {
    module.exports = factory();
  } else {
    root.PulseforgeOperatorReviewDigest = factory();
  }
})(typeof self !== 'undefined' ? self : this, function () {
  const SECTION_TITLES = Object.freeze({
    recommendedDecision: 'Recommended decision',
    whyRecommended: 'Why this is recommended',
    included: 'What is included',
    excluded: 'What is excluded / held back',
    keyWatchouts: 'Key watchouts',
    nextStepAfterApproval: 'Next step after approval',
    primaryActions: 'Primary actions',
  });

  function escapeHtml(value) {
    return String(value == null ? '' : value)
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#39;');
  }

  function listHtml(items, empty) {
    const list = Array.isArray(items) ? items.filter(Boolean) : [];
    if (!list.length) {
      return '<p class="blueprint-empty">' + escapeHtml(empty || 'None.') + '</p>';
    }
    return (
      '<ul class="ord-list">' +
      list.map((item) => '<li>' + escapeHtml(item) + '</li>').join('') +
      '</ul>'
    );
  }

  function evidenceRecordHtml(row) {
    const r = row || {};
    const company = r.companyName || r.company || r.name || '—';
    const fields = [
      ['Company', company],
      ['Location', r.location || '—'],
      ['Source URL', r.sourceUrl || r.website || r.url || '—'],
      [
        'Fit rationale',
        r.fitRationale || r.whyItFits || r.fitReason || '—',
      ],
      [
        'Risk / uncertainty',
        r.riskUncertainty || r.risks || r.statusReason || '—',
      ],
      ['Confidence', r.confidence || '—'],
      ['Review status', r.reviewStatus || r.status || '—'],
    ];
    if (r.suggestedContactRole) {
      fields.push(['Suggested contact role', r.suggestedContactRole]);
    }
    if (r.relationship) fields.push(['Relationship', r.relationship]);
    if (r.rejectionReason || r.statusReason) {
      fields.push([
        'Rejection / hold reason',
        r.rejectionReason || r.statusReason,
      ]);
    }
    if (r.auditNote) fields.push(['Audit note', r.auditNote]);
    if (r.doNotOutreach) {
      fields.push(['Outreach', 'do not include in campaign outreach']);
    }

    const isUrl = (label, value) =>
      /source url/i.test(label) && /^https?:\/\//i.test(String(value || ''));

    return (
      '<article class="ord-evidence-record">' +
      '<h5>' +
      escapeHtml(company) +
      '</h5>' +
      '<dl class="ord-evidence-dl">' +
      fields
        .map(([label, value]) => {
          const valHtml = isUrl(label, value)
            ? '<a href="' +
              escapeHtml(value) +
              '" target="_blank" rel="noopener noreferrer">' +
              escapeHtml(value) +
              '</a>'
            : escapeHtml(value);
          return (
            '<div><dt>' +
            escapeHtml(label) +
            '</dt><dd>' +
            valHtml +
            '</dd></div>'
          );
        })
        .join('') +
      '</dl>' +
      '</article>'
    );
  }

  function evidenceDrawerHtml(evidence, opts) {
    const e = evidence || {};
    const open = opts && opts.open === true;
    const label = e.label || 'View evidence';
    const sections = Array.isArray(e.sections) ? e.sections : [];
    let body = '';

    if (sections.length) {
      body = sections
        .map((section) => {
          const records = section.records || section.rows || [];
          return (
            '<section class="ord-evidence-section">' +
            '<h4>' +
            escapeHtml(section.title || 'Evidence') +
            '</h4>' +
            (section.intro
              ? '<p class="ord-evidence-intro">' +
                escapeHtml(section.intro) +
                '</p>'
              : '') +
            (records.length
              ? records.map(evidenceRecordHtml).join('')
              : '<p class="blueprint-empty">None.</p>') +
            '</section>'
          );
        })
        .join('');
    } else if (Array.isArray(e.records) && e.records.length) {
      body = e.records.map(evidenceRecordHtml).join('');
    } else {
      body = '<p class="blueprint-empty">No evidence records.</p>';
    }

    if (Array.isArray(e.rejectedOrHeld) && e.rejectedOrHeld.length) {
      body +=
        '<section class="ord-evidence-section">' +
        '<h4>Rejected / held candidates</h4>' +
        e.rejectedOrHeld.map(evidenceRecordHtml).join('') +
        '</section>';
    }

    if (Array.isArray(e.auditNotes) && e.auditNotes.length) {
      body +=
        '<section class="ord-evidence-section">' +
        '<h4>Audit notes</h4>' +
        listHtml(e.auditNotes) +
        '</section>';
    }

    return (
      '<details class="ord-evidence-drawer" data-role="view-evidence"' +
      (open ? ' open' : '') +
      '>' +
      '<summary>' +
      escapeHtml(label) +
      '</summary>' +
      '<div class="ord-evidence-body" data-role="evidence-body">' +
      body +
      '</div>' +
      '</details>'
    );
  }

  function primaryActionsHtml(actions, opts) {
    const list = Array.isArray(actions) ? actions : [];
    if (!list.length) return '';
    const disabled = opts && opts.disabled;
    return (
      '<div class="ord-primary-actions" data-role="primary-actions">' +
      list
        .map((a) => {
          const style = a.style === 'primary' ? '' : ' secondary';
          const msg = a.message
            ? ' data-ord-message="' + escapeHtml(a.message) + '"'
            : '';
          return (
            '<button type="button" class="' +
            style.trim() +
            '" data-ord-action="' +
            escapeHtml(a.id || '') +
            '"' +
            msg +
            (disabled ? ' disabled' : '') +
            '>' +
            escapeHtml(a.label || '') +
            '</button>'
          );
        })
        .join('') +
      '</div>'
    );
  }

  /**
   * Render a full Operator Review Digest panel.
   * Order: title → digest sections → primary actions → evidence drawer (collapsed).
   */
  function renderOperatorReviewDigest(digest, opts) {
    const d = digest || {};
    const options = opts || {};
    const kicker = options.kicker || d.title || 'Operator Review';
    const actionsBeforeEvidence = options.actionsBeforeEvidence !== false;

    const digestBody =
      '<div class="ord-digest" data-role="operator-digest">' +
      '<p><strong>' +
      escapeHtml(SECTION_TITLES.recommendedDecision) +
      '</strong></p>' +
      '<p data-ord-field="recommendedDecision">' +
      escapeHtml(d.recommendedDecision || '—') +
      '</p>' +
      '<p><strong>' +
      escapeHtml(SECTION_TITLES.whyRecommended) +
      '</strong></p>' +
      listHtml(d.whyRecommended, '—') +
      '<p><strong>' +
      escapeHtml(SECTION_TITLES.included) +
      '</strong></p>' +
      listHtml(d.included, 'None included.') +
      '<p><strong>' +
      escapeHtml(SECTION_TITLES.excluded) +
      '</strong></p>' +
      listHtml(d.excluded, 'Nothing held back.') +
      '<p><strong>' +
      escapeHtml(SECTION_TITLES.keyWatchouts) +
      '</strong></p>' +
      listHtml(d.keyWatchouts, 'No watchouts.') +
      '<p><strong>' +
      escapeHtml(SECTION_TITLES.nextStepAfterApproval) +
      '</strong></p>' +
      '<p data-ord-field="nextStepAfterApproval">' +
      escapeHtml(d.nextStepAfterApproval || '—') +
      '</p>' +
      '</div>';

    const actionsHtml = primaryActionsHtml(d.primaryActions, {
      disabled: options.actionsDisabled,
    });
    const evidenceHtml = evidenceDrawerHtml(d.evidence, {
      open: options.evidenceOpen === true,
    });

    const closing =
      options.closingQuestion || d.closingQuestion
        ? '<p class="ord-closing">' +
          escapeHtml(options.closingQuestion || d.closingQuestion) +
          '</p>'
        : '';

    const disclaimer = d.disclaimer
      ? '<p class="growth-disclaimer">' + escapeHtml(d.disclaimer) + '</p>'
      : '';

    return (
      '<div class="growth-direction operator-review-digest" id="' +
      escapeHtml(options.elementId || 'operatorReviewDigest') +
      '" data-pattern="operator_review_digest">' +
      '<p class="growth-kicker">' +
      escapeHtml(kicker) +
      '</p>' +
      digestBody +
      (actionsBeforeEvidence ? actionsHtml : '') +
      closing +
      evidenceHtml +
      (actionsBeforeEvidence ? '' : actionsHtml) +
      disclaimer +
      '</div>'
    );
  }

  /**
   * Helpers for tests: locate digest vs evidence in rendered HTML.
   */
  function analyzeOperatorReviewHtml(html) {
    const source = String(html || '');
    const digestIdx = source.indexOf('data-role="operator-digest"');
    const evidenceIdx = source.indexOf('data-role="view-evidence"');
    const actionsIdx = source.indexOf('data-role="primary-actions"');
    const evidenceOpen = /data-role="view-evidence"[^>]*\sopen\b/i.test(source);
    return {
      hasDigest: digestIdx >= 0,
      hasEvidenceDrawer: evidenceIdx >= 0,
      hasPrimaryActions: actionsIdx >= 0,
      digestBeforeEvidence:
        digestIdx >= 0 && evidenceIdx >= 0 && digestIdx < evidenceIdx,
      actionsBeforeEvidence:
        actionsIdx >= 0 && evidenceIdx >= 0 && actionsIdx < evidenceIdx,
      evidenceCollapsedByDefault: evidenceIdx >= 0 && !evidenceOpen,
      digestIndex: digestIdx,
      evidenceIndex: evidenceIdx,
      actionsIndex: actionsIdx,
    };
  }

  return {
    SECTION_TITLES,
    escapeHtml,
    listHtml,
    evidenceRecordHtml,
    evidenceDrawerHtml,
    primaryActionsHtml,
    renderOperatorReviewDigest,
    analyzeOperatorReviewHtml,
  };
});
