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

  const DEFAULT_SECTION_ORDER = Object.freeze([
    'recommendedDecision',
    'whyRecommended',
    'included',
    'excluded',
    'keyWatchouts',
    'nextStepAfterApproval',
  ]);

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

  function sectionBodyHtml(key, digest) {
    const d = digest || {};
    switch (key) {
      case 'recommendedDecision':
        return (
          '<p data-ord-field="recommendedDecision">' +
          escapeHtml(d.recommendedDecision || '—') +
          '</p>'
        );
      case 'whyRecommended':
        if ((d.whyRecommended || []).length === 1) {
          return (
            '<p data-ord-field="whyRecommended">' +
            escapeHtml(d.whyRecommended[0]) +
            '</p>'
          );
        }
        return listHtml(d.whyRecommended, '—');
      case 'included':
        return listHtml(d.included, 'None included.');
      case 'excluded':
        return listHtml(d.excluded || d.heldBack, 'Nothing held back.');
      case 'keyWatchouts':
        return listHtml(d.keyWatchouts, 'No watchouts.');
      case 'nextStepAfterApproval':
        return (
          '<p data-ord-field="nextStepAfterApproval">' +
          escapeHtml(d.nextStepAfterApproval || '—') +
          '</p>'
        );
      default:
        return '';
    }
  }

  function digestSectionsHtml(digest) {
    const d = digest || {};
    const titles = Object.assign({}, SECTION_TITLES, d.sectionTitles || {});
    const order =
      Array.isArray(d.sectionOrder) && d.sectionOrder.length
        ? d.sectionOrder.filter((k) => k !== 'primaryActions')
        : DEFAULT_SECTION_ORDER.slice();

    let html = '<div class="ord-digest" data-role="operator-digest">';
    order.forEach((key) => {
      const items =
        key === 'excluded'
          ? d.excluded || d.heldBack
          : key === 'whyRecommended'
            ? d.whyRecommended
            : key === 'included'
              ? d.included
              : key === 'keyWatchouts'
                ? d.keyWatchouts
                : null;
      if (
        (key === 'keyWatchouts' || key === 'excluded') &&
        Array.isArray(items) &&
        !items.length
      ) {
        // Still show Held back as empty for transparency when titled Held back?
        // Show excluded/held back even when empty so operators see the section.
        if (key === 'keyWatchouts') return;
      }
      html +=
        '<p><strong data-ord-section="' +
        escapeHtml(key) +
        '">' +
        escapeHtml(titles[key] || key) +
        '</strong></p>' +
        sectionBodyHtml(key, d);
    });
    html += '</div>';
    return html;
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

    const digestBody = digestSectionsHtml(d);

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
    const heldBackTitle =
      /data-ord-section="excluded"[^>]*>\s*Held back\s*</i.test(source) ||
      /<strong[^>]*>\s*Held back\s*<\/strong>/i.test(source);
    return {
      hasDigest: digestIdx >= 0,
      hasEvidenceDrawer: evidenceIdx >= 0,
      hasPrimaryActions: actionsIdx >= 0,
      hasHeldBackSection: heldBackTitle,
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
    DEFAULT_SECTION_ORDER,
    escapeHtml,
    listHtml,
    digestSectionsHtml,
    evidenceRecordHtml,
    evidenceDrawerHtml,
    primaryActionsHtml,
    renderOperatorReviewDigest,
    analyzeOperatorReviewHtml,
  };
});
