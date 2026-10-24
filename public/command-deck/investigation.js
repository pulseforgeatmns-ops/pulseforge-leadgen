'use strict';

/**
 * Intelligence Navigation — SPEC-010.
 * Investigation trail + progressive views on Command Deck.
 * Render-only: never scores or invents.
 */

(function (global) {
  const TRAIL_KINDS = Object.freeze({
    DECK: 'deck',
    RECOMMENDATION: 'recommendation',
    COMPANY: 'company',
    EVIDENCE: 'evidence',
    INTERACTION: 'interaction',
  });

  /**
   * @param {object} options
   * @param {function} options.apiRequest
   * @param {function} options.escapeHtml
   * @param {function} options.announce
   * @param {function} [options.onAskMax]
   * @param {function} [options.onFocusChange] - ({ page, companyId, recommendationId, selectedEntity, trail })
   * @param {function} [options.onClose]
   * @param {function} [options.onReviewLive]
   * @param {function} [options.onOperatorEvent] - SPEC-012 interaction callback
   */
  function createInvestigation(options) {
    const apiRequest = options.apiRequest;
    const escapeHtml = options.escapeHtml;
    const announce = options.announce || function () {};
    const onAskMax = options.onAskMax || function () {};
    const onFocusChange = options.onFocusChange || function () {};
    const onClose = options.onClose || function () {};
    const onReviewLive = options.onReviewLive || function () {};
    const onOperatorEvent = options.onOperatorEvent || function () {};

    /** @type {{ kind: string, id: string|null, label: string }[]} */
    let trail = [];
    /** @type {object|null} */
    let currentModel = null;
    /** @type {string|null} */
    let evidenceFocusId = null;
    /** @type {'summary'|'item'|'raw'} */
    let evidenceDepth = 'summary';
    let syncingHash = false;
    /** @type {object[]|null} */
    let pendingLiveEvents = null;

    const root = document.getElementById('cdInvestigation');
    const trailEl = document.getElementById('cdTrail');
    const bodyEl = document.getElementById('cdInvestigationBody');
    const liveBanner = document.getElementById('cdInvLiveBanner');
    const deckMain = document.getElementById('command-deck-main');

    function isOpen() {
      return root && !root.hidden;
    }

    function pushTrail(step) {
      const kind = String((step && step.kind) || TRAIL_KINDS.DECK);
      const id = step && step.id != null ? String(step.id) : null;
      const label =
        step && step.label != null && String(step.label).trim()
          ? String(step.label).trim()
          : kind;
      const last = trail[trail.length - 1];
      if (
        last &&
        last.kind === kind &&
        String(last.id || '') === String(id || '')
      ) {
        return;
      }
      trail.push({ kind, id, label });
    }

    function focusPayload() {
      const tip = trail[trail.length - 1] || null;
      let page = 'command-deck';
      let companyId = null;
      let recommendationId = null;
      let selectedEntity = null;

      for (const step of trail) {
        if (step.kind === TRAIL_KINDS.COMPANY && step.id) companyId = step.id;
        if (step.kind === TRAIL_KINDS.RECOMMENDATION && step.id) {
          recommendationId = step.id;
          const parsed = parseRecId(step.id);
          if (parsed) companyId = parsed.companyId;
        }
      }

      if (!tip || tip.kind === TRAIL_KINDS.DECK) {
        page = 'command-deck';
      } else if (tip.kind === TRAIL_KINDS.RECOMMENDATION) {
        page = 'recommendation';
        selectedEntity = {
          id: tip.id || '',
          type: 'recommendation',
          name: tip.label,
        };
      } else if (tip.kind === TRAIL_KINDS.COMPANY) {
        page = 'company';
        selectedEntity = {
          id: tip.id || '',
          type: 'company',
          name: tip.label,
        };
      } else if (
        tip.kind === TRAIL_KINDS.EVIDENCE ||
        tip.kind === TRAIL_KINDS.INTERACTION
      ) {
        page = recommendationId
          ? 'recommendation'
          : companyId
            ? 'company'
            : 'timeline';
        selectedEntity = {
          id: tip.id || '',
          type: tip.kind,
          name: tip.label,
        };
      }

      return {
        page,
        companyId,
        recommendationId,
        selectedEntity,
        trail: trail.map((s) => ({ ...s })),
      };
    }

    function parseRecId(recommendationId) {
      const raw = String(recommendationId || '');
      if (!raw.startsWith('rec:')) return null;
      const rest = raw.slice(4);
      const idx = rest.indexOf(':');
      if (idx <= 0 || idx === rest.length - 1) return null;
      return { tenantId: rest.slice(0, idx), companyId: rest.slice(idx + 1) };
    }

    function emitFocus() {
      onFocusChange(focusPayload());
    }

    function setHashForTip() {
      const tip = trail[trail.length - 1];
      if (!tip || tip.kind === TRAIL_KINDS.DECK) {
        syncingHash = true;
        if (location.hash && location.hash !== '#') {
          history.replaceState(null, '', location.pathname + location.search);
        }
        syncingHash = false;
        return;
      }
      let hash = '';
      if (tip.kind === TRAIL_KINDS.RECOMMENDATION && tip.id) {
        hash = `#/recommendation/${encodeURIComponent(tip.id)}`;
      } else if (tip.kind === TRAIL_KINDS.COMPANY && tip.id) {
        hash = `#/company/${encodeURIComponent(tip.id)}`;
      } else if (tip.kind === TRAIL_KINDS.EVIDENCE && tip.id) {
        hash = `#/evidence/${encodeURIComponent(tip.id)}`;
      } else if (tip.kind === TRAIL_KINDS.INTERACTION && tip.id) {
        hash = `#/evidence/${encodeURIComponent(tip.id)}`;
      }
      if (hash && location.hash !== hash) {
        syncingHash = true;
        history.pushState({ investigation: true }, '', hash);
        syncingHash = false;
      }
    }

    function renderTrail() {
      if (!trailEl) return;
      if (!trail.length) {
        trailEl.innerHTML = '';
        return;
      }
      trailEl.innerHTML = trail
        .map((step, index) => {
          const isLast = index === trail.length - 1;
          const sep =
            index === 0
              ? ''
              : '<span class="cd-trail-sep" aria-hidden="true">›</span>';
          if (isLast) {
            return `${sep}<span class="cd-trail-current" aria-current="page">${escapeHtml(
              step.label
            )}</span>`;
          }
          return `${sep}<button type="button" class="cd-trail-link" data-trail-index="${index}">${escapeHtml(
            step.label
          )}</button>`;
        })
        .join('');

      trailEl.querySelectorAll('[data-trail-index]').forEach((btn) => {
        btn.addEventListener('click', () => {
          const idx = Number(btn.getAttribute('data-trail-index'));
          popTo(idx);
        });
      });
    }

    function showShell() {
      if (!root) return;
      root.hidden = false;
      if (deckMain) deckMain.classList.add('is-investigating');
      document.body.classList.add('cd-investigating');
    }

    function hideShell() {
      if (!root) return;
      root.hidden = true;
      if (deckMain) deckMain.classList.remove('is-investigating');
      document.body.classList.remove('cd-investigating');
      trail = [];
      currentModel = null;
      evidenceFocusId = null;
      evidenceDepth = 'summary';
      pendingLiveEvents = null;
      hideLiveBanner();
      renderTrail();
      if (bodyEl) bodyEl.innerHTML = '';
      syncingHash = true;
      if (location.hash) {
        history.replaceState(null, '', location.pathname + location.search);
      }
      syncingHash = false;
      onClose();
      emitFocus();
    }

    function showLiveBanner() {
      if (!liveBanner) return;
      liveBanner.hidden = false;
      announce('New intelligence available. Review when ready.');
    }

    function hideLiveBanner() {
      if (!liveBanner) return;
      liveBanner.hidden = true;
    }

    /**
     * SPEC-011: do not interrupt reading — queue a Review banner when focus entity changes.
     * @param {object[]} events
     * @param {string[]} affectedEntityIds
     */
    function noteLiveEvents(events, affectedEntityIds) {
      if (!isOpen()) return;
      const tip = trail[trail.length - 1];
      if (!tip || !tip.id) return;
      const focusId = String(tip.id);
      const affected = new Set((affectedEntityIds || []).map(String));
      const hits = (events || []).filter(
        (e) =>
          (e.entity && String(e.entity.id) === focusId) ||
          affected.has(focusId)
      );
      if (!hits.length && !affected.has(focusId)) return;
      pendingLiveEvents = hits.length ? hits : events || [];
      showLiveBanner();
    }

    async function reviewLiveUpdate() {
      hideLiveBanner();
      const tip = trail[trail.length - 1];
      if (!tip) {
        onReviewLive();
        return;
      }
      if (tip.kind === TRAIL_KINDS.RECOMMENDATION && tip.id) {
        await openRecommendation(tip.id, tip.label, true);
      } else if (tip.kind === TRAIL_KINDS.COMPANY && tip.id) {
        await openCompany(tip.id, tip.label, true);
      }
      pendingLiveEvents = null;
      onReviewLive();
    }

    function renderRelated(related) {
      if (!related) return '';
      const groups = [
        ['Similar companies', related.similarCompanies],
        ['Shared signals', related.sharedSignals],
        ['Competing opportunities', related.competingOpportunities],
        ['Supporting evidence', related.supportingEvidence],
        ['Contradicting evidence', related.contradictingEvidence],
        ['Alternative recommendations', related.alternativeRecommendations],
        ['Other recommendations', related.otherRecommendations],
        ['Source interactions', related.sourceInteractions],
      ];
      const blocks = groups
        .filter(([, items]) => items && items.length)
        .map(([title, items]) => {
          return `
            <div class="cd-related-group">
              <h4>${escapeHtml(title)}</h4>
              <ul>
                ${items
                  .map((item) => {
                    const type = item.type || 'evidence';
                    const id = item.id || '';
                    const label = item.label || id;
                    return `<li><button type="button" class="cd-nav-link" data-nav-type="${escapeHtml(
                      type
                    )}" data-nav-id="${escapeHtml(id)}" data-nav-label="${escapeHtml(
                      label
                    )}">${escapeHtml(label)}</button></li>`;
                  })
                  .join('')}
              </ul>
            </div>
          `;
        });

      if (
        related.recentChanges &&
        related.recentChanges.length &&
        !blocks.length
      ) {
        blocks.push(`
          <div class="cd-related-group">
            <h4>Recent changes</h4>
            <ul>
              ${related.recentChanges
                .map(
                  (c) =>
                    `<li>${escapeHtml(c.summary || c.type || 'change')}</li>`
                )
                .join('')}
            </ul>
          </div>
        `);
      } else if (related.recentChanges && related.recentChanges.length) {
        blocks.push(`
          <div class="cd-related-group">
            <h4>Recent changes</h4>
            <ul>
              ${related.recentChanges
                .map(
                  (c) =>
                    `<li>${escapeHtml(c.summary || c.type || 'change')}</li>`
                )
                .join('')}
            </ul>
          </div>
        `);
      }

      if (!blocks.length) {
        return `
          <section class="cd-related" aria-label="Related intelligence">
            <h3>Related intelligence</h3>
            <p class="cd-muted">Nothing else to explore from this node yet.</p>
          </section>
        `;
      }

      return `
        <section class="cd-related" aria-label="Related intelligence">
          <h3>Related intelligence</h3>
          <p class="cd-muted">What else should I look at?</p>
          <div class="cd-related-grid">${blocks.join('')}</div>
        </section>
      `;
    }

    function renderActions(actions) {
      if (!actions || !actions.length) return '';
      return `
        <footer class="cd-inv-actions">
          ${actions
            .map((a) => {
              const type = a.type || 'ask_max';
              const payload = escapeHtml(JSON.stringify(a.payload || {}));
              return `<button type="button" class="cd-btn ${
                type === 'ask_max' ? 'cd-btn-primary' : 'cd-btn-ghost'
              }" data-inv-action="${escapeHtml(type)}" data-inv-payload="${payload}">${escapeHtml(
                a.label || type
              )}</button>`;
            })
            .join('')}
        </footer>
      `;
    }

    function renderEvidenceList(items, depth) {
      if (!items || !items.length) {
        return '<p class="cd-muted">No evidence in this layer.</p>';
      }
      return `
        <ul class="cd-evidence-list" data-evidence-depth="${escapeHtml(depth)}">
          ${items
            .map((item) => {
              const id = item.id || (item.nav && item.nav.id) || '';
              const label =
                item.summary ||
                (item.nav && item.nav.label) ||
                id;
              const active = evidenceFocusId && String(evidenceFocusId) === String(id);
              return `<li class="${active ? 'is-active' : ''}">
                <button type="button" class="cd-nav-link" data-nav-type="evidence" data-nav-id="${escapeHtml(
                  id
                )}" data-nav-label="${escapeHtml(label)}" data-evidence-step="1">
                  ${escapeHtml(label)}
                </button>
              </li>`;
            })
            .join('')}
        </ul>
      `;
    }

    function renderRecommendation(model) {
      if (model.empty) {
        return `
          <article class="cd-inv-article">
            <p class="cd-eyebrow">Recommendation</p>
            <h2>Unavailable</h2>
            <p class="cd-muted">${escapeHtml(
              model.emptyReason || 'This recommendation is not in memory yet.'
            )}</p>
            ${renderRelated(model.related)}
            ${renderActions(model.actions)}
          </article>
        `;
      }

      const opp = model.opportunity || {};
      const evidence =
        evidenceDepth === 'summary'
          ? model.evidence
          : evidenceDepth === 'item'
            ? model.supportingSignals
            : model.supportingSignals;

      let evidenceSection = `
        <section class="cd-inv-section">
          <h3>Evidence · ${escapeHtml(evidenceDepth)}</h3>
          ${renderEvidenceList(evidence, evidenceDepth)}
          ${
            evidenceFocusId
              ? `<p class="cd-muted">Focused: ${escapeHtml(
                  evidenceFocusId
                )} · depth ${escapeHtml(evidenceDepth)}</p>`
              : ''
          }
        </section>
      `;

      return `
        <article class="cd-inv-article">
          <p class="cd-eyebrow">Recommendation</p>
          <h2>${escapeHtml(model.companyName || 'Recommendation')}</h2>
          <p class="cd-lede">${escapeHtml(opp.summary || '')}</p>
          <dl class="cd-inv-metrics">
            <div><dt>Opportunity</dt><dd>${escapeHtml(
              opp.score != null ? String(opp.score) : '—'
            )}</dd></div>
            <div><dt>Confidence</dt><dd>${escapeHtml(
              model.confidence != null ? String(model.confidence) : '—'
            )}</dd></div>
            <div><dt>Action</dt><dd>${escapeHtml(
              opp.recommendedAction || '—'
            )}</dd></div>
            <div><dt>Policy</dt><dd>${escapeHtml(
              (model.policy && model.policy.outcome) || '—'
            )}</dd></div>
          </dl>
          <section class="cd-inv-section">
            <h3>Supporting signals</h3>
            ${renderEvidenceList(model.supportingSignals, 'supporting')}
          </section>
          <section class="cd-inv-section">
            <h3>Contradicting signals</h3>
            ${renderEvidenceList(model.contradictingSignals, 'contradicting')}
          </section>
          ${evidenceSection}
          ${renderLiveTimeline(model.liveTimeline)}
          ${renderRelated(model.related)}
          ${renderActions(model.actions)}
        </article>
      `;
    }

    function renderLiveTimeline(liveTimeline) {
      const events =
        (liveTimeline && liveTimeline.events) ||
        (Array.isArray(liveTimeline) ? liveTimeline : null);
      if (!events || !events.length) return '';
      const state =
        liveTimeline &&
        liveTimeline.lifecycle &&
        liveTimeline.lifecycle.state
          ? liveTimeline.lifecycle.state
          : null;
      return `
        <section class="cd-inv-section cd-live-timeline" aria-label="Intelligence timeline">
          <h3>Live timeline${
            state ? ` · <span class="cd-lifecycle">${escapeHtml(state)}</span>` : ''
          }</h3>
          <ol class="cd-timeline-list">
            ${events
              .slice(-12)
              .map((ev) => {
                const at = ev.timestamp || ev.at || '';
                const summary = ev.summary || ev.type || '';
                const life = ev.lifecycle || '';
                return `<li>
                  <time>${escapeHtml(formatClock(at))}</time>
                  <span>${escapeHtml(life || summary)}</span>
                  ${
                    life && summary && life !== summary
                      ? `<span class="cd-muted"> · ${escapeHtml(summary)}</span>`
                      : ''
                  }
                </li>`;
              })
              .join('')}
          </ol>
        </section>
      `;
    }

    function formatClock(iso) {
      if (!iso) return '';
      const d = new Date(iso);
      if (Number.isNaN(d.getTime())) return String(iso);
      return new Intl.DateTimeFormat(undefined, {
        hour: 'numeric',
        minute: '2-digit',
      }).format(d);
    }

    function renderCompany(model) {
      if (model.empty) {
        return `
          <article class="cd-inv-article">
            <p class="cd-eyebrow">Company</p>
            <h2>Unavailable</h2>
            <p class="cd-muted">${escapeHtml(
              model.emptyReason || 'Company not found in the knowledge graph.'
            )}</p>
            ${renderRelated(model.related)}
            ${renderActions(model.actions)}
          </article>
        `;
      }

      const overview = model.overview || {};
      const reasoning = model.reasoning;

      return `
        <article class="cd-inv-article">
          <p class="cd-eyebrow">Company intelligence</p>
          <h2>${escapeHtml(model.companyName || overview.name || 'Company')}</h2>
          <p class="cd-lede">${escapeHtml(
            [overview.industry, overview.location].filter(Boolean).join(' · ') ||
              'Company workspace'
          )}</p>
          ${
            reasoning
              ? `<section class="cd-inv-section">
                  <h3>Reasoning</h3>
                  <p>${escapeHtml(
                    reasoning.recommendedAction ||
                      reasoning.type ||
                      'Latest recommendation'
                  )}</p>
                  <dl class="cd-inv-metrics">
                    <div><dt>Score</dt><dd>${escapeHtml(
                      reasoning.score != null ? String(reasoning.score) : '—'
                    )}</dd></div>
                    <div><dt>Confidence</dt><dd>${escapeHtml(
                      reasoning.confidence != null
                        ? String(reasoning.confidence)
                        : '—'
                    )}</dd></div>
                  </dl>
                </section>`
              : ''
          }
          <section class="cd-inv-section">
            <h3>Evidence</h3>
            ${renderEvidenceList(model.evidence, 'company')}
          </section>
          <section class="cd-inv-section">
            <h3>Timeline</h3>
            ${renderEvidenceList(model.timeline, 'timeline')}
          </section>
          ${
            model.recommendations && model.recommendations.length
              ? `<section class="cd-inv-section">
                  <h3>Recommendations</h3>
                  <ul class="cd-evidence-list">
                    ${model.recommendations
                      .map((r) => {
                        const nav = r.nav || r;
                        return `<li><button type="button" class="cd-nav-link" data-nav-type="recommendation" data-nav-id="${escapeHtml(
                          nav.id || r.id
                        )}" data-nav-label="${escapeHtml(
                          nav.label || r.label || r.id
                        )}">${escapeHtml(
                          nav.label || r.label || r.id
                        )}</button></li>`;
                      })
                      .join('')}
                  </ul>
                </section>`
              : ''
          }
          ${renderRelated(model.related)}
          ${renderActions(model.actions)}
        </article>
      `;
    }

    function renderEvidenceFocus(label) {
      return `
        <article class="cd-inv-article">
          <p class="cd-eyebrow">Evidence</p>
          <h2>${escapeHtml(label || evidenceFocusId || 'Evidence')}</h2>
          <p class="cd-muted">Depth: ${escapeHtml(evidenceDepth)}. Click related items to go deeper, or return via the trail.</p>
          ${
            currentModel && currentModel.related
              ? renderRelated(currentModel.related)
              : `<section class="cd-related"><h3>Related intelligence</h3><p class="cd-muted">Explore from the parent recommendation or company.</p></section>`
          }
          ${renderActions([
            {
              type: 'ask_max',
              label: 'Ask Max about this evidence',
              payload: {
                page: focusPayload().page,
                companyId: focusPayload().companyId,
                recommendationId: focusPayload().recommendationId,
                prompt: `Explain evidence ${label || evidenceFocusId}.`,
              },
            },
            {
              type: 'return_deck',
              label: 'Back to Command Deck',
              payload: {},
            },
          ])}
        </article>
      `;
    }

    function bindBody() {
      if (!bodyEl) return;
      bodyEl.querySelectorAll('[data-nav-type]').forEach((btn) => {
        btn.addEventListener('click', () => {
          const type = btn.getAttribute('data-nav-type');
          const id = btn.getAttribute('data-nav-id');
          const label = btn.getAttribute('data-nav-label') || id;
          navigateTo({ type, id, label });
        });
      });
      bodyEl.querySelectorAll('[data-inv-action]').forEach((btn) => {
        btn.addEventListener('click', () => {
          const type = btn.getAttribute('data-inv-action');
          let payload = {};
          try {
            payload = JSON.parse(
              btn.getAttribute('data-inv-payload') || '{}'
            );
          } catch (_err) {
            payload = {};
          }
          handleAction(type, payload);
        });
      });
    }

    function paint() {
      if (!bodyEl) return;
      const tip = trail[trail.length - 1];
      renderTrail();
      if (!tip || tip.kind === TRAIL_KINDS.DECK) {
        bodyEl.innerHTML = '';
        return;
      }
      if (tip.kind === TRAIL_KINDS.EVIDENCE || tip.kind === TRAIL_KINDS.INTERACTION) {
        bodyEl.innerHTML = renderEvidenceFocus(tip.label);
      } else if (currentModel && currentModel.kind === 'recommendation_detail') {
        bodyEl.innerHTML = renderRecommendation(currentModel);
      } else if (currentModel && currentModel.kind === 'company_intelligence') {
        bodyEl.innerHTML = renderCompany(currentModel);
      } else {
        bodyEl.innerHTML =
          '<article class="cd-inv-article"><p class="cd-muted">Loading…</p></article>';
      }
      bindBody();
      emitFocus();
    }

    async function openRecommendation(recommendationId, label, seedTrail) {
      showShell();
      if (seedTrail && seedTrail.length) {
        trail = seedTrail.slice();
      } else if (!trail.length) {
        pushTrail({
          kind: TRAIL_KINDS.DECK,
          id: null,
          label: "Today's Brief",
        });
      }
      pushTrail({
        kind: TRAIL_KINDS.RECOMMENDATION,
        id: recommendationId,
        label: label || 'Recommendation',
      });
      evidenceFocusId = null;
      evidenceDepth = 'summary';
      bodyEl.innerHTML =
        '<article class="cd-inv-article"><p class="cd-muted">Loading recommendation…</p></article>';
      renderTrail();
      setHashForTip();
      try {
        currentModel = await apiRequest(
          `/api/v1/recommendations/${encodeURIComponent(recommendationId)}`
        );
        try {
          const liveTimeline = await apiRequest(
            `/api/v1/intelligence/timeline/${encodeURIComponent(recommendationId)}`
          );
          if (currentModel && liveTimeline) {
            currentModel.liveTimeline = liveTimeline;
          }
        } catch (_liveErr) {
          /* timeline optional */
        }
        if (currentModel && currentModel.companyName) {
          const tip = trail[trail.length - 1];
          if (tip && tip.kind === TRAIL_KINDS.RECOMMENDATION) {
            tip.label =
              currentModel.opportunity && currentModel.opportunity.summary
                ? String(currentModel.opportunity.summary).slice(0, 64)
                : currentModel.companyName;
          }
        }
        paint();
        announce('Opened recommendation detail.');
        onOperatorEvent({
          type: 'ViewedRecommendation',
          recommendationId,
          companyId: (currentModel && currentModel.companyId) || null,
        });
        if (currentModel && currentModel.liveTimeline) {
          onOperatorEvent({
            type: 'OpenedTimeline',
            recommendationId,
            companyId: (currentModel && currentModel.companyId) || null,
            depth: 2,
          });
        }
      } catch (err) {
        bodyEl.innerHTML = `<article class="cd-inv-article"><p class="cd-muted">${escapeHtml(
          err.message || 'Failed to load recommendation'
        )}</p>${renderActions([
          { type: 'return_deck', label: 'Back to Command Deck', payload: {} },
        ])}</article>`;
        bindBody();
      }
    }

    async function openCompany(companyId, label, seedTrail) {
      showShell();
      if (seedTrail && seedTrail.length) {
        trail = seedTrail.slice();
      } else if (!trail.length) {
        pushTrail({
          kind: TRAIL_KINDS.DECK,
          id: null,
          label: "Today's Brief",
        });
      }
      pushTrail({
        kind: TRAIL_KINDS.COMPANY,
        id: companyId,
        label: label || 'Company',
      });
      evidenceFocusId = null;
      evidenceDepth = 'summary';
      bodyEl.innerHTML =
        '<article class="cd-inv-article"><p class="cd-muted">Loading company intelligence…</p></article>';
      renderTrail();
      setHashForTip();
      try {
        currentModel = await apiRequest(
          `/api/v1/companies/${encodeURIComponent(companyId)}/intelligence`
        );
        if (currentModel && currentModel.companyName) {
          const tip = trail[trail.length - 1];
          if (tip && tip.kind === TRAIL_KINDS.COMPANY) {
            tip.label = currentModel.companyName;
          }
        }
        paint();
        announce('Opened company intelligence.');
      } catch (err) {
        bodyEl.innerHTML = `<article class="cd-inv-article"><p class="cd-muted">${escapeHtml(
          err.message || 'Failed to load company'
        )}</p>${renderActions([
          { type: 'return_deck', label: 'Back to Command Deck', payload: {} },
        ])}</article>`;
        bindBody();
      }
    }

    function openEvidence(evidenceId, label) {
      showShell();
      if (!trail.length) {
        pushTrail({
          kind: TRAIL_KINDS.DECK,
          id: null,
          label: "Today's Brief",
        });
      }
      if (evidenceFocusId && String(evidenceFocusId) === String(evidenceId)) {
        if (evidenceDepth === 'summary') evidenceDepth = 'item';
        else if (evidenceDepth === 'item') evidenceDepth = 'raw';
      } else {
        evidenceDepth = 'item';
      }
      evidenceFocusId = evidenceId;
      pushTrail({
        kind: TRAIL_KINDS.EVIDENCE,
        id: evidenceId,
        label: label || evidenceId,
      });
      setHashForTip();
      paint();
      announce('Opened evidence.');
      onOperatorEvent({
        type: 'OpenedEvidence',
        recommendationId:
          (currentModel && currentModel.recommendationId) ||
          (trail.find((t) => t.kind === TRAIL_KINDS.RECOMMENDATION) || {}).id ||
          null,
        companyId: (currentModel && currentModel.companyId) || null,
        depth:
          evidenceDepth === 'raw' ? 3 : evidenceDepth === 'item' ? 2 : 1,
        payload: { evidenceId },
      });
    }

    function navigateTo(ref) {
      const type = String((ref && ref.type) || '');
      const id = ref && ref.id;
      const label = (ref && ref.label) || id;
      if (!id) return;
      if (type === 'recommendation') {
        openRecommendation(id, label);
        return;
      }
      if (type === 'company') {
        openCompany(id, label);
        return;
      }
      if (type === 'evidence' || type === 'claim' || type === 'interaction') {
        openEvidence(id, label);
      }
    }

    function handleAction(type, payload) {
      if (type === 'return_deck' || type === 'back_deck') {
        onOperatorEvent({ type: 'ReturnedToDeck' });
        hideShell();
        return;
      }
      if (type === 'ask_max') {
        const focus = focusPayload();
        onAskMax({
          ...payload,
          page: payload.page || focus.page,
          companyId: payload.companyId || focus.companyId,
          recommendationId: payload.recommendationId || focus.recommendationId,
        });
        return;
      }
      if (type === 'review_recommendation' && payload.recommendationId) {
        openRecommendation(
          payload.recommendationId,
          payload.label || 'Recommendation'
        );
        return;
      }
      if (type === 'open_company' && payload.companyId) {
        openCompany(payload.companyId, payload.label || 'Company');
      }
    }

    async function popTo(index) {
      if (index < 0) {
        hideShell();
        return;
      }
      trail = trail.slice(0, index + 1);
      const tip = trail[trail.length - 1];
      if (!tip || tip.kind === TRAIL_KINDS.DECK) {
        hideShell();
        return;
      }
      if (tip.kind === TRAIL_KINDS.RECOMMENDATION && tip.id) {
        const keep = trail.slice(0, -1);
        trail = keep;
        await openRecommendation(tip.id, tip.label);
        return;
      }
      if (tip.kind === TRAIL_KINDS.COMPANY && tip.id) {
        const keep = trail.slice(0, -1);
        trail = keep;
        await openCompany(tip.id, tip.label);
        return;
      }
      paint();
      setHashForTip();
    }

    function seedFromDeck(entryLabel) {
      trail = [
        {
          kind: TRAIL_KINDS.DECK,
          id: null,
          label: "Today's Brief",
        },
      ];
      if (entryLabel && entryLabel !== "Today's Brief") {
        trail.push({
          kind: TRAIL_KINDS.DECK,
          id: 'entry',
          label: entryLabel,
        });
      }
    }

    async function restoreFromHash() {
      if (syncingHash) return;
      const hash = String(location.hash || '');
      const m = hash.match(/^#\/(recommendation|company|evidence)\/(.+)$/);
      if (!m) {
        if (isOpen() && (!trail.length || trail[trail.length - 1].kind === TRAIL_KINDS.DECK)) {
          hideShell();
        }
        return;
      }
      const kind = m[1];
      const id = decodeURIComponent(m[2]);
      if (kind === 'recommendation') {
        await openRecommendation(id, 'Recommendation');
      } else if (kind === 'company') {
        await openCompany(id, 'Company');
      } else {
        openEvidence(id, id);
      }
    }

    function onKeydown(event) {
      if (event.key === 'Escape' && isOpen()) {
        // Let Max workspace take Escape first if open
        const mx = document.getElementById('maxWorkspace');
        if (mx && !mx.hidden) return;
        event.preventDefault();
        hideShell();
      }
    }

    function init() {
      window.addEventListener('hashchange', () => {
        restoreFromHash();
      });
      window.addEventListener('popstate', () => {
        restoreFromHash();
      });
      document.addEventListener('keydown', onKeydown);
      const closeBtn = document.querySelector('[data-cd-inv-close]');
      if (closeBtn) {
        closeBtn.addEventListener('click', () => hideShell());
      }
      const reviewBtn = document.querySelector('[data-cd-inv-review]');
      if (reviewBtn) {
        reviewBtn.addEventListener('click', () => {
          reviewLiveUpdate();
        });
      }
      // Deep link on load
      if (location.hash && location.hash.indexOf('#/') === 0) {
        restoreFromHash();
      }
    }

    return {
      TRAIL_KINDS,
      init,
      isOpen,
      openRecommendation,
      openCompany,
      openEvidence,
      navigateTo,
      seedFromDeck,
      hideShell,
      focusPayload,
      noteLiveEvents,
      reviewLiveUpdate,
      getTrail: () => trail.map((s) => ({ ...s })),
    };
  }

  global.PulseforgeInvestigation = {
    createInvestigation,
    TRAIL_KINDS,
  };
})(window);
