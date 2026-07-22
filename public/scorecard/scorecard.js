/**
 * Revenue Leak Scorecard — multi-step form wizard.
 * Submits to POST /api/public/scorecard; server owns scoring + capture.
 */
(function () {
  const TOTAL_STEPS = 11;
  const STORAGE_KEY = 'pf_scorecard_result';
  const form = document.getElementById('scorecard-form');
  if (!form) return;

  const steps = Array.from(form.querySelectorAll('.sc-step'));
  const backBtn = document.getElementById('sc-back');
  const nextBtn = document.getElementById('sc-next');
  const stepLabel = document.getElementById('sc-step-label');
  const pctLabel = document.getElementById('sc-pct-label');
  const progressBar = document.getElementById('sc-progressbar');
  const progressFill = document.getElementById('sc-progress-fill');

  let current = 0;
  let submitting = false;

  function updateProgress() {
    const human = current + 1;
    const pct = Math.round((human / TOTAL_STEPS) * 100);
    if (current === TOTAL_STEPS - 1) {
      stepLabel.textContent = 'Last step';
    } else if (current === TOTAL_STEPS - 2) {
      stepLabel.textContent = 'Your details';
    } else {
      stepLabel.textContent = `Question ${human} of ${TOTAL_STEPS - 2}`;
    }
    pctLabel.textContent = pct + '%';
    progressFill.style.width = pct + '%';
    progressBar.setAttribute('aria-valuenow', String(pct));
    backBtn.hidden = current === 0;
    nextBtn.textContent = current === TOTAL_STEPS - 1 ? 'See my score' : 'Continue';
  }

  function showStep(index) {
    steps.forEach((el, i) => {
      el.classList.toggle('is-active', i === index);
    });
    current = index;
    updateProgress();
    const heading = steps[index].querySelector('.sc-question');
    if (heading) heading.focus({ preventScroll: true });
    window.scrollTo({ top: 0, behavior: 'smooth' });
  }

  function clearError(field) {
    const el = document.getElementById('err-' + field);
    if (el) {
      el.hidden = true;
      el.textContent = '';
    }
  }

  function setError(field, message) {
    const el = document.getElementById('err-' + field);
    if (el) {
      el.hidden = false;
      el.textContent = message;
    }
  }

  function validateCurrent() {
    const step = steps[current];
    const field = step.dataset.field;

    if (field === 'contact') {
      clearError('contact');
      const name = form.name.value.trim();
      const business = form.business_name.value.trim();
      const email = form.email.value.trim();
      const mobile = form.mobile.value.trim();
      let ok = true;

      [form.name, form.business_name, form.email, form.mobile].forEach((input) => {
        input.setAttribute('aria-invalid', 'false');
      });

      if (!name) {
        form.name.setAttribute('aria-invalid', 'true');
        ok = false;
      }
      if (!business) {
        form.business_name.setAttribute('aria-invalid', 'true');
        ok = false;
      }
      if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        form.email.setAttribute('aria-invalid', 'true');
        ok = false;
      }
      const digits = mobile.replace(/\D/g, '');
      if (digits.length < 10) {
        form.mobile.setAttribute('aria-invalid', 'true');
        ok = false;
      }
      if (!ok) {
        setError('contact', 'Please enter your name, business, a valid email, and a mobile number.');
        return false;
      }
      return true;
    }

    if (field === 'marketing_consent') {
      clearError('submit');
      return true;
    }

    clearError(field);
    const selected = form.querySelector(`input[name="${field}"]:checked`);
    if (!selected) {
      setError(field, 'Please choose an option to continue.');
      return false;
    }
    return true;
  }

  function collectPayload() {
    const getRadio = (name) => {
      const el = form.querySelector(`input[name="${name}"]:checked`);
      return el ? el.value : '';
    };

    return {
      business_type: getRadio('business_type'),
      monthly_inquiries: getRadio('monthly_inquiries'),
      after_hours_process: getRadio('after_hours_process'),
      missed_call_text: getRadio('missed_call_text'),
      quote_follow_up_speed: getRadio('quote_follow_up_speed'),
      quote_follow_up_count: getRadio('quote_follow_up_count'),
      automatic_review_request: getRadio('automatic_review_request'),
      current_system: getRadio('current_system'),
      typical_job_value: getRadio('typical_job_value'),
      name: form.name.value.trim(),
      business_name: form.business_name.value.trim(),
      email: form.email.value.trim(),
      mobile: form.mobile.value.trim(),
      marketing_consent: Boolean(form.marketing_consent.checked),
      company_website: form.company_website.value,
    };
  }

  async function submitScorecard() {
    if (submitting) return;
    submitting = true;
    nextBtn.disabled = true;
    nextBtn.textContent = 'Scoring…';
    clearError('submit');

    try {
      const res = await fetch('/api/public/scorecard', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
        body: JSON.stringify(collectPayload()),
      });

      const data = await res.json().catch(() => ({}));
      if (!res.ok) {
        const detail = (data.details && data.details[0]) || data.error || 'Something went wrong.';
        setError('submit', detail);
        nextBtn.disabled = false;
        nextBtn.textContent = 'See my score';
        submitting = false;
        return;
      }

      sessionStorage.setItem(STORAGE_KEY, JSON.stringify(data));
      window.location.href = '/scorecard/results';
    } catch (_) {
      setError('submit', 'Network error — check your connection and try again.');
      nextBtn.disabled = false;
      nextBtn.textContent = 'See my score';
      submitting = false;
    }
  }

  nextBtn.addEventListener('click', function () {
    if (!validateCurrent()) return;
    if (current === TOTAL_STEPS - 1) {
      submitScorecard();
      return;
    }
    showStep(current + 1);
  });

  backBtn.addEventListener('click', function () {
    if (current > 0) showStep(current - 1);
  });

  // Advance on option select for radio steps (faster mobile UX)
  form.addEventListener('change', function (e) {
    const target = e.target;
    if (target && target.matches('input[type="radio"]') && current < TOTAL_STEPS - 2) {
      clearError(target.name);
    }
  });

  form.addEventListener('keydown', function (e) {
    if (e.key === 'Enter' && e.target && e.target.tagName !== 'TEXTAREA') {
      if (current < TOTAL_STEPS - 2 || (e.target.type !== 'checkbox' && current !== TOTAL_STEPS - 2)) {
        // Allow Enter in text fields on contact step to move focus, not submit whole form
        if (current === TOTAL_STEPS - 2 && e.target.tagName === 'INPUT') {
          return;
        }
      }
      if (current !== TOTAL_STEPS - 2) {
        e.preventDefault();
        nextBtn.click();
      }
    }
  });

  // Make question headings focusable for a11y step announcements
  steps.forEach((step) => {
    const h = step.querySelector('.sc-question');
    if (h) h.setAttribute('tabindex', '-1');
  });

  updateProgress();
})();
