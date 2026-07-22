/**
 * Revenue Leak Scorecard capture layer.
 *
 * MVP persistence uses the existing `agent_actions` table so submissions
 * surface in the operator dashboard without a new database or CRM.
 *
 * ─────────────────────────────────────────────────────────────────────
 * INTEGRATION POINT (swap or extend here — do not scatter writes):
 *   captureScorecardLead(answers, result) → Promise<{ id, stored }>
 *
 * Future hooks (not built yet):
 *   - Brevo contact create / list add
 *   - Dedicated scorecard_submissions table
 *   - Stripe payment link for the $29 kit
 * ─────────────────────────────────────────────────────────────────────
 */

const pool = require('../db');

const SCORECARD_CLIENT_ID = 1; // PulseForge lead-gen ICP (not Anchor client_id=10)
const CREATED_BY = 'scorecard';
const ACTION_TYPE = 'scorecard_lead';

/**
 * Persist a completed scorecard submission.
 * @param {object} answers - validated answer payload (includes contact + consent)
 * @param {object} result - output of resolveResult()
 * @returns {Promise<{ id: number|string|null, stored: boolean, client_id: number }>}
 */
async function captureScorecardLead(answers, result) {
  const title = `Revenue Leak Scorecard — ${result.title}`;
  const description = [
    answers.business_name,
    answers.name,
    answers.email,
    result.high_intent ? 'high-intent' : 'standard',
  ].join(' · ');

  const payload = {
    source: 'revenue_leak_scorecard',
    result_category: result.category,
    high_intent: result.high_intent,
    primary_cta: result.primary_cta,
    gaps: result.gaps,
    marketing_consent: Boolean(answers.marketing_consent),
    contact: {
      name: answers.name,
      business_name: answers.business_name,
      email: answers.email,
      mobile: answers.mobile,
    },
    answers: {
      business_type: answers.business_type,
      monthly_inquiries: answers.monthly_inquiries,
      after_hours_process: answers.after_hours_process,
      missed_call_text: answers.missed_call_text,
      quote_follow_up_speed: answers.quote_follow_up_speed,
      quote_follow_up_count: answers.quote_follow_up_count,
      automatic_review_request: answers.automatic_review_request,
      current_system: answers.current_system,
      typical_job_value: answers.typical_job_value,
    },
  };

  // ── primary write (existing lead/contact surface) ──────────────────
  const inserted = await pool.query(
    `INSERT INTO agent_actions
       (created_by, action_type, title, description, payload, status, client_id)
     VALUES ($1, $2, $3, $4, $5::jsonb, 'pending', $6)
     RETURNING id`,
    [
      CREATED_BY,
      ACTION_TYPE,
      title,
      description,
      JSON.stringify(payload),
      SCORECARD_CLIENT_ID,
    ]
  );

  // Optional future dual-writes belong here only:
  // await syncToBrevo(answers, result);
  // await insertScorecardSubmission(answers, result);

  return {
    id: inserted.rows[0]?.id ?? null,
    stored: Boolean(inserted.rows[0]?.id),
    client_id: SCORECARD_CLIENT_ID,
  };
}

module.exports = {
  captureScorecardLead,
  SCORECARD_CLIENT_ID,
  ACTION_TYPE,
};
