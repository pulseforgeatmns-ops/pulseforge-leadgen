/**
 * Public Revenue Leak Scorecard funnel.
 * Unauthenticated HTML pages + POST capture. No sessionAuth.
 */

const path = require('path');
const express = require('express');
const { validateScorecardPayload, resolveResult } = require('../lib/scorecardScoring');
const { captureScorecardLead } = require('../lib/scorecardCapture');

const router = express.Router();
const SCORECARD_DIR = path.join(__dirname, '..', 'public', 'scorecard');

const BOOKING_URL =
  process.env.SCORECARD_BOOKING_URL ||
  'https://calendly.com/jacob-gopulseforge/pulsforge-revenue-recovery-assessment';

// Kit checkout is intentionally not wired yet — CTA surfaces intent only.
const KIT_URL = process.env.SCORECARD_KIT_URL || '/scorecard/results#kit';

function sendScorecardPage(res, filename) {
  res.sendFile(path.join(SCORECARD_DIR, filename));
}

router.get('/scorecard', (req, res) => sendScorecardPage(res, 'index.html'));
router.get('/scorecard/form', (req, res) => sendScorecardPage(res, 'form.html'));
router.get('/scorecard/results', (req, res) => sendScorecardPage(res, 'results.html'));

// CSS/JS assets — index disabled so /scorecard stays on the landing handler above
router.use('/scorecard', express.static(SCORECARD_DIR, { index: false, fallthrough: true }));

/**
 * POST /api/public/scorecard
 * Body: scorecard answers + contact fields + optional marketing_consent
 */
router.post('/api/public/scorecard', async (req, res) => {
  try {
    // Honeypot — bots fill hidden "company_website"
    if (String(req.body?.company_website || '').trim()) {
      return res.status(204).end();
    }

    const validated = validateScorecardPayload(req.body);
    if (!validated.ok) {
      // Field-level messages only — never stack traces or DB detail
      return res.status(400).json({ error: 'Validation failed', details: validated.errors });
    }

    const result = resolveResult(validated.answers);
    const stored = await captureScorecardLead(validated.answers, result);

    return res.status(201).json({
      ok: true,
      submission_id: stored.id,
      result: {
        category: result.category,
        title: result.title,
        summary: result.summary,
        high_intent: result.high_intent,
        primary_cta: result.primary_cta,
      },
      ctas: {
        assessment_url: BOOKING_URL,
        kit_url: KIT_URL,
        kit_price: 29,
      },
    });
  } catch (err) {
    console.error('[scorecard] submit failed:', err.message);
    return res.status(500).json({ error: 'Could not save your scorecard. Please try again.' });
  }
});

module.exports = router;
