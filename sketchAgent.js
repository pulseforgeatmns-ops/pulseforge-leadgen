require('dotenv').config();
const Anthropic = require('@anthropic-ai/sdk');
const fs = require('fs');
const path = require('path');
const pool = require('./db');

const client = new Anthropic();
const AGENT_NAME = 'sketch_agent';

const businessName = process.argv[2];
const location = process.argv[3] || 'Manchester, NH';

if (!businessName) {
  console.error('Usage: node sketchAgent.js "Business Name" "City, State"');
  process.exit(1);
}

function slugify(str) {
  return str.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '');
}

async function generateMockup(businessName, location) {
  console.log(`\nSketch generating mockup for: ${businessName} · ${location}\n`);

  const message = await client.messages.create({
    model: 'claude-sonnet-4-5',
    max_tokens: 8000,
    messages: [{
      role: 'user',
      content: `You are Sketch, an elite web designer AI. Generate a complete, stunning, production-quality HTML website mockup for a cleaning services business.

Business Name: ${businessName}
Location: ${location}
Industry: Cleaning Services (residential + commercial)

REQUIREMENTS:
- Single complete HTML file with embedded CSS and JS
- Absolutely no placeholder text like "Lorem ipsum" — write real, compelling copy specific to this business
- Real local copy — mention ${location} specifically throughout
- Mobile responsive
- Sections: Hero, Services, Why Choose Us, Service Areas, Testimonials, Contact Form, Footer
- Contact form with name, email, phone, service type dropdown, message
- Push commercial cleaning slightly more than residential (60/40)
- Google Fonts via CDN link
- No external images — use CSS gradients, shapes, and emoji/icons for visual interest

DESIGN DIRECTION:
Choose ONE bold, distinctive aesthetic and execute it perfectly. Do NOT use generic purple gradients or Inter font. Pick something unexpected and memorable — think editorial, industrial, luxury, minimalist dark, bold typographic, organic, etc. Make it feel genuinely designed for THIS business in THIS location. Every design choice should feel intentional.

The design should be so impressive that when a business owner sees it they immediately think "I need this."

Return ONLY the complete HTML file. No explanation, no markdown, no backticks. Start with <!DOCTYPE html> and end with </html>.`
    }]
  });

  return message.content[0].text;
}

async function saveMockup(html, businessName) {
  const slug = slugify(businessName);
  const filename = `${slug}-${Date.now()}.html`;
  const dir = path.join(__dirname, 'mockups');

  if (!fs.existsSync(dir)) fs.mkdirSync(dir);

  const filepath = path.join(dir, filename);
  fs.writeFileSync(filepath, html);

  const previewUrl = `https://pulseforge-leadgen-production.up.railway.app/preview/${filename}`;

  console.log(`Mockup saved: ${filepath}`);
  console.log(`Preview URL: ${previewUrl}`);

  return { filepath, filename, previewUrl };
}

async function saveToDatabase(businessName, location, previewUrl) {
  // Check if prospect already exists
  const existing = await pool.query(
    `SELECT id FROM prospects WHERE first_name ILIKE $1 LIMIT 1`,
    [businessName.split(' ')[0]]
  );

  if (existing.rows.length > 0) {
    const prospectId = existing.rows[0].id;
    await pool.query(
      `UPDATE prospects SET notes = COALESCE(notes, '') || $1 WHERE id = $2`,
      [`\nSketch mockup: ${previewUrl}`, prospectId]
    );
    console.log(`Mockup URL saved to existing prospect: ${prospectId}`);
    return prospectId;
  }

  // Create new prospect entry
  const result = await pool.query(
    `INSERT INTO prospects (first_name, last_name, status, source, notes)
     VALUES ($1, $2, $3, $4, $5) RETURNING id`,
    [businessName, '', 'cold', 'sketch', `Sketch mockup: ${previewUrl}`]
  );

  const prospectId = result.rows[0].id;
  console.log(`New prospect created: ${prospectId}`);
  return prospectId;
}

async function logAgentRun(businessName, previewUrl, status) {
  await pool.query(
    `INSERT INTO agent_log (agent_name, action, payload, status, ran_at)
     VALUES ($1, $2, $3, $4, NOW())`,
    [AGENT_NAME, 'generate_mockup', JSON.stringify({ businessName, previewUrl }), status]
  );
}

async function run() {
  try {
    // Generate the mockup
    const html = await generateMockup(businessName, location);

    // Save to file
    const { filepath, filename, previewUrl } = await saveMockup(html, businessName);

    // Save to database
    await saveToDatabase(businessName, location, previewUrl);

    // Log the run
    await logAgentRun(businessName, previewUrl, 'success');

    console.log('\n✓ Sketch complete.');
    console.log(`\nShare this URL with ${businessName}:`);
    console.log(`→ ${previewUrl}\n`);

  } catch (err) {
    console.error('Sketch error:', err.message);
    await logAgentRun(businessName, '', 'error').catch(() => {});
  }

  pool.end();
}

run();