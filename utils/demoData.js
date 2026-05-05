'use strict';

function daysAgo(n, offsetHours = 0) {
  const d = new Date();
  d.setDate(d.getDate() - n);
  d.setHours(d.getHours() - offsetHours);
  return d.toISOString();
}

function dateStr(daysBack) {
  const d = new Date();
  d.setDate(d.getDate() - daysBack);
  return d.toISOString().slice(0, 10);
}

function generateDemoData() {

  // ── PROSPECTS (47) ────────────────────────────────────────────────
  const prospects = [
    // Cleaning — target vertical, high ICP
    { id: 'p01', first_name: 'Dan',      last_name: 'Whittaker', email: 'dan@whittakerhome.com',          phone: '(603) 555-0104', status: 'warm', icp_score: 95, notes: 'Whittaker Home Services — whittakerhome.com',          company_name: null, touchpoint_count: 3, last_contacted_at: daysAgo(2) },
    { id: 'p02', first_name: 'Jennifer', last_name: 'Walsh',     email: 'jen@millcitymaids.com',          phone: '(603) 555-0103', status: 'warm', icp_score: 92, notes: 'Mill City Maids — millcitymaids.com',                 company_name: null, touchpoint_count: 2, last_contacted_at: daysAgo(3) },
    { id: 'p03', first_name: 'Sarah',    last_name: 'Connor',    email: 'sarah@manchestercleanco.com',    phone: '(603) 555-0101', status: 'warm', icp_score: 88, notes: 'Manchester Clean Co — manchestercleanco.com',         company_name: null, touchpoint_count: 2, last_contacted_at: daysAgo(4) },
    { id: 'p04', first_name: 'Mike',     last_name: 'Torres',    email: 'mike@granitecleaning.net',       phone: '(603) 555-0102', status: 'warm', icp_score: 85, notes: 'Granite State Cleaning — granitecleaning.net',        company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(5) },
    { id: 'p05', first_name: 'Steve',    last_name: 'Larson',    email: 'steve@concordhvac.com',          phone: '(603) 555-0131', status: 'warm', icp_score: 85, notes: 'Concord Heating & Air — concordhvac.com',            company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(6) },
    { id: 'p06', first_name: 'Mia',      last_name: 'Soto',      email: 'mia@crownandcutsalon.com',      phone: '(603) 555-0117', status: 'warm', icp_score: 82, notes: 'Crown & Cut Salon — crownandcutsalon.com',            company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(7) },
    { id: 'p07', first_name: 'Brenda',   last_name: 'Mills',     email: 'brenda@qccleaning.com',          phone: '(603) 555-0105', status: 'warm', icp_score: 80, notes: 'Queen City Clean — qccleaning.com',                  company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(8) },
    { id: 'p08', first_name: 'Donna',    last_name: 'Castillo',  email: 'donna@snhhvac.com',              phone: '(603) 555-0132', status: 'warm', icp_score: 80, notes: 'Southern NH HVAC — snhhvac.com',                     company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(8) },
    { id: 'p09', first_name: 'Lisa',     last_name: 'Huang',     email: 'lisa@londonderrymaid.com',       phone: '(603) 555-0110', status: 'warm', icp_score: 78, notes: 'Londonderry Maid Service — londonderrymaid.com',     company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(9) },
    { id: 'p10', first_name: 'Alicia',   last_name: 'Hunt',      email: 'alicia@manestudio.com',          phone: '(603) 555-0118', status: 'cold', icp_score: 78, notes: 'The Mane Studio — manestudio.com',                   company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(10) },
    { id: 'p11', first_name: 'Patricia', last_name: 'Lane',      email: 'plane@nashuaclean.com',          phone: null,             status: 'cold', icp_score: 77, notes: 'Nashua Pro Cleaners — nashuaclean.com',               company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(10) },
    { id: 'p12', first_name: 'Tom',      last_name: 'Bridges',   email: 'tom@mvclean.com',                phone: null,             status: 'cold', icp_score: 75, notes: 'Merrimack Valley Cleaning — mvclean.com',            company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(11) },
    { id: 'p13', first_name: 'Rich',     last_name: 'Fontaine',  email: 'rich@manchestermech.com',        phone: null,             status: 'cold', icp_score: 75, notes: 'Manchester Mechanical — manchestermech.com',         company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p14', first_name: 'Courtney', last_name: 'Bell',      email: 'courtney@stylistroots.com',      phone: null,             status: 'cold', icp_score: 75, notes: 'Stylish Roots Salon — stylistroots.com',             company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p15', first_name: 'Amy',      last_name: 'Donahue',   email: 'amy@souheganclean.com',          phone: '(603) 555-0107', status: 'cold', icp_score: 72, notes: 'Souhegan Pro Clean — souheganclean.com',             company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(12) },
    { id: 'p16', first_name: 'Barry',    last_name: 'Simmons',   email: 'bsimmons@merrimackhvac.com',     phone: '(603) 555-0134', status: 'cold', icp_score: 72, notes: 'Merrimack HVAC Pro — merrimackhvac.com',             company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p17', first_name: 'Paul',     last_name: 'Adler',     email: 'paul@foundrynh.com',             phone: '(603) 555-0123', status: 'cold', icp_score: 72, notes: 'The Foundry Restaurant — foundrynh.com',             company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p18', first_name: 'Chris',    last_name: 'Patel',     email: 'chris@hillsboroughclean.com',    phone: null,             status: 'cold', icp_score: 70, notes: 'Hillsborough Cleaning — hillsboroughclean.com',      company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(13) },
    { id: 'p19', first_name: 'Nina',     last_name: 'Reyes',     email: 'nina@bellahairmgh.com',          phone: '(603) 555-0120', status: 'cold', icp_score: 70, notes: 'Bella Hair Co — bellahairmgh.com',                  company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p20', first_name: 'Dave',     last_name: 'Romano',    email: 'dave@queencityauto.com',         phone: '(603) 555-0136', status: 'cold', icp_score: 70, notes: 'Queen City Auto — queencityauto.com',               company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p21', first_name: 'Carlos',   last_name: 'Rivera',    email: 'carlos@greenthumbnh.com',        phone: '(603) 555-0140', status: 'cold', icp_score: 68, notes: 'Green Thumb Lawn Care — greenthumbnh.com',          company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p22', first_name: 'Kim',      last_name: 'Olsen',     email: 'kim@derryair.com',               phone: '(603) 555-0135', status: 'cold', icp_score: 68, notes: 'Derry Air Solutions — derryair.com',                company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p23', first_name: 'Rachel',   last_name: 'Green',     email: 'rachel@bedfordclean.com',        phone: '(603) 555-0109', status: 'cold', icp_score: 68, notes: 'Bedford Deep Clean — bedfordclean.com',             company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(14) },
    { id: 'p24', first_name: 'Rosa',     last_name: 'Espinoza',  email: 'rosa@millkitchennh.com',         phone: '(603) 555-0124', status: 'cold', icp_score: 68, notes: 'Mill Kitchen & Bar — millkitchennh.com',            company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p25', first_name: 'Lan',      last_name: 'Nguyen',    email: 'lan@queensnailspa.com',          phone: '(603) 555-0121', status: 'cold', icp_score: 65, notes: 'Queens Nail Spa — queensnailspa.com',               company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p26', first_name: 'Frank',    last_name: 'Anjos',     email: 'frank@granitestatemotors.com',   phone: '(603) 555-0137', status: 'cold', icp_score: 65, notes: 'Granite State Motors — granitestatemotors.com',     company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p27', first_name: 'Rob',      last_name: 'Fenton',    email: 'rob@hooksettclean.com',          phone: null,             status: 'cold', icp_score: 65, notes: 'Hooksett Cleaning Co — hooksettclean.com',          company_name: null, touchpoint_count: 1, last_contacted_at: daysAgo(15) },
    { id: 'p28', first_name: 'Jake',     last_name: 'Murphy',    email: 'jake@strangebrew.com',           phone: '(603) 555-0125', status: 'cold', icp_score: 65, notes: 'Strange Brew Tavern — strangebrew.com',             company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p29', first_name: 'Kelly',    last_name: 'Nash',      email: 'kelly@auburnclean.com',          phone: '(603) 555-0112', status: 'cold', icp_score: 62, notes: 'Auburn Clean Team — auburnclean.com',               company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p30', first_name: 'Derek',    last_name: 'Stone',     email: 'derek@merrimacklandscaping.com', phone: '(603) 555-0141', status: 'cold', icp_score: 62, notes: 'Merrimack Landscaping — merrimacklandscaping.com',  company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p31', first_name: 'Pete',     last_name: 'Kallis',    email: 'pkallis@redarrownh.com',         phone: null,             status: 'cold', icp_score: 62, notes: 'Red Arrow Diner — redarrownh.com',                  company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p32', first_name: 'Mark',     last_name: 'Okafor',    email: 'mark@derryclean.com',            phone: '(603) 555-0113', status: 'cold', icp_score: 60, notes: 'Derry House Cleaning — derryclean.com',             company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p33', first_name: 'Tamara',   last_name: 'Fox',       email: 'tamara@serenitybeautybar.com',   phone: null,             status: 'cold', icp_score: 60, notes: 'Serenity Beauty Bar — serenitybeautybar.com',       company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p34', first_name: 'Al',       last_name: 'Chen',      email: 'al@millcitytire.com',            phone: null,             status: 'cold', icp_score: 60, notes: 'Mill City Tire & Auto — millcitytire.com',         company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p35', first_name: 'Tyler',    last_name: 'Banks',     email: 'tyler@ironmillgym.com',          phone: '(603) 555-0144', status: 'cold', icp_score: 60, notes: 'Iron Mill Gym — ironmillgym.com',                  company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p36', first_name: 'Greg',     last_name: 'Hanson',    email: 'greg@concordclean.com',          phone: '(603) 555-0115', status: 'cold', icp_score: 58, notes: 'Concord Clean Solutions — concordclean.com',        company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p37', first_name: 'Matt',     last_name: 'Quinn',     email: 'matt@bedfordlawn.com',           phone: null,             status: 'cold', icp_score: 58, notes: 'Bedford Lawn & Garden — bedfordlawn.com',           company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p38', first_name: 'Brian',    last_name: 'Cho',       email: 'brian@hanoverstreetnh.com',      phone: '(603) 555-0127', status: 'cold', icp_score: 58, notes: 'Hanover Street Chophouse — hanoverstreetnh.com',    company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p39', first_name: 'Diane',    last_name: 'Sorrell',   email: 'diane@goffstownclean.com',       phone: null,             status: 'cold', icp_score: 55, notes: 'Goffstown Cleaning LLC — goffstownclean.com',       company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p40', first_name: 'James',    last_name: 'Ware',      email: 'james@bedfordauto.com',          phone: '(603) 555-0139', status: 'cold', icp_score: 55, notes: 'Bedford Auto Repair — bedfordauto.com',            company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p41', first_name: 'Sue',      last_name: 'Farrell',   email: 'sue@llawnpro.com',               phone: '(603) 555-0143', status: 'cold', icp_score: 55, notes: 'Londonderry Lawn Pro — llawnpro.com',              company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p42', first_name: 'Angela',   last_name: 'Park',      email: 'angela@queencityfitness.com',    phone: null,             status: 'cold', icp_score: 55, notes: 'Queen City Fitness — queencityfitness.com',         company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p43', first_name: 'Connor',   last_name: 'Riley',     email: 'connor@murphystaproom.com',      phone: null,             status: 'cold', icp_score: 52, notes: "Murphy's Taproom — murphystaproom.com",             company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p44', first_name: 'Josh',     last_name: 'Keane',     email: 'josh@merrimackcrossfit.com',     phone: '(603) 555-0146', status: 'cold', icp_score: 50, notes: 'Merrimack CrossFit — merrimackcrossfit.com',        company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p45', first_name: 'Travis',   last_name: 'Webb',      email: 'twebb@draftnh.com',              phone: '(603) 555-0130', status: 'cold', icp_score: 48, notes: 'The Draft Sports Bar — draftnh.com',               company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p46', first_name: 'Sandra',   last_name: 'Bloom',     email: 'sbloom@olivegardenmgt.com',      phone: '(603) 555-0128', status: 'cold', icp_score: 45, notes: 'Olive Garden Catering — olivegardenmgt.com',        company_name: null, touchpoint_count: 0, last_contacted_at: null },
    { id: 'p47', first_name: 'Sandra',   last_name: 'Morris',    email: 'sandra@millcityprop.com',        phone: '(603) 555-0147', status: 'cold', icp_score: 45, notes: 'Mill City Property Management — millcityprop.com',  company_name: null, touchpoint_count: 0, last_contacted_at: null },
  ];

  // ── AGENT STATUS ──────────────────────────────────────────────────
  const agentStatus = {
    prospects: 47,
    touchpoints: 124,
    pending: 5,
    weeklyTouchpoints: 18,
    agentRuns: {
      scout_agent: 23, email_agent: 47, linkedin_agent: 18, facebook_agent: 15,
      paige_agent: 12, max_agent: 7, sam_agent: 8, vera_agent: 14,
      cal_agent: 6, riley_agent: 9, rex_agent: 2, ivy_agent: 11,
      penny_agent: 0, sketch_agent: 3,
    },
    rings: { scout: 0.85, link: 0.72, faye: 0.6, emmett: 0.78, max: 1, rex: 1 },
    channels: [
      { channel: 'linkedin',       count: '18' },
      { channel: 'facebook',       count: '15' },
      { channel: 'blog',           count: '4'  },
      { channel: 'instagram',      count: '11' },
      { channel: 'google_business',count: '6'  },
    ],
  };

  // ── AGENT WEEKLY STATS (tooltip counts) ───────────────────────────
  const agentWeeklyStats = {
    scout:  { count: 23, label: 'prospects found'   },
    emmett: { count: 47, label: 'emails sent'        },
    link:   { count: 18, label: 'drafts generated'  },
    faye:   { count: 15, label: 'drafts generated'  },
    ivy:    { count: 11, label: 'drafts generated'  },
    paige:  { count: 12, label: 'posts generated'   },
    max:    { count: 7,  label: 'digests sent'       },
    sam:    { count: 8,  label: 'SMS sent'           },
    rex:    { count: 2,  label: 'reports generated'  },
    riley:  { count: 9,  label: 'emails triaged'     },
    vera:   { count: 14, label: 'reviews monitored'  },
    cal:    { count: 6,  label: 'calls initiated'    },
    penny:  { count: 0,  label: 'accounts analyzed'  },
    sketch: { count: 3,  label: 'mockups generated'  },
  };

  // ── APPROVALS (5 pending) ─────────────────────────────────────────
  const approvals = [
    {
      id: 'demo-approval-01',
      author_name: 'Whittaker Home Services',
      author_title: 'Cleaning',
      post_content: 'LinkedIn Page · Educational',
      comment: 'POST: Most cleaning companies lose 30% of bookings to no-shows — not because of bad service, but because there\'s no follow-up system.\n\nAt Whittaker Home Services, we automated our confirmation and reminder flow. A text goes out 48 hours before every appointment. Another goes out the morning of. No-show rate dropped from 22% to under 5% in six weeks.\n\nThe system costs nothing extra. We already had the customers — we just stopped dropping the ball on the handoff.\n\nIf you\'re running a service business in New Hampshire and losing bookings to scheduling gaps, what\'s your current follow-up process?\n\n#ManchesterNH #SmallBusinessNH #ServiceBusiness #BookingTips #LocalBusiness\nFIRST_COMMENT: https://gopulseforge.com',
      channel: 'linkedin_page',
      status: 'pending',
      created_at: daysAgo(0, 2),
    },
    {
      id: 'demo-approval-02',
      author_name: 'Whittaker Home Services',
      author_title: 'Cleaning',
      post_content: 'LinkedIn Page · Behind the Scenes',
      comment: 'POST: Here\'s what a Monday morning looks like at Whittaker Home Services.\n\n6:00 AM — routes are already set. Crews know exactly where they\'re going. Supplies are loaded. Notes from previous visits are in everyone\'s hands.\n\nWe didn\'t get here by working harder. We got here by building systems.\n\nFor the first three years, we were winging it. Scheduling was a whiteboard. Follow-ups were sticky notes. We lost good clients because we couldn\'t keep up with communication.\n\nNow our operation runs like a machine — and the team actually has time to focus on doing great work instead of chasing logistics.\n\nWhat\'s the one thing you wish you\'d systematized sooner?\n\n#BehindTheScenes #NewHampshire #CleaningBusiness #Operations #LocalBusiness\nFIRST_COMMENT: https://gopulseforge.com',
      channel: 'linkedin_page',
      status: 'pending',
      created_at: daysAgo(0, 5),
    },
    {
      id: 'demo-approval-03',
      author_name: 'Whittaker Home Services',
      author_title: 'Cleaning',
      post_content: 'Facebook Page · Community',
      comment: 'Shoutout to everyone keeping Manchester looking sharp this spring.\n\nWe\'ve been out in the Millyard and over in the North End this week — a lot of commercial spaces doing their spring deep cleans. It\'s that time of year when everything gets a reset.\n\nIf your office or storefront is on the list, we still have a few spots open for next week. Drop us a message or call (603) 555-0104. Happy to get you a same-week quote.\n\n#Manchester #ManchesterNH #SpringCleaning #LocalBusiness #NHSmallBiz',
      channel: 'facebook',
      status: 'pending',
      created_at: daysAgo(1),
    },
    {
      id: 'demo-approval-04',
      author_name: 'Whittaker Home Services',
      author_title: 'Cleaning',
      post_content: 'Google Business · Educational',
      comment: 'Spring is the busiest time of year for deep cleaning in Manchester — and the biggest mistake homeowners make is waiting until the last minute to book.\n\nWhittaker Home Services recommends scheduling your spring deep clean at least 2 weeks out. We cover bedrooms, kitchens, bathrooms, and common areas including the spots that get skipped during regular cleaning: baseboards, window sills, behind appliances, and grout lines.\n\nWe serve Manchester, Bedford, Goffstown, Hooksett, and surrounding areas. Call or message us for a free quote — most appointments book within 3-5 business days.',
      channel: 'google_business',
      status: 'pending',
      created_at: daysAgo(1, 3),
    },
    {
      id: 'demo-approval-05',
      author_name: 'Pulseforge',
      author_title: 'Marketing Automation',
      post_content: 'Blog · Educational',
      comment: '# Why Most Local Businesses Lose Leads Before They Even Start\n\nThere\'s a gap between "someone finds your business" and "someone books your service." Most local business owners in New Hampshire don\'t think about this gap — but it\'s where most of their revenue is leaking.\n\nHere\'s what typically happens: a potential customer finds you on Google, checks your website, and moves on to the next result because there\'s no obvious next step. Or they fill out a contact form and you get back to them two days later. By then, they\'ve already hired someone else.\n\n## The Speed-to-Lead Problem\n\nStudies consistently show that the odds of converting a lead drop by over 80% if you wait more than five minutes to respond. Most small business owners are doing their actual work — cleaning, fixing HVAC, cutting hair — and can\'t respond to every inquiry in real time.\n\nThis is exactly the problem automated outreach solves. Not by replacing the human relationship, but by bridging the gap between inquiry and conversation.\n\n## What Automation Actually Looks Like\n\nFor a cleaning company in Manchester, this might mean an instant text confirmation when someone fills out your booking form, a follow-up email 24 hours later if they haven\'t booked, and a reminder the week before their appointment.\n\nNone of this is complicated. It just needs to be set up once — and then it runs while you focus on the work.\n\nIf you\'re a local business owner in New Hampshire and this sounds familiar, Pulseforge builds these systems for businesses like yours.',
      channel: 'blog',
      status: 'pending',
      created_at: daysAgo(2),
    },
  ];

  // ── AGENT SPARKLINES ──────────────────────────────────────────────
  const agentStats = {};
  [
    { key: 'scout_agent',    total: 23,  week: 5,  success: 22, daily: [3,5,4,6,2,5,3]  },
    { key: 'emmett_agent',   total: 147, week: 12, success: 145,daily: [7,8,5,9,6,8,4]  },
    { key: 'linkedin_agent', total: 56,  week: 8,  success: 54, daily: [2,3,2,4,2,3,2]  },
    { key: 'facebook_agent', total: 48,  week: 7,  success: 47, daily: [2,2,3,2,3,2,1]  },
    { key: 'paige_agent',    total: 36,  week: 6,  success: 36, daily: [1,2,2,3,1,2,1]  },
    { key: 'max_agent',      total: 21,  week: 2,  success: 21, daily: [0,1,0,0,1,0,0]  },
    { key: 'sam_agent',      total: 24,  week: 4,  success: 23, daily: [1,2,1,2,1,2,1]  },
    { key: 'vera_agent',     total: 42,  week: 7,  success: 41, daily: [1,2,1,2,1,2,1]  },
    { key: 'cal_agent',      total: 18,  week: 3,  success: 17, daily: [0,1,1,1,0,1,1]  },
    { key: 'riley_agent',    total: 27,  week: 5,  success: 27, daily: [1,1,1,2,1,2,1]  },
    { key: 'rex_agent',      total: 6,   week: 1,  success: 6,  daily: [0,0,0,1,0,0,0]  },
    { key: 'ivy_agent',      total: 33,  week: 6,  success: 32, daily: [1,2,2,1,1,1,1]  },
    { key: 'penny_agent',    total: 0,   week: 0,  success: 0,  daily: [0,0,0,0,0,0,0]  },
    { key: 'sketch_agent',   total: 9,   week: 2,  success: 9,  daily: [0,0,1,0,0,1,0]  },
  ].forEach(a => {
    agentStats[a.key] = {
      total: a.total, weekRuns: a.week, successCount: a.success,
      lastRun: daysAgo(1),
      daily: a.daily.map((count, i) => ({ date: dateStr(6 - i), count })),
    };
  });

  // ── ACTIVITY FEED (cycling pool — 20 events) ──────────────────────
  const activityEvents = [
    { agent: 'Emmett', action: 'sent Day 4 follow-up · Whittaker Home Services', icon: '✉️', color: 'fi-o', status: 'success' },
    { agent: 'Scout',  action: 'found 3 new prospects in cleaning vertical',      icon: '🔍', color: 'fi-t', status: 'success' },
    { agent: 'Vera',   action: 'monitored 2 new reviews · Whittaker Home Services', icon: '⭐', color: 'fi-p', status: 'success' },
    { agent: 'Paige',  action: 'generated LinkedIn post — pending approval',       icon: '✍️', color: 'fi-p', status: 'success' },
    { agent: 'Sam',    action: 'sent follow-up SMS · Mill City Property Management', icon: '📱', color: 'fi-p', status: 'success' },
    { agent: 'Link',   action: 'drafted comment on small business post',           icon: '💬', color: 'fi-p', status: 'success' },
    { agent: 'Emmett', action: 'sent Day 0 intro · Manchester Clean Co',           icon: '✉️', color: 'fi-o', status: 'success' },
    { agent: 'Scout',  action: 'scored 47 prospects — 9 warm leads flagged',       icon: '🔍', color: 'fi-t', status: 'success' },
    { agent: 'Max',    action: 'daily digest sent · jake@gopulseforge.com',        icon: '🧠', color: 'fi-p', status: 'success' },
    { agent: 'Faye',   action: 'drafted comment · Manchester Small Business group',icon: '📣', color: 'fi-t', status: 'success' },
    { agent: 'Emmett', action: 'sent Day 8 email · Granite State Cleaning',        icon: '✉️', color: 'fi-o', status: 'success' },
    { agent: 'Riley',  action: 'triaged 3 inbound replies from prospects',         icon: '🙋', color: 'fi-p', status: 'success' },
    { agent: 'Cal',    action: 'initiated call · Queen City Clean',                icon: '📞', color: 'fi-p', status: 'success' },
    { agent: 'Scout',  action: 'found 5 new prospects in HVAC vertical',           icon: '🔍', color: 'fi-t', status: 'success' },
    { agent: 'Ivy',    action: 'drafted Instagram comment · #manchesternh',        icon: '📸', color: 'fi-p', status: 'success' },
    { agent: 'Paige',  action: 'generated Google Business update — pending approval', icon: '✍️', color: 'fi-p', status: 'success' },
    { agent: 'Emmett', action: 'sent Day 0 intro · Londonderry Maid Service',      icon: '✉️', color: 'fi-o', status: 'success' },
    { agent: 'Vera',   action: 'flagged 4-star review — response drafted',         icon: '⭐', color: 'fi-p', status: 'success' },
    { agent: 'Sketch', action: 'generated website mockup · Souhegan Pro Clean',    icon: '🎨', color: 'fi-t', status: 'success' },
    { agent: 'Rex',    action: 'weekly performance report dispatched',             icon: '📊', color: 'fi-p', status: 'success' },
  ];

  // ── ACTIVITY PANEL (sequences + timeline) ─────────────────────────
  const sequences = [
    { id: 'p01', business: 'Whittaker Home Services',   status: 'warm', emails_sent: 3, stage_label: 'Day 8 sent · next Day 13', last_touch: daysAgo(2),  next_due_at: daysAgo(-3), overdue: false, complete: false },
    { id: 'p02', business: 'Mill City Maids',           status: 'warm', emails_sent: 2, stage_label: 'Day 4 sent · next Day 8',  last_touch: daysAgo(3),  next_due_at: daysAgo(-1), overdue: false, complete: false },
    { id: 'p03', business: 'Manchester Clean Co',       status: 'warm', emails_sent: 2, stage_label: 'Day 4 sent · next Day 8',  last_touch: daysAgo(5),  next_due_at: daysAgo(1),  overdue: true,  complete: false },
    { id: 'p04', business: 'Granite State Cleaning',    status: 'warm', emails_sent: 1, stage_label: 'Day 0 sent · next Day 4',  last_touch: daysAgo(5),  next_due_at: daysAgo(-1), overdue: false, complete: false },
    { id: 'p05', business: 'Concord Heating & Air',     status: 'warm', emails_sent: 1, stage_label: 'Day 0 sent · next Day 4',  last_touch: daysAgo(6),  next_due_at: daysAgo(0),  overdue: false, complete: false },
    { id: 'p06', business: 'Crown & Cut Salon',         status: 'warm', emails_sent: 1, stage_label: 'Day 0 sent · next Day 4',  last_touch: daysAgo(7),  next_due_at: daysAgo(3),  overdue: true,  complete: false },
    { id: 'p07', business: 'Queen City Clean',          status: 'cold', emails_sent: 1, stage_label: 'Day 0 sent · next Day 4',  last_touch: daysAgo(8),  next_due_at: daysAgo(0),  overdue: false, complete: false },
    { id: 'p08', business: 'Southern NH HVAC',          status: 'cold', emails_sent: 1, stage_label: 'Day 0 sent · next Day 4',  last_touch: daysAgo(8),  next_due_at: daysAgo(0),  overdue: false, complete: false },
    { id: 'p09', business: 'Londonderry Maid Service',  status: 'warm', emails_sent: 1, stage_label: 'Day 0 sent · next Day 4',  last_touch: daysAgo(9),  next_due_at: daysAgo(5),  overdue: true,  complete: false },
    { id: 'p10', business: 'The Mane Studio',           status: 'cold', emails_sent: 1, stage_label: 'Day 0 sent · next Day 4',  last_touch: daysAgo(10), next_due_at: daysAgo(6),  overdue: true,  complete: false },
    { id: 'p11', business: 'Nashua Pro Cleaners',       status: 'cold', emails_sent: 1, stage_label: 'Day 0 sent · next Day 4',  last_touch: daysAgo(10), next_due_at: daysAgo(6),  overdue: true,  complete: false },
    { id: 'p12', business: 'Merrimack Valley Cleaning', status: 'cold', emails_sent: 1, stage_label: 'Day 0 sent · next Day 4',  last_touch: daysAgo(11), next_due_at: daysAgo(7),  overdue: true,  complete: false },
  ];

  const timeline = [
    { id: 'tl01', agent: 'Emmett', icon: '✉️', action: 'sent email',          prospect: 'Whittaker Home Services',   status: 'success', ran_at: daysAgo(0, 2)  },
    { id: 'tl02', agent: 'Scout',  icon: '🔍', action: 'found prospect',       prospect: 'Serenity Beauty Bar',       status: 'success', ran_at: daysAgo(0, 3)  },
    { id: 'tl03', agent: 'Scout',  icon: '🔍', action: 'found prospect',       prospect: 'Londonderry Lawn Pro',      status: 'success', ran_at: daysAgo(0, 3)  },
    { id: 'tl04', agent: 'Vera',   icon: '⭐', action: 'analyzed reviews',     prospect: 'Whittaker Home Services',   status: 'success', ran_at: daysAgo(0, 4)  },
    { id: 'tl05', agent: 'Paige',  icon: '✍️', action: 'generated content',    prospect: null,                        status: 'success', ran_at: daysAgo(0, 5)  },
    { id: 'tl06', agent: 'Sam',    icon: '📱', action: 'sent SMS',             prospect: 'Mill City Property Mgmt',  status: 'success', ran_at: daysAgo(0, 6)  },
    { id: 'tl07', agent: 'Link',   icon: '💬', action: 'drafted comment',      prospect: null,                        status: 'success', ran_at: daysAgo(1, 1)  },
    { id: 'tl08', agent: 'Emmett', icon: '✉️', action: 'sent email',          prospect: 'Manchester Clean Co',       status: 'success', ran_at: daysAgo(1, 2)  },
    { id: 'tl09', agent: 'Max',    icon: '🧠', action: 'sent daily digest',    prospect: null,                        status: 'success', ran_at: daysAgo(1, 3)  },
    { id: 'tl10', agent: 'Faye',   icon: '📣', action: 'drafted comment',      prospect: null,                        status: 'success', ran_at: daysAgo(1, 4)  },
    { id: 'tl11', agent: 'Scout',  icon: '🔍', action: 'found prospect',       prospect: 'Bedford Auto Repair',       status: 'success', ran_at: daysAgo(1, 5)  },
    { id: 'tl12', agent: 'Emmett', icon: '✉️', action: 'sent email',          prospect: 'Granite State Cleaning',    status: 'success', ran_at: daysAgo(1, 6)  },
    { id: 'tl13', agent: 'Riley',  icon: '🙋', action: 'triaged inbox',        prospect: null,                        status: 'success', ran_at: daysAgo(2, 1)  },
    { id: 'tl14', agent: 'Cal',    icon: '📞', action: 'initiated call',       prospect: 'Queen City Clean',          status: 'success', ran_at: daysAgo(2, 2)  },
    { id: 'tl15', agent: 'Ivy',    icon: '📸', action: 'drafted comment',      prospect: null,                        status: 'success', ran_at: daysAgo(2, 3)  },
    { id: 'tl16', agent: 'Scout',  icon: '🔍', action: 'found prospect',       prospect: 'Merrimack CrossFit',        status: 'success', ran_at: daysAgo(2, 4)  },
    { id: 'tl17', agent: 'Paige',  icon: '✍️', action: 'generated content',    prospect: null,                        status: 'success', ran_at: daysAgo(2, 5)  },
    { id: 'tl18', agent: 'Emmett', icon: '✉️', action: 'sent email',          prospect: 'Londonderry Maid Service',  status: 'success', ran_at: daysAgo(2, 6)  },
    { id: 'tl19', agent: 'Vera',   icon: '⭐', action: 'analyzed reviews',     prospect: null,                        status: 'success', ran_at: daysAgo(3, 1)  },
    { id: 'tl20', agent: 'Sketch', icon: '🎨', action: 'generated mockup',     prospect: 'Souhegan Pro Clean',        status: 'success', ran_at: daysAgo(3, 2)  },
    { id: 'tl21', agent: 'Link',   icon: '💬', action: 'drafted comment',      prospect: null,                        status: 'success', ran_at: daysAgo(3, 3)  },
    { id: 'tl22', agent: 'Sam',    icon: '📱', action: 'sent SMS',             prospect: 'Manchester Clean Co',       status: 'success', ran_at: daysAgo(3, 4)  },
    { id: 'tl23', agent: 'Rex',    icon: '📊', action: 'sent weekly report',   prospect: null,                        status: 'success', ran_at: daysAgo(3, 5)  },
    { id: 'tl24', agent: 'Scout',  icon: '🔍', action: 'found prospect',       prospect: 'Queens Nail Spa',           status: 'success', ran_at: daysAgo(4, 1)  },
    { id: 'tl25', agent: 'Emmett', icon: '✉️', action: 'sent email',          prospect: 'Crown & Cut Salon',         status: 'success', ran_at: daysAgo(4, 2)  },
    { id: 'tl26', agent: 'Faye',   icon: '📣', action: 'drafted comment',      prospect: null,                        status: 'success', ran_at: daysAgo(4, 3)  },
    { id: 'tl27', agent: 'Cal',    icon: '📞', action: 'initiated call',       prospect: 'Concord Heating & Air',     status: 'success', ran_at: daysAgo(4, 4)  },
    { id: 'tl28', agent: 'Max',    icon: '🧠', action: 'sent daily digest',    prospect: null,                        status: 'success', ran_at: daysAgo(5, 1)  },
    { id: 'tl29', agent: 'Paige',  icon: '✍️', action: 'generated content',    prospect: null,                        status: 'success', ran_at: daysAgo(5, 2)  },
    { id: 'tl30', agent: 'Scout',  icon: '🔍', action: 'found prospect',       prospect: 'Strange Brew Tavern',       status: 'success', ran_at: daysAgo(5, 3)  },
    { id: 'tl31', agent: 'Emmett', icon: '✉️', action: 'sent email',          prospect: 'Southern NH HVAC',          status: 'success', ran_at: daysAgo(5, 4)  },
    { id: 'tl32', agent: 'Riley',  icon: '🙋', action: 'triaged inbox',        prospect: null,                        status: 'success', ran_at: daysAgo(6, 1)  },
    { id: 'tl33', agent: 'Vera',   icon: '⭐', action: 'analyzed reviews',     prospect: null,                        status: 'success', ran_at: daysAgo(6, 2)  },
    { id: 'tl34', agent: 'Scout',  icon: '🔍', action: 'found prospect',       prospect: 'Green Thumb Lawn Care',     status: 'success', ran_at: daysAgo(6, 3)  },
    { id: 'tl35', agent: 'Ivy',    icon: '📸', action: 'drafted comment',      prospect: null,                        status: 'success', ran_at: daysAgo(7, 1)  },
    { id: 'tl36', agent: 'Emmett', icon: '✉️', action: 'sent email',          prospect: 'Londonderry Maid Service',  status: 'success', ran_at: daysAgo(7, 2)  },
    { id: 'tl37', agent: 'Sketch', icon: '🎨', action: 'generated mockup',     prospect: 'Bedford Deep Clean',        status: 'success', ran_at: daysAgo(7, 3)  },
    { id: 'tl38', agent: 'Link',   icon: '💬', action: 'drafted comment',      prospect: null,                        status: 'success', ran_at: daysAgo(7, 4)  },
    { id: 'tl39', agent: 'Cal',    icon: '📞', action: 'initiated call',       prospect: 'Mill City Maids',           status: 'success', ran_at: daysAgo(8, 1)  },
    { id: 'tl40', agent: 'Sam',    icon: '📱', action: 'sent SMS',             prospect: 'Manchester Clean Co',       status: 'success', ran_at: daysAgo(8, 2)  },
    { id: 'tl41', agent: 'Rex',    icon: '📊', action: 'sent weekly report',   prospect: null,                        status: 'success', ran_at: daysAgo(10, 1) },
    { id: 'tl42', agent: 'Scout',  icon: '🔍', action: 'found prospect',       prospect: 'The Draft Sports Bar',      status: 'success', ran_at: daysAgo(10, 2) },
    { id: 'tl43', agent: 'Emmett', icon: '✉️', action: 'sent email',          prospect: 'Queen City Clean',          status: 'success', ran_at: daysAgo(10, 3) },
    { id: 'tl44', agent: 'Paige',  icon: '✍️', action: 'generated content',    prospect: null,                        status: 'success', ran_at: daysAgo(11, 1) },
    { id: 'tl45', agent: 'Faye',   icon: '📣', action: 'drafted comment',      prospect: null,                        status: 'success', ran_at: daysAgo(11, 2) },
    { id: 'tl46', agent: 'Emmett', icon: '✉️', action: 'sent email',          prospect: 'Granite State Cleaning',    status: 'success', ran_at: daysAgo(12, 1) },
    { id: 'tl47', agent: 'Vera',   icon: '⭐', action: 'analyzed reviews',     prospect: null,                        status: 'success', ran_at: daysAgo(12, 2) },
    { id: 'tl48', agent: 'Scout',  icon: '🔍', action: 'found prospect',       prospect: 'Merrimack HVAC Pro',        status: 'success', ran_at: daysAgo(14, 1) },
    { id: 'tl49', agent: 'Emmett', icon: '✉️', action: 'sent email',          prospect: 'Hillsborough Cleaning',     status: 'success', ran_at: daysAgo(14, 2) },
    { id: 'tl50', agent: 'Max',    icon: '🧠', action: 'sent daily digest',    prospect: null,                        status: 'success', ran_at: daysAgo(15, 1) },
  ];

  // ── ANALYTICS ─────────────────────────────────────────────────────
  // 30-day outbound volume — ramps up over the period
  const rampBase = [1,1,2,1,2,2,1, 2,3,2,3,2,3,2, 3,4,3,4,3,4,3, 4,5,4,6,5,4,5, 6,7,8];
  const outbound_volume = rampBase.map((base, i) => ({
    date: dateStr(29 - i),
    email: base,
    sms:   i < 7 ? 0 : Math.floor(base / 3),
  }));

  // 8-week reply rate — improving trend
  const replyWeeks = [
    { out: 8,  inn: 1 }, { out: 12, inn: 2 }, { out: 15, inn: 3 }, { out: 18, inn: 4 },
    { out: 20, inn: 5 }, { out: 22, inn: 6 }, { out: 25, inn: 7 }, { out: 27, inn: 8 },
  ];
  const reply_rate = replyWeeks.map(({ out, inn }, i) => {
    const d = new Date();
    d.setDate(d.getDate() - (7 - i) * 7);
    return {
      week: d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' }),
      outbound: out, inbound: inn,
      rate: Math.round((inn / out) * 100),
    };
  });

  const icp_distribution = [
    { bucket: '0–20',   count: 0  },
    { bucket: '21–40',  count: 2  },
    { bucket: '41–60',  count: 10 },
    { bucket: '61–80',  count: 23 },
    { bucket: '81–100', count: 12 },
    { bucket: 'Unknown',count: 0  },
  ];

  const agent_breakdown = [
    { agent: 'emmett_agent',   count: 47 }, { agent: 'scout_agent',    count: 23 },
    { agent: 'linkedin_agent', count: 18 }, { agent: 'vera_agent',     count: 14 },
    { agent: 'facebook_agent', count: 15 }, { agent: 'paige_agent',    count: 12 },
    { agent: 'ivy_agent',      count: 11 }, { agent: 'riley_agent',    count: 9  },
    { agent: 'sam_agent',      count: 8  }, { agent: 'max_agent',      count: 7  },
    { agent: 'cal_agent',      count: 6  }, { agent: 'sketch_agent',   count: 3  },
    { agent: 'rex_agent',      count: 2  }, { agent: 'penny_agent',    count: 0  },
  ];

  const pipeline_funnel = [
    { stage: 'cold',      count: 35, pct: 74 },
    { stage: 'warm',      count: 8,  pct: 17 },
    { stage: 'replied',   count: 3,  pct: 6  },
    { stage: 'converted', count: 1,  pct: 2  },
  ];

  const top_prospects = prospects.slice(0, 10).map(p => ({
    id: p.id,
    name: `${p.first_name} ${p.last_name}`,
    business: p.notes.split('—')[0].trim(),
    status: p.status,
    touchpoint_count: p.touchpoint_count,
    last_contacted_at: p.last_contacted_at,
  }));

  // ── TOUCHPOINTS (for prospect detail drawer) ──────────────────────
  const touchpoints = [
    { channel: 'email', action_type: 'outbound', content_summary: 'Day 0 intro — asked about current marketing setup and referral flow', outcome: null, created_at: daysAgo(8) },
    { channel: 'email', action_type: 'outbound', content_summary: 'Day 4 follow-up — shared cleaning company automation case study',    outcome: null, created_at: daysAgo(4) },
    { channel: 'email', action_type: 'inbound',  content_summary: 'Reply: "Interested, can we get on a call this week?"',               outcome: 'replied', created_at: daysAgo(3) },
    { channel: 'sms',   action_type: 'outbound', content_summary: 'SMS follow-up after reply — sent calendar booking link',             outcome: null, created_at: daysAgo(2) },
  ];

  return {
    agentStatus,
    agentWeeklyStats,
    approvals,
    prospects,
    agentStats,
    activityEvents,
    activityPanel: { sequences, timeline: timeline.slice(0, 25) },
    activityTimeline: timeline,
    analytics: { outbound_volume, reply_rate, icp_distribution, agent_breakdown, pipeline_funnel, top_prospects },
    touchpoints,
  };
}

module.exports = { generateDemoData };
