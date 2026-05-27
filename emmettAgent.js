require('dotenv').config();
const axios = require('axios');
const db = require('./dbClient');
const { getClientConfig, getRuntimeClientId } = require('./utils/clientContext');

const AGENT_NAME = 'emmett';
const BREVO_API_KEY = process.env.BREVO_API_KEY;
const FROM_EMAIL = 'jacob@gopulseforge.com';
let FROM_NAME = 'Jacob Maynard';
const CLIENT_ID = getRuntimeClientId();
let CLIENT_CONFIG = null;

const clientConfig = {
  1: { dailyCap: 100, verticalCap: 15 },
  2: { dailyCap: 40, verticalCap: 10 },
  5: { dailyCap: 30, verticalCap: 8, ramp: { afterDays: 14, bounceCeiling: 0.03, newDailyCap: 50 } },
};

function getEmmettClientConfig(clientId = CLIENT_ID) {
  return clientConfig[clientId] || clientConfig[1];
}

// Email sequence definitions — reply-based CTAs only (no external or Calendly links in bodies)

// A/B TEST ACTIVE: restaurant vs restaurant_b — remove restaurant_b when test concludes
const SEQUENCES = {
  mshi: [
    {
      day: 0,
      subject: "Quick question about {{business_name}}",
      body: `Hi {{first_name}},

My name is Brad — I run Mountain State Home Innovations out of Charleston with my partner Dustin.

We specialize in decks, siding, and exterior work across Kanawha, Putnam, and Cabell County. Licensed WV065578.

I came across {{business_name}} and wanted to reach out directly. We do a lot of work for property managers and HOAs who need reliable contractors they can call without the runaround.

Would it be worth a quick conversation?

Brad
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 4,
      subject: "Re: Quick question about {{business_name}}",
      body: `Hi {{first_name}},

Just following up in case my last note got buried.

One thing that sets us apart — most contractors go quiet after the estimate. We don't. Every client gets direct access to Brad or Dustin throughout the whole project. For property managers who need fast turnaround on damage or wear, that matters.

We're happy to do a free walkthrough of any properties you manage in Kanawha, Putnam, or Cabell County — just reply here and we'll set it up.

Brad
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 8,
      subject: "What we've done for other property managers in WV",
      body: `Hi {{first_name}},

We've done subcontract work with some of the larger WV firms — Tri-State Exterior Solutions, St Albans Windows, Secure Construction — so we know what quality at scale looks like.

Decks and siding are our highest volume work. If {{business_name}} has properties that need attention, we'd love to put together a free estimate.

You can also see our Google reviews here: https://share.google/KeVYcU4QxVwfur0cN

Brad
Mountain State Home Innovations
304-483-3655`
    },
    {
      day: 13,
      subject: "Closing the loop",
      body: `Hi {{first_name}},

I don't want to keep filling up your inbox so I'll leave it here.

If the timing ever works out, give us a call — Brad or Dustin will pick up.

304-483-3655

No obligation, free estimate, local crew. We'll be here.

Brad
Mountain State Home Innovations`
    }
  ],
  home_renovation: [
    {
      day: 0,
      subject: "Quick question about {{business_name}}",
      body: `Hi {{first_name}},

We handle exterior and interior renovations for property managers, HOAs, landlords, and banks across the Charleston area.

The thing our clients usually notice first is communication. We show up, keep you updated at every step, and Brad or Dustin are the ones doing the work instead of handing it off to a crew you have never met.

We offer free estimates and are licensed in WV under WV065578.

Are you planning any exterior or interior work on your properties this year?

Brad & Dustin
Mountain State Home Innovations`
    },
    {
      day: 4,
      subject: "Re: Quick question about {{business_name}}",
      body: `Hi {{first_name}},

Most contractors go quiet after the estimate. We do not.

Every client gets direct access to Brad or Dustin throughout the project, which matters when you are managing properties, board expectations, repairs, weather delays, or emergency damage.

If you want to see a free mockup of what that could look like for {{business_name}}, just reply here and I'll have something over to you same day.

Brad & Dustin`
    },
    {
      day: 8,
      subject: "What we've done for other property teams",
      body: `Hi {{first_name}},

Before Mountain State Home Innovations, we subcontracted for larger WV firms including Tri-State Exterior Solutions, St Albans Windows, and Secure Construction. That gave us a lot of experience doing quality work at scale while still caring about the details.

Decks and siding are two of our highest priority services right now, along with windows, interior renovations, and repair work when something needs attention quickly.

Are you currently working with a contractor you trust for this kind of work, or is it more as-needed when something comes up?

Brad & Dustin`
    },
    {
      day: 13,
      subject: "Closing the loop",
      body: `Hi {{first_name}},

Last note from us. We know you are busy, so we will keep it simple.

Our number is 304-483-3655. Brad or Dustin will pick up.

If the timing ever works out, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Brad & Dustin
Mountain State Home Innovations`
    }
  ],
  cleaning: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

Most cleaning companies in Manchester are getting leads — the problem is usually what happens after. Inquiry comes in, owner's on a job, nobody follows up fast enough, and the lead books somebody else.

Is that something you're running into at {{business_name}}, or have you got a system that keeps up with it?

Jacob Maynard
Pulseforge
(603) 293-5816`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Didn't hear back — totally fine, just wanted to make sure my last note landed.

One thing I've been building for cleaning companies in Southern NH: a system that texts new leads within 2 minutes, follows up automatically if they don't respond, and books appointments without the owner touching anything.

If that's something worth a look for {{business_name}}, just reply here and I'll put together a free mockup same day.

Jacob`
    },
    {
      day: 8,
      subject: "quick question about {{business_name}}",
      body: `Hi {{first_name}},

Last one from me on this — genuinely curious.

Are you currently doing anything to follow up with leads automatically, or is it still mostly manual when you get a chance?

Either way, happy to show you what it looks like. Just reply here.

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

I won't keep filling up your inbox. If the timing's ever right, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],
  restaurant: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I spent years running restaurants in New England. The hardest part was not the service or the food. It was staying visible when I was too busy running the floor to think about marketing.

Most owners I talk to are in the same spot. Great restaurant, not enough time to stay in front of new customers consistently.

I built a system that handles that automatically. It finds local prospects, reaches out on your behalf, and keeps your name visible between rushes. It runs in the background while you run the restaurant.

One question: is getting more new customers through the door something you are actively working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

I know you are busy. That is kind of the whole point.

The restaurants I work with are not struggling. They are good at what they do. They just do not have time to chase new customers on top of running the kitchen, managing staff, and everything else. That is the gap I fill.

If you want to see a free mockup of what that could look like for {{business_name}}, just reply here and I'll have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working in Manchester right now",
      body: `Hi {{first_name}},

One thing I am seeing across local restaurants right now. The ones growing consistently are not spending more on ads. They are just staying in front of people longer than their competition.

Most owners go quiet between services. The ones winning do not.

I help restaurants like {{business_name}} stay visible automatically. No extra time required on your end.

Are you currently doing anything to stay in front of new customers between visits, or is it mostly word of mouth at this point?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing ever works out, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],
  restaurant_b: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I spent years running restaurants in New England. The hardest part was not the service or the food. It was staying visible when I was too busy running the floor to think about marketing.

Most owners I talk to are in the same spot. Great restaurant, not enough time to stay in front of new customers consistently.

I built a system that handles that automatically. It finds local prospects, reaches out on your behalf, and keeps your name visible between rushes. It runs in the background while you run the restaurant.

Is bringing in new customers consistently something you're actively working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

I know you are busy. That is kind of the whole point.

The restaurants I work with are not struggling. They are good at what they do. They just do not have time to chase new customers on top of running the kitchen, managing staff, and everything else. That is the gap I fill.

If you want to see a free mockup of what that could look like for {{business_name}}, just reply here and I'll have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working in Manchester right now",
      body: `Hi {{first_name}},

One thing I am seeing across local restaurants right now. The ones growing consistently are not spending more on ads. They are just staying in front of people longer than their competition.

Most owners go quiet between services. The ones winning do not.

I help restaurants like {{business_name}} stay visible automatically. No extra time required on your end.

Are you currently doing anything to stay in front of new customers between rushes, or is it mostly word of mouth at this point?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing ever works out, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  salon: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

Salons in Manchester live and die by repeat bookings and referrals — but the ones growing fastest right now are the ones capturing new clients automatically between appointments.

Is bringing in new clients consistently something you're actively working on at {{business_name}}, or is it mostly word of mouth at this point?

Jacob Maynard
Pulseforge
(603) 293-5816`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Following up in case my last note got buried.

What I build for salons: automated follow-up that re-engages lapsed clients, captures new ones from your Google listing, and keeps your chair full without you chasing anyone.

If you want to see what that looks like for {{business_name}}, just reply here — I'll have a free mockup to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "quick question about {{business_name}}",
      body: `Hi {{first_name}},

One last question — are you currently doing anything to bring in new clients outside of referrals and social media, or is that still the main source?

Happy to show you what else is working for salons in Southern NH right now. Just reply here.

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. If the timing ever works out, just reply — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  fitness: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I work with local fitness studios and gyms in Southern NH on one specific problem: keeping your schedule full without spending your time chasing new members.

Most studio owners I talk to are great at what they do but inconsistent at bringing in new clients. Word of mouth works until it does not. And there is never enough time between classes to market consistently.

I built a system that handles outreach automatically. It finds local prospects, reaches out on your behalf, and keeps your name in front of people who are actively looking for a gym or studio. It runs in the background while you focus on your members.

Is growing your membership consistently something you are actively working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The studios I work with are not struggling. They have great instructors and loyal members. The challenge is always the same: memberships plateau because there is no consistent way to reach new people between managing classes and running the business.

If you want to see a free mockup of what that could look like for {{business_name}}, just reply here and I'll have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for fitness studios in Southern NH",
      body: `Hi {{first_name}},

One thing I am seeing across local studios right now. The ones growing their membership are not running more ads. They are just staying visible to new people longer than their competition.

Most owners go quiet between sessions. The ones building waitlists do not.

I help studios like {{business_name}} stay visible automatically. No extra time required on your end.

Are you currently doing anything to bring in new members consistently, or is it mostly referrals and word of mouth?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing ever works out, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  property: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I work with local property management companies in Southern NH on one specific problem: keeping units filled without relying entirely on Zillow and word of mouth.

Most property managers I talk to spend more time than they should chasing leads manually. The pipeline is inconsistent and there is never enough time between managing tenants and owners to market the portfolio properly.

I built a system that handles outreach automatically. It identifies local prospects, reaches out on your behalf, and keeps your properties in front of people who are actively looking. It runs in the background while you manage the day to day.

Is filling vacancies faster and more consistently something you are actively working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The property management companies I work with are not struggling. They have solid portfolios and good relationships with their owners. The challenge is always the same: vacancy periods stretch longer than they should because outreach is inconsistent or manual.

If you want to see a free mockup of what that could look like for {{business_name}}, just reply here and I'll have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for property managers in Southern NH",
      body: `Hi {{first_name}},

One thing I am seeing across local property management companies right now. The ones keeping vacancy rates low are not spending more on listing sites. They are just staying in front of prospective tenants and owner referrals longer than their competition.

Most companies go quiet between leases. The ones with full portfolios do not.

I help companies like {{business_name}} stay visible automatically. No extra time required on your end.

Are you currently doing anything to generate consistent leads for vacancies, or is it mostly listing sites and referrals?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing ever works out, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  landscaping: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I work with local lawn care and landscaping companies in Southern NH on one specific problem: keeping the schedule full year round without relying on door knocking and referrals alone.

Most owners I talk to are great at the work but inconsistent at bringing in new clients. Spring fills up fast but the pipeline for summer and fall maintenance is always thinner than it should be.

I built a system that handles outreach automatically. It finds local homeowners and commercial properties in your area, reaches out on your behalf, and keeps your name visible between jobs. It runs in the background while you run the crew.

Is keeping the schedule consistently full something you are actively working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The landscaping companies I work with are not struggling. They do great work and their existing clients love them. The challenge is always the same: the off-season pipeline dries up and spring scramble is stressful every year.

If you want to see a free mockup of what that could look like for {{business_name}}, just reply here and I'll have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for landscapers in Southern NH",
      body: `Hi {{first_name}},

One thing I am seeing across local landscaping companies right now. The ones with full schedules are not spending more on ads. They are just staying in front of new clients longer than their competition.

Most owners go quiet between jobs. The ones with waitlists do not.

I help companies like {{business_name}} stay visible automatically. No extra time required on your end.

Are you currently doing anything to bring in new clients consistently, or is it mostly referrals and repeat customers?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing ever works out, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  home_services: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I work with local home service companies in Southern NH on one specific problem: staying visible to homeowners between jobs so the pipeline does not go quiet.

Most owners I talk to get plenty of repeat business from happy customers but are not consistently reaching new homeowners. Referrals are great until they are not enough.

I built a system that handles outreach automatically. It finds local homeowners in your area, reaches out on your behalf, and keeps {{business_name}} visible year round. It runs in the background while you run the jobs.

Is building a more consistent pipeline of new homeowners something you are actively working on?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The home service companies I work with are not struggling. They do great work. The challenge is always the same: the work fills the schedule but not enough new homeowners are finding them consistently.

If you want to see a free mockup of what that could look like for {{business_name}}, just reply here and I'll have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for home services in Southern NH",
      body: `Hi {{first_name}},

One thing I am seeing across local home service companies right now. The ones with full schedules are not relying on referrals alone. They are staying in front of new homeowners consistently.

Most owners go quiet between jobs. The ones with waitlists do not.

I help companies like {{business_name}} stay visible automatically. No extra time required on your end.

Are you currently doing anything to reach new homeowners consistently, or is it mostly word of mouth?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing ever works out, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  auto: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

Auto shops in Manchester are busier than ever right now — but the ones adding real revenue aren't just doing more jobs, they're capturing customers who never came back after the first visit.

Is building a steadier flow of new and returning customers something you're working on at {{business_name}}, or is it pretty much steady as she goes?

Jacob Maynard
Pulseforge
(603) 293-5816`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Just following up — didn't want my last note to get lost.

What I build for auto shops: automated outreach that brings back lapsed customers, follows up on estimates that went quiet, and keeps new leads from slipping through.

If you want to see a free mockup of what that looks like for {{business_name}}, just reply here and I'll have it to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "quick question about {{business_name}}",
      body: `Hi {{first_name}},

Last question — are you currently doing anything to follow up with customers after a job, or is it mostly repeat and referral when it happens?

Happy to show you what's working for shops in Southern NH. Just reply here.

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Won't keep filling up your inbox. If the timing ever works out, just reply — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  med_spa: [
    {
      day: 0,
      subject: "{{business_name}} — honest question",
      body: `Hi {{first_name}},

I work with local med spas and aesthetic practices in Southern NH on one specific problem: consistent new client acquisition without depending entirely on word of mouth and Instagram.

Most owners I talk to have strong retention with existing clients. The challenge is staying visible to people who are actively searching for aesthetic services but have not found {{business_name}} yet.

I built a system that handles outreach automatically. It finds and reaches out to local prospects on your behalf, keeps your name visible to people searching in your area, and runs in the background while you focus on clients.

Is bringing in new clients more consistently something you are actively working on?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Sent you a note a few days ago. Wanted to follow up once before moving on.

The practices I work with are not struggling. They have skilled providers and clients who love them. The gap is always the same: not enough new people finding them consistently outside of referrals.

If you want to see a free mockup of what that could look like for {{business_name}}, just reply here and I'll have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "what is actually working for med spas in Southern NH",
      body: `Hi {{first_name}},

One thing I am seeing across local aesthetic practices right now. The ones growing consistently are not just posting more on Instagram. They are staying in front of local prospects who are actively looking but have not booked yet.

Most practices rely on existing clients to spread the word. The ones with waitlists do more than that.

I help practices like {{business_name}} stay visible to new clients automatically. No extra time required on your end.

Are you currently doing anything to reach new clients consistently outside of social media and referrals?

Jacob`
    },
    {
      day: 13,
      subject: "closing the loop",
      body: `Hi {{first_name}},

Last note from me. I do not want to clutter your inbox.

If the timing ever works out, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ],

  re_engagement: [
    {
      day: 0,
      subject: "still thinking about {{business_name}}",
      body: `Hi {{first_name}},

Reached out a few weeks ago — wanted to check back in before moving on.

I know timing matters. If growing your customer base consistently is still on your radar, I'd love to show you what we've put together for businesses like {{business_name}}.

No pitch, just a look at what the system actually does.

Is growing your customer base consistently still something you're working on right now?

Jacob Maynard
Pulseforge`
    },
    {
      day: 4,
      subject: "one thing I'm seeing in Manchester right now",
      body: `Hi {{first_name}},

One pattern I keep seeing across local businesses right now — the ones picking up new customers consistently aren't doing anything complicated. They're just staying visible longer than their competition.

Most owners go quiet between jobs. The ones growing don't.

I help businesses like {{business_name}} stay visible automatically. If you want to see a free mockup of what that could look like for {{business_name}}, just reply here and I'll have something over to you same day.

Jacob`
    },
    {
      day: 8,
      subject: "closing the loop on {{business_name}}",
      body: `Hi {{first_name}},

Last note from me — I don't want to clutter your inbox.

If the timing ever works out, just reply to this — I'll put something together for {{business_name}} same day. No forms, no pressure.

Rooting for you either way.

Jacob Maynard
Pulseforge
gopulseforge.com`
    }
  ]
};

// Fallback step used when a clicked/warm prospect is selected. Defaults to the
// cleaning Day 4 step until a dedicated warm sequence is defined.
const WARM_STEP = SEQUENCES.cleaning.find(step => step.day === 4);

function fillTemplate(template, prospect) {
  const rawName = prospect.first_name || prospect.name?.split(' ')[0] || '';
  const firstName = (rawName && rawName !== '—') ? rawName : 'there';

  let businessName = prospect.company || '';
  if (!businessName) {
    const fromNotes = prospect.notes?.split('—')[0]?.trim() || '';
    // Clean domain-style names
    businessName = fromNotes
      .replace(/\.(com|net|org|io|us)$/i, '')
      .replace(/^(www\.)/i, '')
      .trim();
  }
  if (!businessName || businessName.length < 4) businessName = 'your business';

  return template
    .replace(/{{first_name}}/g, firstName)
    .replace(/{{business_name}}/g, businessName);
}

async function sendEmail(toEmail, toName, subject, body, tags) {
  try {
    const payload = {
      sender: { name: FROM_NAME, email: FROM_EMAIL },
      to: [{ email: toEmail, name: toName }],
      subject,
      htmlContent: '<html><body style="font-family:Georgia,serif;font-size:16px;line-height:1.6;color:#1a1a1a;max-width:560px;margin:0 auto;padding:20px;">' + body.replace(/\n/g, '<br>') + '</body></html>',
      textContent: body
    };
    if (Array.isArray(tags) && tags.length) payload.tags = tags;
    const response = await axios.post('https://api.brevo.com/v3/smtp/email', payload, {
      headers: {
        'api-key': BREVO_API_KEY,
        'Content-Type': 'application/json'
      }
    });

    console.log(`Email sent to ${toEmail} — Message ID: ${response.data.messageId}`);
    return { success: true, messageId: response.data.messageId };
  } catch (err) {
    const errorDetail = err.response?.data || err.message;
    console.error(`Failed to send to ${toEmail}:`, errorDetail);
    return {
      success: false,
      error: typeof errorDetail === 'string' ? errorDetail : JSON.stringify(errorDetail),
    };
  }
}

function stripForbiddenMshiCopy(body, prospect) {
  if (Number(prospect.client_id) !== 2) return body;

  const forbidden = [
    /\bPulseforge\b/gi,
    /\bAI\b/gi,
    /\bautomation\b/gi,
    /\bmarketing agency\b/gi,
  ];
  let cleaned = body;
  let stripped = false;

  for (const pattern of forbidden) {
    pattern.lastIndex = 0;
    if (pattern.test(cleaned)) {
      stripped = true;
      pattern.lastIndex = 0;
      cleaned = cleaned.replace(pattern, '').replace(/[ \t]{2,}/g, ' ');
    }
  }

  if (stripped) {
    console.warn(`Warning: stripped forbidden MSHI copy before sending to ${prospect.email}`);
  }

  return cleaned;
}

async function getEmailsSentToday() {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT COUNT(*)::int AS count
    FROM agent_log
    WHERE action = 'email_sent'
      AND client_id = $1
      AND DATE(ran_at) = CURRENT_DATE
  `, [CLIENT_ID]);
  return res.rows[0]?.count || 0;
}

async function getEffectiveSendConfig(baseConfig) {
  if (!baseConfig.ramp || CLIENT_ID !== 5) return { ...baseConfig, ramped: false };

  const pool = require('./db');
  const stats = await pool.query(`
    SELECT
      MIN(ran_at) AS first_sent_at,
      COUNT(*)::int AS total_sent
    FROM agent_log
    WHERE action = 'email_sent'
      AND client_id = $1
  `, [CLIENT_ID]);
  const firstSentAt = stats.rows[0]?.first_sent_at;
  const totalSent = Number(stats.rows[0]?.total_sent || 0);
  if (!firstSentAt || totalSent === 0) return { ...baseConfig, ramped: false };

  const bounceStats = await pool.query(`
    SELECT COUNT(*)::int AS bounced
    FROM touchpoints
    WHERE client_id = $1
      AND channel = 'email'
      AND action_type IN ('email_bounced', 'email_soft_bounce')
  `, [CLIENT_ID]);

  const bounced = Number(bounceStats.rows[0]?.bounced || 0);
  const bounceRate = totalSent ? bounced / totalSent : 0;
  const daysSinceFirstSend = (Date.now() - new Date(firstSentAt).getTime()) / (1000 * 60 * 60 * 24);
  const shouldRamp =
    daysSinceFirstSend >= baseConfig.ramp.afterDays &&
    bounceRate < baseConfig.ramp.bounceCeiling;

  if (!shouldRamp) return { ...baseConfig, ramped: false };

  const existingRampLog = await pool.query(`
    SELECT 1
    FROM agent_log
    WHERE agent_name = $1
      AND action = 'cap_ramped'
      AND client_id = $2
    LIMIT 1
  `, [AGENT_NAME, CLIENT_ID]);

  if (!existingRampLog.rows.length) {
    await db.logAgentAction(
      AGENT_NAME,
      'cap_ramped',
      null,
      null,
      {
        client_id: CLIENT_ID,
        previous_daily_cap: baseConfig.dailyCap,
        new_daily_cap: baseConfig.ramp.newDailyCap,
        days_since_first_send: Number(daysSinceFirstSend.toFixed(1)),
        bounce_rate: Number(bounceRate.toFixed(4)),
        bounced,
        total_sent: totalSent,
      },
      'success'
    );
  }

  return { ...baseConfig, dailyCap: baseConfig.ramp.newDailyCap, ramped: true };
}

function sendingWindowEndHour(clientId = CLIENT_ID) {
  return Number(clientId) === 5 ? 16 : 14;
}

function sendingWindowLabel(clientId = CLIENT_ID) {
  return `Tuesday-Thursday 9am-${Number(clientId) === 5 ? '4pm' : '2pm'} ET`;
}

function isWithinSendingWindow(date = new Date(), clientId = CLIENT_ID) {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'America/New_York',
    weekday: 'short',
    hour: 'numeric',
    hour12: false,
  }).formatToParts(date);
  const weekday = parts.find(p => p.type === 'weekday')?.value;
  const hour = Number(parts.find(p => p.type === 'hour')?.value);
  return ['Tue', 'Wed', 'Thu'].includes(weekday) && hour >= 9 && hour < sendingWindowEndHour(clientId);
}

async function logSkippedOutsideWindow() {
  await db.logAgentAction(
    AGENT_NAME,
    'skipped_outside_window',
    null,
    null,
    {
      client_id: CLIENT_ID,
      window: sendingWindowLabel(CLIENT_ID),
      checked_at: new Date().toISOString(),
    },
    'success'
  );
}

async function createEmailSendLog(prospect, payload) {
  const pool = require('./db');
  const res = await pool.query(`
    INSERT INTO agent_log (agent_name, action, prospect_id, payload, status, ran_at, client_id)
    VALUES ($1, 'email_sent', $2, $3, 'pending', NOW(), $4)
    RETURNING id
  `, [AGENT_NAME, prospect.id, JSON.stringify(payload), CLIENT_ID]);
  return res.rows[0].id;
}

async function completeEmailSendLog(logId, payload, status = 'completed') {
  const pool = require('./db');
  await pool.query(`
    UPDATE agent_log
    SET status = $1,
        payload = $2
    WHERE id = $3
      AND client_id = $4
  `, [status, JSON.stringify(payload), logId, CLIENT_ID]);
}

async function getProspectsForEmail() {
  const pool = require('./db');

  const res = await pool.query(`
    SELECT
      p.*,
      c.name as company,
      COALESCE(email_stats.outbound_email_count, 0)::int AS outbound_email_count,
      email_stats.last_touchpoint_at
    FROM prospects p
    LEFT JOIN companies c ON p.company_id = c.id AND c.client_id = p.client_id
    LEFT JOIN LATERAL (
      SELECT
        COUNT(*) FILTER (WHERE t.channel = 'email' AND t.action_type = 'outbound')::int AS outbound_email_count,
        COUNT(*) FILTER (WHERE t.channel = 'email')::int AS email_touchpoint_count,
        MAX(t.created_at) AS last_touchpoint_at
      FROM touchpoints t
      WHERE t.prospect_id = p.id AND t.client_id = p.client_id
    ) email_stats ON true
    WHERE (p.status = 'cold' OR (
      p.status = 'warm'
      AND COALESCE(email_stats.outbound_email_count, 0) > 0
      AND email_stats.last_touchpoint_at <= NOW() - INTERVAL '14 days'
    ))
    AND p.client_id = $1
    AND p.email IS NOT NULL
    AND p.email != ''
    AND p.email NOT LIKE '%@domain.com'
    AND p.email NOT LIKE '%@example.com'
    AND p.do_not_contact IS NOT TRUE
    AND (
      p.status = 'warm'
      OR COALESCE(email_stats.email_touchpoint_count, 0) < 4
    )
    ORDER BY
      CASE WHEN p.status = 'warm' THEN 0 ELSE 1 END ASC,
      p.icp_score DESC NULLS LAST,
      p.last_contacted_at ASC NULLS FIRST
    LIMIT 100
  `, [CLIENT_ID]);

  return res.rows;
}

function getSequenceForProspect(prospect) {
  if (Number(prospect.client_id) === 2) return 'mshi';

  const lastTouchpointAt = prospect.last_touchpoint_at ? new Date(prospect.last_touchpoint_at) : null;
  const daysSinceLastTouchpoint = lastTouchpointAt
    ? (Date.now() - lastTouchpointAt.getTime()) / (1000 * 60 * 60 * 24)
    : null;
  if (
    prospect.status === 'warm' &&
    Number(prospect.outbound_email_count || 0) > 0 &&
    daysSinceLastTouchpoint !== null &&
    daysSinceLastTouchpoint >= 14
  ) {
    return 're_engagement';
  }

  const vertical = (prospect.vertical || '').toLowerCase();
  if (
    vertical === 'home_renovation' ||
    vertical === 'decks' ||
    vertical === 'siding' ||
    vertical === 'exterior_remodeling' ||
    vertical === 'interior_renovation' ||
    vertical === 'emergency_repair' ||
    vertical === 'windows'
  ) {
    return 'home_renovation';
  }
  if (vertical.includes('salon') || vertical.includes('hair') || vertical.includes('spa') || vertical.includes('barber')) {
    return 'salon';
  }
  if (vertical.includes('restaurant') || vertical.includes('cafe') || vertical.includes('diner')) {
    // A/B TEST — restaurant CTA test — remove when test is complete (target: 50 sends per variant)
    return prospect.id % 2 === 0 ? 'restaurant_b' : 'restaurant';
  }
  if (vertical.includes('fitness') || vertical.includes('gym') || vertical.includes('yoga') || vertical.includes('pilates') || vertical.includes('studio') || vertical.includes('barre')) {
    return 'fitness';
  }
  if (vertical.includes('property') || vertical.includes('management') || vertical.includes('millcity')) {
    return 'property';
  }
  if (vertical.includes('landscap') || vertical.includes('lawn')) {
    return 'landscaping';
  }
  if (vertical.includes('home') || vertical.includes('services') || vertical.includes('hvac') || vertical.includes('plumb') || vertical.includes('electric') || vertical.includes('handyman')) {
    return 'home_services';
  }
  if (vertical.includes('auto') || vertical.includes('repair') || vertical.includes('mechanic') || vertical.includes('car')) {
    return 'auto';
  }
  if (vertical.includes('med') || vertical.includes('aesthetic') || vertical.includes('botox') || vertical.includes('laser')) {
    return 'med_spa';
  }
  return 'cleaning';
}

async function getNextSequenceStep(prospect) {
  const pool = require('./db');
  const sequenceName = getSequenceForProspect(prospect);
  const isReEngagement = sequenceName === 're_engagement';

  const res = await pool.query(`
    SELECT * FROM touchpoints
    WHERE prospect_id = $1
    AND client_id = $2
    AND channel = 'email'
    AND ${
      isReEngagement
        ? "action_type = 'outbound' AND COALESCE(outcome, '') LIKE '%re_engagement%'"
        : "action_type IN ('outbound', 'email_warm')"
    }
    ORDER BY created_at ASC
  `, [prospect.id, CLIENT_ID]);

  const emailsSent = res.rows.length;
  const sequence = SEQUENCES[sequenceName];


  if (emailsSent >= sequence.length) {
    console.log('Sequence complete');
    return null;
  }

  const nextStep = sequence[emailsSent];
  console.log('nextStep:', nextStep?.day);

  if (emailsSent > 0) {
    const lastEmail = res.rows[res.rows.length - 1];
    const daysSinceLast = (Date.now() - new Date(lastEmail.created_at)) / (1000 * 60 * 60 * 24);
    const daysRequired = nextStep.day - sequence[emailsSent - 1].day;
    console.log('daysSinceLast:', daysSinceLast, 'daysRequired:', daysRequired);

    if (daysSinceLast < daysRequired) {
      console.log('Too soon');
      return null;
    }
  }

  return nextStep;
}

async function hasClickedEmail(prospectId) {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT 1 FROM touchpoints
    WHERE prospect_id = $1 AND client_id = $2 AND channel = 'email' AND action_type = 'email_clicked'
    LIMIT 1
  `, [prospectId, CLIENT_ID]);
  return res.rows.length > 0;
}

async function hasSentWarmEmail(prospectId) {
  const pool = require('./db');
  const res = await pool.query(`
    SELECT 1 FROM touchpoints
    WHERE prospect_id = $1 AND client_id = $2 AND channel = 'email' AND action_type = 'email_warm'
    LIMIT 1
  `, [prospectId, CLIENT_ID]);
  return res.rows.length > 0;
}

function humanDelay() {
  const ms = (45 + Math.random() * 45) * 1000; // 45–90 seconds
  console.log(`Waiting ${Math.round(ms / 1000)}s before next send...`);
  return new Promise(r => setTimeout(r, ms));
}

function isDashboardTrigger(context = {}) {
  return context?.triggered_by === 'dashboard' ||
    context?.triggeredBy === 'dashboard' ||
    context?.source === 'dashboard';
}

async function run(context = {}) {
  const dashboardOverride = isDashboardTrigger(context);
  if (!dashboardOverride && !isWithinSendingWindow()) {
    console.log(`Outside Emmett sending window (${sendingWindowLabel(CLIENT_ID)}) — skipping run`);
    await logSkippedOutsideWindow();
    return;
  }
  if (dashboardOverride) {
    console.log('Dashboard-triggered Emmett run — bypassing sending window check');
  }

  const HOLIDAYS_2026 = [
    '2026-01-01', '2026-01-19', '2026-02-16', '2026-05-25',
    '2026-07-04', '2026-09-07', '2026-11-11', '2026-11-26', '2026-12-25'
  ];
  const today = new Date().toISOString().split('T')[0];
  if (HOLIDAYS_2026.includes(today)) {
    console.log(`Holiday detected (${today}) — skipping run`);
    return;
  }

  console.log('\nEmmett agent running...\n');
  CLIENT_CONFIG = await getClientConfig(CLIENT_ID);
  if (!CLIENT_CONFIG) throw new Error(`Active client not found: ${CLIENT_ID}`);
  FROM_NAME = CLIENT_CONFIG.sender_name || FROM_NAME;

  const sendConfig = await getEffectiveSendConfig(getEmmettClientConfig(CLIENT_ID));
  const alreadySentToday = await getEmailsSentToday();
  const remainingCapacity = Math.max(0, sendConfig.dailyCap - alreadySentToday);
  console.log(`Daily cap: ${sendConfig.dailyCap}${sendConfig.ramped ? ' (ramped)' : ''}; already sent today: ${alreadySentToday}; remaining capacity: ${remainingCapacity}`);

  if (remainingCapacity <= 0) {
    console.log('Daily send limit already reached from database count.');
    await db.logAgentAction(
      AGENT_NAME,
      'cron_run',
      null,
      null,
      { sent: 0, prospects_evaluated: 0, daily_cap: sendConfig.dailyCap, already_sent_today: alreadySentToday, client_id: CLIENT_ID },
      'success'
    );
    return;
  }

  const prospects = await getProspectsForEmail();
  console.log(`Found ${prospects.length} prospects to contact\n`);

  console.log('Prospects found:', JSON.stringify(prospects, null, 2));

  let sent = 0;
  const dailyLimit = remainingCapacity;
  const verticalCap = sendConfig.verticalCap; // max sends per vertical per run
  const verticalCounts = {};

  for (const prospect of prospects) {
    if (sent >= dailyLimit) {
      console.log('Daily send limit reached.');
      break;
    }

    // Vertical cap — prevent blasting a single vertical in one run
    const vertical = (prospect.vertical || 'unknown').toLowerCase();
    verticalCounts[vertical] = (verticalCounts[vertical] || 0);
    if (verticalCounts[vertical] >= verticalCap) {
      console.log(`Skipping ${prospect.email} — vertical cap reached for "${vertical}"`);
      continue;
    }

    console.log('Processing prospect:', prospect.first_name, prospect.email);

    // Per-prospect DNC check (safety net in case status changed since query)
    const pool2 = require('./db');
    const dncCheck = await pool2.query(
      'SELECT do_not_contact FROM prospects WHERE id = $1 AND client_id = $2', [prospect.id, CLIENT_ID]
    );
    if (dncCheck.rows[0]?.do_not_contact) {
      console.log(`Skipping ${prospect.email} — do_not_contact`);
      continue;
    }

    let step;
    try {
      step = await getNextSequenceStep(prospect);
      console.log('Step result:', step);
    } catch (err) {
      console.error('getNextSequenceStep error:', err.message);
      continue;
    }
    if (!step) continue;

    // Check for warm email substitution on Day 4+ follow-ups
    let useWarm = false;
    const sequenceName = getSequenceForProspect(prospect);
    if (sequenceName !== 'mshi' && sequenceName !== 're_engagement' && step.day > 0) {
      const clicked  = await hasClickedEmail(prospect.id);
      const warmSent = await hasSentWarmEmail(prospect.id);
      if (clicked && warmSent) {
        console.log(`${prospect.email} already received warm email — skipping`);
        continue;
      }
      if (clicked && !warmSent) {
        useWarm = true;
        console.log(`Emmett: ${prospect.email} has clicked — sending warm sequence instead of standard follow-up`);
      }
    }

    const activeStep = useWarm ? WARM_STEP : step;
    const subject = fillTemplate(activeStep.subject, prospect);
    let body      = fillTemplate(activeStep.body,    prospect);
    body = stripForbiddenMshiCopy(body, prospect);

    console.log(`Sending to: ${prospect.email} (${prospect.name})`);
    console.log(`Subject: ${subject}`);

    const tags = [sequenceName, `step_${step.day}`, prospect.vertical].filter(Boolean);
    const logPayload = {
      sequence: sequenceName,
      step: step.day,
      vertical: prospect.vertical,
      subject,
      client_id: CLIENT_ID,
      email: prospect.email,
    };
    const sendLogId = await createEmailSendLog(prospect, logPayload);
    const result = await sendEmail(
      prospect.email,
      `${prospect.first_name} ${prospect.last_name}`,
      subject,
      body,
      tags
    );

    if (result.success) {
      await completeEmailSendLog(sendLogId, { ...logPayload, message_id: result.messageId }, 'completed');
      await db.logTouchpoint(
        prospect.id,
        'email',
        useWarm ? 'email_warm' : 'outbound',
        subject,
        useWarm ? { sequence: 'warm_outreach' } : { step: step.day, sequence: sequenceName === 're_engagement' ? 're_engagement' : 'cold_outreach' },
        'neutral'
      );
      if (step.day === 0 && !useWarm) {
        const pool = require('./db');
        await pool.query(
          `UPDATE prospects SET status = 'contacted', updated_at = NOW()
           WHERE id = $1 AND client_id = $2 AND status = 'cold'`,
          [prospect.id, CLIENT_ID]
        );
      }
      verticalCounts[vertical]++;
      sent++;
      console.log('Touchpoint logged.\n');
    } else {
      await completeEmailSendLog(sendLogId, { ...logPayload, error: result.error }, 'failed');
    }

    if (sent < dailyLimit) await humanDelay();
  }

  console.log(`\nEmmett complete. Emails sent: ${sent}`);
  await db.logAgentAction(
    AGENT_NAME,
    'cron_run',
    null,
    null,
    {
      sent,
      prospects_evaluated: prospects.length,
      daily_cap: sendConfig.dailyCap,
      already_sent_today: alreadySentToday,
      remaining_capacity: remainingCapacity,
      vertical_cap: verticalCap,
      client_id: CLIENT_ID,
    },
    'success'
  );
}

module.exports = { run };

if (require.main === module) {
  run().catch(async (err) => {
    try {
      await db.logAgentAction(
        AGENT_NAME,
        'cron_run',
        null,
        null,
        { client_id: CLIENT_ID },
        'failed',
        err.message
      );
    } catch (logErr) {
      console.error('Failed to log Emmett fatal error:', logErr.message);
    }
    console.error(err);
  });
}
