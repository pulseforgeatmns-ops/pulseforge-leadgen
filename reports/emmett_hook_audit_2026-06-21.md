# Emmett Hook Audit — 2026-06-21

## Summary

- Total active templates: 43 unique sequence-step definitions
- Hook: 3 (7.0%)
- Mixed: 8 (18.6%)
- Positioning: 32 (74.4%)
- Step 1 pass rate: 0.0% (0 of 10 cold first-touch openings)
- Templates flagged for rewriting: 40

The audit population is the set of template definitions in `emmettAgent.js` that are reachable by an active, Emmett-enabled production client and its configured or live prospect verticals. Shared definitions are scored once, even when two clients use them. Template IDs below are stable source-derived identifiers because production has no template table or database template ID.

## Scope and routing notes

- Railway was verified before querying: project `charming-trust`, service `pulseforge-leadgen`, environment `production`.
- Production has no `email_templates`, `emmett_templates`, or equivalent body store. `clients.email_sequence` contains only a sequence name; `sequences` contains prospect enrollments. Subjects and bodies are code-backed in `emmettAgent.js`.
- Active Emmett-enabled clients are Pulseforge (`client_id=1`), Mountain State Home Innovations (`client_id=2`), and Pulseforge Nashville (`client_id=5`). Whittaker is not present in the production `clients` table and therefore has no active templates to audit in this context.
- MSHI routes `property_management` to `mshi_property_management`. Every other MSHI vertical routes to the same `mshi` catch-all, including `probate_attorney`, `investor_flipper`, `insurance_restoration`, `renovation_lender`, `home_inspector`, and `listing_agent`. A historical send used the deprecated `probate_estate` label; it is grouped here under the required canonical `probate_attorney` name.
- No `bank_reo` prospect, configured vertical, route, or active template is present in production.
- `med_spa` currently matches the earlier `spa` condition and routes to `salon`. The defined `med_spa` sequence is therefore not reachable for the active `med_spa` vertical and is excluded. The defined `home_renovation` sequence is also excluded because client 2 overrides all non-property-management verticals to `mshi`, and clients 1 and 5 have no active home-renovation assignment.
- Step 2 and later were judged with the requested relaxed standard. A prior-touch reference can provide context, but an opening that only says the note is a follow-up or final message still fails.

## Findings by Client > Vertical

### Mountain State Home Innovations — property_management

#### Step 1 — template_id: `mshi_property_management:step_1`

**Subject:** one crew for your turns and repairs  
**Opening:** “Brad here from Mountain State Home Innovations.”  
**Classification:** Positioning  
**Reasoning:** Introduces the sender and company without naming a property manager's observable problem.  
**Rewrite (if needed):** Every day a damaged unit waits on a contractor is another day it cannot produce rent.

#### Step 2 — template_id: `mshi_property_management:step_2`

**Subject:** Re: one crew for your turns and repairs  
**Opening:** “Just following up in case my last note got buried.”  
**Classification:** Positioning  
**Reasoning:** References inbox position only and does not re-anchor on the turn-time problem.  
**Rewrite (if needed):** If a make-ready is waiting on separate repair crews, each handoff is adding vacancy days.

#### Step 3 — template_id: `mshi_property_management:step_3`

**Subject:** What we've done for other property managers in WV  
**Opening:** “We've done subcontract work with some of the larger WV firms — Tri-State Exterior Solutions, St Albans Windows, Secure Construction — so we know what quality at scale looks like.”  
**Classification:** Positioning  
**Reasoning:** Leads with credentials and proof rather than a problem in the recipient's portfolio.  
**Rewrite (if needed):** When one turn needs siding, deck, and interior repairs, coordinating separate crews can add days before the unit is rent-ready.

#### Step 4 — template_id: `mshi_property_management:step_4`

**Subject:** Closing the loop  
**Opening:** “I don't want to keep filling up your inbox so I'll leave it here.”  
**Classification:** Positioning  
**Reasoning:** Describes the sender's decision to stop emailing and names no prospect problem.  
**Rewrite (if needed):** If contractor delays are still stretching a vacancy window, we can assess the next turn this week.

### Mountain State Home Innovations — non-property_management catch-all

Active verticals routed here include `probate_attorney`, `investor_flipper`, `commercial_real_estate`, `home_renovation`, `insurance_restoration`, `real_estate_developer`, `renovation_lender`, `home_inspector`, and `listing_agent`. Reusing one opening across these distinct buying contexts is itself a material weakness.

#### Step 1 — template_id: `mshi:step_1`

**Subject:** Quick question about {{business_name}}  
**Opening:** “My name is Brad — I run Mountain State Home Innovations out of Charleston with my partner Dustin.”  
**Classification:** Positioning  
**Reasoning:** It is entirely sender introduction and cannot signal relevance to any of the routed verticals.  
**Rewrite (if needed):** Probate properties can sit exposed to weather damage for weeks while heirs wait for a contractor who will document repairs and keep everyone updated.

#### Step 2 — template_id: `mshi:step_2`

**Subject:** Re: Quick question about {{business_name}}  
**Opening:** “Just following up in case my last note got buried.”  
**Classification:** Positioning  
**Reasoning:** It is a generic bump and does not restore relevance for the prospect's property workflow.  
**Rewrite (if needed):** When a property repair stalls after the estimate, the closing, claim, or project timeline stalls with it.

#### Step 3 — template_id: `mshi:step_3`

**Subject:** What we've done for other property managers in WV  
**Opening:** “We've done subcontract work with some of the larger WV firms — Tri-State Exterior Solutions, St Albans Windows, Secure Construction — so we know what quality at scale looks like.”  
**Classification:** Positioning  
**Reasoning:** The sentence is proof about the contractor, not a recognizable issue for the recipient.  
**Rewrite (if needed):** A property with unresolved siding, deck, or water damage can hold up a sale, claim, or renovation draw.

#### Step 4 — template_id: `mshi:step_4`

**Subject:** Closing the loop  
**Opening:** “I don't want to keep filling up your inbox so I'll leave it here.”  
**Classification:** Positioning  
**Reasoning:** It closes the cadence without re-anchoring on a property delay or repair risk.  
**Rewrite (if needed):** If an unresolved exterior repair is still holding up one of your properties, we can assess it this week.

### Pulseforge and Pulseforge Nashville — cleaning

This sequence also handles Pulseforge prospects with an unknown vertical through the default fallback.

#### Step 1 — template_id: `cleaning:step_1`

**Subject:** {{business_name}} — honest question  
**Opening:** “Most cleaning companies in Manchester are getting leads — the problem is usually what happens after.”  
**Classification:** Mixed  
**Reasoning:** It names a category-level follow-up problem but provides no signal that it is true for this company.  
**Rewrite (if needed):** A quote request that sits unanswered until the end of the cleaning day is usually calling the next company by then.

#### Step 2 — template_id: `cleaning:step_2`

**Subject:** still thinking about {{business_name}}  
**Opening:** “Didn't hear back — totally fine, just wanted to make sure my last note landed.”  
**Classification:** Positioning  
**Reasoning:** It is a response-status check rather than a renewed problem hook.  
**Rewrite (if needed):** If quote requests at {{business_name}} wait until someone has time to call back, those leads are going cold between jobs.

#### Step 3 — template_id: `cleaning:step_3`

**Subject:** quick question about {{business_name}}  
**Opening:** “One last question — are you currently doing anything to follow up with leads automatically, or is it still mostly manual when you get a chance?”  
**Classification:** Hook  
**Reasoning:** Under the relaxed follow-up standard, it asks about a specific, observable lead-follow-up workflow and its manual bottleneck.

#### Step 4 — template_id: `cleaning:step_4`

**Subject:** closing the loop  
**Opening:** “I won't keep filling up your inbox.”  
**Classification:** Positioning  
**Reasoning:** It discusses email frequency, not the cost of slow quote follow-up.  
**Rewrite (if needed):** Any cleaning quote that has gone unanswered since my first note is probably already comparing another provider.

### Pulseforge and Pulseforge Nashville — restaurant

#### Step 1 — template_id: `restaurant:step_1`

**Subject:** {{business_name}} — honest question  
**Opening:** “I spent years running restaurants in New England.”  
**Classification:** Positioning  
**Reasoning:** Establishes sender background but names no restaurant problem.  
**Rewrite (if needed):** Guests who visit {{business_name}} once but never hear from you again have no reason to choose you over the next new spot.

#### Step 2 — template_id: `restaurant:step_2`

**Subject:** still thinking about {{business_name}}  
**Opening:** “Sent you a note a few days ago.”  
**Classification:** Positioning  
**Reasoning:** Merely references the earlier send and gives the reader no reason to continue.  
**Rewrite (if needed):** A slow Tuesday stays slow when last month's guests are not getting a reason to come back.

#### Step 3 — template_id: `restaurant:step_3`

**Subject:** what is actually working in Manchester right now  
**Opening:** “One thing I am seeing across local restaurants right now.”  
**Classification:** Positioning  
**Reasoning:** Teases an observation but the first sentence itself contains no problem.  
**Rewrite (if needed):** Restaurants relying only on social posts are missing past guests who would return for a timely offer.

#### Step 4 — template_id: `restaurant:step_4`

**Subject:** closing the loop  
**Opening:** “Last note from me.”  
**Classification:** Positioning  
**Reasoning:** Cadence housekeeping replaces the problem hook.  
**Rewrite (if needed):** If weekday tables are still uneven, the leak is often the gap between a guest's first visit and their next invitation.

### Pulseforge and Pulseforge Nashville — salon and med_spa

The `salon` definition serves salon prospects and, because of routing order, the active `med_spa` vertical.

#### Step 1 — template_id: `salon:step_1`

**Subject:** {{business_name}} — honest question  
**Opening:** “Salons in Manchester live and die by repeat bookings and referrals — but the ones growing fastest right now are the ones capturing new clients automatically between appointments.”  
**Classification:** Mixed  
**Reasoning:** Names a meaningful vertical problem, but it is a broad market claim with no proxy tied to this salon.  
**Rewrite (if needed):** Every client who leaves {{business_name}} without a future appointment is a chair you have to refill later.

#### Step 2 — template_id: `salon:step_2`

**Subject:** still thinking about {{business_name}}  
**Opening:** “Following up in case my last note got buried.”  
**Classification:** Positioning  
**Reasoning:** It is a generic bump and does not mention rebooking or client acquisition.  
**Rewrite (if needed):** If clients leave without rebooking, next month's calendar starts developing gaps today.

#### Step 3 — template_id: `salon:step_3`

**Subject:** quick question about {{business_name}}  
**Opening:** “One last question — are you currently doing anything to bring in new clients outside of referrals and social media, or is that still the main source?”  
**Classification:** Hook  
**Reasoning:** Under the relaxed follow-up standard, it asks about a concrete acquisition dependency the recipient can verify.

#### Step 4 — template_id: `salon:step_4`

**Subject:** closing the loop  
**Opening:** “Last note from me.”  
**Classification:** Positioning  
**Reasoning:** It contains no booking, retention, or acquisition problem.  
**Rewrite (if needed):** A client overdue for a cut, color, or treatment is a booking opportunity sitting untouched in your list.

### Pulseforge and Pulseforge Nashville — fitness

#### Step 1 — template_id: `fitness:step_1`

**Subject:** {{business_name}} — honest question  
**Opening:** “I work with local fitness studios and gyms in Southern NH on one specific problem: keeping your schedule full without spending your time chasing new members.”  
**Classification:** Mixed  
**Reasoning:** A real category problem is present, but the sentence leads with who the sender serves and offers no prospect-level evidence.  
**Rewrite (if needed):** Every trial guest who leaves without a same-day follow-up is more likely to join the studio that texts first.

#### Step 2 — template_id: `fitness:step_2`

**Subject:** still thinking about {{business_name}}  
**Opening:** “Sent you a note a few days ago.”  
**Classification:** Positioning  
**Reasoning:** It references timing only and does not re-anchor on trial conversion or class occupancy.  
**Rewrite (if needed):** If trial leads wait until the front desk has time to follow up, many will book their first class somewhere else.

#### Step 3 — template_id: `fitness:step_3`

**Subject:** what is actually working for fitness studios in Southern NH  
**Opening:** “One thing I am seeing across local studios right now.”  
**Classification:** Positioning  
**Reasoning:** The setup contains no problem until a later sentence, so the opening itself fails.  
**Rewrite (if needed):** Studios with full classes are following up with trial leads within minutes, while slower replies lose the signup.

#### Step 4 — template_id: `fitness:step_4`

**Subject:** closing the loop  
**Opening:** “Last note from me.”  
**Classification:** Positioning  
**Reasoning:** It is cadence language with no recognizable fitness-studio pain.  
**Rewrite (if needed):** Any trial lead from this month who has not booked a first class is still an empty spot on next week's schedule.

### Pulseforge — property and property_management

#### Step 1 — template_id: `property:step_1`

**Subject:** {{business_name}} — honest question  
**Opening:** “I work with local property management companies in Southern NH on one specific problem: keeping units filled without relying entirely on Zillow and word of mouth.”  
**Classification:** Mixed  
**Reasoning:** Names a legitimate category problem but leads with positioning and no property-specific signal.  
**Rewrite (if needed):** Each extra vacancy day at {{business_name}} costs rent while the same listing competes beside dozens of similar units.

#### Step 2 — template_id: `property:step_2`

**Subject:** still thinking about {{business_name}}  
**Opening:** “Sent you a note a few days ago.”  
**Classification:** Positioning  
**Reasoning:** It does not reconnect the follow-up to vacancy days or leasing-response speed.  
**Rewrite (if needed):** If an inquiry waits until your team finishes tenant issues, that prospect is often touring another unit first.

#### Step 3 — template_id: `property:step_3`

**Subject:** what is actually working for property managers in Southern NH  
**Opening:** “One thing I am seeing across local property management companies right now.”  
**Classification:** Positioning  
**Reasoning:** It promises an observation but names no observable problem in the first sentence.  
**Rewrite (if needed):** Property managers lose leasing leads when they reply after the prospect has already booked another showing.

#### Step 4 — template_id: `property:step_4`

**Subject:** closing the loop  
**Opening:** “Last note from me.”  
**Classification:** Positioning  
**Reasoning:** It closes the sequence without returning to vacancy or lead-response pain.  
**Rewrite (if needed):** An unreturned listing inquiry from this week is a vacancy day waiting to happen.

### Pulseforge and Pulseforge Nashville — landscaping

#### Step 1 — template_id: `landscaping:step_1`

**Subject:** {{business_name}} — honest question  
**Opening:** “I work with local lawn care and landscaping companies in Southern NH on one specific problem: keeping the schedule full year round without relying on door knocking and referrals alone.”  
**Classification:** Mixed  
**Reasoning:** It states a vertical pain but remains a sender-led category claim without a prospect-specific proxy.  
**Rewrite (if needed):** When spring quote requests pile up faster than your crew can answer them, summer maintenance slots go to the company that replied first.

#### Step 2 — template_id: `landscaping:step_2`

**Subject:** still thinking about {{business_name}}  
**Opening:** “Sent you a note a few days ago.”  
**Classification:** Positioning  
**Reasoning:** It is a bump with no seasonal pipeline or estimate-response problem.  
**Rewrite (if needed):** A landscaping quote that waits until the crew gets off-site is often booked elsewhere before dinner.

#### Step 3 — template_id: `landscaping:step_3`

**Subject:** what is actually working for landscapers in Southern NH  
**Opening:** “One thing I am seeing across local landscaping companies right now.”  
**Classification:** Positioning  
**Reasoning:** The opening delays the actual claim and contains no problem on its own.  
**Rewrite (if needed):** Landscapers entering summer without recurring work are rebuilding the pipeline from zero after the spring rush.

#### Step 4 — template_id: `landscaping:step_4`

**Subject:** closing the loop  
**Opening:** “Last note from me.”  
**Classification:** Positioning  
**Reasoning:** It is about sequence completion rather than the recipient's pipeline.  
**Rewrite (if needed):** Any spring estimate that never got a second follow-up is a summer job still sitting in your lead list.

### Pulseforge and Pulseforge Nashville — home_services

#### Step 1 — template_id: `home_services:step_1`

**Subject:** {{business_name}} — honest question  
**Opening:** “I work with local home service companies in Southern NH on one specific problem: staying visible to homeowners between jobs so the pipeline does not go quiet.”  
**Classification:** Mixed  
**Reasoning:** It names a broad pipeline problem but begins with sender positioning and no observable signal.  
**Rewrite (if needed):** A homeowner who calls during a job and reaches voicemail usually calls the next contractor before you can call back.

#### Step 2 — template_id: `home_services:step_2`

**Subject:** still thinking about {{business_name}}  
**Opening:** “Sent you a note a few days ago.”  
**Classification:** Positioning  
**Reasoning:** The sentence supplies chronology but no reason grounded in missed calls or lead leakage.  
**Rewrite (if needed):** If missed calls at {{business_name}} wait until the crew is off-site, those jobs are often booked before the callback.

#### Step 3 — template_id: `home_services:step_3`

**Subject:** what is actually working for home services in Southern NH  
**Opening:** “One thing I am seeing across local home service companies right now.”  
**Classification:** Positioning  
**Reasoning:** It is an empty setup sentence and leaves the actual problem for later.  
**Rewrite (if needed):** Home service companies lose the highest-intent jobs when missed calls and web forms sit unanswered during the workday.

#### Step 4 — template_id: `home_services:step_4`

**Subject:** closing the loop  
**Opening:** “Last note from me.”  
**Classification:** Positioning  
**Reasoning:** It is sender-focused cadence language, not a home-service pain.  
**Rewrite (if needed):** Every missed call from this week that has not received a follow-up is a job another contractor can win.

### Pulseforge and Pulseforge Nashville — auto and auto_repair

#### Step 1 — template_id: `auto:step_1`

**Subject:** {{business_name}} — honest question  
**Opening:** “Auto shops in Manchester are busier than ever right now — but the ones adding real revenue aren't just doing more jobs, they're capturing customers who never came back after the first visit.”  
**Classification:** Mixed  
**Reasoning:** It names a recognizable retention problem, but only as a broad market generalization.  
**Rewrite (if needed):** Every first-time customer who leaves {{business_name}} without a service reminder is one oil change away from becoming another shop's regular.

#### Step 2 — template_id: `auto:step_2`

**Subject:** still thinking about {{business_name}}  
**Opening:** “Just following up — didn't want my last note to get lost.”  
**Classification:** Positioning  
**Reasoning:** It is inbox housekeeping and does not re-anchor on customer retention.  
**Rewrite (if needed):** If first-time customers are not hearing from you before their next service interval, another shop gets the chance to bring them back.

#### Step 3 — template_id: `auto:step_3`

**Subject:** quick question about {{business_name}}  
**Opening:** “Last question — are you currently doing anything to follow up with customers after a job, or is it mostly repeat and referral when it happens?”  
**Classification:** Hook  
**Reasoning:** Under the relaxed follow-up standard, it asks about a specific and observable post-service retention process.

#### Step 4 — template_id: `auto:step_4`

**Subject:** closing the loop  
**Opening:** “Won't keep filling up your inbox.”  
**Classification:** Positioning  
**Reasoning:** It discusses the sender's cadence, not missed repeat-service revenue.  
**Rewrite (if needed):** Any customer due for service who has not heard from {{business_name}} this month is revenue sitting in the repair history.

### Pulseforge and Pulseforge Nashville — re_engagement

This sequence is reachable for warm prospects with prior outbound email and at least 14 days since the last touchpoint, regardless of vertical.

#### Step 1 — template_id: `re_engagement:step_1`

**Subject:** still thinking about {{business_name}}  
**Opening:** “Reached out a few weeks ago — wanted to check back in before moving on.”  
**Classification:** Positioning  
**Reasoning:** References the prior contact and sender intent but gives the prospect no renewed problem context.  
**Rewrite (if needed):** If new inquiries at {{business_name}} are still waiting hours for a reply, those leads are probably contacting competitors before you respond.

#### Step 2 — template_id: `re_engagement:step_2`

**Subject:** one thing I'm seeing in Manchester right now  
**Opening:** “One pattern I keep seeing across local businesses right now — the ones picking up new customers consistently aren't doing anything complicated.”  
**Classification:** Mixed  
**Reasoning:** It gestures at a category-level acquisition gap but is not tied to this prospect or a concrete observable proxy.  
**Rewrite (if needed):** Local businesses are losing ready-to-buy inquiries when forms, calls, and DMs wait until someone has time to follow up.

#### Step 3 — template_id: `re_engagement:step_3`

**Subject:** closing the loop on {{business_name}}  
**Opening:** “Last note from me — I don't want to clutter your inbox.”  
**Classification:** Positioning  
**Reasoning:** It closes the cadence without naming the unresolved acquisition problem.  
**Rewrite (if needed):** If unanswered inquiries are still sitting in {{business_name}}'s pipeline, each one gets colder every day.

## Cross-template findings

The dominant failure pattern is cadence narration: “sent you a note,” “following up,” and “last note from me.” Those openings consume the highest-attention sentence without restating a problem. The second most common pattern is delayed relevance, where the first sentence introduces the sender or teases an observation and the actual pain appears later.

The most severe client/vertical cluster is MSHI's non-`property_management` catch-all. It scores 0 of 4, and one generic sequence is serving materially different contexts such as probate attorneys, investors, restoration firms, lenders, inspectors, and listing agents. Splitting Step 1 by vertical would create the largest immediate gain. Restaurant also scores 0 of 4, but it is at least a single coherent vertical.
