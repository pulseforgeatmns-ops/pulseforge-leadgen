// Approved Anchor sequences. Selection is client-scoped in CLIENT_SEQUENCE_MAP,
// and sending still requires the live Emmett readiness checks to pass.
const ANCHOR_DRAFT_SEQUENCES = {
  anchor_law_firm_draft: [
    {
      day: 0,
      subject: 'the office clients see before the meeting',
      body: `Hi {{first_name}},

Clients form an opinion of a law office before anyone opens a file. Reception, conference rooms, glass, and restrooms all speak first.

Legal offices have another concern too. Access instructions, document areas, and off-limits spaces can't be treated like an ordinary handoff. Anchor Cleaning works from a clear scope, with one person accountable when something needs attention.

Would a short walkthrough of {{business_name}} be useful?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 4,
      subject: 'when the conference room is not reset',
      body: `Hi {{first_name}},

A missed wastebasket sounds minor. It feels different when a client is sitting beside it for a sensitive meeting.

The point of a cleaning plan isn't a longer checklist. It's knowing who owns the result, what spaces need special handling, and how a miss gets corrected without three calls.

Is that accountability already clear with the crew cleaning {{business_name}}?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 8,
      subject: 'who owns the miss?',
      body: `Hi {{first_name}},

When an office looks right, nobody discusses the cleaning. Good.

When it doesn't, someone at the firm loses time finding the crew, repeating the standard, and checking the correction. Anchor keeps that responsibility in one place.

Would you be open to comparing your current scope with what we'd recommend for {{business_name}}?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 13,
      subject: 'a reliable backup, on file',
      body: `Hi {{first_name}},

Even if your current arrangement is working, having a local backup matters when coverage slips or the office needs a deeper reset before clients arrive.

If you'd like, I can walk the space and give you a clear scope to keep on file. No pressure to change what is already working.

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
  ],
  anchor_accounting_draft: [
    {
      day: 0,
      subject: 'cleaning should not become a busy-season task',
      body: `Hi {{first_name}},

During deadline weeks, nobody at an accounting firm should be checking whether the trash was handled or the client restroom was reset.

That work needs to happen predictably. Anchor Cleaning uses a clear scope and one accountable point of contact, so a cleaning issue doesn't become another item on the office manager's list.

Would a walkthrough of {{business_name}} be worth 10 minutes?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 4,
      subject: 'reliable when the office stays late',
      body: `Hi {{first_name}},

Busy season changes the building. Longer hours, more takeout, fuller bins, and more client traffic put extra pressure on a cleaning routine that looked fine in August.

The standard shouldn't wobble when {{business_name}} gets busy. The scope should be clear before the deadline rush, with one person responsible for the result.

Does your current plan adjust when the office hours stretch?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 8,
      subject: 'one less thing to check',
      body: `Hi {{first_name}},

Reliable cleaning is quiet. The team arrives to a reset office and gets back to client work.

Unreliable cleaning creates inspections, reminder emails, and awkward Monday mornings. Anchor's accountability is simple: the standard is written down, and you know exactly who owns the correction.

Would it help to compare that approach with the current setup at {{business_name}}?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 13,
      subject: 'before the next deadline crush',
      body: `Hi {{first_name}},

The easiest time to fix a shaky cleaning arrangement is before the office starts running late again.

I can put together a straightforward scope for {{business_name}} now, so you have a dependable option ready before the next filing deadline.

Would that be useful?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
  ],
};

module.exports = { ANCHOR_DRAFT_SEQUENCES };
