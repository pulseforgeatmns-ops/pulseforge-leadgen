// Approved Anchor sequences. Selection is client-scoped in CLIENT_SEQUENCE_MAP,
// and sending still requires the live Emmett readiness checks to pass.
const ANCHOR_DRAFT_SEQUENCES = {
  anchor_law_firm_draft: [
    {
      day: 0,
      subject: 'the office clients see first',
      body: `Hi {{first_name}},

Clients size up a firm in the lobby, before anyone opens a file. Smudged glass and a tired restroom say plenty.

A law office isn't a normal cleaning job either. There are alarm codes, and there are rooms a crew has no business wandering into. I've spent years running restaurants and a cleaning company, so Anchor works from a written scope. When something's off, you call one person. Me.

Worth a short walkthrough of {{business_name_short}}?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 4,
      subject: "when the conference room isn't reset",
      protectedSegments: [
        "I've run service businesses for over a decade",
      ],
      body: `Hi {{first_name}},

A missed wastebasket sounds like nothing. It stops being nothing when a client is sitting next to it during a sensitive meeting.

I've run service businesses for over a decade, and most cleaning failures trace back to the same gap: nobody clearly owns the correction. So when the crew cleaning {{business_name_short}} misses something, who fixes it, and how many calls does that take?

At Anchor it's one call.

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 8,
      subject: 'who owns the miss?',
      body: `Hi {{first_name}},

When the office looks right, nobody mentions the cleaning. That's how it should be.

When it doesn't, someone at the firm loses an afternoon chasing the crew and re-explaining the standard. Usually someone whose billable rate makes that an expensive afternoon.

Happy to put our recommended scope next to whatever you're running at {{business_name_short}}. Ten minutes.

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 13,
      subject: 'a backup on file',
      body: `Hi {{first_name}},

Maybe your current setup is fine. Plenty are, right up until a skipped night lands before a client morning.

I can walk {{business_name_short}}, write a clear scope, and you keep it on file. If you never use it, it cost you fifteen minutes.

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

Would a walkthrough of {{business_name_short}} be worth 10 minutes?

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

The standard shouldn't wobble when {{business_name_short}} gets busy. The scope should be clear before the deadline rush, with one person responsible for the result.

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

Would it help to compare that approach with the current setup at {{business_name_short}}?

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

I can put together a straightforward scope for {{business_name_short}} now, so you have a dependable option ready before the next filing deadline.

Would that be useful?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
  ],
};

module.exports = { ANCHOR_DRAFT_SEQUENCES };
