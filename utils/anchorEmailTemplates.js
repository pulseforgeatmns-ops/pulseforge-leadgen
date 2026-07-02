// Approved Anchor sequences. Selection is client-scoped in CLIENT_SEQUENCE_MAP,
// and sending still requires the live Emmett readiness checks to pass.
const ANCHOR_DRAFT_SEQUENCES = {
  anchor_law_firm_draft: [
    {
      day: 0,
      subject: 'the office clients see first',
      body: `Hi {{first_name|}},

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
      body: `Hi {{first_name|}},

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
      body: `Hi {{first_name|}},

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
      body: `Hi {{first_name|}},

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
      subject: 'Cleaning that survives busy season',
      body: `Hi {{first_name|}},

Come March, {{business_name_short|your office}} can't afford a cleaning routine that needs chasing. That's the month a shaky setup shows itself, and the month nobody has a minute to fix one.

I've run service businesses for over a decade, restaurants then cleaning. Same lesson both times: the work either happens on its own, or it becomes someone's job to chase. Anchor runs on a written scope and one accountable owner. Me.

Want me to walk the office? Ten minutes, no pitch.

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 4,
      subject: "March isn't August",
      body: `Hi {{first_name|}},

An accounting office in March isn't the same building it was in August. Hours run long and the bins fill twice as fast. Takeout shows up at desks that never used to see it.

That's exactly when a cleaning routine starts to wobble, and exactly when nobody has a spare minute to manage it. Anchor sets the scope before the crush so it doesn't move during one.

Does your current arrangement hold in busy season, or does someone end up babysitting it?

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 8,
      subject: "partners shouldn't inspect the cleaning",
      protectedSegments: [
        "I spent years running restaurant crews before I ever ran cleaning crews, and the standard only held when a specific person answered for it.",
      ],
      body: `Hi {{first_name|}},

Good cleaning is invisible. The office resets overnight and nobody thinks about it again.

Bad cleaning turns partners into inspectors. I spent years running restaurant crews before I ever ran cleaning crews, and the standard only held when a specific person answered for it. Anchor still works that way. The scope is written down and every correction has an owner.

If anyone at {{business_name_short|your office}} has been playing inspector, reply "send it" and I'll share what our scope looks like.

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
    {
      day: 13,
      subject: "fix it while it's quiet",
      body: `Hi {{first_name|}},

Last one from me. Shaky cleaning arrangements get fixed in the quiet months or not at all. Once the office starts running late again, this drops to the bottom of the list and stays there.

I can have a scope ready for {{business_name_short|your office}} this week. If the timing's wrong, tell me when your busy season ends and I'll come back then.

Jacob Maynard
Anchor Cleaning
jacob@goanchorcleaning.com
(603) 420-2430`,
    },
  ],
};

module.exports = { ANCHOR_DRAFT_SEQUENCES };
