const AI_TELL_PHRASES = [
  "is doing a lot of work here",
  "doing the heavy lifting",
  "carrying a lot of weight",
  "it's important to note that",
  "let's unpack",
  "delve into",
  "dive deep",
  "in today's fast-paced world",
  "leverage", // when used as a verb (instruct model to prefer "use")
  "navigate", // when used as "deal with" (instruct model to prefer concrete verbs)
  "robust",
  "comprehensive",
  "that said,",
  "here's the thing,",
  "done right",
  "meets", // as in "X meets Y" overused metaphor
  "worth noting",
  "worth flagging"
];

const STRUCTURAL_RULES = `
Structural rules to follow when drafting:

- Default to contractions. Write "don't" not "do not", "we're" not "we are",
  "it's" not "it is". Only avoid contractions when emphasizing something
  specifically (e.g. "do not contact this person again" is appropriate
  emphasis).

- Vary sentence length aggressively. Mix 3-word sentences with 25-word
  sentences. Use fragments. Don't level out into uniform medium-length prose.

- Avoid triadic structures (groups of three) as default rhythm. Mix in
  twos, fours, fives. Lists of three are an AI tell when overused.

- Skip smooth transitions. Jump between thoughts. Don't always start with
  "Furthermore," or "Additionally," or "That said,".

- Don't always balance arguments. AI tends to write "on the one hand X, on
  the other hand Y." Pick a side sometimes.

- Reach for vivid or unexpected word choices over predictable ones when both
  work. "Strangulation" over "decrease." "Cratered" over "declined."

- Don't wrap up neatly at the end. Sometimes cut mid-thought.

- Avoid constant hedging (might, could, potentially) when uncertainty isn't
  actually being expressed.

- No em dashes in body copy. Subject lines and titles only.
`;

function buildVoiceConstraintBlock() {
  const phraseList = AI_TELL_PHRASES.map(p => `- "${p}"`).join('\n');
  return `
VOICE CONSTRAINTS:

Never use these phrases or close variants. They are AI tells that trigger
detection and read as machine-generated:

${phraseList}

${STRUCTURAL_RULES}
`.trim();
}

module.exports = { AI_TELL_PHRASES, STRUCTURAL_RULES, buildVoiceConstraintBlock };
