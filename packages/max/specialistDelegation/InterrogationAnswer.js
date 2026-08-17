'use strict';

/**
 * SPEC-101 — operator-facing interrogation answers from a cognitive trace.
 * Deterministic. Does not invent missing layers. Does not replay the
 * previous specialist result when the operator asked a different question.
 */

const { asText } = require('./Types');
const { FAILURE_BOUNDARIES, EVIDENCE_LAYERS } = require('./CognitiveTrace');
const { geographyLabel, contextFieldPresent } = require('./ContextLayers');

const VERBATIM_REPLAY_RE =
  /couldn'?t construct a candidate universe because geography couldn'?t be resolved/i;

function specialistName(trace) {
  const raw = String((trace && trace.specialist) || 'the specialist');
  return raw.charAt(0).toUpperCase() + raw.slice(1);
}

function suppliedGeography(trace) {
  return geographyLabel(
    trace &&
      trace.suppliedContext &&
      (trace.suppliedContext.geography || trace.suppliedContext.serviceArea)
  );
}

function availableGeography(trace) {
  return geographyLabel(
    trace &&
      trace.availableContext &&
      (trace.availableContext.serviceArea || trace.availableContext.geography)
  );
}

function consumedGeography(trace) {
  return geographyLabel(
    trace &&
      trace.consumedContext &&
      (trace.consumedContext.geography || trace.consumedContext.serviceArea)
  );
}

function companiesEvaluated(trace) {
  const coverage = trace && trace.investigation && trace.investigation.coverage;
  if (coverage && coverage.candidatesEvaluated != null) {
    return Number(coverage.candidatesEvaluated);
  }
  return null;
}

function coverageBand(trace) {
  return (
    (trace && trace.investigation && trace.investigation.coverageBand) ||
    (trace && trace.maxEvaluation && trace.maxEvaluation.coverageBand) ||
    null
  );
}

function answerGeographyFailure(trace) {
  const boundary = trace.failure && trace.failure.boundary;
  const available = availableGeography(trace);
  const supplied = suppliedGeography(trace);
  const consumed = consumedGeography(trace);
  const name = specialistName(trace);
  const availableRecorded = trace.availableContext && trace.availableContext.recorded === true;
  const consumedRecorded = trace.consumedContext && trace.consumedContext.recorded === true;

  if (boundary === FAILURE_BOUNDARIES.DELEGATION) {
    return (
      `I know ${available ? `${available} is in our service area` : 'the service area'}, ` +
      `but that geography wasn't included in the delegation I sent ${name}. ` +
      `${name} therefore received the objective without a usable location. ` +
      `That's a delegation failure on my side, not evidence that ${name} searched ` +
      `${available || 'the service area'} and found nothing.`
    );
  }

  if (boundary === FAILURE_BOUNDARIES.SPECIALIST_INTERPRETATION) {
    return (
      `I supplied ${supplied} in the delegation. ${name} received it, ` +
      `but the search definition failed to convert it into a usable geography. ` +
      `The failure occurred inside ${name}'s geography resolution rather than in my context handoff.`
    );
  }

  if (boundary === FAILURE_BOUNDARIES.CONTEXT_RETRIEVAL) {
    return (
      `I didn't have a usable service-area geography in the context I could retrieve ` +
      `when I asked ${name} to investigate. ${name} therefore had nothing to resolve. ` +
      `That's a context retrieval failure on my side, not a market finding.`
    );
  }

  if (boundary === FAILURE_BOUNDARIES.UNKNOWN || !boundary) {
    const sawFailure =
      (trace.consumedContext &&
        /geography could not be resolved/i.test(
          String(trace.consumedContext.invalidReason || '')
        )) ||
      (trace.result &&
        /geography could not be resolved/i.test(String(trace.result.summary || '')));
    if (sawFailure && !availableRecorded && !supplied) {
      return (
        `I can confirm that ${name} reported geography resolution failure. ` +
        `The current trace doesn't preserve whether a service-area location was present in the delegation, ` +
        `so I can't tell whether the problem occurred in my handoff or ${name}'s resolver. ` +
        `I don't have enough evidence to tell you which happened.`
      );
    }
    if (sawFailure && supplied && consumedRecorded && !consumed) {
      return (
        `I supplied ${supplied} in the delegation. ${name} received it, ` +
        `but couldn't convert it into a usable geography.`
      );
    }
    return (
      `I can see that geography resolution failed, but the trace doesn't currently tell me ` +
      `whether the location was omitted from my delegation or rejected inside ${name}. ` +
      `I don't have enough evidence to tell you which happened.`
    );
  }

  return (
    `I know where the investigation failed, but I don't yet have evidence showing why ` +
    `the resolver rejected the geography.`
  );
}

function answerSuppliedGeography(trace) {
  const supplied = suppliedGeography(trace);
  const name = specialistName(trace);
  if (supplied) {
    return `I gave ${name} ${supplied} as the geography for this investigation.`;
  }
  if (trace.suppliedContext && trace.suppliedContext.recorded) {
    return (
      `I did not include a usable geography in the delegation I sent ${name} for this investigation.`
    );
  }
  return (
    `The current trace doesn't preserve the geographic information I included in the delegation, ` +
    `so I can't confirm what ${name} was given.`
  );
}

function answerMaxJudgment(trace) {
  const evaluation = trace.maxEvaluation;
  const name = specialistName(trace);
  const evaluated = companiesEvaluated(trace);
  const band = coverageBand(trace);
  const coverageConfidence =
    (evaluation && evaluation.coverageConfidence) ||
    (trace.result && trace.result.coverageConfidence);
  const elevated =
    evaluation &&
    (evaluation.materialChange === true ||
      (evaluation.priorityEffect && evaluation.priorityApplied === true));

  if (elevated) {
    return (
      evaluation.interpretation ||
      `I changed Acquisition's priority because ${name} returned business evidence I accepted as material.`
    );
  }

  const universeFailed =
    evaluated === 0 ||
    (trace.consumedContext &&
      /geography could not be resolved/i.test(
        String(trace.consumedContext.invalidReason || '')
      ));

  if (universeFailed) {
    return (
      `I didn't change Acquisition's priority because ${name} never constructed a candidate universe, ` +
      `evaluated ${evaluated == null ? 'zero' : evaluated} compan${evaluated === 1 ? 'y' : 'ies'}, ` +
      `and coverage was ${band || 'too weak'} ` +
      `${coverageConfidence != null ? `(${coverageConfidence})` : ''}. `.replace(/\s+/g, ' ') +
      `There wasn't enough business evidence to justify changing priority. ` +
      `The zero is an investigation failure, not a market conclusion.`
    );
  }

  if (evaluation && evaluation.interpretation) {
    const cleaned = String(evaluation.interpretation)
      .replace(/I am not elevating Acquisition\.?/gi, '')
      .trim();
    return (
      `I didn't change Acquisition's priority because ${cleaned} ` +
      `That judgment is mine — not a restatement of ${name}'s conclusion as ground truth.`
    ).replace(/\s+/g, ' ');
  }

  return (
    `I didn't change Acquisition's priority because there wasn't enough accepted business evidence ` +
    `to treat ${name}'s result as a reason to elevate it.`
  );
}

function answerTrust(trace) {
  const name = specialistName(trace);
  const evaluation = trace.maxEvaluation;
  const band = coverageBand(trace);
  const accepted = (evaluation && evaluation.acceptedFindings) || [];
  const rejected = (evaluation && evaluation.rejectedFindings) || [];
  if (evaluation && evaluation.conclusionTrust === 'low') {
    return (
      `I don't treat ${name}'s conclusion as a reliable market claim. ` +
      `Coverage was ${band || 'weak'}, so I can trust what he reported about the work he completed ` +
      `without treating a zero as evidence that the market is empty.` +
      (rejected.length ? ` I rejected ${rejected.length} inference${rejected.length === 1 ? '' : 's'}.` : '')
    );
  }
  if (accepted.length && evaluation && evaluation.acceptedAsGroundTruth === false) {
    return (
      `I accepted ${accepted.length} observation${accepted.length === 1 ? '' : 's'} from ${name} as evidence, ` +
      `not as ground truth. Coverage was ${band || 'limited'}.`
    );
  }
  return (
    `I trust the parts of ${name}'s result that are backed by investigation provenance. ` +
    `I do not accept the conclusion as ground truth.`
  );
}

function answerEvidence(trace) {
  const business = (trace.evidenceLayers && trace.evidenceLayers[EVIDENCE_LAYERS.BUSINESS]) || [];
  const investigation = trace.investigation;
  const evaluated = companiesEvaluated(trace);
  const parts = [];
  if (business.length) {
    parts.push(
      `Business evidence: ${business
        .slice(0, 3)
        .map((ref) => ref.label || ref.summary || ref.id)
        .join('; ')}.`
    );
  } else {
    parts.push('There is no business evidence about the market from this investigation.');
  }
  if (investigation && investigation.coverage) {
    parts.push(
      `Investigation provenance: ${evaluated == null ? 0 : evaluated} companies evaluated` +
        `${investigation.coverage.candidatesDiscovered != null ? `, ${investigation.coverage.candidatesDiscovered} discovered` : ''}.`
    );
  }
  parts.push('System provenance records the delegation and evaluation, not market facts.');
  return parts.join(' ');
}

function answerProspectList(trace, input = {}) {
  const evaluated = companiesEvaluated(trace);
  const detected = Number(
    input.detectedCompanyCount != null
      ? input.detectedCompanyCount
      : input.prospectCount != null
        ? input.prospectCount
        : 0
  );
  const name = specialistName(trace);
  const universeBlocked =
    evaluated === 0 ||
    (trace.consumedContext &&
      /geography could not be resolved/i.test(
        String(trace.consumedContext.invalidReason || '')
      ));

  if (detected > 0 && universeBlocked) {
    return (
      `The prospect list showing ${detected} compan${detected === 1 ? 'y' : 'ies'} is a separate surface ` +
      `from ${name}'s investigation. ${name} never constructed a candidate universe` +
      `${evaluated === 0 ? ' and evaluated zero companies' : ''}, so I cannot treat those listed companies ` +
      `as ones he reviewed. I don't have evidence that they were in his search, and I won't invent a relationship.`
    );
  }

  if (detected > 0 && evaluated != null && evaluated !== detected) {
    return (
      `I can see a prospect list with ${detected} compan${detected === 1 ? 'y' : 'ies'} ` +
      `and ${name} evaluated ${evaluated}. I don't have evidence that those listed companies ` +
      `were the same ones in his candidate universe, so I won't connect them.`
    );
  }

  return (
    `I can't establish why those companies weren't evaluated from the current trace. ` +
    `I won't fabricate a relationship between the prospect list and ${name}'s investigation.`
  );
}

function answerServiceArea(trace, input = {}) {
  const availableNow = geographyLabel(input.currentServiceArea) || availableGeography(trace);
  const availableThen = availableGeography(trace);
  const supplied = suppliedGeography(trace);
  const recorded = trace.availableContext && trace.availableContext.recorded === true;

  if (availableNow && supplied && availableNow === supplied) {
    return (
      `I currently understand the service area as ${availableNow}. ` +
      `That same geography was included in the delegation.`
    );
  }
  if (availableNow && recorded && !supplied) {
    return (
      `I currently understand the service area as ${availableNow}. ` +
      `I had ${availableThen || availableNow} available when I asked ${specialistName(trace)}, ` +
      `but I did not include it in the delegation.`
    );
  }
  if (availableNow && !recorded) {
    return (
      `I currently understand the service area as ${availableNow}. ` +
      `The historical trace doesn't record whether I had that geography at delegation time, ` +
      `so I can't use today's understanding to rewrite what I knew then.`
    );
  }
  if (!availableNow) {
    return (
      `I don't currently have a retrieved service-area geography in this context` +
      `${supplied ? `, though the last delegation included ${supplied}` : ''}.`
    );
  }
  return `I currently understand the service area as ${availableNow}.`;
}

function answerGeneral(trace) {
  const name = specialistName(trace);
  const asked = (trace.delegation && trace.delegation.requestedTask) || trace.operatorObjective;
  const reason = trace.delegation && trace.delegation.reason;
  const supplied = suppliedGeography(trace);
  const consumed = consumedGeography(trace);
  const evaluated = companiesEvaluated(trace);
  const boundary = trace.failure && trace.failure.boundary;
  const parts = [
    `I asked ${name} to ${asked || 'investigate the current objective'}` +
      (reason ? ` because ${reason}` : '') +
      '.',
  ];
  if (supplied) parts.push(`I gave him ${supplied}.`);
  else if (trace.suppliedContext && trace.suppliedContext.recorded) {
    parts.push('I did not include a usable geography in that delegation.');
  }
  if (consumed) parts.push(`He consumed ${consumed}.`);
  else if (trace.consumedContext && trace.consumedContext.recorded) {
    parts.push('He did not successfully consume a geography.');
  }
  if (evaluated != null) {
    parts.push(`He evaluated ${evaluated} compan${evaluated === 1 ? 'y' : 'ies'}.`);
  }
  if (boundary === FAILURE_BOUNDARIES.UNKNOWN) {
    parts.push(
      `I know where the investigation failed, but I don't yet have evidence showing why.`
    );
  } else if (boundary) {
    parts.push(`The failure sits at the ${boundary.replace(/_/g, ' ')}.`);
  }
  if (trace.maxEvaluation && !trace.maxEvaluation.materialChange) {
    parts.push(
      `I didn't treat that as market evidence strong enough to change Acquisition's priority.`
    );
  }
  return parts.join(' ');
}

function answerNeedsNewWork(trace, question) {
  const name = specialistName(trace);
  return (
    `I can tell you why the current investigation failed from the existing trace. ` +
    `I cannot determine whether a different geography would produce better coverage ` +
    `without asking ${name} to investigate it. That would be a new, bounded delegation — ` +
    `not an explanation of the work already done.`
  );
}

/**
 * @param {object} input
 * @returns {{ prose: string, rerun: false, topic: string, verbatimReplay: false }}
 */
function answerFromTrace(input = {}) {
  const trace = input.trace;
  const question = String(input.question || '');
  const intent = input.intent || {};

  if (!trace) {
    return {
      prose:
        'I don\'t have a recent specialist investigation I can inspect for this question. ' +
        'I won\'t invent an explanation.',
      rerun: false,
      topic: 'missing_trace',
      verbatimReplay: false,
    };
  }

  if (intent.recommendNewWork) {
    return {
      prose: answerNeedsNewWork(trace, question),
      rerun: false,
      topic: 'needs_new_work',
      verbatimReplay: false,
    };
  }

  let prose;
  const topic = intent.topic || 'general';
  if (topic === 'geography' || /geograph|what geographic|what did you give/i.test(question)) {
    if (/what (?:geographic information|information) did you (?:give|send|provide)/i.test(question)) {
      const why = /why couldn'?t/i.test(question) ? `${answerGeographyFailure(trace)} ` : '';
      prose = `${why}${answerSuppliedGeography(trace)}`.trim();
    } else {
      prose = `${answerGeographyFailure(trace)} ${answerSuppliedGeography(trace)}`;
    }
  } else if (topic === 'max_judgment' || /elevate|priority|immaterial/i.test(question)) {
    prose = answerMaxJudgment(trace);
  } else if (topic === 'trust') {
    prose = answerTrust(trace);
  } else if (topic === 'evidence') {
    prose = answerEvidence(trace);
  } else if (topic === 'prospect_list') {
    prose = answerProspectList(trace, input);
  } else if (topic === 'service_area') {
    prose = answerServiceArea(trace, input);
  } else if (/what did .+ (?:actually )?(?:investigate|do|observe)/i.test(question)) {
    prose = answerGeneral(trace);
  } else {
    prose = answerGeneral(trace);
  }

  const previous =
    (trace.result && trace.result.summary) ||
    (trace.maxEvaluation && trace.maxEvaluation.interpretation) ||
    '';
  const verbatimReplay =
    VERBATIM_REPLAY_RE.test(prose) ||
    (previous &&
      prose.trim() === String(previous).trim() &&
      !/I (?:asked|supplied|gave|know|can confirm|didn't)/i.test(prose));

  if (verbatimReplay) {
    prose =
      `I can see the previous result, but that doesn't answer your question. ` +
      answerGeographyFailure(trace);
  }

  return {
    prose: prose.replace(/\s+/g, ' ').trim(),
    rerun: false,
    topic,
    verbatimReplay: false,
    failureBoundary: trace.failure && trace.failure.boundary,
    traceId: trace.traceId,
  };
}

function specialistHasTraceContract(trace) {
  return Boolean(
    trace &&
      (trace.delegation ||
        trace.result ||
        trace.availableContext ||
        trace.suppliedContext ||
        trace.consumedContext)
  );
}

function limitationAnswer(specialist) {
  const name = asText(specialist) || 'that specialist';
  return (
    `I don't have an inspectable cognitive trace for ${name} yet. ` +
    `The interrogation architecture is generic, but this specialist hasn't persisted ` +
    `delegation, result, and evaluation records I can reason from. I won't invent the missing work.`
  );
}

module.exports = {
  answerFromTrace,
  answerGeographyFailure,
  answerSuppliedGeography,
  answerMaxJudgment,
  answerTrust,
  answerEvidence,
  answerProspectList,
  answerServiceArea,
  specialistHasTraceContract,
  limitationAnswer,
  VERBATIM_REPLAY_RE,
};
