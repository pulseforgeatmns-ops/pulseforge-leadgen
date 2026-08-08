"""Immutable hypothesis / rule-candidate registry (paper/replay research only)."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping


#: Features that encode post-entry or path movement. Entry-time rules must not use them.
FORBIDDEN_ENTRY_FEATURES: frozenset[str] = frozenset({"ask_move_cents", "btc_move_usd"})


@dataclass(frozen=True, slots=True)
class HypothesisCandidate:
    """Immutable definition of a deterministic rule candidate.

    Status values:
      - candidate: defined and awaiting / undergoing evaluation
      - probably_noise: failed holdout, walk-forward, and/or sensitivity checks
      - retired: permanently closed; must not be retuned
      - promoted: survived formal robustness checks (still paper/replay only)
    """

    hypothesis_id: str
    name: str
    description: str
    status: str
    entry_side: str
    entry_price_feature: str
    condition_feature: str
    condition_op: str
    train_threshold_min: int | None = None
    train_threshold_max: int | None = None
    notes: str = ""

    def __post_init__(self) -> None:
        if self.status not in {"candidate", "probably_noise", "retired", "promoted"}:
            raise ValueError(f"invalid hypothesis status: {self.status}")
        used = {self.entry_price_feature, self.condition_feature}
        leaked = used & FORBIDDEN_ENTRY_FEATURES
        if leaked:
            raise ValueError(
                f"{self.hypothesis_id} uses forbidden post-entry features: {sorted(leaked)}"
            )


#: Registry is frozen at import time. Mutating definitions requires a new hypothesis ID.
_HYPOTHESES: dict[str, HypothesisCandidate] = {
    "H-001": HypothesisCandidate(
        hypothesis_id="H-001",
        name="BuyWhenEntryMidpointBelow",
        description="Buy YES when entry midpoint < 40c. Failed robustness; do not retune.",
        status="retired",
        entry_side="yes",
        entry_price_feature="first_yes_ask_cents",
        condition_feature="midpoint_cents",
        condition_op="<",
        train_threshold_min=40,
        train_threshold_max=40,
        notes="Failed robustness. Must not be retuned under the same ID.",
    ),
    "H-005": HypothesisCandidate(
        hypothesis_id="H-005",
        name="BuyWhenEntryMidpointAbove",
        description=(
            "Buy YES at first_yes_ask when entry midpoint is strictly above a train-selected "
            "threshold in [50, 65]. Entry-time book features only."
        ),
        status="candidate",
        entry_side="yes",
        entry_price_feature="first_yes_ask_cents",
        condition_feature="midpoint_cents",
        condition_op=">",
        train_threshold_min=50,
        train_threshold_max=65,
        notes=(
            "Motivated by feature-report midpoint YES/NO separation "
            "(train Δ≈+6.0c, test Δ≈+6.3c). Opposite direction from retired H-001."
        ),
    ),
}


def get_hypothesis(hypothesis_id: str) -> HypothesisCandidate:
    try:
        return _HYPOTHESES[hypothesis_id]
    except KeyError as exc:
        raise KeyError(f"unknown hypothesis id: {hypothesis_id}") from exc


def list_hypotheses() -> tuple[HypothesisCandidate, ...]:
    return tuple(_HYPOTHESES[key] for key in sorted(_HYPOTHESES))


def hypothesis_registry() -> Mapping[str, HypothesisCandidate]:
    """Read-only view of the immutable registry."""
    return _HYPOTHESES
