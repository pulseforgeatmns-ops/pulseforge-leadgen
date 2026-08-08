"""Reconstruct a synthetic cohort from published feature-report midpoint gaps.

Not a substitute for the operator's local research DB. Used only when
kalshi_research.db is absent so the evaluation path can emit an explicit verdict.
Entry-time midpoint / ask only — no path features.
"""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from kalshi_research.hypotheses.evaluate import EntryMarket, evaluate_hypothesis, format_hypothesis_evaluation


def build_cohort(seed: int = 5) -> list[EntryMarket]:
    """256 train + 257 test markets with midpoint means matching feature-report."""
    rng = random.Random(seed)
    start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def block(n_yes: int, n_no: int, yes_mean: float, no_mean: float, t0: int, prefix: str) -> list[EntryMarket]:
        rows: list[tuple[int, int, str]] = []
        for _ in range(n_yes):
            rows.append(_quote(rng, yes_mean, lo=35.0, hi=70.0) + ("yes",))
        for _ in range(n_no):
            rows.append(_quote(rng, no_mean, lo=30.0, hi=70.0) + ("no",))
        rng.shuffle(rows)
        out: list[EntryMarket] = []
        for idx, (bid, ask, result) in enumerate(rows):
            out.append(
                EntryMarket(
                    ticker=f"{prefix}{t0 + idx}",
                    first_observed_at=start + timedelta(minutes=t0 + idx),
                    first_yes_ask_cents=ask,
                    first_yes_bid_cents=bid,
                    midpoint_cents=(bid + ask) / 2.0,
                    result=result,
                )
            )
        return out

    return (
        block(128, 128, 52.60, 46.60, 0, "TR")
        + block(129, 128, 52.24, 45.96, 1000, "TE")
    )


def _quote(rng: random.Random, mean: float, lo: float, hi: float) -> tuple[int, int]:
    mid = min(hi, max(lo, mean + rng.uniform(-6.0, 6.0)))
    spread = rng.choice([1, 2, 3, 4])
    bid = int(round(mid - spread / 2))
    ask = int(round(mid + spread / 2))
    ask = min(99, max(1, ask))
    bid = min(ask - 1, max(0, bid))
    return bid, ask


def main() -> None:
    report = evaluate_hypothesis(
        database_url="sqlite://",
        hypothesis_id="H-005",
        train_fraction=0.5,
        fee_rate=0.07,
        walk_forward_folds=4,
        sensitivity_radius=2,
        markets=build_cohort(),
    )
    print("NOTE: Synthetic cohort from published feature-report midpoint means.")
    print("Authoritative verdict requires local kalshi_research.db (~513 resolved).")
    print()
    print(format_hypothesis_evaluation(report))


if __name__ == "__main__":
    main()
