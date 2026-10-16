import argparse
from datetime import datetime, timezone

from kalshi_research.config import Settings
from kalshi_research.collector import PaperCollector
from kalshi_research.data.btc import CoinbaseSpotPrice
from kalshi_research.data.kalshi import KalshiRestMarketData
from kalshi_research.domain import MarketSnapshot
from kalshi_research.engine.paper import PaperExecutionEngine
from kalshi_research.engine.portfolio import VirtualPortfolio
from kalshi_research.engine.research import ResearchRunner
from kalshi_research.engine.risk import RiskLimits, RiskManager
from kalshi_research.logging import configure_logging
from kalshi_research.reporting.metrics import summarize
from kalshi_research.reporting.health import format_data_health, inspect_data
from kalshi_research.reporting.diagnostics import format_diagnostics, inspect_diagnostics
from kalshi_research.features import format_feature_report, inspect_feature_report
from kalshi_research.settlement import SettlementResolver
from kalshi_research.outcomes import resolve_captured_outcomes
from kalshi_research.hypotheses import (
    evaluate_hypothesis,
    format_hypothesis_evaluation,
    list_hypotheses,
)
from kalshi_research.replay import (
    DEFAULT_FEE_RATE,
    format_replay,
    format_replay_sweep,
    format_train_test_split,
    replay_buy_below,
    replay_threshold_sweep,
    replay_train_test_split,
)
from kalshi_research.strategies.threshold import BuyBelowThreshold


def main() -> None:
    parser = argparse.ArgumentParser(description="Paper-trading research tools")
    parser.add_argument(
        "command",
        choices=[
            "demo",
            "init-db",
            "collect-once",
            "collect-loop",
            "list-btc-series",
            "report",
            "resolve-settlements",
            "resolve-outcomes",
            "replay",
            "replay-sweep",
            "replay-split",
            "diagnose-data",
            "feature-report",
            "list-hypotheses",
            "evaluate-hypothesis",
        ],
    )
    parser.add_argument("--max-yes-ask", type=int, default=35)
    parser.add_argument("--min-threshold", type=int, default=1)
    parser.add_argument("--max-threshold", type=int, default=99)
    parser.add_argument("--top", type=int, default=10)
    parser.add_argument("--train-fraction", type=float, default=0.5)
    parser.add_argument("--hypothesis-id", type=str, default="H-005")
    parser.add_argument("--walk-forward-folds", type=int, default=4)
    parser.add_argument("--sensitivity-radius", type=int, default=2)
    parser.add_argument(
        "--fee-rate", type=float, default=DEFAULT_FEE_RATE,
        help=(
            "Fee multiplier applied to replay results, mirroring Kalshi's general taker-fee "
            f"formula fee=ceil(rate * price * (1-price)). Default: {DEFAULT_FEE_RATE} "
            "(conservative). Applies only to replay/replay-sweep/replay-split."
        ),
    )
    parser.add_argument(
        "--min-edge-cents", type=int, default=0,
        help=(
            "Optional deterministic filter: only take an entry if the fee-adjusted best case "
            "outweighs the fee-adjusted worst case by at least this many cents. 0 disables the "
            "filter (default). Applies only to replay/replay-sweep/replay-split."
        ),
    )
    args = parser.parse_args()
    settings = Settings()
    configure_logging(settings.log_level)
    from kalshi_research.storage.repository import ResearchRepository
    if args.command == "init-db":
        ResearchRepository(settings.database_url).initialize()
        print("Research database initialized.")
        return
    if args.command == "report":
        print(format_data_health(inspect_data(settings.database_url)))
        return
    if args.command == "diagnose-data":
        print(format_diagnostics(inspect_diagnostics(
            settings.database_url, fee_rate=args.fee_rate, train_fraction=args.train_fraction,
        )))
        return
    if args.command == "feature-report":
        print(format_feature_report(inspect_feature_report(
            settings.database_url, train_fraction=args.train_fraction,
        )))
        return
    if args.command == "list-hypotheses":
        for hyp in list_hypotheses():
            print(
                f"{hyp.hypothesis_id}\t{hyp.name}\t{hyp.status}\t"
                f"{hyp.condition_feature} {hyp.condition_op}"
            )
        return
    if args.command == "evaluate-hypothesis":
        report = evaluate_hypothesis(
            settings.database_url,
            hypothesis_id=args.hypothesis_id,
            train_fraction=args.train_fraction,
            fee_rate=args.fee_rate,
            walk_forward_folds=args.walk_forward_folds,
            sensitivity_radius=args.sensitivity_radius,
        )
        print(format_hypothesis_evaluation(report))
        return
    if args.command == "resolve-settlements":
        market_data = KalshiRestMarketData(str(settings.kalshi_api_base_url), settings.kalshi_timeout_seconds)
        try:
            summary = SettlementResolver(market_data, ResearchRepository(settings.database_url)).resolve()
            print(f"Settlement check complete: {summary.resolved} resolved, {summary.still_open} still open, {summary.checked} checked.")
        finally:
            market_data.close()
        return
    if args.command == "resolve-outcomes":
        market_data = KalshiRestMarketData(str(settings.kalshi_api_base_url), settings.kalshi_timeout_seconds)
        try:
            resolved, pending = resolve_captured_outcomes(market_data, ResearchRepository(settings.database_url))
            print(f"Market outcome check complete: {resolved} resolved, {pending} pending.")
        finally:
            market_data.close()
        return
    if args.command == "replay":
        print(format_replay(replay_buy_below(
            settings.database_url, args.max_yes_ask,
            fee_rate=args.fee_rate, min_edge_cents=args.min_edge_cents,
        )))
        return
    if args.command == "replay-sweep":
        rows = replay_threshold_sweep(
            settings.database_url, args.min_threshold, args.max_threshold,
            fee_rate=args.fee_rate, min_edge_cents=args.min_edge_cents,
        )
        print(format_replay_sweep(rows, args.top))
        return
    if args.command == "replay-split":
        summary = replay_train_test_split(
            settings.database_url,
            args.min_threshold,
            args.max_threshold,
            args.train_fraction,
            fee_rate=args.fee_rate,
            min_edge_cents=args.min_edge_cents,
        )
        print(format_train_test_split(summary))
        return
    if args.command == "list-btc-series":
        market_data = KalshiRestMarketData(str(settings.kalshi_api_base_url), settings.kalshi_timeout_seconds)
        try:
            keyword = settings.kalshi_btc_market_keyword.upper()
            aliases = (keyword, "BITCOIN") if keyword == "BTC" else (keyword,)
            for series in market_data.list_series():
                searchable = " ".join((str(series.get("ticker", "")), str(series.get("title", "")),
                                         " ".join(str(tag) for tag in (series.get("tags") or [])))).upper()
                if any(term in searchable for term in aliases):
                    print("\t".join((str(series.get("ticker", "")), str(series.get("title", "")),
                                      str(series.get("frequency", "")), str(series.get("category", "")))))
        finally:
            market_data.close()
        return
    if args.command in ("collect-once", "collect-loop"):
        portfolio = VirtualPortfolio(settings.initial_cash_cents)
        risk = RiskManager(RiskLimits(settings.max_position_cents, settings.max_contracts_per_market,
                                      settings.max_daily_loss_cents, settings.max_open_orders))
        market_data = KalshiRestMarketData(str(settings.kalshi_api_base_url), settings.kalshi_timeout_seconds)
        btc_data = CoinbaseSpotPrice(settings.kalshi_timeout_seconds)
        try:
            runner = ResearchRunner(BuyBelowThreshold(), PaperExecutionEngine(portfolio, risk))
            collector = PaperCollector(market_data, runner, ResearchRepository(settings.database_url), btc_data,
                                       settings.kalshi_btc_market_keyword, settings.collector_market_limit,
                                       tuple(x.strip() for x in settings.kalshi_btc_series_tickers.split(",") if x.strip()))
            if args.command == "collect-once":
                print(f"Captured {collector.collect_once()} matching market snapshots.")
            else:
                print("Paper collector running. Press Ctrl+C to stop.")
                collector.run_forever(settings.collector_interval_seconds)
        finally:
            market_data.close()
            btc_data.close()
        return
    if args.command == "demo":
        portfolio = VirtualPortfolio(settings.initial_cash_cents)
        risk = RiskManager(RiskLimits(settings.max_position_cents, settings.max_contracts_per_market,
                                      settings.max_daily_loss_cents, settings.max_open_orders))
        runner = ResearchRunner(BuyBelowThreshold(), PaperExecutionEngine(portfolio, risk))
        runner.process(MarketSnapshot("KXBTC15M-DEMO", datetime.now(timezone.utc), 32, 34, 33))
        print(summarize(portfolio, runner.execution.orders))


if __name__ == "__main__":
    main()
