"""
Phase 10C — Walk-Forward Validation and Monte Carlo Stress Testing

This module evaluates the zero-lookahead institutional backtest engine across
chronological folds and randomized trade-path simulations.

It never modifies the live scanner or SQLite database.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import random
import statistics
from dataclasses import asdict, dataclass, replace
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

try:
    from backtesting.backtest_engine import (
        BacktestEngineError,
        BacktestMetrics,
        ConservativeCompositeStrategy,
        EngineConfiguration,
        InstitutionalBacktestEngine,
        RiskLimits,
    )
    from backtesting.event_builder import (
        BacktestEvent,
        BacktestEventCollection,
        EventBuilderError,
        build_backtest_events,
    )
    from backtesting.historical_dataset import DEFAULT_DATABASE_PATH
except ModuleNotFoundError:
    from backtest_engine import (  # type: ignore
        BacktestEngineError,
        BacktestMetrics,
        ConservativeCompositeStrategy,
        EngineConfiguration,
        InstitutionalBacktestEngine,
        RiskLimits,
    )
    from event_builder import (  # type: ignore
        BacktestEvent,
        BacktestEventCollection,
        EventBuilderError,
        build_backtest_events,
    )
    from historical_dataset import DEFAULT_DATABASE_PATH  # type: ignore


LOGGER = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIRECTORY = Path("backtesting") / "validation_results"
DEFAULT_FOLDS_CSV = DEFAULT_OUTPUT_DIRECTORY / "walk_forward_folds.csv"
DEFAULT_MONTE_CARLO_CSV = DEFAULT_OUTPUT_DIRECTORY / "monte_carlo_paths.csv"
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIRECTORY / "validation_report.json"


class ValidationEngineError(RuntimeError):
    """Base exception for validation failures."""


class InvalidValidationConfigurationError(ValidationEngineError):
    """Raised when walk-forward or Monte Carlo settings are invalid."""


@dataclass(frozen=True, slots=True)
class WalkForwardConfiguration:
    train_cycles: int = 6
    test_cycles: int = 3
    step_cycles: int = 2
    minimum_train_events: int = 300
    minimum_test_events: int = 100

    def validate(self) -> None:
        for name in (
            "train_cycles",
            "test_cycles",
            "step_cycles",
            "minimum_train_events",
            "minimum_test_events",
        ):
            value = int(getattr(self, name))
            if value <= 0:
                raise InvalidValidationConfigurationError(
                    f"{name} must be positive."
                )


@dataclass(frozen=True, slots=True)
class MonteCarloConfiguration:
    simulations: int = 5_000
    random_seed: int = 7
    fee_multiplier_min: float = 0.90
    fee_multiplier_max: float = 1.35
    slippage_multiplier_min: float = 0.90
    slippage_multiplier_max: float = 1.50
    missed_trade_probability: float = 0.05
    failed_execution_probability: float = 0.02
    failed_execution_cost_usd: float = 0.0001

    def validate(self) -> None:
        if self.simulations <= 0:
            raise InvalidValidationConfigurationError(
                "simulations must be positive."
            )

        for name in (
            "fee_multiplier_min",
            "fee_multiplier_max",
            "slippage_multiplier_min",
            "slippage_multiplier_max",
        ):
            value = float(getattr(self, name))
            if not math.isfinite(value) or value <= 0:
                raise InvalidValidationConfigurationError(
                    f"{name} must be positive and finite."
                )

        if self.fee_multiplier_min > self.fee_multiplier_max:
            raise InvalidValidationConfigurationError(
                "fee multiplier minimum exceeds maximum."
            )

        if self.slippage_multiplier_min > self.slippage_multiplier_max:
            raise InvalidValidationConfigurationError(
                "slippage multiplier minimum exceeds maximum."
            )

        for name in (
            "missed_trade_probability",
            "failed_execution_probability",
        ):
            value = float(getattr(self, name))
            if not 0.0 <= value <= 1.0:
                raise InvalidValidationConfigurationError(
                    f"{name} must be in [0, 1]."
                )

        if self.failed_execution_cost_usd < 0:
            raise InvalidValidationConfigurationError(
                "failed_execution_cost_usd cannot be negative."
            )


@dataclass(frozen=True, slots=True)
class StrategyParameters:
    minimum_composite_score: float
    minimum_market_score: float
    minimum_liquidity_score: float
    minimum_intelligence_score: float
    maximum_score_dispersion: float

    def to_dict(self) -> dict[str, float]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class WalkForwardFoldResult:
    fold_number: int
    train_cycle_start: int
    train_cycle_end: int
    test_cycle_start: int
    test_cycle_end: int
    train_events: int
    test_events: int
    selected_parameters: StrategyParameters
    train_metrics: BacktestMetrics
    test_metrics: BacktestMetrics

    def to_record(self) -> dict[str, Any]:
        record = {
            "fold_number": self.fold_number,
            "train_cycle_start": self.train_cycle_start,
            "train_cycle_end": self.train_cycle_end,
            "test_cycle_start": self.test_cycle_start,
            "test_cycle_end": self.test_cycle_end,
            "train_events": self.train_events,
            "test_events": self.test_events,
        }

        for key, value in self.selected_parameters.to_dict().items():
            record[f"parameter_{key}"] = value

        for prefix, metrics in (
            ("train", self.train_metrics),
            ("test", self.test_metrics),
        ):
            for key, value in asdict(metrics).items():
                record[f"{prefix}_{key}"] = value

        return record


@dataclass(frozen=True, slots=True)
class MonteCarloPathResult:
    simulation: int
    ending_capital_usd: float
    net_profit_usd: float
    total_return_pct: float
    maximum_drawdown_pct: float
    winning_trades: int
    losing_trades: int
    missed_trades: int
    failed_executions: int

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ValidationSummary:
    generated_at: datetime
    folds: int
    profitable_test_folds: int
    unprofitable_test_folds: int
    total_test_trades: int
    aggregate_test_profit_usd: float
    average_test_return_pct: float
    worst_test_drawdown_pct: float
    median_monte_carlo_ending_capital_usd: float
    p05_monte_carlo_ending_capital_usd: float
    p95_monte_carlo_ending_capital_usd: float
    probability_of_finishing_below_start: float
    median_monte_carlo_drawdown_pct: float
    p95_monte_carlo_drawdown_pct: float

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["generated_at"] = self.generated_at.isoformat(sep=" ")
        return result


class StrategyGridSearch:
    """Simple chronological training-only parameter selection."""

    def __init__(self) -> None:
        self.parameter_grid = tuple(
            StrategyParameters(
                minimum_composite_score=composite,
                minimum_market_score=market,
                minimum_liquidity_score=liquidity,
                minimum_intelligence_score=intelligence,
                maximum_score_dispersion=dispersion,
            )
            for composite in (75.0, 80.0, 85.0, 90.0)
            for market in (70.0, 80.0, 90.0)
            for liquidity in (70.0, 80.0, 90.0)
            for intelligence in (50.0, 60.0, 70.0)
            for dispersion in (15.0, 25.0, 35.0)
        )

    def select(
        self,
        train_events: BacktestEventCollection,
        engine_configuration: EngineConfiguration,
    ) -> tuple[StrategyParameters, BacktestMetrics]:
        best_parameters: StrategyParameters | None = None
        best_metrics: BacktestMetrics | None = None
        best_score: tuple[float, ...] | None = None

        for parameters in self.parameter_grid:
            strategy = ConservativeCompositeStrategy(
                minimum_composite_score=parameters.minimum_composite_score,
                minimum_market_score=parameters.minimum_market_score,
                minimum_liquidity_score=parameters.minimum_liquidity_score,
                minimum_intelligence_score=parameters.minimum_intelligence_score,
                maximum_score_dispersion=parameters.maximum_score_dispersion,
            )

            result = InstitutionalBacktestEngine(
                strategy,
                engine_configuration,
            ).run(train_events)

            metrics = result.metrics

            score = (
                1.0 if metrics.net_profit_usd > 0 else 0.0,
                metrics.net_profit_usd,
                -metrics.maximum_drawdown_pct,
                metrics.profit_factor,
                metrics.win_rate_pct,
                -float(metrics.trades == 0),
            )

            if best_score is None or score > best_score:
                best_score = score
                best_parameters = parameters
                best_metrics = metrics

        if best_parameters is None or best_metrics is None:
            raise ValidationEngineError(
                "Grid search did not produce a valid strategy."
            )

        return best_parameters, best_metrics


class WalkForwardValidator:
    def __init__(
        self,
        configuration: WalkForwardConfiguration | None = None,
        *,
        engine_configuration: EngineConfiguration | None = None,
    ) -> None:
        self.configuration = configuration or WalkForwardConfiguration()
        self.configuration.validate()
        self.engine_configuration = (
            engine_configuration
            or EngineConfiguration(
                risk=RiskLimits(),
            )
        )
        self.engine_configuration.validate()
        self.search = StrategyGridSearch()

    def run(
        self,
        events: BacktestEventCollection,
    ) -> tuple[WalkForwardFoldResult, ...]:
        grouped = events.group_by_cycle()
        cycle_ids = list(grouped.keys())
        folds: list[WalkForwardFoldResult] = []

        start = 0
        fold_number = 1

        while (
            start
            + self.configuration.train_cycles
            + self.configuration.test_cycles
            <= len(cycle_ids)
        ):
            train_ids = cycle_ids[
                start : start + self.configuration.train_cycles
            ]
            test_start = start + self.configuration.train_cycles
            test_ids = cycle_ids[
                test_start : test_start + self.configuration.test_cycles
            ]

            train_events = _build_subset_collection(
                event
                for cycle_id in train_ids
                for event in grouped[cycle_id]
            )
            test_events = _build_subset_collection(
                event
                for cycle_id in test_ids
                for event in grouped[cycle_id]
            )

            if (
                len(train_events)
                < self.configuration.minimum_train_events
                or len(test_events)
                < self.configuration.minimum_test_events
            ):
                start += self.configuration.step_cycles
                continue

            selected_parameters, train_metrics = self.search.select(
                train_events,
                self.engine_configuration,
            )

            test_strategy = ConservativeCompositeStrategy(
                minimum_composite_score=(
                    selected_parameters.minimum_composite_score
                ),
                minimum_market_score=(
                    selected_parameters.minimum_market_score
                ),
                minimum_liquidity_score=(
                    selected_parameters.minimum_liquidity_score
                ),
                minimum_intelligence_score=(
                    selected_parameters.minimum_intelligence_score
                ),
                maximum_score_dispersion=(
                    selected_parameters.maximum_score_dispersion
                ),
            )

            test_result = InstitutionalBacktestEngine(
                test_strategy,
                self.engine_configuration,
            ).run(test_events)

            folds.append(
                WalkForwardFoldResult(
                    fold_number=fold_number,
                    train_cycle_start=(
                        train_events.events[0].cycle_number
                    ),
                    train_cycle_end=(
                        train_events.events[-1].cycle_number
                    ),
                    test_cycle_start=(
                        test_events.events[0].cycle_number
                    ),
                    test_cycle_end=(
                        test_events.events[-1].cycle_number
                    ),
                    train_events=len(train_events),
                    test_events=len(test_events),
                    selected_parameters=selected_parameters,
                    train_metrics=train_metrics,
                    test_metrics=test_result.metrics,
                )
            )

            fold_number += 1
            start += self.configuration.step_cycles

        return tuple(folds)



def _build_subset_collection(
    events: Iterable[BacktestEvent],
) -> BacktestEventCollection:
    """
    Build a valid standalone event collection from a chronological subset.

    BacktestEventCollection intentionally requires event_number values to begin
    at 1 and remain sequential. Walk-forward folds contain events copied from
    the full dataset, so their original event_number values may begin at 601,
    801, and so on. Renumbering the immutable copies preserves source_event_id,
    timestamps, cycles, and all market data while satisfying the collection's
    local sequencing invariant.
    """

    ordered = sorted(
        events,
        key=lambda event: (
            event.timestamp,
            event.source_event_id,
        ),
    )

    renumbered = [
        replace(
            event,
            event_number=position,
        )
        for position, event in enumerate(
            ordered,
            start=1,
        )
    ]

    return BacktestEventCollection(renumbered)


class MonteCarloStressTester:
    def __init__(
        self,
        configuration: MonteCarloConfiguration | None = None,
    ) -> None:
        self.configuration = configuration or MonteCarloConfiguration()
        self.configuration.validate()

    def run(
        self,
        trade_profits: Sequence[float],
        *,
        initial_capital_usd: float,
    ) -> tuple[MonteCarloPathResult, ...]:
        if initial_capital_usd <= 0:
            raise InvalidValidationConfigurationError(
                "initial_capital_usd must be positive."
            )

        if not trade_profits:
            return ()

        randomizer = random.Random(
            self.configuration.random_seed
        )
        paths: list[MonteCarloPathResult] = []

        for simulation in range(1, self.configuration.simulations + 1):
            sampled = [
                trade_profits[
                    randomizer.randrange(len(trade_profits))
                ]
                for _ in range(len(trade_profits))
            ]
            randomizer.shuffle(sampled)

            capital = initial_capital_usd
            peak = initial_capital_usd
            maximum_drawdown = 0.0
            wins = 0
            losses = 0
            missed = 0
            failures = 0

            for base_profit in sampled:
                if (
                    randomizer.random()
                    < self.configuration.missed_trade_probability
                ):
                    missed += 1
                    continue

                if (
                    randomizer.random()
                    < self.configuration.failed_execution_probability
                ):
                    failures += 1
                    realized = -self.configuration.failed_execution_cost_usd
                else:
                    fee_multiplier = randomizer.uniform(
                        self.configuration.fee_multiplier_min,
                        self.configuration.fee_multiplier_max,
                    )
                    slippage_multiplier = randomizer.uniform(
                        self.configuration.slippage_multiplier_min,
                        self.configuration.slippage_multiplier_max,
                    )

                    if base_profit >= 0:
                        realized = base_profit / max(
                            fee_multiplier * slippage_multiplier,
                            1e-12,
                        )
                    else:
                        realized = (
                            base_profit
                            * fee_multiplier
                            * slippage_multiplier
                        )

                capital += realized
                peak = max(peak, capital)

                drawdown = (
                    (peak - capital) / peak * 100.0
                    if peak > 0
                    else 100.0
                )
                maximum_drawdown = max(
                    maximum_drawdown,
                    drawdown,
                )

                if realized > 0:
                    wins += 1
                elif realized < 0:
                    losses += 1

            net_profit = capital - initial_capital_usd

            paths.append(
                MonteCarloPathResult(
                    simulation=simulation,
                    ending_capital_usd=capital,
                    net_profit_usd=net_profit,
                    total_return_pct=(
                        net_profit / initial_capital_usd * 100.0
                    ),
                    maximum_drawdown_pct=maximum_drawdown,
                    winning_trades=wins,
                    losing_trades=losses,
                    missed_trades=missed,
                    failed_executions=failures,
                )
            )

        return tuple(paths)


def summarize_validation(
    folds: Sequence[WalkForwardFoldResult],
    paths: Sequence[MonteCarloPathResult],
    *,
    initial_capital_usd: float,
) -> ValidationSummary:
    test_profits = [
        fold.test_metrics.net_profit_usd
        for fold in folds
    ]
    test_returns = [
        fold.test_metrics.total_return_pct
        for fold in folds
    ]
    test_drawdowns = [
        fold.test_metrics.maximum_drawdown_pct
        for fold in folds
    ]

    ending_capitals = sorted(
        path.ending_capital_usd
        for path in paths
    )
    drawdowns = sorted(
        path.maximum_drawdown_pct
        for path in paths
    )

    return ValidationSummary(
        generated_at=datetime.now(),
        folds=len(folds),
        profitable_test_folds=sum(
            profit > 0 for profit in test_profits
        ),
        unprofitable_test_folds=sum(
            profit < 0 for profit in test_profits
        ),
        total_test_trades=sum(
            fold.test_metrics.trades for fold in folds
        ),
        aggregate_test_profit_usd=sum(test_profits),
        average_test_return_pct=(
            statistics.fmean(test_returns)
            if test_returns
            else 0.0
        ),
        worst_test_drawdown_pct=(
            max(test_drawdowns)
            if test_drawdowns
            else 0.0
        ),
        median_monte_carlo_ending_capital_usd=(
            statistics.median(ending_capitals)
            if ending_capitals
            else initial_capital_usd
        ),
        p05_monte_carlo_ending_capital_usd=(
            _percentile(ending_capitals, 0.05)
            if ending_capitals
            else initial_capital_usd
        ),
        p95_monte_carlo_ending_capital_usd=(
            _percentile(ending_capitals, 0.95)
            if ending_capitals
            else initial_capital_usd
        ),
        probability_of_finishing_below_start=(
            sum(
                ending < initial_capital_usd
                for ending in ending_capitals
            )
            / len(ending_capitals)
            if ending_capitals
            else 0.0
        ),
        median_monte_carlo_drawdown_pct=(
            statistics.median(drawdowns)
            if drawdowns
            else 0.0
        ),
        p95_monte_carlo_drawdown_pct=(
            _percentile(drawdowns, 0.95)
            if drawdowns
            else 0.0
        ),
    )


def _percentile(
    values: Sequence[float],
    percentile: float,
) -> float:
    if not values:
        raise ValueError("values cannot be empty.")

    if not 0.0 <= percentile <= 1.0:
        raise ValueError("percentile must be in [0, 1].")

    if len(values) == 1:
        return float(values[0])

    index = (len(values) - 1) * percentile
    lower = math.floor(index)
    upper = math.ceil(index)

    if lower == upper:
        return float(values[lower])

    weight = index - lower

    return (
        float(values[lower]) * (1.0 - weight)
        + float(values[upper]) * weight
    )


def _write_csv(
    path: Path,
    records: Sequence[Mapping[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not records:
        path.write_text("", encoding="utf-8")
        return

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(records[0].keys()),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(records)


def run_validation(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    output_directory: str | Path = DEFAULT_OUTPUT_DIRECTORY,
    simulations: int = 5_000,
) -> tuple[
    tuple[WalkForwardFoldResult, ...],
    tuple[MonteCarloPathResult, ...],
    ValidationSummary,
]:
    events = build_backtest_events(
        database_path,
        strict=True,
    )

    engine_configuration = EngineConfiguration(
        risk=RiskLimits(),
    )

    folds = WalkForwardValidator(
        WalkForwardConfiguration(),
        engine_configuration=engine_configuration,
    ).run(events)

    baseline_result = InstitutionalBacktestEngine(
        ConservativeCompositeStrategy(),
        engine_configuration,
    ).run(events)

    trade_profits = [
        trade.realized_profit_usd
        for trade in baseline_result.trades
    ]

    paths = MonteCarloStressTester(
        MonteCarloConfiguration(
            simulations=simulations,
        )
    ).run(
        trade_profits,
        initial_capital_usd=(
            engine_configuration.risk.initial_capital_usd
        ),
    )

    summary = summarize_validation(
        folds,
        paths,
        initial_capital_usd=(
            engine_configuration.risk.initial_capital_usd
        ),
    )

    output = Path(output_directory)
    output.mkdir(parents=True, exist_ok=True)

    _write_csv(
        output / DEFAULT_FOLDS_CSV.name,
        [fold.to_record() for fold in folds],
    )
    _write_csv(
        output / DEFAULT_MONTE_CARLO_CSV.name,
        [path.to_record() for path in paths],
    )

    (output / DEFAULT_REPORT_JSON.name).write_text(
        json.dumps(
            {
                "summary": summary.to_dict(),
                "walk_forward_configuration": asdict(
                    WalkForwardConfiguration()
                ),
                "monte_carlo_configuration": asdict(
                    MonteCarloConfiguration(
                        simulations=simulations,
                    )
                ),
                "folds": [
                    fold.to_record()
                    for fold in folds
                ],
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    return folds, paths, summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run walk-forward validation and Monte Carlo stress testing."
        )
    )
    parser.add_argument(
        "--database",
        default=str(DEFAULT_DATABASE_PATH),
    )
    parser.add_argument(
        "--output-directory",
        default=str(DEFAULT_OUTPUT_DIRECTORY),
    )
    parser.add_argument(
        "--simulations",
        type=int,
        default=5_000,
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)

    logging.basicConfig(
        level=(
            logging.DEBUG
            if args.verbose
            else logging.INFO
        ),
        format=(
            "%(asctime)s | %(levelname)s | "
            "%(name)s | %(message)s"
        ),
    )

    try:
        folds, paths, summary = run_validation(
            args.database,
            output_directory=args.output_directory,
            simulations=args.simulations,
        )

    except (
        ValidationEngineError,
        BacktestEngineError,
        EventBuilderError,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print("\nWalk-Forward and Monte Carlo Validation")
    print("=" * 76)
    print(f"Walk-forward folds: {summary.folds}")
    print(
        "Profitable / unprofitable test folds: "
        f"{summary.profitable_test_folds} / "
        f"{summary.unprofitable_test_folds}"
    )
    print(
        "Aggregate out-of-sample profit: "
        f"${summary.aggregate_test_profit_usd:.6f}"
    )
    print(
        "Average out-of-sample return: "
        f"{summary.average_test_return_pct:.6f}%"
    )
    print(
        "Worst out-of-sample drawdown: "
        f"{summary.worst_test_drawdown_pct:.6f}%"
    )
    print()
    print(f"Monte Carlo paths: {len(paths)}")
    print(
        "Median ending capital: "
        f"${summary.median_monte_carlo_ending_capital_usd:.6f}"
    )
    print(
        "5th / 95th percentile ending capital: "
        f"${summary.p05_monte_carlo_ending_capital_usd:.6f} / "
        f"${summary.p95_monte_carlo_ending_capital_usd:.6f}"
    )
    print(
        "Probability of finishing below start: "
        f"{summary.probability_of_finishing_below_start * 100.0:.2f}%"
    )
    print(
        "Median / 95th percentile drawdown: "
        f"{summary.median_monte_carlo_drawdown_pct:.6f}% / "
        f"{summary.p95_monte_carlo_drawdown_pct:.6f}%"
    )
    print()
    print("Output files")
    print(
        Path(args.output_directory)
        / DEFAULT_FOLDS_CSV.name
    )
    print(
        Path(args.output_directory)
        / DEFAULT_MONTE_CARLO_CSV.name
    )
    print(
        Path(args.output_directory)
        / DEFAULT_REPORT_JSON.name
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())