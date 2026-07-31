"""
Phase 10C — Step 3: Strategy Diagnostics and Training-Only Feature Analysis

Purpose
-------
1. Explain exactly why historical trades were allowed.
2. Compare profitable and losing observations using pre-outcome features.
3. Run threshold diagnostics on training cycles only.
4. Export audit files for later strategy design.

This module is research-only. It does not modify SQLite, the scanner, the risk
manager, or live execution code.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

try:
    from backtesting.backtest_engine import (
        BacktestEngineError,
        BacktestResult,
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
        BacktestResult,
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

DEFAULT_OUTPUT_DIRECTORY = Path("backtesting") / "diagnostics"
DEFAULT_EXECUTED_TRADES_CSV = (
    DEFAULT_OUTPUT_DIRECTORY / "executed_trade_diagnostics.csv"
)
DEFAULT_FEATURE_COMPARISON_CSV = (
    DEFAULT_OUTPUT_DIRECTORY / "feature_comparison.csv"
)
DEFAULT_THRESHOLD_DIAGNOSTICS_CSV = (
    DEFAULT_OUTPUT_DIRECTORY / "threshold_diagnostics.csv"
)
DEFAULT_CYCLE_DIAGNOSTICS_CSV = (
    DEFAULT_OUTPUT_DIRECTORY / "cycle_diagnostics.csv"
)
DEFAULT_REPORT_JSON = DEFAULT_OUTPUT_DIRECTORY / "strategy_diagnostics.json"


class StrategyDiagnosticsError(RuntimeError):
    """Base exception for strategy diagnostics failures."""


class InvalidDiagnosticsConfigurationError(StrategyDiagnosticsError):
    """Raised when diagnostics configuration is invalid."""


@dataclass(frozen=True, slots=True)
class DiagnosticsConfiguration:
    training_fraction: float = 0.70
    minimum_training_events: int = 300
    minimum_positive_examples: int = 1
    minimum_negative_examples: int = 25
    threshold_steps: int = 20
    require_successful_quotes_for_feature_analysis: bool = True

    def validate(self) -> None:
        if not 0.50 <= self.training_fraction < 1.0:
            raise InvalidDiagnosticsConfigurationError(
                "training_fraction must be in [0.50, 1.0)."
            )

        for name in (
            "minimum_training_events",
            "minimum_positive_examples",
            "minimum_negative_examples",
            "threshold_steps",
        ):
            value = int(getattr(self, name))

            if value <= 0:
                raise InvalidDiagnosticsConfigurationError(
                    f"{name} must be positive."
                )


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    name: str
    direction: str
    getter: Callable[[BacktestEvent], float]


@dataclass(frozen=True, slots=True)
class ExecutedTradeDiagnostic:
    trade_number: int
    source_event_id: int
    timestamp: datetime
    cycle_number: int
    cycle_id: str
    token: str
    asset_key: str

    realized_profit_usd: float
    winning_trade: bool

    strategy_reason: str
    strategy_confidence: float

    source_decision: str
    source_eligible: bool
    quote_successful: bool

    market_score: float
    liquidity_score: float
    volume_score: float
    pair_score: float
    intelligence_score: float
    composite_market_score: float
    score_dispersion: float
    minimum_component_score: float
    maximum_component_score: float

    passed_composite_gate: bool
    passed_market_gate: bool
    passed_liquidity_gate: bool
    passed_intelligence_gate: bool
    passed_dispersion_gate: bool
    passed_route_gate: bool

    failure_explanation: str

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat(sep=" ")
        return record


@dataclass(frozen=True, slots=True)
class FeatureComparison:
    feature: str
    positive_count: int
    negative_count: int

    positive_mean: float
    negative_mean: float
    mean_difference: float

    positive_median: float
    negative_median: float
    median_difference: float

    pooled_standard_deviation: float
    standardized_mean_difference: float

    positive_minimum: float
    positive_maximum: float
    negative_minimum: float
    negative_maximum: float

    separation_direction: str

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class ThresholdDiagnostic:
    feature: str
    direction: str
    threshold: float

    selected_events: int
    selected_positive: int
    selected_negative: int

    win_rate_pct: float
    average_profit_usd: float
    total_profit_usd: float
    median_profit_usd: float
    best_profit_usd: float
    worst_profit_usd: float

    precision: float
    recall: float
    false_positive_rate: float

    training_cycle_start: int
    training_cycle_end: int

    def to_record(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class CycleDiagnostic:
    cycle_number: int
    cycle_id: str
    timestamp: datetime
    events: int
    successful_quotes: int
    profitable_events: int
    losing_events: int
    execution_candidates: int
    executed_trades: int
    realized_profit_usd: float
    average_composite_score: float
    best_event_profit_usd: float
    worst_event_profit_usd: float

    def to_record(self) -> dict[str, Any]:
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat(sep=" ")
        return record


@dataclass(frozen=True, slots=True)
class DiagnosticsSummary:
    generated_at: datetime
    total_events: int
    total_cycles: int

    training_events: int
    training_cycles: int
    holdout_events: int
    holdout_cycles: int

    profitable_events: int
    losing_events: int
    quote_errors: int

    executed_trades: int
    executed_wins: int
    executed_losses: int
    executed_net_profit_usd: float

    strongest_feature: str | None
    strongest_standardized_difference: float
    best_training_threshold_feature: str | None
    best_training_threshold: float | None
    best_training_threshold_profit_usd: float

    statistically_weak: bool
    weakness_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        record = asdict(self)
        record["generated_at"] = self.generated_at.isoformat(sep=" ")
        return record


FEATURES: tuple[FeatureDefinition, ...] = (
    FeatureDefinition(
        name="market_score",
        direction="HIGHER",
        getter=lambda event: event.market_score,
    ),
    FeatureDefinition(
        name="liquidity_score",
        direction="HIGHER",
        getter=lambda event: event.liquidity_score,
    ),
    FeatureDefinition(
        name="volume_score",
        direction="HIGHER",
        getter=lambda event: event.volume_score,
    ),
    FeatureDefinition(
        name="pair_score",
        direction="HIGHER",
        getter=lambda event: event.pair_score,
    ),
    FeatureDefinition(
        name="intelligence_score",
        direction="HIGHER",
        getter=lambda event: event.intelligence_score,
    ),
    FeatureDefinition(
        name="composite_market_score",
        direction="HIGHER",
        getter=lambda event: event.composite_market_score,
    ),
    FeatureDefinition(
        name="minimum_component_score",
        direction="HIGHER",
        getter=lambda event: event.minimum_component_score,
    ),
    FeatureDefinition(
        name="maximum_component_score",
        direction="HIGHER",
        getter=lambda event: event.maximum_component_score,
    ),
    FeatureDefinition(
        name="score_dispersion",
        direction="LOWER",
        getter=lambda event: event.score_dispersion,
    ),
    FeatureDefinition(
        name="cost_bps",
        direction="LOWER",
        getter=lambda event: event.cost_bps,
    ),
    FeatureDefinition(
        name="estimated_cost_usd",
        direction="LOWER",
        getter=lambda event: event.estimated_cost_usd,
    ),
)


class StrategyDiagnosticsEngine:
    def __init__(
        self,
        configuration: DiagnosticsConfiguration | None = None,
    ) -> None:
        self.configuration = (
            configuration
            or DiagnosticsConfiguration()
        )
        self.configuration.validate()

    def run(
        self,
        events: BacktestEventCollection,
        backtest_result: BacktestResult,
        *,
        strategy: ConservativeCompositeStrategy,
    ) -> tuple[
        tuple[ExecutedTradeDiagnostic, ...],
        tuple[FeatureComparison, ...],
        tuple[ThresholdDiagnostic, ...],
        tuple[CycleDiagnostic, ...],
        DiagnosticsSummary,
    ]:
        if events.is_empty:
            raise StrategyDiagnosticsError(
                "Cannot diagnose an empty event collection."
            )

        training_events, holdout_events = self._split_training_holdout(
            events
        )

        executed = self._diagnose_executed_trades(
            events,
            backtest_result,
            strategy,
        )

        comparisons = self._compare_features(
            training_events
        )

        thresholds = self._diagnose_thresholds(
            training_events
        )

        cycles = self._diagnose_cycles(
            events,
            backtest_result,
        )

        summary = self._summarize(
            events=events,
            training_events=training_events,
            holdout_events=holdout_events,
            executed=executed,
            comparisons=comparisons,
            thresholds=thresholds,
        )

        return (
            executed,
            comparisons,
            thresholds,
            cycles,
            summary,
        )

    def _split_training_holdout(
        self,
        events: BacktestEventCollection,
    ) -> tuple[
        BacktestEventCollection,
        BacktestEventCollection,
    ]:
        grouped = events.group_by_cycle()
        cycle_ids = list(grouped.keys())

        training_cycle_count = max(
            1,
            int(
                math.floor(
                    len(cycle_ids)
                    * self.configuration.training_fraction
                )
            ),
        )

        training_cycle_count = min(
            training_cycle_count,
            len(cycle_ids) - 1,
        )

        training_ids = cycle_ids[:training_cycle_count]
        holdout_ids = cycle_ids[training_cycle_count:]

        training = self._renumber_collection(
            event
            for cycle_id in training_ids
            for event in grouped[cycle_id]
        )

        holdout = self._renumber_collection(
            event
            for cycle_id in holdout_ids
            for event in grouped[cycle_id]
        )

        if len(training) < self.configuration.minimum_training_events:
            raise StrategyDiagnosticsError(
                "Training partition is too small: "
                f"{len(training)} events."
            )

        return training, holdout

    @staticmethod
    def _renumber_collection(
        events: Iterable[BacktestEvent],
    ) -> BacktestEventCollection:
        from dataclasses import replace

        ordered = sorted(
            events,
            key=lambda event: (
                event.timestamp,
                event.source_event_id,
            ),
        )

        return BacktestEventCollection(
            [
                replace(
                    event,
                    event_number=position,
                )
                for position, event in enumerate(
                    ordered,
                    start=1,
                )
            ]
        )

    def _diagnose_executed_trades(
        self,
        events: BacktestEventCollection,
        result: BacktestResult,
        strategy: ConservativeCompositeStrategy,
    ) -> tuple[ExecutedTradeDiagnostic, ...]:
        event_by_id = {
            event.source_event_id: event
            for event in events
        }

        diagnostics: list[ExecutedTradeDiagnostic] = []

        for trade in result.trades:
            event = event_by_id.get(
                trade.source_event_id
            )

            if event is None:
                raise StrategyDiagnosticsError(
                    "Executed trade has no matching source event: "
                    f"{trade.source_event_id}"
                )

            gates = {
                "composite": (
                    event.composite_market_score
                    >= strategy.minimum_composite_score
                ),
                "market": (
                    event.market_score
                    >= strategy.minimum_market_score
                ),
                "liquidity": (
                    event.liquidity_score
                    >= strategy.minimum_liquidity_score
                ),
                "intelligence": (
                    event.intelligence_score
                    >= strategy.minimum_intelligence_score
                ),
                "dispersion": (
                    event.score_dispersion
                    <= strategy.maximum_score_dispersion
                ),
                "route": event.has_route,
            }

            failure_explanation = self._trade_failure_explanation(
                event=event,
                winning=trade.winning_trade,
                gates=gates,
            )

            diagnostics.append(
                ExecutedTradeDiagnostic(
                    trade_number=trade.trade_number,
                    source_event_id=event.source_event_id,
                    timestamp=event.timestamp,
                    cycle_number=event.cycle_number,
                    cycle_id=event.cycle_id,
                    token=event.token,
                    asset_key=event.asset_key,
                    realized_profit_usd=(
                        trade.realized_profit_usd
                    ),
                    winning_trade=trade.winning_trade,
                    strategy_reason=trade.reason,
                    strategy_confidence=trade.confidence,
                    source_decision=event.decision,
                    source_eligible=event.eligible,
                    quote_successful=(
                        event.quote_successful
                    ),
                    market_score=event.market_score,
                    liquidity_score=event.liquidity_score,
                    volume_score=event.volume_score,
                    pair_score=event.pair_score,
                    intelligence_score=(
                        event.intelligence_score
                    ),
                    composite_market_score=(
                        event.composite_market_score
                    ),
                    score_dispersion=(
                        event.score_dispersion
                    ),
                    minimum_component_score=(
                        event.minimum_component_score
                    ),
                    maximum_component_score=(
                        event.maximum_component_score
                    ),
                    passed_composite_gate=(
                        gates["composite"]
                    ),
                    passed_market_gate=gates["market"],
                    passed_liquidity_gate=(
                        gates["liquidity"]
                    ),
                    passed_intelligence_gate=(
                        gates["intelligence"]
                    ),
                    passed_dispersion_gate=(
                        gates["dispersion"]
                    ),
                    passed_route_gate=gates["route"],
                    failure_explanation=(
                        failure_explanation
                    ),
                )
            )

        return tuple(diagnostics)

    @staticmethod
    def _trade_failure_explanation(
        *,
        event: BacktestEvent,
        winning: bool,
        gates: Mapping[str, bool],
    ) -> str:
        if winning:
            return (
                "Trade was profitable after passing all "
                "pre-outcome gates."
            )

        passed = [
            name
            for name, value in gates.items()
            if value
        ]

        return (
            "Losing trade passed all configured gates. "
            "The current score thresholds did not separate "
            "this negative outcome from favorable-looking "
            "pre-trade conditions. Passed gates: "
            + ", ".join(passed)
            + "."
        )

    def _analysis_events(
        self,
        events: BacktestEventCollection,
    ) -> tuple[BacktestEvent, ...]:
        if (
            self.configuration
            .require_successful_quotes_for_feature_analysis
        ):
            return tuple(
                event
                for event in events
                if event.quote_successful
            )

        return events.events

    def _compare_features(
        self,
        training_events: BacktestEventCollection,
    ) -> tuple[FeatureComparison, ...]:
        analyzed = self._analysis_events(
            training_events
        )

        positives = tuple(
            event
            for event in analyzed
            if event.net_profit_usd > 0
        )
        negatives = tuple(
            event
            for event in analyzed
            if event.net_profit_usd < 0
        )

        comparisons: list[FeatureComparison] = []

        for feature in FEATURES:
            positive_values = [
                float(feature.getter(event))
                for event in positives
            ]
            negative_values = [
                float(feature.getter(event))
                for event in negatives
            ]

            if not positive_values or not negative_values:
                continue

            positive_mean = statistics.fmean(
                positive_values
            )
            negative_mean = statistics.fmean(
                negative_values
            )

            pooled_std = self._pooled_standard_deviation(
                positive_values,
                negative_values,
            )

            standardized = (
                (positive_mean - negative_mean)
                / pooled_std
                if pooled_std > 0
                else 0.0
            )

            comparisons.append(
                FeatureComparison(
                    feature=feature.name,
                    positive_count=len(
                        positive_values
                    ),
                    negative_count=len(
                        negative_values
                    ),
                    positive_mean=positive_mean,
                    negative_mean=negative_mean,
                    mean_difference=(
                        positive_mean
                        - negative_mean
                    ),
                    positive_median=statistics.median(
                        positive_values
                    ),
                    negative_median=statistics.median(
                        negative_values
                    ),
                    median_difference=(
                        statistics.median(
                            positive_values
                        )
                        - statistics.median(
                            negative_values
                        )
                    ),
                    pooled_standard_deviation=(
                        pooled_std
                    ),
                    standardized_mean_difference=(
                        standardized
                    ),
                    positive_minimum=min(
                        positive_values
                    ),
                    positive_maximum=max(
                        positive_values
                    ),
                    negative_minimum=min(
                        negative_values
                    ),
                    negative_maximum=max(
                        negative_values
                    ),
                    separation_direction=(
                        "POSITIVE_HIGHER"
                        if positive_mean
                        > negative_mean
                        else "POSITIVE_LOWER"
                    ),
                )
            )

        comparisons.sort(
            key=lambda row: abs(
                row.standardized_mean_difference
            ),
            reverse=True,
        )

        return tuple(comparisons)

    @staticmethod
    def _pooled_standard_deviation(
        first: Sequence[float],
        second: Sequence[float],
    ) -> float:
        if len(first) < 2 or len(second) < 2:
            return 0.0

        first_variance = statistics.variance(first)
        second_variance = statistics.variance(
            second
        )

        denominator = len(first) + len(second) - 2

        if denominator <= 0:
            return 0.0

        pooled_variance = (
            (len(first) - 1) * first_variance
            + (len(second) - 1) * second_variance
        ) / denominator

        return math.sqrt(
            max(0.0, pooled_variance)
        )

    def _diagnose_thresholds(
        self,
        training_events: BacktestEventCollection,
    ) -> tuple[ThresholdDiagnostic, ...]:
        analyzed = self._analysis_events(
            training_events
        )

        if not analyzed:
            return ()

        total_positive = sum(
            event.net_profit_usd > 0
            for event in analyzed
        )
        total_negative = sum(
            event.net_profit_usd < 0
            for event in analyzed
        )

        first_cycle = min(
            event.cycle_number
            for event in analyzed
        )
        last_cycle = max(
            event.cycle_number
            for event in analyzed
        )

        diagnostics: list[ThresholdDiagnostic] = []

        for feature in FEATURES:
            values = sorted(
                {
                    float(feature.getter(event))
                    for event in analyzed
                }
            )

            thresholds = self._candidate_thresholds(
                values
            )

            for threshold in thresholds:
                if feature.direction == "HIGHER":
                    selected = [
                        event
                        for event in analyzed
                        if feature.getter(event)
                        >= threshold
                    ]
                else:
                    selected = [
                        event
                        for event in analyzed
                        if feature.getter(event)
                        <= threshold
                    ]

                if not selected:
                    continue

                profits = [
                    event.net_profit_usd
                    for event in selected
                ]

                selected_positive = sum(
                    value > 0
                    for value in profits
                )
                selected_negative = sum(
                    value < 0
                    for value in profits
                )

                precision = (
                    selected_positive
                    / len(selected)
                )

                recall = (
                    selected_positive
                    / total_positive
                    if total_positive
                    else 0.0
                )

                false_positive_rate = (
                    selected_negative
                    / total_negative
                    if total_negative
                    else 0.0
                )

                diagnostics.append(
                    ThresholdDiagnostic(
                        feature=feature.name,
                        direction=feature.direction,
                        threshold=float(threshold),
                        selected_events=len(
                            selected
                        ),
                        selected_positive=(
                            selected_positive
                        ),
                        selected_negative=(
                            selected_negative
                        ),
                        win_rate_pct=(
                            selected_positive
                            / len(selected)
                            * 100.0
                        ),
                        average_profit_usd=(
                            statistics.fmean(
                                profits
                            )
                        ),
                        total_profit_usd=sum(
                            profits
                        ),
                        median_profit_usd=(
                            statistics.median(
                                profits
                            )
                        ),
                        best_profit_usd=max(
                            profits
                        ),
                        worst_profit_usd=min(
                            profits
                        ),
                        precision=precision,
                        recall=recall,
                        false_positive_rate=(
                            false_positive_rate
                        ),
                        training_cycle_start=(
                            first_cycle
                        ),
                        training_cycle_end=(
                            last_cycle
                        ),
                    )
                )

        diagnostics.sort(
            key=lambda row: (
                row.total_profit_usd,
                row.average_profit_usd,
                row.precision,
                -row.false_positive_rate,
                row.selected_events,
            ),
            reverse=True,
        )

        return tuple(diagnostics)

    def _candidate_thresholds(
        self,
        values: Sequence[float],
    ) -> tuple[float, ...]:
        if not values:
            return ()

        if len(values) <= self.configuration.threshold_steps:
            return tuple(values)

        thresholds: list[float] = []

        for step in range(
            self.configuration.threshold_steps
        ):
            percentile = (
                step
                / (
                    self.configuration
                    .threshold_steps
                    - 1
                )
            )

            thresholds.append(
                self._percentile(
                    values,
                    percentile,
                )
            )

        return tuple(
            sorted(set(thresholds))
        )

    @staticmethod
    def _percentile(
        values: Sequence[float],
        percentile: float,
    ) -> float:
        if not values:
            raise ValueError(
                "values cannot be empty."
            )

        if len(values) == 1:
            return float(values[0])

        position = (
            len(values) - 1
        ) * percentile
        lower = math.floor(position)
        upper = math.ceil(position)

        if lower == upper:
            return float(values[lower])

        weight = position - lower

        return (
            float(values[lower])
            * (1.0 - weight)
            + float(values[upper])
            * weight
        )

    def _diagnose_cycles(
        self,
        events: BacktestEventCollection,
        result: BacktestResult,
    ) -> tuple[CycleDiagnostic, ...]:
        grouped = events.group_by_cycle()
        trades_by_cycle: dict[
            str,
            list[Any],
        ] = defaultdict(list)

        for trade in result.trades:
            trades_by_cycle[
                trade.cycle_id
            ].append(trade)

        diagnostics: list[
            CycleDiagnostic
        ] = []

        for cycle_id, cycle_events in grouped.items():
            successful = [
                event
                for event in cycle_events
                if event.quote_successful
            ]
            profits = [
                event.net_profit_usd
                for event in successful
            ]
            cycle_trades = trades_by_cycle.get(
                cycle_id,
                [],
            )

            diagnostics.append(
                CycleDiagnostic(
                    cycle_number=(
                        cycle_events[0]
                        .cycle_number
                    ),
                    cycle_id=cycle_id,
                    timestamp=(
                        cycle_events[0]
                        .timestamp
                    ),
                    events=len(cycle_events),
                    successful_quotes=len(
                        successful
                    ),
                    profitable_events=sum(
                        value > 0
                        for value in profits
                    ),
                    losing_events=sum(
                        value < 0
                        for value in profits
                    ),
                    execution_candidates=sum(
                        event.execution_candidate
                        for event in cycle_events
                    ),
                    executed_trades=len(
                        cycle_trades
                    ),
                    realized_profit_usd=sum(
                        trade.realized_profit_usd
                        for trade in cycle_trades
                    ),
                    average_composite_score=(
                        statistics.fmean(
                            event.composite_market_score
                            for event in cycle_events
                        )
                    ),
                    best_event_profit_usd=(
                        max(profits)
                        if profits
                        else 0.0
                    ),
                    worst_event_profit_usd=(
                        min(profits)
                        if profits
                        else 0.0
                    ),
                )
            )

        return tuple(diagnostics)

    def _summarize(
        self,
        *,
        events: BacktestEventCollection,
        training_events: BacktestEventCollection,
        holdout_events: BacktestEventCollection,
        executed: Sequence[
            ExecutedTradeDiagnostic
        ],
        comparisons: Sequence[
            FeatureComparison
        ],
        thresholds: Sequence[
            ThresholdDiagnostic
        ],
    ) -> DiagnosticsSummary:
        profitable_events = sum(
            event.quote_successful
            and event.net_profit_usd > 0
            for event in events
        )

        losing_events = sum(
            event.quote_successful
            and event.net_profit_usd < 0
            for event in events
        )

        quote_errors = sum(
            not event.quote_successful
            for event in events
        )

        strongest = (
            comparisons[0]
            if comparisons
            else None
        )

        best_threshold = (
            thresholds[0]
            if thresholds
            else None
        )

        weakness_reasons: list[str] = []

        training_positive = sum(
            event.quote_successful
            and event.net_profit_usd > 0
            for event in training_events
        )

        training_negative = sum(
            event.quote_successful
            and event.net_profit_usd < 0
            for event in training_events
        )

        if (
            training_positive
            < self.configuration
            .minimum_positive_examples
        ):
            weakness_reasons.append(
                "Too few positive training examples."
            )

        if (
            training_negative
            < self.configuration
            .minimum_negative_examples
        ):
            weakness_reasons.append(
                "Too few negative training examples."
            )

        if len(events.cycle_ids) < 30:
            weakness_reasons.append(
                "Fewer than 30 scanner cycles."
            )

        if profitable_events < 30:
            weakness_reasons.append(
                "Fewer than 30 profitable observations."
            )

        if len(executed) < 30:
            weakness_reasons.append(
                "Fewer than 30 executed backtest trades."
            )

        if holdout_events.is_empty:
            weakness_reasons.append(
                "No chronological holdout partition."
            )

        return DiagnosticsSummary(
            generated_at=datetime.now(),
            total_events=len(events),
            total_cycles=len(
                events.cycle_ids
            ),
            training_events=len(
                training_events
            ),
            training_cycles=len(
                training_events.cycle_ids
            ),
            holdout_events=len(
                holdout_events
            ),
            holdout_cycles=len(
                holdout_events.cycle_ids
            ),
            profitable_events=(
                profitable_events
            ),
            losing_events=losing_events,
            quote_errors=quote_errors,
            executed_trades=len(executed),
            executed_wins=sum(
                row.winning_trade
                for row in executed
            ),
            executed_losses=sum(
                not row.winning_trade
                for row in executed
            ),
            executed_net_profit_usd=sum(
                row.realized_profit_usd
                for row in executed
            ),
            strongest_feature=(
                strongest.feature
                if strongest
                else None
            ),
            strongest_standardized_difference=(
                strongest
                .standardized_mean_difference
                if strongest
                else 0.0
            ),
            best_training_threshold_feature=(
                best_threshold.feature
                if best_threshold
                else None
            ),
            best_training_threshold=(
                best_threshold.threshold
                if best_threshold
                else None
            ),
            best_training_threshold_profit_usd=(
                best_threshold
                .total_profit_usd
                if best_threshold
                else 0.0
            ),
            statistically_weak=bool(
                weakness_reasons
            ),
            weakness_reasons=tuple(
                weakness_reasons
            ),
        )


def run_strategy_diagnostics(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    output_directory: str | Path = DEFAULT_OUTPUT_DIRECTORY,
    training_fraction: float = 0.70,
) -> DiagnosticsSummary:
    events = build_backtest_events(
        database_path,
        strict=True,
    )

    strategy = ConservativeCompositeStrategy()

    engine_configuration = EngineConfiguration(
        risk=RiskLimits(),
    )

    result = InstitutionalBacktestEngine(
        strategy,
        engine_configuration,
    ).run(events)

    diagnostics_engine = StrategyDiagnosticsEngine(
        DiagnosticsConfiguration(
            training_fraction=training_fraction,
        )
    )

    (
        executed,
        comparisons,
        thresholds,
        cycles,
        summary,
    ) = diagnostics_engine.run(
        events,
        result,
        strategy=strategy,
    )

    output = Path(output_directory)
    output.mkdir(
        parents=True,
        exist_ok=True,
    )

    _write_csv(
        output
        / DEFAULT_EXECUTED_TRADES_CSV.name,
        [
            row.to_record()
            for row in executed
        ],
    )

    _write_csv(
        output
        / DEFAULT_FEATURE_COMPARISON_CSV.name,
        [
            row.to_record()
            for row in comparisons
        ],
    )

    _write_csv(
        output
        / DEFAULT_THRESHOLD_DIAGNOSTICS_CSV.name,
        [
            row.to_record()
            for row in thresholds
        ],
    )

    _write_csv(
        output
        / DEFAULT_CYCLE_DIAGNOSTICS_CSV.name,
        [
            row.to_record()
            for row in cycles
        ],
    )

    report = {
        "summary": summary.to_dict(),
        "configuration": asdict(
            diagnostics_engine.configuration
        ),
        "baseline_strategy": {
            "name": strategy.name,
            "minimum_composite_score": (
                strategy.minimum_composite_score
            ),
            "minimum_market_score": (
                strategy.minimum_market_score
            ),
            "minimum_liquidity_score": (
                strategy.minimum_liquidity_score
            ),
            "minimum_intelligence_score": (
                strategy.minimum_intelligence_score
            ),
            "maximum_score_dispersion": (
                strategy.maximum_score_dispersion
            ),
        },
        "baseline_metrics": asdict(
            result.metrics
        ),
        "executed_trades": [
            row.to_record()
            for row in executed
        ],
        "top_feature_comparisons": [
            row.to_record()
            for row in comparisons[:20]
        ],
        "top_training_thresholds": [
            row.to_record()
            for row in thresholds[:50]
        ],
    }

    (
        output
        / DEFAULT_REPORT_JSON.name
    ).write_text(
        json.dumps(
            report,
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return summary


def _write_csv(
    path: Path,
    records: Sequence[
        Mapping[str, Any]
    ],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    if not records:
        path.write_text(
            "",
            encoding="utf-8",
        )
        return

    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(
                records[0].keys()
            ),
            extrasaction="raise",
        )
        writer.writeheader()
        writer.writerows(records)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Diagnose strategy gates and "
            "training-only feature separation."
        )
    )

    parser.add_argument(
        "--database",
        default=str(
            DEFAULT_DATABASE_PATH
        ),
    )

    parser.add_argument(
        "--output-directory",
        default=str(
            DEFAULT_OUTPUT_DIRECTORY
        ),
    )

    parser.add_argument(
        "--training-fraction",
        type=float,
        default=0.70,
    )

    parser.add_argument(
        "--verbose",
        action="store_true",
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:
    args = _parser().parse_args(
        argv
    )

    logging.basicConfig(
        level=(
            logging.DEBUG
            if args.verbose
            else logging.INFO
        ),
        format=(
            "%(asctime)s | "
            "%(levelname)s | "
            "%(name)s | "
            "%(message)s"
        ),
    )

    try:
        summary = run_strategy_diagnostics(
            args.database,
            output_directory=(
                args.output_directory
            ),
            training_fraction=(
                args.training_fraction
            ),
        )

    except (
        StrategyDiagnosticsError,
        BacktestEngineError,
        EventBuilderError,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error(
            "%s",
            error,
        )
        return 1

    print(
        "\nPhase 10C Step 3 — "
        "Strategy Diagnostics"
    )
    print("=" * 76)

    print(
        f"Events: {summary.total_events}"
    )
    print(
        f"Cycles: {summary.total_cycles}"
    )

    print(
        "Training / holdout events: "
        f"{summary.training_events} / "
        f"{summary.holdout_events}"
    )

    print(
        "Training / holdout cycles: "
        f"{summary.training_cycles} / "
        f"{summary.holdout_cycles}"
    )

    print()

    print("Historical outcomes")
    print("-" * 76)

    print(
        "Profitable / losing / quote errors: "
        f"{summary.profitable_events} / "
        f"{summary.losing_events} / "
        f"{summary.quote_errors}"
    )

    print()

    print("Executed strategy trades")
    print("-" * 76)

    print(
        "Trades / wins / losses: "
        f"{summary.executed_trades} / "
        f"{summary.executed_wins} / "
        f"{summary.executed_losses}"
    )

    print(
        "Executed net profit: "
        f"${summary.executed_net_profit_usd:.6f}"
    )

    print()

    print("Training-only feature diagnostics")
    print("-" * 76)

    print(
        "Strongest feature: "
        f"{summary.strongest_feature}"
    )

    print(
        "Standardized difference: "
        f"{summary.strongest_standardized_difference:.6f}"
    )

    print(
        "Best threshold feature: "
        f"{summary.best_training_threshold_feature}"
    )

    print(
        "Best threshold: "
        f"{summary.best_training_threshold}"
    )

    print(
        "Best threshold training profit: "
        f"${summary.best_training_threshold_profit_usd:.6f}"
    )

    print()

    print(
        "Statistically weak: "
        f"{summary.statistically_weak}"
    )

    if summary.weakness_reasons:
        print("Weakness reasons:")

        for reason in summary.weakness_reasons:
            print(f"  - {reason}")

    print()

    print("Output files")
    print("-" * 76)

    output = Path(
        args.output_directory
    )

    for name in (
        DEFAULT_EXECUTED_TRADES_CSV.name,
        DEFAULT_FEATURE_COMPARISON_CSV.name,
        DEFAULT_THRESHOLD_DIAGNOSTICS_CSV.name,
        DEFAULT_CYCLE_DIAGNOSTICS_CSV.name,
        DEFAULT_REPORT_JSON.name,
    ):
        print(output / name)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())