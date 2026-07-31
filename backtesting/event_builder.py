"""
Phase 10B — Backtest Event Builder

Transforms normalized HistoricalDatasetRow records into immutable BacktestEvent
objects suitable for validation, export, and institutional backtesting.

This module is read-only. It does not modify SQLite or the live scanner.
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
from typing import Any, Iterable, Iterator, Sequence

try:
    from backtesting.historical_dataset import (
        DEFAULT_DATABASE_PATH,
        HistoricalDataset,
        HistoricalDatasetError,
        HistoricalDatasetRow,
        build_historical_dataset,
    )
except ModuleNotFoundError:
    from historical_dataset import (  # type: ignore
        DEFAULT_DATABASE_PATH,
        HistoricalDataset,
        HistoricalDatasetError,
        HistoricalDatasetRow,
        build_historical_dataset,
    )


LOGGER = logging.getLogger(__name__)

DEFAULT_OUTPUT_DIRECTORY = Path("backtesting") / "output"
DEFAULT_EVENTS_CSV_PATH = DEFAULT_OUTPUT_DIRECTORY / "backtest_events.csv"
DEFAULT_EVENTS_JSON_PATH = DEFAULT_OUTPUT_DIRECTORY / "backtest_events.json"


class EventBuilderError(RuntimeError):
    """Base exception for backtest event construction failures."""


class EmptyEventCollectionError(EventBuilderError):
    """Raised when exporting an empty event collection."""


class InvalidEventConfigurationError(EventBuilderError):
    """Raised when builder configuration is invalid."""


@dataclass(frozen=True, slots=True)
class BacktestEvent:
    """
    Immutable backtest-ready event.

    Every field is derived only from information available at scan time.
    No future observations are joined here.
    """

    event_number: int
    source_event_id: int

    timestamp: datetime
    cycle_number: int
    cycle_id: str
    cycle_position: int

    token: str
    token_key: str
    mint: str | None
    asset_key: str

    buy_route: str | None
    sell_route: str | None
    route_pair: str

    starting_amount_usd: float
    ending_amount_usd: float
    quoted_profit_usd: float
    estimated_cost_usd: float
    net_profit_usd: float

    gross_return_bps: float
    cost_bps: float
    net_return_bps: float
    gross_to_cost_ratio: float | None

    decision_raw: str
    decision: str
    eligible: bool
    quote_successful: bool
    profitable: bool
    error: str | None

    market_score: float
    liquidity_score: float
    volume_score: float
    pair_score: float
    intelligence_score: float

    composite_market_score: float
    score_dispersion: float
    minimum_component_score: float
    maximum_component_score: float

    execution_candidate: bool
    informational_only: bool
    outcome_label: str
    decision_rank: int

    has_mint: bool
    has_route: bool
    has_error: bool

    def to_dict(self) -> dict[str, Any]:
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat(sep=" ")
        return record


@dataclass(frozen=True, slots=True)
class EventCycleSummary:
    cycle_number: int
    cycle_id: str
    timestamp: datetime
    events: int
    unique_assets: int
    successful_quotes: int
    quote_errors: int
    profitable_events: int
    execution_candidates: int
    average_net_profit_usd: float
    average_net_return_bps: float
    best_net_profit_usd: float
    worst_net_profit_usd: float
    average_composite_market_score: float

    def to_dict(self) -> dict[str, Any]:
        record = asdict(self)
        record["timestamp"] = self.timestamp.isoformat(sep=" ")
        return record


@dataclass(frozen=True, slots=True)
class EventCollectionSummary:
    total_events: int
    total_cycles: int
    unique_assets: int
    unique_tokens: int

    successful_quotes: int
    quote_errors: int
    profitable_events: int
    eligible_events: int
    execution_candidates: int
    informational_events: int

    execute_decisions: int
    watch_decisions: int
    skip_decisions: int
    quote_error_decisions: int
    unknown_decisions: int

    positive_outcomes: int
    negative_outcomes: int
    flat_outcomes: int
    quote_error_outcomes: int

    first_timestamp: datetime | None
    last_timestamp: datetime | None

    average_net_profit_usd: float
    median_net_profit_usd: float
    best_net_profit_usd: float
    worst_net_profit_usd: float
    average_net_return_bps: float
    average_composite_market_score: float

    def to_dict(self) -> dict[str, Any]:
        record = asdict(self)

        for field_name in ("first_timestamp", "last_timestamp"):
            value = record[field_name]
            record[field_name] = value.isoformat(sep=" ") if value else None

        return record


class BacktestEventCollection:
    """Immutable chronological collection of BacktestEvent objects."""

    def __init__(self, events: Sequence[BacktestEvent]) -> None:
        self._events = tuple(events)
        self._validate()

    def __len__(self) -> int:
        return len(self._events)

    def __iter__(self) -> Iterator[BacktestEvent]:
        return iter(self._events)

    def __getitem__(
        self,
        index: int | slice,
    ) -> BacktestEvent | tuple[BacktestEvent, ...]:
        return self._events[index]

    @property
    def events(self) -> tuple[BacktestEvent, ...]:
        return self._events

    @property
    def is_empty(self) -> bool:
        return not self._events

    @property
    def cycle_ids(self) -> tuple[str, ...]:
        ordered: list[str] = []
        seen: set[str] = set()

        for event in self._events:
            if event.cycle_id not in seen:
                seen.add(event.cycle_id)
                ordered.append(event.cycle_id)

        return tuple(ordered)

    def group_by_cycle(self) -> dict[str, tuple[BacktestEvent, ...]]:
        grouped: dict[str, list[BacktestEvent]] = defaultdict(list)

        for event in self._events:
            grouped[event.cycle_id].append(event)

        return {
            cycle_id: tuple(events)
            for cycle_id, events in grouped.items()
        }

    def filter(
        self,
        *,
        token: str | None = None,
        decision: str | None = None,
        outcome_label: str | None = None,
        quote_successful: bool | None = None,
        eligible: bool | None = None,
        execution_candidate: bool | None = None,
        profitable: bool | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> "BacktestEventCollection":
        normalized_token = token.strip().upper() if token else None
        normalized_decision = decision.strip().upper() if decision else None
        normalized_outcome = outcome_label.strip().upper() if outcome_label else None

        filtered: list[BacktestEvent] = []

        for event in self._events:
            if normalized_token and event.token_key != normalized_token:
                continue
            if normalized_decision and event.decision != normalized_decision:
                continue
            if normalized_outcome and event.outcome_label != normalized_outcome:
                continue
            if quote_successful is not None and event.quote_successful != quote_successful:
                continue
            if eligible is not None and event.eligible != eligible:
                continue
            if (
                execution_candidate is not None
                and event.execution_candidate != execution_candidate
            ):
                continue
            if profitable is not None and event.profitable != profitable:
                continue
            if start_time is not None and event.timestamp < start_time:
                continue
            if end_time is not None and event.timestamp > end_time:
                continue

            filtered.append(event)

        return BacktestEventCollection(filtered)

    def summarize(self) -> EventCollectionSummary:
        if not self._events:
            return EventCollectionSummary(
                total_events=0,
                total_cycles=0,
                unique_assets=0,
                unique_tokens=0,
                successful_quotes=0,
                quote_errors=0,
                profitable_events=0,
                eligible_events=0,
                execution_candidates=0,
                informational_events=0,
                execute_decisions=0,
                watch_decisions=0,
                skip_decisions=0,
                quote_error_decisions=0,
                unknown_decisions=0,
                positive_outcomes=0,
                negative_outcomes=0,
                flat_outcomes=0,
                quote_error_outcomes=0,
                first_timestamp=None,
                last_timestamp=None,
                average_net_profit_usd=0.0,
                median_net_profit_usd=0.0,
                best_net_profit_usd=0.0,
                worst_net_profit_usd=0.0,
                average_net_return_bps=0.0,
                average_composite_market_score=0.0,
            )

        net_profits = [event.net_profit_usd for event in self._events]
        net_returns = [event.net_return_bps for event in self._events]
        composite_scores = [
            event.composite_market_score for event in self._events
        ]

        decision_counts = Counter(event.decision for event in self._events)
        outcome_counts = Counter(event.outcome_label for event in self._events)

        return EventCollectionSummary(
            total_events=len(self._events),
            total_cycles=len(self.cycle_ids),
            unique_assets=len({event.asset_key for event in self._events}),
            unique_tokens=len({event.token_key for event in self._events}),
            successful_quotes=sum(event.quote_successful for event in self._events),
            quote_errors=sum(not event.quote_successful for event in self._events),
            profitable_events=sum(event.profitable for event in self._events),
            eligible_events=sum(event.eligible for event in self._events),
            execution_candidates=sum(
                event.execution_candidate for event in self._events
            ),
            informational_events=sum(
                event.informational_only for event in self._events
            ),
            execute_decisions=decision_counts.get("EXECUTE", 0),
            watch_decisions=decision_counts.get("WATCH", 0),
            skip_decisions=decision_counts.get("SKIP", 0),
            quote_error_decisions=decision_counts.get("QUOTE_ERROR", 0),
            unknown_decisions=decision_counts.get("UNKNOWN", 0),
            positive_outcomes=outcome_counts.get("POSITIVE", 0),
            negative_outcomes=outcome_counts.get("NEGATIVE", 0),
            flat_outcomes=outcome_counts.get("FLAT", 0),
            quote_error_outcomes=outcome_counts.get("QUOTE_ERROR", 0),
            first_timestamp=self._events[0].timestamp,
            last_timestamp=self._events[-1].timestamp,
            average_net_profit_usd=statistics.fmean(net_profits),
            median_net_profit_usd=statistics.median(net_profits),
            best_net_profit_usd=max(net_profits),
            worst_net_profit_usd=min(net_profits),
            average_net_return_bps=statistics.fmean(net_returns),
            average_composite_market_score=statistics.fmean(
                composite_scores
            ),
        )

    def summarize_cycles(self) -> tuple[EventCycleSummary, ...]:
        summaries: list[EventCycleSummary] = []

        for cycle_id, events in self.group_by_cycle().items():
            net_profits = [event.net_profit_usd for event in events]
            net_returns = [event.net_return_bps for event in events]
            composite_scores = [
                event.composite_market_score for event in events
            ]

            summaries.append(
                EventCycleSummary(
                    cycle_number=events[0].cycle_number,
                    cycle_id=cycle_id,
                    timestamp=events[0].timestamp,
                    events=len(events),
                    unique_assets=len({event.asset_key for event in events}),
                    successful_quotes=sum(
                        event.quote_successful for event in events
                    ),
                    quote_errors=sum(
                        not event.quote_successful for event in events
                    ),
                    profitable_events=sum(
                        event.profitable for event in events
                    ),
                    execution_candidates=sum(
                        event.execution_candidate for event in events
                    ),
                    average_net_profit_usd=statistics.fmean(net_profits),
                    average_net_return_bps=statistics.fmean(net_returns),
                    best_net_profit_usd=max(net_profits),
                    worst_net_profit_usd=min(net_profits),
                    average_composite_market_score=statistics.fmean(
                        composite_scores
                    ),
                )
            )

        return tuple(summaries)

    def to_records(self) -> list[dict[str, Any]]:
        return [event.to_dict() for event in self._events]

    def export_csv(
        self,
        path: str | Path = DEFAULT_EVENTS_CSV_PATH,
    ) -> Path:
        if not self._events:
            raise EmptyEventCollectionError(
                "Cannot export an empty backtest event collection."
            )

        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        records = self.to_records()

        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=list(records[0].keys()),
                extrasaction="raise",
            )
            writer.writeheader()
            writer.writerows(records)

        return output_path

    def export_json(
        self,
        path: str | Path = DEFAULT_EVENTS_JSON_PATH,
    ) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "summary": self.summarize().to_dict(),
            "cycles": [
                cycle_summary.to_dict()
                for cycle_summary in self.summarize_cycles()
            ],
            "events": self.to_records(),
        }

        output_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return output_path

    def _validate(self) -> None:
        previous: BacktestEvent | None = None
        seen_source_ids: set[int] = set()
        expected_number = 1

        for event in self._events:
            if event.event_number != expected_number:
                raise EventBuilderError(
                    "Event numbers are not sequential: "
                    f"expected {expected_number}, got {event.event_number}."
                )

            if event.source_event_id in seen_source_ids:
                raise EventBuilderError(
                    f"Duplicate source event ID: {event.source_event_id}"
                )

            if previous is not None:
                if (event.timestamp, event.source_event_id) < (
                    previous.timestamp,
                    previous.source_event_id,
                ):
                    raise EventBuilderError(
                        "Events are not chronological: "
                        f"{previous.source_event_id} -> "
                        f"{event.source_event_id}"
                    )

                if event.cycle_number < previous.cycle_number:
                    raise EventBuilderError(
                        "Cycle number moved backwards: "
                        f"{previous.cycle_number} -> "
                        f"{event.cycle_number}"
                    )

            seen_source_ids.add(event.source_event_id)
            previous = event
            expected_number += 1


class EventBuilder:
    """
    Converts HistoricalDatasetRow objects into BacktestEvent objects.

    Score weights must be non-negative and sum to more than zero.
    """

    def __init__(
        self,
        *,
        market_weight: float = 0.30,
        liquidity_weight: float = 0.25,
        volume_weight: float = 0.20,
        pair_weight: float = 0.15,
        intelligence_weight: float = 0.10,
        flat_tolerance_usd: float = 1e-12,
    ) -> None:
        weights = {
            "market": market_weight,
            "liquidity": liquidity_weight,
            "volume": volume_weight,
            "pair": pair_weight,
            "intelligence": intelligence_weight,
        }

        for name, value in weights.items():
            if not math.isfinite(float(value)) or float(value) < 0:
                raise InvalidEventConfigurationError(
                    f"{name}_weight must be finite and non-negative."
                )

        total_weight = sum(float(value) for value in weights.values())

        if total_weight <= 0:
            raise InvalidEventConfigurationError(
                "At least one score weight must be greater than zero."
            )

        if not math.isfinite(float(flat_tolerance_usd)):
            raise InvalidEventConfigurationError(
                "flat_tolerance_usd must be finite."
            )

        if flat_tolerance_usd < 0:
            raise InvalidEventConfigurationError(
                "flat_tolerance_usd cannot be negative."
            )

        self.market_weight = float(market_weight) / total_weight
        self.liquidity_weight = float(liquidity_weight) / total_weight
        self.volume_weight = float(volume_weight) / total_weight
        self.pair_weight = float(pair_weight) / total_weight
        self.intelligence_weight = float(intelligence_weight) / total_weight
        self.flat_tolerance_usd = float(flat_tolerance_usd)

    def from_dataset(
        self,
        dataset: HistoricalDataset,
    ) -> BacktestEventCollection:
        return self.from_rows(dataset.rows)

    def from_rows(
        self,
        rows: Iterable[HistoricalDatasetRow],
    ) -> BacktestEventCollection:
        ordered_rows = sorted(
            rows,
            key=lambda row: (row.scanned_at, row.event_id),
        )

        events = [
            self._build_event(row, event_number=index)
            for index, row in enumerate(ordered_rows, start=1)
        ]

        return BacktestEventCollection(events)

    def _build_event(
        self,
        row: HistoricalDatasetRow,
        *,
        event_number: int,
    ) -> BacktestEvent:
        scores = (
            self._finite_score(row.market_score, "market_score", row.event_id),
            self._finite_score(
                row.liquidity_score,
                "liquidity_score",
                row.event_id,
            ),
            self._finite_score(row.volume_score, "volume_score", row.event_id),
            self._finite_score(row.pair_score, "pair_score", row.event_id),
            self._finite_score(
                row.intelligence_score,
                "intelligence_score",
                row.event_id,
            ),
        )

        composite_score = (
            scores[0] * self.market_weight
            + scores[1] * self.liquidity_weight
            + scores[2] * self.volume_weight
            + scores[3] * self.pair_weight
            + scores[4] * self.intelligence_weight
        )

        score_dispersion = (
            statistics.pstdev(scores) if len(scores) > 1 else 0.0
        )

        execution_candidate = (
            row.quote_successful
            and row.eligible
            and row.decision == "EXECUTE"
        )

        informational_only = not execution_candidate

        outcome_label = self._outcome_label(row)
        decision_rank = self._decision_rank(row.decision)

        return BacktestEvent(
            event_number=event_number,
            source_event_id=row.event_id,
            timestamp=row.scanned_at,
            cycle_number=row.cycle_number,
            cycle_id=row.cycle_id,
            cycle_position=row.cycle_position,
            token=row.token,
            token_key=row.token_key,
            mint=row.mint,
            asset_key=row.asset_key,
            buy_route=row.buy_route,
            sell_route=row.sell_route,
            route_pair=row.route_pair,
            starting_amount_usd=row.starting_amount_usd,
            ending_amount_usd=row.ending_amount_usd,
            quoted_profit_usd=row.quoted_profit_usd,
            estimated_cost_usd=row.estimated_cost_usd,
            net_profit_usd=row.net_profit_usd,
            gross_return_bps=row.gross_return_bps,
            cost_bps=row.cost_bps,
            net_return_bps=row.net_return_bps,
            gross_to_cost_ratio=row.gross_to_cost_ratio,
            decision_raw=row.decision_raw,
            decision=row.decision,
            eligible=row.eligible,
            quote_successful=row.quote_successful,
            profitable=row.profitable,
            error=row.error,
            market_score=scores[0],
            liquidity_score=scores[1],
            volume_score=scores[2],
            pair_score=scores[3],
            intelligence_score=scores[4],
            composite_market_score=composite_score,
            score_dispersion=score_dispersion,
            minimum_component_score=min(scores),
            maximum_component_score=max(scores),
            execution_candidate=execution_candidate,
            informational_only=informational_only,
            outcome_label=outcome_label,
            decision_rank=decision_rank,
            has_mint=row.has_mint,
            has_route=row.has_route,
            has_error=row.has_error,
        )

    def _outcome_label(self, row: HistoricalDatasetRow) -> str:
        if not row.quote_successful:
            return "QUOTE_ERROR"

        if row.net_profit_usd > self.flat_tolerance_usd:
            return "POSITIVE"

        if row.net_profit_usd < -self.flat_tolerance_usd:
            return "NEGATIVE"

        return "FLAT"

    @staticmethod
    def _decision_rank(decision: str) -> int:
        ranks = {
            "QUOTE_ERROR": 0,
            "UNKNOWN": 0,
            "SKIP": 1,
            "WATCH": 2,
            "EXECUTE": 3,
        }
        return ranks.get(decision, 0)

    @staticmethod
    def _finite_score(
        value: Any,
        field_name: str,
        source_event_id: int,
    ) -> float:
        try:
            score = float(value)
        except (TypeError, ValueError) as error:
            raise EventBuilderError(
                f"Event {source_event_id} has invalid {field_name}: {value!r}"
            ) from error

        if not math.isfinite(score):
            raise EventBuilderError(
                f"Event {source_event_id} has non-finite "
                f"{field_name}: {score!r}"
            )

        if score < 0.0 or score > 100.0:
            LOGGER.warning(
                "Event %s has %s outside 0-100: %.6f",
                source_event_id,
                field_name,
                score,
            )

        return score


def build_backtest_events(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    strict: bool = True,
) -> BacktestEventCollection:
    dataset = build_historical_dataset(
        database_path,
        strict=strict,
    )
    builder = EventBuilder()
    return builder.from_dataset(dataset)


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build immutable backtest events from scanner history."
    )
    parser.add_argument(
        "--database",
        default=str(DEFAULT_DATABASE_PATH),
        help="Path to trades.db",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of sample events to display",
    )
    parser.add_argument("--token", help="Optional token filter")
    parser.add_argument(
        "--decision",
        choices=("EXECUTE", "WATCH", "SKIP", "QUOTE_ERROR", "UNKNOWN"),
        help="Optional normalized decision filter",
    )
    parser.add_argument(
        "--outcome",
        choices=("POSITIVE", "NEGATIVE", "FLAT", "QUOTE_ERROR"),
        help="Optional outcome filter",
    )
    parser.add_argument(
        "--successful-only",
        action="store_true",
        help="Keep only successful quote events",
    )
    parser.add_argument(
        "--eligible-only",
        action="store_true",
        help="Keep only eligible events",
    )
    parser.add_argument(
        "--execution-candidates-only",
        action="store_true",
        help="Keep only execution candidates",
    )
    parser.add_argument(
        "--profitable-only",
        action="store_true",
        help="Keep only profitable events",
    )
    parser.add_argument(
        "--export-csv",
        nargs="?",
        const=str(DEFAULT_EVENTS_CSV_PATH),
        help="Export events to CSV",
    )
    parser.add_argument(
        "--export-json",
        nargs="?",
        const=str(DEFAULT_EVENTS_JSON_PATH),
        help="Export events to JSON",
    )
    parser.add_argument(
        "--non-strict",
        action="store_true",
        help="Skip malformed historical rows",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_argument_parser()
    args = parser.parse_args(argv)
    _configure_logging(args.verbose)

    try:
        collection = build_backtest_events(
            args.database,
            strict=not args.non_strict,
        )
    except (
        EventBuilderError,
        HistoricalDatasetError,
        ValueError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    collection = collection.filter(
        token=args.token,
        decision=args.decision,
        outcome_label=args.outcome,
        quote_successful=True if args.successful_only else None,
        eligible=True if args.eligible_only else None,
        execution_candidate=(
            True if args.execution_candidates_only else None
        ),
        profitable=True if args.profitable_only else None,
    )

    summary = collection.summarize()

    print("\nBacktest Event Builder")
    print("=" * 72)
    print(f"Database: {args.database}")
    print(f"Events: {summary.total_events}")
    print(f"Cycles: {summary.total_cycles}")
    print(f"Unique assets: {summary.unique_assets}")
    print(f"Unique tokens: {summary.unique_tokens}")
    print()

    print("Event Summary")
    print("-" * 72)
    print(f"Successful quotes: {summary.successful_quotes}")
    print(f"Quote errors: {summary.quote_errors}")
    print(f"Profitable events: {summary.profitable_events}")
    print(f"Eligible events: {summary.eligible_events}")
    print(f"Execution candidates: {summary.execution_candidates}")
    print(f"Informational events: {summary.informational_events}")
    print(
        "EXECUTE / WATCH / SKIP / QUOTE_ERROR / UNKNOWN: "
        f"{summary.execute_decisions} / "
        f"{summary.watch_decisions} / "
        f"{summary.skip_decisions} / "
        f"{summary.quote_error_decisions} / "
        f"{summary.unknown_decisions}"
    )
    print(
        "POSITIVE / NEGATIVE / FLAT / QUOTE_ERROR outcomes: "
        f"{summary.positive_outcomes} / "
        f"{summary.negative_outcomes} / "
        f"{summary.flat_outcomes} / "
        f"{summary.quote_error_outcomes}"
    )
    print(f"Average net profit: ${summary.average_net_profit_usd:.6f}")
    print(f"Median net profit: ${summary.median_net_profit_usd:.6f}")
    print(f"Best net profit: ${summary.best_net_profit_usd:.6f}")
    print(f"Worst net profit: ${summary.worst_net_profit_usd:.6f}")
    print(f"Average net return: {summary.average_net_return_bps:.4f} bps")
    print(
        "Average composite market score: "
        f"{summary.average_composite_market_score:.4f}"
    )
    print(f"First event: {summary.first_timestamp}")
    print(f"Last event: {summary.last_timestamp}")
    print()

    print("Sample events")
    print("-" * 72)

    for event in collection.events[: max(args.limit, 0)]:
        print(
            f"{event.event_number}: {event.cycle_id} | "
            f"{event.token} | "
            f"net ${event.net_profit_usd:.6f} | "
            f"{event.net_return_bps:.4f} bps | "
            f"{event.decision} | "
            f"{event.outcome_label} | "
            f"candidate={event.execution_candidate}"
        )

    if args.export_csv:
        csv_path = collection.export_csv(args.export_csv)
        print(f"\nCSV exported: {csv_path}")

    if args.export_json:
        json_path = collection.export_json(args.export_json)
        print(f"JSON exported: {json_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())