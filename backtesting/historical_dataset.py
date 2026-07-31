"""
Phase 10B — Historical Research Dataset

Transforms ScannerHistoryEvent records into a normalized, immutable research
dataset suitable for later event building, validation, export, and backtesting.

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
    from backtesting.scanner_history_adapter import (
        DEFAULT_DATABASE_PATH,
        ScannerHistoryAdapter,
        ScannerHistoryAdapterError,
        ScannerHistoryEvent,
    )
except ModuleNotFoundError:
    from scanner_history_adapter import (  # type: ignore
        DEFAULT_DATABASE_PATH,
        ScannerHistoryAdapter,
        ScannerHistoryAdapterError,
        ScannerHistoryEvent,
    )


LOGGER = logging.getLogger(__name__)

DEFAULT_CYCLE_GAP_SECONDS = 30.0
DEFAULT_DATASET_DIRECTORY = Path("backtesting") / "output"
DEFAULT_CSV_PATH = DEFAULT_DATASET_DIRECTORY / "historical_scanner_dataset.csv"
DEFAULT_JSON_PATH = DEFAULT_DATASET_DIRECTORY / "historical_scanner_dataset.json"


class HistoricalDatasetError(RuntimeError):
    """Base exception for historical research dataset failures."""


class EmptyHistoricalDatasetError(HistoricalDatasetError):
    """Raised when a dataset operation requires at least one row."""


class InvalidDatasetConfigurationError(HistoricalDatasetError):
    """Raised when dataset configuration values are invalid."""


@dataclass(frozen=True, slots=True)
class HistoricalDatasetRow:
    """
    Normalized research row derived from one scanner observation.

    No predictions, decisions, or risk values from the future are joined here.
    This file intentionally preserves only fields available at scan time.
    """

    row_number: int
    event_id: int
    cycle_number: int
    cycle_id: str
    cycle_position: int
    scanned_at: datetime

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

    has_mint: bool
    has_route: bool
    has_error: bool

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["scanned_at"] = self.scanned_at.isoformat(sep=" ")
        return result


@dataclass(frozen=True, slots=True)
class HistoricalCycleSummary:
    cycle_number: int
    cycle_id: str
    scanned_at: datetime
    observations: int
    unique_assets: int
    successful_quotes: int
    quote_errors: int
    eligible_events: int
    profitable_events: int
    average_net_profit_usd: float
    best_net_profit_usd: float
    worst_net_profit_usd: float
    average_market_score: float

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["scanned_at"] = self.scanned_at.isoformat(sep=" ")
        return result


@dataclass(frozen=True, slots=True)
class HistoricalDatasetSummary:
    total_rows: int
    total_cycles: int
    unique_assets: int
    unique_tokens: int
    rows_with_mint: int
    rows_without_mint: int
    successful_quotes: int
    quote_errors: int
    eligible_events: int
    profitable_events: int
    execute_decisions: int
    watch_decisions: int
    skip_decisions: int
    quote_error_decisions: int
    unknown_decisions: int
    first_scanned_at: datetime | None
    last_scanned_at: datetime | None
    average_net_profit_usd: float
    median_net_profit_usd: float
    best_net_profit_usd: float
    worst_net_profit_usd: float
    average_net_return_bps: float
    average_cost_bps: float

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)

        for field_name in ("first_scanned_at", "last_scanned_at"):
            value = result[field_name]
            result[field_name] = value.isoformat(sep=" ") if value else None

        return result


class HistoricalDataset:
    """
    Immutable chronological research dataset.

    Rows are kept in strict `(scanned_at, event_id)` order.
    """

    def __init__(
        self,
        rows: Sequence[HistoricalDatasetRow],
        *,
        cycle_gap_seconds: float = DEFAULT_CYCLE_GAP_SECONDS,
    ) -> None:
        if cycle_gap_seconds < 0:
            raise InvalidDatasetConfigurationError(
                "cycle_gap_seconds cannot be negative."
            )

        self._rows = tuple(rows)
        self.cycle_gap_seconds = float(cycle_gap_seconds)
        self._validate_internal_order()

    def __len__(self) -> int:
        return len(self._rows)

    def __iter__(self) -> Iterator[HistoricalDatasetRow]:
        return iter(self._rows)

    def __getitem__(
        self,
        index: int | slice,
    ) -> HistoricalDatasetRow | tuple[HistoricalDatasetRow, ...]:
        return self._rows[index]

    @property
    def rows(self) -> tuple[HistoricalDatasetRow, ...]:
        return self._rows

    @property
    def is_empty(self) -> bool:
        return not self._rows

    @property
    def cycle_ids(self) -> tuple[str, ...]:
        ordered: list[str] = []
        seen: set[str] = set()

        for row in self._rows:
            if row.cycle_id not in seen:
                seen.add(row.cycle_id)
                ordered.append(row.cycle_id)

        return tuple(ordered)

    def filter(
        self,
        *,
        token: str | None = None,
        mint: str | None = None,
        decision: str | None = None,
        quote_successful: bool | None = None,
        eligible: bool | None = None,
        profitable: bool | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
    ) -> "HistoricalDataset":
        normalized_token = token.strip().upper() if token else None
        normalized_mint = mint.strip() if mint else None
        normalized_decision = decision.strip().upper() if decision else None

        filtered_rows: list[HistoricalDatasetRow] = []

        for row in self._rows:
            if normalized_token and row.token_key != normalized_token:
                continue
            if normalized_mint and row.mint != normalized_mint:
                continue
            if normalized_decision and row.decision != normalized_decision:
                continue
            if quote_successful is not None and row.quote_successful != quote_successful:
                continue
            if eligible is not None and row.eligible != eligible:
                continue
            if profitable is not None and row.profitable != profitable:
                continue
            if start_time is not None and row.scanned_at < start_time:
                continue
            if end_time is not None and row.scanned_at > end_time:
                continue

            filtered_rows.append(row)

        return HistoricalDataset(
            filtered_rows,
            cycle_gap_seconds=self.cycle_gap_seconds,
        )

    def cycle_rows(self, cycle_id: str) -> tuple[HistoricalDatasetRow, ...]:
        normalized_cycle_id = cycle_id.strip()
        return tuple(
            row for row in self._rows if row.cycle_id == normalized_cycle_id
        )

    def group_by_cycle(
        self,
    ) -> dict[str, tuple[HistoricalDatasetRow, ...]]:
        grouped: dict[str, list[HistoricalDatasetRow]] = defaultdict(list)

        for row in self._rows:
            grouped[row.cycle_id].append(row)

        return {
            cycle_id: tuple(rows)
            for cycle_id, rows in grouped.items()
        }

    def summarize(self) -> HistoricalDatasetSummary:
        if not self._rows:
            return HistoricalDatasetSummary(
                total_rows=0,
                total_cycles=0,
                unique_assets=0,
                unique_tokens=0,
                rows_with_mint=0,
                rows_without_mint=0,
                successful_quotes=0,
                quote_errors=0,
                eligible_events=0,
                profitable_events=0,
                execute_decisions=0,
                watch_decisions=0,
                skip_decisions=0,
                quote_error_decisions=0,
                unknown_decisions=0,
                first_scanned_at=None,
                last_scanned_at=None,
                average_net_profit_usd=0.0,
                median_net_profit_usd=0.0,
                best_net_profit_usd=0.0,
                worst_net_profit_usd=0.0,
                average_net_return_bps=0.0,
                average_cost_bps=0.0,
            )

        net_profits = [row.net_profit_usd for row in self._rows]
        net_returns = [row.net_return_bps for row in self._rows]
        cost_returns = [row.cost_bps for row in self._rows]
        decision_counts = Counter(row.decision for row in self._rows)

        recognized_decisions = {
            "EXECUTE",
            "WATCH",
            "SKIP",
            "QUOTE_ERROR",
        }
        unknown_decisions = sum(
            count
            for decision, count in decision_counts.items()
            if decision not in recognized_decisions
        )

        return HistoricalDatasetSummary(
            total_rows=len(self._rows),
            total_cycles=len(self.cycle_ids),
            unique_assets=len({row.asset_key for row in self._rows}),
            unique_tokens=len({row.token_key for row in self._rows}),
            rows_with_mint=sum(row.has_mint for row in self._rows),
            rows_without_mint=sum(not row.has_mint for row in self._rows),
            successful_quotes=sum(row.quote_successful for row in self._rows),
            quote_errors=sum(not row.quote_successful for row in self._rows),
            eligible_events=sum(row.eligible for row in self._rows),
            profitable_events=sum(row.profitable for row in self._rows),
            execute_decisions=decision_counts.get("EXECUTE", 0),
            watch_decisions=decision_counts.get("WATCH", 0),
            skip_decisions=decision_counts.get("SKIP", 0),
            quote_error_decisions=decision_counts.get("QUOTE_ERROR", 0),
            unknown_decisions=unknown_decisions,
            first_scanned_at=self._rows[0].scanned_at,
            last_scanned_at=self._rows[-1].scanned_at,
            average_net_profit_usd=statistics.fmean(net_profits),
            median_net_profit_usd=statistics.median(net_profits),
            best_net_profit_usd=max(net_profits),
            worst_net_profit_usd=min(net_profits),
            average_net_return_bps=statistics.fmean(net_returns),
            average_cost_bps=statistics.fmean(cost_returns),
        )

    def summarize_cycles(self) -> tuple[HistoricalCycleSummary, ...]:
        cycle_summaries: list[HistoricalCycleSummary] = []

        for cycle_id, rows in self.group_by_cycle().items():
            net_profits = [row.net_profit_usd for row in rows]
            market_scores = [row.market_score for row in rows]

            cycle_summaries.append(
                HistoricalCycleSummary(
                    cycle_number=rows[0].cycle_number,
                    cycle_id=cycle_id,
                    scanned_at=rows[0].scanned_at,
                    observations=len(rows),
                    unique_assets=len({row.asset_key for row in rows}),
                    successful_quotes=sum(row.quote_successful for row in rows),
                    quote_errors=sum(not row.quote_successful for row in rows),
                    eligible_events=sum(row.eligible for row in rows),
                    profitable_events=sum(row.profitable for row in rows),
                    average_net_profit_usd=statistics.fmean(net_profits),
                    best_net_profit_usd=max(net_profits),
                    worst_net_profit_usd=min(net_profits),
                    average_market_score=statistics.fmean(market_scores),
                )
            )

        return tuple(cycle_summaries)

    def to_records(self) -> list[dict[str, Any]]:
        return [row.to_dict() for row in self._rows]

    def export_csv(self, path: str | Path = DEFAULT_CSV_PATH) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        records = self.to_records()

        if not records:
            raise EmptyHistoricalDatasetError(
                "Cannot export an empty historical dataset."
            )

        with output_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=list(records[0].keys()),
                extrasaction="raise",
            )
            writer.writeheader()
            writer.writerows(records)

        return output_path

    def export_json(self, path: str | Path = DEFAULT_JSON_PATH) -> Path:
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "summary": self.summarize().to_dict(),
            "cycles": [
                cycle_summary.to_dict()
                for cycle_summary in self.summarize_cycles()
            ],
            "rows": self.to_records(),
        }

        output_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        return output_path

    def _validate_internal_order(self) -> None:
        previous: HistoricalDatasetRow | None = None
        seen_event_ids: set[int] = set()
        expected_row_number = 1

        for row in self._rows:
            if row.row_number != expected_row_number:
                raise HistoricalDatasetError(
                    "Dataset row numbers are not sequential: "
                    f"expected {expected_row_number}, got {row.row_number}."
                )

            if row.event_id in seen_event_ids:
                raise HistoricalDatasetError(
                    f"Duplicate event_id in dataset: {row.event_id}"
                )

            if previous is not None:
                if (row.scanned_at, row.event_id) < (
                    previous.scanned_at,
                    previous.event_id,
                ):
                    raise HistoricalDatasetError(
                        "Dataset rows are not chronological: "
                        f"{previous.event_id} -> {row.event_id}"
                    )

                if row.cycle_number < previous.cycle_number:
                    raise HistoricalDatasetError(
                        "Cycle numbers moved backwards: "
                        f"{previous.cycle_number} -> {row.cycle_number}"
                    )

            seen_event_ids.add(row.event_id)
            previous = row
            expected_row_number += 1


class HistoricalDatasetBuilder:
    """
    Converts ScannerHistoryEvent objects into normalized dataset rows.

    Cycle rules:
    - records with exactly the same scanner timestamp are one cycle
    - a later record starts a new cycle when the timestamp changes
    - cycle_gap_seconds is retained for future timestamp-tolerance support, but
      exact scanner timestamps remain the authoritative grouping key today
    """

    def __init__(
        self,
        *,
        cycle_gap_seconds: float = DEFAULT_CYCLE_GAP_SECONDS,
    ) -> None:
        if cycle_gap_seconds < 0:
            raise InvalidDatasetConfigurationError(
                "cycle_gap_seconds cannot be negative."
            )

        self.cycle_gap_seconds = float(cycle_gap_seconds)

    def from_adapter(
        self,
        adapter: ScannerHistoryAdapter,
        *,
        strict: bool = True,
    ) -> HistoricalDataset:
        events = adapter.load_events(strict=strict)
        return self.from_events(events)

    def from_events(
        self,
        events: Iterable[ScannerHistoryEvent],
    ) -> HistoricalDataset:
        ordered_events = sorted(
            events,
            key=lambda event: (event.scanned_at, event.event_id),
        )

        rows: list[HistoricalDatasetRow] = []
        current_cycle_timestamp: datetime | None = None
        current_cycle_number = 0
        cycle_position = 0

        for row_number, event in enumerate(ordered_events, start=1):
            if current_cycle_timestamp != event.scanned_at:
                current_cycle_number += 1
                current_cycle_timestamp = event.scanned_at
                cycle_position = 1
            else:
                cycle_position += 1

            cycle_id = self._build_cycle_id(
                current_cycle_number,
                event.scanned_at,
            )

            rows.append(
                self._normalize_event(
                    event=event,
                    row_number=row_number,
                    cycle_number=current_cycle_number,
                    cycle_id=cycle_id,
                    cycle_position=cycle_position,
                )
            )

        return HistoricalDataset(
            rows,
            cycle_gap_seconds=self.cycle_gap_seconds,
        )

    def _normalize_event(
        self,
        *,
        event: ScannerHistoryEvent,
        row_number: int,
        cycle_number: int,
        cycle_id: str,
        cycle_position: int,
    ) -> HistoricalDatasetRow:
        token = event.token.strip()
        token_key = token.upper()
        mint = event.mint.strip() if event.mint else None
        asset_key = mint or f"SYMBOL:{token_key}"

        starting_amount = self._finite_float(
            event.starting_amount_usd,
            "starting_amount_usd",
            event.event_id,
        )
        ending_amount = self._finite_float(
            event.ending_amount_usd,
            "ending_amount_usd",
            event.event_id,
        )
        quoted_profit = self._finite_float(
            event.quoted_profit_usd,
            "quoted_profit_usd",
            event.event_id,
        )
        estimated_cost = self._finite_float(
            event.estimated_cost_usd,
            "estimated_cost_usd",
            event.event_id,
        )
        net_profit = self._finite_float(
            event.net_profit_usd,
            "net_profit_usd",
            event.event_id,
        )

        market_score = self._bounded_score(
            event.market_score,
            "market_score",
            event.event_id,
        )
        liquidity_score = self._bounded_score(
            event.liquidity_score,
            "liquidity_score",
            event.event_id,
        )
        volume_score = self._bounded_score(
            event.volume_score,
            "volume_score",
            event.event_id,
        )
        pair_score = self._bounded_score(
            event.pair_score,
            "pair_score",
            event.event_id,
        )
        intelligence_score = self._bounded_score(
            event.intelligence_score,
            "intelligence_score",
            event.event_id,
        )

        if starting_amount > 0:
            gross_return_bps = quoted_profit / starting_amount * 10_000.0
            cost_bps = estimated_cost / starting_amount * 10_000.0
            net_return_bps = net_profit / starting_amount * 10_000.0
        else:
            gross_return_bps = 0.0
            cost_bps = 0.0
            net_return_bps = 0.0

        if estimated_cost > 0:
            gross_to_cost_ratio: float | None = quoted_profit / estimated_cost
        else:
            gross_to_cost_ratio = None

        buy_route = event.buy_route.strip() if event.buy_route else None
        sell_route = event.sell_route.strip() if event.sell_route else None
        has_route = bool(buy_route and sell_route)

        if buy_route or sell_route:
            route_pair = f"{buy_route or 'UNKNOWN'} || {sell_route or 'UNKNOWN'}"
        else:
            route_pair = "UNKNOWN"

        return HistoricalDatasetRow(
            row_number=row_number,
            event_id=event.event_id,
            cycle_number=cycle_number,
            cycle_id=cycle_id,
            cycle_position=cycle_position,
            scanned_at=event.scanned_at,
            token=token,
            token_key=token_key,
            mint=mint,
            asset_key=asset_key,
            buy_route=buy_route,
            sell_route=sell_route,
            route_pair=route_pair,
            starting_amount_usd=starting_amount,
            ending_amount_usd=ending_amount,
            quoted_profit_usd=quoted_profit,
            estimated_cost_usd=estimated_cost,
            net_profit_usd=net_profit,
            gross_return_bps=gross_return_bps,
            cost_bps=cost_bps,
            net_return_bps=net_return_bps,
            gross_to_cost_ratio=gross_to_cost_ratio,
            decision_raw=event.decision,
            decision=event.normalized_decision,
            eligible=event.eligible,
            quote_successful=event.quote_successful,
            profitable=event.profitable,
            error=event.error,
            market_score=market_score,
            liquidity_score=liquidity_score,
            volume_score=volume_score,
            pair_score=pair_score,
            intelligence_score=intelligence_score,
            has_mint=mint is not None,
            has_route=has_route,
            has_error=bool(event.error),
        )

    @staticmethod
    def _build_cycle_id(
        cycle_number: int,
        scanned_at: datetime,
    ) -> str:
        timestamp = scanned_at.strftime("%Y%m%dT%H%M%S%f")
        return f"CYCLE-{cycle_number:06d}-{timestamp}"

    @staticmethod
    def _finite_float(
        value: Any,
        field_name: str,
        event_id: int,
    ) -> float:
        try:
            number = float(value)
        except (TypeError, ValueError) as error:
            raise HistoricalDatasetError(
                f"Event {event_id} has invalid {field_name}: {value!r}"
            ) from error

        if not math.isfinite(number):
            raise HistoricalDatasetError(
                f"Event {event_id} has non-finite {field_name}: {number!r}"
            )

        return number

    @classmethod
    def _bounded_score(
        cls,
        value: Any,
        field_name: str,
        event_id: int,
    ) -> float:
        number = cls._finite_float(value, field_name, event_id)

        if number < 0.0 or number > 100.0:
            LOGGER.warning(
                "Event %s has %s outside 0-100: %.6f",
                event_id,
                field_name,
                number,
            )

        return number


def build_historical_dataset(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    strict: bool = True,
    cycle_gap_seconds: float = DEFAULT_CYCLE_GAP_SECONDS,
) -> HistoricalDataset:
    adapter = ScannerHistoryAdapter(database_path)
    builder = HistoricalDatasetBuilder(
        cycle_gap_seconds=cycle_gap_seconds,
    )
    return builder.from_adapter(adapter, strict=strict)


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build a normalized historical scanner research dataset."
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
        help="Number of sample rows to display",
    )
    parser.add_argument(
        "--token",
        help="Optional token filter applied after dataset construction",
    )
    parser.add_argument(
        "--decision",
        choices=("EXECUTE", "WATCH", "SKIP", "QUOTE_ERROR"),
        help="Optional normalized decision filter",
    )
    parser.add_argument(
        "--successful-only",
        action="store_true",
        help="Keep only successful quote rows",
    )
    parser.add_argument(
        "--eligible-only",
        action="store_true",
        help="Keep only eligible rows",
    )
    parser.add_argument(
        "--profitable-only",
        action="store_true",
        help="Keep only profitable rows",
    )
    parser.add_argument(
        "--export-csv",
        nargs="?",
        const=str(DEFAULT_CSV_PATH),
        help="Export CSV, optionally to a supplied path",
    )
    parser.add_argument(
        "--export-json",
        nargs="?",
        const=str(DEFAULT_JSON_PATH),
        help="Export JSON, optionally to a supplied path",
    )
    parser.add_argument(
        "--non-strict",
        action="store_true",
        help="Skip malformed scanner rows instead of failing",
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
        dataset = build_historical_dataset(
            args.database,
            strict=not args.non_strict,
        )
    except (
        HistoricalDatasetError,
        ScannerHistoryAdapterError,
        ValueError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    dataset = dataset.filter(
        token=args.token,
        decision=args.decision,
        quote_successful=True if args.successful_only else None,
        eligible=True if args.eligible_only else None,
        profitable=True if args.profitable_only else None,
    )

    summary = dataset.summarize()

    print("\nHistorical Research Dataset")
    print("=" * 64)
    print(f"Database: {args.database}")
    print(f"Rows: {summary.total_rows}")
    print(f"Cycles: {summary.total_cycles}")
    print(f"Unique assets: {summary.unique_assets}")
    print(f"Unique tokens: {summary.unique_tokens}")
    print()

    print("Dataset Summary")
    print("-" * 64)
    print(f"Successful quotes: {summary.successful_quotes}")
    print(f"Quote errors: {summary.quote_errors}")
    print(f"Eligible events: {summary.eligible_events}")
    print(f"Profitable events: {summary.profitable_events}")
    print(
        "EXECUTE / WATCH / SKIP / QUOTE_ERROR / UNKNOWN: "
        f"{summary.execute_decisions} / "
        f"{summary.watch_decisions} / "
        f"{summary.skip_decisions} / "
        f"{summary.quote_error_decisions} / "
        f"{summary.unknown_decisions}"
    )
    print(f"Rows with mint: {summary.rows_with_mint}")
    print(f"Rows without mint: {summary.rows_without_mint}")
    print(f"Average net profit: ${summary.average_net_profit_usd:.6f}")
    print(f"Median net profit: ${summary.median_net_profit_usd:.6f}")
    print(f"Best net profit: ${summary.best_net_profit_usd:.6f}")
    print(f"Worst net profit: ${summary.worst_net_profit_usd:.6f}")
    print(f"Average net return: {summary.average_net_return_bps:.4f} bps")
    print(f"Average cost: {summary.average_cost_bps:.4f} bps")
    print(f"First scan: {summary.first_scanned_at}")
    print(f"Last scan: {summary.last_scanned_at}")
    print()

    print("Sample rows")
    print("-" * 64)

    for row in dataset.rows[: max(args.limit, 0)]:
        print(
            f"{row.row_number}: {row.cycle_id} | {row.token} | "
            f"net ${row.net_profit_usd:.6f} | "
            f"{row.net_return_bps:.4f} bps | "
            f"{row.decision} | eligible={row.eligible}"
        )

    if args.export_csv:
        csv_path = dataset.export_csv(args.export_csv)
        print(f"\nCSV exported: {csv_path}")

    if args.export_json:
        json_path = dataset.export_json(args.export_json)
        print(f"JSON exported: {json_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())