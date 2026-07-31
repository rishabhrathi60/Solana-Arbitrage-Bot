"""
Phase 11A — Institutional Feature Store

Builds a versioned, zero-lookahead research feature store from validated
BacktestEvent records. It never modifies SQLite, the scanner, or live state.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import os
import statistics
import tempfile
from collections import defaultdict, deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

try:
    from backtesting.event_builder import (
        BacktestEvent,
        BacktestEventCollection,
        EventBuilderError,
        build_backtest_events,
    )
    from backtesting.historical_dataset import DEFAULT_DATABASE_PATH
except ModuleNotFoundError:
    from event_builder import (  # type: ignore
        BacktestEvent,
        BacktestEventCollection,
        EventBuilderError,
        build_backtest_events,
    )
    from historical_dataset import DEFAULT_DATABASE_PATH  # type: ignore


LOGGER = logging.getLogger(__name__)

FEATURE_SCHEMA_VERSION = "11A.1.0"
DEFAULT_OUTPUT_DIRECTORY = Path("research") / "feature_store"


class FeatureStoreError(RuntimeError):
    pass


class InvalidFeatureStoreConfigurationError(FeatureStoreError):
    pass


class EmptyFeatureStoreError(FeatureStoreError):
    pass


@dataclass(frozen=True, slots=True)
class FeatureStoreConfiguration:
    rolling_window_events: int = 25
    include_quote_errors: bool = True
    include_outcome_labels: bool = True
    output_directory: Path = DEFAULT_OUTPUT_DIRECTORY
    overwrite: bool = True

    def validate(self) -> None:
        if self.rolling_window_events <= 0:
            raise InvalidFeatureStoreConfigurationError(
                "rolling_window_events must be positive."
            )
        if not str(self.output_directory).strip():
            raise InvalidFeatureStoreConfigurationError(
                "output_directory cannot be empty."
            )


@dataclass(frozen=True, slots=True)
class FeatureDefinition:
    name: str
    role: str
    data_type: str
    source: str
    lookahead_safe: bool

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class FeatureRow:
    feature_row_number: int
    source_event_id: int
    feature_schema_version: str

    timestamp: datetime
    timestamp_unix: int
    date_utc: str
    hour_utc: int
    weekday_utc: int

    cycle_number: int
    cycle_id: str
    cycle_position: int
    cycle_size: int
    cycle_progress: float

    token: str
    token_key: str
    mint: str | None
    asset_key: str
    route_pair: str

    starting_amount_usd: float
    estimated_cost_usd: float
    cost_bps: float

    source_decision: str
    source_decision_rank: int
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
    score_range: float

    has_mint: bool
    has_route: bool
    has_error: bool

    prior_token_observations: int
    prior_token_profitable_observations: int
    prior_token_win_rate: float
    prior_token_average_net_profit_usd: float
    prior_token_average_cost_bps: float

    prior_global_observations: int
    prior_global_profitable_observations: int
    prior_global_win_rate: float
    prior_global_average_net_profit_usd: float

    rolling_token_observations: int
    rolling_token_win_rate: float
    rolling_token_average_profit_usd: float
    rolling_token_profit_std_usd: float
    rolling_token_average_composite_score: float

    rolling_global_observations: int
    rolling_global_win_rate: float
    rolling_global_average_profit_usd: float
    rolling_global_profit_std_usd: float
    rolling_global_average_composite_score: float

    prior_cycle_average_market_score: float
    prior_cycle_average_composite_score: float
    prior_cycle_profitable_rate: float
    prior_cycle_average_net_profit_usd: float
    cycles_since_token_seen: int

    execution_candidate_label: bool | None
    realized_net_profit_usd: float | None
    profitable_label: bool | None
    outcome_label: str | None

    def to_record(self) -> dict[str, Any]:
        result = asdict(self)
        result["timestamp"] = self.timestamp.isoformat(sep=" ")
        return result


@dataclass(frozen=True, slots=True)
class FeatureStoreSummary:
    generated_at: datetime
    schema_version: str
    rows: int
    cycles: int
    unique_assets: int
    unique_tokens: int
    successful_quotes: int
    quote_errors: int
    profitable_labels: int
    negative_labels: int
    feature_count: int
    label_count: int
    csv_path: str
    jsonl_path: str
    metadata_path: str
    manifest_path: str
    sha256_csv: str
    sha256_jsonl: str

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["generated_at"] = self.generated_at.isoformat()
        return result


class _History:
    def __init__(self, maxlen: int) -> None:
        self.profits: deque[float] = deque(maxlen=maxlen)
        self.composite_scores: deque[float] = deque(maxlen=maxlen)
        self.total_observations = 0
        self.total_profitable = 0
        self.total_profit = 0.0
        self.total_cost_bps = 0.0

    def snapshot(self) -> dict[str, float | int]:
        count = len(self.profits)
        return {
            "prior_observations": self.total_observations,
            "prior_profitable": self.total_profitable,
            "prior_win_rate": (
                self.total_profitable / self.total_observations
                if self.total_observations else 0.0
            ),
            "prior_average_profit": (
                self.total_profit / self.total_observations
                if self.total_observations else 0.0
            ),
            "prior_average_cost_bps": (
                self.total_cost_bps / self.total_observations
                if self.total_observations else 0.0
            ),
            "rolling_observations": count,
            "rolling_win_rate": (
                sum(value > 0 for value in self.profits) / count
                if count else 0.0
            ),
            "rolling_average_profit": (
                statistics.fmean(self.profits) if self.profits else 0.0
            ),
            "rolling_profit_std": (
                statistics.pstdev(self.profits)
                if len(self.profits) > 1 else 0.0
            ),
            "rolling_average_composite_score": (
                statistics.fmean(self.composite_scores)
                if self.composite_scores else 0.0
            ),
        }

    def update(
        self,
        *,
        profit: float,
        composite_score: float,
        cost_bps: float,
    ) -> None:
        self.total_observations += 1
        self.total_profitable += int(profit > 0)
        self.total_profit += profit
        self.total_cost_bps += cost_bps
        self.profits.append(profit)
        self.composite_scores.append(composite_score)


class FeatureStoreBuilder:
    def __init__(
        self,
        configuration: FeatureStoreConfiguration | None = None,
    ) -> None:
        self.configuration = configuration or FeatureStoreConfiguration()
        self.configuration.validate()

    def build(
        self,
        events: BacktestEventCollection | Sequence[BacktestEvent],
    ) -> tuple[FeatureRow, ...]:
        ordered = sorted(
            tuple(events),
            key=lambda event: (event.timestamp, event.source_event_id),
        )
        if not ordered:
            raise EmptyFeatureStoreError("No events available.")

        cycle_sizes: dict[str, int] = defaultdict(int)
        for event in ordered:
            cycle_sizes[event.cycle_id] += 1

        token_history: dict[str, _History] = {}
        global_history = _History(self.configuration.rolling_window_events)
        last_seen_cycle: dict[str, int] = {}

        previous_cycle_stats = {
            "market": 0.0,
            "composite": 0.0,
            "profitable_rate": 0.0,
            "average_profit": 0.0,
        }
        active_cycle_id: str | None = None
        active_cycle_events: list[BacktestEvent] = []
        rows: list[FeatureRow] = []

        for event in ordered:
            if active_cycle_id is not None and event.cycle_id != active_cycle_id:
                previous_cycle_stats = self._cycle_stats(active_cycle_events)
                active_cycle_events = []

            active_cycle_id = event.cycle_id

            history = token_history.setdefault(
                event.asset_key,
                _History(self.configuration.rolling_window_events),
            )
            token_snapshot = history.snapshot()
            global_snapshot = global_history.snapshot()

            last_cycle = last_seen_cycle.get(event.asset_key)
            cycles_since_seen = (
                event.cycle_number - last_cycle
                if last_cycle is not None else 0
            )

            cycle_size = cycle_sizes[event.cycle_id]
            include_labels = self.configuration.include_outcome_labels

            row = FeatureRow(
                feature_row_number=len(rows) + 1,
                source_event_id=event.source_event_id,
                feature_schema_version=FEATURE_SCHEMA_VERSION,
                timestamp=event.timestamp,
                timestamp_unix=int(event.timestamp.timestamp()),
                date_utc=event.timestamp.strftime("%Y-%m-%d"),
                hour_utc=event.timestamp.hour,
                weekday_utc=event.timestamp.weekday(),
                cycle_number=event.cycle_number,
                cycle_id=event.cycle_id,
                cycle_position=event.cycle_position,
                cycle_size=cycle_size,
                cycle_progress=(
                    event.cycle_position / cycle_size if cycle_size else 0.0
                ),
                token=event.token,
                token_key=event.token_key,
                mint=event.mint,
                asset_key=event.asset_key,
                route_pair=event.route_pair,
                starting_amount_usd=event.starting_amount_usd,
                estimated_cost_usd=event.estimated_cost_usd,
                cost_bps=event.cost_bps,
                source_decision=event.decision,
                source_decision_rank=event.decision_rank,
                source_eligible=event.eligible,
                quote_successful=event.quote_successful,
                market_score=event.market_score,
                liquidity_score=event.liquidity_score,
                volume_score=event.volume_score,
                pair_score=event.pair_score,
                intelligence_score=event.intelligence_score,
                composite_market_score=event.composite_market_score,
                score_dispersion=event.score_dispersion,
                minimum_component_score=event.minimum_component_score,
                maximum_component_score=event.maximum_component_score,
                score_range=(
                    event.maximum_component_score
                    - event.minimum_component_score
                ),
                has_mint=event.has_mint,
                has_route=event.has_route,
                has_error=event.has_error,
                prior_token_observations=int(
                    token_snapshot["prior_observations"]
                ),
                prior_token_profitable_observations=int(
                    token_snapshot["prior_profitable"]
                ),
                prior_token_win_rate=float(
                    token_snapshot["prior_win_rate"]
                ),
                prior_token_average_net_profit_usd=float(
                    token_snapshot["prior_average_profit"]
                ),
                prior_token_average_cost_bps=float(
                    token_snapshot["prior_average_cost_bps"]
                ),
                prior_global_observations=int(
                    global_snapshot["prior_observations"]
                ),
                prior_global_profitable_observations=int(
                    global_snapshot["prior_profitable"]
                ),
                prior_global_win_rate=float(
                    global_snapshot["prior_win_rate"]
                ),
                prior_global_average_net_profit_usd=float(
                    global_snapshot["prior_average_profit"]
                ),
                rolling_token_observations=int(
                    token_snapshot["rolling_observations"]
                ),
                rolling_token_win_rate=float(
                    token_snapshot["rolling_win_rate"]
                ),
                rolling_token_average_profit_usd=float(
                    token_snapshot["rolling_average_profit"]
                ),
                rolling_token_profit_std_usd=float(
                    token_snapshot["rolling_profit_std"]
                ),
                rolling_token_average_composite_score=float(
                    token_snapshot["rolling_average_composite_score"]
                ),
                rolling_global_observations=int(
                    global_snapshot["rolling_observations"]
                ),
                rolling_global_win_rate=float(
                    global_snapshot["rolling_win_rate"]
                ),
                rolling_global_average_profit_usd=float(
                    global_snapshot["rolling_average_profit"]
                ),
                rolling_global_profit_std_usd=float(
                    global_snapshot["rolling_profit_std"]
                ),
                rolling_global_average_composite_score=float(
                    global_snapshot["rolling_average_composite_score"]
                ),
                prior_cycle_average_market_score=float(
                    previous_cycle_stats["market"]
                ),
                prior_cycle_average_composite_score=float(
                    previous_cycle_stats["composite"]
                ),
                prior_cycle_profitable_rate=float(
                    previous_cycle_stats["profitable_rate"]
                ),
                prior_cycle_average_net_profit_usd=float(
                    previous_cycle_stats["average_profit"]
                ),
                cycles_since_token_seen=cycles_since_seen,
                execution_candidate_label=(
                    event.execution_candidate if include_labels else None
                ),
                realized_net_profit_usd=(
                    event.net_profit_usd if include_labels else None
                ),
                profitable_label=(
                    event.profitable if include_labels else None
                ),
                outcome_label=(
                    event.outcome_label if include_labels else None
                ),
            )

            rows.append(row)
            active_cycle_events.append(event)

            # Past-only guarantee: update history after creating this row.
            if event.quote_successful:
                history.update(
                    profit=event.net_profit_usd,
                    composite_score=event.composite_market_score,
                    cost_bps=event.cost_bps,
                )
                global_history.update(
                    profit=event.net_profit_usd,
                    composite_score=event.composite_market_score,
                    cost_bps=event.cost_bps,
                )

            last_seen_cycle[event.asset_key] = event.cycle_number

        return tuple(rows)

    @staticmethod
    def _cycle_stats(
        events: Sequence[BacktestEvent],
    ) -> dict[str, float]:
        if not events:
            return {
                "market": 0.0,
                "composite": 0.0,
                "profitable_rate": 0.0,
                "average_profit": 0.0,
            }

        successful = [
            event for event in events if event.quote_successful
        ]

        return {
            "market": statistics.fmean(
                event.market_score for event in events
            ),
            "composite": statistics.fmean(
                event.composite_market_score for event in events
            ),
            "profitable_rate": (
                sum(event.net_profit_usd > 0 for event in successful)
                / len(successful)
                if successful else 0.0
            ),
            "average_profit": (
                statistics.fmean(
                    event.net_profit_usd for event in successful
                )
                if successful else 0.0
            ),
        }


def feature_definitions() -> tuple[FeatureDefinition, ...]:
    labels = {
        "execution_candidate_label",
        "realized_net_profit_usd",
        "profitable_label",
        "outcome_label",
    }
    identifiers = {
        "feature_row_number",
        "source_event_id",
        "feature_schema_version",
        "timestamp",
        "cycle_id",
        "token",
        "token_key",
        "mint",
        "asset_key",
        "route_pair",
    }

    definitions: list[FeatureDefinition] = []

    for name, annotation in FeatureRow.__annotations__.items():
        if name in labels:
            role = "LABEL"
            source = "BacktestEvent realized outcome"
            lookahead_safe = False
        elif name in identifiers:
            role = "IDENTIFIER"
            source = "BacktestEvent identity"
            lookahead_safe = True
        elif name.startswith("prior_") or name.startswith("rolling_"):
            role = "FEATURE"
            source = "Past-only historical aggregation"
            lookahead_safe = True
        else:
            role = "FEATURE"
            source = "BacktestEvent pre-outcome field"
            lookahead_safe = True

        definitions.append(
            FeatureDefinition(
                name=name,
                role=role,
                data_type=str(annotation),
                source=source,
                lookahead_safe=lookahead_safe,
            )
        )

    return tuple(definitions)


def export_feature_store(
    rows: Sequence[FeatureRow],
    configuration: FeatureStoreConfiguration,
) -> FeatureStoreSummary:
    if not rows:
        raise EmptyFeatureStoreError("Feature store is empty.")

    output = configuration.output_directory
    output.mkdir(parents=True, exist_ok=True)

    csv_path = output / "features.csv"
    jsonl_path = output / "features.jsonl"
    metadata_path = output / "feature_store_metadata.json"
    manifest_path = output / "feature_manifest.json"

    records = [row.to_record() for row in rows]

    _write_csv(csv_path, records, overwrite=configuration.overwrite)
    _write_jsonl(jsonl_path, records, overwrite=configuration.overwrite)

    definitions = feature_definitions()

    summary = FeatureStoreSummary(
        generated_at=datetime.now(timezone.utc),
        schema_version=FEATURE_SCHEMA_VERSION,
        rows=len(rows),
        cycles=len({row.cycle_id for row in rows}),
        unique_assets=len({row.asset_key for row in rows}),
        unique_tokens=len({row.token_key for row in rows}),
        successful_quotes=sum(row.quote_successful for row in rows),
        quote_errors=sum(not row.quote_successful for row in rows),
        profitable_labels=sum(
            row.profitable_label is True for row in rows
        ),
        negative_labels=sum(
            row.profitable_label is False and row.quote_successful
            for row in rows
        ),
        feature_count=sum(
            definition.role == "FEATURE" for definition in definitions
        ),
        label_count=sum(
            definition.role == "LABEL" for definition in definitions
        ),
        csv_path=str(csv_path),
        jsonl_path=str(jsonl_path),
        metadata_path=str(metadata_path),
        manifest_path=str(manifest_path),
        sha256_csv=_sha256(csv_path),
        sha256_jsonl=_sha256(jsonl_path),
    )

    metadata_path.write_text(
        json.dumps(
            {
                "summary": summary.to_dict(),
                "configuration": {
                    "rolling_window_events": (
                        configuration.rolling_window_events
                    ),
                    "include_quote_errors": (
                        configuration.include_quote_errors
                    ),
                    "include_outcome_labels": (
                        configuration.include_outcome_labels
                    ),
                },
                "zero_lookahead_rule": (
                    "Each feature row is emitted before the same event outcome "
                    "updates token or global history."
                ),
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    manifest_path.write_text(
        json.dumps(
            {
                "schema_version": FEATURE_SCHEMA_VERSION,
                "definitions": [
                    definition.to_dict()
                    for definition in definitions
                ],
                "files": {
                    "features_csv": str(csv_path),
                    "features_jsonl": str(jsonl_path),
                    "metadata_json": str(metadata_path),
                },
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    return summary


def build_feature_store(
    database_path: str | Path = DEFAULT_DATABASE_PATH,
    *,
    configuration: FeatureStoreConfiguration | None = None,
) -> tuple[tuple[FeatureRow, ...], FeatureStoreSummary]:
    active_configuration = (
        configuration or FeatureStoreConfiguration()
    )
    active_configuration.validate()

    events = build_backtest_events(
        database_path,
        strict=True,
    )

    if not active_configuration.include_quote_errors:
        events = events.filter(quote_successful=True)

    rows = FeatureStoreBuilder(
        active_configuration
    ).build(events)

    summary = export_feature_store(
        rows,
        active_configuration,
    )

    return rows, summary


def _write_csv(
    path: Path,
    records: Sequence[Mapping[str, Any]],
    *,
    overwrite: bool,
) -> None:
    if path.exists() and not overwrite:
        raise FeatureStoreError(f"Refusing to overwrite: {path}")

    def writer(temp_path: Path) -> None:
        with temp_path.open(
            "w",
            encoding="utf-8",
            newline="",
        ) as handle:
            csv_writer = csv.DictWriter(
                handle,
                fieldnames=list(records[0].keys()),
                extrasaction="raise",
            )
            csv_writer.writeheader()
            csv_writer.writerows(records)

    _atomic_write(path, writer)


def _write_jsonl(
    path: Path,
    records: Sequence[Mapping[str, Any]],
    *,
    overwrite: bool,
) -> None:
    if path.exists() and not overwrite:
        raise FeatureStoreError(f"Refusing to overwrite: {path}")

    def writer(temp_path: Path) -> None:
        with temp_path.open("w", encoding="utf-8") as handle:
            for record in records:
                handle.write(
                    json.dumps(
                        record,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                )
                handle.write("\n")

    _atomic_write(path, writer)


def _atomic_write(destination: Path, writer: Any) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)

    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".tmp",
        dir=str(destination.parent),
    )
    os.close(descriptor)
    temporary = Path(temporary_name)

    try:
        writer(temporary)

        if not temporary.exists() or temporary.stat().st_size == 0:
            raise FeatureStoreError(
                "Temporary export was not created correctly."
            )

        os.replace(temporary, destination)

    except Exception:
        temporary.unlink(missing_ok=True)
        raise


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(
            lambda: handle.read(1024 * 1024),
            b"",
        ):
            digest.update(chunk)

    return digest.hexdigest()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Build the Phase 11A zero-lookahead feature store."
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
        "--rolling-window-events",
        type=int,
        default=25,
    )
    parser.add_argument(
        "--exclude-quote-errors",
        action="store_true",
    )
    parser.add_argument(
        "--exclude-labels",
        action="store_true",
    )
    parser.add_argument(
        "--no-overwrite",
        action="store_true",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

    configuration = FeatureStoreConfiguration(
        rolling_window_events=args.rolling_window_events,
        include_quote_errors=not args.exclude_quote_errors,
        include_outcome_labels=not args.exclude_labels,
        output_directory=Path(args.output_directory),
        overwrite=not args.no_overwrite,
    )

    try:
        _rows, summary = build_feature_store(
            args.database,
            configuration=configuration,
        )
    except (
        FeatureStoreError,
        EventBuilderError,
        OSError,
        ValueError,
    ) as error:
        LOGGER.error("%s", error)
        return 1

    print("\nPhase 11A — Institutional Feature Store")
    print("=" * 80)
    print(f"Schema version: {summary.schema_version}")
    print(f"Rows: {summary.rows}")
    print(f"Cycles: {summary.cycles}")
    print(f"Unique assets: {summary.unique_assets}")
    print(f"Unique tokens: {summary.unique_tokens}")
    print(f"Successful quotes: {summary.successful_quotes}")
    print(f"Quote errors: {summary.quote_errors}")
    print(f"Profitable labels: {summary.profitable_labels}")
    print(f"Negative labels: {summary.negative_labels}")
    print(f"Features: {summary.feature_count}")
    print(f"Labels: {summary.label_count}")
    print()
    print("Zero-lookahead rule")
    print("-" * 80)
    print(
        "Each feature row is emitted before the same event outcome "
        "updates token or global history."
    )
    print()
    print("Output files")
    print("-" * 80)
    print(summary.csv_path)
    print(summary.jsonl_path)
    print(summary.metadata_path)
    print(summary.manifest_path)
    print()
    print(f"CSV SHA-256: {summary.sha256_csv}")
    print(f"JSONL SHA-256: {summary.sha256_jsonl}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())