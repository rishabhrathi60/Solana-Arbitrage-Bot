"""
Phase 10B — Historical Scanner Data Adapter

Read-only adapter for loading scanner history from SQLite and exposing a clean,
chronological event stream for research and backtesting.

This module never modifies the database.
"""

from __future__ import annotations

import argparse
import json
import logging
import sqlite3
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence


LOGGER = logging.getLogger(__name__)

DEFAULT_DATABASE_PATH = Path("database") / "trades.db"
SUPPORTED_TIMESTAMP_FORMATS: tuple[str, ...] = (
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
)


class ScannerHistoryAdapterError(RuntimeError):
    """Base exception for scanner history adapter failures."""


class DatabaseNotFoundError(ScannerHistoryAdapterError):
    """Raised when the configured SQLite database does not exist."""


class RequiredTableMissingError(ScannerHistoryAdapterError):
    """Raised when a required SQLite table is missing."""


class InvalidHistoricalRowError(ScannerHistoryAdapterError):
    """Raised when a historical row cannot be normalized safely."""


@dataclass(frozen=True, slots=True)
class ScannerHistoryEvent:
    """
    Canonical historical scanner observation.

    All monetary fields are interpreted in USD because the existing scanner
    stores starting_amount, ending_amount, profits, and costs in dollar terms.
    """

    event_id: int
    scanned_at: datetime
    token: str
    mint: str | None

    buy_route: str | None
    sell_route: str | None

    starting_amount_usd: float
    ending_amount_usd: float
    quoted_profit_usd: float
    estimated_cost_usd: float
    net_profit_usd: float

    decision: str
    eligible: bool
    quote_successful: bool
    error: str | None

    market_score: float
    liquidity_score: float
    volume_score: float
    pair_score: float
    intelligence_score: float

    @property
    def profitable(self) -> bool:
        return self.quote_successful and self.net_profit_usd > 0.0

    @property
    def normalized_decision(self) -> str:
        decision = self.decision.upper().strip()

        if "TEST FURTHER" in decision or "EXECUTE" in decision:
            return "EXECUTE"

        if "WATCH" in decision:
            return "WATCH"

        if "SKIP" in decision:
            return "SKIP"

        if "QUOTE ERROR" in decision:
            return "QUOTE_ERROR"

        return "UNKNOWN"

    @property
    def gross_profit_check_usd(self) -> float:
        return self.ending_amount_usd - self.starting_amount_usd

    @property
    def net_profit_check_usd(self) -> float:
        return self.quoted_profit_usd - self.estimated_cost_usd

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)
        result["scanned_at"] = self.scanned_at.isoformat(sep=" ")
        result["profitable"] = self.profitable
        result["normalized_decision"] = self.normalized_decision
        return result


@dataclass(frozen=True, slots=True)
class ScannerHistorySummary:
    total_events: int
    unique_tokens: int
    successful_quotes: int
    quote_errors: int
    eligible_events: int
    profitable_events: int
    first_scanned_at: datetime | None
    last_scanned_at: datetime | None
    average_net_profit_usd: float
    best_net_profit_usd: float
    worst_net_profit_usd: float

    def to_dict(self) -> dict[str, Any]:
        result = asdict(self)

        for key in ("first_scanned_at", "last_scanned_at"):
            value = result[key]
            result[key] = value.isoformat(sep=" ") if value else None

        return result


class ScannerHistoryAdapter:
    """
    Read-only SQLite adapter for opportunity_history.

    The adapter:
    - validates the database and required table
    - loads rows in strict chronological order
    - normalizes SQLite values into typed dataclasses
    - detects duplicate event IDs
    - validates scanner profit arithmetic
    """

    REQUIRED_TABLE = "opportunity_history"

    REQUIRED_COLUMNS: frozenset[str] = frozenset(
        {
            "id",
            "token",
            "buy_route",
            "sell_route",
            "starting_amount",
            "ending_amount",
            "quoted_profit",
            "estimated_cost",
            "net_profit",
            "decision",
            "eligible",
            "quote_successful",
            "error",
            "market_score",
            "liquidity_score",
            "volume_score",
            "pair_score",
            "scanned_at",
            "mint",
            "intelligence_score",
        }
    )

    SELECT_SQL = """
        SELECT
            id,
            token,
            buy_route,
            sell_route,
            starting_amount,
            ending_amount,
            quoted_profit,
            estimated_cost,
            net_profit,
            decision,
            eligible,
            quote_successful,
            error,
            market_score,
            liquidity_score,
            volume_score,
            pair_score,
            scanned_at,
            mint,
            intelligence_score
        FROM opportunity_history
        WHERE 1 = 1
    """

    def __init__(
        self,
        database_path: str | Path = DEFAULT_DATABASE_PATH,
        *,
        arithmetic_tolerance_usd: float = 1e-8,
    ) -> None:
        self.database_path = Path(database_path)
        self.arithmetic_tolerance_usd = float(arithmetic_tolerance_usd)

        if self.arithmetic_tolerance_usd < 0:
            raise ValueError("arithmetic_tolerance_usd cannot be negative.")

    def _connect(self) -> sqlite3.Connection:
        if not self.database_path.exists():
            raise DatabaseNotFoundError(
                f"SQLite database does not exist: {self.database_path}"
            )

        connection = sqlite3.connect(
            f"file:{self.database_path.resolve()}?mode=ro",
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only = ON")
        return connection

    def validate_schema(self) -> None:
        with self._connect() as connection:
            table_row = connection.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'table'
                  AND name = ?
                """,
                (self.REQUIRED_TABLE,),
            ).fetchone()

            if table_row is None:
                raise RequiredTableMissingError(
                    f"Required table is missing: {self.REQUIRED_TABLE}"
                )

            columns = {
                row["name"]
                for row in connection.execute(
                    f"PRAGMA table_info('{self.REQUIRED_TABLE}')"
                ).fetchall()
            }

            missing_columns = sorted(self.REQUIRED_COLUMNS - columns)

            if missing_columns:
                raise RequiredTableMissingError(
                    "Required columns are missing from "
                    f"{self.REQUIRED_TABLE}: {', '.join(missing_columns)}"
                )

    def count_events(
        self,
        *,
        start_time: datetime | str | None = None,
        end_time: datetime | str | None = None,
        token: str | None = None,
        mint: str | None = None,
        quote_successful_only: bool = False,
        eligible_only: bool = False,
    ) -> int:
        query, parameters = self._build_query(
            count_only=True,
            start_time=start_time,
            end_time=end_time,
            token=token,
            mint=mint,
            quote_successful_only=quote_successful_only,
            eligible_only=eligible_only,
            limit=None,
            offset=0,
        )

        with self._connect() as connection:
            row = connection.execute(query, parameters).fetchone()
            return int(row["row_count"])

    def load_events(
        self,
        *,
        start_time: datetime | str | None = None,
        end_time: datetime | str | None = None,
        token: str | None = None,
        mint: str | None = None,
        quote_successful_only: bool = False,
        eligible_only: bool = False,
        limit: int | None = None,
        offset: int = 0,
        strict: bool = True,
    ) -> list[ScannerHistoryEvent]:
        """
        Load normalized historical events.

        strict=True:
            raises on invalid rows, duplicate IDs, or arithmetic mismatches.

        strict=False:
            logs invalid rows and skips them.
        """

        self.validate_schema()

        query, parameters = self._build_query(
            count_only=False,
            start_time=start_time,
            end_time=end_time,
            token=token,
            mint=mint,
            quote_successful_only=quote_successful_only,
            eligible_only=eligible_only,
            limit=limit,
            offset=offset,
        )

        events: list[ScannerHistoryEvent] = []
        seen_event_ids: set[int] = set()

        with self._connect() as connection:
            rows = connection.execute(query, parameters)

            for row in rows:
                try:
                    event = self._row_to_event(row)
                    self._validate_event(event)

                    if event.event_id in seen_event_ids:
                        raise InvalidHistoricalRowError(
                            f"Duplicate event ID detected: {event.event_id}"
                        )

                    seen_event_ids.add(event.event_id)
                    events.append(event)

                except (InvalidHistoricalRowError, TypeError, ValueError) as error:
                    if strict:
                        raise

                    LOGGER.warning(
                        "Skipping invalid opportunity_history row id=%r: %s",
                        row["id"] if "id" in row.keys() else None,
                        error,
                    )

        self._validate_chronology(events, strict=strict)
        return events

    def iter_events(
        self,
        *,
        batch_size: int = 500,
        start_time: datetime | str | None = None,
        end_time: datetime | str | None = None,
        token: str | None = None,
        mint: str | None = None,
        quote_successful_only: bool = False,
        eligible_only: bool = False,
        strict: bool = True,
    ) -> Iterator[ScannerHistoryEvent]:
        if batch_size <= 0:
            raise ValueError("batch_size must be greater than zero.")

        offset = 0

        while True:
            batch = self.load_events(
                start_time=start_time,
                end_time=end_time,
                token=token,
                mint=mint,
                quote_successful_only=quote_successful_only,
                eligible_only=eligible_only,
                limit=batch_size,
                offset=offset,
                strict=strict,
            )

            if not batch:
                break

            yield from batch
            offset += len(batch)

            if len(batch) < batch_size:
                break

    def summarize(
        self,
        events: Sequence[ScannerHistoryEvent] | None = None,
    ) -> ScannerHistorySummary:
        if events is None:
            events = self.load_events(strict=False)

        if not events:
            return ScannerHistorySummary(
                total_events=0,
                unique_tokens=0,
                successful_quotes=0,
                quote_errors=0,
                eligible_events=0,
                profitable_events=0,
                first_scanned_at=None,
                last_scanned_at=None,
                average_net_profit_usd=0.0,
                best_net_profit_usd=0.0,
                worst_net_profit_usd=0.0,
            )

        net_profits = [event.net_profit_usd for event in events]

        return ScannerHistorySummary(
            total_events=len(events),
            unique_tokens=len({event.token for event in events}),
            successful_quotes=sum(event.quote_successful for event in events),
            quote_errors=sum(not event.quote_successful for event in events),
            eligible_events=sum(event.eligible for event in events),
            profitable_events=sum(event.profitable for event in events),
            first_scanned_at=min(event.scanned_at for event in events),
            last_scanned_at=max(event.scanned_at for event in events),
            average_net_profit_usd=sum(net_profits) / len(net_profits),
            best_net_profit_usd=max(net_profits),
            worst_net_profit_usd=min(net_profits),
        )

    def _build_query(
        self,
        *,
        count_only: bool,
        start_time: datetime | str | None,
        end_time: datetime | str | None,
        token: str | None,
        mint: str | None,
        quote_successful_only: bool,
        eligible_only: bool,
        limit: int | None,
        offset: int,
    ) -> tuple[str, list[Any]]:
        if offset < 0:
            raise ValueError("offset cannot be negative.")

        if limit is not None and limit <= 0:
            raise ValueError("limit must be greater than zero when provided.")

        query = (
            "SELECT COUNT(*) AS row_count "
            "FROM opportunity_history WHERE 1 = 1"
            if count_only
            else self.SELECT_SQL
        )

        parameters: list[Any] = []

        if start_time is not None:
            query += " AND scanned_at >= ?"
            parameters.append(self._to_sql_timestamp(start_time))

        if end_time is not None:
            query += " AND scanned_at <= ?"
            parameters.append(self._to_sql_timestamp(end_time))

        if token:
            query += " AND UPPER(token) = UPPER(?)"
            parameters.append(token.strip())

        if mint:
            query += " AND mint = ?"
            parameters.append(mint.strip())

        if quote_successful_only:
            query += " AND quote_successful = 1"

        if eligible_only:
            query += " AND eligible = 1"

        if not count_only:
            query += " ORDER BY datetime(scanned_at) ASC, id ASC"

            if limit is not None:
                query += " LIMIT ? OFFSET ?"
                parameters.extend([limit, offset])
            elif offset:
                query += " LIMIT -1 OFFSET ?"
                parameters.append(offset)

        return query, parameters

    def _row_to_event(self, row: sqlite3.Row) -> ScannerHistoryEvent:
        event_id = self._required_int(row["id"], "id")
        scanned_at = self._parse_timestamp(row["scanned_at"], "scanned_at")
        token = self._required_text(row["token"], "token")

        return ScannerHistoryEvent(
            event_id=event_id,
            scanned_at=scanned_at,
            token=token,
            mint=self._optional_text(row["mint"]),
            buy_route=self._optional_text(row["buy_route"]),
            sell_route=self._optional_text(row["sell_route"]),
            starting_amount_usd=self._required_float(
                row["starting_amount"], "starting_amount"
            ),
            ending_amount_usd=self._required_float(
                row["ending_amount"], "ending_amount"
            ),
            quoted_profit_usd=self._required_float(
                row["quoted_profit"], "quoted_profit"
            ),
            estimated_cost_usd=self._required_float(
                row["estimated_cost"], "estimated_cost"
            ),
            net_profit_usd=self._required_float(row["net_profit"], "net_profit"),
            decision=self._optional_text(row["decision"]) or "UNKNOWN",
            eligible=self._to_bool(row["eligible"]),
            quote_successful=self._to_bool(row["quote_successful"]),
            error=self._optional_text(row["error"]),
            market_score=self._optional_float(row["market_score"]),
            liquidity_score=self._optional_float(row["liquidity_score"]),
            volume_score=self._optional_float(row["volume_score"]),
            pair_score=self._optional_float(row["pair_score"]),
            intelligence_score=self._optional_float(row["intelligence_score"]),
        )

    def _validate_event(self, event: ScannerHistoryEvent) -> None:
        if event.starting_amount_usd < 0:
            raise InvalidHistoricalRowError(
                f"Event {event.event_id} has negative starting_amount."
            )

        if event.ending_amount_usd < 0:
            raise InvalidHistoricalRowError(
                f"Event {event.event_id} has negative ending_amount."
            )

        # Failed quote rows are valid historical observations.
        # Their monetary values may be scanner sentinel values.
        if not event.quote_successful:
            if not event.error:
                LOGGER.debug(
                    "Event %s is marked unsuccessful but has no error message.",
                    event.event_id,
                )
            return

        gross_difference = abs(
            event.quoted_profit_usd - event.gross_profit_check_usd
        )

        if gross_difference > self.arithmetic_tolerance_usd:
            raise InvalidHistoricalRowError(
                f"Event {event.event_id} quoted profit mismatch: "
                f"stored={event.quoted_profit_usd:.12f}, "
                f"calculated={event.gross_profit_check_usd:.12f}"
            )

        net_difference = abs(
            event.net_profit_usd - event.net_profit_check_usd
        )

        if net_difference > self.arithmetic_tolerance_usd:
            raise InvalidHistoricalRowError(
                f"Event {event.event_id} net profit mismatch: "
                f"stored={event.net_profit_usd:.12f}, "
                f"calculated={event.net_profit_check_usd:.12f}"
            )

    @staticmethod
    def _validate_chronology(
        events: Sequence[ScannerHistoryEvent],
        *,
        strict: bool,
    ) -> None:
        previous: ScannerHistoryEvent | None = None

        for event in events:
            if previous is not None:
                out_of_order = (
                    event.scanned_at < previous.scanned_at
                    or (
                        event.scanned_at == previous.scanned_at
                        and event.event_id < previous.event_id
                    )
                )

                if out_of_order:
                    message = (
                        "Historical event stream is not chronological: "
                        f"{previous.event_id} -> {event.event_id}"
                    )

                    if strict:
                        raise InvalidHistoricalRowError(message)

                    LOGGER.warning(message)

            previous = event

    @staticmethod
    def _parse_timestamp(value: Any, field_name: str) -> datetime:
        if isinstance(value, datetime):
            return value

        if value is None:
            raise InvalidHistoricalRowError(
                f"Required timestamp field {field_name!r} is null."
            )

        text = str(value).strip()

        if not text:
            raise InvalidHistoricalRowError(
                f"Required timestamp field {field_name!r} is empty."
            )

        try:
            return datetime.fromisoformat(text)
        except ValueError:
            pass

        for timestamp_format in SUPPORTED_TIMESTAMP_FORMATS:
            try:
                return datetime.strptime(text, timestamp_format)
            except ValueError:
                continue

        raise InvalidHistoricalRowError(
            f"Could not parse {field_name!r} timestamp: {text!r}"
        )

    @classmethod
    def _to_sql_timestamp(cls, value: datetime | str) -> str:
        parsed = cls._parse_timestamp(value, "filter_timestamp")
        return parsed.strftime("%Y-%m-%d %H:%M:%S.%f")

    @staticmethod
    def _required_text(value: Any, field_name: str) -> str:
        if value is None:
            raise InvalidHistoricalRowError(
                f"Required text field {field_name!r} is null."
            )

        text = str(value).strip()

        if not text:
            raise InvalidHistoricalRowError(
                f"Required text field {field_name!r} is empty."
            )

        return text

    @staticmethod
    def _optional_text(value: Any) -> str | None:
        if value is None:
            return None

        text = str(value).strip()
        return text or None

    @staticmethod
    def _required_float(value: Any, field_name: str) -> float:
        if value is None:
            raise InvalidHistoricalRowError(
                f"Required numeric field {field_name!r} is null."
            )

        try:
            return float(value)
        except (TypeError, ValueError) as error:
            raise InvalidHistoricalRowError(
                f"Field {field_name!r} is not numeric: {value!r}"
            ) from error

    @staticmethod
    def _optional_float(value: Any, default: float = 0.0) -> float:
        if value is None:
            return default

        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _required_int(value: Any, field_name: str) -> int:
        if value is None:
            raise InvalidHistoricalRowError(
                f"Required integer field {field_name!r} is null."
            )

        try:
            return int(value)
        except (TypeError, ValueError) as error:
            raise InvalidHistoricalRowError(
                f"Field {field_name!r} is not an integer: {value!r}"
            ) from error

    @staticmethod
    def _to_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value

        if value is None:
            return False

        if isinstance(value, (int, float)):
            return bool(value)

        normalized = str(value).strip().lower()
        return normalized in {"1", "true", "yes", "y", "on"}


def _configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def _build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Inspect historical scanner observations from SQLite."
    )
    parser.add_argument(
        "--database",
        default=str(DEFAULT_DATABASE_PATH),
        help="Path to trades.db",
    )
    parser.add_argument("--token", help="Filter by token symbol")
    parser.add_argument("--mint", help="Filter by token mint")
    parser.add_argument("--start-time", help="Inclusive starting timestamp")
    parser.add_argument("--end-time", help="Inclusive ending timestamp")
    parser.add_argument(
        "--successful-only",
        action="store_true",
        help="Return only successful quotes",
    )
    parser.add_argument(
        "--eligible-only",
        action="store_true",
        help="Return only eligible opportunities",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Number of sample events to print",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print output as JSON",
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

    adapter = ScannerHistoryAdapter(args.database)

    try:
        total_matching = adapter.count_events(
            start_time=args.start_time,
            end_time=args.end_time,
            token=args.token,
            mint=args.mint,
            quote_successful_only=args.successful_only,
            eligible_only=args.eligible_only,
        )

        events = adapter.load_events(
            start_time=args.start_time,
            end_time=args.end_time,
            token=args.token,
            mint=args.mint,
            quote_successful_only=args.successful_only,
            eligible_only=args.eligible_only,
            limit=args.limit,
            strict=True,
        )

        summary_events = adapter.load_events(
            start_time=args.start_time,
            end_time=args.end_time,
            token=args.token,
            mint=args.mint,
            quote_successful_only=args.successful_only,
            eligible_only=args.eligible_only,
            strict=False,
        )
        summary = adapter.summarize(summary_events)

    except ScannerHistoryAdapterError as error:
        LOGGER.error("%s", error)
        return 1

    if args.json:
        payload = {
            "database": str(adapter.database_path),
            "matching_rows": total_matching,
            "summary": summary.to_dict(),
            "sample_events": [event.to_dict() for event in events],
        }
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return 0

    print("\nHistorical Scanner Adapter")
    print("=" * 60)
    print(f"Database: {adapter.database_path}")
    print(f"Matching rows: {total_matching}")
    print(f"Loaded sample: {len(events)}")
    print()

    print("Summary")
    print("-" * 60)
    print(f"Total events: {summary.total_events}")
    print(f"Unique tokens: {summary.unique_tokens}")
    print(f"Successful quotes: {summary.successful_quotes}")
    print(f"Quote errors: {summary.quote_errors}")
    print(f"Eligible events: {summary.eligible_events}")
    print(f"Profitable events: {summary.profitable_events}")
    print(f"Average net profit: ${summary.average_net_profit_usd:.6f}")
    print(f"Best net profit: ${summary.best_net_profit_usd:.6f}")
    print(f"Worst net profit: ${summary.worst_net_profit_usd:.6f}")
    print(f"First scan: {summary.first_scanned_at}")
    print(f"Last scan: {summary.last_scanned_at}")
    print()

    print("Sample events")
    print("-" * 60)

    for event in events:
        print(
            f"{event.event_id}: {event.scanned_at} | {event.token} | "
            f"net ${event.net_profit_usd:.6f} | "
            f"{event.normalized_decision} | "
            f"eligible={event.eligible}"
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())