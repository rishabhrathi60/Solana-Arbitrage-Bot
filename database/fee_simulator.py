import json
import os
import sqlite3
from datetime import datetime
from pathlib import Path


DATABASE_FILE = (
    Path(__file__).resolve().parent
    / "trades.db"
)

OPERATING_MODE = "SIMULATION_ONLY"
LIVE_TRANSACTION_SIGNING = False
LIVE_TRANSACTION_SENDING = False

# The scanner already reports:
#   quoted_profit = ending_amount - starting_amount
#   estimated_cost = scanner's estimated round-trip cost
#   net_profit = quoted_profit - estimated_cost
#
# This simulator must not subtract an invented swap fee again.
DEFAULT_EXECUTION_DETERIORATION_BPS = 5.0
DEFAULT_FAILURE_RESERVE_USD = 0.00005
DEFAULT_NETWORK_FEE_RESERVE_USD = 0.00010
MINIMUM_CONSERVATIVE_NET_PROFIT_USD = 0.001
MINIMUM_GROSS_TO_COST_RATIO = 1.50
MAXIMUM_ESTIMATED_COST_PERCENT = 2.00

# Exact quote age is mandatory only for a true execution candidate.
MAXIMUM_QUOTE_AGE_SECONDS = 20.0

EXECUTION_RECOMMENDATION = "EXECUTE"
STATUS_NOT_EXECUTION_CANDIDATE = (
    "NOT_EXECUTION_CANDIDATE"
)
STATUS_DATA_INCOMPLETE = "DATA_INCOMPLETE"
STATUS_UNPROFITABLE = "ECONOMICALLY_UNPROFITABLE"
STATUS_SIMULATION_PASS = "SIMULATION_PASS"


def get_database_connection():
    connection = sqlite3.connect(
        DATABASE_FILE,
        timeout=30,
    )
    connection.row_factory = sqlite3.Row
    return connection


def current_timestamp():
    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def safe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def safe_bool(value):
    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        return bool(value)

    return str(value or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }


def initialize_fee_simulator():
    """
    Create a clean schema-aware audit table.

    The older phase-10 table is intentionally preserved.
    This revision writes to a new table so old assumptions
    do not contaminate the corrected statistics.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS
            fee_execution_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,

                cycle_id TEXT NOT NULL,
                mint TEXT,
                symbol TEXT NOT NULL,

                evaluation_scope TEXT NOT NULL,
                final_status TEXT NOT NULL,

                quote_successful INTEGER NOT NULL DEFAULT 0,
                eligible INTEGER NOT NULL DEFAULT 0,
                final_recommendation TEXT NOT NULL,
                risk_approved INTEGER NOT NULL DEFAULT 0,

                starting_amount REAL NOT NULL DEFAULT 0,
                ending_amount REAL NOT NULL DEFAULT 0,
                quoted_profit_usd REAL NOT NULL DEFAULT 0,
                scanner_estimated_cost_usd
                    REAL NOT NULL DEFAULT 0,
                scanner_net_profit_usd
                    REAL NOT NULL DEFAULT 0,

                network_fee_reserve_usd
                    REAL NOT NULL DEFAULT 0,
                execution_deterioration_usd
                    REAL NOT NULL DEFAULT 0,
                failure_reserve_usd
                    REAL NOT NULL DEFAULT 0,

                total_all_in_cost_usd
                    REAL NOT NULL DEFAULT 0,
                conservative_net_profit_usd
                    REAL NOT NULL DEFAULT 0,
                gross_to_cost_ratio
                    REAL NOT NULL DEFAULT 0,
                total_cost_percent
                    REAL NOT NULL DEFAULT 0,

                quote_timestamp TEXT,
                quote_age_seconds REAL,
                exact_quote_timestamp_available
                    INTEGER NOT NULL DEFAULT 0,

                required_data_complete
                    INTEGER NOT NULL DEFAULT 0,
                simulation_passed
                    INTEGER NOT NULL DEFAULT 0,

                missing_fields_json TEXT NOT NULL,
                blocked_reasons_json TEXT NOT NULL,
                explanation TEXT NOT NULL,

                operating_mode TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,

                UNIQUE(cycle_id, mint, symbol)
            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_fee_execution_cycle
            ON fee_execution_audit(cycle_id)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_fee_execution_scope
            ON fee_execution_audit(evaluation_scope)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_fee_execution_status
            ON fee_execution_audit(final_status)
            """
        )

        connection.commit()

    finally:
        connection.close()


def _first_present(result, fields):
    for field in fields:
        if field in result:
            return result.get(field), field

    return None, None


def _parse_timestamp(raw_value):
    if not raw_value:
        return None

    value = str(raw_value).strip()

    formats = (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%SZ",
    )

    for timestamp_format in formats:
        try:
            return datetime.strptime(
                value,
                timestamp_format,
            )
        except ValueError:
            continue

    return None


def _quote_timing(result):
    raw_timestamp, source = _first_present(
        result,
        (
            "quote_timestamp",
            "quoted_at",
            "quote_received_at",
        ),
    )

    parsed = _parse_timestamp(raw_timestamp)

    if parsed is None:
        return {
            "timestamp": None,
            "source": None,
            "age_seconds": None,
            "exact_available": False,
        }

    age_seconds = max(
        0.0,
        (
            datetime.now()
            - parsed
        ).total_seconds(),
    )

    return {
        "timestamp": str(raw_timestamp),
        "source": source,
        "age_seconds": age_seconds,
        "exact_available": True,
    }


def _network_fee_reserve(result):
    """
    Prefer a scanner/RPC supplied fee.

    Until transaction construction exists, use a tiny,
    configurable paper reserve rather than inventing a
    large percentage fee.
    """

    explicit_value, source = _first_present(
        result,
        (
            "network_fee_usd",
            "transaction_fee_usd",
            "priority_and_base_fee_usd",
        ),
    )

    explicit_fee = safe_float(explicit_value)

    if explicit_fee > 0:
        return explicit_fee, source

    environment_fee = safe_float(
        os.getenv(
            "FEE_SIMULATOR_NETWORK_FEE_USD",
            DEFAULT_NETWORK_FEE_RESERVE_USD,
        )
    )

    return max(0.0, environment_fee), (
        "FEE_SIMULATOR_NETWORK_FEE_USD"
    )


def _evaluation_scope(result):
    recommendation = str(
        result.get("final_recommendation")
        or ""
    ).strip().upper()

    eligible = safe_bool(
        result.get("eligible")
    )

    risk_approved = safe_bool(
        result.get("risk_approved")
    )

    if (
        recommendation == EXECUTION_RECOMMENDATION
        and eligible
        and risk_approved
    ):
        return "EXECUTION_CANDIDATE"

    return "INFORMATIONAL"


def simulate_execution_costs(
    result,
    cycle_id=None,
):
    initialize_fee_simulator()

    cycle_id = cycle_id or current_timestamp()
    scope = _evaluation_scope(result)

    quote_successful = safe_bool(
        result.get("quote_successful")
    )

    eligible = safe_bool(
        result.get("eligible")
    )

    recommendation = str(
        result.get("final_recommendation")
        or "SKIP"
    ).strip().upper()

    risk_approved = safe_bool(
        result.get("risk_approved")
    )

    missing_fields = []
    blocked_reasons = []

    starting_value, starting_source = (
        _first_present(
            result,
            (
                "starting_amount",
                "trade_notional_usd",
                "input_value_usd",
            ),
        )
    )

    ending_value, ending_source = (
        _first_present(
            result,
            (
                "ending_amount",
                "output_value_usd",
            ),
        )
    )

    quoted_value, quoted_source = (
        _first_present(
            result,
            (
                "quoted_profit",
                "gross_profit_usd",
            ),
        )
    )

    scanner_cost_value, scanner_cost_source = (
        _first_present(
            result,
            (
                "estimated_cost",
                "estimated_cost_usd",
                "scanner_estimated_cost_usd",
            ),
        )
    )

    net_profit_value, net_profit_source = (
        _first_present(
            result,
            (
                "net_profit",
                "net_profit_usd",
            ),
        )
    )

    if starting_source is None:
        missing_fields.append("starting_amount")

    if ending_source is None:
        missing_fields.append("ending_amount")

    if quoted_source is None:
        missing_fields.append("quoted_profit")

    if scanner_cost_source is None:
        missing_fields.append("estimated_cost")

    if net_profit_source is None:
        missing_fields.append("net_profit")

    starting_amount = safe_float(starting_value)
    ending_amount = safe_float(ending_value)
    quoted_profit = safe_float(quoted_value)
    scanner_estimated_cost = max(
        0.0,
        safe_float(scanner_cost_value),
    )
    scanner_net_profit = safe_float(
        net_profit_value
    )

    # Defensive consistency checks. They do not silently
    # replace scanner values; they block a true candidate.
    expected_quoted_profit = (
        ending_amount
        - starting_amount
    )

    expected_net_profit = (
        quoted_profit
        - scanner_estimated_cost
    )

    tolerance = 0.000001

    quote_math_consistent = (
        abs(
            quoted_profit
            - expected_quoted_profit
        )
        <= tolerance
    )

    net_math_consistent = (
        abs(
            scanner_net_profit
            - expected_net_profit
        )
        <= tolerance
    )

    if not quote_successful:
        blocked_reasons.append(
            "Scanner quote was unsuccessful."
        )

    if not quote_math_consistent:
        blocked_reasons.append(
            "Quoted profit does not match "
            "ending amount minus starting amount."
        )

    if not net_math_consistent:
        blocked_reasons.append(
            "Scanner net profit does not match "
            "quoted profit minus estimated cost."
        )

    timing = _quote_timing(result)

    if (
        scope == "EXECUTION_CANDIDATE"
        and not timing["exact_available"]
    ):
        missing_fields.append(
            "exact_quote_timestamp"
        )

    if (
        scope == "EXECUTION_CANDIDATE"
        and timing["age_seconds"] is not None
        and timing["age_seconds"]
        > MAXIMUM_QUOTE_AGE_SECONDS
    ):
        blocked_reasons.append(
            "Quote is stale for execution."
        )

    network_fee_reserve, network_fee_source = (
        _network_fee_reserve(result)
    )

    deterioration_bps = max(
        0.0,
        safe_float(
            result.get(
                "execution_deterioration_bps"
            )
            or DEFAULT_EXECUTION_DETERIORATION_BPS
        ),
    )

    execution_deterioration = (
        starting_amount
        * deterioration_bps
        / 10_000.0
    )

    failure_reserve = max(
        0.0,
        safe_float(
            result.get("failure_reserve_usd")
            or DEFAULT_FAILURE_RESERVE_USD
        ),
    )

    total_all_in_cost = (
        scanner_estimated_cost
        + network_fee_reserve
        + execution_deterioration
        + failure_reserve
    )

    conservative_net_profit = (
        quoted_profit
        - total_all_in_cost
    )

    gross_to_cost_ratio = (
        quoted_profit
        / total_all_in_cost
        if total_all_in_cost > 0
        else 0.0
    )

    total_cost_percent = (
        total_all_in_cost
        / starting_amount
        * 100.0
        if starting_amount > 0
        else 0.0
    )

    # For informational WATCH/SKIP rows, the scanner schema
    # itself is sufficient. Missing live-execution fields do
    # not count as incomplete.
    required_data_complete = (
        len(missing_fields) == 0
    )

    if scope == "INFORMATIONAL":
        final_status = (
            STATUS_NOT_EXECUTION_CANDIDATE
        )
        simulation_passed = False

        explanation = (
            "Informational cost audit only. "
            "The Decision/Risk pipeline did not produce "
            "an approved execution candidate."
        )

    else:
        if not required_data_complete:
            final_status = (
                STATUS_DATA_INCOMPLETE
            )
            simulation_passed = False

        elif conservative_net_profit < (
            MINIMUM_CONSERVATIVE_NET_PROFIT_USD
        ):
            blocked_reasons.append(
                "Conservative net profit is below "
                "the execution minimum."
            )
            final_status = STATUS_UNPROFITABLE
            simulation_passed = False

        elif gross_to_cost_ratio < (
            MINIMUM_GROSS_TO_COST_RATIO
        ):
            blocked_reasons.append(
                "Gross-profit-to-cost ratio is below "
                "the execution minimum."
            )
            final_status = STATUS_UNPROFITABLE
            simulation_passed = False

        elif total_cost_percent > (
            MAXIMUM_ESTIMATED_COST_PERCENT
        ):
            blocked_reasons.append(
                "All-in cost percentage exceeds "
                "the execution maximum."
            )
            final_status = STATUS_UNPROFITABLE
            simulation_passed = False

        elif blocked_reasons:
            final_status = STATUS_UNPROFITABLE
            simulation_passed = False

        else:
            final_status = STATUS_SIMULATION_PASS
            simulation_passed = True

        explanation = (
            "All schema, freshness, consistency, "
            "cost, and profitability gates passed."
            if simulation_passed
            else " | ".join(
                blocked_reasons
                or [
                    "Required execution data is missing."
                ]
            )
        )

    return {
        "cycle_id": cycle_id,
        "evaluation_scope": scope,
        "final_status": final_status,

        "quote_successful": int(
            quote_successful
        ),
        "eligible": int(eligible),
        "final_recommendation": recommendation,
        "risk_approved": int(risk_approved),

        "starting_amount": starting_amount,
        "ending_amount": ending_amount,
        "quoted_profit_usd": quoted_profit,
        "scanner_estimated_cost_usd": (
            scanner_estimated_cost
        ),
        "scanner_net_profit_usd": (
            scanner_net_profit
        ),

        "network_fee_reserve_usd": (
            network_fee_reserve
        ),
        "execution_deterioration_usd": (
            execution_deterioration
        ),
        "failure_reserve_usd": (
            failure_reserve
        ),

        "total_all_in_cost_usd": (
            total_all_in_cost
        ),
        "conservative_net_profit_usd": (
            conservative_net_profit
        ),
        "gross_to_cost_ratio": (
            gross_to_cost_ratio
        ),
        "total_cost_percent": (
            total_cost_percent
        ),

        "quote_timestamp": timing["timestamp"],
        "quote_age_seconds": timing["age_seconds"],
        "exact_quote_timestamp_available": int(
            timing["exact_available"]
        ),

        "required_data_complete": int(
            required_data_complete
        ),
        "simulation_passed": int(
            simulation_passed
        ),

        "missing_fields": missing_fields,
        "blocked_reasons": blocked_reasons,
        "explanation": explanation,

        "field_sources": {
            "starting_amount": starting_source,
            "ending_amount": ending_source,
            "quoted_profit": quoted_source,
            "estimated_cost": scanner_cost_source,
            "net_profit": net_profit_source,
            "network_fee": network_fee_source,
            "quote_timestamp": timing["source"],
        },
    }


def save_fee_simulation(
    result,
    simulation,
):
    initialize_fee_simulator()

    connection = get_database_connection()
    cursor = connection.cursor()

    symbol = str(
        result.get("token")
        or result.get("symbol")
        or "UNKNOWN"
    ).strip().upper()

    mint = str(
        result.get("mint")
        or result.get("token_mint")
        or ""
    ).strip() or None

    try:
        cursor.execute(
            """
            INSERT INTO fee_execution_audit (
                cycle_id,
                mint,
                symbol,
                evaluation_scope,
                final_status,
                quote_successful,
                eligible,
                final_recommendation,
                risk_approved,
                starting_amount,
                ending_amount,
                quoted_profit_usd,
                scanner_estimated_cost_usd,
                scanner_net_profit_usd,
                network_fee_reserve_usd,
                execution_deterioration_usd,
                failure_reserve_usd,
                total_all_in_cost_usd,
                conservative_net_profit_usd,
                gross_to_cost_ratio,
                total_cost_percent,
                quote_timestamp,
                quote_age_seconds,
                exact_quote_timestamp_available,
                required_data_complete,
                simulation_passed,
                missing_fields_json,
                blocked_reasons_json,
                explanation,
                operating_mode,
                created_at,
                updated_at
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            ON CONFLICT(cycle_id, mint, symbol)
            DO UPDATE SET
                evaluation_scope =
                    excluded.evaluation_scope,
                final_status =
                    excluded.final_status,
                quote_successful =
                    excluded.quote_successful,
                eligible =
                    excluded.eligible,
                final_recommendation =
                    excluded.final_recommendation,
                risk_approved =
                    excluded.risk_approved,
                starting_amount =
                    excluded.starting_amount,
                ending_amount =
                    excluded.ending_amount,
                quoted_profit_usd =
                    excluded.quoted_profit_usd,
                scanner_estimated_cost_usd =
                    excluded.scanner_estimated_cost_usd,
                scanner_net_profit_usd =
                    excluded.scanner_net_profit_usd,
                network_fee_reserve_usd =
                    excluded.network_fee_reserve_usd,
                execution_deterioration_usd =
                    excluded.execution_deterioration_usd,
                failure_reserve_usd =
                    excluded.failure_reserve_usd,
                total_all_in_cost_usd =
                    excluded.total_all_in_cost_usd,
                conservative_net_profit_usd =
                    excluded.conservative_net_profit_usd,
                gross_to_cost_ratio =
                    excluded.gross_to_cost_ratio,
                total_cost_percent =
                    excluded.total_cost_percent,
                quote_timestamp =
                    excluded.quote_timestamp,
                quote_age_seconds =
                    excluded.quote_age_seconds,
                exact_quote_timestamp_available =
                    excluded.exact_quote_timestamp_available,
                required_data_complete =
                    excluded.required_data_complete,
                simulation_passed =
                    excluded.simulation_passed,
                missing_fields_json =
                    excluded.missing_fields_json,
                blocked_reasons_json =
                    excluded.blocked_reasons_json,
                explanation =
                    excluded.explanation,
                operating_mode =
                    excluded.operating_mode,
                updated_at =
                    excluded.updated_at
            """,
            (
                simulation["cycle_id"],
                mint,
                symbol,
                simulation["evaluation_scope"],
                simulation["final_status"],
                simulation["quote_successful"],
                simulation["eligible"],
                simulation[
                    "final_recommendation"
                ],
                simulation["risk_approved"],
                simulation["starting_amount"],
                simulation["ending_amount"],
                simulation[
                    "quoted_profit_usd"
                ],
                simulation[
                    "scanner_estimated_cost_usd"
                ],
                simulation[
                    "scanner_net_profit_usd"
                ],
                simulation[
                    "network_fee_reserve_usd"
                ],
                simulation[
                    "execution_deterioration_usd"
                ],
                simulation[
                    "failure_reserve_usd"
                ],
                simulation[
                    "total_all_in_cost_usd"
                ],
                simulation[
                    "conservative_net_profit_usd"
                ],
                simulation[
                    "gross_to_cost_ratio"
                ],
                simulation[
                    "total_cost_percent"
                ],
                simulation["quote_timestamp"],
                simulation["quote_age_seconds"],
                simulation[
                    "exact_quote_timestamp_available"
                ],
                simulation[
                    "required_data_complete"
                ],
                simulation["simulation_passed"],
                json.dumps(
                    simulation["missing_fields"],
                    sort_keys=True,
                ),
                json.dumps(
                    simulation["blocked_reasons"],
                    sort_keys=True,
                ),
                simulation["explanation"],
                OPERATING_MODE,
                current_timestamp(),
                current_timestamp(),
            ),
        )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def simulate_cycle(
    results,
    cycle_id=None,
):
    initialize_fee_simulator()

    cycle_id = cycle_id or current_timestamp()

    if not results:
        return {
            "cycle_id": cycle_id,
            "evaluated": 0,
            "execution_candidates": 0,
            "informational": 0,
            "passed": 0,
            "blocked": 0,
            "incomplete": 0,
            "results": results,
        }

    execution_candidates = 0
    informational = 0
    passed = 0
    blocked = 0
    incomplete = 0

    for result in results:
        simulation = simulate_execution_costs(
            result=result,
            cycle_id=cycle_id,
        )

        save_fee_simulation(
            result=result,
            simulation=simulation,
        )

        result["fee_evaluation_scope"] = (
            simulation["evaluation_scope"]
        )
        result["fee_simulation_status"] = (
            simulation["final_status"]
        )
        result["fee_simulation_passed"] = (
            simulation["simulation_passed"]
        )
        result[
            "conservative_net_profit_usd"
        ] = simulation[
            "conservative_net_profit_usd"
        ]
        result[
            "total_estimated_execution_cost_usd"
        ] = simulation[
            "total_all_in_cost_usd"
        ]
        result["profit_to_cost_ratio"] = (
            simulation["gross_to_cost_ratio"]
        )
        result[
            "fee_simulation_missing_fields"
        ] = simulation["missing_fields"]
        result[
            "fee_simulation_blocked_reasons"
        ] = simulation["blocked_reasons"]

        if (
            simulation["evaluation_scope"]
            == "EXECUTION_CANDIDATE"
        ):
            execution_candidates += 1

            if simulation["simulation_passed"]:
                passed += 1
            else:
                blocked += 1

            if (
                simulation["final_status"]
                == STATUS_DATA_INCOMPLETE
            ):
                incomplete += 1

        else:
            informational += 1

    return {
        "cycle_id": cycle_id,
        "evaluated": len(results),
        "execution_candidates": (
            execution_candidates
        ),
        "informational": informational,
        "passed": passed,
        "blocked": blocked,
        "incomplete": incomplete,
        "results": results,
    }


def get_fee_simulation_summary():
    initialize_fee_simulator()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_audits,
                COUNT(DISTINCT cycle_id)
                    AS audit_cycles,

                SUM(
                    CASE
                        WHEN evaluation_scope =
                            'EXECUTION_CANDIDATE'
                        THEN 1
                        ELSE 0
                    END
                ) AS execution_candidates,

                SUM(
                    CASE
                        WHEN evaluation_scope =
                            'INFORMATIONAL'
                        THEN 1
                        ELSE 0
                    END
                ) AS informational_audits,

                SUM(simulation_passed)
                    AS passed,

                SUM(
                    CASE
                        WHEN evaluation_scope =
                            'EXECUTION_CANDIDATE'
                         AND simulation_passed = 0
                        THEN 1
                        ELSE 0
                    END
                ) AS blocked,

                SUM(
                    CASE
                        WHEN final_status =
                            'DATA_INCOMPLETE'
                        THEN 1
                        ELSE 0
                    END
                ) AS incomplete_candidates,

                AVG(total_all_in_cost_usd)
                    AS average_total_cost_usd,

                AVG(conservative_net_profit_usd)
                    AS average_conservative_net_profit_usd,

                MAX(conservative_net_profit_usd)
                    AS best_conservative_net_profit_usd,

                AVG(gross_to_cost_ratio)
                    AS average_profit_to_cost_ratio,

                MAX(updated_at)
                    AS last_updated_at

            FROM fee_execution_audit
            """
        )

        row = cursor.fetchone()

    finally:
        connection.close()

    result = dict(row) if row else {}

    integer_fields = (
        "total_audits",
        "audit_cycles",
        "execution_candidates",
        "informational_audits",
        "passed",
        "blocked",
        "incomplete_candidates",
    )

    float_fields = (
        "average_total_cost_usd",
        "average_conservative_net_profit_usd",
        "best_conservative_net_profit_usd",
        "average_profit_to_cost_ratio",
    )

    for field in integer_fields:
        result[field] = safe_int(
            result.get(field)
        )

    for field in float_fields:
        result[field] = safe_float(
            result.get(field)
        )

    result.setdefault(
        "last_updated_at",
        None,
    )

    return result


if __name__ == "__main__":
    initialize_fee_simulator()
    summary = get_fee_simulation_summary()

    print(
        "\nSchema-Aware Fee Simulator ready."
    )
    print(
        "Operating mode: "
        f"{OPERATING_MODE}"
    )
    print(
        "Live signing: "
        f"{LIVE_TRANSACTION_SIGNING}"
    )
    print(
        "Live sending: "
        f"{LIVE_TRANSACTION_SENDING}"
    )
    print(
        "Corrected audit rows: "
        f"{summary['total_audits']:,}"
    )