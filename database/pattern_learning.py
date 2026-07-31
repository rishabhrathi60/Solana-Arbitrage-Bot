import math
import sqlite3
from datetime import datetime
from pathlib import Path

from database.opportunity_history import (
    create_opportunity_history_table,
)


DATABASE = (
    Path(__file__).resolve().parent
    / "trades.db"
)


# ---------------------------------------------------------
# Pattern Learning settings
# ---------------------------------------------------------

RECENT_WINDOW_DAYS = 7
CONFIDENCE_REFERENCE_QUOTES = 40

PROFIT_REFERENCE_USD = 0.05
VOLATILITY_REFERENCE_USD = 0.05
DOWNSIDE_REFERENCE_USD = 0.10

QUOTE_RELIABILITY_WEIGHT = 0.18
PROFITABLE_RATE_WEIGHT = 0.18
ELIGIBLE_RATE_WEIGHT = 0.12
AVERAGE_PROFIT_WEIGHT = 0.18
RECENT_PROFIT_WEIGHT = 0.14
TREND_WEIGHT = 0.10
STABILITY_WEIGHT = 0.10


def get_connection():
    """
    Create a SQLite connection with dictionary-like rows.
    """

    connection = sqlite3.connect(
        DATABASE,
        timeout=30,
    )
    connection.row_factory = sqlite3.Row
    return connection


def current_timestamp():
    """
    Return the current local timestamp.
    """

    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def safe_float(value):
    """
    Convert a value to float safely.
    """

    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value):
    """
    Convert a value to integer safely.
    """

    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def clamp(
    value,
    minimum=0.0,
    maximum=100.0,
):
    """
    Restrict a value to a safe range.
    """

    return max(
        minimum,
        min(maximum, safe_float(value)),
    )


def get_required_pattern_columns():
    """
    Return the columns required by the current pattern schema.
    """

    return {
        "mint",
        "symbol",
        "total_scans",
        "successful_quotes",
        "quote_errors",
        "profitable_quotes",
        "eligible_quotes",
        "quote_success_rate",
        "profitable_rate",
        "eligible_rate",
        "average_profit",
        "recent_average_profit",
        "best_profit",
        "worst_profit",
        "profit_volatility",
        "downside_severity",
        "average_market_score",
        "current_intelligence_score",
        "current_ai_opportunity_score",
        "current_opportunity_probability",
        "current_confidence",
        "current_downside_risk",
        "profit_score",
        "recent_profit_score",
        "trend_score",
        "stability_score",
        "sample_confidence",
        "pattern_score",
        "last_seen",
        "updated_at",
    }


def create_current_pattern_table(cursor):
    """
    Create the current rebuildable pattern table and indexes.
    """

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS
        pattern_learning (
            mint TEXT PRIMARY KEY,
            symbol TEXT NOT NULL,

            total_scans INTEGER NOT NULL DEFAULT 0,
            successful_quotes INTEGER NOT NULL DEFAULT 0,
            quote_errors INTEGER NOT NULL DEFAULT 0,
            profitable_quotes INTEGER NOT NULL DEFAULT 0,
            eligible_quotes INTEGER NOT NULL DEFAULT 0,

            quote_success_rate REAL NOT NULL DEFAULT 0,
            profitable_rate REAL NOT NULL DEFAULT 0,
            eligible_rate REAL NOT NULL DEFAULT 0,

            average_profit REAL NOT NULL DEFAULT 0,
            recent_average_profit REAL NOT NULL DEFAULT 0,
            best_profit REAL NOT NULL DEFAULT 0,
            worst_profit REAL NOT NULL DEFAULT 0,
            profit_volatility REAL NOT NULL DEFAULT 0,
            downside_severity REAL NOT NULL DEFAULT 0,

            average_market_score REAL NOT NULL DEFAULT 0,

            current_intelligence_score
                REAL NOT NULL DEFAULT 0,

            current_ai_opportunity_score
                REAL NOT NULL DEFAULT 0,

            current_opportunity_probability
                REAL NOT NULL DEFAULT 0,

            current_confidence
                REAL NOT NULL DEFAULT 0,

            current_downside_risk
                REAL NOT NULL DEFAULT 0,

            profit_score REAL NOT NULL DEFAULT 0,
            recent_profit_score REAL NOT NULL DEFAULT 0,
            trend_score REAL NOT NULL DEFAULT 50,
            stability_score REAL NOT NULL DEFAULT 0,
            sample_confidence REAL NOT NULL DEFAULT 0,
            pattern_score REAL NOT NULL DEFAULT 0,

            last_seen TEXT,
            updated_at TEXT NOT NULL
        )
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_pattern_learning_score
        ON pattern_learning(pattern_score)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_pattern_learning_confidence
        ON pattern_learning(sample_confidence)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_pattern_learning_profit
        ON pattern_learning(average_profit)
        """
    )


def initialize_pattern_learning():
    """
    Create or automatically migrate the pattern summary table.

    Pattern learning is derived from permanent opportunity
    history. If an older incompatible table is detected, only the
    derived table is dropped and recreated. Original scan history
    remains untouched.
    """

    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT name
            FROM sqlite_master
            WHERE type = 'table'
              AND name = 'pattern_learning'
            """
        )

        table_exists = (
            cursor.fetchone() is not None
        )

        if table_exists:
            cursor.execute(
                """
                PRAGMA table_info(
                    pattern_learning
                )
                """
            )

            existing_columns = {
                row["name"]
                for row in cursor.fetchall()
            }

            required_columns = (
                get_required_pattern_columns()
            )

            if not required_columns.issubset(
                existing_columns
            ):
                print(
                    "Older pattern_learning schema "
                    "detected. Rebuilding derived "
                    "pattern table safely."
                )

                cursor.execute(
                    """
                    DROP TABLE IF EXISTS
                    pattern_learning
                    """
                )

        create_current_pattern_table(
            cursor
        )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()


def ensure_pattern_schema():
    """
    Ensure the current self-migrating pattern schema exists.
    """

    initialize_pattern_learning()


def calculate_profit_score(profit):
    """
    Convert profit into a smooth 0-to-100 score.
    """

    scaled_profit = (
        safe_float(profit)
        / max(
            PROFIT_REFERENCE_USD,
            0.000001,
        )
    )

    try:
        score = (
            100.0
            / (
                1.0
                + math.exp(
                    -2.0 * scaled_profit
                )
            )
        )
    except OverflowError:
        score = (
            100.0
            if scaled_profit > 0
            else 0.0
        )

    return clamp(score)


def calculate_sample_confidence(
    successful_quotes,
):
    """
    Calculate evidence confidence from valid quote count.
    """

    successful_quotes = max(
        0,
        safe_int(successful_quotes),
    )

    if successful_quotes == 0:
        return 0.0

    return clamp(
        (
            1.0
            - math.exp(
                -successful_quotes
                / CONFIDENCE_REFERENCE_QUOTES
            )
        )
        * 100.0
    )


def calculate_pattern_record(row):
    """
    Calculate hardened pattern features for one token.
    """

    total_scans = safe_int(
        row.get("total_scans")
    )

    successful_quotes = safe_int(
        row.get("successful_quotes")
    )

    quote_errors = max(
        total_scans - successful_quotes,
        0,
    )

    profitable_quotes = safe_int(
        row.get("profitable_quotes")
    )

    eligible_quotes = safe_int(
        row.get("eligible_quotes")
    )

    average_profit = safe_float(
        row.get("average_profit")
    )

    recent_average_profit = safe_float(
        row.get("recent_average_profit")
    )

    best_profit = safe_float(
        row.get("best_profit")
    )

    worst_profit = safe_float(
        row.get("worst_profit")
    )

    average_profit_squared = safe_float(
        row.get("average_profit_squared")
    )

    variance = max(
        0.0,
        average_profit_squared
        - average_profit ** 2,
    )

    profit_volatility = math.sqrt(
        variance
    )

    quote_success_rate = (
        successful_quotes
        / total_scans
        * 100.0
        if total_scans > 0
        else 0.0
    )

    profitable_rate = (
        profitable_quotes
        / successful_quotes
        * 100.0
        if successful_quotes > 0
        else 0.0
    )

    eligible_rate = (
        eligible_quotes
        / successful_quotes
        * 100.0
        if successful_quotes > 0
        else 0.0
    )

    downside_severity = clamp(
        abs(min(worst_profit, 0.0))
        / max(
            DOWNSIDE_REFERENCE_USD,
            0.000001,
        )
        * 100.0
    )

    stability_score = clamp(
        100.0
        - (
            profit_volatility
            / max(
                VOLATILITY_REFERENCE_USD,
                0.000001,
            )
            * 100.0
        )
    )

    profit_score = (
        calculate_profit_score(
            average_profit
        )
    )

    recent_profit_score = (
        calculate_profit_score(
            recent_average_profit
        )
    )

    profit_change = (
        recent_average_profit
        - average_profit
    )

    trend_score = clamp(
        50.0
        + (
            profit_change
            / max(
                PROFIT_REFERENCE_USD,
                0.000001,
            )
            * 50.0
        )
    )

    sample_confidence = (
        calculate_sample_confidence(
            successful_quotes
        )
    )

    raw_pattern_score = (
        quote_success_rate
        * QUOTE_RELIABILITY_WEIGHT
        + profitable_rate
        * PROFITABLE_RATE_WEIGHT
        + eligible_rate
        * ELIGIBLE_RATE_WEIGHT
        + profit_score
        * AVERAGE_PROFIT_WEIGHT
        + recent_profit_score
        * RECENT_PROFIT_WEIGHT
        + trend_score
        * TREND_WEIGHT
        + stability_score
        * STABILITY_WEIGHT
    )

    confidence_ratio = (
        sample_confidence / 100.0
    )

    confidence_adjusted_score = (
        50.0
        * (1.0 - confidence_ratio)
        + raw_pattern_score
        * confidence_ratio
    )

    risk_penalty = (
        downside_severity
        * (
            0.05
            + confidence_ratio * 0.15
        )
    )

    maximum_allowed_score = (
        60.0
        + sample_confidence * 0.40
    )

    pattern_score = clamp(
        confidence_adjusted_score
        - risk_penalty,
        maximum=maximum_allowed_score,
    )

    return {
        "mint": row.get("mint"),
        "symbol": (
            row.get("symbol")
            or "UNKNOWN"
        ),
        "total_scans": total_scans,
        "successful_quotes": (
            successful_quotes
        ),
        "quote_errors": quote_errors,
        "profitable_quotes": (
            profitable_quotes
        ),
        "eligible_quotes": eligible_quotes,
        "quote_success_rate": (
            quote_success_rate
        ),
        "profitable_rate": profitable_rate,
        "eligible_rate": eligible_rate,
        "average_profit": average_profit,
        "recent_average_profit": (
            recent_average_profit
        ),
        "best_profit": best_profit,
        "worst_profit": worst_profit,
        "profit_volatility": (
            profit_volatility
        ),
        "downside_severity": (
            downside_severity
        ),
        "average_market_score": (
            safe_float(
                row.get(
                    "average_market_score"
                )
            )
        ),
        "current_intelligence_score": (
            safe_float(
                row.get(
                    "current_intelligence_score"
                )
            )
        ),
        "current_ai_opportunity_score": (
            safe_float(
                row.get(
                    "current_ai_opportunity_score"
                )
            )
        ),
        "current_opportunity_probability": (
            safe_float(
                row.get(
                    "current_opportunity_probability"
                )
            )
        ),
        "current_confidence": safe_float(
            row.get("current_confidence")
        ),
        "current_downside_risk": safe_float(
            row.get(
                "current_downside_risk"
            )
        ),
        "profit_score": profit_score,
        "recent_profit_score": (
            recent_profit_score
        ),
        "trend_score": trend_score,
        "stability_score": stability_score,
        "sample_confidence": (
            sample_confidence
        ),
        "pattern_score": pattern_score,
        "last_seen": row.get("last_seen"),
        "updated_at": current_timestamp(),
    }


def load_pattern_aggregates():
    """
    Aggregate permanent history by mint.

    The existing history table stores token symbols. A symbol is
    mapped only when token_universe contains exactly one matching
    mint. Ambiguous duplicate symbols are excluded deliberately.
    """

    create_opportunity_history_table()

    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            f"""
            WITH unique_symbols AS (
                SELECT
                    UPPER(symbol) AS symbol_key,
                    MIN(mint) AS mint
                FROM token_universe
                WHERE symbol IS NOT NULL
                  AND TRIM(symbol) <> ''
                GROUP BY UPPER(symbol)
                HAVING COUNT(DISTINCT mint) = 1
            )
            SELECT
                unique_symbols.mint AS mint,
                MAX(
                    opportunity_history.token
                ) AS symbol,

                COUNT(*)
                    AS total_scans,

                SUM(
                    CASE
                        WHEN opportunity_history
                             .quote_successful = 1
                        THEN 1
                        ELSE 0
                    END
                ) AS successful_quotes,

                SUM(
                    CASE
                        WHEN opportunity_history
                             .quote_successful = 1
                         AND opportunity_history
                             .net_profit > 0
                        THEN 1
                        ELSE 0
                    END
                ) AS profitable_quotes,

                SUM(
                    CASE
                        WHEN opportunity_history
                             .quote_successful = 1
                         AND opportunity_history
                             .eligible = 1
                        THEN 1
                        ELSE 0
                    END
                ) AS eligible_quotes,

                AVG(
                    CASE
                        WHEN opportunity_history
                             .quote_successful = 1
                        THEN opportunity_history
                             .net_profit
                    END
                ) AS average_profit,

                AVG(
                    CASE
                        WHEN opportunity_history
                             .quote_successful = 1
                        THEN opportunity_history
                             .net_profit
                             * opportunity_history
                               .net_profit
                    END
                ) AS average_profit_squared,

                AVG(
                    CASE
                        WHEN opportunity_history
                             .quote_successful = 1
                         AND datetime(
                                opportunity_history
                                .scanned_at
                             ) >= datetime(
                                'now',
                                '-{RECENT_WINDOW_DAYS} days'
                             )
                        THEN opportunity_history
                             .net_profit
                    END
                ) AS recent_average_profit,

                MAX(
                    CASE
                        WHEN opportunity_history
                             .quote_successful = 1
                        THEN opportunity_history
                             .net_profit
                    END
                ) AS best_profit,

                MIN(
                    CASE
                        WHEN opportunity_history
                             .quote_successful = 1
                        THEN opportunity_history
                             .net_profit
                    END
                ) AS worst_profit,

                AVG(
                    CASE
                        WHEN opportunity_history
                             .quote_successful = 1
                        THEN opportunity_history
                             .market_score
                    END
                ) AS average_market_score,

                COALESCE(
                    token_intelligence
                    .intelligence_score,
                    0
                ) AS current_intelligence_score,

                COALESCE(
                    ai_rankings
                    .ai_opportunity_score,
                    0
                ) AS current_ai_opportunity_score,

                COALESCE(
                    ai_rankings
                    .opportunity_probability,
                    0
                ) AS current_opportunity_probability,

                COALESCE(
                    ai_rankings
                    .combined_confidence,
                    0
                ) AS current_confidence,

                COALESCE(
                    ai_rankings
                    .downside_risk_score,
                    0
                ) AS current_downside_risk,

                MAX(
                    opportunity_history
                    .scanned_at
                ) AS last_seen

            FROM opportunity_history

            INNER JOIN unique_symbols
                ON UPPER(
                    opportunity_history.token
                ) = unique_symbols.symbol_key

            LEFT JOIN token_intelligence
                ON token_intelligence.mint =
                   unique_symbols.mint

            LEFT JOIN ai_rankings
                ON ai_rankings.mint =
                   unique_symbols.mint

            GROUP BY unique_symbols.mint
            """
        )

        return [
            dict(row)
            for row in cursor.fetchall()
        ]

    finally:
        connection.close()


def rebuild_pattern_learning():
    """
    Rebuild patterns from permanent history.

    Running this repeatedly produces identical statistics until
    new opportunity-history observations are added.
    """

    ensure_pattern_schema()

    aggregates = load_pattern_aggregates()

    records = [
        calculate_pattern_record(row)
        for row in aggregates
    ]

    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            DELETE FROM pattern_learning
            """
        )

        for record in records:
            cursor.execute(
                """
                INSERT INTO pattern_learning (
                    mint,
                    symbol,
                    total_scans,
                    successful_quotes,
                    quote_errors,
                    profitable_quotes,
                    eligible_quotes,
                    quote_success_rate,
                    profitable_rate,
                    eligible_rate,
                    average_profit,
                    recent_average_profit,
                    best_profit,
                    worst_profit,
                    profit_volatility,
                    downside_severity,
                    average_market_score,
                    current_intelligence_score,
                    current_ai_opportunity_score,
                    current_opportunity_probability,
                    current_confidence,
                    current_downside_risk,
                    profit_score,
                    recent_profit_score,
                    trend_score,
                    stability_score,
                    sample_confidence,
                    pattern_score,
                    last_seen,
                    updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    record["mint"],
                    record["symbol"],
                    record["total_scans"],
                    record[
                        "successful_quotes"
                    ],
                    record["quote_errors"],
                    record[
                        "profitable_quotes"
                    ],
                    record["eligible_quotes"],
                    record[
                        "quote_success_rate"
                    ],
                    record["profitable_rate"],
                    record["eligible_rate"],
                    record["average_profit"],
                    record[
                        "recent_average_profit"
                    ],
                    record["best_profit"],
                    record["worst_profit"],
                    record[
                        "profit_volatility"
                    ],
                    record[
                        "downside_severity"
                    ],
                    record[
                        "average_market_score"
                    ],
                    record[
                        "current_intelligence_score"
                    ],
                    record[
                        "current_ai_opportunity_score"
                    ],
                    record[
                        "current_opportunity_probability"
                    ],
                    record[
                        "current_confidence"
                    ],
                    record[
                        "current_downside_risk"
                    ],
                    record["profit_score"],
                    record[
                        "recent_profit_score"
                    ],
                    record["trend_score"],
                    record["stability_score"],
                    record[
                        "sample_confidence"
                    ],
                    record["pattern_score"],
                    record["last_seen"],
                    record["updated_at"],
                ),
            )

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    return {
        "patterns_rebuilt": len(records),
        "updated_at": current_timestamp(),
    }


def update_learning(results=None):
    """
    Backward-compatible scan-cycle entry point.

    auto_scanner already saves results into opportunity_history
    before calling this function, so rebuilding avoids duplicates.
    """

    return rebuild_pattern_learning()


def get_best_patterns(
    limit=25,
    minimum_scans=5,
    minimum_confidence=0,
):
    """
    Return the strongest learned token patterns.
    """

    ensure_pattern_schema()

    limit = max(1, int(limit))

    minimum_scans = max(
        1,
        int(minimum_scans),
    )

    minimum_confidence = clamp(
        minimum_confidence
    )

    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM pattern_learning
            WHERE successful_quotes >= ?
              AND sample_confidence >= ?
            ORDER BY
                pattern_score DESC,
                average_profit DESC,
                profitable_rate DESC,
                quote_success_rate DESC,
                sample_confidence DESC,
                symbol ASC
            LIMIT ?
            """,
            (
                minimum_scans,
                minimum_confidence,
                limit,
            ),
        )

        return [
            dict(row)
            for row in cursor.fetchall()
        ]

    finally:
        connection.close()


def get_pattern_learning_summary():
    """
    Return overall pattern-engine statistics.
    """

    ensure_pattern_schema()

    connection = get_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                COUNT(*)
                    AS total_patterns,

                SUM(total_scans)
                    AS total_observations,

                SUM(successful_quotes)
                    AS successful_quotes,

                AVG(pattern_score)
                    AS average_pattern_score,

                MAX(pattern_score)
                    AS highest_pattern_score,

                AVG(sample_confidence)
                    AS average_sample_confidence,

                AVG(quote_success_rate)
                    AS average_quote_success_rate,

                AVG(profitable_rate)
                    AS average_profitable_rate,

                AVG(average_profit)
                    AS average_token_profit,

                SUM(
                    CASE
                        WHEN average_profit > 0
                        THEN 1
                        ELSE 0
                    END
                ) AS positive_average_tokens,

                SUM(
                    CASE
                        WHEN trend_score > 55
                        THEN 1
                        ELSE 0
                    END
                ) AS improving_tokens,

                MAX(updated_at)
                    AS last_updated_at

            FROM pattern_learning
            """
        )

        row = cursor.fetchone()

    finally:
        connection.close()

    if not row:
        return {}

    result = dict(row)

    for key in (
        "total_patterns",
        "total_observations",
        "successful_quotes",
        "positive_average_tokens",
        "improving_tokens",
    ):
        result[key] = safe_int(
            result.get(key)
        )

    for key in (
        "average_pattern_score",
        "highest_pattern_score",
        "average_sample_confidence",
        "average_quote_success_rate",
        "average_profitable_rate",
        "average_token_profit",
    ):
        result[key] = safe_float(
            result.get(key)
        )

    return result


if __name__ == "__main__":
    rebuild_result = (
        rebuild_pattern_learning()
    )

    print(
        "\nPattern Learning Engine rebuilt."
    )

    print(
        "Patterns rebuilt: "
        f"{rebuild_result['patterns_rebuilt']:,}"
    )

    print(
        "Updated at: "
        f"{rebuild_result['updated_at']}"
    )

    top_patterns = get_best_patterns(
        limit=10,
        minimum_scans=1,
    )

    print("\nTop learned patterns:")

    for position, pattern in enumerate(
        top_patterns,
        start=1,
    ):
        print(
            f"{position}. "
            f"{pattern['symbol']} — "
            f"pattern "
            f"{pattern['pattern_score']:.2f}/100, "
            f"average profit "
            f"${pattern['average_profit']:.6f}, "
            f"quote success "
            f"{pattern['quote_success_rate']:.1f}%, "
            f"profitable "
            f"{pattern['profitable_rate']:.1f}%, "
            f"confidence "
            f"{pattern['sample_confidence']:.2f}/100"
        )
