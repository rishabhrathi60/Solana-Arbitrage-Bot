import math
import sqlite3
from datetime import datetime
from pathlib import Path

from database.opportunity_history import (
    create_opportunity_history_table,
)
from database.token_metrics import (
    get_liquid_tokens,
)


DATABASE_FILE = (
    Path(__file__).resolve().parent
    / "trades.db"
)


# ---------------------------------------------------------
# Intelligence model settings
# ---------------------------------------------------------

# Final exploitation-score weights.
MARKET_QUALITY_WEIGHT = 0.25
PROFITABILITY_WEIGHT = 0.22
QUOTE_RELIABILITY_WEIGHT = 0.15
ELIGIBILITY_WEIGHT = 0.12
RECENT_PERFORMANCE_WEIGHT = 0.16
DOWNSIDE_SAFETY_WEIGHT = 0.10

# Controls how strongly limited history reduces confidence.
CONFIDENCE_FULL_STRENGTH_SCANS = 30

# New and under-tested tokens receive an exploration bonus.
MAXIMUM_EXPLORATION_BONUS = 12.0
EXPLORATION_DECAY_SCANS = 20

# Historical profit normalization.
#
# A net profit near this amount receives a strong positive
# profitability score. Change carefully after collecting
# substantially more data.
PROFIT_REFERENCE_USD = 0.05

# Large negative observations trigger stronger risk penalties.
LOSS_REFERENCE_USD = 0.10

# The token-metrics query supports a large result limit.
MAXIMUM_MARKET_TOKENS = 20_000


def get_database_connection():
    """
    Create a SQLite connection that returns dictionary-like rows.
    """

    connection = sqlite3.connect(
        DATABASE_FILE,
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


def clamp(value, minimum=0.0, maximum=100.0):
    """
    Restrict a numeric value to a safe range.
    """

    return max(
        minimum,
        min(maximum, float(value)),
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


def initialize_token_intelligence_table():
    """
    Create the token-intelligence table and indexes.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS token_intelligence (
                mint TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                name TEXT,

                intelligence_score REAL NOT NULL DEFAULT 0,
                exploitation_score REAL NOT NULL DEFAULT 0,
                exploration_bonus REAL NOT NULL DEFAULT 0,
                confidence_score REAL NOT NULL DEFAULT 0,

                market_quality_score REAL NOT NULL DEFAULT 0,
                profitability_score REAL NOT NULL DEFAULT 0,
                quote_reliability_score REAL NOT NULL DEFAULT 0,
                eligibility_score REAL NOT NULL DEFAULT 0,
                recent_performance_score REAL NOT NULL DEFAULT 0,
                downside_safety_score REAL NOT NULL DEFAULT 0,

                total_scans INTEGER NOT NULL DEFAULT 0,
                successful_quotes INTEGER NOT NULL DEFAULT 0,
                quote_errors INTEGER NOT NULL DEFAULT 0,
                eligible_scans INTEGER NOT NULL DEFAULT 0,
                profitable_scans INTEGER NOT NULL DEFAULT 0,

                average_net_profit REAL NOT NULL DEFAULT 0,
                recent_average_net_profit REAL NOT NULL DEFAULT 0,
                best_net_profit REAL NOT NULL DEFAULT 0,
                worst_net_profit REAL NOT NULL DEFAULT 0,

                quote_success_rate REAL NOT NULL DEFAULT 0,
                eligible_scan_rate REAL NOT NULL DEFAULT 0,
                profitable_scan_rate REAL NOT NULL DEFAULT 0,

                last_scanned_at TEXT,
                intelligence_updated_at TEXT NOT NULL,

                FOREIGN KEY (mint)
                    REFERENCES token_universe(mint)
            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_token_intelligence_score
            ON token_intelligence(intelligence_score)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_token_intelligence_confidence
            ON token_intelligence(confidence_score)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_token_intelligence_symbol
            ON token_intelligence(symbol)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_token_intelligence_updated
            ON token_intelligence(
                intelligence_updated_at
            )
            """
        )

        connection.commit()

    finally:
        connection.close()


def get_historical_learning_by_symbol():
    """
    Aggregate historical scanner learning by token symbol.

    The current opportunity-history table stores symbols rather
    than mint addresses. Therefore, historical learning is joined
    to market tokens by symbol.

    A future database migration should also store mint addresses
    in opportunity_history so duplicate symbols can be separated.
    """

    create_opportunity_history_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                token AS symbol,
                COUNT(*) AS total_scans,

                COALESCE(
                    SUM(quote_successful),
                    0
                ) AS successful_quotes,

                SUM(
                    CASE
                        WHEN quote_successful = 0
                        THEN 1
                        ELSE 0
                    END
                ) AS quote_errors,

                COALESCE(
                    SUM(eligible),
                    0
                ) AS eligible_scans,

                SUM(
                    CASE
                        WHEN quote_successful = 1
                         AND net_profit > 0
                        THEN 1
                        ELSE 0
                    END
                ) AS profitable_scans,

                AVG(
                    CASE
                        WHEN quote_successful = 1
                        THEN net_profit
                        ELSE NULL
                    END
                ) AS average_net_profit,

                AVG(
                    CASE
                        WHEN quote_successful = 1
                         AND datetime(scanned_at)
                             >= datetime(
                                 'now',
                                 '-7 days'
                             )
                        THEN net_profit
                        ELSE NULL
                    END
                ) AS recent_average_net_profit,

                MAX(
                    CASE
                        WHEN quote_successful = 1
                        THEN net_profit
                        ELSE NULL
                    END
                ) AS best_net_profit,

                MIN(
                    CASE
                        WHEN quote_successful = 1
                        THEN net_profit
                        ELSE NULL
                    END
                ) AS worst_net_profit,

                MAX(scanned_at) AS last_scanned_at

            FROM opportunity_history
            GROUP BY token
            """
        )

        rows = cursor.fetchall()

    finally:
        connection.close()

    return {
        str(row["symbol"] or "").upper(): dict(row)
        for row in rows
        if row["symbol"]
    }


def calculate_confidence_score(total_scans):
    """
    Calculate experience confidence from sample size.

    Confidence grows quickly at first, then slows as the
    historical sample becomes larger.
    """

    total_scans = max(
        0,
        safe_int(total_scans),
    )

    if total_scans == 0:
        return 0.0

    confidence = (
        1.0
        - math.exp(
            -total_scans
            / CONFIDENCE_FULL_STRENGTH_SCANS
        )
    ) * 100.0

    return clamp(confidence)


def calculate_exploration_bonus(total_scans):
    """
    Reward new and under-tested tokens.

    This prevents the scanner from permanently ignoring tokens
    that do not yet have enough historical observations.
    """

    total_scans = max(
        0,
        safe_int(total_scans),
    )

    bonus = (
        MAXIMUM_EXPLORATION_BONUS
        * math.exp(
            -total_scans
            / EXPLORATION_DECAY_SCANS
        )
    )

    return clamp(
        bonus,
        minimum=0.0,
        maximum=MAXIMUM_EXPLORATION_BONUS,
    )


def calculate_profit_score(net_profit):
    """
    Convert historical average net profit into a 0-to-100 score.

    A smooth logistic curve prevents one extreme result from
    dominating the model.
    """

    net_profit = safe_float(net_profit)

    scaled_profit = (
        net_profit
        / max(PROFIT_REFERENCE_USD, 0.000001)
    )

    score = (
        100.0
        / (
            1.0
            + math.exp(
                -2.0 * scaled_profit
            )
        )
    )

    return clamp(score)


def calculate_rate_score(
    successes,
    attempts,
    prior_successes,
    prior_attempts,
):
    """
    Calculate a Bayesian-smoothed percentage score.

    Prior observations prevent a token with one lucky scan from
    immediately receiving a perfect score.
    """

    successes = max(
        0,
        safe_float(successes),
    )
    attempts = max(
        0,
        safe_float(attempts),
    )

    adjusted_successes = (
        successes + prior_successes
    )
    adjusted_attempts = (
        attempts + prior_attempts
    )

    if adjusted_attempts <= 0:
        return 0.0

    return clamp(
        adjusted_successes
        / adjusted_attempts
        * 100.0
    )


def calculate_downside_safety_score(
    worst_net_profit,
    quote_success_rate,
):
    """
    Reward stable tokens and penalize large losses and failures.
    """

    worst_net_profit = safe_float(
        worst_net_profit
    )
    quote_success_rate = clamp(
        quote_success_rate
    )

    downside_loss = abs(
        min(worst_net_profit, 0.0)
    )

    loss_penalty = clamp(
        downside_loss
        / max(LOSS_REFERENCE_USD, 0.000001)
        * 100.0
    )

    failure_penalty = (
        100.0 - quote_success_rate
    )

    combined_penalty = (
        loss_penalty * 0.65
        + failure_penalty * 0.35
    )

    return clamp(
        100.0 - combined_penalty
    )


def calculate_token_intelligence(
    market_token,
    history=None,
):
    """
    Calculate complete intelligence details for one token.
    """

    history = history or {}

    market_score = clamp(
        safe_float(
            market_token.get("market_score")
        )
    )

    total_scans = safe_int(
        history.get("total_scans")
    )
    successful_quotes = safe_int(
        history.get("successful_quotes")
    )
    quote_errors = safe_int(
        history.get("quote_errors")
    )
    eligible_scans = safe_int(
        history.get("eligible_scans")
    )
    profitable_scans = safe_int(
        history.get("profitable_scans")
    )

    average_net_profit = safe_float(
        history.get("average_net_profit")
    )
    recent_average_net_profit = safe_float(
        history.get(
            "recent_average_net_profit"
        )
    )
    best_net_profit = safe_float(
        history.get("best_net_profit")
    )
    worst_net_profit = safe_float(
        history.get("worst_net_profit")
    )

    quote_success_rate = (
        successful_quotes
        / total_scans
        * 100.0
        if total_scans > 0
        else 0.0
    )

    eligible_scan_rate = (
        eligible_scans
        / successful_quotes
        * 100.0
        if successful_quotes > 0
        else 0.0
    )

    profitable_scan_rate = (
        profitable_scans
        / successful_quotes
        * 100.0
        if successful_quotes > 0
        else 0.0
    )

    profitability_score = (
        calculate_profit_score(
            average_net_profit
        )
    )

    recent_performance_score = (
        calculate_profit_score(
            recent_average_net_profit
        )
    )

    quote_reliability_score = (
        calculate_rate_score(
            successes=successful_quotes,
            attempts=total_scans,
            prior_successes=8,
            prior_attempts=10,
        )
    )

    eligibility_score = (
        calculate_rate_score(
            successes=eligible_scans,
            attempts=successful_quotes,
            prior_successes=1,
            prior_attempts=10,
        )
    )

    downside_safety_score = (
        calculate_downside_safety_score(
            worst_net_profit=worst_net_profit,
            quote_success_rate=(
                quote_success_rate
            ),
        )
    )

    confidence_score = (
        calculate_confidence_score(
            total_scans
        )
    )

    exploration_bonus = (
        calculate_exploration_bonus(
            total_scans
        )
    )

    raw_exploitation_score = (
        market_score
        * MARKET_QUALITY_WEIGHT
        + profitability_score
        * PROFITABILITY_WEIGHT
        + quote_reliability_score
        * QUOTE_RELIABILITY_WEIGHT
        + eligibility_score
        * ELIGIBILITY_WEIGHT
        + recent_performance_score
        * RECENT_PERFORMANCE_WEIGHT
        + downside_safety_score
        * DOWNSIDE_SAFETY_WEIGHT
    )

    # Historical components become more influential as confidence
    # increases. New tokens begin closer to their market score.
    confidence_ratio = (
        confidence_score / 100.0
    )

    exploitation_score = (
        market_score
        * (1.0 - confidence_ratio)
        + raw_exploitation_score
        * confidence_ratio
    )

    intelligence_score = clamp(
        exploitation_score
        + exploration_bonus
    )

    return {
        "mint": market_token.get("mint"),
        "symbol": (
            market_token.get("symbol")
            or "UNKNOWN"
        ),
        "name": (
            market_token.get("name")
            or "Unknown"
        ),
        "intelligence_score": round(
            intelligence_score,
            4,
        ),
        "exploitation_score": round(
            exploitation_score,
            4,
        ),
        "exploration_bonus": round(
            exploration_bonus,
            4,
        ),
        "confidence_score": round(
            confidence_score,
            4,
        ),
        "market_quality_score": round(
            market_score,
            4,
        ),
        "profitability_score": round(
            profitability_score,
            4,
        ),
        "quote_reliability_score": round(
            quote_reliability_score,
            4,
        ),
        "eligibility_score": round(
            eligibility_score,
            4,
        ),
        "recent_performance_score": round(
            recent_performance_score,
            4,
        ),
        "downside_safety_score": round(
            downside_safety_score,
            4,
        ),
        "total_scans": total_scans,
        "successful_quotes": successful_quotes,
        "quote_errors": quote_errors,
        "eligible_scans": eligible_scans,
        "profitable_scans": profitable_scans,
        "average_net_profit": (
            average_net_profit
        ),
        "recent_average_net_profit": (
            recent_average_net_profit
        ),
        "best_net_profit": best_net_profit,
        "worst_net_profit": worst_net_profit,
        "quote_success_rate": round(
            quote_success_rate,
            4,
        ),
        "eligible_scan_rate": round(
            eligible_scan_rate,
            4,
        ),
        "profitable_scan_rate": round(
            profitable_scan_rate,
            4,
        ),
        "last_scanned_at": history.get(
            "last_scanned_at"
        ),
        "intelligence_updated_at": (
            current_timestamp()
        ),
    }


def save_token_intelligence(records):
    """
    Insert or update calculated intelligence records.
    """

    initialize_token_intelligence_table()

    if not records:
        return 0

    connection = get_database_connection()
    cursor = connection.cursor()

    saved_count = 0

    try:
        for record in records:
            if not record.get("mint"):
                continue

            cursor.execute(
                """
                INSERT INTO token_intelligence (
                    mint,
                    symbol,
                    name,
                    intelligence_score,
                    exploitation_score,
                    exploration_bonus,
                    confidence_score,
                    market_quality_score,
                    profitability_score,
                    quote_reliability_score,
                    eligibility_score,
                    recent_performance_score,
                    downside_safety_score,
                    total_scans,
                    successful_quotes,
                    quote_errors,
                    eligible_scans,
                    profitable_scans,
                    average_net_profit,
                    recent_average_net_profit,
                    best_net_profit,
                    worst_net_profit,
                    quote_success_rate,
                    eligible_scan_rate,
                    profitable_scan_rate,
                    last_scanned_at,
                    intelligence_updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(mint) DO UPDATE SET
                    symbol = excluded.symbol,
                    name = excluded.name,
                    intelligence_score =
                        excluded.intelligence_score,
                    exploitation_score =
                        excluded.exploitation_score,
                    exploration_bonus =
                        excluded.exploration_bonus,
                    confidence_score =
                        excluded.confidence_score,
                    market_quality_score =
                        excluded.market_quality_score,
                    profitability_score =
                        excluded.profitability_score,
                    quote_reliability_score =
                        excluded.quote_reliability_score,
                    eligibility_score =
                        excluded.eligibility_score,
                    recent_performance_score =
                        excluded.recent_performance_score,
                    downside_safety_score =
                        excluded.downside_safety_score,
                    total_scans =
                        excluded.total_scans,
                    successful_quotes =
                        excluded.successful_quotes,
                    quote_errors =
                        excluded.quote_errors,
                    eligible_scans =
                        excluded.eligible_scans,
                    profitable_scans =
                        excluded.profitable_scans,
                    average_net_profit =
                        excluded.average_net_profit,
                    recent_average_net_profit =
                        excluded.recent_average_net_profit,
                    best_net_profit =
                        excluded.best_net_profit,
                    worst_net_profit =
                        excluded.worst_net_profit,
                    quote_success_rate =
                        excluded.quote_success_rate,
                    eligible_scan_rate =
                        excluded.eligible_scan_rate,
                    profitable_scan_rate =
                        excluded.profitable_scan_rate,
                    last_scanned_at =
                        excluded.last_scanned_at,
                    intelligence_updated_at =
                        excluded.intelligence_updated_at
                """,
                (
                    record["mint"],
                    record["symbol"],
                    record["name"],
                    record["intelligence_score"],
                    record["exploitation_score"],
                    record["exploration_bonus"],
                    record["confidence_score"],
                    record["market_quality_score"],
                    record["profitability_score"],
                    record[
                        "quote_reliability_score"
                    ],
                    record["eligibility_score"],
                    record[
                        "recent_performance_score"
                    ],
                    record["downside_safety_score"],
                    record["total_scans"],
                    record["successful_quotes"],
                    record["quote_errors"],
                    record["eligible_scans"],
                    record["profitable_scans"],
                    record["average_net_profit"],
                    record[
                        "recent_average_net_profit"
                    ],
                    record["best_net_profit"],
                    record["worst_net_profit"],
                    record["quote_success_rate"],
                    record["eligible_scan_rate"],
                    record[
                        "profitable_scan_rate"
                    ],
                    record["last_scanned_at"],
                    record[
                        "intelligence_updated_at"
                    ],
                ),
            )

            saved_count += 1

        connection.commit()

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()

    return saved_count


def refresh_token_intelligence(
    minimum_liquidity_usd=0,
    minimum_volume_24h_usd=0,
):
    """
    Recalculate intelligence scores for all active market tokens.
    """

    initialize_token_intelligence_table()

    market_tokens = get_liquid_tokens(
        minimum_liquidity_usd=(
            minimum_liquidity_usd
        ),
        minimum_volume_24h_usd=(
            minimum_volume_24h_usd
        ),
        limit=MAXIMUM_MARKET_TOKENS,
    )

    history_by_symbol = (
        get_historical_learning_by_symbol()
    )

    intelligence_records = []

    for token in market_tokens:
        symbol_key = str(
            token.get("symbol") or ""
        ).upper()

        historical_data = (
            history_by_symbol.get(symbol_key)
            or {}
        )

        intelligence_records.append(
            calculate_token_intelligence(
                market_token=token,
                history=historical_data,
            )
        )

    saved_count = save_token_intelligence(
        intelligence_records
    )

    return {
        "market_tokens_processed": len(
            market_tokens
        ),
        "intelligence_records_saved": (
            saved_count
        ),
        "updated_at": current_timestamp(),
    }


def get_token_intelligence(mint):
    """
    Return intelligence information for one mint.
    """

    if not mint:
        return None

    initialize_token_intelligence_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM token_intelligence
            WHERE mint = ?
            """,
            (mint,),
        )

        row = cursor.fetchone()

    finally:
        connection.close()

    return dict(row) if row else None


def get_top_intelligent_tokens(
    limit=100,
    minimum_confidence=0,
):
    """
    Return the highest-ranked intelligent tokens.
    """

    initialize_token_intelligence_table()

    limit = max(
        1,
        int(limit),
    )
    minimum_confidence = clamp(
        minimum_confidence
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM token_intelligence
            WHERE confidence_score >= ?
            ORDER BY
                intelligence_score DESC,
                exploitation_score DESC,
                confidence_score DESC,
                market_quality_score DESC,
                symbol ASC
            LIMIT ?
            """,
            (
                minimum_confidence,
                limit,
            ),
        )

        rows = cursor.fetchall()

    finally:
        connection.close()

    return [
        dict(row)
        for row in rows
    ]


def get_intelligence_summary():
    """
    Return overall intelligence-engine statistics.
    """

    initialize_token_intelligence_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_tokens,
                AVG(intelligence_score)
                    AS average_intelligence_score,
                MAX(intelligence_score)
                    AS highest_intelligence_score,
                AVG(confidence_score)
                    AS average_confidence_score,
                SUM(
                    CASE
                        WHEN total_scans > 0
                        THEN 1
                        ELSE 0
                    END
                ) AS tokens_with_history,
                SUM(
                    CASE
                        WHEN eligible_scans > 0
                        THEN 1
                        ELSE 0
                    END
                ) AS tokens_with_opportunities,
                MAX(intelligence_updated_at)
                    AS last_updated_at
            FROM token_intelligence
            """
        )

        row = cursor.fetchone()

    finally:
        connection.close()

    if not row:
        return {
            "total_tokens": 0,
            "average_intelligence_score": 0.0,
            "highest_intelligence_score": 0.0,
            "average_confidence_score": 0.0,
            "tokens_with_history": 0,
            "tokens_with_opportunities": 0,
            "last_updated_at": None,
        }

    result = dict(row)

    for key in (
        "total_tokens",
        "tokens_with_history",
        "tokens_with_opportunities",
    ):
        result[key] = safe_int(
            result.get(key)
        )

    for key in (
        "average_intelligence_score",
        "highest_intelligence_score",
        "average_confidence_score",
    ):
        result[key] = safe_float(
            result.get(key)
        )

    return result


if __name__ == "__main__":
    refresh_result = refresh_token_intelligence(
        minimum_liquidity_usd=50_000,
        minimum_volume_24h_usd=10_000,
    )

    print("\nToken Intelligence Engine refreshed.")
    print(
        "Market tokens processed: "
        f"{refresh_result['market_tokens_processed']:,}"
    )
    print(
        "Intelligence records saved: "
        f"{refresh_result['intelligence_records_saved']:,}"
    )

    top_tokens = get_top_intelligent_tokens(
        limit=10
    )

    print("\nTop intelligent tokens:")

    for position, token in enumerate(
        top_tokens,
        start=1,
    ):
        print(
            f"{position}. "
            f"{token['symbol']} — "
            f"intelligence "
            f"{token['intelligence_score']:.2f}/100, "
            f"confidence "
            f"{token['confidence_score']:.2f}/100, "
            f"scans {token['total_scans']}"
        )