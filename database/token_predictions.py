import math
import sqlite3
from datetime import datetime
from pathlib import Path
from statistics import mean, pstdev

from database.opportunity_history import (
    create_opportunity_history_table,
)
from database.token_intelligence import (
    initialize_token_intelligence_table,
)


DATABASE_FILE = (
    Path(__file__).resolve().parent
    / "trades.db"
)


# ---------------------------------------------------------
# Prediction model settings
# ---------------------------------------------------------

RECENT_OBSERVATION_LIMIT = 30
SHORT_TREND_WINDOW = 5
LONG_TREND_WINDOW = 15

# Bayesian prior for profitable/eligible outcomes.
PROBABILITY_PRIOR_SUCCESSES = 1.0
PROBABILITY_PRIOR_ATTEMPTS = 20.0

# Confidence approaches 100 as repeated scans accumulate.
CONFIDENCE_FULL_STRENGTH_SCANS = 50

# Expected-profit shrinkage prevents one lucky scan from
# dominating predictions when history is limited.
EXPECTED_PROFIT_PRIOR_WEIGHT = 12.0
EXPECTED_PROFIT_PRIOR_USD = 0.0

# Profit and risk normalization references.
PROFIT_REFERENCE_USD = 0.05
VOLATILITY_REFERENCE_USD = 0.05
LOSS_REFERENCE_USD = 0.10

# Final AI-priority weights. These sum to 1.00.
OPPORTUNITY_WEIGHT = 0.30
EXPECTED_PROFIT_WEIGHT = 0.22
TREND_WEIGHT = 0.15
STABILITY_WEIGHT = 0.10
INTELLIGENCE_WEIGHT = 0.13
CONFIDENCE_WEIGHT = 0.10

MAXIMUM_TOKENS = 20_000


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


def clamp(value, minimum=0.0, maximum=100.0):
    """
    Restrict a numeric value to a safe range.
    """

    return max(
        minimum,
        min(maximum, safe_float(value)),
    )


def initialize_token_predictions_table():
    """
    Create the token-predictions table and supporting indexes.
    """

    initialize_token_intelligence_table()
    create_opportunity_history_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS token_predictions (
                mint TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                name TEXT,

                opportunity_probability REAL NOT NULL DEFAULT 0,
                expected_profit_usd REAL NOT NULL DEFAULT 0,
                expected_profit_score REAL NOT NULL DEFAULT 0,
                trend_score REAL NOT NULL DEFAULT 50,
                stability_score REAL NOT NULL DEFAULT 0,
                downside_risk_score REAL NOT NULL DEFAULT 0,
                prediction_confidence REAL NOT NULL DEFAULT 0,
                ai_priority REAL NOT NULL DEFAULT 0,

                recent_average_profit REAL NOT NULL DEFAULT 0,
                long_average_profit REAL NOT NULL DEFAULT 0,
                profit_volatility REAL NOT NULL DEFAULT 0,
                recent_profitable_rate REAL NOT NULL DEFAULT 0,
                recent_eligible_rate REAL NOT NULL DEFAULT 0,

                total_scans INTEGER NOT NULL DEFAULT 0,
                successful_quotes INTEGER NOT NULL DEFAULT 0,
                recent_observations INTEGER NOT NULL DEFAULT 0,
                profitable_observations INTEGER NOT NULL DEFAULT 0,
                eligible_observations INTEGER NOT NULL DEFAULT 0,

                intelligence_score REAL NOT NULL DEFAULT 0,
                intelligence_confidence REAL NOT NULL DEFAULT 0,
                market_quality_score REAL NOT NULL DEFAULT 0,

                last_scanned_at TEXT,
                prediction_updated_at TEXT NOT NULL,

                FOREIGN KEY (mint)
                    REFERENCES token_universe(mint)
            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_token_predictions_priority
            ON token_predictions(ai_priority)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_token_predictions_probability
            ON token_predictions(opportunity_probability)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_token_predictions_confidence
            ON token_predictions(prediction_confidence)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_token_predictions_updated
            ON token_predictions(prediction_updated_at)
            """
        )

        connection.commit()

    finally:
        connection.close()


def get_recent_history_by_symbol(
    per_token_limit=RECENT_OBSERVATION_LIMIT,
):
    """
    Return recent historical observations grouped by symbol.

    opportunity_history currently stores token symbols rather
    than mint addresses, so prediction history is matched by
    upper-cased symbol. A later migration should store mint in
    opportunity_history to eliminate duplicate-symbol ambiguity.
    """

    create_opportunity_history_table()

    per_token_limit = max(
        1,
        int(per_token_limit),
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                token,
                net_profit,
                eligible,
                quote_successful,
                market_score,
                scanned_at
            FROM opportunity_history
            ORDER BY token ASC, id DESC
            """
        )

        rows = cursor.fetchall()

    finally:
        connection.close()

    grouped = {}

    for row in rows:
        symbol = str(
            row["token"] or ""
        ).upper()

        if not symbol:
            continue

        history = grouped.setdefault(
            symbol,
            [],
        )

        if len(history) >= per_token_limit:
            continue

        history.append(dict(row))

    return grouped


def calculate_confidence(total_scans):
    """
    Convert sample size into a 0-to-100 confidence score.
    """

    total_scans = max(
        0,
        safe_int(total_scans),
    )

    if total_scans == 0:
        return 0.0

    score = (
        1.0
        - math.exp(
            -total_scans
            / CONFIDENCE_FULL_STRENGTH_SCANS
        )
    ) * 100.0

    return clamp(score)


def calculate_probability_score(
    successes,
    attempts,
):
    """
    Return a Bayesian-smoothed event probability percentage.
    """

    successes = max(
        0.0,
        safe_float(successes),
    )
    attempts = max(
        0.0,
        safe_float(attempts),
    )

    adjusted_successes = (
        successes
        + PROBABILITY_PRIOR_SUCCESSES
    )
    adjusted_attempts = (
        attempts
        + PROBABILITY_PRIOR_ATTEMPTS
    )

    if adjusted_attempts <= 0:
        return 0.0

    return clamp(
        adjusted_successes
        / adjusted_attempts
        * 100.0
    )


def calculate_expected_profit(
    profits,
):
    """
    Calculate shrinkage-adjusted expected profit.
    """

    if not profits:
        return EXPECTED_PROFIT_PRIOR_USD

    observed_average = mean(profits)
    observed_count = len(profits)

    weighted_total = (
        observed_average * observed_count
        + EXPECTED_PROFIT_PRIOR_USD
        * EXPECTED_PROFIT_PRIOR_WEIGHT
    )

    total_weight = (
        observed_count
        + EXPECTED_PROFIT_PRIOR_WEIGHT
    )

    if total_weight <= 0:
        return 0.0

    return weighted_total / total_weight


def calculate_profit_score(profit_usd):
    """
    Convert expected profit into a bounded 0-to-100 score.
    """

    scaled_profit = (
        safe_float(profit_usd)
        / max(PROFIT_REFERENCE_USD, 0.000001)
    )

    score = (
        100.0
        / (
            1.0
            + math.exp(-2.0 * scaled_profit)
        )
    )

    return clamp(score)


def calculate_stability_score(profits):
    """
    Reward low volatility and sufficient observations.
    """

    if not profits:
        return 0.0

    if len(profits) == 1:
        return 25.0

    volatility = pstdev(profits)

    volatility_penalty = clamp(
        volatility
        / max(
            VOLATILITY_REFERENCE_USD,
            0.000001,
        )
        * 100.0
    )

    sample_factor = min(
        1.0,
        len(profits) / 10.0,
    )

    return clamp(
        (100.0 - volatility_penalty)
        * sample_factor
    )


def calculate_trend_score(profits):
    """
    Compare recent and longer-term average profit.

    A score above 50 means improving performance.
    A score below 50 means deteriorating performance.
    """

    if not profits:
        return 50.0

    short_values = profits[
        :SHORT_TREND_WINDOW
    ]
    long_values = profits[
        :LONG_TREND_WINDOW
    ]

    short_average = mean(short_values)
    long_average = mean(long_values)

    difference = short_average - long_average

    scaled_difference = (
        difference
        / max(PROFIT_REFERENCE_USD, 0.000001)
    )

    score = 50.0 + math.tanh(
        scaled_difference * 2.0
    ) * 50.0

    return clamp(score)


def calculate_downside_risk_score(profits):
    """
    Return a 0-to-100 risk score where higher means riskier.
    """

    if not profits:
        return 50.0

    worst_profit = min(profits)
    negative_profits = [
        abs(value)
        for value in profits
        if value < 0
    ]

    average_loss = (
        mean(negative_profits)
        if negative_profits
        else 0.0
    )

    worst_loss_penalty = clamp(
        abs(min(worst_profit, 0.0))
        / max(LOSS_REFERENCE_USD, 0.000001)
        * 100.0
    )

    average_loss_penalty = clamp(
        average_loss
        / max(LOSS_REFERENCE_USD, 0.000001)
        * 100.0
    )

    volatility_penalty = 0.0

    if len(profits) > 1:
        volatility_penalty = clamp(
            pstdev(profits)
            / max(
                VOLATILITY_REFERENCE_USD,
                0.000001,
            )
            * 100.0
        )

    return clamp(
        worst_loss_penalty * 0.50
        + average_loss_penalty * 0.30
        + volatility_penalty * 0.20
    )


def calculate_prediction_record(
    intelligence_record,
    history_rows=None,
):
    """
    Calculate one token's complete prediction profile.
    """

    history_rows = history_rows or []

    successful_rows = [
        row
        for row in history_rows
        if safe_int(
            row.get("quote_successful")
        ) == 1
    ]

    profits = [
        safe_float(row.get("net_profit"))
        for row in successful_rows
    ]

    recent_count = len(history_rows)
    successful_count = len(successful_rows)

    profitable_count = sum(
        profit > 0
        for profit in profits
    )

    eligible_count = sum(
        safe_int(row.get("eligible")) == 1
        for row in successful_rows
    )

    opportunity_probability = (
        calculate_probability_score(
            successes=profitable_count,
            attempts=successful_count,
        )
    )

    expected_profit_usd = (
        calculate_expected_profit(profits)
    )

    expected_profit_score = (
        calculate_profit_score(
            expected_profit_usd
        )
    )

    trend_score = calculate_trend_score(
        profits
    )

    stability_score = (
        calculate_stability_score(profits)
    )

    downside_risk_score = (
        calculate_downside_risk_score(profits)
    )

    total_scans = safe_int(
        intelligence_record.get("total_scans")
    )

    prediction_confidence = (
        calculate_confidence(total_scans)
    )

    intelligence_score = clamp(
        intelligence_record.get(
            "intelligence_score"
        )
    )

    intelligence_confidence = clamp(
        intelligence_record.get(
            "confidence_score"
        )
    )

    market_quality_score = clamp(
        intelligence_record.get(
            "market_quality_score"
        )
    )

    raw_priority = (
        opportunity_probability
        * OPPORTUNITY_WEIGHT
        + expected_profit_score
        * EXPECTED_PROFIT_WEIGHT
        + trend_score
        * TREND_WEIGHT
        + stability_score
        * STABILITY_WEIGHT
        + intelligence_score
        * INTELLIGENCE_WEIGHT
        + prediction_confidence
        * CONFIDENCE_WEIGHT
    )

    risk_penalty = (
        downside_risk_score
        * 0.25
    )

    confidence_ratio = (
        prediction_confidence / 100.0
    )

    # New tokens remain anchored to their intelligence score.
    # As evidence grows, the prediction model contributes more.
    blended_priority = (
        intelligence_score
        * (1.0 - confidence_ratio)
        + raw_priority
        * confidence_ratio
    )

    maximum_allowed_priority = (
        90.0
        + prediction_confidence * 0.10
    )

    ai_priority = clamp(
        blended_priority - risk_penalty,
        maximum=maximum_allowed_priority,
    )

    recent_average_profit = (
        mean(profits[:SHORT_TREND_WINDOW])
        if profits
        else 0.0
    )

    long_average_profit = (
        mean(profits[:LONG_TREND_WINDOW])
        if profits
        else 0.0
    )

    profit_volatility = (
        pstdev(profits)
        if len(profits) > 1
        else 0.0
    )

    recent_profitable_rate = (
        profitable_count
        / successful_count
        * 100.0
        if successful_count > 0
        else 0.0
    )

    recent_eligible_rate = (
        eligible_count
        / successful_count
        * 100.0
        if successful_count > 0
        else 0.0
    )

    last_scanned_at = None

    for row in history_rows:
        scanned_at = row.get("scanned_at")

        if scanned_at:
            last_scanned_at = scanned_at
            break

    return {
        "mint": intelligence_record.get("mint"),
        "symbol": (
            intelligence_record.get("symbol")
            or "UNKNOWN"
        ),
        "name": (
            intelligence_record.get("name")
            or "Unknown"
        ),
        "opportunity_probability": round(
            opportunity_probability,
            4,
        ),
        "expected_profit_usd": round(
            expected_profit_usd,
            8,
        ),
        "expected_profit_score": round(
            expected_profit_score,
            4,
        ),
        "trend_score": round(
            trend_score,
            4,
        ),
        "stability_score": round(
            stability_score,
            4,
        ),
        "downside_risk_score": round(
            downside_risk_score,
            4,
        ),
        "prediction_confidence": round(
            prediction_confidence,
            4,
        ),
        "ai_priority": round(
            ai_priority,
            4,
        ),
        "recent_average_profit": round(
            recent_average_profit,
            8,
        ),
        "long_average_profit": round(
            long_average_profit,
            8,
        ),
        "profit_volatility": round(
            profit_volatility,
            8,
        ),
        "recent_profitable_rate": round(
            recent_profitable_rate,
            4,
        ),
        "recent_eligible_rate": round(
            recent_eligible_rate,
            4,
        ),
        "total_scans": total_scans,
        "successful_quotes": safe_int(
            intelligence_record.get(
                "successful_quotes"
            )
        ),
        "recent_observations": recent_count,
        "profitable_observations": (
            profitable_count
        ),
        "eligible_observations": eligible_count,
        "intelligence_score": round(
            intelligence_score,
            4,
        ),
        "intelligence_confidence": round(
            intelligence_confidence,
            4,
        ),
        "market_quality_score": round(
            market_quality_score,
            4,
        ),
        "last_scanned_at": last_scanned_at,
        "prediction_updated_at": (
            current_timestamp()
        ),
    }


def save_token_predictions(records):
    """
    Insert or update token prediction records.
    """

    initialize_token_predictions_table()

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
                INSERT INTO token_predictions (
                    mint,
                    symbol,
                    name,
                    opportunity_probability,
                    expected_profit_usd,
                    expected_profit_score,
                    trend_score,
                    stability_score,
                    downside_risk_score,
                    prediction_confidence,
                    ai_priority,
                    recent_average_profit,
                    long_average_profit,
                    profit_volatility,
                    recent_profitable_rate,
                    recent_eligible_rate,
                    total_scans,
                    successful_quotes,
                    recent_observations,
                    profitable_observations,
                    eligible_observations,
                    intelligence_score,
                    intelligence_confidence,
                    market_quality_score,
                    last_scanned_at,
                    prediction_updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(mint) DO UPDATE SET
                    symbol = excluded.symbol,
                    name = excluded.name,
                    opportunity_probability =
                        excluded.opportunity_probability,
                    expected_profit_usd =
                        excluded.expected_profit_usd,
                    expected_profit_score =
                        excluded.expected_profit_score,
                    trend_score = excluded.trend_score,
                    stability_score =
                        excluded.stability_score,
                    downside_risk_score =
                        excluded.downside_risk_score,
                    prediction_confidence =
                        excluded.prediction_confidence,
                    ai_priority = excluded.ai_priority,
                    recent_average_profit =
                        excluded.recent_average_profit,
                    long_average_profit =
                        excluded.long_average_profit,
                    profit_volatility =
                        excluded.profit_volatility,
                    recent_profitable_rate =
                        excluded.recent_profitable_rate,
                    recent_eligible_rate =
                        excluded.recent_eligible_rate,
                    total_scans = excluded.total_scans,
                    successful_quotes =
                        excluded.successful_quotes,
                    recent_observations =
                        excluded.recent_observations,
                    profitable_observations =
                        excluded.profitable_observations,
                    eligible_observations =
                        excluded.eligible_observations,
                    intelligence_score =
                        excluded.intelligence_score,
                    intelligence_confidence =
                        excluded.intelligence_confidence,
                    market_quality_score =
                        excluded.market_quality_score,
                    last_scanned_at =
                        excluded.last_scanned_at,
                    prediction_updated_at =
                        excluded.prediction_updated_at
                """,
                (
                    record["mint"],
                    record["symbol"],
                    record["name"],
                    record[
                        "opportunity_probability"
                    ],
                    record["expected_profit_usd"],
                    record["expected_profit_score"],
                    record["trend_score"],
                    record["stability_score"],
                    record["downside_risk_score"],
                    record["prediction_confidence"],
                    record["ai_priority"],
                    record["recent_average_profit"],
                    record["long_average_profit"],
                    record["profit_volatility"],
                    record[
                        "recent_profitable_rate"
                    ],
                    record["recent_eligible_rate"],
                    record["total_scans"],
                    record["successful_quotes"],
                    record["recent_observations"],
                    record[
                        "profitable_observations"
                    ],
                    record["eligible_observations"],
                    record["intelligence_score"],
                    record[
                        "intelligence_confidence"
                    ],
                    record["market_quality_score"],
                    record["last_scanned_at"],
                    record["prediction_updated_at"],
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


def refresh_token_predictions():
    """
    Recalculate predictions for all stored intelligence tokens.
    """

    initialize_token_predictions_table()

    history_by_symbol = (
        get_recent_history_by_symbol(
            per_token_limit=(
                RECENT_OBSERVATION_LIMIT
            )
        )
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM token_intelligence
            ORDER BY intelligence_score DESC
            LIMIT ?
            """,
            (MAXIMUM_TOKENS,),
        )

        intelligence_records = [
            dict(row)
            for row in cursor.fetchall()
        ]

    finally:
        connection.close()

    prediction_records = []

    for intelligence_record in intelligence_records:
        symbol_key = str(
            intelligence_record.get("symbol")
            or ""
        ).upper()

        history_rows = (
            history_by_symbol.get(symbol_key)
            or []
        )

        prediction_records.append(
            calculate_prediction_record(
                intelligence_record=(
                    intelligence_record
                ),
                history_rows=history_rows,
            )
        )

    saved_count = save_token_predictions(
        prediction_records
    )

    return {
        "intelligence_tokens_processed": len(
            intelligence_records
        ),
        "prediction_records_saved": saved_count,
        "updated_at": current_timestamp(),
    }


def get_token_prediction(mint):
    """
    Return prediction information for one mint.
    """

    if not mint:
        return None

    initialize_token_predictions_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM token_predictions
            WHERE mint = ?
            """,
            (mint,),
        )

        row = cursor.fetchone()

    finally:
        connection.close()

    return dict(row) if row else None


def get_top_predicted_tokens(
    limit=100,
    minimum_confidence=0,
):
    """
    Return tokens ranked by final AI priority.
    """

    initialize_token_predictions_table()

    limit = max(1, int(limit))
    minimum_confidence = clamp(
        minimum_confidence
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM token_predictions
            WHERE prediction_confidence >= ?
            ORDER BY
                ai_priority DESC,
                opportunity_probability DESC,
                expected_profit_usd DESC,
                trend_score DESC,
                stability_score DESC,
                intelligence_score DESC,
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


def get_prediction_summary():
    """
    Return overall prediction-engine statistics.
    """

    initialize_token_predictions_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_tokens,
                AVG(opportunity_probability)
                    AS average_opportunity_probability,
                AVG(expected_profit_usd)
                    AS average_expected_profit_usd,
                AVG(prediction_confidence)
                    AS average_prediction_confidence,
                AVG(ai_priority)
                    AS average_ai_priority,
                MAX(ai_priority)
                    AS highest_ai_priority,
                SUM(
                    CASE
                        WHEN expected_profit_usd > 0
                        THEN 1
                        ELSE 0
                    END
                ) AS positive_expected_profit_tokens,
                SUM(
                    CASE
                        WHEN trend_score > 55
                        THEN 1
                        ELSE 0
                    END
                ) AS improving_tokens,
                MAX(prediction_updated_at)
                    AS last_updated_at
            FROM token_predictions
            """
        )

        row = cursor.fetchone()

    finally:
        connection.close()

    if not row:
        return {
            "total_tokens": 0,
            "average_opportunity_probability": 0.0,
            "average_expected_profit_usd": 0.0,
            "average_prediction_confidence": 0.0,
            "average_ai_priority": 0.0,
            "highest_ai_priority": 0.0,
            "positive_expected_profit_tokens": 0,
            "improving_tokens": 0,
            "last_updated_at": None,
        }

    result = dict(row)

    integer_fields = (
        "total_tokens",
        "positive_expected_profit_tokens",
        "improving_tokens",
    )

    float_fields = (
        "average_opportunity_probability",
        "average_expected_profit_usd",
        "average_prediction_confidence",
        "average_ai_priority",
        "highest_ai_priority",
    )

    for key in integer_fields:
        result[key] = safe_int(
            result.get(key)
        )

    for key in float_fields:
        result[key] = safe_float(
            result.get(key)
        )

    return result


if __name__ == "__main__":
    refresh_result = refresh_token_predictions()

    print("\nToken Prediction Engine refreshed.")
    print(
        "Intelligence tokens processed: "
        f"{refresh_result['intelligence_tokens_processed']:,}"
    )
    print(
        "Prediction records saved: "
        f"{refresh_result['prediction_records_saved']:,}"
    )

    top_tokens = get_top_predicted_tokens(
        limit=10,
        minimum_confidence=0,
    )

    print("\nTop predicted tokens:")

    for position, token in enumerate(
        top_tokens,
        start=1,
    ):
        print(
            f"{position}. "
            f"{token['symbol']} — "
            f"AI priority "
            f"{token['ai_priority']:.2f}/100, "
            f"opportunity "
            f"{token['opportunity_probability']:.2f}%, "
            f"expected profit "
            f"${token['expected_profit_usd']:.6f}, "
            f"trend "
            f"{token['trend_score']:.2f}/100, "
            f"confidence "
            f"{token['prediction_confidence']:.2f}/100"
        )