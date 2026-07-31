import math
import sqlite3
from datetime import datetime
from pathlib import Path

from database.token_intelligence import (
    initialize_token_intelligence_table,
)
from database.token_metrics import (
    initialize_token_metrics_table,
)
from database.token_predictions import (
    initialize_token_predictions_table,
)
from database.pattern_learning import (
    ensure_pattern_schema,
)
from database.reinforcement_learning import (
    get_champion_config,
)


DATABASE_FILE = (
    Path(__file__).resolve().parent
    / "trades.db"
)


# ---------------------------------------------------------
# AI Opportunity Ranking settings
# ---------------------------------------------------------

# Component weights sum to 1.00.
MARKET_SCORE_WEIGHT = 0.15
INTELLIGENCE_SCORE_WEIGHT = 0.20
PREDICTION_PRIORITY_WEIGHT = 0.30
OPPORTUNITY_PROBABILITY_WEIGHT = 0.12
EXPECTED_PROFIT_SCORE_WEIGHT = 0.08
TREND_SCORE_WEIGHT = 0.08
STABILITY_SCORE_WEIGHT = 0.07

# Higher downside risk reduces the final score.
DOWNSIDE_RISK_PENALTY_WEIGHT = 0.20

# Confidence combines intelligence and prediction evidence.
INTELLIGENCE_CONFIDENCE_WEIGHT = 0.40
PREDICTION_CONFIDENCE_WEIGHT = 0.60

# Freshness penalty settings.
FRESHNESS_GRACE_HOURS = 1.0
FRESHNESS_FULL_PENALTY_HOURS = 24.0
MAXIMUM_FRESHNESS_PENALTY = 8.0

MAXIMUM_RANKED_TOKENS = 20_000


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


def parse_database_timestamp(value):
    """
    Parse a database timestamp.

    Returns None when the timestamp is missing or invalid.
    """

    if not value:
        return None

    try:
        return datetime.strptime(
            str(value),
            "%Y-%m-%d %H:%M:%S",
        )
    except (TypeError, ValueError):
        return None


def calculate_freshness_score(*timestamps):
    """
    Return a 0-to-100 data-freshness score.

    The newest available timestamp is used. Data receives full
    freshness during the grace period, then gradually declines.
    """

    parsed = [
        parse_database_timestamp(value)
        for value in timestamps
    ]

    parsed = [
        value
        for value in parsed
        if value is not None
    ]

    if not parsed:
        return 0.0

    newest_timestamp = max(parsed)

    age_hours = max(
        0.0,
        (
            datetime.now() - newest_timestamp
        ).total_seconds() / 3600.0,
    )

    if age_hours <= FRESHNESS_GRACE_HOURS:
        return 100.0

    usable_range = max(
        0.000001,
        (
            FRESHNESS_FULL_PENALTY_HOURS
            - FRESHNESS_GRACE_HOURS
        ),
    )

    age_after_grace = (
        age_hours - FRESHNESS_GRACE_HOURS
    )

    freshness_ratio = (
        1.0
        - age_after_grace / usable_range
    )

    return clamp(
        freshness_ratio * 100.0
    )


def initialize_ai_ranking_table():
    """
    Create the permanent AI-ranking table and indexes.
    """

    initialize_token_metrics_table()
    initialize_token_intelligence_table()
    initialize_token_predictions_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS ai_rankings (
                mint TEXT PRIMARY KEY,
                symbol TEXT NOT NULL,
                name TEXT,

                ai_opportunity_score REAL NOT NULL DEFAULT 0,
                raw_opportunity_score REAL NOT NULL DEFAULT 0,
                baseline_score REAL NOT NULL DEFAULT 0,
                combined_confidence REAL NOT NULL DEFAULT 0,
                confidence_adjustment REAL NOT NULL DEFAULT 0,
                risk_penalty REAL NOT NULL DEFAULT 0,
                freshness_score REAL NOT NULL DEFAULT 0,
                freshness_penalty REAL NOT NULL DEFAULT 0,

                market_score REAL NOT NULL DEFAULT 0,
                intelligence_score REAL NOT NULL DEFAULT 0,
                prediction_ai_priority REAL NOT NULL DEFAULT 0,
                opportunity_probability REAL NOT NULL DEFAULT 0,
                expected_profit_usd REAL NOT NULL DEFAULT 0,
                expected_profit_score REAL NOT NULL DEFAULT 0,
                trend_score REAL NOT NULL DEFAULT 50,
                stability_score REAL NOT NULL DEFAULT 0,
                downside_risk_score REAL NOT NULL DEFAULT 0,

                intelligence_confidence REAL NOT NULL DEFAULT 0,
                prediction_confidence REAL NOT NULL DEFAULT 0,
                total_scans INTEGER NOT NULL DEFAULT 0,

                liquidity_usd REAL NOT NULL DEFAULT 0,
                volume_24h_usd REAL NOT NULL DEFAULT 0,
                pair_count INTEGER NOT NULL DEFAULT 0,

                last_scanned_at TEXT,
                metrics_updated_at TEXT,
                intelligence_updated_at TEXT,
                prediction_updated_at TEXT,
                ranking_updated_at TEXT NOT NULL,

                FOREIGN KEY (mint)
                    REFERENCES token_universe(mint)
            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_ai_rankings_score
            ON ai_rankings(ai_opportunity_score)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_ai_rankings_confidence
            ON ai_rankings(combined_confidence)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_ai_rankings_probability
            ON ai_rankings(opportunity_probability)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_ai_rankings_expected_profit
            ON ai_rankings(expected_profit_usd)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_ai_rankings_updated
            ON ai_rankings(ranking_updated_at)
            """
        )

        connection.commit()

    finally:
        connection.close()


def get_ranking_candidates(
    minimum_liquidity_usd=0,
    minimum_volume_24h_usd=0,
    limit=MAXIMUM_RANKED_TOKENS,
):
    """
    Load active tokens with metrics, intelligence and predictions.

    Tokens require a prediction record because the ranking engine
    is the layer that combines all three upstream systems.
    """

    initialize_ai_ranking_table()
    ensure_pattern_schema()

    minimum_liquidity_usd = max(
        0.0,
        safe_float(minimum_liquidity_usd),
    )

    minimum_volume_24h_usd = max(
        0.0,
        safe_float(minimum_volume_24h_usd),
    )

    limit = max(
        1,
        int(limit),
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                token_universe.mint,
                token_universe.symbol,
                token_universe.name,

                token_metrics.liquidity_usd,
                token_metrics.volume_24h_usd,
                token_metrics.pair_count,
                token_metrics.metrics_updated_at,

                token_intelligence.intelligence_score,
                token_intelligence.confidence_score
                    AS intelligence_confidence,
                token_intelligence.market_quality_score
                    AS market_score,
                token_intelligence.total_scans,
                token_intelligence.last_scanned_at,
                token_intelligence.intelligence_updated_at,

                token_predictions.ai_priority
                    AS prediction_ai_priority,
                token_predictions.opportunity_probability,
                token_predictions.expected_profit_usd,
                token_predictions.expected_profit_score,
                token_predictions.trend_score,
                token_predictions.stability_score,
                token_predictions.downside_risk_score,
                token_predictions.prediction_confidence,
                token_predictions.prediction_updated_at,

                COALESCE(
                    pattern_learning.pattern_score,
                    50
                ) AS pattern_score,

                COALESCE(
                    pattern_learning.sample_confidence,
                    0
                ) AS pattern_confidence

            FROM token_universe

            INNER JOIN token_metrics
                ON token_metrics.mint =
                   token_universe.mint

            INNER JOIN token_intelligence
                ON token_intelligence.mint =
                   token_universe.mint

            INNER JOIN token_predictions
                ON token_predictions.mint =
                   token_universe.mint

            LEFT JOIN pattern_learning
                ON pattern_learning.mint =
                   token_universe.mint

            WHERE token_universe.enabled = 1

              AND COALESCE(
                    token_universe.failed_scans,
                    0
                  ) < 3

              AND token_metrics.pair_count > 0

              AND token_metrics.liquidity_usd >= ?

              AND token_metrics.volume_24h_usd >= ?

            ORDER BY
                token_predictions.ai_priority DESC,
                token_intelligence.intelligence_score DESC,
                token_intelligence.market_quality_score DESC,
                token_universe.symbol ASC,
                token_universe.mint ASC

            LIMIT ?
            """,
            (
                minimum_liquidity_usd,
                minimum_volume_24h_usd,
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


def calculate_ai_ranking_record(candidate):
    """
    Calculate the final AI opportunity ranking for one token.

    Low-confidence tokens remain anchored to a baseline made from
    market quality and intelligence rather than being multiplied
    toward zero. Predictive signals gain influence as evidence
    accumulates.
    """

    model = get_champion_config()

    market_weight = safe_float(
        model.get("market_weight")
        if model
        else MARKET_SCORE_WEIGHT
    )
    intelligence_weight = safe_float(
        model.get("intelligence_weight")
        if model
        else INTELLIGENCE_SCORE_WEIGHT
    )
    prediction_weight = safe_float(
        model.get("prediction_weight")
        if model
        else PREDICTION_PRIORITY_WEIGHT
    )
    opportunity_weight = safe_float(
        model.get("opportunity_weight")
        if model
        else OPPORTUNITY_PROBABILITY_WEIGHT
    )
    expected_profit_weight = safe_float(
        model.get("expected_profit_weight")
        if model
        else EXPECTED_PROFIT_SCORE_WEIGHT
    )
    trend_weight = safe_float(
        model.get("trend_weight")
        if model
        else TREND_SCORE_WEIGHT
    )
    stability_weight = safe_float(
        model.get("stability_weight")
        if model
        else STABILITY_SCORE_WEIGHT
    )
    pattern_weight = safe_float(
        model.get("pattern_weight")
        if model
        else 0.0
    )
    risk_penalty_weight = safe_float(
        model.get("risk_penalty_weight")
        if model
        else DOWNSIDE_RISK_PENALTY_WEIGHT
    )
    intelligence_confidence_weight = safe_float(
        model.get(
            "intelligence_confidence_weight"
        )
        if model
        else INTELLIGENCE_CONFIDENCE_WEIGHT
    )
    prediction_confidence_weight = safe_float(
        model.get(
            "prediction_confidence_weight"
        )
        if model
        else PREDICTION_CONFIDENCE_WEIGHT
    )

    market_score = clamp(
        candidate.get("market_score")
    )

    intelligence_score = clamp(
        candidate.get("intelligence_score")
    )

    prediction_ai_priority = clamp(
        candidate.get("prediction_ai_priority")
    )

    opportunity_probability = clamp(
        candidate.get("opportunity_probability")
    )

    expected_profit_score = clamp(
        candidate.get("expected_profit_score")
    )

    trend_score = clamp(
        candidate.get("trend_score"),
    )

    stability_score = clamp(
        candidate.get("stability_score")
    )

    pattern_score = clamp(
        candidate.get("pattern_score")
        or 50.0
    )

    downside_risk_score = clamp(
        candidate.get("downside_risk_score")
    )

    intelligence_confidence = clamp(
        candidate.get("intelligence_confidence")
    )

    prediction_confidence = clamp(
        candidate.get("prediction_confidence")
    )

    raw_opportunity_score = (
        market_score
        * market_weight
        + intelligence_score
        * intelligence_weight
        + prediction_ai_priority
        * prediction_weight
        + opportunity_probability
        * opportunity_weight
        + expected_profit_score
        * expected_profit_weight
        + trend_score
        * trend_weight
        + stability_score
        * stability_weight
        + pattern_score
        * pattern_weight
    )

    combined_confidence = clamp(
        intelligence_confidence
        * intelligence_confidence_weight
        + prediction_confidence
        * prediction_confidence_weight
    )

    confidence_ratio = (
        combined_confidence / 100.0
    )

    # During early learning, use a defensible baseline. Prediction
    # signals gradually control more of the score as evidence grows.
    baseline_score = (
        market_score * 0.60
        + intelligence_score * 0.40
    )

    confidence_adjusted_score = (
        baseline_score
        * (1.0 - confidence_ratio)
        + raw_opportunity_score
        * confidence_ratio
    )

    risk_penalty = (
        downside_risk_score
        * risk_penalty_weight
        * (
            0.35
            + confidence_ratio * 0.65
        )
    )

    freshness_score = (
        calculate_freshness_score(
            candidate.get("metrics_updated_at"),
            candidate.get(
                "intelligence_updated_at"
            ),
            candidate.get(
                "prediction_updated_at"
            ),
        )
    )

    freshness_penalty = (
        (
            100.0 - freshness_score
        )
        / 100.0
        * MAXIMUM_FRESHNESS_PENALTY
    )

    maximum_allowed_score = (
        90.0
        + combined_confidence * 0.10
    )

    ai_opportunity_score = clamp(
        confidence_adjusted_score
        - risk_penalty
        - freshness_penalty,
        maximum=maximum_allowed_score,
    )

    confidence_adjustment = (
        confidence_adjusted_score
        - raw_opportunity_score
    )

    return {
        "mint": candidate.get("mint"),
        "symbol": (
            candidate.get("symbol")
            or "UNKNOWN"
        ),
        "name": (
            candidate.get("name")
            or "Unknown"
        ),
        "ai_opportunity_score": round(
            ai_opportunity_score,
            4,
        ),
        "raw_opportunity_score": round(
            raw_opportunity_score,
            4,
        ),
        "baseline_score": round(
            baseline_score,
            4,
        ),
        "combined_confidence": round(
            combined_confidence,
            4,
        ),
        "confidence_adjustment": round(
            confidence_adjustment,
            4,
        ),
        "risk_penalty": round(
            risk_penalty,
            4,
        ),
        "freshness_score": round(
            freshness_score,
            4,
        ),
        "freshness_penalty": round(
            freshness_penalty,
            4,
        ),
        "market_score": round(
            market_score,
            4,
        ),
        "intelligence_score": round(
            intelligence_score,
            4,
        ),
        "prediction_ai_priority": round(
            prediction_ai_priority,
            4,
        ),
        "opportunity_probability": round(
            opportunity_probability,
            4,
        ),
        "expected_profit_usd": round(
            safe_float(
                candidate.get(
                    "expected_profit_usd"
                )
            ),
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
        "pattern_score": round(
            pattern_score,
            4,
        ),
        "pattern_confidence": round(
            safe_float(
                candidate.get(
                    "pattern_confidence"
                )
            ),
            4,
        ),
        "reinforcement_model_id": (
            safe_int(
                model.get("model_id")
            )
            if model
            else 0
        ),
        "downside_risk_score": round(
            downside_risk_score,
            4,
        ),
        "intelligence_confidence": round(
            intelligence_confidence,
            4,
        ),
        "prediction_confidence": round(
            prediction_confidence,
            4,
        ),
        "total_scans": safe_int(
            candidate.get("total_scans")
        ),
        "liquidity_usd": safe_float(
            candidate.get("liquidity_usd")
        ),
        "volume_24h_usd": safe_float(
            candidate.get("volume_24h_usd")
        ),
        "pair_count": safe_int(
            candidate.get("pair_count")
        ),
        "last_scanned_at": candidate.get(
            "last_scanned_at"
        ),
        "metrics_updated_at": candidate.get(
            "metrics_updated_at"
        ),
        "intelligence_updated_at": (
            candidate.get(
                "intelligence_updated_at"
            )
        ),
        "prediction_updated_at": (
            candidate.get(
                "prediction_updated_at"
            )
        ),
        "ranking_updated_at": (
            current_timestamp()
        ),
    }


def save_ai_rankings(records):
    """
    Insert or update AI-ranking records.
    """

    initialize_ai_ranking_table()

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
                INSERT INTO ai_rankings (
                    mint,
                    symbol,
                    name,
                    ai_opportunity_score,
                    raw_opportunity_score,
                    baseline_score,
                    combined_confidence,
                    confidence_adjustment,
                    risk_penalty,
                    freshness_score,
                    freshness_penalty,
                    market_score,
                    intelligence_score,
                    prediction_ai_priority,
                    opportunity_probability,
                    expected_profit_usd,
                    expected_profit_score,
                    trend_score,
                    stability_score,
                    downside_risk_score,
                    intelligence_confidence,
                    prediction_confidence,
                    total_scans,
                    liquidity_usd,
                    volume_24h_usd,
                    pair_count,
                    last_scanned_at,
                    metrics_updated_at,
                    intelligence_updated_at,
                    prediction_updated_at,
                    ranking_updated_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?
                )
                ON CONFLICT(mint) DO UPDATE SET
                    symbol = excluded.symbol,
                    name = excluded.name,
                    ai_opportunity_score =
                        excluded.ai_opportunity_score,
                    raw_opportunity_score =
                        excluded.raw_opportunity_score,
                    baseline_score =
                        excluded.baseline_score,
                    combined_confidence =
                        excluded.combined_confidence,
                    confidence_adjustment =
                        excluded.confidence_adjustment,
                    risk_penalty =
                        excluded.risk_penalty,
                    freshness_score =
                        excluded.freshness_score,
                    freshness_penalty =
                        excluded.freshness_penalty,
                    market_score =
                        excluded.market_score,
                    intelligence_score =
                        excluded.intelligence_score,
                    prediction_ai_priority =
                        excluded.prediction_ai_priority,
                    opportunity_probability =
                        excluded.opportunity_probability,
                    expected_profit_usd =
                        excluded.expected_profit_usd,
                    expected_profit_score =
                        excluded.expected_profit_score,
                    trend_score =
                        excluded.trend_score,
                    stability_score =
                        excluded.stability_score,
                    downside_risk_score =
                        excluded.downside_risk_score,
                    intelligence_confidence =
                        excluded.intelligence_confidence,
                    prediction_confidence =
                        excluded.prediction_confidence,
                    total_scans =
                        excluded.total_scans,
                    liquidity_usd =
                        excluded.liquidity_usd,
                    volume_24h_usd =
                        excluded.volume_24h_usd,
                    pair_count =
                        excluded.pair_count,
                    last_scanned_at =
                        excluded.last_scanned_at,
                    metrics_updated_at =
                        excluded.metrics_updated_at,
                    intelligence_updated_at =
                        excluded.intelligence_updated_at,
                    prediction_updated_at =
                        excluded.prediction_updated_at,
                    ranking_updated_at =
                        excluded.ranking_updated_at
                """,
                (
                    record["mint"],
                    record["symbol"],
                    record["name"],
                    record["ai_opportunity_score"],
                    record["raw_opportunity_score"],
                    record["baseline_score"],
                    record["combined_confidence"],
                    record["confidence_adjustment"],
                    record["risk_penalty"],
                    record["freshness_score"],
                    record["freshness_penalty"],
                    record["market_score"],
                    record["intelligence_score"],
                    record["prediction_ai_priority"],
                    record[
                        "opportunity_probability"
                    ],
                    record["expected_profit_usd"],
                    record["expected_profit_score"],
                    record["trend_score"],
                    record["stability_score"],
                    record["downside_risk_score"],
                    record[
                        "intelligence_confidence"
                    ],
                    record["prediction_confidence"],
                    record["total_scans"],
                    record["liquidity_usd"],
                    record["volume_24h_usd"],
                    record["pair_count"],
                    record["last_scanned_at"],
                    record["metrics_updated_at"],
                    record[
                        "intelligence_updated_at"
                    ],
                    record[
                        "prediction_updated_at"
                    ],
                    record["ranking_updated_at"],
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


def refresh_ai_rankings(
    minimum_liquidity_usd=0,
    minimum_volume_24h_usd=0,
):
    """
    Recalculate AI opportunity rankings for all eligible tokens.
    """

    candidates = get_ranking_candidates(
        minimum_liquidity_usd=(
            minimum_liquidity_usd
        ),
        minimum_volume_24h_usd=(
            minimum_volume_24h_usd
        ),
        limit=MAXIMUM_RANKED_TOKENS,
    )

    records = [
        calculate_ai_ranking_record(
            candidate
        )
        for candidate in candidates
    ]

    saved_count = save_ai_rankings(
        records
    )

    return {
        "ranking_candidates_processed": len(
            candidates
        ),
        "ranking_records_saved": saved_count,
        "updated_at": current_timestamp(),
    }


def get_top_ai_ranked_tokens(
    limit=100,
    minimum_confidence=0,
    minimum_score=0,
):
    """
    Return tokens ordered by final AI opportunity score.
    """

    initialize_ai_ranking_table()

    limit = max(1, int(limit))

    minimum_confidence = clamp(
        minimum_confidence
    )

    minimum_score = clamp(
        minimum_score
    )

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT *
            FROM ai_rankings
            WHERE combined_confidence >= ?
              AND ai_opportunity_score >= ?
            ORDER BY
                ai_opportunity_score DESC,
                expected_profit_usd DESC,
                opportunity_probability DESC,
                combined_confidence DESC,
                downside_risk_score ASC,
                intelligence_score DESC,
                market_score DESC,
                symbol ASC,
                mint ASC
            LIMIT ?
            """,
            (
                minimum_confidence,
                minimum_score,
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


def get_ai_ranking_summary():
    """
    Return overall AI-ranking statistics.
    """

    initialize_ai_ranking_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_ranked_tokens,

                AVG(ai_opportunity_score)
                    AS average_ai_opportunity_score,

                MAX(ai_opportunity_score)
                    AS highest_ai_opportunity_score,

                AVG(combined_confidence)
                    AS average_combined_confidence,

                AVG(opportunity_probability)
                    AS average_opportunity_probability,

                AVG(expected_profit_usd)
                    AS average_expected_profit_usd,

                AVG(downside_risk_score)
                    AS average_downside_risk_score,

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

                MAX(ranking_updated_at)
                    AS last_updated_at

            FROM ai_rankings
            """
        )

        row = cursor.fetchone()

    finally:
        connection.close()

    if not row:
        return {
            "total_ranked_tokens": 0,
            "average_ai_opportunity_score": 0.0,
            "highest_ai_opportunity_score": 0.0,
            "average_combined_confidence": 0.0,
            "average_opportunity_probability": 0.0,
            "average_expected_profit_usd": 0.0,
            "average_downside_risk_score": 0.0,
            "positive_expected_profit_tokens": 0,
            "improving_tokens": 0,
            "last_updated_at": None,
        }

    result = dict(row)

    integer_fields = (
        "total_ranked_tokens",
        "positive_expected_profit_tokens",
        "improving_tokens",
    )

    float_fields = (
        "average_ai_opportunity_score",
        "highest_ai_opportunity_score",
        "average_combined_confidence",
        "average_opportunity_probability",
        "average_expected_profit_usd",
        "average_downside_risk_score",
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
    refresh_result = refresh_ai_rankings(
        minimum_liquidity_usd=50_000,
        minimum_volume_24h_usd=10_000,
    )

    print("\nAI Opportunity Ranking Engine refreshed.")

    print(
        "Ranking candidates processed: "
        f"{refresh_result['ranking_candidates_processed']:,}"
    )

    print(
        "Ranking records saved: "
        f"{refresh_result['ranking_records_saved']:,}"
    )

    top_tokens = get_top_ai_ranked_tokens(
        limit=10,
        minimum_confidence=0,
        minimum_score=0,
    )

    print("\nTop AI-ranked tokens:")

    for position, token in enumerate(
        top_tokens,
        start=1,
    ):
        print(
            f"{position}. "
            f"{token['symbol']} — "
            f"AI opportunity "
            f"{token['ai_opportunity_score']:.2f}/100, "
            f"expected profit "
            f"${token['expected_profit_usd']:.6f}, "
            f"opportunity "
            f"{token['opportunity_probability']:.2f}%, "
            f"confidence "
            f"{token['combined_confidence']:.2f}/100, "
            f"risk "
            f"{token['downside_risk_score']:.2f}/100"
        )