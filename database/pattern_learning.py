import sqlite3
from datetime import datetime
from pathlib import Path

DATABASE = Path(__file__).resolve().parent / "trades.db"


def get_connection():
    connection = sqlite3.connect(DATABASE)
    connection.row_factory = sqlite3.Row
    return connection


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def initialize_pattern_learning():
    """
    Stores everything the AI learns about each token over time.
    """

    connection = get_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS pattern_learning (

            mint TEXT PRIMARY KEY,

            symbol TEXT,

            total_scans INTEGER DEFAULT 0,

            successful_quotes INTEGER DEFAULT 0,

            profitable_quotes INTEGER DEFAULT 0,

            eligible_quotes INTEGER DEFAULT 0,

            average_profit REAL DEFAULT 0,

            best_profit REAL DEFAULT 0,

            worst_profit REAL DEFAULT 0,

            average_market_score REAL DEFAULT 0,

            average_ai_score REAL DEFAULT 0,

            average_prediction REAL DEFAULT 0,

            average_confidence REAL DEFAULT 0,

            average_risk REAL DEFAULT 0,

            last_seen TEXT,

            updated_at TEXT
        )
        """
    )

    connection.commit()
    connection.close()


def safe_float(value):

    try:
        return float(value or 0)

    except Exception:
        return 0.0


def safe_int(value):

    try:
        return int(value or 0)

    except Exception:
        return 0


def update_learning(results):
    """
    Update long-term AI memory after every scan cycle.
    """

    initialize_pattern_learning()

    if not results:
        return

    connection = get_connection()
    cursor = connection.cursor()

    for result in results:

        mint = result.get("mint")

        if not mint:
            continue

        symbol = result.get("token", "UNKNOWN")

        cursor.execute(
            """
            SELECT *
            FROM pattern_learning
            WHERE mint=?
            """,
            (mint,),
        )

        existing = cursor.fetchone()

        profit = safe_float(result.get("net_profit"))

        market = safe_float(result.get("market_score"))

        ai = safe_float(
            result.get(
                "ai_opportunity_score",
                result.get("intelligence_score"),
            )
        )

        prediction = safe_float(
            result.get(
                "opportunity_probability"
            )
        )

        confidence = safe_float(
            result.get(
                "combined_confidence",
                result.get("confidence_score"),
            )
        )

        risk = safe_float(
            result.get("downside_risk_score")
        )

        success = (
            result.get("decision")
            != "⚠️ QUOTE ERROR"
        )

        eligible = bool(result.get("eligible"))

        profitable = success and profit > 0

        if existing:

            scans = existing["total_scans"] + 1

            avg_profit = (
                existing["average_profit"]
                * existing["total_scans"]
                + profit
            ) / scans

            avg_market = (
                existing["average_market_score"]
                * existing["total_scans"]
                + market
            ) / scans

            avg_ai = (
                existing["average_ai_score"]
                * existing["total_scans"]
                + ai
            ) / scans

            avg_prediction = (
                existing["average_prediction"]
                * existing["total_scans"]
                + prediction
            ) / scans

            avg_confidence = (
                existing["average_confidence"]
                * existing["total_scans"]
                + confidence
            ) / scans

            avg_risk = (
                existing["average_risk"]
                * existing["total_scans"]
                + risk
            ) / scans

            cursor.execute(
                """
                UPDATE pattern_learning
                SET

                total_scans=?,

                successful_quotes=?,

                profitable_quotes=?,

                eligible_quotes=?,

                average_profit=?,

                best_profit=?,

                worst_profit=?,

                average_market_score=?,

                average_ai_score=?,

                average_prediction=?,

                average_confidence=?,

                average_risk=?,

                last_seen=?,

                updated_at=?

                WHERE mint=?
                """,
                (
                    scans,

                    existing["successful_quotes"]
                    + int(success),

                    existing["profitable_quotes"]
                    + int(profitable),

                    existing["eligible_quotes"]
                    + int(eligible),

                    avg_profit,

                    max(
                        existing["best_profit"],
                        profit,
                    ),

                    min(
                        existing["worst_profit"],
                        profit,
                    ),

                    avg_market,

                    avg_ai,

                    avg_prediction,

                    avg_confidence,

                    avg_risk,

                    now(),

                    now(),

                    mint,
                ),
            )

        else:

            cursor.execute(
                """
                INSERT INTO pattern_learning (

                mint,

                symbol,

                total_scans,

                successful_quotes,

                profitable_quotes,

                eligible_quotes,

                average_profit,

                best_profit,

                worst_profit,

                average_market_score,

                average_ai_score,

                average_prediction,

                average_confidence,

                average_risk,

                last_seen,

                updated_at

                )

                VALUES (

                ?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?

                )
                """,
                (
                    mint,

                    symbol,

                    1,

                    int(success),

                    int(profitable),

                    int(eligible),

                    profit,

                    profit,

                    profit,

                    market,

                    ai,

                    prediction,

                    confidence,

                    risk,

                    now(),

                    now(),
                ),
            )

    connection.commit()
    connection.close()


def get_best_patterns(limit=25):

    initialize_pattern_learning()

    connection = get_connection()

    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT *

        FROM pattern_learning

        WHERE total_scans>=5

        ORDER BY

        average_profit DESC,

        profitable_quotes DESC,

        average_confidence DESC,

        average_ai_score DESC

        LIMIT ?
        """,
        (limit,),
    )

    rows = cursor.fetchall()

    connection.close()

    return [dict(r) for r in rows]