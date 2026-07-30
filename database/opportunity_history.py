import sqlite3
from datetime import datetime
from pathlib import Path


DATABASE_FILE = (
    Path(__file__).resolve().parent
    / "trades.db"
)


def get_database_connection():
    """
    Create a SQLite connection that returns
    dictionary-like rows.
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


def create_opportunity_history_table():
    """
    Create the permanent scanner-history table
    and supporting indexes.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS
            opportunity_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT NOT NULL,
                buy_route TEXT,
                sell_route TEXT,
                starting_amount REAL NOT NULL DEFAULT 0,
                ending_amount REAL NOT NULL DEFAULT 0,
                quoted_profit REAL NOT NULL DEFAULT 0,
                estimated_cost REAL NOT NULL DEFAULT 0,
                net_profit REAL NOT NULL DEFAULT 0,
                decision TEXT NOT NULL,
                eligible INTEGER NOT NULL DEFAULT 0,
                quote_successful INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                market_score REAL NOT NULL DEFAULT 0,
                liquidity_score REAL NOT NULL DEFAULT 0,
                volume_score REAL NOT NULL DEFAULT 0,
                pair_score REAL NOT NULL DEFAULT 0,
                scanned_at TEXT NOT NULL
            )
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_opportunity_history_token
            ON opportunity_history(token)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_opportunity_history_scanned_at
            ON opportunity_history(scanned_at)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_opportunity_history_net_profit
            ON opportunity_history(net_profit)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_opportunity_history_success
            ON opportunity_history(quote_successful)
            """
        )

        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS
            idx_opportunity_history_eligible
            ON opportunity_history(eligible)
            """
        )

        connection.commit()

    finally:
        connection.close()


def safe_float(value):
    """
    Convert a value to float safely.
    """

    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def determine_quote_success(result):
    """
    Determine whether the scanner successfully
    received and processed a quote.
    """

    if "quote_successful" in result:
        return int(
            bool(result.get("quote_successful"))
        )

    error = str(
        result.get("error") or ""
    ).strip()

    if error:
        return 0

    decision = str(
        result.get("decision") or ""
    ).strip().upper()

    quote_error_indicators = (
        "QUOTE ERROR",
        "QUOTE FAILED",
        "REQUEST ERROR",
        "JUPITER ERROR",
        "NO QUOTE",
    )

    for indicator in quote_error_indicators:
        if indicator in decision:
            return 0

    return 1


def save_opportunity_history(results):
    """
    Permanently save every scanner result.

    Rows in this table are never replaced.
    Every scanner batch creates new historical
    observations.
    """

    create_opportunity_history_table()

    if not results:
        return 0

    connection = get_database_connection()
    cursor = connection.cursor()

    batch_timestamp = current_timestamp()
    saved_count = 0

    try:
        for result in results:
            error_message = str(
                result.get("error") or ""
            ).strip()

            quote_successful = (
                determine_quote_success(result)
            )

            decision = str(
                result.get("decision")
                or (
                    "⚠️ QUOTE ERROR"
                    if not quote_successful
                    else "NO DECISION"
                )
            )

            eligible = int(
                bool(result.get("eligible"))
                and quote_successful
            )

            scanned_at = (
                result.get("scanned_at")
                or batch_timestamp
            )

            cursor.execute(
                """
                INSERT INTO opportunity_history (
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
                    scanned_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                (
                    str(
                        result.get("token")
                        or "UNKNOWN"
                    ),
                    str(
                        result.get("buy_route")
                        or "Unavailable"
                    ),
                    str(
                        result.get("sell_route")
                        or "Unavailable"
                    ),
                    safe_float(
                        result.get(
                            "starting_amount"
                        )
                    ),
                    safe_float(
                        result.get(
                            "ending_amount"
                        )
                    ),
                    safe_float(
                        result.get(
                            "quoted_profit"
                        )
                    ),
                    safe_float(
                        result.get(
                            "estimated_cost"
                        )
                    ),
                    safe_float(
                        result.get("net_profit")
                    ),
                    decision,
                    eligible,
                    quote_successful,
                    error_message,
                    safe_float(
                        result.get("market_score")
                    ),
                    safe_float(
                        result.get(
                            "liquidity_score"
                        )
                    ),
                    safe_float(
                        result.get("volume_score")
                    ),
                    safe_float(
                        result.get("pair_score")
                    ),
                    str(scanned_at),
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


def get_recent_opportunity_history(limit=100):
    """
    Return the most recent historical
    scanner observations.
    """

    create_opportunity_history_table()

    limit = max(1, int(limit))

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
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
                scanned_at
            FROM opportunity_history
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        )

        return [
            dict(row)
            for row in cursor.fetchall()
        ]

    finally:
        connection.close()


def get_token_performance(
    minimum_scans=1,
    limit=100,
):
    """
    Return aggregated historical performance
    for each token.
    """

    create_opportunity_history_table()

    minimum_scans = max(
        1,
        int(minimum_scans),
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
                token,
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
                        WHEN net_profit > 0
                         AND quote_successful = 1
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

                AVG(
                    CASE
                        WHEN quote_successful = 1
                        THEN market_score
                        ELSE NULL
                    END
                ) AS average_market_score,

                MAX(scanned_at) AS last_scanned_at,

                (
                    COALESCE(
                        SUM(quote_successful),
                        0
                    ) * 100.0
                    / NULLIF(COUNT(*), 0)
                ) AS quote_success_rate,

                (
                    SUM(
                        CASE
                            WHEN net_profit > 0
                             AND quote_successful = 1
                            THEN 1
                            ELSE 0
                        END
                    ) * 100.0
                    / NULLIF(
                        SUM(quote_successful),
                        0
                    )
                ) AS profitable_scan_rate,

                (
                    COALESCE(
                        SUM(eligible),
                        0
                    ) * 100.0
                    / NULLIF(
                        SUM(quote_successful),
                        0
                    )
                ) AS eligible_scan_rate

            FROM opportunity_history
            GROUP BY token
            HAVING COUNT(*) >= ?
            ORDER BY
                COALESCE(
                    average_net_profit,
                    -999999
                ) DESC,
                COALESCE(
                    best_net_profit,
                    -999999
                ) DESC,
                quote_success_rate DESC,
                average_market_score DESC
            LIMIT ?
            """,
            (
                minimum_scans,
                limit,
            ),
        )

        return [
            dict(row)
            for row in cursor.fetchall()
        ]

    finally:
        connection.close()


def get_top_opportunity_tokens(
    minimum_scans=3,
    limit=20,
):
    """
    Return tokens with the strongest
    historical performance.
    """

    return get_token_performance(
        minimum_scans=minimum_scans,
        limit=limit,
    )


def get_opportunity_history_summary():
    """
    Return overall historical scanner statistics.
    """

    create_opportunity_history_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                COUNT(*) AS total_observations,

                COUNT(
                    DISTINCT token
                ) AS unique_tokens,

                COUNT(
                    DISTINCT scanned_at
                ) AS scan_cycles,

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
                ) AS eligible_observations,

                SUM(
                    CASE
                        WHEN net_profit > 0
                         AND quote_successful = 1
                        THEN 1
                        ELSE 0
                    END
                ) AS profitable_observations,

                AVG(
                    CASE
                        WHEN quote_successful = 1
                        THEN net_profit
                        ELSE NULL
                    END
                ) AS average_net_profit,

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

                AVG(
                    CASE
                        WHEN quote_successful = 1
                        THEN market_score
                        ELSE NULL
                    END
                ) AS average_market_score,

                MAX(scanned_at) AS last_scanned_at

            FROM opportunity_history
            """
        )

        row = cursor.fetchone()

    finally:
        connection.close()

    if not row:
        return {
            "total_observations": 0,
            "unique_tokens": 0,
            "scan_cycles": 0,
            "successful_quotes": 0,
            "quote_errors": 0,
            "eligible_observations": 0,
            "profitable_observations": 0,
            "average_net_profit": 0.0,
            "best_net_profit": 0.0,
            "worst_net_profit": 0.0,
            "average_market_score": 0.0,
            "last_scanned_at": None,
        }

    result = dict(row)

    integer_fields = (
        "total_observations",
        "unique_tokens",
        "scan_cycles",
        "successful_quotes",
        "quote_errors",
        "eligible_observations",
        "profitable_observations",
    )

    float_fields = (
        "average_net_profit",
        "best_net_profit",
        "worst_net_profit",
        "average_market_score",
    )

    for key in integer_fields:
        result[key] = int(
            result.get(key) or 0
        )

    for key in float_fields:
        result[key] = float(
            result.get(key) or 0
        )

    return result