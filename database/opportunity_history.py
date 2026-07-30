import sqlite3
from datetime import datetime
from pathlib import Path


DATABASE_FILE = Path(__file__).resolve().parent / "trades.db"


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


def create_opportunity_history_table():
    """
    Create the permanent scanner-history table and indexes.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS opportunity_history (
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

    connection.commit()
    connection.close()


def save_opportunity_history(results):
    """
    Permanently save every scanner result.

    Unlike scanner_results, rows in this table are never replaced.
    Each scan becomes a new historical observation.
    """

    create_opportunity_history_table()

    if not results:
        return 0

    connection = get_database_connection()
    cursor = connection.cursor()

    scanned_at = current_timestamp()
    saved_count = 0

    try:
        for result in results:
            decision = (
                result.get("decision")
                or "⚠️ QUOTE ERROR"
            )

            quote_successful = int(
                decision != "⚠️ QUOTE ERROR"
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
                    result.get("token") or "UNKNOWN",
                    result.get("buy_route") or "Unavailable",
                    result.get("sell_route") or "Unavailable",
                    float(
                        result.get("starting_amount") or 0
                    ),
                    float(
                        result.get("ending_amount") or 0
                    ),
                    float(
                        result.get("quoted_profit") or 0
                    ),
                    float(
                        result.get("estimated_cost") or 0
                    ),
                    float(
                        result.get("net_profit") or 0
                    ),
                    decision,
                    int(
                        bool(result.get("eligible"))
                    ),
                    quote_successful,
                    result.get("error") or "",
                    float(
                        result.get("market_score") or 0
                    ),
                    float(
                        result.get("liquidity_score") or 0
                    ),
                    float(
                        result.get("volume_score") or 0
                    ),
                    float(
                        result.get("pair_score") or 0
                    ),
                    scanned_at,
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
    Return the most recent historical scanner observations.
    """

    create_opportunity_history_table()

    limit = max(1, int(limit))

    connection = get_database_connection()
    cursor = connection.cursor()

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

    rows = cursor.fetchall()
    connection.close()

    return [
        dict(row)
        for row in rows
    ]


def get_token_performance(
    minimum_scans=1,
    limit=100,
):
    """
    Return aggregated historical performance by token.

    Rankings prioritize:
    1. Average net profit.
    2. Best observed net profit.
    3. Quote-success rate.
    4. Market score.
    """

    create_opportunity_history_table()

    minimum_scans = max(
        1,
        int(minimum_scans),
    )
    limit = max(1, int(limit))

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            token,
            COUNT(*) AS total_scans,
            SUM(quote_successful) AS successful_quotes,
            SUM(
                CASE
                    WHEN quote_successful = 0
                    THEN 1
                    ELSE 0
                END
            ) AS quote_errors,
            SUM(eligible) AS eligible_scans,
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
            AVG(market_score) AS average_market_score,
            MAX(scanned_at) AS last_scanned_at,
            (
                SUM(quote_successful) * 100.0
                / COUNT(*)
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
                / NULLIF(SUM(quote_successful), 0)
            ) AS profitable_scan_rate
        FROM opportunity_history
        GROUP BY token
        HAVING COUNT(*) >= ?
        ORDER BY
            COALESCE(average_net_profit, -999999) DESC,
            COALESCE(best_net_profit, -999999) DESC,
            quote_success_rate DESC,
            average_market_score DESC
        LIMIT ?
        """,
        (
            minimum_scans,
            limit,
        ),
    )

    rows = cursor.fetchall()
    connection.close()

    return [
        dict(row)
        for row in rows
    ]


def get_top_opportunity_tokens(
    minimum_scans=3,
    limit=20,
):
    """
    Return tokens with the strongest historical performance.
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

    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_observations,
            COUNT(DISTINCT token) AS unique_tokens,
            SUM(quote_successful) AS successful_quotes,
            SUM(
                CASE
                    WHEN quote_successful = 0
                    THEN 1
                    ELSE 0
                END
            ) AS quote_errors,
            SUM(eligible) AS eligible_observations,
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
            MAX(net_profit) AS best_net_profit,
            MIN(
                CASE
                    WHEN quote_successful = 1
                    THEN net_profit
                    ELSE NULL
                END
            ) AS worst_net_profit,
            MAX(scanned_at) AS last_scanned_at
        FROM opportunity_history
        """
    )

    row = cursor.fetchone()
    connection.close()

    if not row:
        return {
            "total_observations": 0,
            "unique_tokens": 0,
            "successful_quotes": 0,
            "quote_errors": 0,
            "eligible_observations": 0,
            "profitable_observations": 0,
            "average_net_profit": 0.0,
            "best_net_profit": 0.0,
            "worst_net_profit": 0.0,
            "last_scanned_at": None,
        }

    result = dict(row)

    for key in (
        "total_observations",
        "unique_tokens",
        "successful_quotes",
        "quote_errors",
        "eligible_observations",
        "profitable_observations",
    ):
        result[key] = int(
            result.get(key) or 0
        )

    for key in (
        "average_net_profit",
        "best_net_profit",
        "worst_net_profit",
    ):
        result[key] = float(
            result.get(key) or 0
        )

    return result