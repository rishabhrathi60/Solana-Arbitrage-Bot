import sqlite3
from datetime import datetime
from pathlib import Path


DATABASE_FILE = Path(__file__).resolve().parent / "trades.db"


SCANNER_RESULT_COLUMNS = {
    "market_score": "REAL NOT NULL DEFAULT 0",
    "liquidity_score": "REAL NOT NULL DEFAULT 0",
    "volume_score": "REAL NOT NULL DEFAULT 0",
    "pair_score": "REAL NOT NULL DEFAULT 0",
}


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


def create_scanner_table():
    """
    Create or migrate the latest scanner-results table.
    """

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS scanner_results (
            token TEXT PRIMARY KEY,
            buy_route TEXT NOT NULL,
            sell_route TEXT NOT NULL,
            starting_amount REAL NOT NULL,
            ending_amount REAL NOT NULL,
            quoted_profit REAL NOT NULL,
            estimated_cost REAL NOT NULL,
            net_profit REAL NOT NULL,
            decision TEXT NOT NULL,
            eligible INTEGER NOT NULL,
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
        PRAGMA table_info(scanner_results)
        """
    )

    existing_columns = {
        row["name"]
        for row in cursor.fetchall()
    }

    for column_name, column_definition in (
        SCANNER_RESULT_COLUMNS.items()
    ):
        if column_name in existing_columns:
            continue

        cursor.execute(
            f"""
            ALTER TABLE scanner_results
            ADD COLUMN {column_name}
            {column_definition}
            """
        )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_scanner_results_scanned_at
        ON scanner_results(scanned_at)
        """
    )

    cursor.execute(
        """
        CREATE INDEX IF NOT EXISTS
        idx_scanner_results_net_profit
        ON scanner_results(net_profit)
        """
    )

    connection.commit()
    connection.close()


def save_scanner_results(results):
    """
    Save the latest result for each token.

    This table acts as a current snapshot. Historical results
    are stored separately in opportunity_history.
    """

    create_scanner_table()

    if not results:
        return 0

    connection = get_database_connection()
    cursor = connection.cursor()

    scanned_at = datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    saved_count = 0

    try:
        for result in results:
            cursor.execute(
                """
                INSERT INTO scanner_results (
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
                    error,
                    market_score,
                    liquidity_score,
                    volume_score,
                    pair_score,
                    scanned_at
                )
                VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?
                )
                ON CONFLICT(token) DO UPDATE SET
                    buy_route = excluded.buy_route,
                    sell_route = excluded.sell_route,
                    starting_amount =
                        excluded.starting_amount,
                    ending_amount =
                        excluded.ending_amount,
                    quoted_profit =
                        excluded.quoted_profit,
                    estimated_cost =
                        excluded.estimated_cost,
                    net_profit = excluded.net_profit,
                    decision = excluded.decision,
                    eligible = excluded.eligible,
                    error = excluded.error,
                    market_score =
                        excluded.market_score,
                    liquidity_score =
                        excluded.liquidity_score,
                    volume_score =
                        excluded.volume_score,
                    pair_score =
                        excluded.pair_score,
                    scanned_at = excluded.scanned_at
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
                    (
                        result.get("decision")
                        or "⚠️ QUOTE ERROR"
                    ),
                    int(
                        bool(result.get("eligible"))
                    ),
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


def get_latest_scanner_results():
    """
    Return the latest scanner snapshot.
    """

    create_scanner_table()

    connection = get_database_connection()
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
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
            error,
            market_score,
            liquidity_score,
            volume_score,
            pair_score,
            scanned_at
        FROM scanner_results
        ORDER BY
            CASE
                WHEN decision = '⚠️ QUOTE ERROR'
                THEN 1
                ELSE 0
            END ASC,
            net_profit DESC,
            market_score DESC,
            token ASC
        """
    )

    rows = cursor.fetchall()
    connection.close()

    return [
        dict(row)
        for row in rows
    ]