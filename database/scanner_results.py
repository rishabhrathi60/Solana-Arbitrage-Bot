import sqlite3
from datetime import datetime
from pathlib import Path


DATABASE_FILE = Path(__file__).resolve().parent / "trades.db"


def create_scanner_table():
    connection = sqlite3.connect(DATABASE_FILE)
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
            scanned_at TEXT NOT NULL
        )
        """
    )

    connection.commit()
    connection.close()


def save_scanner_results(results):
    create_scanner_table()

    connection = sqlite3.connect(DATABASE_FILE)
    cursor = connection.cursor()

    scanned_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for result in results:
        cursor.execute(
            """
            INSERT OR REPLACE INTO scanner_results (
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
                scanned_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                result["token"],
                result["buy_route"],
                result["sell_route"],
                result["starting_amount"],
                result["ending_amount"],
                result["quoted_profit"],
                result["estimated_cost"],
                result["net_profit"],
                result["decision"],
                int(result["eligible"]),
                result.get("error", ""),
                scanned_at,
            ),
        )

    connection.commit()
    connection.close()


def get_latest_scanner_results():
    create_scanner_table()

    connection = sqlite3.connect(DATABASE_FILE)
    connection.row_factory = sqlite3.Row
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
            scanned_at
        FROM scanner_results
        ORDER BY
            CASE
                WHEN decision = '⚠️ QUOTE ERROR' THEN 1
                ELSE 0
            END,
            net_profit DESC
        """
    )

    rows = cursor.fetchall()
    connection.close()

    return [dict(row) for row in rows]