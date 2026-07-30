import sqlite3
from pathlib import Path


# This creates the database inside the database folder.
DATABASE_FILE = Path(__file__).resolve().parent / "trades.db"


def create_database():
    connection = sqlite3.connect(DATABASE_FILE)
    cursor = connection.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS paper_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            time TEXT NOT NULL,
            buy_route TEXT NOT NULL,
            sell_route TEXT NOT NULL,
            starting_amount REAL NOT NULL,
            ending_amount REAL NOT NULL,
            expected_profit REAL NOT NULL,
            decision TEXT NOT NULL
        )
        """
    )

    connection.commit()
    connection.close()


def save_trade(trade):
    connection = sqlite3.connect(DATABASE_FILE)
    cursor = connection.cursor()

    cursor.execute(
        """
        INSERT INTO paper_trades (
            time,
            buy_route,
            sell_route,
            starting_amount,
            ending_amount,
            expected_profit,
            decision
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            trade["time"],
            trade["buy"],
            trade["sell"],
            trade["starting_amount"],
            trade["ending_amount"],
            trade["profit"],
            trade["decision"],
        ),
    )

    connection.commit()
    connection.close()


def get_all_trades():
    connection = sqlite3.connect(DATABASE_FILE)
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            time,
            buy_route,
            sell_route,
            starting_amount,
            ending_amount,
            expected_profit,
            decision
        FROM paper_trades
        ORDER BY id DESC
        """
    )

    rows = cursor.fetchall()
    connection.close()

    return [dict(row) for row in rows]
def get_trade_statistics():
    connection = sqlite3.connect(DATABASE_FILE)
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT
            COUNT(*) AS total_trades,
            SUM(
                CASE
                    WHEN expected_profit > 0 THEN 1
                    ELSE 0
                END
            ) AS winning_trades,
            COALESCE(SUM(expected_profit), 0) AS total_profit,
            COALESCE(MAX(expected_profit), 0) AS best_trade,
            COALESCE(MIN(expected_profit), 0) AS worst_trade
        FROM paper_trades
        """
    )

    row = cursor.fetchone()
    connection.close()

    total_trades = row[0] or 0
    winning_trades = row[1] or 0
    total_profit = row[2] or 0
    best_trade = row[3] or 0
    worst_trade = row[4] or 0

    if total_trades > 0:
        win_rate = winning_trades / total_trades * 100
    else:
        win_rate = 0

    return {
        "total_trades": total_trades,
        "winning_trades": winning_trades,
        "total_profit": total_profit,
        "win_rate": win_rate,
        "best_trade": best_trade,
        "worst_trade": worst_trade,
    }
def get_profit_history():
    connection = sqlite3.connect(DATABASE_FILE)
    cursor = connection.cursor()

    cursor.execute(
        """
        SELECT expected_profit
        FROM paper_trades
        ORDER BY id
        """
    )

    rows = cursor.fetchall()
    connection.close()

    running_total = 0
    history = []

    for row in rows:
        running_total += row[0]
        history.append(running_total)

    return history