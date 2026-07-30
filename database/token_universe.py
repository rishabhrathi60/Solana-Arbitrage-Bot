import sqlite3
from datetime import datetime
from pathlib import Path


DATABASE = Path(__file__).resolve().parent / "trades.db"


def initialize_token_table():
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS token_universe (
            mint TEXT PRIMARY KEY,
            symbol TEXT,
            name TEXT,
            decimals INTEGER,
            enabled INTEGER DEFAULT 1,
            last_scan TEXT,
            successful_scans INTEGER DEFAULT 0,
            failed_scans INTEGER DEFAULT 0
        )
        """
    )

    conn.commit()
    conn.close()


def save_tokens(tokens):
    initialize_token_table()

    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

    for token in tokens:
        cursor.execute(
            """
            INSERT OR IGNORE INTO token_universe (
                mint,
                symbol,
                name,
                decimals
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                token["mint"],
                token["symbol"],
                token["name"],
                token["decimals"],
            ),
        )

    conn.commit()
    conn.close()


def get_enabled_tokens():
    initialize_token_table()

    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM token_universe
        WHERE enabled = 1
        ORDER BY symbol
        """
    )

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def get_token_batch(batch_size=20):
    """
    Return a rotating batch of enabled tokens.

    Tokens scanned least recently are returned first.
    """

    initialize_token_table()

    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    cursor.execute(
        """
        SELECT *
        FROM token_universe
        WHERE enabled = 1
        ORDER BY
            CASE
                WHEN last_scan IS NULL THEN 0
                ELSE 1
            END,
            last_scan ASC,
            symbol ASC
        LIMIT ?
        """,
        (batch_size,),
    )

    rows = cursor.fetchall()
    conn.close()

    return [dict(row) for row in rows]


def mark_token_scanned(mint, successful):
    """
    Update a token's scan time and success/failure counter.
    """

    initialize_token_table()

    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

    scanned_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if successful:
        cursor.execute(
            """
            UPDATE token_universe
            SET
                last_scan = ?,
                successful_scans = successful_scans + 1
            WHERE mint = ?
            """,
            (
                scanned_at,
                mint,
            ),
        )
    else:
        cursor.execute(
            """
            UPDATE token_universe
            SET
                last_scan = ?,
                failed_scans = failed_scans + 1
            WHERE mint = ?
            """,
            (
                scanned_at,
                mint,
            ),
        )

    conn.commit()
    conn.close()