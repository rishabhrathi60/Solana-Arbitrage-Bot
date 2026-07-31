import sqlite3
from pathlib import Path
from datetime import datetime

DATABASE = Path(__file__).resolve().parent / "trades.db"


def get_connection():
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row
    return conn


def now():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def initialize_context_engine():
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
    CREATE TABLE IF NOT EXISTS market_context (

        id INTEGER PRIMARY KEY AUTOINCREMENT,

        scan_time TEXT,

        average_market_score REAL,

        average_ai_score REAL,

        average_prediction REAL,

        average_confidence REAL,

        average_profit REAL,

        profitable_rate REAL,

        eligible_rate REAL,

        quote_success_rate REAL,

        scanner_speed REAL,

        tokens_scanned INTEGER,

        profitable_quotes INTEGER,

        eligible_quotes INTEGER,

        successful_quotes INTEGER,

        market_quality REAL,

        created_at TEXT
    )
    """)

    conn.commit()
    conn.close()


def safe(value):
    try:
        return float(value or 0)
    except:
        return 0.0


def save_context(results, elapsed_seconds):

    initialize_context_engine()

    if not results:
        return

    conn = get_connection()
    cur = conn.cursor()

    market_scores = []
    ai_scores = []
    prediction_scores = []
    confidence_scores = []
    profits = []

    successful = 0
    eligible = 0
    profitable = 0

    for r in results:

        market_scores.append(
            safe(r.get("market_score"))
        )

        ai_scores.append(
            safe(
                r.get(
                    "ai_opportunity_score",
                    r.get(
                        "intelligence_score"
                    )
                )
            )
        )

        prediction_scores.append(
            safe(
                r.get(
                    "opportunity_probability"
                )
            )
        )

        confidence_scores.append(
            safe(
                r.get(
                    "combined_confidence",
                    r.get(
                        "prediction_confidence"
                    )
                )
            )
        )

        profit = safe(
            r.get("net_profit")
        )

        profits.append(profit)

        if r.get("decision") != "⚠️ QUOTE ERROR":
            successful += 1

        if r.get("eligible"):
            eligible += 1

        if profit > 0:
            profitable += 1

    scanned = len(results)

    avg_market = sum(market_scores)/max(1,len(market_scores))
    avg_ai = sum(ai_scores)/max(1,len(ai_scores))
    avg_prediction = sum(prediction_scores)/max(1,len(prediction_scores))
    avg_confidence = sum(confidence_scores)/max(1,len(confidence_scores))
    avg_profit = sum(profits)/max(1,len(profits))

    profitable_rate = profitable*100/max(1,successful)
    eligible_rate = eligible*100/max(1,successful)
    success_rate = successful*100/max(1,scanned)

    speed = scanned/max(1,elapsed_seconds)*60

    market_quality = (

        avg_market*.25+

        avg_ai*.25+

        avg_prediction*.20+

        avg_confidence*.15+

        success_rate*.15

    )

    cur.execute("""
    INSERT INTO market_context(

        scan_time,

        average_market_score,

        average_ai_score,

        average_prediction,

        average_confidence,

        average_profit,

        profitable_rate,

        eligible_rate,

        quote_success_rate,

        scanner_speed,

        tokens_scanned,

        profitable_quotes,

        eligible_quotes,

        successful_quotes,

        market_quality,

        created_at

    )

    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)

    """,(

        now(),

        avg_market,

        avg_ai,

        avg_prediction,

        avg_confidence,

        avg_profit,

        profitable_rate,

        eligible_rate,

        success_rate,

        speed,

        scanned,

        profitable,

        eligible,

        successful,

        market_quality,

        now()

    ))

    conn.commit()
    conn.close()


def get_best_market_conditions(limit=20):

    initialize_context_engine()

    conn=get_connection()

    cur=conn.cursor()

    cur.execute("""

    SELECT *

    FROM market_context

    ORDER BY

    market_quality DESC,

    average_profit DESC,

    profitable_rate DESC

    LIMIT ?

    """,(limit,))

    rows=cur.fetchall()

    conn.close()

    return [dict(r) for r in rows]


def get_context_summary():

    initialize_context_engine()

    conn=get_connection()

    cur=conn.cursor()

    cur.execute("""

    SELECT

    COUNT(*) total_cycles,

    AVG(market_quality) avg_market_quality,

    AVG(average_profit) avg_profit,

    AVG(scanner_speed) avg_speed,

    AVG(quote_success_rate) avg_success,

    MAX(market_quality) best_market,

    MAX(created_at) updated

    FROM market_context

    """)

    row=cur.fetchone()

    conn.close()

    return dict(row)