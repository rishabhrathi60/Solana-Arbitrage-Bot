import sqlite3
import sys
from pathlib import Path

import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh


# This helps the dashboard find files in the main project folder.
PROJECT_FOLDER = Path(__file__).resolve().parent.parent

if str(PROJECT_FOLDER) not in sys.path:
    sys.path.append(str(PROJECT_FOLDER))


from config import (  # noqa: E402
    BOT_NAME,
    LIVE_TRADING,
    MAX_DAILY_LOSS_USD,
    MIN_PROFIT_USD,
    TRADE_AMOUNT_USD,
)
from dashboard.analytics import (  # noqa: E402
    render_historical_analytics,
)
from database.scanner_results import (  # noqa: E402
    get_latest_scanner_results,
)
from database.token_metrics import (  # noqa: E402
    DATABASE,
    count_scanner_tokens,
    get_liquid_tokens,
    get_metrics_progress,
    get_scanner_rotation_status,
)
from database.trades import (  # noqa: E402
    get_profit_history,
    get_trade_statistics,
)
from execution.paper_trader import (  # noqa: E402
    get_trade_history,
)
from market_data import get_crypto_prices  # noqa: E402
from strategies.arbitrage import (  # noqa: E402
    find_best_opportunity,
)


# These must match the filtered scanner settings.
SCANNER_MINIMUM_LIQUIDITY_USD = 50_000
SCANNER_MINIMUM_VOLUME_24H_USD = 10_000

TOP_TOKEN_LIMIT = 10


st.set_page_config(
    page_title=BOT_NAME,
    page_icon="🤖",
    layout="wide",
)

# Refresh the dashboard every 3 minutes.
st_autorefresh(
    interval=180000,
    key="dashboard_refresh",
)

st.title("🤖 Rishabh Multi-Strategy Trading Bot")
st.caption(
    "Automatic paper-trading dashboard — "
    "live trading is turned off"
)


def get_metrics_database_summary():
    """
    Return additional token-metrics database statistics.
    """

    connection = sqlite3.connect(
        DATABASE,
        timeout=30,
    )
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                MAX(metrics_updated_at) AS last_metrics_refresh,
                COUNT(*) AS stored_metric_rows,
                SUM(
                    CASE
                        WHEN pair_count > 0
                        THEN 1
                        ELSE 0
                    END
                ) AS tokens_with_pairs,
                SUM(
                    CASE
                        WHEN liquidity_usd > 0
                        THEN 1
                        ELSE 0
                    END
                ) AS tokens_with_liquidity,
                SUM(
                    CASE
                        WHEN volume_24h_usd > 0
                        THEN 1
                        ELSE 0
                    END
                ) AS tokens_with_volume
            FROM token_metrics
            """
        )

        row = cursor.fetchone()

        if not row:
            return {
                "last_metrics_refresh": None,
                "stored_metric_rows": 0,
                "tokens_with_pairs": 0,
                "tokens_with_liquidity": 0,
                "tokens_with_volume": 0,
            }

        return {
            "last_metrics_refresh": row[
                "last_metrics_refresh"
            ],
            "stored_metric_rows": (
                row["stored_metric_rows"] or 0
            ),
            "tokens_with_pairs": (
                row["tokens_with_pairs"] or 0
            ),
            "tokens_with_liquidity": (
                row["tokens_with_liquidity"] or 0
            ),
            "tokens_with_volume": (
                row["tokens_with_volume"] or 0
            ),
        }

    finally:
        connection.close()


def get_top_tokens_by_volume(limit=10):
    """
    Return tokens with the highest stored 24-hour volume.
    """

    limit = max(1, int(limit))

    connection = sqlite3.connect(
        DATABASE,
        timeout=30,
    )
    connection.row_factory = sqlite3.Row
    cursor = connection.cursor()

    try:
        cursor.execute(
            """
            SELECT
                token_universe.symbol,
                token_universe.name,
                token_universe.mint,
                token_metrics.price_usd,
                token_metrics.liquidity_usd,
                token_metrics.volume_24h_usd,
                token_metrics.pair_count,
                token_metrics.best_dex,
                token_metrics.metrics_updated_at
            FROM token_metrics
            INNER JOIN token_universe
                ON token_universe.mint =
                   token_metrics.mint
            WHERE token_universe.enabled = 1
              AND COALESCE(
                    token_universe.failed_scans,
                    0
                  ) < 3
              AND token_metrics.pair_count > 0
              AND token_metrics.volume_24h_usd > 0
            ORDER BY
                token_metrics.volume_24h_usd DESC,
                token_metrics.liquidity_usd DESC
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


def format_token_table(tokens):
    """
    Convert token dictionaries into dashboard table rows.
    """

    rows = []

    for token in tokens:
        rows.append(
            {
                "Symbol": (
                    token.get("symbol") or "UNKNOWN"
                ),
                "Name": (
                    token.get("name") or "Unknown"
                ),
                "Price USD": round(
                    token.get("price_usd") or 0,
                    8,
                ),
                "Liquidity USD": round(
                    token.get("liquidity_usd") or 0,
                    2,
                ),
                "24h Volume USD": round(
                    token.get("volume_24h_usd") or 0,
                    2,
                ),
                "Pairs": (
                    token.get("pair_count") or 0
                ),
                "Best DEX": (
                    token.get("best_dex")
                    or "Unknown"
                ),
                "Last Updated": (
                    token.get("metrics_updated_at")
                    or "Unknown"
                ),
            }
        )

    return rows


# -----------------------------
# Market-data system overview
# -----------------------------
st.subheader("🌐 Market Data System")

try:
    metrics_progress = get_metrics_progress()

    eligible_token_count = count_scanner_tokens(
        minimum_liquidity_usd=(
            SCANNER_MINIMUM_LIQUIDITY_USD
        ),
        minimum_volume_24h_usd=(
            SCANNER_MINIMUM_VOLUME_24H_USD
        ),
    )

    rotation_status = get_scanner_rotation_status(
        minimum_liquidity_usd=(
            SCANNER_MINIMUM_LIQUIDITY_USD
        ),
        minimum_volume_24h_usd=(
            SCANNER_MINIMUM_VOLUME_24H_USD
        ),
    )

    metrics_summary = get_metrics_database_summary()

    total_enabled = metrics_progress[
        "total_enabled"
    ]
    tokens_with_metrics = metrics_progress[
        "tokens_with_metrics"
    ]
    tokens_remaining = metrics_progress[
        "tokens_remaining"
    ]

    coverage_percentage = (
        tokens_with_metrics
        / total_enabled
        * 100
        if total_enabled
        else 0.0
    )

    (
        market_stat1,
        market_stat2,
        market_stat3,
        market_stat4,
    ) = st.columns(4)

    market_stat1.metric(
        label="Metrics Coverage",
        value=f"{coverage_percentage:.2f}%",
        help=(
            f"{tokens_with_metrics:,} of "
            f"{total_enabled:,} enabled tokens"
        ),
    )

    market_stat2.metric(
        label="Tokens With Metrics",
        value=f"{tokens_with_metrics:,}",
        delta=f"{tokens_remaining:,} remaining",
        delta_color="off",
    )

    market_stat3.metric(
        label="Eligible Scanner Tokens",
        value=f"{eligible_token_count:,}",
        help=(
            "Tokens meeting the current liquidity, "
            "volume and pair requirements."
        ),
    )

    current_offset = rotation_status[
        "current_offset"
    ]

    market_stat4.metric(
        label="Scanner Rotation Position",
        value=(
            f"{current_offset:,} / "
            f"{eligible_token_count:,}"
        ),
        help=(
            "The filtered scanner continues from "
            "this saved database offset."
        ),
    )

    detail1, detail2, detail3, detail4 = (
        st.columns(4)
    )

    detail1.metric(
        label="Tokens With Active Pairs",
        value=(
            f"{metrics_summary['tokens_with_pairs']:,}"
        ),
    )

    detail2.metric(
        label="Tokens With Liquidity",
        value=(
            f"{metrics_summary['tokens_with_liquidity']:,}"
        ),
    )

    detail3.metric(
        label="Tokens With 24h Volume",
        value=(
            f"{metrics_summary['tokens_with_volume']:,}"
        ),
    )

    detail4.metric(
        label="Last Metrics Refresh",
        value=(
            metrics_summary["last_metrics_refresh"]
            or "Not available"
        ),
    )

    st.progress(
        min(
            max(
                coverage_percentage / 100,
                0.0,
            ),
            1.0,
        ),
        text=(
            f"Metrics coverage: "
            f"{tokens_with_metrics:,} of "
            f"{total_enabled:,} tokens"
        ),
    )

    st.caption(
        "Current scanner filters: "
        f"liquidity ≥ "
        f"${SCANNER_MINIMUM_LIQUIDITY_USD:,.0f} "
        "and 24-hour volume ≥ "
        f"${SCANNER_MINIMUM_VOLUME_24H_USD:,.0f}."
    )

except Exception as error:
    st.error(
        "The dashboard could not load the token-metrics "
        "system information."
    )
    st.code(str(error))


# -----------------------------
# Top market tokens
# -----------------------------
st.divider()

st.subheader("💧 Top Market Tokens")

liquidity_tab, volume_tab = st.tabs(
    [
        "Top by Liquidity",
        "Top by 24h Volume",
    ]
)

with liquidity_tab:
    try:
        top_liquidity_tokens = get_liquid_tokens(
            minimum_liquidity_usd=0,
            minimum_volume_24h_usd=0,
            limit=TOP_TOKEN_LIMIT,
        )

        liquidity_rows = format_token_table(
            top_liquidity_tokens
        )

        if liquidity_rows:
            st.dataframe(
                liquidity_rows,
                width="stretch",
                hide_index=True,
            )
        else:
            st.info(
                "No token liquidity information "
                "is stored yet."
            )

    except Exception as error:
        st.error(
            "The top-liquidity token table "
            "could not be loaded."
        )
        st.code(str(error))

with volume_tab:
    try:
        top_volume_tokens = (
            get_top_tokens_by_volume(
                limit=TOP_TOKEN_LIMIT
            )
        )

        volume_rows = format_token_table(
            top_volume_tokens
        )

        if volume_rows:
            st.dataframe(
                volume_rows,
                width="stretch",
                hide_index=True,
            )
        else:
            st.info(
                "No token volume information "
                "is stored yet."
            )

    except Exception as error:
        st.error(
            "The top-volume token table "
            "could not be loaded."
        )
        st.code(str(error))


# -----------------------------
# Paper trading statistics
# -----------------------------
st.divider()

statistics = get_trade_statistics()

st.subheader("Paper Trading Statistics")

stat1, stat2, stat3, stat4 = st.columns(4)

stat1.metric(
    label="Total Trades",
    value=statistics["total_trades"],
)

stat2.metric(
    label="Winning Trades",
    value=statistics["winning_trades"],
)

stat3.metric(
    label="Total Expected Profit",
    value=f"${statistics['total_profit']:.6f}",
)

stat4.metric(
    label="Win Rate",
    value=f"{statistics['win_rate']:.1f}%",
)

best_column, worst_column = st.columns(2)

best_column.metric(
    label="Best Paper Trade",
    value=f"${statistics['best_trade']:.6f}",
)

worst_column.metric(
    label="Worst Paper Trade",
    value=f"${statistics['worst_trade']:.6f}",
)


# -----------------------------
# Profit history chart
# -----------------------------
st.divider()

st.subheader("📈 Profit History")

profit_history = get_profit_history()

if profit_history:
    st.line_chart(profit_history)
else:
    st.info("No trades available to plot.")


# -----------------------------
# Multi-token scanner
# -----------------------------
st.divider()

st.subheader("📊 Multi-Token Scanner")

try:
    scanner_results = get_latest_scanner_results()

    display_rows = []

    for trade in scanner_results:
        display_rows.append(
            {
                "Token": trade["token"],
                "Market Score": round(
                    trade.get("market_score", 0),
                    2,
                ),
                "Buy Route": trade["buy_route"],
                "Sell Route": trade["sell_route"],
                "Starting Amount": round(
                    trade["starting_amount"],
                    6,
                ),
                "Ending Amount": round(
                    trade["ending_amount"],
                    6,
                ),
                "Quoted Profit": round(
                    trade["quoted_profit"],
                    6,
                ),
                "Estimated Cost": round(
                    trade["estimated_cost"],
                    6,
                ),
                "Net Profit": round(
                    trade["net_profit"],
                    6,
                ),
                "Decision": trade["decision"],
                "Last Scanned": trade.get(
                    "scanned_at",
                    "Unknown",
                ),
            }
        )

    if display_rows:
        st.dataframe(
            display_rows,
            width="stretch",
            hide_index=True,
        )

        best_scan = scanner_results[0]

        scanner_col1, scanner_col2 = (
            st.columns(2)
        )

        with scanner_col1:
            st.write(
                f"**Best token right now:** "
                f"{best_scan['token']}"
            )

            st.write(
                f"**Scanner decision:** "
                f"{best_scan['decision']}"
            )

            st.write(
                f"**Market score:** "
                f"{best_scan.get('market_score', 0):.2f}"
            )

        with scanner_col2:
            st.write(
                f"**Best estimated net profit:** "
                f"${best_scan['net_profit']:.6f}"
            )

            st.write(
                f"**Last automatic scan:** "
                f"{best_scan.get('scanned_at', 'Unknown')}"
            )

    else:
        st.info(
            "No scanner results are stored yet. "
            "Start the automatic scanner in a terminal."
        )

except (KeyError, ValueError, TypeError) as error:
    st.error(
        "The saved scanner information could not "
        "be displayed."
    )
    st.code(str(error))

except Exception as error:
    st.error(
        "The dashboard could not read the "
        "scanner database."
    )
    st.code(str(error))


# -----------------------------
# Historical analytics
# -----------------------------
render_historical_analytics()


# -----------------------------
# Live market prices
# -----------------------------
st.divider()

try:
    prices = get_crypto_prices()

    bitcoin_price = prices["bitcoin"]["usd"]
    ethereum_price = prices["ethereum"]["usd"]
    solana_price = prices["solana"]["usd"]

    st.subheader("Live Market")

    column1, column2, column3 = st.columns(3)

    column1.metric(
        label="Bitcoin",
        value=f"${bitcoin_price:,.2f}",
    )

    column2.metric(
        label="Ethereum",
        value=f"${ethereum_price:,.2f}",
    )

    column3.metric(
        label="Solana",
        value=f"${solana_price:,.2f}",
    )

except requests.RequestException as error:
    st.error(
        "The dashboard could not get live prices."
    )
    st.code(str(error))

except (KeyError, ValueError, TypeError) as error:
    st.error(
        "The live-price service returned "
        "unexpected information."
    )
    st.code(str(error))


# -----------------------------
# Bot and safety status
# -----------------------------
st.divider()

left_column, right_column = st.columns(2)

with left_column:
    st.subheader("Bot Status")
    st.write(
        "Automatic scanner: "
        "**Run separately in its terminal**"
    )
    st.write(
        "Continuous metrics updater: "
        "**Run separately in its terminal**"
    )
    st.write("Multi-token scanner: **Enabled**")
    st.write("Market-quality filter: **Enabled**")
    st.write(
        "Persistent scanner rotation: **Enabled**"
    )
    st.write(
        "Historical opportunity tracking: **Enabled**"
    )
    st.write("Parallel scanner: **Enabled**")
    st.write("Paper trading: **Enabled**")

    if LIVE_TRADING:
        st.error("Live trading is ON")
    else:
        st.success("Live trading is OFF")

with right_column:
    st.subheader("Safety Settings")

    st.write(
        f"Trade amount: "
        f"**${TRADE_AMOUNT_USD:.2f}**"
    )

    st.write(
        f"Maximum daily loss: "
        f"**${MAX_DAILY_LOSS_USD:.2f}**"
    )

    st.write(
        f"Minimum expected profit: "
        f"**${MIN_PROFIT_USD:.2f}**"
    )

    st.write(
        f"Minimum token liquidity: "
        f"**${SCANNER_MINIMUM_LIQUIDITY_USD:,.0f}**"
    )

    st.write(
        f"Minimum 24h token volume: "
        f"**${SCANNER_MINIMUM_VOLUME_24H_USD:,.0f}**"
    )


# -----------------------------
# Safety message
# -----------------------------
st.divider()

st.subheader("Important Safety Message")

st.warning(
    "This dashboard does not connect to a wallet and cannot "
    "place real trades. Prices and Jupiter quotes are used "
    "only for monitoring and paper testing."
)


# -----------------------------
# Live Jupiter quote
# -----------------------------
st.divider()

st.subheader("🔍 Live Jupiter Round-Trip Quote")

try:
    opportunity = find_best_opportunity()

    left, right = st.columns(2)

    with left:
        st.write("Buy Route")
        st.success(opportunity["buy"])

        st.write("Sell Route")
        st.success(opportunity["sell"])

    with right:
        st.metric(
            "Estimated Net Profit",
            f"${opportunity['profit']:.6f}",
        )

        st.metric(
            "Quote Source",
            opportunity["confidence"],
        )

    st.subheader(opportunity["decision"])

    st.write(
        f"**Starting Amount:** "
        f"${opportunity['starting_amount']:.6f}"
    )

    st.write(
        f"**Ending Amount:** "
        f"${opportunity['ending_amount']:.6f}"
    )

    st.write(
        f"**Quoted Profit:** "
        f"${opportunity['quoted_profit']:.6f}"
    )

    st.write(
        f"**Estimated Execution Cost:** "
        f"${opportunity['estimated_cost']:.6f}"
    )

    st.write(
        f"**Estimated Net Profit:** "
        f"${opportunity['profit']:.6f}"
    )

    st.write(
        f"**Price Impact (Buy):** "
        f"{opportunity['price_impact_1']}"
    )

    st.write(
        f"**Price Impact (Sell):** "
        f"{opportunity['price_impact_2']}"
    )

except requests.RequestException as error:
    st.error(
        "Jupiter could not provide the "
        "round-trip quote."
    )
    st.code(str(error))

except (KeyError, ValueError, TypeError) as error:
    st.error(
        "Jupiter returned information the dashboard "
        "could not understand."
    )
    st.code(str(error))


# -----------------------------
# Paper trade history
# -----------------------------
st.divider()

st.subheader("📜 Paper Trade History")

history = get_trade_history()

if history:
    st.dataframe(
        history,
        width="stretch",
        hide_index=True,
    )
else:
    st.info("No paper trades yet.")