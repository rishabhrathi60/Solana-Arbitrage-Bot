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
from database.scanner_results import (  # noqa: E402
    get_latest_scanner_results,
)
from database.trades import (  # noqa: E402
    get_profit_history,
    get_trade_statistics,
)
from execution.paper_trader import (  # noqa: E402
    get_trade_history,
    paper_trade,
)
from market_data import get_crypto_prices  # noqa: E402
from strategies.arbitrage import find_best_opportunity  # noqa: E402


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
    "Automatic paper-trading dashboard — live trading is turned off"
)


# -----------------------------
# Paper trading statistics
# -----------------------------
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
        )

        best_scan = scanner_results[0]

        st.write(
            f"**Best token right now:** "
            f"{best_scan['token']}"
        )

        st.write(
            f"**Best estimated net profit:** "
            f"${best_scan['net_profit']:.6f}"
        )

        st.write(
            f"**Scanner decision:** "
            f"{best_scan['decision']}"
        )

        st.write(
            f"**Last automatic scan:** "
            f"{best_scan.get('scanned_at', 'Unknown')}"
        )

    else:
        st.info(
            "No scanner results are stored yet. "
            "Start the automatic scanner in Terminal 1."
        )

except (KeyError, ValueError, TypeError) as error:
    st.error(
        "The saved scanner information could not be displayed."
    )
    st.code(str(error))

except Exception as error:
    st.error(
        "The dashboard could not read the scanner database."
    )
    st.code(str(error))


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
    st.error("The dashboard could not get live prices.")
    st.code(str(error))

except (KeyError, ValueError, TypeError) as error:
    st.error(
        "The live-price service returned unexpected information."
    )
    st.code(str(error))


# -----------------------------
# Bot and safety status
# -----------------------------
st.divider()

left_column, right_column = st.columns(2)

with left_column:
    st.subheader("Bot Status")
    st.write("Automatic scanner: **Running separately**")
    st.write("Multi-token scanner: **Enabled**")
    st.write("Paper trading: **Enabled**")

    if LIVE_TRADING:
        st.error("Live trading is ON")
    else:
        st.success("Live trading is OFF")

with right_column:
    st.subheader("Safety Settings")
    st.write(
        f"Trade amount: **${TRADE_AMOUNT_USD:.2f}**"
    )
    st.write(
        f"Maximum daily loss: "
        f"**${MAX_DAILY_LOSS_USD:.2f}**"
    )
    st.write(
        f"Minimum expected profit: "
        f"**${MIN_PROFIT_USD:.2f}**"
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
        "Jupiter could not provide the round-trip quote."
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
    )
else:
    st.info("No paper trades yet.")