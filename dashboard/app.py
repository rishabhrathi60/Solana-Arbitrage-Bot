import sys
from pathlib import Path

import requests
import streamlit as st
from streamlit_autorefresh import st_autorefresh


# This helps the dashboard find files in the main project folder.
PROJECT_FOLDER = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_FOLDER))

from config import (  # noqa: E402
    BOT_NAME,
    LIVE_TRADING,
    MAX_DAILY_LOSS_USD,
    MIN_PROFIT_USD,
    TRADE_AMOUNT_USD,
)
from execution.paper_trader import get_trade_history, paper_trade  # noqa: E402
from market_data import get_crypto_prices  # noqa: E402
from strategies.arbitrage import find_best_opportunity  # noqa: E402


st.set_page_config(
    page_title=BOT_NAME,
    page_icon="🤖",
    layout="wide",
)

st_autorefresh(interval=60000, key="refresh")

st.title("🤖 Rishabh Multi-Strategy Trading Bot")
st.caption("Live quote dashboard — live trading is turned off")


# -----------------------------
# Live market prices
# -----------------------------
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


# -----------------------------
# Bot and safety status
# -----------------------------
st.divider()

left_column, right_column = st.columns(2)

with left_column:
    st.subheader("Bot Status")
    st.write("Arbitrage strategy: **Live quote testing**")
    st.write("Trend strategy: **Coming soon**")
    st.write("Paper trading: **Enabled for approved test quotes**")

    if LIVE_TRADING:
        st.error("Live trading is ON")
    else:
        st.success("Live trading is OFF")

with right_column:
    st.subheader("Safety Settings")
    st.write(f"Trade amount: **${TRADE_AMOUNT_USD:.2f}**")
    st.write(f"Maximum daily loss: **${MAX_DAILY_LOSS_USD:.2f}**")
    st.write(f"Minimum expected profit: **${MIN_PROFIT_USD:.2f}**")


# -----------------------------
# Safety message
# -----------------------------
st.divider()

st.subheader("Important Safety Message")

st.warning(
    "This dashboard does not connect to a wallet and cannot place trades. "
    "The displayed prices and Jupiter quotes are for monitoring and testing."
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
            "Expected Profit",
            f"${opportunity['profit']:.6f}",
        )

        st.metric(
            "Quote Source",
            opportunity["confidence"],
        )

    st.subheader(opportunity["decision"])

    if "TEST FURTHER" in opportunity["decision"]:
        paper_trade(opportunity)

    st.write(f"**Ending Amount:** ${opportunity['ending_amount']:.6f}")
    st.write(f"**Price Impact (Buy):** {opportunity['price_impact_1']}")
    st.write(f"**Price Impact (Sell):** {opportunity['price_impact_2']}")

except requests.RequestException as error:
    st.error("Jupiter could not provide the round-trip quote.")
    st.code(str(error))

except (KeyError, ValueError, TypeError) as error:
    st.error("Jupiter returned information the dashboard could not understand.")
    st.code(str(error))


# -----------------------------
# Paper trade history
# -----------------------------
st.divider()

st.subheader("📜 Paper Trade History")

history = get_trade_history()

if history:
    st.dataframe(history, width="stretch")
else:
    st.info("No paper trades yet.")