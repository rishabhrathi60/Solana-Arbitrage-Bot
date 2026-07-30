import pandas as pd
import streamlit as st

from database.opportunity_history import (
    get_opportunity_history_summary,
    get_recent_opportunity_history,
    get_token_performance,
)


RECENT_HISTORY_LIMIT = 500
LEADERBOARD_LIMIT = 25
MINIMUM_SCANS_FOR_RANKING = 2


def safe_float(value):
    """
    Convert a value to float without raising an exception.
    """

    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value):
    """
    Convert a value to integer without raising an exception.
    """

    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def calculate_percentage(numerator, denominator):
    """
    Return a percentage while safely handling zero.
    """

    numerator = safe_float(numerator)
    denominator = safe_float(denominator)

    if denominator <= 0:
        return 0.0

    return numerator / denominator * 100


def calculate_average_scan_interval(history):
    """
    Estimate the average time between stored observations.

    This value is based on recent historical rows and is only
    shown when at least two distinct timestamps are available.
    """

    timestamps = {
        row.get("scanned_at")
        for row in history
        if row.get("scanned_at")
    }

    if len(timestamps) < 2:
        return None

    parsed_timestamps = pd.to_datetime(
        list(timestamps),
        errors="coerce",
    )

    parsed_timestamps = (
        parsed_timestamps
        .dropna()
        .sort_values()
    )

    if len(parsed_timestamps) < 2:
        return None

    differences = (
        parsed_timestamps
        .to_series()
        .diff()
        .dropna()
    )

    if differences.empty:
        return None

    average_seconds = (
        differences.dt.total_seconds().mean()
    )

    if pd.isna(average_seconds):
        return None

    return float(average_seconds)


def format_duration(seconds):
    """
    Convert seconds into a readable duration.
    """

    if seconds is None:
        return "Not enough data"

    seconds = max(0, float(seconds))

    if seconds < 60:
        return f"{seconds:.0f} sec"

    if seconds < 3600:
        return f"{seconds / 60:.1f} min"

    return f"{seconds / 3600:.1f} hr"


def prepare_recent_history_table(history):
    """
    Convert recent history into dashboard-friendly rows.
    """

    rows = []

    for record in history:
        rows.append(
            {
                "Time": (
                    record.get("scanned_at")
                    or "Unknown"
                ),
                "Token": (
                    record.get("token")
                    or "UNKNOWN"
                ),
                "Market Score": round(
                    safe_float(
                        record.get("market_score")
                    ),
                    2,
                ),
                "Starting Amount": round(
                    safe_float(
                        record.get("starting_amount")
                    ),
                    6,
                ),
                "Ending Amount": round(
                    safe_float(
                        record.get("ending_amount")
                    ),
                    6,
                ),
                "Quoted Profit": round(
                    safe_float(
                        record.get("quoted_profit")
                    ),
                    6,
                ),
                "Net Profit": round(
                    safe_float(
                        record.get("net_profit")
                    ),
                    6,
                ),
                "Decision": (
                    record.get("decision")
                    or "Unknown"
                ),
                "Quote Successful": (
                    "Yes"
                    if safe_int(
                        record.get("quote_successful")
                    )
                    else "No"
                ),
                "Error": (
                    record.get("error") or ""
                ),
            }
        )

    return rows


def prepare_performance_table(performance):
    """
    Convert aggregated token performance into table rows.
    """

    rows = []

    for record in performance:
        successful_quotes = safe_int(
            record.get("successful_quotes")
        )
        total_scans = safe_int(
            record.get("total_scans")
        )

        rows.append(
            {
                "Token": (
                    record.get("token")
                    or "UNKNOWN"
                ),
                "Total Scans": total_scans,
                "Successful Quotes": successful_quotes,
                "Quote Errors": safe_int(
                    record.get("quote_errors")
                ),
                "Quote Success Rate": round(
                    safe_float(
                        record.get(
                            "quote_success_rate"
                        )
                    ),
                    2,
                ),
                "Profitable Scans": safe_int(
                    record.get("profitable_scans")
                ),
                "Profitable Scan Rate": round(
                    safe_float(
                        record.get(
                            "profitable_scan_rate"
                        )
                    ),
                    2,
                ),
                "Average Net Profit": round(
                    safe_float(
                        record.get(
                            "average_net_profit"
                        )
                    ),
                    6,
                ),
                "Best Net Profit": round(
                    safe_float(
                        record.get(
                            "best_net_profit"
                        )
                    ),
                    6,
                ),
                "Worst Net Profit": round(
                    safe_float(
                        record.get(
                            "worst_net_profit"
                        )
                    ),
                    6,
                ),
                "Average Market Score": round(
                    safe_float(
                        record.get(
                            "average_market_score"
                        )
                    ),
                    2,
                ),
                "Last Scanned": (
                    record.get("last_scanned_at")
                    or "Unknown"
                ),
            }
        )

    return rows


def render_history_charts(history):
    """
    Display opportunity-history charts.
    """

    if not history:
        st.info(
            "No historical scanner observations "
            "are available yet."
        )
        return

    dataframe = pd.DataFrame(history)

    dataframe["scanned_at"] = pd.to_datetime(
        dataframe["scanned_at"],
        errors="coerce",
    )

    dataframe["net_profit"] = pd.to_numeric(
        dataframe["net_profit"],
        errors="coerce",
    ).fillna(0)

    dataframe["market_score"] = pd.to_numeric(
        dataframe["market_score"],
        errors="coerce",
    ).fillna(0)

    dataframe = dataframe.dropna(
        subset=["scanned_at"]
    )

    if dataframe.empty:
        st.info(
            "Historical timestamps could not "
            "be converted for charting."
        )
        return

    dataframe = dataframe.sort_values(
        "scanned_at"
    )

    chart_tab1, chart_tab2, chart_tab3 = st.tabs(
        [
            "Net Profit Over Time",
            "Market Score Over Time",
            "Observations by Hour",
        ]
    )

    with chart_tab1:
        profit_chart = (
            dataframe[
                [
                    "scanned_at",
                    "net_profit",
                ]
            ]
            .set_index("scanned_at")
        )

        st.line_chart(
            profit_chart,
            height=350,
        )

    with chart_tab2:
        score_chart = (
            dataframe[
                [
                    "scanned_at",
                    "market_score",
                ]
            ]
            .set_index("scanned_at")
        )

        st.line_chart(
            score_chart,
            height=350,
        )

    with chart_tab3:
        hourly_data = dataframe.copy()

        hourly_data["hour"] = (
            hourly_data["scanned_at"]
            .dt.floor("h")
        )

        hourly_counts = (
            hourly_data
            .groupby("hour")
            .size()
            .rename("Observations")
            .to_frame()
        )

        st.bar_chart(
            hourly_counts,
            height=350,
        )


def render_scanner_summary(
    summary,
    recent_history,
):
    """
    Display high-level historical scanner metrics.
    """

    total_observations = safe_int(
        summary.get("total_observations")
    )
    successful_quotes = safe_int(
        summary.get("successful_quotes")
    )
    quote_errors = safe_int(
        summary.get("quote_errors")
    )
    profitable_observations = safe_int(
        summary.get(
            "profitable_observations"
        )
    )

    quote_success_rate = calculate_percentage(
        successful_quotes,
        total_observations,
    )

    profitable_rate = calculate_percentage(
        profitable_observations,
        successful_quotes,
    )

    average_scan_interval = (
        calculate_average_scan_interval(
            recent_history
        )
    )

    row1_col1, row1_col2, row1_col3, row1_col4 = (
        st.columns(4)
    )

    row1_col1.metric(
        label="Historical Observations",
        value=f"{total_observations:,}",
    )

    row1_col2.metric(
        label="Unique Tokens",
        value=(
            f"{safe_int(summary.get('unique_tokens')):,}"
        ),
    )

    row1_col3.metric(
        label="Quote Success Rate",
        value=f"{quote_success_rate:.2f}%",
        help=(
            f"{successful_quotes:,} successful quotes "
            f"and {quote_errors:,} errors"
        ),
    )

    row1_col4.metric(
        label="Profitable Observation Rate",
        value=f"{profitable_rate:.2f}%",
        help=(
            "Profitable observations divided by "
            "successful quotes."
        ),
    )

    row2_col1, row2_col2, row2_col3, row2_col4 = (
        st.columns(4)
    )

    row2_col1.metric(
        label="Average Net Profit",
        value=(
            f"${safe_float(summary.get('average_net_profit')):.6f}"
        ),
    )

    row2_col2.metric(
        label="Best Net Profit",
        value=(
            f"${safe_float(summary.get('best_net_profit')):.6f}"
        ),
    )

    row2_col3.metric(
        label="Worst Net Profit",
        value=(
            f"${safe_float(summary.get('worst_net_profit')):.6f}"
        ),
    )

    row2_col4.metric(
        label="Average Scan Interval",
        value=format_duration(
            average_scan_interval
        ),
    )

    st.caption(
        "Last historical observation: "
        f"{summary.get('last_scanned_at') or 'Not available'}"
    )


def render_token_leaderboards(performance):
    """
    Display token-performance leaderboards.
    """

    if not performance:
        st.info(
            "Not enough historical token data is available "
            "for performance rankings."
        )
        return

    most_profitable = sorted(
        performance,
        key=lambda item: safe_float(
            item.get("average_net_profit")
        ),
        reverse=True,
    )

    most_reliable = sorted(
        performance,
        key=lambda item: (
            safe_float(
                item.get("quote_success_rate")
            ),
            safe_int(
                item.get("total_scans")
            ),
        ),
        reverse=True,
    )

    best_single_results = sorted(
        performance,
        key=lambda item: safe_float(
            item.get("best_net_profit")
        ),
        reverse=True,
    )

    worst_performers = sorted(
        performance,
        key=lambda item: safe_float(
            item.get("average_net_profit")
        ),
    )

    (
        profit_tab,
        reliability_tab,
        best_tab,
        worst_tab,
    ) = st.tabs(
        [
            "Highest Average Profit",
            "Most Reliable",
            "Best Single Result",
            "Lowest Average Profit",
        ]
    )

    with profit_tab:
        st.dataframe(
            prepare_performance_table(
                most_profitable[:LEADERBOARD_LIMIT]
            ),
            width="stretch",
            hide_index=True,
        )

    with reliability_tab:
        st.dataframe(
            prepare_performance_table(
                most_reliable[:LEADERBOARD_LIMIT]
            ),
            width="stretch",
            hide_index=True,
        )

    with best_tab:
        st.dataframe(
            prepare_performance_table(
                best_single_results[
                    :LEADERBOARD_LIMIT
                ]
            ),
            width="stretch",
            hide_index=True,
        )

    with worst_tab:
        st.dataframe(
            prepare_performance_table(
                worst_performers[
                    :LEADERBOARD_LIMIT
                ]
            ),
            width="stretch",
            hide_index=True,
        )


def render_historical_analytics():
    """
    Render the complete historical opportunity dashboard.
    """

    st.divider()
    st.subheader(
        "🧠 Historical Opportunity Analytics"
    )

    try:
        summary = (
            get_opportunity_history_summary()
        )

        recent_history = (
            get_recent_opportunity_history(
                limit=RECENT_HISTORY_LIMIT
            )
        )

        performance = get_token_performance(
            minimum_scans=(
                MINIMUM_SCANS_FOR_RANKING
            ),
            limit=500,
        )

        render_scanner_summary(
            summary,
            recent_history,
        )

        st.markdown(
            "### 🏆 Token Performance Leaderboards"
        )

        st.caption(
            "Rankings require at least "
            f"{MINIMUM_SCANS_FOR_RANKING} scans "
            "per token."
        )

        render_token_leaderboards(
            performance
        )

        st.markdown(
            "### 📈 Opportunity History Charts"
        )

        render_history_charts(
            recent_history
        )

        st.markdown(
            "### 🕒 Recent Historical Observations"
        )

        recent_rows = (
            prepare_recent_history_table(
                recent_history[:100]
            )
        )

        if recent_rows:
            st.dataframe(
                recent_rows,
                width="stretch",
                hide_index=True,
            )
        else:
            st.info(
                "No historical observations "
                "have been stored yet."
            )

    except Exception as error:
        st.error(
            "The historical analytics section "
            "could not be loaded."
        )
        st.code(str(error))