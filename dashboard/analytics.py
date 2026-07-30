import pandas as pd
import streamlit as st

from database.opportunity_history import (
    get_opportunity_history_summary,
    get_recent_opportunity_history,
    get_token_performance,
)


RECENT_HISTORY_LIMIT = 1000
RECENT_TABLE_LIMIT = 100
LEADERBOARD_LIMIT = 25
MINIMUM_SCANS_FOR_RANKING = 1

def safe_float(value):
    """
    Convert a value to float safely.
    """

    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def safe_int(value):
    """
    Convert a value to integer safely.
    """

    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return 0


def calculate_percentage(
    numerator,
    denominator,
):
    """
    Calculate a percentage safely.
    """

    numerator = safe_float(numerator)
    denominator = safe_float(denominator)

    if denominator <= 0:
        return 0.0

    return (
        numerator
        / denominator
        * 100
    )


def calculate_average_scan_interval(history):
    """
    Estimate the average time between scanner
    batches using distinct timestamps.
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

    timestamp_series = pd.Series(
        parsed_timestamps
    )

    differences = (
        timestamp_series
        .diff()
        .dropna()
    )

    if differences.empty:
        return None

    average_seconds = (
        differences
        .dt.total_seconds()
        .mean()
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

    seconds = max(
        0,
        float(seconds),
    )

    if seconds < 60:
        return f"{seconds:.0f} sec"

    if seconds < 3600:
        return (
            f"{seconds / 60:.1f} min"
        )

    return (
        f"{seconds / 3600:.1f} hr"
    )


def prepare_recent_history_table(history):
    """
    Convert recent observations into
    dashboard-friendly rows.
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
                        record.get(
                            "market_score"
                        )
                    ),
                    2,
                ),
                "Starting Amount": round(
                    safe_float(
                        record.get(
                            "starting_amount"
                        )
                    ),
                    6,
                ),
                "Ending Amount": round(
                    safe_float(
                        record.get(
                            "ending_amount"
                        )
                    ),
                    6,
                ),
                "Quoted Profit": round(
                    safe_float(
                        record.get(
                            "quoted_profit"
                        )
                    ),
                    6,
                ),
                "Estimated Cost": round(
                    safe_float(
                        record.get(
                            "estimated_cost"
                        )
                    ),
                    6,
                ),
                "Net Profit": round(
                    safe_float(
                        record.get(
                            "net_profit"
                        )
                    ),
                    6,
                ),
                "Eligible": (
                    "Yes"
                    if safe_int(
                        record.get("eligible")
                    )
                    else "No"
                ),
                "Quote Successful": (
                    "Yes"
                    if safe_int(
                        record.get(
                            "quote_successful"
                        )
                    )
                    else "No"
                ),
                "Decision": (
                    record.get("decision")
                    or "Unknown"
                ),
                "Error": (
                    record.get("error")
                    or ""
                ),
            }
        )

    return rows


def prepare_performance_table(performance):
    """
    Convert token performance into
    dashboard-friendly rows.
    """

    rows = []

    for record in performance:
        rows.append(
            {
                "Token": (
                    record.get("token")
                    or "UNKNOWN"
                ),
                "Total Scans": safe_int(
                    record.get("total_scans")
                ),
                "Successful Quotes": safe_int(
                    record.get(
                        "successful_quotes"
                    )
                ),
                "Quote Errors": safe_int(
                    record.get(
                        "quote_errors"
                    )
                ),
                "Quote Success Rate %": round(
                    safe_float(
                        record.get(
                            "quote_success_rate"
                        )
                    ),
                    2,
                ),
                "Eligible Scans": safe_int(
                    record.get(
                        "eligible_scans"
                    )
                ),
                "Eligible Rate %": round(
                    safe_float(
                        record.get(
                            "eligible_scan_rate"
                        )
                    ),
                    2,
                ),
                "Profitable Scans": safe_int(
                    record.get(
                        "profitable_scans"
                    )
                ),
                "Profitable Rate %": round(
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
                    record.get(
                        "last_scanned_at"
                    )
                    or "Unknown"
                ),
            }
        )

    return rows


def build_scan_cycle_dataframe(history):
    """
    Aggregate token observations into scanner
    batches using the shared scanned_at timestamp.
    """

    if not history:
        return pd.DataFrame()

    dataframe = pd.DataFrame(history)

    required_columns = (
        "scanned_at",
        "net_profit",
        "market_score",
        "quote_successful",
        "eligible",
    )

    for column in required_columns:
        if column not in dataframe.columns:
            dataframe[column] = 0

    dataframe["scanned_at"] = pd.to_datetime(
        dataframe["scanned_at"],
        errors="coerce",
    )

    numeric_columns = (
        "net_profit",
        "market_score",
        "quote_successful",
        "eligible",
    )

    for column in numeric_columns:
        dataframe[column] = pd.to_numeric(
            dataframe[column],
            errors="coerce",
        ).fillna(0)

    dataframe = dataframe.dropna(
        subset=["scanned_at"]
    )

    if dataframe.empty:
        return dataframe

    successful_rows = dataframe[
        dataframe["quote_successful"] == 1
    ].copy()

    cycle_counts = (
        dataframe
        .groupby("scanned_at")
        .agg(
            Observations=(
                "scanned_at",
                "size",
            ),
            Successful_Quotes=(
                "quote_successful",
                "sum",
            ),
            Eligible_Opportunities=(
                "eligible",
                "sum",
            ),
        )
    )

    if successful_rows.empty:
        cycle_counts[
            "Average_Net_Profit"
        ] = 0.0
        cycle_counts[
            "Best_Net_Profit"
        ] = 0.0
        cycle_counts[
            "Average_Market_Score"
        ] = 0.0

        return (
            cycle_counts
            .reset_index()
            .sort_values("scanned_at")
        )

    successful_summary = (
        successful_rows
        .groupby("scanned_at")
        .agg(
            Average_Net_Profit=(
                "net_profit",
                "mean",
            ),
            Best_Net_Profit=(
                "net_profit",
                "max",
            ),
            Average_Market_Score=(
                "market_score",
                "mean",
            ),
        )
    )

    combined = cycle_counts.join(
        successful_summary,
        how="left",
    )

    combined = combined.fillna(0)

    return (
        combined
        .reset_index()
        .sort_values("scanned_at")
    )


def render_history_charts(history):
    """
    Display scanner-batch historical charts.
    """

    cycle_dataframe = (
        build_scan_cycle_dataframe(
            history
        )
    )

    if cycle_dataframe.empty:
        st.info(
            "No historical scanner observations "
            "are available for charting."
        )
        return

    (
        profit_tab,
        score_tab,
        activity_tab,
        eligibility_tab,
    ) = st.tabs(
        [
            "Profit by Scan",
            "Market Score",
            "Scanner Activity",
            "Eligible Opportunities",
        ]
    )

    with profit_tab:
        profit_chart = (
            cycle_dataframe[
                [
                    "scanned_at",
                    "Average_Net_Profit",
                    "Best_Net_Profit",
                ]
            ]
            .set_index("scanned_at")
            .rename(
                columns={
                    "Average_Net_Profit": (
                        "Average Net Profit"
                    ),
                    "Best_Net_Profit": (
                        "Best Net Profit"
                    ),
                }
            )
        )

        st.line_chart(
            profit_chart,
            height=350,
        )

    with score_tab:
        score_chart = (
            cycle_dataframe[
                [
                    "scanned_at",
                    "Average_Market_Score",
                ]
            ]
            .set_index("scanned_at")
            .rename(
                columns={
                    "Average_Market_Score": (
                        "Average Market Score"
                    ),
                }
            )
        )

        st.line_chart(
            score_chart,
            height=350,
        )

    with activity_tab:
        activity_chart = (
            cycle_dataframe[
                [
                    "scanned_at",
                    "Observations",
                    "Successful_Quotes",
                ]
            ]
            .set_index("scanned_at")
            .rename(
                columns={
                    "Successful_Quotes": (
                        "Successful Quotes"
                    ),
                }
            )
        )

        st.bar_chart(
            activity_chart,
            height=350,
        )

    with eligibility_tab:
        eligible_chart = (
            cycle_dataframe[
                [
                    "scanned_at",
                    "Eligible_Opportunities",
                ]
            ]
            .set_index("scanned_at")
            .rename(
                columns={
                    "Eligible_Opportunities": (
                        "Eligible Opportunities"
                    ),
                }
            )
        )

        st.bar_chart(
            eligible_chart,
            height=350,
        )


def render_scanner_summary(
    summary,
    recent_history,
):
    """
    Display high-level historical
    scanner statistics.
    """

    total_observations = safe_int(
        summary.get(
            "total_observations"
        )
    )
    successful_quotes = safe_int(
        summary.get(
            "successful_quotes"
        )
    )
    quote_errors = safe_int(
        summary.get("quote_errors")
    )
    eligible_observations = safe_int(
        summary.get(
            "eligible_observations"
        )
    )
    profitable_observations = safe_int(
        summary.get(
            "profitable_observations"
        )
    )

    quote_success_rate = (
        calculate_percentage(
            successful_quotes,
            total_observations,
        )
    )

    eligible_rate = (
        calculate_percentage(
            eligible_observations,
            successful_quotes,
        )
    )

    profitable_rate = (
        calculate_percentage(
            profitable_observations,
            successful_quotes,
        )
    )

    average_scan_interval = (
        calculate_average_scan_interval(
            recent_history
        )
    )

    (
        row1_col1,
        row1_col2,
        row1_col3,
        row1_col4,
    ) = st.columns(4)

    row1_col1.metric(
        label="Historical Observations",
        value=f"{total_observations:,}",
    )

    row1_col2.metric(
        label="Scanner Cycles",
        value=(
            f"{safe_int(summary.get('scan_cycles')):,}"
        ),
    )

    row1_col3.metric(
        label="Unique Tokens",
        value=(
            f"{safe_int(summary.get('unique_tokens')):,}"
        ),
    )

    row1_col4.metric(
        label="Quote Success Rate",
        value=f"{quote_success_rate:.2f}%",
        help=(
            f"{successful_quotes:,} successful "
            f"quotes and {quote_errors:,} errors."
        ),
    )

    (
        row2_col1,
        row2_col2,
        row2_col3,
        row2_col4,
    ) = st.columns(4)

    row2_col1.metric(
        label="Eligible Opportunities",
        value=f"{eligible_observations:,}",
        delta=f"{eligible_rate:.2f}% of quotes",
        delta_color="off",
    )

    row2_col2.metric(
        label="Profitable Observations",
        value=f"{profitable_observations:,}",
        delta=f"{profitable_rate:.2f}% of quotes",
        delta_color="off",
    )

    row2_col3.metric(
        label="Average Market Score",
        value=(
            f"{safe_float(summary.get('average_market_score')):.2f}"
        ),
    )

    row2_col4.metric(
        label="Average Scan Interval",
        value=format_duration(
            average_scan_interval
        ),
    )

    (
        row3_col1,
        row3_col2,
        row3_col3,
    ) = st.columns(3)

    row3_col1.metric(
        label="Average Net Profit",
        value=(
            f"${safe_float(summary.get('average_net_profit')):.6f}"
        ),
    )

    row3_col2.metric(
        label="Best Net Profit",
        value=(
            f"${safe_float(summary.get('best_net_profit')):.6f}"
        ),
    )

    row3_col3.metric(
        label="Worst Net Profit",
        value=(
            f"${safe_float(summary.get('worst_net_profit')):.6f}"
        ),
    )

    st.caption(
        "Last historical observation: "
        f"{summary.get('last_scanned_at') or 'Not available'}"
    )


def render_token_leaderboards(
    performance,
):
    """
    Display historical token leaderboards.
    """

    if not performance:
        st.info(
            "Not enough historical token data "
            "is available for rankings."
        )
        return

    highest_average_profit = sorted(
        performance,
        key=lambda item: (
            safe_float(
                item.get(
                    "average_net_profit"
                )
            ),
            safe_int(
                item.get("total_scans")
            ),
        ),
        reverse=True,
    )

    most_reliable = sorted(
        performance,
        key=lambda item: (
            safe_float(
                item.get(
                    "quote_success_rate"
                )
            ),
            safe_int(
                item.get("total_scans")
            ),
        ),
        reverse=True,
    )

    most_eligible = sorted(
        performance,
        key=lambda item: (
            safe_float(
                item.get(
                    "eligible_scan_rate"
                )
            ),
            safe_int(
                item.get(
                    "eligible_scans"
                )
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

    lowest_average_profit = sorted(
        performance,
        key=lambda item: safe_float(
            item.get(
                "average_net_profit"
            )
        ),
    )

    (
        profit_tab,
        reliability_tab,
        eligibility_tab,
        best_tab,
        lowest_tab,
    ) = st.tabs(
        [
            "Highest Average Profit",
            "Most Reliable",
            "Most Frequently Eligible",
            "Best Single Result",
            "Lowest Average Profit",
        ]
    )

    with profit_tab:
        st.dataframe(
            prepare_performance_table(
                highest_average_profit[
                    :LEADERBOARD_LIMIT
                ]
            ),
            width="stretch",
            hide_index=True,
        )

    with reliability_tab:
        st.dataframe(
            prepare_performance_table(
                most_reliable[
                    :LEADERBOARD_LIMIT
                ]
            ),
            width="stretch",
            hide_index=True,
        )

    with eligibility_tab:
        st.dataframe(
            prepare_performance_table(
                most_eligible[
                    :LEADERBOARD_LIMIT
                ]
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

    with lowest_tab:
        st.dataframe(
            prepare_performance_table(
                lowest_average_profit[
                    :LEADERBOARD_LIMIT
                ]
            ),
            width="stretch",
            hide_index=True,
        )


def render_historical_analytics():
    """
    Render the complete historical
    opportunity analytics section.
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

        performance = (
            get_token_performance(
                minimum_scans=(
                    MINIMUM_SCANS_FOR_RANKING
                ),
                limit=500,
            )
        )

        if not recent_history:
            st.info(
                "No historical observations "
                "have been stored yet. Keep the "
                "automatic scanner running."
            )
            return

        render_scanner_summary(
            summary,
            recent_history,
        )

        st.markdown(
            "### 🏆 Token Performance Leaderboards"
        )

        st.caption(
            "Rankings require at least "
            f"{MINIMUM_SCANS_FOR_RANKING} "
            "historical observations per token."
        )

        render_token_leaderboards(
            performance
        )

        st.markdown(
            "### 📈 Historical Scanner Charts"
        )

        render_history_charts(
            recent_history
        )

        st.markdown(
            "### 🕒 Recent Historical Observations"
        )

        recent_rows = (
            prepare_recent_history_table(
                recent_history[
                    :RECENT_TABLE_LIMIT
                ]
            )
        )

        st.dataframe(
            recent_rows,
            width="stretch",
            hide_index=True,
        )

    except Exception as error:
        st.error(
            "The historical analytics section "
            "could not be loaded."
        )
        st.code(str(error))