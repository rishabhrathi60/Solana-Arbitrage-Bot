import time
from datetime import datetime

import requests

from database.opportunity_history import (
    get_opportunity_history_summary,
    get_top_opportunity_tokens,
    save_opportunity_history,
)
from database.scanner_results import (
    save_scanner_results,
)
from execution.paper_trader import paper_trade
from strategies.multi_token_scanner import (
    scan_all_tokens,
)


SCAN_INTERVAL_SECONDS = 180
ERROR_WAIT_SECONDS = 60

TOP_TOKEN_MINIMUM_SCANS = 3
TOP_TOKEN_DISPLAY_LIMIT = 5


def current_timestamp():
    """
    Return the current local timestamp.
    """

    return datetime.now().strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def convert_scanner_result(result):
    """
    Convert a scanner result into the format expected by the
    paper-trading system.
    """

    return {
        "buy": result["buy_route"],
        "sell": result["sell_route"],
        "starting_amount": result["starting_amount"],
        "ending_amount": result["ending_amount"],
        "profit": result["net_profit"],
        "decision": result["decision"],
    }


def print_historical_summary():
    """
    Display the overall historical scanner summary.
    """

    summary = get_opportunity_history_summary()

    print("\nHistorical opportunity summary:")
    print(
        f"  Total observations: "
        f"{summary['total_observations']:,}"
    )
    print(
        f"  Unique tokens: "
        f"{summary['unique_tokens']:,}"
    )
    print(
        f"  Successful quotes: "
        f"{summary['successful_quotes']:,}"
    )
    print(
        f"  Quote errors: "
        f"{summary['quote_errors']:,}"
    )
    print(
        f"  Profitable observations: "
        f"{summary['profitable_observations']:,}"
    )
    print(
        f"  Average net profit: "
        f"${summary['average_net_profit']:.6f}"
    )
    print(
        f"  Best net profit: "
        f"${summary['best_net_profit']:.6f}"
    )


def print_top_historical_tokens():
    """
    Display the strongest tokens based on historical results.
    """

    top_tokens = get_top_opportunity_tokens(
        minimum_scans=TOP_TOKEN_MINIMUM_SCANS,
        limit=TOP_TOKEN_DISPLAY_LIMIT,
    )

    print(
        "\nTop historical opportunity tokens "
        f"(minimum {TOP_TOKEN_MINIMUM_SCANS} scans):"
    )

    if not top_tokens:
        print(
            "  Not enough historical observations yet."
        )
        return

    for position, token in enumerate(
        top_tokens,
        start=1,
    ):
        average_profit = float(
            token.get("average_net_profit") or 0
        )
        best_profit = float(
            token.get("best_net_profit") or 0
        )
        success_rate = float(
            token.get("quote_success_rate") or 0
        )
        total_scans = int(
            token.get("total_scans") or 0
        )

        print(
            f"  {position}. {token['token']} — "
            f"average ${average_profit:.6f}, "
            f"best ${best_profit:.6f}, "
            f"quote success {success_rate:.1f}%, "
            f"scans {total_scans}"
        )


def process_paper_trades(results):
    """
    Save paper trades for eligible scanner opportunities.
    """

    successful_results = [
        result
        for result in results
        if result.get("decision")
        != "⚠️ QUOTE ERROR"
    ]

    if not successful_results:
        print("No successful quotes were received.")
        return 0

    paper_trade_count = 0

    for result in successful_results:
        token = result.get("token") or "UNKNOWN"
        net_profit = float(
            result.get("net_profit") or 0
        )
        decision = (
            result.get("decision") or "Unknown"
        )
        market_score = float(
            result.get("market_score") or 0
        )

        print(
            f"{token}: "
            f"net profit ${net_profit:.6f} — "
            f"{decision} — "
            f"market score {market_score:.2f}"
        )

        if not result.get("eligible"):
            continue

        opportunity = convert_scanner_result(
            result
        )

        saved_trade = paper_trade(
            opportunity
        )

        print(
            "PAPER TRADE SAVED: "
            f"{token}, "
            f"profit "
            f"${saved_trade['profit']:.6f}"
        )

        paper_trade_count += 1

    return paper_trade_count


def run_one_scan_cycle():
    """
    Run one complete scanner cycle.

    Returns the scanner results so the function can be tested
    independently from the infinite loop.
    """

    scan_time = current_timestamp()

    print(
        f"[{scan_time}] Starting token scan..."
    )

    results = scan_all_tokens()

    latest_saved_count = save_scanner_results(
        results
    )

    print(
        "Latest scanner snapshot saved: "
        f"{latest_saved_count} rows."
    )

    history_saved_count = save_opportunity_history(
        results
    )

    print(
        "Historical scanner observations saved: "
        f"{history_saved_count} rows."
    )

    paper_trade_count = process_paper_trades(
        results
    )

    print(
        f"Paper trades saved this cycle: "
        f"{paper_trade_count}"
    )

    print_historical_summary()
    print_top_historical_tokens()

    return results


def run_automatic_scanner():
    """
    Run scanner cycles continuously.
    """

    print("=" * 60)
    print("RISHABH AUTOMATIC PAPER SCANNER")
    print("=" * 60)
    print("Live trading: OFF")
    print("Wallet connected: NO")
    print(
        f"Scan interval: "
        f"{SCAN_INTERVAL_SECONDS} seconds"
    )
    print(
        "Historical opportunity tracking: ON"
    )
    print("Press Control + C to stop.")
    print()

    while True:
        try:
            run_one_scan_cycle()

            print(
                f"\nScan complete. Waiting "
                f"{SCAN_INTERVAL_SECONDS} seconds."
            )
            print("-" * 60)

            time.sleep(
                SCAN_INTERVAL_SECONDS
            )

        except requests.RequestException as error:
            print("Internet or quote error:")
            print(error)
            print(
                f"Waiting {ERROR_WAIT_SECONDS} seconds "
                "before trying again."
            )

            time.sleep(ERROR_WAIT_SECONDS)

        except (
            KeyError,
            TypeError,
            ValueError,
        ) as error:
            print(
                "The scanner received unexpected "
                "information:"
            )
            print(error)
            print(
                f"Waiting {ERROR_WAIT_SECONDS} seconds "
                "before trying again."
            )

            time.sleep(ERROR_WAIT_SECONDS)

        except Exception as error:
            print("Unexpected scanner error:")
            print(error)
            print(
                f"Waiting {ERROR_WAIT_SECONDS} seconds "
                "before trying again."
            )

            time.sleep(ERROR_WAIT_SECONDS)


if __name__ == "__main__":
    try:
        run_automatic_scanner()

    except KeyboardInterrupt:
        print()
        print("Automatic scanner stopped safely.")