import time
from datetime import datetime

import requests

from database.scanner_results import save_scanner_results
from execution.paper_trader import paper_trade
from strategies.multi_token_scanner import scan_all_tokens


SCAN_INTERVAL_SECONDS = 180
ERROR_WAIT_SECONDS = 60


def convert_scanner_result(result):
    """
    Convert a scanner result into the format
    expected by the paper-trading system.
    """

    return {
        "buy": result["buy_route"],
        "sell": result["sell_route"],
        "starting_amount": result["starting_amount"],
        "ending_amount": result["ending_amount"],
        "profit": result["net_profit"],
        "decision": result["decision"],
    }


def run_automatic_scanner():
    print("=" * 50)
    print("RISHABH AUTOMATIC PAPER SCANNER")
    print("=" * 50)
    print("Live trading: OFF")
    print("Wallet connected: NO")
    print(f"Scan interval: {SCAN_INTERVAL_SECONDS} seconds")
    print("Press Control + C to stop.")
    print()

    while True:
        scan_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        try:
            print(f"[{scan_time}] Starting token scan...")

            results = scan_all_tokens()

            save_scanner_results(results)
            print("Latest scanner results saved to the database.")

            successful_results = [
                result
                for result in results
                if result["decision"] != "⚠️ QUOTE ERROR"
            ]

            if not successful_results:
                print("No successful quotes were received.")

            for result in successful_results:
                token = result["token"]
                net_profit = result["net_profit"]
                decision = result["decision"]

                print(
                    f"{token}: "
                    f"net profit ${net_profit:.6f} — "
                    f"{decision}"
                )

                if result["eligible"]:
                    opportunity = convert_scanner_result(result)
                    saved_trade = paper_trade(opportunity)

                    print(
                        "PAPER TRADE SAVED: "
                        f"{token}, "
                        f"profit ${saved_trade['profit']:.6f}"
                    )

            print(
                f"Scan complete. Waiting "
                f"{SCAN_INTERVAL_SECONDS} seconds."
            )
            print("-" * 50)

            time.sleep(SCAN_INTERVAL_SECONDS)

        except requests.RequestException as error:
            print("Internet or quote error:")
            print(error)
            print(
                f"Waiting {ERROR_WAIT_SECONDS} seconds "
                "before trying again."
            )

            time.sleep(ERROR_WAIT_SECONDS)

        except (KeyError, TypeError, ValueError) as error:
            print("The scanner received unexpected information:")
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